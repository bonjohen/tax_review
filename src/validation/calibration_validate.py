"""Calibration validation: compare empirical ratios vs reform assumptions.

Produces two calibration tables:
  1. Loss Harvesting Ratios (from Table 1.4A: LT losses + carryovers vs gains)
  2. Holding Period Fractions (from SOCA Table 4: 1-5yr vs 5+yr LT gains)

Usage:
    python -m src.validation.calibration_validate [--years 2018 2019 2020 2021 2022]
"""

import argparse
import csv
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path

from src.etl.db import get_connection, DEFAULT_DB_PATH
from src.etl.url_registry import YEARS, SOCA_YEARS
from src.etl.agi_bins import BINS_BY_ID

logger = logging.getLogger(__name__)

ASSUMPTIONS_PATH = Path("data") / "parameters" / "reform_assumptions.json"
VALIDATION_DIR = Path("data") / "validation"


@dataclass
class CalibrationRow:
    """One row of calibration comparison."""
    metric: str           # 'loss_ratio' or 'holding_fraction'
    year: int
    agi_group: str        # AGI range label or 'all'
    agi_bin_ids: list[int]
    empirical_value: float | None
    assumed_value: float | None
    abs_diff: float | None
    pct_diff: float | None


@dataclass
class CalibrationReport:
    """Collection of calibration comparison rows."""
    rows: list[CalibrationRow] = field(default_factory=list)

    def add(self, row: CalibrationRow) -> None:
        self.rows.append(row)

    def summary(self) -> str:
        lines = ["Calibration Validation Report", "=" * 90]
        current_metric = None
        for r in self.rows:
            if r.metric != current_metric:
                current_metric = r.metric
                lines.append("")
                title = {
                    "loss_ratio": "LOSS HARVESTING RATIOS (Implied from Table 1.4A)",
                    "holding_fraction": "HOLDING PERIOD FRACTIONS (From SOCA Table 4)",
                }
                lines.append(title.get(r.metric, r.metric))
                lines.append("-" * 90)
                lines.append(f"{'Year':>6}  {'AGI Group':<35}  {'Empirical':>10}  "
                             f"{'Assumed':>10}  {'Diff':>8}  {'%Diff':>8}")
                lines.append("-" * 90)

            emp = f"{r.empirical_value:.4f}" if r.empirical_value is not None else "N/A"
            assum = f"{r.assumed_value:.4f}" if r.assumed_value is not None else "N/A"
            diff = f"{r.abs_diff:+.4f}" if r.abs_diff is not None else "N/A"
            pct = f"{r.pct_diff:+.1%}" if r.pct_diff is not None else "N/A"
            lines.append(f"{r.year:>6}  {r.agi_group:<35}  {emp:>10}  "
                         f"{assum:>10}  {diff:>8}  {pct:>8}")

        lines.append("=" * 90)
        return "\n".join(lines)


def _load_assumptions() -> dict:
    """Load reform_assumptions.json."""
    with open(ASSUMPTIONS_PATH) as f:
        return json.load(f)


def _agi_group_label(bin_ids: list[int]) -> str:
    """Build a human-readable label for a group of AGI bin IDs."""
    if len(bin_ids) == 1:
        return BINS_BY_ID[bin_ids[0]].label
    first = BINS_BY_ID[bin_ids[0]]
    last = BINS_BY_ID[bin_ids[-1]]
    if first.lower_bound == float("-inf"):
        return f"Under {_fmt_bound(last.upper_bound)}"
    if last.upper_bound == float("inf"):
        return f"{_fmt_bound(first.lower_bound)} or more"
    return f"{_fmt_bound(first.lower_bound)} to {_fmt_bound(last.upper_bound)}"


def _fmt_bound(v: float) -> str:
    """Format a dollar bound for display."""
    if v >= 1_000_000:
        return f"${v / 1_000_000:.1f}M"
    if v >= 1_000:
        return f"${v / 1_000:.0f}K"
    return f"${v:.0f}"


def compute_empirical_loss_ratios(conn, year: int) -> list[dict]:
    """Compute implied loss harvesting ratios from Table 1.4A by AGI bin.

    Formula per the v3 plan:
      Implied_Loss_Ratio = (LT losses + LT carryover) / (LT gains + LT losses + LT carryover)

    This gives a lower bound on harvesting intensity — how much of gross
    LT activity is loss-related.
    """
    rows = conn.execute("""
        SELECT agi_bin_id,
               COALESCE(long_term_gain, 0) AS lt_gain,
               COALESCE(long_term_loss, 0) AS lt_loss,
               COALESCE(lt_loss_carryover, 0) AS lt_carry
        FROM raw_table_14a
        WHERE year = ?
        ORDER BY agi_bin_id
    """, (year,)).fetchall()

    results = []
    for row in rows:
        lt_gain = row["lt_gain"]
        lt_loss = row["lt_loss"]
        lt_carry = row["lt_carry"]

        # Implied gross = net gains + losses used to offset
        denom = lt_gain + lt_loss + lt_carry
        if denom > 0:
            ratio = (lt_loss + lt_carry) / denom
        else:
            ratio = None

        results.append({
            "agi_bin_id": row["agi_bin_id"],
            "lt_gain": lt_gain,
            "lt_loss": lt_loss,
            "lt_carry": lt_carry,
            "empirical_ratio": ratio,
        })

    return results


def compute_empirical_holding_fractions(conn, soca_year: int = 2015) -> dict:
    """Compute holding period fractions from SOCA Table 4.

    Uses only 'all_assets' and 'long_term' rows. Returns dict with
    gains_1_to_5yr, gains_5yr_plus, fraction, and unknown share.
    """
    rows = conn.execute("""
        SELECT holding_duration, COALESCE(gain_amount, 0) AS gain
        FROM raw_soca_t4
        WHERE year = ? AND asset_type = 'all_assets' AND holding_period = 'long_term'
    """, (soca_year,)).fetchall()

    if not rows:
        return {"gains_1_to_5yr": 0, "gains_5yr_plus": 0, "fraction": None,
                "total_lt": 0, "unknown_share": None}

    from src.etl.parse_soca import classify_holding_duration

    gains_1_to_5 = 0
    gains_5_plus = 0
    gains_unknown = 0

    for row in rows:
        cls = classify_holding_duration(row["holding_duration"])
        gain = row["gain"]
        if cls == "lt_1_to_5yr":
            gains_1_to_5 += gain
        elif cls == "lt_5yr_plus":
            gains_5_plus += gain
        else:
            gains_unknown += gain

    total_lt = gains_1_to_5 + gains_5_plus + gains_unknown
    classifiable = gains_1_to_5 + gains_5_plus
    fraction = gains_1_to_5 / classifiable if classifiable > 0 else None
    unknown_share = gains_unknown / total_lt if total_lt > 0 else None

    return {
        "gains_1_to_5yr": gains_1_to_5,
        "gains_5yr_plus": gains_5_plus,
        "gains_unknown": gains_unknown,
        "total_lt": total_lt,
        "fraction": fraction,
        "unknown_share": unknown_share,
    }


def validate_calibration(conn, years: list[int] | None = None,
                         soca_year: int = 2015) -> CalibrationReport:
    """Run full calibration validation. Compare empirical vs assumed values."""
    years = years or YEARS
    assumptions = _load_assumptions()
    report = CalibrationReport()

    # --- Loss Harvesting Ratios ---
    loss_groups = assumptions["gross_loss_ratio"]["by_agi_group"]
    for year in years:
        empirical = compute_empirical_loss_ratios(conn, year)
        emp_by_bin = {r["agi_bin_id"]: r["empirical_ratio"] for r in empirical}

        for group in loss_groups:
            bin_ids = group["agi_bin_ids"]
            assumed = group["ratio"]

            # Average empirical ratio across bins in this group
            emp_vals = [emp_by_bin[b] for b in bin_ids if emp_by_bin.get(b) is not None]
            if emp_vals:
                # Weighted average would be better, but simple mean for comparison
                empirical_avg = sum(emp_vals) / len(emp_vals)
            else:
                empirical_avg = None

            abs_diff = (empirical_avg - assumed) if empirical_avg is not None else None
            pct_diff = abs_diff / assumed if abs_diff is not None and assumed != 0 else None

            report.add(CalibrationRow(
                metric="loss_ratio",
                year=year,
                agi_group=_agi_group_label(bin_ids),
                agi_bin_ids=bin_ids,
                empirical_value=empirical_avg,
                assumed_value=assumed,
                abs_diff=abs_diff,
                pct_diff=pct_diff,
            ))

    # --- Holding Period Fractions ---
    holding_groups = assumptions["holding_period_lt_under_5yr"]["by_agi_group"]
    soca_data = compute_empirical_holding_fractions(conn, soca_year)
    empirical_fraction = soca_data.get("fraction")

    # SOCA doesn't have AGI breakdowns for holding periods, so we apply
    # the same empirical fraction to each group with a note about the limitation
    for group in holding_groups:
        bin_ids = group["agi_bin_ids"]
        assumed = group["fraction"]
        abs_diff = (empirical_fraction - assumed) if empirical_fraction is not None else None
        pct_diff = abs_diff / assumed if abs_diff is not None and assumed != 0 else None

        report.add(CalibrationRow(
            metric="holding_fraction",
            year=soca_year,
            agi_group=_agi_group_label(bin_ids),
            agi_bin_ids=bin_ids,
            empirical_value=empirical_fraction,
            assumed_value=assumed,
            abs_diff=abs_diff,
            pct_diff=pct_diff,
        ))

    return report


def write_calibration_report(report: CalibrationReport) -> None:
    """Write calibration report as text and CSV."""
    VALIDATION_DIR.mkdir(parents=True, exist_ok=True)

    # Text report
    txt_path = VALIDATION_DIR / "calibration_report.txt"
    with open(txt_path, "w") as f:
        f.write(report.summary())
    logger.info(f"Wrote {txt_path}")

    # CSV report
    csv_path = VALIDATION_DIR / "calibration_report.csv"
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "metric", "year", "agi_group", "agi_bin_ids",
            "empirical_value", "assumed_value", "abs_diff", "pct_diff",
        ])
        for r in report.rows:
            writer.writerow([
                r.metric, r.year, r.agi_group, str(r.agi_bin_ids),
                r.empirical_value, r.assumed_value, r.abs_diff, r.pct_diff,
            ])
    logger.info(f"Wrote {csv_path}")


def main():
    parser = argparse.ArgumentParser(description="Run calibration validation")
    parser.add_argument("--years", nargs="+", type=int, default=YEARS)
    parser.add_argument("--soca-year", type=int, default=2015,
                        help="SOCA year for holding period data (default: 2015)")
    parser.add_argument("--db", type=str, default=None)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    conn = get_connection(args.db)
    report = validate_calibration(conn, args.years, soca_year=args.soca_year)
    conn.close()

    print()
    print(report.summary())
    write_calibration_report(report)


if __name__ == "__main__":
    main()
