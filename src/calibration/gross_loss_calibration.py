"""Gross loss ratio calibration pipeline.

Three complementary approaches combined into a single empirical estimate
per AGI group per year:

  Approach 1: Reconstruct gross LT activity from Table 1.4A components
  Approach 2: Adjust for non-taxable gain bias (~0.5% of dollars)
  Approach 3: Coverage bounds from PDF validation data

Usage:
    python -m src.calibration.gross_loss_calibration              # all years
    python -m src.calibration.gross_loss_calibration --year 2020  # single year
"""

import argparse
import json
import logging
from pathlib import Path

from src.etl.agi_bins import BINS_BY_ID
from src.etl.parse_table14a import YEARS, RAW_DIR, parse_table14a_extended

logger = logging.getLogger(__name__)

OUTPUT_DIR = Path("data/derived")
ASSUMPTIONS_PATH = Path("data/parameters/reform_assumptions.json")

# AGI groups matching reform_assumptions.json
AGI_GROUPS = {
    "Under 30K":  [1, 2, 3, 4, 5, 6, 7],
    "30K-100K":   [8, 9, 10, 11],
    "100K-500K":  [12, 13],
    "500K-5M":    [14, 15, 16, 17],
    "5M+":        [18, 19],
}

# Income-graduated internal loss ratios for opaque sources (partnership/S-corp)
# These represent the fraction of GROSS partnership activity that consists of
# losses netted internally before K-1 reporting.
INTERNAL_RATE_DEFAULTS = {
    "Under 30K":  0.05,
    "30K-100K":   0.10,
    "100K-500K":  0.20,
    "500K-5M":    0.30,
    "5M+":        0.40,
}

# Sensitivity analysis range for internal rates
SENSITIVITY_RATES = [0.0, 0.10, 0.20, 0.30, 0.40, 0.50, 0.60]


def _load_assumptions() -> dict:
    """Load reform_assumptions.json."""
    with open(ASSUMPTIONS_PATH) as f:
        return json.load(f)


def _index_by_bin(rows: list[dict], section: str = "all_returns") -> dict[int, dict]:
    """Index extended rows by agi_bin_id for a given section."""
    out = {}
    for r in rows:
        if r["section"] != section:
            continue
        bid = r["agi_bin_id"]
        if isinstance(bid, int) and bid not in out:
            out[bid] = r
    return out


def _aggregate_group(by_bin: dict[int, dict], bin_ids: list[int]) -> dict:
    """Sum numeric fields across bins in a group."""
    agg = {}
    for bid in bin_ids:
        row = by_bin.get(bid)
        if row is None:
            continue
        for k, v in row.items():
            if k in ("year", "section", "agi_bin_label", "agi_bin_id"):
                continue
            if isinstance(v, (int, float)):
                agg[k] = agg.get(k, 0.0) + v
    return agg


# ---------------------------------------------------------------------------
# Approach 1: Reconstruct Gross from Components
# ---------------------------------------------------------------------------

def approach1_bin(row: dict, internal_rate: float) -> dict:
    """Run Approach 1 for a single AGI bin at a given internal rate.

    Returns dict with layer details and reconstructed gross loss ratio.
    """
    # Layer 1: Transaction-level gross from sub-categories
    gross_lt_gain_txn = (
        row.get("sub_a_gain_amount", 0)
        + row.get("sub_b_gain_amount", 0)
        + row.get("sub_c_gain_amount", 0)
        + row.get("sub_d_gain_amount", 0)
    )
    gross_lt_loss_txn = (
        row.get("sub_a_loss_amount", 0)
        + row.get("sub_b_loss_amount", 0)
        + row.get("sub_c_loss_amount", 0)
        + row.get("sub_d_loss_amount", 0)
    )

    # Layer 2: Within-sales per-return netting
    net_lt_gain_sales = row.get("lt_gain_sales_amount", 0)
    netting_L2 = gross_lt_gain_txn - net_lt_gain_sales

    # Layer 3: Opaque sources — estimate hidden gross via internal loss ratio
    ps_net_gain = row.get("partnership_gain_amount", 0)
    ps_net_loss = abs(row.get("partnership_loss_amount", 0))
    other_net_gain = row.get("other_forms_gain_amount", 0)
    other_net_loss = abs(row.get("other_forms_loss_amount", 0))
    cap_gain_dist = row.get("cap_gain_dist_amount", 0)

    if internal_rate < 1.0:
        # For positive net gains: gross_gain = net / (1 - r), hidden_loss = net * r / (1 - r)
        ps_gross_gain = ps_net_gain / (1 - internal_rate) if ps_net_gain > 0 else ps_net_gain
        ps_hidden_loss = ps_net_gain * internal_rate / (1 - internal_rate) if ps_net_gain > 0 else 0
        other_gross_gain = other_net_gain / (1 - internal_rate) if other_net_gain > 0 else other_net_gain
        other_hidden_loss = other_net_gain * internal_rate / (1 - internal_rate) if other_net_gain > 0 else 0
    else:
        ps_gross_gain = ps_net_gain
        ps_hidden_loss = 0
        other_gross_gain = other_net_gain
        other_hidden_loss = 0

    total_gross_gain = gross_lt_gain_txn + ps_gross_gain + other_gross_gain + cap_gain_dist
    total_gross_loss = (
        gross_lt_loss_txn
        + ps_hidden_loss + other_hidden_loss
        + ps_net_loss + other_net_loss
    )

    denom = total_gross_gain + total_gross_loss
    ratio = total_gross_loss / denom if denom > 0 else None

    return {
        "gross_lt_gain_txn": gross_lt_gain_txn,
        "gross_lt_loss_txn": gross_lt_loss_txn,
        "netting_L2": netting_L2,
        "ps_gross_gain": ps_gross_gain,
        "ps_hidden_loss": ps_hidden_loss,
        "ps_visible_loss": ps_net_loss,
        "other_gross_gain": other_gross_gain,
        "other_hidden_loss": other_hidden_loss,
        "other_visible_loss": other_net_loss,
        "cap_gain_dist": cap_gain_dist,
        "total_gross_gain": total_gross_gain,
        "total_gross_loss": total_gross_loss,
        "internal_rate": internal_rate,
        "ratio": ratio,
    }


def approach1_year(rows: list[dict], year: int) -> dict:
    """Run Approach 1 for all AGI groups in a given year.

    Returns per-group results with default estimate + sensitivity analysis.
    """
    by_bin = _index_by_bin(rows, "all_returns")
    results = {"year": year, "groups": {}}

    for grp_name, bin_ids in AGI_GROUPS.items():
        agg = _aggregate_group(by_bin, bin_ids)
        if not agg:
            continue

        default_rate = INTERNAL_RATE_DEFAULTS[grp_name]
        best = approach1_bin(agg, default_rate)

        sensitivity = {}
        for rate in SENSITIVITY_RATES:
            s = approach1_bin(agg, rate)
            sensitivity[f"{rate:.0%}"] = s["ratio"]

        # Transaction-level loss ratio (direct sales only)
        txn_denom = best["gross_lt_gain_txn"] + best["gross_lt_loss_txn"]
        txn_loss_ratio = best["gross_lt_loss_txn"] / txn_denom if txn_denom > 0 else None

        results["groups"][grp_name] = {
            "bin_ids": bin_ids,
            "default_internal_rate": default_rate,
            "best_estimate_ratio": best["ratio"],
            "txn_loss_ratio": txn_loss_ratio,
            "total_gross_gain": best["total_gross_gain"],
            "total_gross_loss": best["total_gross_loss"],
            "layers": {
                "gross_lt_gain_txn": best["gross_lt_gain_txn"],
                "gross_lt_loss_txn": best["gross_lt_loss_txn"],
                "netting_L2": best["netting_L2"],
                "ps_gross_gain": best["ps_gross_gain"],
                "ps_hidden_loss": best["ps_hidden_loss"],
                "ps_visible_loss": best["ps_visible_loss"],
                "other_gross_gain": best["other_gross_gain"],
                "other_hidden_loss": best["other_hidden_loss"],
                "other_visible_loss": best["other_visible_loss"],
                "cap_gain_dist": best["cap_gain_dist"],
            },
            "sensitivity": sensitivity,
        }

    return results


# ---------------------------------------------------------------------------
# Approach 2: Non-Taxable Gain Adjustment
# ---------------------------------------------------------------------------

def approach2_year(rows: list[dict], year: int) -> dict:
    """Run Approach 2 for all AGI groups: adjust for non-taxable gain bias.

    From high-income PDF cross-check: ~10% of gain *filers* are non-taxable
    but carry only ~0.5% of *dollars*. Loss data is exact (0.00% difference).
    """
    by_bin_all = _index_by_bin(rows, "all_returns")

    # Internal consistency check: compare all-returns vs taxable-returns
    by_bin_tax = _index_by_bin(rows, "taxable_returns")
    consistency = {}
    for bid in sorted(by_bin_all.keys()):
        a = by_bin_all[bid]
        t = by_bin_tax.get(bid)
        if t is None:
            continue
        all_gain = a.get("lt_gain_amount", 0)
        tax_gain = t.get("lt_gain_amount", 0)
        all_loss = a.get("lt_loss_amount", 0)
        tax_loss = t.get("lt_loss_amount", 0)
        gain_diff_pct = (all_gain - tax_gain) / all_gain if all_gain > 0 else None
        loss_diff_pct = (all_loss - tax_loss) / all_loss if all_loss > 0 else None
        consistency[bid] = {
            "all_lt_gain": all_gain,
            "taxable_lt_gain": tax_gain,
            "gain_diff_pct": gain_diff_pct,
            "all_lt_loss": all_loss,
            "taxable_lt_loss": tax_loss,
            "loss_diff_pct": loss_diff_pct,
        }

    results = {"year": year, "groups": {}, "consistency_check": consistency}

    for grp_name, bin_ids in AGI_GROUPS.items():
        agg = _aggregate_group(by_bin_all, bin_ids)
        if not agg:
            continue

        lt_gain = agg.get("lt_gain_amount", 0)
        lt_loss = abs(agg.get("lt_loss_amount", 0))
        lt_carry = agg.get("lt_carry_amount", 0)

        # Unadjusted empirical ratio (lower bound)
        denom = lt_gain + lt_loss + lt_carry
        unadjusted_ratio = (lt_loss + lt_carry) / denom if denom > 0 else None

        # Adjusted: expand gain by 0.5% for non-taxable bias
        gain_expansion = 0.005
        adjusted_lt_gain = lt_gain / (1 - gain_expansion)
        adj_denom = adjusted_lt_gain + lt_loss + lt_carry
        adjusted_ratio = (lt_loss + lt_carry) / adj_denom if adj_denom > 0 else None

        adjustment_magnitude = None
        if unadjusted_ratio is not None and adjusted_ratio is not None:
            adjustment_magnitude = unadjusted_ratio - adjusted_ratio

        results["groups"][grp_name] = {
            "bin_ids": bin_ids,
            "lt_gain": lt_gain,
            "lt_loss": lt_loss,
            "lt_carry": lt_carry,
            "unadjusted_ratio": unadjusted_ratio,
            "gain_expansion_pct": gain_expansion,
            "adjusted_lt_gain": adjusted_lt_gain,
            "adjusted_ratio": adjusted_ratio,
            "adjustment_magnitude": adjustment_magnitude,
        }

    return results


# ---------------------------------------------------------------------------
# Approach 3: Coverage Bounds from PDF Validation
# ---------------------------------------------------------------------------

def approach3_year(rows: list[dict], year: int) -> dict:
    """Run Approach 3: establish upper/lower bounds on empirical ratio.

    For $200K+ bins (13-19): tight bounds from PDF validation (0-0.5% undercount)
    For <$200K bins (1-12): wider bounds (0-2% undercount, no PDF data)
    """
    by_bin = _index_by_bin(rows, "all_returns")
    results = {"year": year, "groups": {}}

    for grp_name, bin_ids in AGI_GROUPS.items():
        bin_results = []
        for bid in bin_ids:
            row = by_bin.get(bid)
            if row is None:
                continue

            lt_gain = row.get("lt_gain_amount", 0)
            lt_loss = abs(row.get("lt_loss_amount", 0))
            lt_carry = row.get("lt_carry_amount", 0)

            denom = lt_gain + lt_loss + lt_carry
            empirical = (lt_loss + lt_carry) / denom if denom > 0 else None

            # Determine bounds based on whether we have PDF validation
            has_pdf = bid >= 13  # $200K+ bins
            if has_pdf:
                # Tight: gain expanded by at most 0.5%, loss data exact
                expansion = 0.005
                confidence = "high"
            else:
                # Wider: assume 0-2% undercount, no PDF reference
                expansion = 0.02
                confidence = "medium"

            if empirical is not None and lt_gain > 0:
                expanded_gain = lt_gain / (1 - expansion)
                lower_denom = expanded_gain + lt_loss + lt_carry
                lower_bound = (lt_loss + lt_carry) / lower_denom if lower_denom > 0 else empirical
                upper_bound = empirical  # loss data exact, so unadjusted = upper
            else:
                lower_bound = empirical
                upper_bound = empirical

            bin_results.append({
                "agi_bin_id": bid,
                "lt_gain": lt_gain,
                "lt_loss": lt_loss,
                "lt_carry": lt_carry,
                "empirical": empirical,
                "lower_bound": lower_bound,
                "upper_bound": upper_bound,
                "confidence": confidence,
            })

        # Dollar-weighted group bounds
        total_lt_gain = sum(b["lt_gain"] for b in bin_results)
        total_lt_loss = sum(b["lt_loss"] for b in bin_results)
        total_lt_carry = sum(b["lt_carry"] for b in bin_results)
        total_denom = total_lt_gain + total_lt_loss + total_lt_carry

        group_empirical = (total_lt_loss + total_lt_carry) / total_denom if total_denom > 0 else None

        # Weighted bounds
        w_lower_num = sum(b["lt_loss"] + b["lt_carry"] for b in bin_results)
        if bin_results:
            has_pdf_any = any(b["confidence"] == "high" for b in bin_results)
            all_high = all(b["confidence"] == "high" for b in bin_results)
            group_confidence = "high" if all_high else "medium"

            # Use worst-case expansion for group lower bound
            max_expansion = 0.005 if all_high else 0.02
            expanded_gain = total_lt_gain / (1 - max_expansion) if total_lt_gain > 0 else 0
            lower_denom = expanded_gain + total_lt_loss + total_lt_carry
            group_lower = (total_lt_loss + total_lt_carry) / lower_denom if lower_denom > 0 else group_empirical
            group_upper = group_empirical
        else:
            group_confidence = "medium"
            group_lower = group_empirical
            group_upper = group_empirical

        results["groups"][grp_name] = {
            "bin_ids": bin_ids,
            "empirical": group_empirical,
            "lower_bound": group_lower,
            "upper_bound": group_upper,
            "confidence": group_confidence,
            "per_bin": bin_results,
        }

    return results


# ---------------------------------------------------------------------------
# Combiner: merge all 3 approaches into final calibrated estimates
# ---------------------------------------------------------------------------

def combine_approaches(a1: dict, a2: dict, a3: dict, year: int) -> dict:
    """Combine Approaches 1-3 into final calibrated estimate per AGI group.

    1. Start with Approach 1 best_estimate_ratio
    2. Apply Approach 2 correction (small downward shift)
    3. Clip to Approach 3 bounds
    """
    results = {"year": year, "groups": {}}

    for grp_name in AGI_GROUPS:
        g1 = a1["groups"].get(grp_name, {})
        g2 = a2["groups"].get(grp_name, {})
        g3 = a3["groups"].get(grp_name, {})

        raw_estimate = g1.get("best_estimate_ratio")
        adjustment = g2.get("adjustment_magnitude", 0) or 0
        lower = g3.get("lower_bound")
        upper = g3.get("upper_bound")

        if raw_estimate is not None:
            # Apply Approach 2 downward correction
            adjusted = raw_estimate - abs(adjustment)

            # Clip to Approach 3 bounds
            if lower is not None and adjusted < lower:
                adjusted = lower
            if upper is not None and adjusted > upper:
                adjusted = upper

            calibrated = adjusted
        else:
            calibrated = None

        results["groups"][grp_name] = {
            "bin_ids": AGI_GROUPS[grp_name],
            "approach1_raw": raw_estimate,
            "approach2_adjustment": -abs(adjustment) if adjustment else 0,
            "approach3_lower": lower,
            "approach3_upper": upper,
            "calibrated_ratio": calibrated,
            "confidence": g3.get("confidence", "medium"),
        }

    return results


# ---------------------------------------------------------------------------
# Summary: multi-year + comparison to current assumptions
# ---------------------------------------------------------------------------

def build_summary(combined_by_year: dict[int, dict]) -> dict:
    """Build multi-year summary matching reform_assumptions.json schema."""
    assumptions = _load_assumptions()
    current = {
        tuple(g["agi_bin_ids"]): g["ratio"]
        for g in assumptions["gross_loss_ratio"]["by_agi_group"]
    }

    groups_summary = {}
    for grp_name, bin_ids in AGI_GROUPS.items():
        yearly_ratios = {}
        for year, comb in sorted(combined_by_year.items()):
            g = comb["groups"].get(grp_name, {})
            r = g.get("calibrated_ratio")
            if r is not None:
                yearly_ratios[str(year)] = round(r, 4)

        all_ratios = list(yearly_ratios.values())
        mean_ratio = sum(all_ratios) / len(all_ratios) if all_ratios else None

        current_assumption = current.get(tuple(bin_ids))
        diff = None
        if mean_ratio is not None and current_assumption is not None:
            diff = round(mean_ratio - current_assumption, 4)

        first_comb = next(iter(combined_by_year.values()), {})
        confidence = first_comb.get("groups", {}).get(grp_name, {}).get("confidence", "medium")

        groups_summary[grp_name] = {
            "agi_bin_ids": bin_ids,
            "by_year": yearly_ratios,
            "mean_calibrated_ratio": round(mean_ratio, 4) if mean_ratio is not None else None,
            "current_assumption": current_assumption,
            "difference": diff,
            "confidence": confidence,
        }

    return {
        "description": "Calibrated gross loss ratios from 3-approach pipeline",
        "years": sorted(combined_by_year.keys()),
        "by_agi_group": groups_summary,
        "vs_current_assumptions": {
            grp: {
                "current": gs["current_assumption"],
                "calibrated": gs["mean_calibrated_ratio"],
                "diff": gs["difference"],
                "confidence": gs["confidence"],
            }
            for grp, gs in groups_summary.items()
        },
    }


# ---------------------------------------------------------------------------
# JSON serializer + file output
# ---------------------------------------------------------------------------

def _json_safe(obj):
    """Make objects JSON-serializable."""
    if isinstance(obj, float):
        if obj != obj:  # NaN
            return None
        return round(obj, 6) if abs(obj) < 1e12 else round(obj, 0)
    return obj


def _clean_for_json(d):
    """Recursively clean a dict/list for JSON serialization."""
    if isinstance(d, dict):
        return {k: _clean_for_json(v) for k, v in d.items()}
    if isinstance(d, list):
        return [_clean_for_json(v) for v in d]
    return _json_safe(d)


def _write_json(path: Path, data: dict) -> None:
    """Write JSON with consistent formatting."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(_clean_for_json(data), f, indent=2)
    logger.info(f"Wrote {path}")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def run_calibration(years: list[int] | None = None) -> dict:
    """Run full calibration pipeline for specified years."""
    if years is None:
        years = sorted(YEARS.keys())

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    combined_by_year = {}

    for year in years:
        filepath = RAW_DIR / str(year) / YEARS[year]
        if not filepath.exists():
            logger.warning(f"File not found: {filepath}")
            continue

        logger.info(f"\n{'='*60}")
        logger.info(f"Processing TY{year}")
        logger.info(f"{'='*60}")

        rows, totals = parse_table14a_extended(filepath, year)

        # Run all 3 approaches
        a1 = approach1_year(rows, year)
        a2 = approach2_year(rows, year)
        a3 = approach3_year(rows, year)
        combined = combine_approaches(a1, a2, a3, year)

        combined_by_year[year] = combined

        # Write per-year outputs
        _write_json(OUTPUT_DIR / f"calibration_approach1_{year}.json", a1)
        _write_json(OUTPUT_DIR / f"calibration_approach2_{year}.json", a2)
        _write_json(OUTPUT_DIR / f"calibration_approach3_{year}.json", a3)
        _write_json(OUTPUT_DIR / f"calibration_combined_{year}.json", combined)

    # Build and write summary
    summary = build_summary(combined_by_year)
    _write_json(OUTPUT_DIR / "calibration_summary.json", summary)

    # Print summary table
    print(f"\n{'='*80}")
    print("CALIBRATION SUMMARY: Gross Loss Ratios")
    print(f"{'='*80}")
    print(f"\n{'AGI Group':<14} {'Current':>8} {'Calibrated':>10} {'Diff':>8} {'Conf':>8}")
    print("-" * 56)
    for grp_name, gs in summary["by_agi_group"].items():
        cur = gs["current_assumption"]
        cal = gs["mean_calibrated_ratio"]
        diff = gs["difference"]
        conf = gs["confidence"]
        cur_s = f"{cur:.2f}" if cur is not None else "N/A"
        cal_s = f"{cal:.4f}" if cal is not None else "N/A"
        diff_s = f"{diff:+.4f}" if diff is not None else "N/A"
        print(f"{grp_name:<14} {cur_s:>8} {cal_s:>10} {diff_s:>8} {conf:>8}")

    print(f"\n{'AGI Group':<14}", end="")
    for year in sorted(combined_by_year.keys()):
        print(f" {'TY'+str(year):>10}", end="")
    print()
    print("-" * (14 + 11 * len(combined_by_year)))
    for grp_name in AGI_GROUPS:
        gs = summary["by_agi_group"].get(grp_name, {})
        print(f"{grp_name:<14}", end="")
        by_year = gs.get("by_year", {})
        for year in sorted(combined_by_year.keys()):
            v = by_year.get(str(year))
            print(f" {v:>10.4f}" if v is not None else f" {'N/A':>10}", end="")
        print()

    print(f"\nOutput files in {OUTPUT_DIR}/:")
    for f in sorted(OUTPUT_DIR.glob("calibration_*.json")):
        print(f"  {f.name}")

    return summary


def main():
    parser = argparse.ArgumentParser(
        description="Run gross loss ratio calibration pipeline"
    )
    parser.add_argument("--year", type=int, help="Single year to process")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    years = [args.year] if args.year else None
    run_calibration(years=years)


if __name__ == "__main__":
    main()
