"""Export dashboard data from SQLite to JSON for the web dashboard.

Queries the tax_review database and writes src/web/data/dashboard.json
containing summary stats, capital gains by bin, concentration curves,
reform revenue estimates, and bracket distribution data.

Usage:
    python -m src.web.export_data
"""

import json
import logging
import sqlite3
from pathlib import Path

from src.etl.db import get_connection
from src.reform.assumptions import load_assumptions, get_holding_period_fraction, get_gross_loss_ratio
from src.reform.marginal_rate import load_bracket_thresholds, compute_weighted_marginal_rate
from src.reform.filing_status import compute_reform_by_filing_status

logger = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).parent / "data"
OUTPUT_FILE = OUTPUT_DIR / "dashboard.json"

YEARS = [2018, 2019, 2020, 2021, 2022, 2023]
PREFERENTIAL_CG_RATE = 0.15  # typical preferential capital gains rate
ANNUAL_LOSS_DEDUCTION_LIMIT = 3000  # IRC §1211(b) — $3K/year cap on capital losses vs ordinary income


def _query_summary(conn: sqlite3.Connection) -> list[dict]:
    """Per-year totals: returns, AGI, tax, LT/ST gains."""
    rows = []
    for year in YEARS:
        cur = conn.execute("""
            SELECT SUM(return_count) AS total_returns,
                   SUM(total_agi) AS total_agi,
                   SUM(total_income_tax) AS total_tax
            FROM raw_table_12
            WHERE year = ? AND filing_status = 'all'
        """, (year,))
        agg = dict(cur.fetchone())

        cur = conn.execute("""
            SELECT SUM(long_term_gain) AS total_lt_gain,
                   SUM(short_term_gain) AS total_st_gain,
                   SUM(total_gain) AS total_gain,
                   SUM(schedule_d_count) AS total_schedule_d
            FROM raw_table_14a
            WHERE year = ?
        """, (year,))
        cg = dict(cur.fetchone())

        rows.append({
            "year": year,
            "total_returns": agg["total_returns"],
            "total_agi": agg["total_agi"],
            "total_tax": agg["total_tax"],
            "total_lt_gain": cg["total_lt_gain"],
            "total_st_gain": cg["total_st_gain"],
            "total_gain": cg["total_gain"],
            "total_schedule_d": cg["total_schedule_d"],
        })
    return rows


def _query_capital_gains_by_bin(conn: sqlite3.Connection) -> list[dict]:
    """Per-bin capital gains with labels for all years."""
    cur = conn.execute("""
        SELECT cg.year, cg.agi_bin_id, b.label,
               cg.short_term_gain, cg.long_term_gain, cg.total_gain,
               cg.schedule_d_count
        FROM raw_table_14a cg
        JOIN agi_bins b ON cg.agi_bin_id = b.agi_bin_id
        ORDER BY cg.year, cg.agi_bin_id
    """)
    return [dict(r) for r in cur.fetchall()]


def _query_concentration(conn: sqlite3.Connection) -> list[dict]:
    """Cumulative % of LT gains from lowest bin upward, per year."""
    results = []
    for year in YEARS:
        cur = conn.execute("""
            SELECT cg.agi_bin_id, b.label, cg.long_term_gain
            FROM raw_table_14a cg
            JOIN agi_bins b ON cg.agi_bin_id = b.agi_bin_id
            WHERE cg.year = ?
            ORDER BY cg.agi_bin_id
        """, (year,))
        rows = [dict(r) for r in cur.fetchall()]

        total_lt = sum(r["long_term_gain"] or 0 for r in rows)
        cumulative = 0
        for r in rows:
            cumulative += (r["long_term_gain"] or 0)
            results.append({
                "year": year,
                "agi_bin_id": r["agi_bin_id"],
                "label": r["label"],
                "long_term_gain": r["long_term_gain"],
                "cumulative_lt_gain": cumulative,
                "cumulative_pct": round(cumulative / total_lt * 100, 2)
                    if total_lt else 0,
            })
    return results


def _query_returns_aggregate(conn: sqlite3.Connection) -> list[dict]:
    """Return count, AGI, tax by bin — all filers, all years."""
    cur = conn.execute("""
        SELECT t12.year, t12.agi_bin_id, b.label,
               t12.return_count, t12.total_agi,
               t12.total_taxable_income, t12.total_income_tax
        FROM raw_table_12 t12
        JOIN agi_bins b ON t12.agi_bin_id = b.agi_bin_id
        WHERE t12.filing_status = 'all'
        ORDER BY t12.year, t12.agi_bin_id
    """)
    return [dict(r) for r in cur.fetchall()]


def _query_income_sources(conn: sqlite3.Connection) -> list[dict]:
    """Per-bin income source breakdown for all years."""
    cur = conn.execute("""
        SELECT t14.year, t14.agi_bin_id, b.label,
               t14.wages, t14.taxable_interest, t14.ordinary_dividends,
               t14.qualified_dividends, t14.tax_exempt_interest,
               t14.business_income, t14.capital_gains,
               t14.partnership_scorp, t14.ira_pension,
               t14.social_security, t14.rental_royalty, t14.estate_trust
        FROM raw_table_14 t14
        JOIN agi_bins b ON t14.agi_bin_id = b.agi_bin_id
        ORDER BY t14.year, t14.agi_bin_id
    """)
    return [dict(r) for r in cur.fetchall()]


def _query_bracket_distribution(conn: sqlite3.Connection) -> list[dict]:
    """Tax by marginal rate bracket — all filers, all years."""
    cur = conn.execute("""
        SELECT year, marginal_rate, bracket_return_count,
               bracket_taxable_income, bracket_tax
        FROM raw_table_36
        WHERE filing_status = 'all'
        ORDER BY year, marginal_rate
    """)
    return [dict(r) for r in cur.fetchall()]


def _compute_reform_estimate(conn: sqlite3.Connection) -> list[dict]:
    """Full 5-step reform revenue estimate per (year, bin).

    Key insight: Under the reform, affected gross LT gains become ordinary
    income. The corresponding losses that previously netted against those
    gains are now STRANDED — capital losses can only offset other capital
    gains or $3K/year of ordinary income (IRC §1211(b)).

    This means the reform tax base is GROSS gains, not net. Losses that
    used to shelter gains at 15% are now nearly worthless, so the government
    collects marginal rates on income that was previously netted to zero.

    Pipeline:
      1. Cross-category loss netting (data-driven, for SOI reconciliation)
      2. Estimate gross LT gains and stranded losses
      3. Holding period filter (from assumptions)
      4. Marginal rate (bracket-based, return-count-weighted)
      5. Revenue: reform tax on gross gains minus current-law tax on net gains
    """
    assumptions = load_assumptions()
    results = []

    for year in YEARS:
        brackets = load_bracket_thresholds(year)

        cur = conn.execute("""
            SELECT cg.agi_bin_id, b.label,
                   cg.short_term_gain, cg.long_term_gain, cg.total_gain,
                   cg.schedule_d_count,
                   t12.total_income_tax, t12.total_agi
            FROM raw_table_14a cg
            JOIN agi_bins b ON cg.agi_bin_id = b.agi_bin_id
            JOIN raw_table_12 t12
              ON cg.year = t12.year AND cg.agi_bin_id = t12.agi_bin_id
            WHERE cg.year = ? AND t12.filing_status = 'all'
            ORDER BY cg.agi_bin_id
        """, (year,))
        rows = [dict(r) for r in cur.fetchall()]

        for r in rows:
            bin_id = r["agi_bin_id"]
            st_gain = r["short_term_gain"] or 0
            lt_gain = r["long_term_gain"] or 0
            total_gain = r["total_gain"] or 0
            agi = r["total_agi"] or 0
            tax = r["total_income_tax"] or 0
            schedule_d_count = r["schedule_d_count"] or 0

            # ── Step 1: Cross-category loss netting ──
            # SOI reports net ST and net LT gains. total_gain may be less
            # than st+lt due to cross-category netting.
            st_lt_sum = st_gain + lt_gain
            if st_lt_sum > 0 and total_gain < st_lt_sum:
                loss_offset = st_lt_sum - total_gain
                lt_share = lt_gain / st_lt_sum
                net_lt_gain = lt_gain - (loss_offset * lt_share)
            else:
                loss_offset = 0
                net_lt_gain = lt_gain

            net_lt_gain = max(net_lt_gain, 0)

            # ── Step 2: Estimate gross LT gains ──
            # SOI only reports net LT gains. Gross gains are larger because
            # gross losses have been netted out. The loss ratio tells us
            # what fraction of gross realizations are losses.
            gross_loss_ratio = get_gross_loss_ratio(assumptions, bin_id)
            if gross_loss_ratio < 1:
                # gross_gains = net / (1 - ratio)
                gross_lt_gains = net_lt_gain / (1 - gross_loss_ratio)
                estimated_lt_losses = gross_lt_gains - net_lt_gain
            else:
                gross_lt_gains = net_lt_gain
                estimated_lt_losses = 0

            # ── Step 3: Holding period filter ──
            hp_fraction = get_holding_period_fraction(assumptions, bin_id)
            affected_gross_gains = gross_lt_gains * hp_fraction
            affected_net_gains = net_lt_gain * hp_fraction
            affected_losses = estimated_lt_losses * hp_fraction

            # ── Step 4: Marginal rate ──
            weighted_marginal_rate = compute_weighted_marginal_rate(
                conn, year, bin_id, brackets
            )
            effective_rate = (tax / agi) if agi > 0 else 0

            # ── Step 5: Revenue calculation ──
            # Current law: affected net gains taxed at preferential rate
            current_law_tax = affected_net_gains * PREFERENTIAL_CG_RATE

            # Reform: affected GROSS gains become ordinary income.
            # Stranded losses can only offset $3K/year of ordinary income.
            # Approximate filers with stranded losses as fraction of Sched D filers.
            max_loss_deduction = ANNUAL_LOSS_DEDUCTION_LIMIT * schedule_d_count * hp_fraction
            usable_loss_offset = min(max_loss_deduction, affected_losses)
            reform_taxable = affected_gross_gains - usable_loss_offset
            reform_tax = reform_taxable * weighted_marginal_rate

            net_additional_revenue = max(reform_tax - current_law_tax, 0)

            # Decompose: rate-differential component + stranded-loss component
            rate_diff_revenue = affected_net_gains * max(weighted_marginal_rate - PREFERENTIAL_CG_RATE, 0)
            stranded_loss_revenue = (affected_losses - usable_loss_offset) * weighted_marginal_rate

            results.append({
                "year": year,
                "agi_bin_id": bin_id,
                "label": r["label"],
                # Raw data
                "long_term_gain": lt_gain,
                "short_term_gain": st_gain,
                "total_gain": total_gain,
                "schedule_d_count": schedule_d_count,
                # Step 1: cross-category netting
                "loss_offset": round(loss_offset),
                "net_lt_gain": round(net_lt_gain),
                # Step 2: gross gain estimation
                "gross_loss_ratio": gross_loss_ratio,
                "gross_lt_gains": round(gross_lt_gains),
                "estimated_lt_losses": round(estimated_lt_losses),
                # Step 3: holding period
                "holding_period_fraction": hp_fraction,
                "affected_gross_gains": round(affected_gross_gains),
                "affected_net_gains": round(affected_net_gains),
                "affected_losses": round(affected_losses),
                # Step 4: rates
                "weighted_marginal_rate": round(weighted_marginal_rate, 6),
                "effective_rate": round(effective_rate, 6),
                "preferential_rate": PREFERENTIAL_CG_RATE,
                # Step 5: revenue
                "current_law_tax": round(current_law_tax),
                "usable_loss_offset": round(usable_loss_offset),
                "reform_tax": round(reform_tax),
                "rate_diff_revenue": round(rate_diff_revenue),
                "stranded_loss_revenue": round(stranded_loss_revenue),
                "net_additional_revenue": round(net_additional_revenue),
                # Dashboard compatibility
                "additional_revenue": round(net_additional_revenue),
                "gross_additional_revenue": round(rate_diff_revenue),
            })
    return results


def _query_reform_by_filing_status(
    conn: sqlite3.Connection,
    reform_estimate: list[dict],
) -> list[dict]:
    """Break down reform revenue by filing status using AGI shares."""
    results = []
    for year in YEARS:
        year_rows = [r for r in reform_estimate if r["year"] == year]
        fs_rows = compute_reform_by_filing_status(conn, year, year_rows)
        results.extend(fs_rows)
    return results


def _build_reform_metadata(assumptions: dict) -> dict:
    """Build metadata dict describing the reform model assumptions."""
    return {
        "model_version": "v2.1",
        "preferential_cg_rate": PREFERENTIAL_CG_RATE,
        "annual_loss_deduction_limit": ANNUAL_LOSS_DEDUCTION_LIMIT,
        "holding_period_target": "LT gains held 1-5 years (vs 5+)",
        "holding_period_fractions": assumptions["holding_period_lt_under_5yr"]["by_agi_group"],
        "gross_loss_ratios": assumptions["gross_loss_ratio"]["by_agi_group"],
        "methodology": (
            "5-step pipeline: (1) cross-category loss netting, "
            "(2) gross gain estimation from loss ratios, "
            "(3) holding period filter, "
            "(4) bracket-based marginal rate, "
            "(5) revenue = reform tax on gross gains - current law tax on net gains. "
            "Under reform, gross gains become ordinary income; corresponding losses "
            "are stranded (only $3K/yr deductible against ordinary income per IRC 1211(b))."
        ),
        "note": (
            "Holding period fractions and gross loss ratios are calibration "
            "placeholders based on IRS SOI Capital Assets Study and JCT estimates. "
            "This is a static estimate; actual revenue depends on behavioral responses."
        ),
    }


def export_dashboard_json(db_path=None):
    """Main export: query all data and write dashboard.json."""
    conn = get_connection(db_path)
    try:
        reform_estimate = _compute_reform_estimate(conn)
        reform_by_fs = _query_reform_by_filing_status(conn, reform_estimate)
        assumptions = load_assumptions()

        data = {
            "summary": _query_summary(conn),
            "capital_gains_by_bin": _query_capital_gains_by_bin(conn),
            "concentration": _query_concentration(conn),
            "returns_aggregate": _query_returns_aggregate(conn),
            "income_sources": _query_income_sources(conn),
            "bracket_distribution": _query_bracket_distribution(conn),
            "reform_estimate": reform_estimate,
            "reform_by_filing_status": reform_by_fs,
            "reform_metadata": _build_reform_metadata(assumptions),
            "forecast_years": [2023],
        }
    finally:
        conn.close()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")
    logger.info(f"Wrote {OUTPUT_FILE} ({OUTPUT_FILE.stat().st_size:,} bytes)")
    return data


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    data = export_dashboard_json()
    # Print quick summary
    for s in data["summary"]:
        yr = s["year"]
        lt = s["total_lt_gain"] / 1e9
        gross_total = sum(
            r["gross_additional_revenue"]
            for r in data["reform_estimate"]
            if r["year"] == yr
        ) / 1e9
        net_total = sum(
            r["net_additional_revenue"]
            for r in data["reform_estimate"]
            if r["year"] == yr
        ) / 1e9
        print(f"TY{yr}: LT gains ${lt:,.1f}B | "
              f"Gross reform ${gross_total:,.1f}B | Net reform ${net_total:,.1f}B")
