"""Bracket lookup and return-count-weighted marginal rate computation."""

import json
import sqlite3
from pathlib import Path

PARAMS_DIR = Path(__file__).resolve().parents[2] / "data" / "parameters"

FILING_STATUSES = [
    "single",
    "married_filing_jointly",
    "married_filing_separately",
    "head_of_household",
]


def load_bracket_thresholds(year: int) -> dict:
    """Load ordinary income bracket schedule from {year}_tax_parameters.json.

    Returns dict mapping filing_status -> list of {rate, threshold, upper}.
    """
    path = PARAMS_DIR / f"{year}_tax_parameters.json"
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return data["ordinary_income_brackets"]


def marginal_rate_for_income(brackets: list[dict], taxable_income: float) -> float:
    """Walk the bracket schedule to find the marginal rate at a given income.

    Args:
        brackets: list of {rate, threshold, upper} sorted by threshold ascending
        taxable_income: the income level to evaluate

    Returns:
        The marginal rate (e.g. 0.24) for the bracket containing taxable_income.
    """
    if taxable_income <= 0:
        return 0.10  # lowest bracket rate

    rate = brackets[0]["rate"]
    for bracket in brackets:
        if taxable_income >= bracket["threshold"]:
            rate = bracket["rate"]
        else:
            break
    return rate


def compute_weighted_marginal_rate(
    conn: sqlite3.Connection,
    year: int,
    agi_bin_id: int,
    brackets_by_status: dict,
) -> float:
    """Compute return-count-weighted average marginal rate for a bin.

    For each filing status, finds average taxable income from Table 1.2,
    looks up the marginal rate, then weights by return count.
    """
    total_weighted_rate = 0.0
    total_returns = 0.0

    for fs in FILING_STATUSES:
        cur = conn.execute("""
            SELECT return_count, total_taxable_income
            FROM raw_table_12
            WHERE year = ? AND agi_bin_id = ? AND filing_status = ?
        """, (year, agi_bin_id, fs))
        row = cur.fetchone()
        if row is None:
            continue

        return_count = row["return_count"] or 0
        taxable_income = row["total_taxable_income"] or 0

        if return_count <= 0:
            continue

        avg_taxable = taxable_income / return_count
        brackets = brackets_by_status.get(fs, [])
        if not brackets:
            continue

        rate = marginal_rate_for_income(brackets, avg_taxable)
        total_weighted_rate += rate * return_count
        total_returns += return_count

    if total_returns <= 0:
        return 0.0

    return total_weighted_rate / total_returns
