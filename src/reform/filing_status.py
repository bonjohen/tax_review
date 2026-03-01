"""AGI-share allocation of capital gains and reform revenue by filing status."""

import sqlite3

FILING_STATUSES = [
    "single",
    "married_filing_jointly",
    "married_filing_separately",
    "head_of_household",
]


def compute_agi_shares(
    conn: sqlite3.Connection,
    year: int,
    agi_bin_id: int,
) -> dict[str, float]:
    """Compute each filing status's share of AGI within a bin.

    Returns dict mapping filing_status -> fraction (0-1). Shares sum to ~1.0.
    """
    shares = {}
    total_agi = 0.0

    for fs in FILING_STATUSES:
        cur = conn.execute("""
            SELECT total_agi
            FROM raw_table_12
            WHERE year = ? AND agi_bin_id = ? AND filing_status = ?
        """, (year, agi_bin_id, fs))
        row = cur.fetchone()
        agi = abs(row["total_agi"]) if row and row["total_agi"] else 0.0
        shares[fs] = agi
        total_agi += agi

    if total_agi > 0:
        for fs in shares:
            shares[fs] /= total_agi
    else:
        # Equal split fallback
        for fs in shares:
            shares[fs] = 1.0 / len(FILING_STATUSES)

    return shares


def allocate_gains_by_filing_status(
    total_amount: float,
    agi_shares: dict[str, float],
) -> dict[str, float]:
    """Allocate a total amount proportionally by AGI share."""
    return {fs: total_amount * share for fs, share in agi_shares.items()}


def compute_reform_by_filing_status(
    conn: sqlite3.Connection,
    year: int,
    reform_by_bin: list[dict],
) -> list[dict]:
    """Break down reform estimates by filing status using AGI shares.

    Args:
        conn: SQLite connection
        year: tax year
        reform_by_bin: list of dicts with at least
            {agi_bin_id, label, net_additional_revenue}

    Returns:
        list of dicts: {year, agi_bin_id, label, filing_status,
                        agi_share, allocated_revenue}
    """
    results = []
    for row in reform_by_bin:
        bin_id = row["agi_bin_id"]
        label = row["label"]
        net_rev = row["net_additional_revenue"]

        shares = compute_agi_shares(conn, year, bin_id)
        allocated = allocate_gains_by_filing_status(net_rev, shares)

        for fs in FILING_STATUSES:
            results.append({
                "year": year,
                "agi_bin_id": bin_id,
                "label": label,
                "filing_status": fs,
                "agi_share": round(shares[fs], 6),
                "allocated_revenue": round(allocated[fs]),
            })

    return results
