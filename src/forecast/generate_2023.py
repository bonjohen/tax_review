"""Generate TY2023 forecast data by trend-projecting 2018-2022 actuals.

Reads historical data from SQLite, fits a linear trend per (bin, filing_status)
combination, projects 2023 values, and inserts them as synthetic rows.

The dashboard marks 2023 data as forecast; actual IRS SOI Publication 1304
for TY2023 is expected March 2026.

Usage:
    python -m src.forecast.generate_2023
"""

import logging
import numpy as np

from src.etl.db import get_connection, init_schema, reset_year, insert_rows, load_cpi_factors

logger = logging.getLogger(__name__)

HISTORICAL_YEARS = [2018, 2019, 2020, 2021, 2022]
FORECAST_YEAR = 2023
YEARS_ARRAY = np.array(HISTORICAL_YEARS, dtype=float)


def _trend_project(values: list[float]) -> float:
    """Fit a linear trend to historical values and project one year forward.

    Returns the projected value, floored at zero for non-negative fields.
    """
    y = np.array(values, dtype=float)
    # Replace None/NaN with 0
    y = np.nan_to_num(y, nan=0.0)

    if np.all(y == 0):
        return 0.0

    # Simple linear regression: y = slope * x + intercept
    coeffs = np.polyfit(YEARS_ARRAY, y, 1)
    projected = np.polyval(coeffs, FORECAST_YEAR)

    # Floor at zero (no negative counts or dollar amounts)
    return max(projected, 0.0)


def _project_table_12(conn) -> list[dict]:
    """Project raw_table_12 rows for 2023.

    Table 1.2 has 19 AGI bins x 5 filing statuses = 95 rows per year.
    """
    filing_statuses = ['all', 'single', 'married_filing_jointly',
                       'married_filing_separately', 'head_of_household']
    value_columns = ['return_count', 'total_agi', 'total_taxable_income',
                     'total_income_tax']

    rows = []
    for fs in filing_statuses:
        for bin_id in range(1, 20):
            cur = conn.execute("""
                SELECT year, return_count, total_agi,
                       total_taxable_income, total_income_tax
                FROM raw_table_12
                WHERE agi_bin_id = ? AND filing_status = ?
                ORDER BY year
            """, (bin_id, fs))
            historical = {r['year']: dict(r) for r in cur.fetchall()}

            row = {
                'year': FORECAST_YEAR,
                'agi_bin_id': bin_id,
                'filing_status': fs,
            }
            for col in value_columns:
                values = [historical.get(y, {}).get(col, 0) or 0
                          for y in HISTORICAL_YEARS]
                row[col] = round(_trend_project(values))

            rows.append(row)

    return rows


def _project_table_14a(conn) -> list[dict]:
    """Project raw_table_14a rows for 2023.

    Table 1.4A has 19 AGI bins = 19 rows per year.
    """
    value_columns = ['schedule_d_count', 'short_term_gain',
                     'long_term_gain', 'total_gain']

    rows = []
    for bin_id in range(1, 20):
        cur = conn.execute("""
            SELECT year, schedule_d_count, short_term_gain,
                   long_term_gain, total_gain
            FROM raw_table_14a
            WHERE agi_bin_id = ?
            ORDER BY year
        """, (bin_id,))
        historical = {r['year']: dict(r) for r in cur.fetchall()}

        row = {
            'year': FORECAST_YEAR,
            'agi_bin_id': bin_id,
        }
        for col in value_columns:
            values = [historical.get(y, {}).get(col, 0) or 0
                      for y in HISTORICAL_YEARS]
            row[col] = round(_trend_project(values))

        rows.append(row)

    return rows


def _project_table_14(conn) -> list[dict]:
    """Project raw_table_14 rows for 2023.

    Table 1.4 has 19 AGI bins = 19 rows per year.
    """
    value_columns = [
        'wages', 'taxable_interest', 'ordinary_dividends',
        'qualified_dividends', 'tax_exempt_interest', 'business_income',
        'capital_gains', 'partnership_scorp', 'ira_pension',
        'social_security', 'rental_royalty', 'estate_trust',
    ]

    rows = []
    for bin_id in range(1, 20):
        cur = conn.execute("""
            SELECT year, wages, taxable_interest, ordinary_dividends,
                   qualified_dividends, tax_exempt_interest, business_income,
                   capital_gains, partnership_scorp, ira_pension,
                   social_security, rental_royalty, estate_trust
            FROM raw_table_14
            WHERE agi_bin_id = ?
            ORDER BY year
        """, (bin_id,))
        historical = {r['year']: dict(r) for r in cur.fetchall()}

        row = {
            'year': FORECAST_YEAR,
            'agi_bin_id': bin_id,
        }
        for col in value_columns:
            values = [historical.get(y, {}).get(col, 0) or 0
                      for y in HISTORICAL_YEARS]
            row[col] = round(_trend_project(values))

        rows.append(row)

    return rows


def _project_table_36(conn) -> list[dict]:
    """Project raw_table_36 rows for 2023.

    Table 3.6 has 7 brackets x 5 filing statuses = 35 rows per year.
    """
    filing_statuses = ['all', 'single', 'married_filing_jointly',
                       'married_filing_separately', 'head_of_household']
    value_columns = ['bracket_return_count', 'bracket_taxable_income',
                     'bracket_tax']

    # Get distinct marginal rates from existing data
    cur = conn.execute("""
        SELECT DISTINCT marginal_rate FROM raw_table_36 ORDER BY marginal_rate
    """)
    marginal_rates = [r['marginal_rate'] for r in cur.fetchall()]

    rows = []
    for fs in filing_statuses:
        for rate in marginal_rates:
            cur = conn.execute("""
                SELECT year, bracket_return_count, bracket_taxable_income,
                       bracket_tax
                FROM raw_table_36
                WHERE filing_status = ? AND marginal_rate = ?
                ORDER BY year
            """, (fs, rate))
            historical = {r['year']: dict(r) for r in cur.fetchall()}

            row = {
                'year': FORECAST_YEAR,
                'filing_status': fs,
                'marginal_rate': rate,
            }
            for col in value_columns:
                values = [historical.get(y, {}).get(col, 0) or 0
                          for y in HISTORICAL_YEARS]
                row[col] = round(_trend_project(values))

            rows.append(row)

    return rows


def generate_forecast(db_path=None):
    """Main entry point: project 2023 data and insert into SQLite."""
    conn = get_connection(db_path)
    try:
        init_schema(conn)

        # Clear any existing 2023 data
        reset_year(conn, FORECAST_YEAR)

        # Project each table
        t12_rows = _project_table_12(conn)
        logger.info(f"Projected {len(t12_rows)} raw_table_12 rows")

        t14a_rows = _project_table_14a(conn)
        logger.info(f"Projected {len(t14a_rows)} raw_table_14a rows")

        t14_rows = _project_table_14(conn)
        logger.info(f"Projected {len(t14_rows)} raw_table_14 rows")

        t36_rows = _project_table_36(conn)
        logger.info(f"Projected {len(t36_rows)} raw_table_36 rows")

        # Insert into database
        insert_rows(conn, 'raw_table_12', t12_rows)
        insert_rows(conn, 'raw_table_14a', t14a_rows)
        insert_rows(conn, 'raw_table_14', t14_rows)
        insert_rows(conn, 'raw_table_36', t36_rows)
        conn.commit()

        # Refresh CPI factors to include 2023
        load_cpi_factors(conn)

        total = len(t12_rows) + len(t14a_rows) + len(t14_rows) + len(t36_rows)
        logger.info(f"Inserted {total} forecast rows for TY{FORECAST_YEAR}")
        return total
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    total = generate_forecast()
    print(f"TY{FORECAST_YEAR} forecast: {total} rows inserted")
