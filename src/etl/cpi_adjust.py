"""CPI-U adjustment to 2022 constant dollars.

CPI-U annual averages from BLS (All Items, 1982-84=100 base).
Source: Bureau of Labor Statistics, Series CUUR0000SA0
https://www.bls.gov/cpi/data.htm
"""

import pandas as pd

# Published final CPI-U annual averages (will not change for these years)
CPI_U_ANNUAL = {
    2018: 251.107,
    2019: 255.657,
    2020: 258.811,
    2021: 270.970,
    2022: 292.655,
}

TARGET_YEAR = 2022

# Monetary columns per canonical table
MONEY_COLUMNS = {
    "returns_aggregate": [
        "total_agi",
        "total_taxable_income",
        "total_income_tax",
        "total_credits",
    ],
    "capital_gains": [
        "short_term_gain",
        "long_term_gain",
        "total_gain",
    ],
    "bracket_distribution": [
        "bracket_taxable_income",
        "bracket_tax",
    ],
}


def get_cpi_rows() -> list[dict]:
    """Return CPI factor rows suitable for SQLite insertion."""
    target_cpi = CPI_U_ANNUAL[TARGET_YEAR]
    return [
        {
            "year": year,
            "cpi_u_annual": cpi,
            "factor_to_2022": target_cpi / cpi,
        }
        for year, cpi in CPI_U_ANNUAL.items()
    ]


def get_adjustment_factor(year: int) -> float:
    """Return multiplier to convert a year's dollars to 2022 dollars."""
    if year not in CPI_U_ANNUAL:
        raise ValueError(f"No CPI-U data for year {year}. Available: {list(CPI_U_ANNUAL.keys())}")
    return CPI_U_ANNUAL[TARGET_YEAR] / CPI_U_ANNUAL[year]


def adjust_dataframe(
    df: pd.DataFrame,
    money_columns: list[str],
    year_col: str = "year",
) -> pd.DataFrame:
    """Return a copy of df with money columns adjusted to 2022 constant dollars.

    Non-monetary columns (counts, rates, IDs) are left unchanged.
    """
    result = df.copy()
    for col in money_columns:
        if col not in result.columns:
            continue
        result[col] = result.apply(
            lambda row, c=col: (
                row[c] * get_adjustment_factor(int(row[year_col]))
                if pd.notna(row[c])
                else None
            ),
            axis=1,
        )
    return result
