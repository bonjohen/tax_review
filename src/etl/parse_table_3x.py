"""Parse IRS SOI Tables 3.4 and 3.6 into BRACKET_DISTRIBUTION records.

Table 3.4 (15 cols, 100 rows): Tax by marginal rate and filing status.
  Section-based layout — filing status appears as section headers:
    rows 8-24:  All returns
    rows 25-41: Married filing jointly
    rows 42-58: Married filing separately
    rows 59-75: Head of household
    rows 76-92: Single
  Within each section:
    col 0 = rate label (e.g., "10 percent", "0 percent (capital gains)")
    col 1 = Number of returns
    col 2 = AGI (thousands)
    col 3 = Modified taxable income at all rates (thousands)
    col 4 = Modified taxable income at marginal rate (thousands)
    col 5 = Tax generated at all rates (thousands)
    col 6 = Tax generated at marginal rate (thousands)

Table 3.6 (16 cols, 27 rows): Taxable income and tax by rate and filing status.
  Row 9 = first data row. Compact: one section, all filing statuses as columns.
  Rate labels in col 0 (with leading spaces).
  Filing-status column groups (each 3 cols: count, income_taxed, tax_generated):
    All=1, MFJ=4, MFS=7, HoH=10, Single=13
"""

import logging
import re
import sqlite3
from pathlib import Path

import pandas as pd
import xlrd

from .db import insert_rows
from .parse_table_1x import _clean_cell, _money

logger = logging.getLogger(__name__)

# Standard marginal tax rates for 2018-2022 (TCJA rates)
MARGINAL_RATES = [0.10, 0.12, 0.22, 0.24, 0.32, 0.35, 0.37]

# Filing status labels as they appear in IRS tables
FILING_STATUS_LABELS = {
    "all returns": "all",
    "married filing jointly": "married_filing_jointly",
    "married persons filing jointly": "married_filing_jointly",
    "surviving spouses": "married_filing_jointly",
    "married filing separately": "married_filing_separately",
    "married persons filing separately": "married_filing_separately",
    "single": "single",
    "single persons": "single",
    "head of household": "head_of_household",
    "heads of households": "head_of_household",
}

# Map rate labels to numeric rates
_RATE_MAP = {
    "0 percent": 0.0,
    "0 percent (capital gains)": None,   # Skip capital gains rates
    "10 percent": 0.10,
    "10 percent (form 8814)": None,      # Skip Form 8814
    "12 percent": 0.12,
    "15 percent (capital gains)": None,
    "20 percent (capital gains)": None,
    "22 percent": 0.22,
    "24 percent": 0.24,
    "25 percent (capital gains)": None,
    "28 percent (capital gains)": None,
    "32 percent": 0.32,
    "35 percent": 0.35,
    "37 percent": 0.37,
    "form 8615": None,                   # Skip kiddie tax
}


def _parse_rate(text: str) -> float | None:
    """Parse a marginal rate label to a float, or None to skip."""
    if not isinstance(text, str):
        return None
    key = text.strip().lower().rstrip()
    if key in _RATE_MAP:
        return _RATE_MAP[key]
    # Try numeric extraction as fallback
    match = re.search(r"(\d+(?:\.\d+)?)\s*(?:percent|%)", text, re.I)
    if match:
        return float(match.group(1)) / 100
    return None


def _detect_filing_status(text: str) -> str | None:
    """Check if a row label indicates a filing status section header."""
    text_lower = text.strip().lower()
    for label, code in FILING_STATUS_LABELS.items():
        if label in text_lower:
            return code
    return None


def parse_table_34(filepath: Path, year: int) -> pd.DataFrame:
    """Parse Table 3.4 — Tax by marginal rate and filing status.

    Section-based: filing status changes as we encounter section headers.
    """
    wb = xlrd.open_workbook(str(filepath))
    sh = wb.sheet_by_index(0)

    rows = []
    current_fs = "all"  # Default to "all returns" for first section

    for r in range(8, sh.nrows):
        label = str(sh.cell_value(r, 0)).strip()
        if not label:
            continue
        if label.startswith("[") or label.startswith("*") or "NOTE:" in label or "SOURCE:" in label:
            break

        # Check for filing status section header
        fs = _detect_filing_status(label)
        if fs is not None:
            current_fs = fs
            continue

        # Skip "All tax rates" totals
        if "all tax rates" in label.lower():
            continue

        # Parse rate
        rate = _parse_rate(label)
        if rate is None:
            continue  # Skip capital gains rates, Form 8814/8615, etc.

        row = {
            "year": year,
            "filing_status": current_fs,
            "marginal_rate": rate,
            "bracket_return_count": _clean_cell(sh.cell_value(r, 1)),
            "bracket_taxable_income": _money(sh.cell_value(r, 3)),
            "bracket_tax": _money(sh.cell_value(r, 5)),
        }
        rows.append(row)

    logger.info(f"Table 3.4: parsed {len(rows)} rows from {filepath.name}")
    return pd.DataFrame(rows)


def parse_table_36(filepath: Path, year: int) -> pd.DataFrame:
    """Parse Table 3.6 — Taxable income and tax by rate and filing status.

    Compact layout: all filing statuses as column groups.
    """
    wb = xlrd.open_workbook(str(filepath))
    sh = wb.sheet_by_index(0)

    filing_groups = [
        ("all",                     1),
        ("married_filing_jointly",  4),
        ("married_filing_separately", 7),
        ("head_of_household",       10),
        ("single",                  13),
    ]

    rows = []
    for r in range(9, sh.nrows):
        label = str(sh.cell_value(r, 0)).strip()
        if not label:
            continue
        if label.startswith("[") or "NOTE:" in label or "SOURCE:" in label:
            break

        # Skip "All tax rates" total row
        if "all tax rates" in label.lower():
            continue

        rate = _parse_rate(label)
        if rate is None:
            continue

        for fs_label, base_col in filing_groups:
            row = {
                "year": year,
                "filing_status": fs_label,
                "marginal_rate": rate,
                "bracket_return_count": _clean_cell(sh.cell_value(r, base_col)),
                "bracket_taxable_income": _money(sh.cell_value(r, base_col + 1)),
                "bracket_tax": _money(sh.cell_value(r, base_col + 2)),
            }
            rows.append(row)

    logger.info(f"Table 3.6: parsed {len(rows)} rows from {filepath.name}")
    return pd.DataFrame(rows)


def build_bracket_distribution(raw_dir: Path, year: int) -> pd.DataFrame:
    """Build BRACKET_DISTRIBUTION from Tables 3.4 and 3.6.

    Uses Table 3.6 as primary source (has taxable income, tax, and count
    by rate and filing status in a single compact table). Table 3.4 is
    available as supplementary data.
    """
    prefix = str(year)[2:]

    df = parse_table_36(raw_dir / f"{prefix}in36tr.xls", year)

    canonical = [
        "year", "filing_status", "marginal_rate",
        "bracket_taxable_income", "bracket_tax", "bracket_return_count",
    ]
    for col in canonical:
        if col not in df.columns:
            df[col] = None

    logger.info(f"BRACKET_DISTRIBUTION TY{year}: {len(df)} rows")
    return df[canonical]


# --- SQLite load functions ------------------------------------------------

def load_table_34(conn: sqlite3.Connection, filepath: Path, year: int) -> int:
    """Parse Table 3.4 and insert into raw_table_34."""
    df = parse_table_34(filepath, year)
    rows = df.to_dict("records")
    return insert_rows(conn, "raw_table_34", rows)


def load_table_36(conn: sqlite3.Connection, filepath: Path, year: int) -> int:
    """Parse Table 3.6 and insert into raw_table_36."""
    df = parse_table_36(filepath, year)
    rows = df.to_dict("records")
    return insert_rows(conn, "raw_table_36", rows)
