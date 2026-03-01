"""Parse IRS SOI Tables 1.1, 1.2, 3.2, 3.3 into RETURNS_AGGREGATE records.

Verified column layouts (consistent across Tax Years 2020-2022):

Table 1.1 (21 cols, row 9 = first data row):
  col 0  = AGI label
  col 1  = Number of returns
  col 3  = AGI (amount, thousands)
  col 11 = Taxable income (amount, thousands)
  col 14 = Income tax after credits (amount, thousands)
  col 16 = Total income tax (amount, thousands)

Table 1.2 (63 cols, row 8 = first data row):
  Filing-status column groups (each 12 cols wide):
    All=1, MFJ=13, MFS=25, HoH=37, Single=49
  Within each group (offset from start):
    +0 = Number of returns
    +1 = AGI (amount, thousands)
    +6 = Taxable income: count     +7 = Taxable income (amount, thousands)
    +8 = Income tax after credits: count   +9 = Amount (thousands)
    +10 = Total income tax: count   +11 = Total income tax (amount, thousands)

Table 3.2 (37 cols, row 9 = first data row, coarser bins):
  col 0 = AGI label
  col 1 = Number of returns
  col 2 = AGI (amount, thousands)
  col 3 = Total income tax (amount, thousands)

Table 3.3 (131 cols, row 9 = first data row):
  col 0  = AGI label
  col 1  = Number of returns
  col 3  = Total credits (amount, thousands)
  col 119 = Total income tax minus refundable credits (amount, thousands)
"""

import logging
import re
import sqlite3
from pathlib import Path

import pandas as pd
import xlrd

from .agi_bins import match_agi_bin
from .db import insert_rows

logger = logging.getLogger(__name__)

# Units multiplier: IRS reports money in thousands of dollars
_K = 1_000


def _clean_cell(value) -> float | None:
    """Convert a cell value to float, handling footnote markers, dashes, blanks.

    IRS Excel cells may contain:
    - Numeric values (int or float)
    - Strings with commas: "1,234,567"
    - Footnote markers: "[1]", "[2]", "*", "d"
    - Suppressed data: "--", "-", "†", "‡"
    - Empty strings or None
    """
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        if isinstance(value, float) and value != value:  # NaN check
            return None
        return float(value)

    text = str(value)
    # Remove footnote markers
    text = re.sub(r"\[\d+\]", "", text)
    text = re.sub(r"[*†‡]", "", text)
    text = text.strip()

    if text in ("--", "-", "d", "D", ""):
        return None

    text = text.replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def _money(value) -> float | None:
    """Clean a cell and convert from thousands to actual dollars."""
    v = _clean_cell(value)
    return v * _K if v is not None else None


def _parse_rows(sheet, data_start: int, col_map: dict, year: int,
                filing_status: str = "all") -> list[dict]:
    """Parse data rows from a sheet using a column mapping.

    col_map: {output_field_name: (col_index, is_money)}
    """
    rows = []
    seen_bins = set()  # Track seen bin_ids to skip accumulated-size duplicates
    for r in range(data_start, sheet.nrows):
        label = str(sheet.cell_value(r, 0)).strip()
        if not label:
            continue
        # Stop at footnotes
        if label.startswith("[") or label.startswith("*") or "footnote" in label.lower():
            break
        if "NOTE:" in label or "SOURCE:" in label:
            break

        bin_id = match_agi_bin(label)
        if bin_id is None:
            continue  # Skip aggregate/total/accumulated rows
        if bin_id in seen_bins:
            continue  # Skip duplicate from "Accumulated Size of AGI" section
        seen_bins.add(bin_id)

        row = {"year": year, "agi_bin_id": bin_id, "filing_status": filing_status}
        for field, (col, is_money) in col_map.items():
            raw = sheet.cell_value(r, col)
            row[field] = _money(raw) if is_money else _clean_cell(raw)
        rows.append(row)
    return rows


def parse_table_11(filepath: Path, year: int) -> pd.DataFrame:
    """Parse Table 1.1 — Selected Income and Tax Items.

    Returns rows with filing_status='all'.
    """
    wb = xlrd.open_workbook(str(filepath))
    sh = wb.sheet_by_index(0)
    col_map = {
        "return_count":       (1, False),
        "total_agi":          (3, True),
        "total_taxable_income": (11, True),
        "total_income_tax":   (16, True),
    }
    rows = _parse_rows(sh, data_start=9, col_map=col_map, year=year,
                       filing_status="all")
    logger.info(f"Table 1.1: parsed {len(rows)} rows from {filepath.name}")
    return pd.DataFrame(rows)


def parse_table_12(filepath: Path, year: int) -> pd.DataFrame:
    """Parse Table 1.2 — Income and Tax Items by Filing Status.

    Returns rows for each filing status.
    """
    wb = xlrd.open_workbook(str(filepath))
    sh = wb.sheet_by_index(0)

    # Filing status groups: (label, col_offset)
    filing_groups = [
        ("all",                     1),
        ("married_filing_jointly",  13),
        ("married_filing_separately", 25),
        ("head_of_household",       37),
        ("single",                  49),
    ]

    all_rows = []
    for fs_label, base_col in filing_groups:
        col_map = {
            "return_count":         (base_col + 0, False),
            "total_agi":            (base_col + 1, True),
            "total_taxable_income": (base_col + 7, True),
            "total_income_tax":     (base_col + 11, True),
        }
        rows = _parse_rows(sh, data_start=8, col_map=col_map, year=year,
                           filing_status=fs_label)
        all_rows.extend(rows)

    logger.info(f"Table 1.2: parsed {len(all_rows)} rows from {filepath.name}")
    return pd.DataFrame(all_rows)


def parse_table_32(filepath: Path, year: int) -> pd.DataFrame:
    """Parse Table 3.2 — Total Income Tax as Percentage of AGI.

    Note: Table 3.2 uses coarser AGI bins than Table 1.1. The top bin is
    "$200,000 or more" instead of finer breakdown. Rows that don't match
    canonical bins are skipped.
    """
    wb = xlrd.open_workbook(str(filepath))
    sh = wb.sheet_by_index(0)
    col_map = {
        "return_count":       (1, False),
        "total_agi":          (2, True),
        "total_income_tax":   (3, True),
    }
    rows = _parse_rows(sh, data_start=9, col_map=col_map, year=year,
                       filing_status="all")
    # Compute effective tax rate
    for row in rows:
        agi = row.get("total_agi")
        tax = row.get("total_income_tax")
        if agi and agi != 0 and tax is not None:
            row["effective_tax_rate"] = tax / agi
        else:
            row["effective_tax_rate"] = None

    logger.info(f"Table 3.2: parsed {len(rows)} rows from {filepath.name}")
    return pd.DataFrame(rows)


def parse_table_33(filepath: Path, year: int) -> pd.DataFrame:
    """Parse Table 3.3 — Tax Liability, Tax Credits, and Tax Payments."""
    wb = xlrd.open_workbook(str(filepath))
    sh = wb.sheet_by_index(0)

    # Find "Total income tax minus refundable credits" column dynamically —
    # its position varies by year (2018: 105, 2019: 101, 2020: 119, etc.)
    income_tax_col = None
    for c in range(sh.ncols):
        val = str(sh.cell_value(2, c)).lower()
        if "total income tax" in val and "minus" in val:
            income_tax_col = c + 1  # Amount column is one to the right
            break
    if income_tax_col is None:
        raise ValueError(f"Could not find 'Total income tax minus refundable credits' column in {filepath.name}")

    col_map = {
        "return_count":       (1, False),
        "total_credits":      (3, True),
        "total_income_tax":   (income_tax_col, True),
    }
    rows = _parse_rows(sh, data_start=9, col_map=col_map, year=year,
                       filing_status="all")
    logger.info(f"Table 3.3: parsed {len(rows)} rows from {filepath.name}")
    return pd.DataFrame(rows)


def build_returns_aggregate(raw_dir: Path, year: int) -> pd.DataFrame:
    """Combine Tables 1.1, 1.2, 3.2, 3.3 into RETURNS_AGGREGATE.

    Strategy:
    - Table 1.2 provides the primary data (all filing statuses)
    - Table 3.2 adds effective_tax_rate (coarser bins, merged where available)
    - Table 3.3 adds total_credits
    """
    prefix = str(year)[2:]

    # Primary source: Table 1.2 (has filing status breakdown)
    df = parse_table_12(raw_dir / f"{prefix}in12ms.xls", year)

    # Add total_credits from Table 3.3 (only for filing_status='all')
    df_33 = parse_table_33(raw_dir / f"{prefix}in33ar.xls", year)
    if not df_33.empty:
        credits = df_33[["agi_bin_id", "total_credits"]].drop_duplicates()
        df = df.merge(credits, on="agi_bin_id", how="left")
    else:
        df["total_credits"] = None

    # Add effective_tax_rate from Table 3.2 (only for filing_status='all')
    df_32 = parse_table_32(raw_dir / f"{prefix}in32tt.xls", year)
    if not df_32.empty:
        rates = df_32[["agi_bin_id", "effective_tax_rate"]].drop_duplicates()
        df = df.merge(rates, on="agi_bin_id", how="left")
    else:
        df["effective_tax_rate"] = None

    # Ensure schema columns exist
    for col in ["total_credits", "effective_tax_rate"]:
        if col not in df.columns:
            df[col] = None

    # Select and order canonical columns
    canonical = [
        "year", "agi_bin_id", "filing_status", "return_count",
        "total_agi", "total_taxable_income", "total_income_tax",
        "total_credits", "effective_tax_rate",
    ]
    for col in canonical:
        if col not in df.columns:
            df[col] = None

    logger.info(f"RETURNS_AGGREGATE TY{year}: {len(df)} rows")
    return df[canonical]


# --- SQLite load functions ------------------------------------------------

def load_table_11(conn: sqlite3.Connection, filepath: Path, year: int) -> int:
    """Parse Table 1.1 and insert into raw_table_11."""
    df = parse_table_11(filepath, year)
    rows = df.to_dict("records")
    return insert_rows(conn, "raw_table_11", rows)


def load_table_12(conn: sqlite3.Connection, filepath: Path, year: int) -> int:
    """Parse Table 1.2 and insert into raw_table_12."""
    df = parse_table_12(filepath, year)
    rows = df.to_dict("records")
    return insert_rows(conn, "raw_table_12", rows)


def load_table_32(conn: sqlite3.Connection, filepath: Path, year: int) -> int:
    """Parse Table 3.2 and insert into raw_table_32."""
    df = parse_table_32(filepath, year)
    rows = df.to_dict("records")
    return insert_rows(conn, "raw_table_32", rows)


def load_table_33(conn: sqlite3.Connection, filepath: Path, year: int) -> int:
    """Parse Table 3.3 and insert into raw_table_33."""
    df = parse_table_33(filepath, year)
    rows = df.to_dict("records")
    return insert_rows(conn, "raw_table_33", rows)
