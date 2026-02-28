"""Parse IRS SOI Tables 3.2-3.6.

Tables 3.2-3.3: AGI-binned tax computation data → feeds RETURNS_AGGREGATE
Tables 3.4-3.6: Bracket distribution data → feeds BRACKET_DISTRIBUTION

Tables 3.4-3.6 are structurally different from Tables 1.x: rows represent
marginal tax rate brackets (10%, 12%, 22%, 24%, 32%, 35%, 37%) rather than
AGI bins. Filing status appears as section headers within the table.
"""

import logging
import re
from pathlib import Path

import pandas as pd
import xlrd

from .agi_bins import match_agi_bin
from .parse_table_1x import _clean_cell, _find_header_row, _find_data_start

logger = logging.getLogger(__name__)

# Standard marginal tax rates for 2020-2022
MARGINAL_RATES = [0.10, 0.12, 0.22, 0.24, 0.32, 0.35, 0.37]

# Filing status labels as they appear in IRS tables
FILING_STATUS_LABELS = {
    "all returns": "all",
    "married filing jointly": "married_filing_jointly",
    "married filing separately": "married_filing_separately",
    "single": "single",
    "head of household": "head_of_household",
    "qualifying widow": "qualifying_widow",
}


def _parse_rate(text: str) -> float | None:
    """Parse a marginal rate label like '10 percent' or '10%' to 0.10."""
    if not isinstance(text, str):
        return None
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


def parse_table_32(filepath: Path, year: int) -> pd.DataFrame:
    """Parse Table 3.2 — Total Income Tax as Percentage of AGI.

    Returns rows with: year, agi_bin_id, total_agi, total_income_tax,
    effective_tax_rate.
    """
    workbook = xlrd.open_workbook(str(filepath))
    sheet = workbook.sheet_by_index(0)

    header_row = _find_header_row(sheet)
    data_start = _find_data_start(sheet, after_row=(header_row or 0) + 1)

    # TODO: Map columns from actual file inspection
    rows = []
    if data_start is None:
        return pd.DataFrame(rows)

    for row_idx in range(data_start, sheet.nrows):
        agi_text = str(sheet.cell_value(row_idx, 0)).strip()
        if not agi_text or "footnote" in agi_text.lower():
            break

        bin_id = match_agi_bin(agi_text)
        if bin_id is None:
            continue

        row = {
            "year": year,
            "agi_bin_id": bin_id,
        }
        # Column indices TBD from actual file
        rows.append(row)

    return pd.DataFrame(rows)


def parse_table_33(filepath: Path, year: int) -> pd.DataFrame:
    """Parse Table 3.3 — Tax Liability and Credits by AGI.

    Returns rows with: year, agi_bin_id, total_income_tax, total_credits,
    return_count.
    """
    # TODO: Implement after inspecting actual file layout
    return pd.DataFrame()


def parse_bracket_tables(
    filepath_34: Path,
    filepath_35: Path,
    filepath_36: Path,
    year: int,
) -> pd.DataFrame:
    """Parse Tables 3.4-3.6 into BRACKET_DISTRIBUTION schema.

    These tables organize data by marginal tax rate rather than AGI bin.
    Filing status appears as section headers within each table.

    Returns DataFrame with columns:
        year, filing_status, marginal_rate, bracket_taxable_income,
        bracket_tax, bracket_return_count
    """
    # TODO: Implement after inspecting actual file layouts.
    # Key challenges:
    # - Row labels are tax rates (10%, 12%, etc.), not AGI bins
    # - Filing status encoded as section headers within the worksheet
    # - Must detect section boundaries and tag each rate row
    raise NotImplementedError("Requires inspection of actual IRS Excel file layouts")
