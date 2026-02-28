"""Parse IRS SOI Tables 1.1-1.4 into RETURNS_AGGREGATE records.

These tables contain income structure and AGI data. All use the old .xls (BIFF)
format and require xlrd as the engine. Money amounts are in thousands of dollars.

Table structures (approximate row/column positions, verified against actual files):
- Table 1.1: Selected Income and Tax Items by AGI (all filing statuses combined)
- Table 1.2: Income and Tax Items by Filing Status
- Table 1.3: Detailed Income and Tax Items by Filing Status
- Table 1.4: Sources of Income by Size of AGI
"""

import logging
import re
from pathlib import Path

import pandas as pd
import xlrd

from .agi_bins import match_agi_bin

logger = logging.getLogger(__name__)


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


def _find_header_row(sheet: xlrd.sheet.Sheet, max_scan: int = 15) -> int | None:
    """Find the row containing column headers (e.g., 'Number of returns')."""
    for row_idx in range(min(max_scan, sheet.nrows)):
        row_text = " ".join(
            str(sheet.cell_value(row_idx, c)) for c in range(sheet.ncols)
        ).lower()
        if "number of returns" in row_text or "adjusted gross income" in row_text:
            return row_idx
    return None


def _find_data_start(sheet: xlrd.sheet.Sheet, after_row: int = 0) -> int | None:
    """Find the first data row containing an AGI bin label or 'All returns'."""
    for row_idx in range(after_row, min(after_row + 10, sheet.nrows)):
        cell = str(sheet.cell_value(row_idx, 0)).strip()
        if cell and ("$" in cell or "all returns" in cell.lower() or "no adjusted" in cell.lower()):
            return row_idx
    return None


def parse_table(filepath: Path, year: int, table_id: str) -> pd.DataFrame:
    """Parse a single IRS Table 1.x file into structured rows.

    Returns a DataFrame with columns appropriate for the specific table.
    All monetary values are converted from thousands to actual dollars.
    """
    workbook = xlrd.open_workbook(str(filepath))
    sheet = workbook.sheet_by_index(0)

    header_row = _find_header_row(sheet)
    if header_row is None:
        raise ValueError(f"Could not find header row in {filepath}")

    data_start = _find_data_start(sheet, after_row=header_row + 1)
    if data_start is None:
        raise ValueError(f"Could not find data start in {filepath}")

    logger.info(
        f"Parsing {filepath.name}: header_row={header_row}, data_start={data_start}, "
        f"nrows={sheet.nrows}, ncols={sheet.ncols}"
    )

    # Build column index from header text
    header_text = []
    for col in range(sheet.ncols):
        parts = []
        # Check header row and one row below for multi-row headers
        for r in range(header_row, min(header_row + 2, sheet.nrows)):
            val = str(sheet.cell_value(r, col)).strip()
            if val:
                parts.append(val)
        header_text.append(" ".join(parts).lower())

    # Parse data rows
    rows = []
    for row_idx in range(data_start, sheet.nrows):
        agi_text = str(sheet.cell_value(row_idx, 0)).strip()
        if not agi_text:
            continue
        # Stop at footnotes
        if "footnote" in agi_text.lower() or agi_text.startswith("["):
            break

        bin_id = match_agi_bin(agi_text)
        # bin_id is None for aggregate rows; we still capture them with bin_id=None

        row_data = {"year": year, "agi_bin_id": bin_id, "agi_label": agi_text}
        for col in range(1, sheet.ncols):
            raw = sheet.cell_value(row_idx, col)
            cleaned = _clean_cell(raw)
            if cleaned is not None:
                # Multiply money amounts by 1000 (IRS reports in thousands)
                cleaned *= 1_000
            col_name = header_text[col] if col < len(header_text) else f"col_{col}"
            row_data[col_name] = cleaned

        rows.append(row_data)

    return pd.DataFrame(rows)


def build_returns_aggregate(raw_dir: Path, year: int) -> pd.DataFrame:
    """Combine Tables 1.1-1.4 and 3.2-3.3 into RETURNS_AGGREGATE.

    This is the main assembly function that merges data from multiple source
    tables into the canonical RETURNS_AGGREGATE schema.
    """
    # TODO: Implement full assembly logic once table column mappings are
    # determined from inspecting actual downloaded files.
    # The approach:
    # 1. Parse Table 1.1 (all filing statuses combined)
    # 2. Parse Table 1.2 (by filing status)
    # 3. Merge effective_tax_rate from Table 3.2
    # 4. Merge credits from Table 3.3
    raise NotImplementedError("Requires inspection of actual IRS Excel file layouts")
