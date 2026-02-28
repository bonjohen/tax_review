"""Parse IRS SOI Table 1.4A into CAPITAL_GAINS records.

Table 1.4A: Returns with Income or Loss from Sales of Capital Assets
(Form 1040, Schedule D). Organized by AGI bin with columns for short-term
and long-term capital gains, and Schedule D return counts.
"""

import logging
from pathlib import Path

import pandas as pd
import xlrd

from .agi_bins import match_agi_bin
from .parse_table_1x import _clean_cell, _find_header_row, _find_data_start

logger = logging.getLogger(__name__)


def parse_capital_gains(filepath: Path, year: int) -> pd.DataFrame:
    """Parse Table 1.4A into CAPITAL_GAINS schema.

    Returns DataFrame with columns:
        year, agi_bin_id, short_term_gain, long_term_gain,
        total_gain, schedule_d_count
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
        f"Parsing {filepath.name}: header_row={header_row}, "
        f"data_start={data_start}"
    )

    # TODO: Map specific columns once actual file layout is inspected.
    # Expected columns: Schedule D count, short-term gain, long-term gain,
    # total capital gain. Column indices will be determined from the header row.

    rows = []
    for row_idx in range(data_start, sheet.nrows):
        agi_text = str(sheet.cell_value(row_idx, 0)).strip()
        if not agi_text:
            continue
        if "footnote" in agi_text.lower() or agi_text.startswith("["):
            break

        bin_id = match_agi_bin(agi_text)
        if bin_id is None:
            continue

        row = {
            "year": year,
            "agi_bin_id": bin_id,
            "schedule_d_count": _clean_cell(sheet.cell_value(row_idx, 1)),
            "short_term_gain": _clean_cell(sheet.cell_value(row_idx, 3)),
            "long_term_gain": _clean_cell(sheet.cell_value(row_idx, 5)),
            "total_gain": _clean_cell(sheet.cell_value(row_idx, 7)),
        }

        # Convert from thousands to actual dollars
        for field in ("short_term_gain", "long_term_gain", "total_gain"):
            if row[field] is not None:
                row[field] *= 1_000

        rows.append(row)

    return pd.DataFrame(rows)
