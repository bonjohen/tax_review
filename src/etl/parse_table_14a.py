"""Parse IRS SOI Table 1.4A into CAPITAL_GAINS records.

Table 1.4A: Returns with Income or Loss from Sales of Capital Assets
(Form 1040, Schedule D). 120 columns, row 9 = first data row.

Verified column layout (consistent across Tax Years 2018-2022):
  col 0  = AGI label
  col 1  = Taxable net gain: Number of returns
  col 2  = Taxable net gain: Amount (thousands)
  col 3  = Taxable net loss: Number of returns
  col 4  = Taxable net loss: Amount (thousands)
  col 5  = Net short-term capital gain: Number of returns
  col 6  = Net short-term capital gain: Amount (thousands)
  col 7  = Net short-term capital loss: Number of returns
  col 8  = Net short-term capital loss: Amount (thousands)
  col 59 = Short-term loss carryover: Number of returns
  col 60 = Short-term loss carryover: Amount (thousands)
  col 61 = Net long-term capital gain: Number of returns
  col 62 = Net long-term capital gain: Amount (thousands)
  col 63 = Net long-term capital loss: Number of returns
  col 64 = Net long-term capital loss: Amount (thousands)
  col 117 = Long-term loss carryover: Number of returns
  col 118 = Long-term loss carryover: Amount (thousands)
"""

import logging
import sqlite3
from pathlib import Path

import pandas as pd
import xlrd

from .agi_bins import match_agi_bin
from .db import insert_rows
from .parse_table_1x import _clean_cell, _money

logger = logging.getLogger(__name__)


def parse_capital_gains(filepath: Path, year: int) -> pd.DataFrame:
    """Parse Table 1.4A into CAPITAL_GAINS schema.

    Returns DataFrame with columns:
        year, agi_bin_id, short_term_gain, long_term_gain,
        total_gain, schedule_d_count
    """
    wb = xlrd.open_workbook(str(filepath))
    sh = wb.sheet_by_index(0)

    rows = []
    seen_bins = set()  # Track seen bin_ids to skip accumulated-size duplicates
    for r in range(9, sh.nrows):
        label = str(sh.cell_value(r, 0)).strip()
        if not label:
            continue
        if label.startswith("[") or label.startswith("*") or "NOTE:" in label or "SOURCE:" in label:
            break

        bin_id = match_agi_bin(label)
        if bin_id is None:
            continue
        if bin_id in seen_bins:
            continue  # Skip duplicate from "Accumulated Size of AGI" section
        seen_bins.add(bin_id)

        row = {
            "year": year,
            "agi_bin_id": bin_id,
            "schedule_d_count": _clean_cell(sh.cell_value(r, 1)),
            "short_term_gain": _money(sh.cell_value(r, 6)),
            "short_term_loss": _money(sh.cell_value(r, 8)),
            "long_term_gain": _money(sh.cell_value(r, 62)),
            "long_term_loss": _money(sh.cell_value(r, 64)),
            "total_gain": _money(sh.cell_value(r, 2)),
            "total_loss": _money(sh.cell_value(r, 4)),
            "st_loss_carryover": _money(sh.cell_value(r, 60)),
            "lt_loss_carryover": _money(sh.cell_value(r, 118)),
        }
        rows.append(row)

    logger.info(f"CAPITAL_GAINS TY{year}: parsed {len(rows)} rows from {filepath.name}")
    return pd.DataFrame(rows)


def load_table_14a(conn: sqlite3.Connection, filepath: Path, year: int) -> int:
    """Parse Table 1.4A and insert into raw_table_14a."""
    df = parse_capital_gains(filepath, year)
    rows = df.to_dict("records")
    return insert_rows(conn, "raw_table_14a", rows)
