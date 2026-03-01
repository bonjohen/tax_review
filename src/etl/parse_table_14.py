"""Parse IRS SOI Table 1.4 into INCOME_SOURCES records.

Table 1.4: All Returns: Sources of Income, Adjustments, and Tax Items,
by Size of Adjusted Gross Income.

Column layouts shift between tax years (140-153 cols), so we use dynamic
header matching on row 2 (and sub-headers on row 3) rather than fixed
column indices.

Extracts 12 income source fields per AGI bin:
  wages, taxable_interest, ordinary_dividends, qualified_dividends,
  tax_exempt_interest, business_income, capital_gains, partnership_scorp,
  ira_pension, social_security, rental_royalty, estate_trust
"""

import logging
import re
import sqlite3
from pathlib import Path

import xlrd

from .agi_bins import match_agi_bin
from .db import insert_rows
from .parse_table_1x import _clean_cell, _money

logger = logging.getLogger(__name__)


# ── Header matching rules ──────────────────────────────────────────────────
# Each rule: (field_name, row2_keyword, amount_strategy)
#
# amount_strategy:
#   "simple"     → amount col = header_col + 1
#   "net_income" → scan row 3 from header_col for "Net\nincome" or "Net income",
#                   then amount col = that_col + 1
#   "taxable"    → scan row 3 from header_col for "Taxable", amount = that_col + 1
#   "ira_pension" → special: sum IRA amount + Pensions amount

_HEADER_RULES = [
    ("wages",             ["salaries and wages", "total wages"], "simple"),
    ("taxable_interest",  ["taxable interest"], "simple"),
    ("tax_exempt_interest", ["tax-exempt interest"], "simple"),
    ("ordinary_dividends", ["ordinary dividends"], "simple"),
    ("qualified_dividends", ["qualified dividends"], "simple"),
    ("business_income",   ["business or profession"], "net_income"),
    ("capital_gains",     ["sales of capital assets reported on form 1040, schedule d"], "net_gain"),
    ("rental_royalty",    ["total rental and royalty"], "net_income"),
    ("estate_trust",      ["estate and trust"], "net_income"),
    ("social_security",   ["social security benefits"], "taxable"),
]

# These are handled specially — matched but processed as compound fields
_IRA_KEYWORDS = ["taxable individual retirement", "ira distribution"]
_PENSION_KEYWORDS = ["pensions and annuities"]
_PARTNERSHIP_KEYWORDS = ["partnership and s corporation", "partnership and s-corporation"]
_PARTNERSHIP_ONLY_KEYWORDS = ["partnership"]  # 2021-2022: just "Partnership"
_SCORP_KEYWORDS = ["s corporation", "s-corporation"]


def _normalize(text: str) -> str:
    """Normalize header text for matching: lowercase, collapse whitespace."""
    return re.sub(r"\s+", " ", str(text).strip().lower())


def _find_header_col(sheet, keywords: list[str]) -> int | None:
    """Find the column in row 2 whose text matches any keyword."""
    for c in range(sheet.ncols):
        text = _normalize(sheet.cell_value(2, c))
        # Remove footnote markers like [1], [2], [3]
        text = re.sub(r"\[\d+\]", "", text).strip()
        for kw in keywords:
            if kw in text:
                return c
    return None


def _find_subheader_col(sheet, start_col: int, end_col: int,
                        keyword: str) -> int | None:
    """Find a sub-header in row 3 between start_col and end_col."""
    for c in range(start_col, min(end_col, sheet.ncols)):
        text = _normalize(sheet.cell_value(3, c))
        text = re.sub(r"\[\d+\]", "", text).strip()
        if keyword in text:
            return c
    return None


def _resolve_amount_col(sheet, header_col: int, strategy: str) -> int | None:
    """Determine the amount column given a header column and strategy."""
    if strategy == "simple":
        # Count col at header_col, amount col at header_col + 1
        return header_col + 1

    # For sub-header strategies, scan row 3 within the next few columns
    # (up to 6 cols from header to cover count+amount pairs)
    search_end = header_col + 8

    if strategy == "net_income":
        sub_col = _find_subheader_col(sheet, header_col, search_end, "net income")
        if sub_col is not None:
            return sub_col + 1
        # Fallback: amount at header + 1
        return header_col + 1

    if strategy == "net_gain":
        # "Taxable net gain" for Schedule D capital gains
        sub_col = _find_subheader_col(sheet, header_col, search_end, "taxable net gain")
        if sub_col is not None:
            return sub_col + 1
        # Also try just "net gain"
        sub_col = _find_subheader_col(sheet, header_col, search_end, "net gain")
        if sub_col is not None:
            return sub_col + 1
        return header_col + 1

    if strategy == "taxable":
        sub_col = _find_subheader_col(sheet, header_col, search_end, "taxable")
        if sub_col is not None:
            return sub_col + 1
        return header_col + 1

    return header_col + 1


def _build_col_map(sheet, year: int) -> dict:
    """Build a mapping of field_name -> amount_col_index by scanning headers.

    Returns dict like {"wages": 6, "taxable_interest": 8, ...}
    """
    col_map = {}

    # Standard fields
    for field, keywords, strategy in _HEADER_RULES:
        hcol = _find_header_col(sheet, keywords)
        if hcol is not None:
            amount_col = _resolve_amount_col(sheet, hcol, strategy)
            if amount_col is not None:
                col_map[field] = amount_col
                logger.debug(f"  {field}: header c{hcol} -> amount c{amount_col}")
        else:
            logger.warning(f"  {field}: header not found for {keywords}")

    # IRA + Pensions → combined as ira_pension
    ira_col = _find_header_col(sheet, _IRA_KEYWORDS)
    pension_col = _find_header_col(sheet, _PENSION_KEYWORDS)
    ira_amount = (ira_col + 1) if ira_col is not None else None
    pension_amount = (pension_col + 1) if pension_col is not None else None
    col_map["_ira_col"] = ira_amount
    col_map["_pension_col"] = pension_amount

    # Partnership & S-corp handling
    # Try combined "Partnership and S corporation" first (2018-2020)
    combined_col = _find_header_col(sheet, _PARTNERSHIP_KEYWORDS)
    if combined_col is not None:
        amount_col = _resolve_amount_col(sheet, combined_col, "net_income")
        col_map["_partnership_combined"] = amount_col
        col_map["_scorp_separate"] = None
        logger.debug(f"  partnership_scorp (combined): c{amount_col}")
    else:
        # Separate Partnership and S-corp (2021-2022)
        p_col = _find_header_col(sheet, _PARTNERSHIP_ONLY_KEYWORDS)
        s_col = _find_header_col(sheet, _SCORP_KEYWORDS)
        if p_col is not None:
            col_map["_partnership_combined"] = _resolve_amount_col(
                sheet, p_col, "net_income")
        else:
            col_map["_partnership_combined"] = None
        if s_col is not None:
            col_map["_scorp_separate"] = _resolve_amount_col(
                sheet, s_col, "net_income")
        else:
            col_map["_scorp_separate"] = None
        logger.debug(f"  partnership: c{col_map['_partnership_combined']}, "
                     f"scorp: c{col_map['_scorp_separate']}")

    return col_map


def parse_table_14(filepath: Path, year: int) -> list[dict]:
    """Parse Table 1.4 into income source records.

    Returns list of dicts with keys:
        year, agi_bin_id, wages, taxable_interest, ordinary_dividends,
        qualified_dividends, tax_exempt_interest, business_income,
        capital_gains, partnership_scorp, ira_pension, social_security,
        rental_royalty, estate_trust
    """
    wb = xlrd.open_workbook(str(filepath))
    sh = wb.sheet_by_index(0)

    logger.info(f"Table 1.4 TY{year}: {sh.ncols} cols, {sh.nrows} rows")
    col_map = _build_col_map(sh, year)

    # Standard fields to extract
    simple_fields = [
        "wages", "taxable_interest", "ordinary_dividends",
        "qualified_dividends", "tax_exempt_interest", "business_income",
        "capital_gains", "rental_royalty", "estate_trust", "social_security",
    ]

    rows = []
    seen_bins = set()
    for r in range(9, sh.nrows):
        label = str(sh.cell_value(r, 0)).strip()
        if not label:
            continue
        if (label.startswith("[") or label.startswith("*")
                or "NOTE:" in label or "SOURCE:" in label):
            break

        bin_id = match_agi_bin(label)
        if bin_id is None:
            continue
        if bin_id in seen_bins:
            continue
        seen_bins.add(bin_id)

        row = {"year": year, "agi_bin_id": bin_id}

        # Extract standard fields
        for field in simple_fields:
            c = col_map.get(field)
            if c is not None:
                row[field] = _money(sh.cell_value(r, c))
            else:
                row[field] = None

        # IRA + Pensions combined
        ira_val = None
        pension_val = None
        if col_map.get("_ira_col") is not None:
            ira_val = _money(sh.cell_value(r, col_map["_ira_col"]))
        if col_map.get("_pension_col") is not None:
            pension_val = _money(sh.cell_value(r, col_map["_pension_col"]))
        row["ira_pension"] = (ira_val or 0) + (pension_val or 0) or None

        # Partnership + S-corp
        p_val = None
        s_val = None
        if col_map.get("_partnership_combined") is not None:
            p_val = _money(sh.cell_value(r, col_map["_partnership_combined"]))
        if col_map.get("_scorp_separate") is not None:
            s_val = _money(sh.cell_value(r, col_map["_scorp_separate"]))
        if p_val is not None and s_val is not None:
            row["partnership_scorp"] = (p_val or 0) + (s_val or 0) or None
        elif p_val is not None:
            row["partnership_scorp"] = p_val
        else:
            row["partnership_scorp"] = s_val

        rows.append(row)

    logger.info(f"Table 1.4 TY{year}: parsed {len(rows)} rows from {filepath.name}")
    return rows


def load_table_14(conn: sqlite3.Connection, filepath: Path, year: int) -> int:
    """Parse Table 1.4 and insert into raw_table_14."""
    rows = parse_table_14(filepath, year)
    return insert_rows(conn, "raw_table_14", rows)
