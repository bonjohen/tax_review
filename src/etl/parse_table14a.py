"""Parse IRS SOI Table 1.4A into a normalized gain/loss schema.

Implements dynamic sheet detection and header finding per the v4 plan:
1. Scan sheets for "Table 1.4A" cell
2. Locate header row with AGI label + gain/loss keywords
3. Extract AGI rows with numeric cleanup
4. Produce normalized DataFrame with: year, agi_bin, returns_total,
   returns_net_gain, amount_net_gain, returns_net_loss, amount_net_loss

Usage:
    python -m src.etl.parse_table14a              # parse all years
    python -m src.etl.parse_table14a --year 2022   # single year
"""

import argparse
import logging
import re
from pathlib import Path

import pandas as pd
import xlrd

logger = logging.getLogger(__name__)

RAW_DIR = Path("data/raw")

YEARS = {
    2018: "18in14acg.xls",
    2019: "19in14acg.xls",
    2020: "20in14acg.xls",
    2021: "21in14acg.xls",
    2022: "22in14acg.xls",
}

# Rows to skip: footnotes, section headers, accumulated-size repeats
_SKIP_PATTERNS = re.compile(
    r"^\[|^\*|^NOTE:|^SOURCE:|^Accumulated|^Taxable returns|^Nontaxable returns",
    re.IGNORECASE,
)


def _find_table_sheet(wb: xlrd.Book) -> xlrd.sheet.Sheet:
    """Find the sheet containing Table 1.4A by scanning cell values."""
    for idx in range(wb.nsheets):
        sh = wb.sheet_by_index(idx)
        # Check first 10 rows for "Table 1.4A" or "Schedule D"
        for r in range(min(10, sh.nrows)):
            for c in range(min(5, sh.ncols)):
                val = str(sh.cell_value(r, c)).lower()
                if "table 1.4a" in val or "sales of capital assets" in val:
                    return sh
    # Fallback: first sheet
    return wb.sheet_by_index(0)


def _detect_scale(sh: xlrd.sheet.Sheet) -> int:
    """Detect whether amounts are in thousands of dollars."""
    for r in range(min(10, sh.nrows)):
        for c in range(min(5, sh.ncols)):
            val = str(sh.cell_value(r, c)).lower()
            if "thousands of dollars" in val or "in thousands" in val:
                return 1_000
    return 1


def _find_header_row(sh: xlrd.sheet.Sheet) -> int:
    """Find the header row containing AGI label and gain/loss keywords."""
    for r in range(sh.nrows):
        row_text = " ".join(
            str(sh.cell_value(r, c)).lower() for c in range(min(10, sh.ncols))
        )
        has_agi = "adjusted gross income" in row_text or "size of" in row_text
        keyword_count = sum(
            1 for kw in ["returns", "net gain", "net loss", "taxable"]
            if kw in row_text
        )
        if has_agi and keyword_count >= 2:
            return r
    # Fallback: known layout (row 2 for SOI Table 1.4A)
    return 2


def _find_data_start(sh: xlrd.sheet.Sheet, header_row: int) -> int:
    """Find the first data row after the header."""
    # Data typically starts after column-number reference row
    for r in range(header_row + 1, min(header_row + 10, sh.nrows)):
        label = str(sh.cell_value(r, 0)).strip()
        if label and not label.replace(".", "").isdigit():
            # Non-empty, non-numeric label = first data row
            return r
    return header_row + 1


def _clean_numeric(val) -> float | None:
    """Clean a cell value to a float, handling SOI formatting."""
    if val is None or val == "":
        return None
    s = str(val).strip()
    if not s or s in ("--", "\u2014", "-", "***", "d"):
        return None
    # Handle parentheses for negatives: (123) -> -123
    if s.startswith("(") and s.endswith(")"):
        s = "-" + s[1:-1]
    # Remove commas, footnote markers, whitespace
    s = re.sub(r"[,\[\]*]", "", s).strip()
    try:
        return float(s)
    except ValueError:
        return None


def _clean_count(val) -> int | None:
    """Clean a cell value to an integer count."""
    v = _clean_numeric(val)
    if v is None:
        return None
    return int(round(v))


def _normalize_agi_label(label: str) -> str:
    """Normalize whitespace in AGI bin label."""
    return " ".join(label.split())


def _is_agi_row(label: str) -> bool:
    """Check if a row label is a valid AGI bin (not a footer/note)."""
    if not label:
        return False
    if _SKIP_PATTERNS.search(label):
        return False
    return True


def parse_table14a(filepath: Path, year: int) -> tuple[pd.DataFrame, dict | None]:
    """Parse Table 1.4A into normalized gain/loss schema.

    Returns:
        (data_df, totals_dict) where totals_dict contains the "All returns"
        row if found, or None if not present.
    """
    wb = xlrd.open_workbook(str(filepath))
    sh = _find_table_sheet(wb)
    scale = _detect_scale(sh)
    header_row = _find_header_row(sh)
    data_start = _find_data_start(sh, header_row)

    logger.info(
        f"TY{year}: sheet={sh.name!r}, scale={scale}x, "
        f"header_row={header_row}, data_start={data_start}"
    )

    # Column positions (verified consistent across TY2018-2022):
    #   col 0  = AGI label
    #   col 1  = Taxable net gain: Number of returns
    #   col 2  = Taxable net gain: Amount
    #   col 3  = Taxable net loss: Number of returns
    #   col 4  = Taxable net loss: Amount
    COL_GAIN_COUNT = 1
    COL_GAIN_AMOUNT = 2
    COL_LOSS_COUNT = 3
    COL_LOSS_AMOUNT = 4

    rows = []
    totals = None

    for r in range(data_start, sh.nrows):
        label = str(sh.cell_value(r, 0)).strip()
        if not label:
            continue

        # Stop at the "Taxable returns" or "Nontaxable returns" subsections
        # (we only want the "All returns" section)
        label_lower = label.lower()
        if ("taxable returns" in label_lower or "nontaxable returns" in label_lower) \
                and "total" in label_lower:
            break

        normalized_label = _normalize_agi_label(label)

        # Check for total row
        if "all returns" in label_lower and "total" in label_lower:
            gain_count = _clean_count(sh.cell_value(r, COL_GAIN_COUNT))
            gain_amount = _clean_numeric(sh.cell_value(r, COL_GAIN_AMOUNT))
            loss_count = _clean_count(sh.cell_value(r, COL_LOSS_COUNT))
            loss_amount = _clean_numeric(sh.cell_value(r, COL_LOSS_AMOUNT))

            totals = {
                "returns_net_gain": gain_count,
                "amount_net_gain": gain_amount * scale if gain_amount else None,
                "returns_net_loss": loss_count,
                "amount_net_loss": abs(loss_amount * scale) if loss_amount else None,
            }
            if gain_count is not None and loss_count is not None:
                totals["returns_total"] = gain_count + loss_count
            continue

        if not _is_agi_row(label):
            continue

        gain_count = _clean_count(sh.cell_value(r, COL_GAIN_COUNT))
        gain_amount = _clean_numeric(sh.cell_value(r, COL_GAIN_AMOUNT))
        loss_count = _clean_count(sh.cell_value(r, COL_LOSS_COUNT))
        loss_amount = _clean_numeric(sh.cell_value(r, COL_LOSS_AMOUNT))

        # Compute returns_total as sum of gain + loss filers
        returns_total = None
        if gain_count is not None and loss_count is not None:
            returns_total = gain_count + loss_count

        rows.append({
            "year": year,
            "agi_bin": normalized_label,
            "returns_total": returns_total,
            "returns_net_gain": gain_count,
            "amount_net_gain": gain_amount * scale if gain_amount is not None else None,
            "returns_net_loss": loss_count,
            "amount_net_loss": abs(loss_amount * scale) if loss_amount is not None else None,
        })

    logger.info(f"TY{year}: parsed {len(rows)} AGI bins")
    df = pd.DataFrame(rows)

    # Cast count columns to nullable int
    for col in ["returns_total", "returns_net_gain", "returns_net_loss"]:
        df[col] = df[col].astype("Int64")

    return df, totals


def _extract_extended_row(sh, r: int, scale: int) -> dict:
    """Extract all ~30 extended columns from a single data row.

    Column layout (verified across TY2018-2022):
    - Odd-indexed columns = counts (number of returns)
    - Even-indexed columns = amounts (in thousands of dollars)

    Returns dict with amounts scaled to dollars and counts as integers.
    """
    def amt(c):
        v = _clean_numeric(sh.cell_value(r, c))
        return v * scale if v is not None else 0.0

    def cnt(c):
        return _clean_count(sh.cell_value(r, c)) or 0

    return {
        # Overall net ST (cols 5-8)
        "st_gain_count": cnt(5),
        "st_gain_amount": amt(6),
        "st_loss_count": cnt(7),
        "st_loss_amount": amt(8),
        # ST from sales (cols 9-12)
        "st_gain_sales_count": cnt(9),
        "st_gain_sales_amount": amt(10),
        "st_loss_sales_count": cnt(11),
        "st_loss_sales_amount": amt(12),
        # ST carryover (col 59 — amount only, no count pair)
        "st_carry_amount": amt(59),
        # Overall net LT (cols 61-64)
        "lt_gain_count": cnt(61),
        "lt_gain_amount": amt(62),
        "lt_loss_count": cnt(63),
        "lt_loss_amount": amt(64),
        # LT from sales (cols 65-68)
        "lt_gain_sales_count": cnt(65),
        "lt_gain_sales_amount": amt(66),
        "lt_loss_sales_count": cnt(67),
        "lt_loss_sales_amount": amt(68),
        # LT sub-category A: basis reported, no Form 8949 (cols 73-76)
        "sub_a_gain_amount": amt(74),
        "sub_a_loss_amount": amt(76),
        # LT sub-category B: basis reported on Form 8949 (cols 83-86)
        "sub_b_gain_amount": amt(84),
        "sub_b_loss_amount": amt(86),
        # LT sub-category C: no basis reported (cols 93-96)
        "sub_c_gain_amount": amt(94),
        "sub_c_loss_amount": amt(96),
        # LT sub-category D: no Form 1099-B (cols 103-106)
        "sub_d_gain_amount": amt(104),
        "sub_d_loss_amount": amt(106),
        # Opaque sources: other forms (cols 107-110)
        "other_forms_gain_count": cnt(107),
        "other_forms_gain_amount": amt(108),
        "other_forms_loss_count": cnt(109),
        "other_forms_loss_amount": amt(110),
        # Opaque sources: partnership/S-corp (cols 111-114)
        "partnership_gain_count": cnt(111),
        "partnership_gain_amount": amt(112),
        "partnership_loss_count": cnt(113),
        "partnership_loss_amount": amt(114),
        # Capital gain distributions (cols 115-116)
        "cap_gain_dist_count": cnt(115),
        "cap_gain_dist_amount": amt(116),
        # LT loss carryover (cols 117-118)
        "lt_carry_count": cnt(117),
        "lt_carry_amount": amt(118),
    }


def parse_table14a_extended(
    filepath: Path, year: int
) -> tuple[list[dict], dict | None]:
    """Parse Table 1.4A with extended columns for calibration.

    Extracts ~30 additional columns per AGI bin covering:
    - Net ST/LT from sales (counts + amounts)
    - Transaction sub-categories A-D (LT gain/loss amounts)
    - Opaque sources: other forms, partnership/S-corp (counts + amounts)
    - Capital gain distributions, loss carryovers
    - Both "All returns" and "Taxable returns" sections

    Returns:
        (rows, totals) where rows is list[dict] (no pandas dependency)
        and totals is the "All returns, total" row dict or None.
        Each row includes 'section' key: 'all_returns' or 'taxable_returns'.
    """
    from src.etl.agi_bins import match_agi_bin

    wb = xlrd.open_workbook(str(filepath))
    sh = _find_table_sheet(wb)
    scale = _detect_scale(sh)
    header_row = _find_header_row(sh)
    data_start = _find_data_start(sh, header_row)

    logger.info(
        f"TY{year} extended: sheet={sh.name!r}, scale={scale}x, "
        f"header_row={header_row}, data_start={data_start}, ncols={sh.ncols}"
    )

    rows = []
    totals = None
    current_section = "all_returns"

    for r in range(data_start, sh.nrows):
        label = str(sh.cell_value(r, 0)).strip()
        if not label:
            continue

        label_lower = label.lower()

        # Detect footnotes / end of data
        if label.startswith("[") or label.startswith("*") or "NOTE:" in label:
            break

        # Detect section transitions
        if "taxable returns" in label_lower and "total" in label_lower:
            current_section = "taxable_returns"
            # Extract totals row for taxable section
            ext = _extract_extended_row(sh, r, scale)
            ext["year"] = year
            ext["section"] = current_section
            ext["agi_bin_label"] = _normalize_agi_label(label)
            ext["agi_bin_id"] = None  # total row
            rows.append(ext)
            continue

        if "nontaxable returns" in label_lower and "total" in label_lower:
            break  # Done — nontaxable section not needed

        # Handle "All returns, total" row
        if "all returns" in label_lower and "total" in label_lower:
            ext = _extract_extended_row(sh, r, scale)
            ext["year"] = year
            ext["section"] = "all_returns"
            ext["agi_bin_label"] = _normalize_agi_label(label)
            ext["agi_bin_id"] = None
            totals = ext
            continue

        # Match AGI bin
        bin_id = match_agi_bin(label)
        if bin_id is None:
            # In taxable section, "$1,000,000 or more" is a collapsed bin
            if current_section == "taxable_returns":
                if "$1,000,000 or more" in label or "$1,000,000 and over" in label.replace(",", ","):
                    ext = _extract_extended_row(sh, r, scale)
                    ext["year"] = year
                    ext["section"] = current_section
                    ext["agi_bin_label"] = _normalize_agi_label(label)
                    ext["agi_bin_id"] = "15_plus"  # collapsed bins 15-19
                    rows.append(ext)
            continue

        ext = _extract_extended_row(sh, r, scale)
        ext["year"] = year
        ext["section"] = current_section
        ext["agi_bin_label"] = _normalize_agi_label(label)
        ext["agi_bin_id"] = bin_id
        rows.append(ext)

    all_ct = sum(1 for r in rows if r["section"] == "all_returns")
    tax_ct = sum(1 for r in rows if r["section"] == "taxable_returns")
    logger.info(f"TY{year} extended: {all_ct} all-returns bins, {tax_ct} taxable bins")

    return rows, totals


def parse_all(years: list[int] | None = None) -> dict[int, tuple[pd.DataFrame, dict | None]]:
    """Parse Table 1.4A for all specified years."""
    if years is None:
        years = sorted(YEARS.keys())

    results = {}
    for year in years:
        filepath = RAW_DIR / str(year) / YEARS[year]
        if not filepath.exists():
            logger.warning(f"File not found: {filepath}")
            continue
        results[year] = parse_table14a(filepath, year)

    return results


def main():
    parser = argparse.ArgumentParser(description="Parse Table 1.4A files")
    parser.add_argument("--year", type=int, help="Single year to parse")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    years = [args.year] if args.year else None
    results = parse_all(years=years)

    for year, (df, totals) in results.items():
        print(f"\n=== TY{year}: {len(df)} bins ===")
        print(df.to_string(index=False))
        if totals:
            print(f"\nTotals row: {totals}")


if __name__ == "__main__":
    main()
