"""Canonical AGI bin definitions and text-to-bin-ID mapping.

The IRS uses a standard set of AGI ranges across most Publication 1304 tables.
This module defines the canonical bin IDs and provides fuzzy matching from the
various text representations found in IRS Excel files.
"""

import re
from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class AgiBin:
    bin_id: int
    lower_bound: float    # inclusive, in dollars
    upper_bound: float    # exclusive, in dollars (inf for top bin)
    label: str            # canonical display label


# Standard IRS SOI AGI bins (consistent across Publication 1304 tables)
CANONICAL_BINS = [
    AgiBin(1,  float("-inf"), 0,          "No adjusted gross income"),
    AgiBin(2,  1,             5_000,       "$1 under $5,000"),
    AgiBin(3,  5_000,         10_000,      "$5,000 under $10,000"),
    AgiBin(4,  10_000,        15_000,      "$10,000 under $15,000"),
    AgiBin(5,  15_000,        20_000,      "$15,000 under $20,000"),
    AgiBin(6,  20_000,        25_000,      "$20,000 under $25,000"),
    AgiBin(7,  25_000,        30_000,      "$25,000 under $30,000"),
    AgiBin(8,  30_000,        40_000,      "$30,000 under $40,000"),
    AgiBin(9,  40_000,        50_000,      "$40,000 under $50,000"),
    AgiBin(10, 50_000,        75_000,      "$50,000 under $75,000"),
    AgiBin(11, 75_000,        100_000,     "$75,000 under $100,000"),
    AgiBin(12, 100_000,       200_000,     "$100,000 under $200,000"),
    AgiBin(13, 200_000,       500_000,     "$200,000 under $500,000"),
    AgiBin(14, 500_000,       1_000_000,   "$500,000 under $1,000,000"),
    AgiBin(15, 1_000_000,     1_500_000,   "$1,000,000 under $1,500,000"),
    AgiBin(16, 1_500_000,     2_000_000,   "$1,500,000 under $2,000,000"),
    AgiBin(17, 2_000_000,     5_000_000,   "$2,000,000 under $5,000,000"),
    AgiBin(18, 5_000_000,     10_000_000,  "$5,000,000 under $10,000,000"),
    AgiBin(19, 10_000_000,    float("inf"), "$10,000,000 or more"),
]

BINS_BY_ID = {b.bin_id: b for b in CANONICAL_BINS}

# Patterns for matching IRS text labels to bin IDs.
# Order matters: more specific patterns first.
_MATCH_PATTERNS: list[tuple[re.Pattern, int]] = [
    (re.compile(r"no adjusted gross income", re.I), 1),
    (re.compile(r"^\$1 under \$5,?000$", re.I), 2),
    (re.compile(r"^under \$5,?000$", re.I), 2),
    (re.compile(r"\$5,?000 under \$10,?000", re.I), 3),
    (re.compile(r"\$10,?000 under \$15,?000", re.I), 4),
    (re.compile(r"\$15,?000 under \$20,?000", re.I), 5),
    (re.compile(r"\$20,?000 under \$25,?000", re.I), 6),
    (re.compile(r"\$25,?000 under \$30,?000", re.I), 7),
    (re.compile(r"\$30,?000 under \$40,?000", re.I), 8),
    (re.compile(r"\$40,?000 under \$50,?000", re.I), 9),
    (re.compile(r"\$50,?000 under \$75,?000", re.I), 10),
    (re.compile(r"\$75,?000 under \$100,?000", re.I), 11),
    (re.compile(r"\$100,?000 under \$200,?000", re.I), 12),
    (re.compile(r"\$200,?000 under \$500,?000", re.I), 13),
    (re.compile(r"\$500,?000 under \$1,?000,?000", re.I), 14),
    (re.compile(r"\$1,?000,?000 under \$1,?500,?000", re.I), 15),
    (re.compile(r"\$1,?500,?000 under \$2,?000,?000", re.I), 16),
    (re.compile(r"\$2,?000,?000 under \$5,?000,?000", re.I), 17),
    (re.compile(r"\$5,?000,?000 under \$10,?000,?000", re.I), 18),
    (re.compile(r"\$10,?000,?000 or more", re.I), 19),
]

# Rows to skip (aggregate/total rows)
_SKIP_PATTERNS = [
    re.compile(r"all returns", re.I),
    re.compile(r"total", re.I),
    re.compile(r"taxable returns", re.I),
    re.compile(r"nontaxable returns", re.I),
    re.compile(r"size of adjusted gross income", re.I),
    re.compile(r"footnote", re.I),
]


def match_agi_bin(text: str) -> int | None:
    """Match an AGI bin text label to its canonical bin_id.

    Returns None for aggregate/total rows and unrecognized labels.
    """
    if not isinstance(text, str):
        return None
    text = text.strip()
    if not text:
        return None

    for pattern in _SKIP_PATTERNS:
        if pattern.search(text):
            return None

    for pattern, bin_id in _MATCH_PATTERNS:
        if pattern.search(text):
            return bin_id

    return None


def get_bins_dataframe(years: list[int] | None = None) -> pd.DataFrame:
    """Return AGI_BINS as a DataFrame.

    If years is provided, returns one row per (year, bin) combination.
    Otherwise returns bins without a year column.
    """
    rows = []
    target_years = years or [None]
    for year in target_years:
        for b in CANONICAL_BINS:
            row = {
                "agi_bin_id": b.bin_id,
                "agi_lower_bound": b.lower_bound,
                "agi_upper_bound": b.upper_bound,
                "label": b.label,
            }
            if year is not None:
                row["year"] = year
            rows.append(row)
    return pd.DataFrame(rows)
