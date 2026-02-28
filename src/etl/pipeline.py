"""Main ETL pipeline: raw IRS Excel files → canonical Parquet tables.

Usage:
    python -m src.etl.pipeline [--years 2020 2021 2022] [--skip-download]
"""

import argparse
import logging
from pathlib import Path

import pandas as pd

from .agi_bins import get_bins_dataframe
from .cpi_adjust import adjust_dataframe, MONEY_COLUMNS
from .url_registry import YEARS

logger = logging.getLogger(__name__)

DATA_ROOT = Path("data")
RAW_DIR = DATA_ROOT / "raw"
NOMINAL_DIR = DATA_ROOT / "processed" / "nominal"
REAL_DIR = DATA_ROOT / "processed" / "real_2022"


def _write_parquet(df: pd.DataFrame, name: str, output_dir: Path) -> None:
    """Write a DataFrame to Parquet."""
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{name}.parquet"
    df.to_parquet(path, index=False, engine="pyarrow")
    logger.info(f"Wrote {path} ({len(df)} rows)")


def run_pipeline(years: list[int] | None = None) -> None:
    """Execute the full ETL pipeline for the specified years."""
    years = years or YEARS
    NOMINAL_DIR.mkdir(parents=True, exist_ok=True)
    REAL_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Build AGI_BINS reference table
    agi_bins_df = get_bins_dataframe(years=years)
    _write_parquet(agi_bins_df, "agi_bins", NOMINAL_DIR)

    # 2-4. Parse tables for each year
    # TODO: Uncomment once parsers are implemented against actual file layouts
    #
    # from .parse_table_1x import build_returns_aggregate
    # from .parse_table_14a import parse_capital_gains
    # from .parse_table_3x import parse_bracket_tables
    #
    # all_returns = []
    # all_capgains = []
    # all_brackets = []
    #
    # for year in years:
    #     raw = RAW_DIR / str(year)
    #     prefix = str(year)[2:]
    #
    #     returns_df = build_returns_aggregate(raw, year)
    #     all_returns.append(returns_df)
    #
    #     cg_df = parse_capital_gains(raw / f"{prefix}in14acg.xls", year)
    #     all_capgains.append(cg_df)
    #
    #     brackets_df = parse_bracket_tables(
    #         raw / f"{prefix}in34tr.xls",
    #         raw / f"{prefix}in35tr.xls",
    #         raw / f"{prefix}in36tr.xls",
    #         year,
    #     )
    #     all_brackets.append(brackets_df)
    #
    # # 5. Concatenate all years and write nominal Parquet
    # returns_all = pd.concat(all_returns, ignore_index=True)
    # capgains_all = pd.concat(all_capgains, ignore_index=True)
    # brackets_all = pd.concat(all_brackets, ignore_index=True)
    #
    # _write_parquet(returns_all, "returns_aggregate", NOMINAL_DIR)
    # _write_parquet(capgains_all, "capital_gains", NOMINAL_DIR)
    # _write_parquet(brackets_all, "bracket_distribution", NOMINAL_DIR)
    #
    # # 6. CPI-adjust and write real Parquet
    # _write_parquet(
    #     adjust_dataframe(returns_all, MONEY_COLUMNS["returns_aggregate"]),
    #     "returns_aggregate", REAL_DIR,
    # )
    # _write_parquet(
    #     adjust_dataframe(capgains_all, MONEY_COLUMNS["capital_gains"]),
    #     "capital_gains", REAL_DIR,
    # )
    # _write_parquet(
    #     adjust_dataframe(brackets_all, MONEY_COLUMNS["bracket_distribution"]),
    #     "bracket_distribution", REAL_DIR,
    # )

    logger.info("Pipeline complete.")


def main():
    parser = argparse.ArgumentParser(description="Run ETL pipeline")
    parser.add_argument("--years", nargs="+", type=int, default=YEARS)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    run_pipeline(args.years)


if __name__ == "__main__":
    main()
