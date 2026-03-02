"""Main ETL pipeline: raw IRS Excel files -> SQLite -> canonical Parquet tables.

Usage:
    python -m src.etl.pipeline [--years 2018 2019 2020 2021 2022]
"""

import argparse
import logging
from pathlib import Path

import pandas as pd

from .db import get_connection, init_schema, reset_year, reset_soca_year, load_agi_bins, load_cpi_factors
from .parse_table_1x import load_table_11, load_table_12, load_table_32, load_table_33
from .parse_table_14a import load_table_14a
from .parse_table_14 import load_table_14
from .parse_table_3x import load_table_34, load_table_36
from .parse_soca import load_soca_t4
from .url_registry import YEARS, SOCA_YEARS

logger = logging.getLogger(__name__)

DATA_ROOT = Path("data")
RAW_DIR = DATA_ROOT / "raw"
SOCA_DIR = RAW_DIR / "soca"
NOMINAL_DIR = DATA_ROOT / "processed" / "nominal"
REAL_DIR = DATA_ROOT / "processed" / "real_2022"
DB_PATH = DATA_ROOT / "tax_review.db"

# View-to-Parquet export mapping: (view_name, parquet_name, output_dir)
_NOMINAL_EXPORTS = [
    ("v_agi_bins", "agi_bins", NOMINAL_DIR),
    ("v_returns_aggregate", "returns_aggregate", NOMINAL_DIR),
    ("v_capital_gains", "capital_gains", NOMINAL_DIR),
    ("v_bracket_distribution", "bracket_distribution", NOMINAL_DIR),
]

_REAL_EXPORTS = [
    ("v_returns_aggregate_real2022", "returns_aggregate", REAL_DIR),
    ("v_capital_gains_real2022", "capital_gains", REAL_DIR),
    ("v_bracket_distribution_real2022", "bracket_distribution", REAL_DIR),
]


def _write_parquet(df: pd.DataFrame, name: str, output_dir: Path) -> None:
    """Write a DataFrame to Parquet."""
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{name}.parquet"
    df.to_parquet(path, index=False, engine="pyarrow")
    logger.info(f"Wrote {path} ({len(df)} rows)")


def run_pipeline(years: list[int] | None = None,
                 db_path: Path | str | None = None) -> None:
    """Execute the full ETL pipeline for the specified years.

    1. Initialize SQLite schema and load reference tables.
    2. Parse raw Excel files and insert into SQLite raw tables.
    3. Export canonical SQL views to Parquet (nominal + CPI-adjusted).
    """
    years = years or YEARS
    db_path = db_path or DB_PATH

    conn = get_connection(db_path)
    init_schema(conn)
    load_agi_bins(conn)
    load_cpi_factors(conn)

    for year in years:
        reset_year(conn, year)
        raw = RAW_DIR / str(year)
        prefix = str(year)[2:]

        load_table_11(conn, raw / f"{prefix}in11si.xls", year)
        load_table_12(conn, raw / f"{prefix}in12ms.xls", year)
        load_table_32(conn, raw / f"{prefix}in32tt.xls", year)
        load_table_33(conn, raw / f"{prefix}in33ar.xls", year)
        load_table_14a(conn, raw / f"{prefix}in14acg.xls", year)
        load_table_14(conn, raw / f"{prefix}in14ar.xls", year)
        load_table_34(conn, raw / f"{prefix}in34tr.xls", year)
        load_table_36(conn, raw / f"{prefix}in36tr.xls", year)

    conn.commit()
    logger.info("All raw tables loaded into SQLite")

    # Load SOCA data if available (separate download via --soca flag)
    for soca_year in SOCA_YEARS:
        yy = str(soca_year)[2:]
        soca_file = SOCA_DIR / str(soca_year) / f"{yy}in04soca.xlsx"
        if soca_file.exists():
            reset_soca_year(conn, soca_year)
            n = load_soca_t4(conn, soca_file, soca_year)
            logger.info(f"SOCA Table 4 TY{soca_year}: loaded {n} rows")
    conn.commit()

    # Export canonical views to Parquet
    NOMINAL_DIR.mkdir(parents=True, exist_ok=True)
    REAL_DIR.mkdir(parents=True, exist_ok=True)

    for view, name, output_dir in _NOMINAL_EXPORTS + _REAL_EXPORTS:
        df = pd.read_sql(f"SELECT * FROM {view}", conn)
        _write_parquet(df, name, output_dir)

    conn.close()
    logger.info("Pipeline complete.")


def main():
    parser = argparse.ArgumentParser(description="Run ETL pipeline")
    parser.add_argument("--years", nargs="+", type=int, default=YEARS)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    run_pipeline(args.years)


if __name__ == "__main__":
    main()
