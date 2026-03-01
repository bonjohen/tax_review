"""SQLite connection manager and helpers for the tax_review ETL pipeline.

Provides schema initialization, idempotent year resets, and bulk insert
for loading parsed IRS data into SQLite.
"""

import logging
import sqlite3
from pathlib import Path

from .agi_bins import CANONICAL_BINS
from .cpi_adjust import CPI_U_ANNUAL, TARGET_YEAR

logger = logging.getLogger(__name__)

_SCHEMA_PATH = Path(__file__).parent / "schema.sql"
DEFAULT_DB_PATH = Path("data") / "tax_review.db"

# All raw tables that store per-year data
_RAW_TABLES = [
    "raw_table_11", "raw_table_12", "raw_table_32", "raw_table_33",
    "raw_table_14a", "raw_table_14", "raw_table_34", "raw_table_36",
]


def get_connection(db_path: Path | str | None = None) -> sqlite3.Connection:
    """Open (or create) a SQLite database and return a connection.

    Uses WAL mode for better concurrent read performance.
    """
    db_path = Path(db_path) if db_path else DEFAULT_DB_PATH
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    """Create all tables and views from schema.sql (idempotent)."""
    sql = _SCHEMA_PATH.read_text(encoding="utf-8")
    conn.executescript(sql)
    logger.info("Schema initialized")


def reset_year(conn: sqlite3.Connection, year: int) -> None:
    """Delete all data for a given year from raw tables (idempotent re-runs)."""
    for table in _RAW_TABLES:
        conn.execute(f"DELETE FROM {table} WHERE year = ?", (year,))
    conn.commit()
    logger.info(f"Reset data for TY{year}")


def insert_rows(conn: sqlite3.Connection, table: str,
                rows: list[dict]) -> int:
    """Insert a list of row dicts into a table. Returns count inserted."""
    if not rows:
        return 0
    columns = list(rows[0].keys())
    placeholders = ", ".join(["?"] * len(columns))
    col_names = ", ".join(columns)
    sql = f"INSERT OR REPLACE INTO {table} ({col_names}) VALUES ({placeholders})"
    values = [tuple(row[c] for c in columns) for row in rows]
    conn.executemany(sql, values)
    logger.debug(f"Inserted {len(values)} rows into {table}")
    return len(values)


def load_agi_bins(conn: sqlite3.Connection) -> None:
    """Populate the agi_bins reference table from canonical definitions."""
    conn.execute("DELETE FROM agi_bins")
    rows = [
        {
            "agi_bin_id": b.bin_id,
            "lower_bound": b.lower_bound,
            "upper_bound": b.upper_bound,
            "label": b.label,
        }
        for b in CANONICAL_BINS
    ]
    insert_rows(conn, "agi_bins", rows)
    conn.commit()
    logger.info(f"Loaded {len(rows)} AGI bins")


def load_cpi_factors(conn: sqlite3.Connection) -> None:
    """Populate the cpi_factors reference table."""
    conn.execute("DELETE FROM cpi_factors")
    target_cpi = CPI_U_ANNUAL[TARGET_YEAR]
    rows = [
        {
            "year": year,
            "cpi_u_annual": cpi,
            "factor_to_2022": target_cpi / cpi,
        }
        for year, cpi in CPI_U_ANNUAL.items()
    ]
    insert_rows(conn, "cpi_factors", rows)
    conn.commit()
    logger.info(f"Loaded {len(rows)} CPI factors")
