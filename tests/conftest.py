"""Shared test fixtures."""

import pytest
from pathlib import Path

from src.etl.db import get_connection, init_schema

RAW_DIR = Path("data") / "raw"


@pytest.fixture
def db(tmp_path):
    """SQLite database with schema initialized (temp directory, auto-cleaned)."""
    db_path = tmp_path / "test.db"
    conn = get_connection(db_path)
    init_schema(conn)
    yield conn
    conn.close()


@pytest.fixture
def raw_2020_dir():
    """Path to real 2020 IRS data."""
    return RAW_DIR / "2020"


@pytest.fixture
def raw_2021_dir():
    """Path to real 2021 IRS data."""
    return RAW_DIR / "2021"


@pytest.fixture
def raw_2022_dir():
    """Path to real 2022 IRS data."""
    return RAW_DIR / "2022"
