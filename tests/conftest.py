"""Shared test fixtures."""

import pytest
from pathlib import Path

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def fixtures_dir():
    """Path to test fixtures directory."""
    return FIXTURES_DIR


@pytest.fixture
def raw_2020_dir():
    """Path to real 2020 data (skips if not downloaded)."""
    path = Path("data") / "raw" / "2020"
    if not path.exists() or not any(path.glob("*.xls")):
        pytest.skip("Real IRS data not downloaded (run: python -m src.etl.download)")
    return path
