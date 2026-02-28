"""Tests for Table 1.4A (capital gains) parsing.

Full integration tests require actual IRS data files.
See conftest.py raw_2020_dir fixture.
"""

import pytest


class TestParseCapitalGains:
    @pytest.mark.skip(reason="Requires actual IRS data files")
    def test_parse_2020(self, raw_2020_dir):
        from src.etl.parse_table_14a import parse_capital_gains
        df = parse_capital_gains(raw_2020_dir / "20in14acg.xls", 2020)
        assert len(df) > 0
        assert "agi_bin_id" in df.columns
        assert "short_term_gain" in df.columns
        assert "long_term_gain" in df.columns
