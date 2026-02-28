"""Tests for Table 3.x parsing.

Full integration tests require actual IRS data files.
"""

import pytest

from src.etl.parse_table_3x import _parse_rate, _detect_filing_status


class TestParseRate:
    def test_percent_word(self):
        assert _parse_rate("10 percent") == 0.10
        assert _parse_rate("37 percent") == 0.37

    def test_percent_sign(self):
        assert _parse_rate("10%") == 0.10
        assert _parse_rate("22%") == 0.22

    def test_invalid(self):
        assert _parse_rate("Total") is None
        assert _parse_rate(None) is None


class TestDetectFilingStatus:
    def test_known_statuses(self):
        assert _detect_filing_status("All returns") == "all"
        assert _detect_filing_status("Married filing jointly") == "married_filing_jointly"
        assert _detect_filing_status("Single") == "single"
        assert _detect_filing_status("Head of household") == "head_of_household"

    def test_unknown(self):
        assert _detect_filing_status("$50,000 under $75,000") is None
