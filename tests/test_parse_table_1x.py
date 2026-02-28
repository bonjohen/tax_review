"""Tests for Table 1.x parsing."""

import pytest

from src.etl.parse_table_1x import _clean_cell


class TestCleanCell:
    def test_numeric_passthrough(self):
        assert _clean_cell(1234.0) == 1234.0
        assert _clean_cell(0) == 0.0

    def test_string_with_commas(self):
        assert _clean_cell("1,234,567") == 1234567.0

    def test_footnote_markers(self):
        assert _clean_cell("1234[1]") == 1234.0
        assert _clean_cell("1234[2]") == 1234.0
        assert _clean_cell("1234*") == 1234.0

    def test_suppressed_data(self):
        assert _clean_cell("--") is None
        assert _clean_cell("-") is None
        assert _clean_cell("d") is None
        assert _clean_cell("D") is None

    def test_empty_values(self):
        assert _clean_cell(None) is None
        assert _clean_cell("") is None

    def test_nan(self):
        assert _clean_cell(float("nan")) is None
