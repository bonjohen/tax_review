"""Tests for AGI bin definitions and text matching."""

import pytest

from src.etl.agi_bins import match_agi_bin, CANONICAL_BINS, get_bins_dataframe


class TestMatchAgiBin:
    """Test fuzzy matching of IRS AGI text labels to canonical bin IDs."""

    @pytest.mark.parametrize("text,expected", [
        ("No adjusted gross income", 1),
        ("$1 under $5,000", 2),
        ("$5,000 under $10,000", 3),
        ("$10,000 under $15,000", 4),
        ("$15,000 under $20,000", 5),
        ("$20,000 under $25,000", 6),
        ("$25,000 under $30,000", 7),
        ("$30,000 under $40,000", 8),
        ("$40,000 under $50,000", 9),
        ("$50,000 under $75,000", 10),
        ("$75,000 under $100,000", 11),
        ("$100,000 under $200,000", 12),
        ("$200,000 under $500,000", 13),
        ("$500,000 under $1,000,000", 14),
        ("$1,000,000 under $1,500,000", 15),
        ("$1,500,000 under $2,000,000", 16),
        ("$2,000,000 under $5,000,000", 17),
        ("$5,000,000 under $10,000,000", 18),
        ("$10,000,000 or more", 19),
    ])
    def test_standard_labels(self, text, expected):
        assert match_agi_bin(text) == expected

    @pytest.mark.parametrize("text,expected", [
        ("Under $5,000", 2),
        ("  $50,000 under $75,000  ", 10),
    ])
    def test_variant_labels(self, text, expected):
        assert match_agi_bin(text) == expected

    @pytest.mark.parametrize("text", [
        "All returns",
        "All returns, total",
        "Total",
        "Taxable returns",
        "Nontaxable returns",
        "",
        "Size of adjusted gross income",
    ])
    def test_skip_rows(self, text):
        assert match_agi_bin(text) is None

    def test_none_input(self):
        assert match_agi_bin(None) is None

    def test_numeric_input(self):
        assert match_agi_bin(12345) is None


class TestCanonicalBins:
    def test_bin_count(self):
        assert len(CANONICAL_BINS) == 19

    def test_bin_ids_sequential(self):
        ids = [b.bin_id for b in CANONICAL_BINS]
        assert ids == list(range(1, 20))

    def test_bins_non_overlapping(self):
        for i in range(len(CANONICAL_BINS) - 1):
            assert CANONICAL_BINS[i].upper_bound <= CANONICAL_BINS[i + 1].lower_bound


class TestGetBinsDataframe:
    def test_without_years(self):
        df = get_bins_dataframe()
        assert len(df) == 19
        assert "year" not in df.columns

    def test_with_years(self):
        df = get_bins_dataframe(years=[2020, 2021, 2022])
        assert len(df) == 19 * 3
        assert "year" in df.columns
