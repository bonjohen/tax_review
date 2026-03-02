"""Tests for Table 1.4 (income sources) parsing."""

import pytest

from src.etl.parse_table_14 import parse_table_14, load_table_14, _normalize


INCOME_FIELDS = [
    "wages", "taxable_interest", "ordinary_dividends",
    "qualified_dividends", "tax_exempt_interest", "business_income",
    "capital_gains", "partnership_scorp", "ira_pension",
    "social_security", "rental_royalty", "estate_trust",
]


class TestNormalize:
    def test_basic(self):
        assert _normalize("  Salaries  and  Wages ") == "salaries and wages"

    def test_newline_collapse(self):
        assert _normalize("Net\nincome") == "net income"

    def test_empty(self):
        assert _normalize("") == ""


class TestParseTable14:
    """Integration tests using actual IRS data files."""

    @pytest.mark.parametrize("year,prefix", [
        (2018, "18"), (2019, "19"), (2020, "20"), (2021, "21"), (2022, "22"),
    ])
    def test_parse_all_years(self, year, prefix):
        from pathlib import Path
        filepath = Path("data/raw") / str(year) / f"{prefix}in14ar.xls"
        rows = parse_table_14(filepath, year)

        # Should produce exactly 19 AGI bins
        assert len(rows) == 19, f"TY{year}: expected 19 rows, got {len(rows)}"

        # All rows should have the correct year
        assert all(r["year"] == year for r in rows)

        # Bin IDs should be 1-19
        assert {r["agi_bin_id"] for r in rows} == set(range(1, 20))

        # All 12 income fields should be present in each row
        for r in rows:
            for field in INCOME_FIELDS:
                assert field in r, f"Missing field {field} in bin {r['agi_bin_id']}"

    def test_wages_are_largest_source(self):
        """Wages should be the dominant income source (basic sanity check)."""
        from pathlib import Path
        filepath = Path("data/raw/2022/22in14ar.xls")
        rows = parse_table_14(filepath, 2022)

        total_wages = sum(r["wages"] or 0 for r in rows)
        total_dividends = sum(r["ordinary_dividends"] or 0 for r in rows)
        assert total_wages > total_dividends, "Wages should exceed dividends"

    def test_values_in_dollars(self):
        """Values should be multiplied by 1000 (IRS reports in thousands)."""
        from pathlib import Path
        filepath = Path("data/raw/2022/22in14ar.xls")
        rows = parse_table_14(filepath, 2022)

        total_wages = sum(r["wages"] or 0 for r in rows)
        # Total wages in the US are in the trillions range
        assert total_wages > 1e12, f"Total wages {total_wages} too small — not in dollars"

    def test_ira_pension_combines_two_sources(self):
        """ira_pension should be non-None for most bins (combined IRA + pensions)."""
        from pathlib import Path
        filepath = Path("data/raw/2022/22in14ar.xls")
        rows = parse_table_14(filepath, 2022)

        non_null = [r for r in rows if r["ira_pension"] is not None and r["ira_pension"] != 0]
        assert len(non_null) >= 10, "Most bins should have IRA+pension income"


class TestLoadTable14:
    """Test the SQLite load wrapper."""

    def test_load_into_db(self, db):
        from pathlib import Path
        filepath = Path("data/raw/2020/20in14ar.xls")
        count = load_table_14(db, filepath, 2020)
        assert count == 19

        result = db.execute(
            "SELECT COUNT(*) AS n FROM raw_table_14 WHERE year = 2020"
        ).fetchone()
        assert result["n"] == 19
