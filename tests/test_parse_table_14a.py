"""Tests for Table 1.4A (capital gains) parsing."""

import pytest

from src.etl.parse_table_14a import parse_capital_gains, load_table_14a
from src.etl.db import insert_rows


class TestParseCapitalGains:
    """Integration tests using actual IRS data files."""

    @pytest.mark.parametrize("year,prefix", [
        (2018, "18"), (2019, "19"), (2020, "20"), (2021, "21"), (2022, "22"),
    ])
    def test_parse_all_years(self, year, prefix):
        from pathlib import Path
        filepath = Path("data/raw") / str(year) / f"{prefix}in14acg.xls"
        df = parse_capital_gains(filepath, year)

        # Should produce exactly 19 AGI bins
        assert len(df) == 19, f"TY{year}: expected 19 rows, got {len(df)}"

        # Required columns
        for col in ["year", "agi_bin_id", "short_term_gain",
                     "long_term_gain", "total_gain", "schedule_d_count"]:
            assert col in df.columns, f"Missing column: {col}"

        # All rows should have the correct year
        assert (df["year"] == year).all()

        # Bin IDs should be 1-19
        assert set(df["agi_bin_id"]) == set(range(1, 20))

        # Schedule D counts should be non-negative
        assert (df["schedule_d_count"] >= 0).all()

    def test_gains_are_in_dollars_not_thousands(self):
        """Values should be multiplied by 1000 (IRS reports in thousands)."""
        from pathlib import Path
        filepath = Path("data/raw/2022/22in14acg.xls")
        df = parse_capital_gains(filepath, 2022)

        # Top bin (19: $10M+) LT gains should be in the billions range
        top_bin = df[df["agi_bin_id"] == 19].iloc[0]
        assert top_bin["long_term_gain"] > 1e9, "LT gains should be in dollars, not thousands"

    def test_total_gain_relationship(self):
        """total_gain should be <= short_term + long_term (netting can reduce it)."""
        from pathlib import Path
        filepath = Path("data/raw/2022/22in14acg.xls")
        df = parse_capital_gains(filepath, 2022)

        for _, row in df.iterrows():
            st = row["short_term_gain"] or 0
            lt = row["long_term_gain"] or 0
            total = row["total_gain"] or 0
            # total_gain can be less than st+lt due to netting, but not more
            assert total <= st + lt + 1, (
                f"Bin {row['agi_bin_id']}: total {total} > st+lt {st+lt}"
            )


class TestLoadTable14a:
    """Test the SQLite load wrapper."""

    def test_load_into_db(self, db):
        from pathlib import Path
        filepath = Path("data/raw/2020/20in14acg.xls")
        count = load_table_14a(db, filepath, 2020)
        assert count == 19

        result = db.execute(
            "SELECT COUNT(*) AS n FROM raw_table_14a WHERE year = 2020"
        ).fetchone()
        assert result["n"] == 19
