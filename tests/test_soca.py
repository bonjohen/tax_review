"""Tests for SOCA parsing, extended Table 1.4A, and calibration validation."""

import json
from pathlib import Path

import pytest

from src.etl.db import insert_rows, load_agi_bins
from src.etl.parse_soca import (
    parse_soca_t4, load_soca_t4, classify_holding_duration,
)
from src.etl.parse_table_14a import parse_capital_gains, load_table_14a
from src.validation.calibration_validate import (
    compute_empirical_loss_ratios,
    compute_empirical_holding_fractions,
    validate_calibration,
)


SOCA_DIR = Path("data") / "raw" / "soca"


class TestClassifyHoldingDuration:
    """Test the holding period classification helper."""

    def test_1_to_5_year_labels(self):
        assert classify_holding_duration("Under 18 months [3]") == "lt_1_to_5yr"
        assert classify_holding_duration("18 months under 2 years") == "lt_1_to_5yr"
        assert classify_holding_duration("2 years under 3 years") == "lt_1_to_5yr"
        assert classify_holding_duration("3 years under 4 years") == "lt_1_to_5yr"
        assert classify_holding_duration("4 years under 5 years") == "lt_1_to_5yr"

    def test_5_plus_year_labels(self):
        assert classify_holding_duration("5 years under 10 years") == "lt_5yr_plus"
        assert classify_holding_duration("10 years under 15 years") == "lt_5yr_plus"
        assert classify_holding_duration("15 years under 20 years") == "lt_5yr_plus"
        assert classify_holding_duration("20 years or more") == "lt_5yr_plus"

    def test_unclassifiable(self):
        assert classify_holding_duration("Period not determinable") is None
        assert classify_holding_duration("Total") is None


class TestParseSocaT4:
    """Integration tests for SOCA Table 4 parsing."""

    @pytest.fixture
    def soca_2015_file(self):
        path = SOCA_DIR / "2015" / "15in04soca.xlsx"
        if not path.exists():
            pytest.skip("SOCA 2015 data not downloaded")
        return path

    def test_parse_returns_rows(self, soca_2015_file):
        rows = parse_soca_t4(soca_2015_file, 2015)
        assert len(rows) > 0

    def test_all_asset_types_present(self, soca_2015_file):
        rows = parse_soca_t4(soca_2015_file, 2015)
        asset_types = {r["asset_type"] for r in rows}
        assert "all_assets" in asset_types
        assert "corporate_stock" in asset_types

    def test_holding_periods(self, soca_2015_file):
        rows = parse_soca_t4(soca_2015_file, 2015)
        periods = {r["holding_period"] for r in rows}
        assert "short_term" in periods
        assert "long_term" in periods

    def test_gains_in_dollars(self, soca_2015_file):
        """Values should be multiplied by 1000 (SOCA reports in thousands)."""
        rows = parse_soca_t4(soca_2015_file, 2015)
        # All-assets LT total gain should be in the hundreds of billions
        lt_rows = [r for r in rows
                    if r["asset_type"] == "all_assets"
                    and r["holding_period"] == "long_term"]
        total_gain = sum(r["gain_amount"] or 0 for r in lt_rows)
        assert total_gain > 1e9, "LT gains should be in dollars, not thousands"

    def test_long_term_duration_bins(self, soca_2015_file):
        rows = parse_soca_t4(soca_2015_file, 2015)
        lt_all = [r for r in rows
                  if r["asset_type"] == "all_assets"
                  and r["holding_period"] == "long_term"]
        # Should have at least 9 duration bins + "Period not determinable"
        assert len(lt_all) >= 10

        # Check classifiable gains sum to a reasonable amount
        classifiable = [r for r in lt_all
                        if classify_holding_duration(r["holding_duration"]) is not None]
        assert len(classifiable) >= 9

    def test_load_into_db(self, db, soca_2015_file):
        count = load_soca_t4(db, soca_2015_file, 2015)
        assert count > 0

        result = db.execute(
            "SELECT COUNT(*) AS n FROM raw_soca_t4 WHERE year = 2015"
        ).fetchone()
        assert result["n"] == count


class TestExtendedTable14a:
    """Test extended Table 1.4A parsing with loss/carryover columns."""

    @pytest.mark.parametrize("year,prefix", [
        (2018, "18"), (2019, "19"), (2020, "20"), (2021, "21"), (2022, "22"),
    ])
    def test_loss_columns_present(self, year, prefix):
        filepath = Path("data/raw") / str(year) / f"{prefix}in14acg.xls"
        if not filepath.exists():
            pytest.skip(f"Data file for {year} not downloaded")
        df = parse_capital_gains(filepath, year)

        for col in ["short_term_loss", "long_term_loss", "total_loss",
                     "st_loss_carryover", "lt_loss_carryover"]:
            assert col in df.columns, f"Missing column: {col}"

    @pytest.mark.parametrize("year,prefix", [
        (2018, "18"), (2019, "19"), (2020, "20"), (2021, "21"), (2022, "22"),
    ])
    def test_loss_values_nonnegative(self, year, prefix):
        filepath = Path("data/raw") / str(year) / f"{prefix}in14acg.xls"
        if not filepath.exists():
            pytest.skip(f"Data file for {year} not downloaded")
        df = parse_capital_gains(filepath, year)

        # Losses should be non-negative (they are absolute amounts)
        for col in ["long_term_loss", "lt_loss_carryover"]:
            vals = df[col].dropna()
            assert (vals >= 0).all(), f"{col} has negative values in TY{year}"

    def test_lt_loss_in_dollars(self):
        filepath = Path("data/raw/2022/22in14acg.xls")
        if not filepath.exists():
            pytest.skip("2022 data not downloaded")
        df = parse_capital_gains(filepath, 2022)

        # Top bin LT losses should be in millions/billions
        top = df[df["agi_bin_id"] == 19].iloc[0]
        assert top["long_term_loss"] > 1e6, "LT loss should be in dollars"

    def test_load_extended_into_db(self, db):
        filepath = Path("data/raw/2020/20in14acg.xls")
        if not filepath.exists():
            pytest.skip("2020 data not downloaded")
        count = load_table_14a(db, filepath, 2020)
        assert count == 19

        row = db.execute("""
            SELECT long_term_loss, lt_loss_carryover
            FROM raw_table_14a
            WHERE year = 2020 AND agi_bin_id = 19
        """).fetchone()
        assert row["long_term_loss"] is not None
        assert row["long_term_loss"] > 0


class TestCalibrationValidation:
    """Test the calibration validation module."""

    def _load_test_data(self, db):
        """Insert test data for calibration computation."""
        load_agi_bins(db)
        # Insert Table 1.4A data with loss columns for all 19 bins
        for bin_id in range(1, 20):
            insert_rows(db, "raw_table_14a", [{
                "year": 2022,
                "agi_bin_id": bin_id,
                "schedule_d_count": 1000,
                "short_term_gain": 100_000,
                "short_term_loss": 50_000,
                "long_term_gain": 500_000,
                "long_term_loss": 100_000,
                "total_gain": 600_000,
                "total_loss": 20_000,
                "st_loss_carryover": 10_000,
                "lt_loss_carryover": 50_000,
            }])
        db.commit()

    def _load_soca_test_data(self, db):
        """Insert test SOCA Table 4 data."""
        insert_rows(db, "raw_soca_t4", [
            {"year": 2015, "asset_type": "all_assets", "holding_period": "long_term",
             "holding_duration": "Under 18 months [3]",
             "number_of_gain_transactions": 100, "gross_sales_price": 1000,
             "cost_basis": 800, "gain_amount": 200_000, "loss_amount": 50_000,
             "net_gain_loss": 150_000},
            {"year": 2015, "asset_type": "all_assets", "holding_period": "long_term",
             "holding_duration": "18 months under 2 years",
             "number_of_gain_transactions": 80, "gross_sales_price": 800,
             "cost_basis": 600, "gain_amount": 150_000, "loss_amount": 30_000,
             "net_gain_loss": 120_000},
            {"year": 2015, "asset_type": "all_assets", "holding_period": "long_term",
             "holding_duration": "5 years under 10 years",
             "number_of_gain_transactions": 200, "gross_sales_price": 5000,
             "cost_basis": 3000, "gain_amount": 500_000, "loss_amount": 100_000,
             "net_gain_loss": 400_000},
            {"year": 2015, "asset_type": "all_assets", "holding_period": "long_term",
             "holding_duration": "20 years or more",
             "number_of_gain_transactions": 50, "gross_sales_price": 2000,
             "cost_basis": 500, "gain_amount": 300_000, "loss_amount": 20_000,
             "net_gain_loss": 280_000},
            {"year": 2015, "asset_type": "all_assets", "holding_period": "long_term",
             "holding_duration": "Period not determinable",
             "number_of_gain_transactions": 500, "gross_sales_price": 10000,
             "cost_basis": 8000, "gain_amount": 1_000_000, "loss_amount": 200_000,
             "net_gain_loss": 800_000},
        ])
        db.commit()

    def test_loss_ratio_computation(self, db):
        self._load_test_data(db)
        results = compute_empirical_loss_ratios(db, 2022)
        assert len(results) == 19

        # Check ratio formula: (lt_loss + lt_carry) / (lt_gain + lt_loss + lt_carry)
        # = (100K + 50K) / (500K + 100K + 50K) = 150K / 650K ≈ 0.2308
        for r in results:
            assert r["empirical_ratio"] == pytest.approx(150_000 / 650_000, rel=1e-4)

    def test_holding_fraction_computation(self, db):
        self._load_soca_test_data(db)
        result = compute_empirical_holding_fractions(db, 2015)

        # 1-5yr: Under 18m (200K) + 18m-2yr (150K) = 350K
        # 5+yr: 5-10yr (500K) + 20yr+ (300K) = 800K
        # fraction = 350K / (350K + 800K) = 350/1150 ≈ 0.3043
        assert result["gains_1_to_5yr"] == 350_000
        assert result["gains_5yr_plus"] == 800_000
        assert result["fraction"] == pytest.approx(350_000 / 1_150_000, rel=1e-4)

    def test_validate_calibration_report(self, db):
        self._load_test_data(db)
        self._load_soca_test_data(db)
        report = validate_calibration(db, years=[2022], soca_year=2015)
        assert len(report.rows) > 0

        # Should have both loss_ratio and holding_fraction rows
        metrics = {r.metric for r in report.rows}
        assert "loss_ratio" in metrics
        assert "holding_fraction" in metrics

        # Summary should be printable
        summary = report.summary()
        assert "LOSS HARVESTING" in summary
        assert "HOLDING PERIOD" in summary


class TestUrlRegistry:
    """Test SOCA URL registry additions."""

    def test_soca_years_defined(self):
        from src.etl.url_registry import SOCA_YEARS
        assert SOCA_YEARS == [2013, 2014, 2015]

    def test_get_soca_files(self):
        from src.etl.url_registry import get_soca_files
        files = get_soca_files(2015)
        assert "soca_t4" in files
        assert files["soca_t4"]["filename"] == "15in04soca.xlsx"
        assert "irs-soi" in files["soca_t4"]["url"]

    def test_get_all_soca_downloads(self):
        from src.etl.url_registry import get_all_soca_downloads
        downloads = get_all_soca_downloads()
        assert len(downloads) > 0
        # Should have table entries for each year
        years = {d["year"] for d in downloads}
        assert years == {2013, 2014, 2015}
