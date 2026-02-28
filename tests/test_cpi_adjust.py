"""Tests for CPI adjustment logic."""

import pytest
import pandas as pd

from src.etl.cpi_adjust import get_adjustment_factor, adjust_dataframe, CPI_U_ANNUAL


class TestAdjustmentFactor:
    def test_2022_is_unity(self):
        assert get_adjustment_factor(2022) == 1.0

    def test_2020_inflates(self):
        factor = get_adjustment_factor(2020)
        assert factor > 1.0
        expected = CPI_U_ANNUAL[2022] / CPI_U_ANNUAL[2020]
        assert abs(factor - expected) < 1e-10

    def test_2021_inflates(self):
        factor = get_adjustment_factor(2021)
        assert factor > 1.0
        assert factor < get_adjustment_factor(2020)  # 2021 closer to 2022

    def test_invalid_year(self):
        with pytest.raises(ValueError):
            get_adjustment_factor(2019)


class TestAdjustDataframe:
    def test_basic_adjustment(self):
        df = pd.DataFrame({
            "year": [2020, 2021, 2022],
            "total_agi": [100_000, 100_000, 100_000],
            "return_count": [10, 10, 10],
        })
        result = adjust_dataframe(df, money_columns=["total_agi"])

        # 2020 and 2021 values should be inflated; 2022 unchanged
        assert result.loc[2, "total_agi"] == 100_000
        assert result.loc[0, "total_agi"] > 100_000
        assert result.loc[1, "total_agi"] > 100_000

        # Non-money column unchanged
        assert list(result["return_count"]) == [10, 10, 10]

    def test_none_values_preserved(self):
        df = pd.DataFrame({
            "year": [2020],
            "total_agi": [None],
        })
        result = adjust_dataframe(df, money_columns=["total_agi"])
        assert pd.isna(result.loc[0, "total_agi"])
