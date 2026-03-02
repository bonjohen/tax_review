"""Tests for TY2023 forecast generation."""

import pytest
import numpy as np

from src.forecast.generate_2023 import (
    _trend_project,
    HISTORICAL_YEARS,
    FORECAST_YEAR,
)


class TestTrendProject:
    """Unit tests for the linear regression projection."""

    def test_linear_trend(self):
        """Perfect linear sequence should extrapolate correctly."""
        # y = 100 + 10*x, projected from 2018-2022 to 2023
        values = [100 + 10 * (y - 2018) for y in HISTORICAL_YEARS]
        # values = [100, 110, 120, 130, 140]
        projected = _trend_project(values)
        assert projected == pytest.approx(150.0, abs=0.1)

    def test_all_zeros(self):
        """All-zero input should project to zero."""
        result = _trend_project([0, 0, 0, 0, 0])
        assert result == 0.0

    def test_constant_values(self):
        """Constant values should project to the same constant."""
        result = _trend_project([500, 500, 500, 500, 500])
        assert result == pytest.approx(500.0, abs=1.0)

    def test_nan_replaced_with_zero(self):
        """NaN values should be treated as zero."""
        result = _trend_project([100, float("nan"), 100, float("nan"), 100])
        # With 3 values at 100 and 2 at 0, regression still works
        assert isinstance(result, float)
        assert result >= 0

    def test_none_replaced_with_zero(self):
        """None values should be treated as zero (via nan_to_num)."""
        values = [100, None, 200, None, 300]
        result = _trend_project(values)
        assert isinstance(result, float)
        assert result >= 0

    def test_declining_trend_floored_at_zero(self):
        """Declining trend projected below zero should be floored at 0."""
        values = [100, 80, 60, 40, 20]
        result = _trend_project(values)
        # Trend is -20/year, so 2023 would be 0
        assert result == pytest.approx(0.0, abs=1.0)

    def test_steeply_declining_floors_at_zero(self):
        """Very steep decline should not produce negative values."""
        values = [1000, 500, 200, 50, 10]
        result = _trend_project(values)
        assert result >= 0

    def test_forecast_year_is_2023(self):
        assert FORECAST_YEAR == 2023
        assert len(HISTORICAL_YEARS) == 5


class TestGenerateForecast:
    """Integration test for the full forecast pipeline."""

    def test_forecast_inserts_rows(self):
        """Running the forecast should insert rows for 2023."""
        from src.forecast.generate_2023 import generate_forecast
        total = generate_forecast()

        # 95 (table_12) + 19 (table_14a) + 19 (table_14) + 35 (table_36) = 168
        assert total == 168
