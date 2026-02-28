"""Tests for validation/reconciliation logic."""

from src.validation.reconcile import _pct_variance, _check, TOLERANCE


class TestPctVariance:
    def test_identical(self):
        assert _pct_variance(100, 100) == 0.0

    def test_both_zero(self):
        assert _pct_variance(0, 0) == 0.0

    def test_small_variance(self):
        var = _pct_variance(100_000, 100_040)
        assert var < TOLERANCE

    def test_large_variance(self):
        var = _pct_variance(100_000, 101_000)
        assert var > TOLERANCE


class TestCheck:
    def test_passing_check(self):
        result = _check("test", 2020, "A", "B", 100_000, 100_000)
        assert result.passed is True
        assert result.status == "PASS"

    def test_failing_check(self):
        result = _check("test", 2020, "A", "B", 100_000, 110_000)
        assert result.passed is False
        assert result.status == "FAIL"
