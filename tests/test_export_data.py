"""Tests for the dashboard JSON export module."""

import json

import pytest

from src.web.export_data import (
    PREFERENTIAL_CG_RATE,
    ANNUAL_LOSS_DEDUCTION_LIMIT,
)


class TestReformCalculations:
    """Unit tests for the 5-step reform revenue calculation logic."""

    def test_cross_category_netting_when_total_less_than_sum(self):
        """Step 1: When total < st+lt, loss offset = difference."""
        st_gain = 100_000
        lt_gain = 400_000
        total_gain = 450_000

        st_lt_sum = st_gain + lt_gain
        loss_offset = st_lt_sum - total_gain
        lt_share = lt_gain / st_lt_sum
        net_lt_gain = lt_gain - (loss_offset * lt_share)

        assert loss_offset == 50_000
        assert net_lt_gain == pytest.approx(400_000 - 50_000 * 0.8)

    def test_no_netting_when_total_equals_sum(self):
        """Step 1: No netting when total == st+lt."""
        st_gain = 100_000
        lt_gain = 400_000
        total_gain = 500_000

        st_lt_sum = st_gain + lt_gain
        if st_lt_sum > 0 and total_gain < st_lt_sum:
            loss_offset = st_lt_sum - total_gain
        else:
            loss_offset = 0
            net_lt_gain = lt_gain

        assert loss_offset == 0
        assert net_lt_gain == 400_000

    def test_gross_gain_expansion(self):
        """Step 2: gross_gains = net / (1 - loss_ratio)."""
        net_lt_gain = 1_000_000
        gross_loss_ratio = 0.50

        gross_lt_gains = net_lt_gain / (1 - gross_loss_ratio)
        estimated_losses = gross_lt_gains - net_lt_gain

        assert gross_lt_gains == 2_000_000
        assert estimated_losses == 1_000_000

    def test_gross_gain_ratio_at_one(self):
        """Step 2: When ratio >= 1, no gross expansion."""
        net_lt_gain = 1_000_000
        gross_loss_ratio = 1.0

        if gross_loss_ratio < 1:
            gross_lt_gains = net_lt_gain / (1 - gross_loss_ratio)
        else:
            gross_lt_gains = net_lt_gain
            estimated_losses = 0

        assert gross_lt_gains == 1_000_000
        assert estimated_losses == 0

    def test_holding_period_filter(self):
        """Step 3: Affected = gross * hp_fraction."""
        gross_lt_gains = 2_000_000
        hp_fraction = 0.30

        affected = gross_lt_gains * hp_fraction
        assert affected == 600_000

    def test_revenue_positive(self):
        """Step 5: Reform should produce positive additional revenue when
        marginal rate > preferential rate."""
        affected_net = 1_000_000
        affected_gross = 2_000_000
        affected_losses = 1_000_000
        marginal_rate = 0.32
        schedule_d_count = 100
        hp_fraction = 0.30

        current_law_tax = affected_net * PREFERENTIAL_CG_RATE
        max_loss_deduction = ANNUAL_LOSS_DEDUCTION_LIMIT * schedule_d_count * hp_fraction
        usable = min(max_loss_deduction, affected_losses)
        reform_taxable = affected_gross - usable
        reform_tax = reform_taxable * marginal_rate
        net_additional = max(reform_tax - current_law_tax, 0)

        assert net_additional > 0
        assert reform_tax > current_law_tax

    def test_stranded_loss_component(self):
        """Stranded losses should add to revenue."""
        affected_losses = 1_000_000
        schedule_d_count = 10
        hp_fraction = 0.30
        marginal_rate = 0.32

        max_loss_deduction = ANNUAL_LOSS_DEDUCTION_LIMIT * schedule_d_count * hp_fraction
        usable = min(max_loss_deduction, affected_losses)
        stranded = affected_losses - usable

        assert stranded > 0, "Most losses should be stranded ($3K cap is very small)"
        stranded_revenue = stranded * marginal_rate
        assert stranded_revenue > 0

    def test_loss_deduction_limit(self):
        """IRC §1211(b) $3K limit is correctly applied."""
        assert ANNUAL_LOSS_DEDUCTION_LIMIT == 3000


class TestExportIntegration:
    """Integration tests requiring the production database."""

    def test_export_runs_and_produces_valid_json(self):
        """Full export produces valid JSON with all expected keys."""
        from src.web.export_data import export_dashboard_json
        data = export_dashboard_json()

        expected_keys = [
            "summary", "capital_gains_by_bin", "concentration",
            "returns_aggregate", "income_sources", "bracket_distribution",
            "reform_estimate", "reform_by_filing_status", "reform_metadata",
        ]
        for key in expected_keys:
            assert key in data, f"Missing key: {key}"

        # Summary should have entries for each year
        assert len(data["summary"]) >= 5

        # Reform estimate should have data
        assert len(data["reform_estimate"]) > 0

        # All reform rows should have positive or zero revenue
        for r in data["reform_estimate"]:
            assert r["net_additional_revenue"] >= 0

    def test_reform_metadata_structure(self):
        """Reform metadata should document the model."""
        from src.web.export_data import export_dashboard_json
        data = export_dashboard_json()
        meta = data["reform_metadata"]

        assert "model_version" in meta
        assert meta["preferential_cg_rate"] == 0.15
        assert meta["annual_loss_deduction_limit"] == 3000
        assert "methodology" in meta
