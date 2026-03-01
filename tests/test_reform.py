"""Tests for reform model modules: assumptions, marginal_rate, filing_status."""

import json
import math
import sqlite3
from pathlib import Path

import pytest

from src.reform.assumptions import (
    ALL_BIN_IDS,
    load_assumptions,
    get_holding_period_fraction,
    get_gross_loss_ratio,
)
from src.reform.marginal_rate import (
    load_bracket_thresholds,
    marginal_rate_for_income,
    compute_weighted_marginal_rate,
)
from src.reform.filing_status import (
    compute_agi_shares,
    allocate_gains_by_filing_status,
    compute_reform_by_filing_status,
    FILING_STATUSES,
)

PARAMS_DIR = Path(__file__).resolve().parents[1] / "data" / "parameters"


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def assumptions():
    return load_assumptions()


@pytest.fixture
def brackets_2022():
    return load_bracket_thresholds(2022)


@pytest.fixture
def mock_db():
    """In-memory SQLite with minimal raw_table_12 data for testing."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE raw_table_12 (
            year INTEGER,
            agi_bin_id INTEGER,
            filing_status TEXT,
            return_count REAL,
            total_agi REAL,
            total_taxable_income REAL,
            total_income_tax REAL,
            PRIMARY KEY (year, agi_bin_id, filing_status)
        )
    """)
    # Bin 13 ($200K-$500K) — 2022 data
    test_data = [
        # (year, bin_id, status, returns, agi, taxable_income, tax)
        (2022, 13, "single",                    2_000_000, 500e9, 400e9, 100e9),
        (2022, 13, "married_filing_jointly",     3_000_000, 800e9, 650e9, 150e9),
        (2022, 13, "married_filing_separately",    200_000,  50e9,  40e9,  10e9),
        (2022, 13, "head_of_household",            800_000, 150e9, 120e9,  30e9),
        (2022, 13, "all",                        6_000_000, 1500e9, 1210e9, 290e9),
        # Bin 1 (No AGI) — minimal data
        (2022, 1, "single",         100_000,  -5e9, 0, 0),
        (2022, 1, "married_filing_jointly", 50_000, -3e9, 0, 0),
        (2022, 1, "married_filing_separately", 10_000, -1e9, 0, 0),
        (2022, 1, "head_of_household", 20_000, -1e9, 0, 0),
        (2022, 1, "all",            180_000, -10e9, 0, 0),
    ]
    conn.executemany(
        "INSERT INTO raw_table_12 VALUES (?,?,?,?,?,?,?)",
        test_data,
    )
    conn.commit()
    yield conn
    conn.close()


# ── Assumptions tests ─────────────────────────────────────────────────────────

class TestAssumptions:

    def test_loads_valid_json(self, assumptions):
        assert "holding_period_lt_under_5yr" in assumptions
        assert "gross_loss_ratio" in assumptions

    def test_all_19_bins_covered_holding_period(self, assumptions):
        covered = set()
        for group in assumptions["holding_period_lt_under_5yr"]["by_agi_group"]:
            covered.update(group["agi_bin_ids"])
        assert covered == ALL_BIN_IDS

    def test_all_19_bins_covered_gross_loss(self, assumptions):
        covered = set()
        for group in assumptions["gross_loss_ratio"]["by_agi_group"]:
            covered.update(group["agi_bin_ids"])
        assert covered == ALL_BIN_IDS

    def test_fractions_in_range(self, assumptions):
        for group in assumptions["holding_period_lt_under_5yr"]["by_agi_group"]:
            assert 0 <= group["fraction"] <= 1

    def test_ratios_in_range(self, assumptions):
        for group in assumptions["gross_loss_ratio"]["by_agi_group"]:
            assert 0 <= group["ratio"] <= 1

    def test_get_holding_period_fraction(self, assumptions):
        # Low bin (1) should be 0.55
        assert get_holding_period_fraction(assumptions, 1) == 0.55
        # Top bin (19) should be 0.20
        assert get_holding_period_fraction(assumptions, 19) == 0.20
        # Mid bin (12) should be 0.40
        assert get_holding_period_fraction(assumptions, 12) == 0.40

    def test_get_gross_loss_ratio(self, assumptions):
        # Low bins -> 0.70 (less active loss harvesting)
        assert get_gross_loss_ratio(assumptions, 1) == 0.70
        # Top bins -> 0.90 (aggressive tax-loss harvesting)
        assert get_gross_loss_ratio(assumptions, 19) == 0.90

    def test_invalid_fraction_rejected(self, tmp_path):
        bad = {
            "holding_period_lt_under_5yr": {
                "by_agi_group": [
                    {"agi_bin_ids": list(range(1, 20)), "fraction": 1.5}
                ]
            },
            "gross_loss_ratio": {
                "default": 0.5,
                "by_agi_group": [
                    {"agi_bin_ids": list(range(1, 20)), "ratio": 0.5}
                ]
            },
        }
        path = tmp_path / "bad.json"
        path.write_text(json.dumps(bad))
        with pytest.raises(ValueError, match="out of range"):
            load_assumptions(path)


# ── Marginal rate tests ───────────────────────────────────────────────────────

class TestMarginalRate:

    def test_load_brackets_all_years(self):
        for year in [2020, 2021, 2022]:
            brackets = load_bracket_thresholds(year)
            assert "single" in brackets
            assert "married_filing_jointly" in brackets
            assert len(brackets["single"]) == 7  # 7 brackets

    def test_single_300k_2022(self, brackets_2022):
        rate = marginal_rate_for_income(brackets_2022["single"], 300_000)
        assert rate == 0.35

    def test_mfj_100k_2022(self, brackets_2022):
        rate = marginal_rate_for_income(
            brackets_2022["married_filing_jointly"], 100_000
        )
        assert rate == 0.22

    def test_single_10k_is_12pct(self, brackets_2022):
        rate = marginal_rate_for_income(brackets_2022["single"], 15_000)
        assert rate == 0.12

    def test_top_bracket_single(self, brackets_2022):
        rate = marginal_rate_for_income(brackets_2022["single"], 1_000_000)
        assert rate == 0.37

    def test_zero_income(self, brackets_2022):
        rate = marginal_rate_for_income(brackets_2022["single"], 0)
        assert rate == 0.10

    def test_negative_income(self, brackets_2022):
        rate = marginal_rate_for_income(brackets_2022["single"], -5000)
        assert rate == 0.10

    def test_weighted_marginal_rate(self, mock_db, brackets_2022):
        """Bin 13 ($200-500K) should have a weighted marginal rate around 24-35%."""
        rate = compute_weighted_marginal_rate(
            mock_db, 2022, 13, brackets_2022
        )
        # Average taxable per filing status:
        # single: 400e9/2e6 = 200K -> 32%
        # MFJ: 650e9/3e6 = ~216K -> 24% (MFJ threshold is higher)
        # MFS: 40e9/200K = 200K -> 32%
        # HoH: 120e9/800K = 150K -> 24%
        assert 0.20 < rate < 0.40

    def test_weighted_marginal_rate_no_income_bin(self, mock_db, brackets_2022):
        """Bin 1 (no AGI) with zero taxable income should return 0.10 or similar."""
        rate = compute_weighted_marginal_rate(
            mock_db, 2022, 1, brackets_2022
        )
        assert rate == 0.10


# ── Filing status tests ──────────────────────────────────────────────────────

class TestFilingStatus:

    def test_agi_shares_sum_to_one(self, mock_db):
        shares = compute_agi_shares(mock_db, 2022, 13)
        total = sum(shares.values())
        assert abs(total - 1.0) < 0.001

    def test_agi_shares_has_all_statuses(self, mock_db):
        shares = compute_agi_shares(mock_db, 2022, 13)
        for fs in FILING_STATUSES:
            assert fs in shares

    def test_mfj_largest_share(self, mock_db):
        """MFJ has the most AGI ($800B) in our test data."""
        shares = compute_agi_shares(mock_db, 2022, 13)
        assert shares["married_filing_jointly"] > shares["single"]

    def test_allocate_gains_sums_to_total(self):
        shares = {
            "single": 0.30,
            "married_filing_jointly": 0.50,
            "married_filing_separately": 0.05,
            "head_of_household": 0.15,
        }
        allocated = allocate_gains_by_filing_status(1_000_000, shares)
        total = sum(allocated.values())
        assert abs(total - 1_000_000) < 1

    def test_compute_reform_by_filing_status(self, mock_db):
        reform_rows = [{
            "agi_bin_id": 13,
            "label": "$200,000 under $500,000",
            "net_additional_revenue": 50_000_000_000,
        }]
        results = compute_reform_by_filing_status(mock_db, 2022, reform_rows)
        assert len(results) == len(FILING_STATUSES)
        total_allocated = sum(r["allocated_revenue"] for r in results)
        assert abs(total_allocated - 50_000_000_000) < len(FILING_STATUSES)  # rounding

    def test_negative_agi_bin_shares(self, mock_db):
        """Bin 1 has negative AGI — shares should use abs values."""
        shares = compute_agi_shares(mock_db, 2022, 1)
        total = sum(shares.values())
        assert abs(total - 1.0) < 0.001


# ── Loss adjustment tests ────────────────────────────────────────────────────

class TestLossAdjustment:
    """Test the loss offset calculation logic (will be in export_data.py)."""

    def test_cross_category_netting(self):
        """(st + lt) - total_gain = cross-category offset."""
        st_gain = 208.5e9
        lt_gain = 1922.4e9
        total_gain = 2048.8e9
        loss_offset = (st_gain + lt_gain) - total_gain
        assert abs(loss_offset - 82.1e9) < 0.1e9

    def test_lt_share_allocation(self):
        """Loss offset allocated proportionally to LT share."""
        st_gain = 208.5e9
        lt_gain = 1922.4e9
        total_gain = 2048.8e9
        loss_offset = (st_gain + lt_gain) - total_gain
        lt_share = lt_gain / (st_gain + lt_gain)
        adjusted_lt = lt_gain - (loss_offset * lt_share)
        assert adjusted_lt < lt_gain
        assert adjusted_lt > 0

    def test_stranded_loss_revenue(self):
        """Stranded losses INCREASE revenue: gross gains taxed at ordinary rates,
        but losses can only offset $3K/year of ordinary income."""
        net_lt = 1850e9
        gross_loss_ratio = 0.50
        # Gross gains = net / (1 - ratio) = 1850 / 0.5 = 3700
        gross_lt = net_lt / (1 - gross_loss_ratio)
        estimated_losses = gross_lt - net_lt  # 1850
        assert estimated_losses == net_lt  # ratio 0.5 means losses = net gains

        hp_fraction = 0.30
        marginal_rate = 0.32

        affected_gross = gross_lt * hp_fraction      # 1110
        affected_net = net_lt * hp_fraction           # 555
        affected_losses = estimated_losses * hp_fraction  # 555

        # Under reform: gross gains become ordinary income, losses stranded
        reform_tax = affected_gross * marginal_rate   # 355.2
        current_law_tax = affected_net * 0.15         # 83.25
        additional_revenue = reform_tax - current_law_tax  # 271.95

        # Revenue is MUCH larger than just the rate differential on net gains
        rate_diff_only = affected_net * (marginal_rate - 0.15)  # 94.35
        assert additional_revenue > rate_diff_only * 2  # stranded losses roughly double it

        # Stranded loss component
        stranded = affected_losses * marginal_rate  # 177.6
        assert stranded > 0
        assert abs(additional_revenue - (rate_diff_only + stranded)) < 1
