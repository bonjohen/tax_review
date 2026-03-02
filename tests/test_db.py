"""Tests for the SQLite database layer (db.py + schema.sql)."""

import sqlite3

import pytest

from src.etl.db import (
    get_connection,
    init_schema,
    insert_rows,
    load_agi_bins,
    load_cpi_factors,
    reset_year,
)


class TestSchema:
    """Verify schema creation produces expected tables and views."""

    def test_raw_tables_exist(self, db):
        rows = db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        names = [r["name"] for r in rows]
        expected = [
            "agi_bins", "cpi_factors",
            "raw_table_11", "raw_table_12", "raw_table_14a",
            "raw_table_32", "raw_table_33", "raw_table_34", "raw_table_36",
        ]
        for t in expected:
            assert t in names, f"Missing table: {t}"

    def test_views_exist(self, db):
        rows = db.execute(
            "SELECT name FROM sqlite_master WHERE type='view' ORDER BY name"
        ).fetchall()
        names = [r["name"] for r in rows]
        expected_views = [
            "v_agi_bins",
            "v_returns_aggregate", "v_capital_gains", "v_bracket_distribution",
            "v_returns_aggregate_real2022", "v_capital_gains_real2022",
            "v_bracket_distribution_real2022",
            "v_check_agi", "v_check_income_tax", "v_check_income_tax_33",
            "v_check_bracket_tax", "v_check_return_counts",
            "v_check_filing_status", "v_check_capital_gains",
        ]
        for v in expected_views:
            assert v in names, f"Missing view: {v}"

    def test_idempotent_init(self, db):
        """init_schema can be called multiple times without error."""
        init_schema(db)
        init_schema(db)
        count = db.execute(
            "SELECT COUNT(*) AS n FROM sqlite_master WHERE type IN ('table','view')"
        ).fetchone()["n"]
        assert count > 0


class TestReferenceTables:
    """Verify reference table loading."""

    def test_load_agi_bins(self, db):
        load_agi_bins(db)
        count = db.execute("SELECT COUNT(*) AS n FROM agi_bins").fetchone()["n"]
        assert count == 19

    def test_load_agi_bins_idempotent(self, db):
        load_agi_bins(db)
        load_agi_bins(db)
        count = db.execute("SELECT COUNT(*) AS n FROM agi_bins").fetchone()["n"]
        assert count == 19

    def test_agi_bin_values(self, db):
        load_agi_bins(db)
        row = db.execute(
            "SELECT * FROM agi_bins WHERE agi_bin_id = 1"
        ).fetchone()
        assert row["label"] == "No adjusted gross income"

    def test_load_cpi_factors(self, db):
        load_cpi_factors(db)
        count = db.execute("SELECT COUNT(*) AS n FROM cpi_factors").fetchone()["n"]
        assert count == 6

    def test_cpi_2022_factor_is_unity(self, db):
        load_cpi_factors(db)
        row = db.execute(
            "SELECT factor_to_2022 FROM cpi_factors WHERE year = 2022"
        ).fetchone()
        assert abs(row["factor_to_2022"] - 1.0) < 1e-10


class TestInsertAndQuery:
    """Verify insert_rows and basic querying."""

    def test_insert_rows(self, db):
        rows = [
            {"year": 2020, "agi_bin_id": 1, "filing_status": "all",
             "return_count": 100, "total_agi": 50000, "total_taxable_income": 40000,
             "total_income_tax": 10000},
            {"year": 2020, "agi_bin_id": 2, "filing_status": "all",
             "return_count": 200, "total_agi": 150000, "total_taxable_income": 120000,
             "total_income_tax": 30000},
        ]
        count = insert_rows(db, "raw_table_11", rows)
        assert count == 2

        result = db.execute("SELECT COUNT(*) AS n FROM raw_table_11").fetchone()
        assert result["n"] == 2

    def test_insert_replaces_on_conflict(self, db):
        row = {"year": 2020, "agi_bin_id": 1, "filing_status": "all",
               "return_count": 100, "total_agi": 50000,
               "total_taxable_income": 40000, "total_income_tax": 10000}
        insert_rows(db, "raw_table_11", [row])

        row["return_count"] = 999
        insert_rows(db, "raw_table_11", [row])

        result = db.execute(
            "SELECT return_count FROM raw_table_11 WHERE year=2020 AND agi_bin_id=1"
        ).fetchone()
        assert result["return_count"] == 999

    def test_insert_empty_list(self, db):
        count = insert_rows(db, "raw_table_11", [])
        assert count == 0


class TestResetYear:
    """Verify reset_year clears only the targeted year."""

    def test_reset_clears_year(self, db):
        rows_2020 = [
            {"year": 2020, "agi_bin_id": 1, "filing_status": "all",
             "return_count": 100, "total_agi": 50000,
             "total_taxable_income": 40000, "total_income_tax": 10000},
        ]
        rows_2021 = [
            {"year": 2021, "agi_bin_id": 1, "filing_status": "all",
             "return_count": 200, "total_agi": 80000,
             "total_taxable_income": 60000, "total_income_tax": 15000},
        ]
        insert_rows(db, "raw_table_11", rows_2020)
        insert_rows(db, "raw_table_11", rows_2021)
        db.commit()

        reset_year(db, 2020)

        count_2020 = db.execute(
            "SELECT COUNT(*) AS n FROM raw_table_11 WHERE year=2020"
        ).fetchone()["n"]
        count_2021 = db.execute(
            "SELECT COUNT(*) AS n FROM raw_table_11 WHERE year=2021"
        ).fetchone()["n"]
        assert count_2020 == 0
        assert count_2021 == 1

    def test_reset_across_tables(self, db):
        insert_rows(db, "raw_table_11", [
            {"year": 2020, "agi_bin_id": 1, "filing_status": "all",
             "return_count": 100, "total_agi": 50000,
             "total_taxable_income": 40000, "total_income_tax": 10000},
        ])
        insert_rows(db, "raw_table_12", [
            {"year": 2020, "agi_bin_id": 1, "filing_status": "all",
             "return_count": 100, "total_agi": 50000,
             "total_taxable_income": 40000, "total_income_tax": 10000},
        ])
        db.commit()

        reset_year(db, 2020)

        for table in ["raw_table_11", "raw_table_12"]:
            count = db.execute(
                f"SELECT COUNT(*) AS n FROM {table} WHERE year=2020"
            ).fetchone()["n"]
            assert count == 0, f"{table} still has rows after reset"


class TestViews:
    """Verify canonical views produce correct results from test data."""

    def _load_test_data(self, db):
        """Insert minimal test data for view testing."""
        load_agi_bins(db)
        load_cpi_factors(db)

        insert_rows(db, "raw_table_12", [
            {"year": 2020, "agi_bin_id": 1, "filing_status": "all",
             "return_count": 1000, "total_agi": 5_000_000,
             "total_taxable_income": 4_000_000, "total_income_tax": 800_000},
        ])
        insert_rows(db, "raw_table_33", [
            {"year": 2020, "agi_bin_id": 1, "filing_status": "all",
             "return_count": 1000, "total_credits": 50_000,
             "total_income_tax": 800_000},
        ])
        insert_rows(db, "raw_table_32", [
            {"year": 2020, "agi_bin_id": 1, "filing_status": "all",
             "return_count": 1000, "total_agi": 5_000_000,
             "total_income_tax": 800_000, "effective_tax_rate": 0.16},
        ])
        insert_rows(db, "raw_table_14a", [
            {"year": 2020, "agi_bin_id": 1,
             "schedule_d_count": 500, "short_term_gain": 100_000,
             "long_term_gain": 400_000, "total_gain": 500_000},
        ])
        insert_rows(db, "raw_table_36", [
            {"year": 2020, "filing_status": "all", "marginal_rate": 0.10,
             "bracket_return_count": 500, "bracket_taxable_income": 1_000_000,
             "bracket_tax": 100_000},
        ])
        db.commit()

    def test_v_returns_aggregate(self, db):
        self._load_test_data(db)
        row = db.execute("SELECT * FROM v_returns_aggregate").fetchone()
        assert row["total_credits"] == 50_000
        assert row["effective_tax_rate"] == pytest.approx(0.16)
        assert row["total_income_tax"] == 800_000

    def test_v_capital_gains(self, db):
        self._load_test_data(db)
        row = db.execute("SELECT * FROM v_capital_gains").fetchone()
        assert row["short_term_gain"] == 100_000
        assert row["long_term_gain"] == 400_000

    def test_v_bracket_distribution(self, db):
        self._load_test_data(db)
        row = db.execute("SELECT * FROM v_bracket_distribution").fetchone()
        assert row["marginal_rate"] == pytest.approx(0.10)
        assert row["bracket_tax"] == 100_000

    def test_v_returns_aggregate_real2022(self, db):
        self._load_test_data(db)
        row = db.execute("SELECT * FROM v_returns_aggregate_real2022").fetchone()
        # 2020 factor = 292.655 / 258.811 ≈ 1.1308
        factor = 292.655 / 258.811
        assert row["total_agi"] == pytest.approx(5_000_000 * factor, rel=1e-4)
        assert row["return_count"] == 1000  # counts not adjusted

    def test_v_agi_bins(self, db):
        self._load_test_data(db)
        rows = db.execute("SELECT * FROM v_agi_bins").fetchall()
        assert len(rows) == 19  # 19 bins × 1 year

    def test_v_check_capital_gains(self, db):
        self._load_test_data(db)
        row = db.execute(
            "SELECT * FROM v_check_capital_gains WHERE year = 2020"
        ).fetchone()
        assert row["st_plus_lt"] == 500_000
        assert row["total"] == 500_000
