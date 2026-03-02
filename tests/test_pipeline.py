"""Tests for the ETL pipeline orchestrator."""

import pytest

from src.etl.pipeline import run_pipeline, _write_parquet, RAW_DIR, _NOMINAL_EXPORTS, _REAL_EXPORTS
from src.etl.db import get_connection, init_schema


class TestWriteParquet:
    """Test the Parquet writing helper."""

    def test_write_creates_file(self, tmp_path):
        import pandas as pd
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        _write_parquet(df, "test_table", tmp_path)
        assert (tmp_path / "test_table.parquet").exists()

    def test_write_creates_subdirectory(self, tmp_path):
        import pandas as pd
        df = pd.DataFrame({"x": [10]})
        nested = tmp_path / "sub" / "dir"
        _write_parquet(df, "nested", nested)
        assert (nested / "nested.parquet").exists()

    def test_roundtrip_preserves_data(self, tmp_path):
        import pandas as pd
        df = pd.DataFrame({"year": [2020, 2021], "value": [100.5, 200.3]})
        _write_parquet(df, "roundtrip", tmp_path)
        df2 = pd.read_parquet(tmp_path / "roundtrip.parquet")
        assert list(df2["year"]) == [2020, 2021]
        assert list(df2["value"]) == pytest.approx([100.5, 200.3])


class TestRunPipeline:
    """Integration test using real data for a single year."""

    def test_single_year_pipeline(self, tmp_path):
        """Run the full pipeline for 2020 and verify outputs."""
        db_path = tmp_path / "test_pipeline.db"
        run_pipeline(years=[2020], db_path=db_path)

        conn = get_connection(db_path)
        try:
            # Verify raw tables have data
            for table in ["raw_table_11", "raw_table_12", "raw_table_14a",
                          "raw_table_14", "raw_table_32", "raw_table_33",
                          "raw_table_34", "raw_table_36"]:
                count = conn.execute(
                    f"SELECT COUNT(*) AS n FROM {table} WHERE year = 2020"
                ).fetchone()["n"]
                assert count > 0, f"{table} has no rows for 2020"

            # Verify views produce results
            for view, _, _ in _NOMINAL_EXPORTS:
                rows = conn.execute(f"SELECT COUNT(*) AS n FROM {view}").fetchone()["n"]
                assert rows > 0, f"View {view} is empty"
        finally:
            conn.close()

    def test_pipeline_idempotent(self, tmp_path):
        """Running pipeline twice for same year should produce same results."""
        db_path = tmp_path / "test_idempotent.db"
        run_pipeline(years=[2020], db_path=db_path)

        conn = get_connection(db_path)
        count1 = conn.execute(
            "SELECT COUNT(*) AS n FROM raw_table_11 WHERE year = 2020"
        ).fetchone()["n"]
        conn.close()

        # Run again
        run_pipeline(years=[2020], db_path=db_path)

        conn = get_connection(db_path)
        count2 = conn.execute(
            "SELECT COUNT(*) AS n FROM raw_table_11 WHERE year = 2020"
        ).fetchone()["n"]
        conn.close()

        assert count1 == count2, "Idempotent run changed row counts"

    def test_export_config_coverage(self):
        """All export configs should have valid view names."""
        all_exports = _NOMINAL_EXPORTS + _REAL_EXPORTS
        assert len(all_exports) == 7
        view_names = {e[0] for e in all_exports}
        assert "v_returns_aggregate" in view_names
        assert "v_capital_gains" in view_names
        assert "v_bracket_distribution" in view_names
