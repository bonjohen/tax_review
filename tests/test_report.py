"""Tests for reconciliation report generation."""

import csv

import pytest

from src.validation.reconcile import ValidationResult, ReconciliationReport
from src.validation.report import write_text_report, write_csv_report


@pytest.fixture
def sample_report():
    """Create a ReconciliationReport with sample results."""
    report = ReconciliationReport()
    report.add(ValidationResult(
        check_name="AGI Check",
        year=2022,
        table_a="Table 1.1",
        table_b="Table 1.2",
        value_a=10_000_000.0,
        value_b=10_005_000.0,
        variance_pct=0.0005,
        passed=True,
    ))
    report.add(ValidationResult(
        check_name="Tax Check",
        year=2022,
        table_a="Table 1.1",
        table_b="Table 3.3",
        value_a=5_000_000.0,
        value_b=5_100_000.0,
        variance_pct=0.02,
        passed=False,
    ))
    return report


class TestWriteTextReport:
    def test_creates_file(self, tmp_path, sample_report):
        path = write_text_report(sample_report, tmp_path / "report.txt")
        assert path.exists()
        content = path.read_text()
        assert "Reconciliation Report" in content
        assert "PASS" in content
        assert "FAIL" in content

    def test_creates_parent_directory(self, tmp_path, sample_report):
        nested = tmp_path / "sub" / "dir" / "report.txt"
        path = write_text_report(sample_report, nested)
        assert path.exists()

    def test_empty_report(self, tmp_path):
        report = ReconciliationReport()
        path = write_text_report(report, tmp_path / "empty.txt")
        content = path.read_text()
        assert "0/0 checks passed" in content


class TestWriteCsvReport:
    def test_creates_file(self, tmp_path, sample_report):
        path = write_csv_report(sample_report, tmp_path / "report.csv")
        assert path.exists()

    def test_csv_has_header_and_rows(self, tmp_path, sample_report):
        path = write_csv_report(sample_report, tmp_path / "report.csv")
        with open(path) as f:
            reader = csv.reader(f)
            rows = list(reader)
        # Header + 2 data rows
        assert len(rows) == 3
        assert rows[0][0] == "check_name"

    def test_csv_values(self, tmp_path, sample_report):
        path = write_csv_report(sample_report, tmp_path / "report.csv")
        with open(path) as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        assert rows[0]["check_name"] == "AGI Check"
        assert rows[0]["status"] == "PASS"
        assert rows[1]["status"] == "FAIL"

    def test_empty_report(self, tmp_path):
        report = ReconciliationReport()
        path = write_csv_report(report, tmp_path / "empty.csv")
        with open(path) as f:
            reader = csv.reader(f)
            rows = list(reader)
        assert len(rows) == 1  # Header only
