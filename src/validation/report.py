"""Generate human-readable reconciliation reports."""

import csv
import logging
from pathlib import Path

from .reconcile import ReconciliationReport

logger = logging.getLogger(__name__)

VALIDATION_DIR = Path("data") / "validation"


def write_text_report(report: ReconciliationReport, output_path: Path | None = None) -> Path:
    """Write a plain-text reconciliation report."""
    output_path = output_path or VALIDATION_DIR / "reconciliation_report.txt"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        f.write(report.summary())
    logger.info(f"Wrote text report to {output_path}")
    return output_path


def write_csv_report(report: ReconciliationReport, output_path: Path | None = None) -> Path:
    """Write validation results as CSV."""
    output_path = output_path or VALIDATION_DIR / "reconciliation_report.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "check_name", "year", "table_a", "table_b",
            "value_a", "value_b", "variance_pct", "status",
        ])
        for r in report.results:
            writer.writerow([
                r.check_name, r.year, r.table_a, r.table_b,
                r.value_a, r.value_b, r.variance_pct, r.status,
            ])
    logger.info(f"Wrote CSV report to {output_path}")
    return output_path
