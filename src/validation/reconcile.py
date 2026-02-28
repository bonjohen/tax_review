"""Cross-table reconciliation and validation via SQLite views.

Queries pre-built validation views instead of re-parsing Excel files.
Implements six validation checks per tax year:
1. AGI totals: Table 1.1 vs Table 1.2 (all filers)
2. Income tax totals: Table 1.1 vs Table 1.2, and Table 1.1 vs Table 3.3
3. Bracket tax: sum of bracket_tax (Table 3.6) vs total_income_tax (Table 1.2)
4. Return count consistency: Table 1.1 vs Table 1.2
5. Filing status decomposition: sum of groups ≈ 'all' filers in Table 1.2
6. Capital gain internals: short_term + long_term ≈ total_gain

Usage:
    python -m src.validation.reconcile [--years 2020 2021 2022]
"""

import argparse
import logging
from dataclasses import dataclass, field
from pathlib import Path

from src.etl.db import get_connection, DEFAULT_DB_PATH
from src.etl.url_registry import YEARS

logger = logging.getLogger(__name__)

TOLERANCE = 0.005  # 0.5% — IRS tables may have rounding differences
STRICT_TOLERANCE = 0.0005  # 0.05% — for same-source comparisons


@dataclass
class ValidationResult:
    check_name: str
    year: int
    table_a: str
    table_b: str
    value_a: float
    value_b: float
    variance_pct: float
    passed: bool

    @property
    def status(self) -> str:
        return "PASS" if self.passed else "FAIL"


@dataclass
class ReconciliationReport:
    results: list[ValidationResult] = field(default_factory=list)

    def add(self, result: ValidationResult) -> None:
        self.results.append(result)

    @property
    def all_passed(self) -> bool:
        return all(r.passed for r in self.results)

    def summary(self) -> str:
        lines = ["Reconciliation Report", "=" * 80]
        for r in self.results:
            lines.append(
                f"[{r.status}] {r.check_name} (TY {r.year}): "
                f"{r.table_a}={r.value_a:,.0f} vs {r.table_b}={r.value_b:,.0f} "
                f"(variance: {r.variance_pct:.4%})"
            )
        lines.append("=" * 80)
        passed = sum(1 for r in self.results if r.passed)
        total = len(self.results)
        lines.append(f"Result: {passed}/{total} checks passed")
        return "\n".join(lines)


def _pct_variance(a: float, b: float) -> float:
    """Compute percentage variance between two values."""
    if a == 0 and b == 0:
        return 0.0
    denom = max(abs(a), abs(b))
    return abs(a - b) / denom


def _check(name: str, year: int, tbl_a: str, tbl_b: str,
           val_a: float, val_b: float,
           tolerance: float = TOLERANCE) -> ValidationResult:
    """Create a validation result for a single check."""
    var = _pct_variance(val_a, val_b)
    return ValidationResult(
        check_name=name,
        year=year,
        table_a=tbl_a,
        table_b=tbl_b,
        value_a=val_a,
        value_b=val_b,
        variance_pct=var,
        passed=(var < tolerance),
    )


def run_all_validations(years: list[int] | None = None,
                        db_path: Path | str | None = None) -> ReconciliationReport:
    """Query validation views and run all reconciliation checks."""
    years = years or YEARS
    db_path = db_path or DEFAULT_DB_PATH
    conn = get_connection(db_path)
    report = ReconciliationReport()

    for year in years:
        logger.info(f"Running validations for TY{year}...")

        # Check 1: AGI — Table 1.1 vs Table 1.2
        row = conn.execute(
            "SELECT agi_table_11, agi_table_12, variance_pct FROM v_check_agi WHERE year = ?",
            (year,),
        ).fetchone()
        if row:
            report.add(_check(
                "AGI: Table 1.1 vs Table 1.2", year,
                "Table 1.1", "Table 1.2",
                row["agi_table_11"], row["agi_table_12"],
                STRICT_TOLERANCE,
            ))

        # Check 2a: Income Tax — Table 1.1 vs Table 1.2
        row = conn.execute(
            "SELECT tax_table_11, tax_table_12, variance_pct FROM v_check_income_tax WHERE year = ?",
            (year,),
        ).fetchone()
        if row:
            report.add(_check(
                "Income Tax: Table 1.1 vs Table 1.2", year,
                "Table 1.1", "Table 1.2",
                row["tax_table_11"], row["tax_table_12"],
                STRICT_TOLERANCE,
            ))

        # Check 2b: Income Tax — Table 1.1 vs Table 3.3
        row = conn.execute(
            "SELECT tax_table_11, tax_table_33, variance_pct FROM v_check_income_tax_33 WHERE year = ?",
            (year,),
        ).fetchone()
        if row and row["tax_table_33"] and row["tax_table_33"] > 0:
            report.add(_check(
                "Income Tax: Table 1.1 vs Table 3.3", year,
                "Table 1.1", "Table 3.3",
                row["tax_table_11"], row["tax_table_33"],
                TOLERANCE,
            ))

        # Check 3: Bracket tax vs total income tax
        row = conn.execute(
            "SELECT bracket_tax, income_tax, ratio FROM v_check_bracket_tax WHERE year = ?",
            (year,),
        ).fetchone()
        if row:
            bracket_total = row["bracket_tax"]
            income_tax = row["income_tax"]
            ratio = row["ratio"] if row["ratio"] else 0
            passed = 0.70 <= ratio <= 1.05 if income_tax > 0 else False
            report.add(ValidationResult(
                check_name="Bracket Tax vs Total Income Tax",
                year=year,
                table_a="Table 3.6 bracket_tax",
                table_b="Table 1.2 total_income_tax",
                value_a=bracket_total,
                value_b=income_tax,
                variance_pct=abs(1.0 - ratio) if income_tax > 0 else 1.0,
                passed=passed,
            ))

        # Check 4: Return counts — Table 1.1 vs Table 1.2
        row = conn.execute(
            "SELECT count_table_11, count_table_12, variance_pct FROM v_check_return_counts WHERE year = ?",
            (year,),
        ).fetchone()
        if row:
            report.add(_check(
                "Return Count: Table 1.1 vs Table 1.2", year,
                "Table 1.1", "Table 1.2",
                row["count_table_11"], row["count_table_12"],
                STRICT_TOLERANCE,
            ))

        # Check 5: Filing status decomposition
        row = conn.execute(
            "SELECT all_agi, parts_agi, all_count, parts_count FROM v_check_filing_status WHERE year = ?",
            (year,),
        ).fetchone()
        if row:
            report.add(_check(
                "Filing Status AGI Decomposition", year,
                "All filers", "Sum of statuses",
                row["all_agi"], row["parts_agi"],
                STRICT_TOLERANCE,
            ))
            report.add(_check(
                "Filing Status Return Count Decomposition", year,
                "All filers", "Sum of statuses",
                row["all_count"], row["parts_count"],
                STRICT_TOLERANCE,
            ))

        # Check 6: Capital gains internal consistency
        row = conn.execute(
            "SELECT st_plus_lt, total FROM v_check_capital_gains WHERE year = ?",
            (year,),
        ).fetchone()
        if row:
            report.add(_check(
                "Capital Gains: ST + LT vs Total", year,
                "ST+LT gains", "Total gain",
                row["st_plus_lt"], row["total"],
                TOLERANCE,
            ))

    conn.close()
    return report


def main():
    parser = argparse.ArgumentParser(description="Run cross-table validation")
    parser.add_argument("--years", nargs="+", type=int, default=YEARS)
    parser.add_argument("--db", type=str, default=None,
                        help="Path to SQLite database")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    report = run_all_validations(args.years, db_path=args.db)
    print()
    print(report.summary())

    # Write reports
    from .report import write_text_report, write_csv_report
    write_text_report(report)
    write_csv_report(report)

    raise SystemExit(0 if report.all_passed else 1)


if __name__ == "__main__":
    main()
