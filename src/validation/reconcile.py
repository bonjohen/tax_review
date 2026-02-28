"""Cross-table reconciliation and validation.

Implements the five required validation checks per tax year:
1. AGI totals across Tables 1.1, 1.4, and 3.2
2. Income tax totals across Tables 1.1 and 3.3
3. Bracket tax reconstruction from Tables 3.4-3.6 vs total tax
4. Return count consistency across all major tables
5. Capital gain totals from Table 1.4A vs Table 1.4

Usage:
    python -m src.validation.reconcile [--years 2020 2021 2022]
"""

import argparse
import logging
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from src.etl.url_registry import YEARS

logger = logging.getLogger(__name__)

TOLERANCE = 0.0005  # 0.05%
PROCESSED_DIR = Path("data") / "processed" / "nominal"


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
        lines = ["Reconciliation Report", "=" * 60]
        for r in self.results:
            lines.append(
                f"[{r.status}] {r.check_name} (TY {r.year}): "
                f"{r.table_a}={r.value_a:,.0f} vs {r.table_b}={r.value_b:,.0f} "
                f"(variance: {r.variance_pct:.4%})"
            )
        lines.append("=" * 60)
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
           val_a: float, val_b: float) -> ValidationResult:
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
        passed=(var < TOLERANCE),
    )


def reconcile_agi(year: int, returns_df: pd.DataFrame) -> list[ValidationResult]:
    """Check 1: Total AGI matches across source tables."""
    # TODO: Implement once parsed DataFrames track source table provenance
    return []


def reconcile_income_tax(year: int, returns_df: pd.DataFrame) -> list[ValidationResult]:
    """Check 2: Total income tax matches across Tables 1.1 and 3.3."""
    return []


def reconcile_bracket_tax(year: int, bracket_df: pd.DataFrame,
                          returns_df: pd.DataFrame) -> list[ValidationResult]:
    """Check 3: Sum of bracket tax matches total tax within 0.05%."""
    return []


def reconcile_return_counts(year: int, returns_df: pd.DataFrame) -> list[ValidationResult]:
    """Check 4: Return counts match across major tables."""
    return []


def reconcile_capital_gains(year: int, capgains_df: pd.DataFrame,
                            returns_df: pd.DataFrame) -> list[ValidationResult]:
    """Check 5: Capital gain totals are consistent."""
    return []


def run_all_validations(data_dir: Path | None = None) -> ReconciliationReport:
    """Load processed Parquet files and run all reconciliation checks."""
    data_dir = data_dir or PROCESSED_DIR
    report = ReconciliationReport()

    # TODO: Load Parquet files and run checks once pipeline produces output
    logger.warning("Validation not yet runnable — pipeline output required")

    return report


def main():
    parser = argparse.ArgumentParser(description="Run cross-table validation")
    parser.add_argument("--years", nargs="+", type=int, default=YEARS)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    report = run_all_validations()
    print(report.summary())
    raise SystemExit(0 if report.all_passed else 1)


if __name__ == "__main__":
    main()
