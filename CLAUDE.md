# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Synthetic microsimulation model of the federal individual income tax system for Tax Years 2020-2022. The goal is to simulate a reform eliminating preferential treatment for capital gains held less than five years, using IRS Statistics of Income (SOI) data.

## Build & Run Commands

```bash
# Install dependencies (from project root)
pip install -e ".[dev]"

# Download IRS data files (33 Excel + 3 PDFs)
python -m src.etl.download
python -m src.etl.download --years 2020   # single year
python -m src.etl.download --verify-only  # check manifest checksums

# Run ETL pipeline (requires downloaded data)
python -m src.etl.pipeline

# Extract tax parameters from Revenue Procedure PDFs
python -m src.parameters.extract_tax_params

# Run validation
python -m src.validation.reconcile

# Run tests
pytest tests/
pytest tests/test_agi_bins.py              # single test file
pytest tests/test_agi_bins.py -k "test_standard_labels"  # single test
```

## Architecture

```
Excel (.xls) → Python parsers (xlrd) → SQLite raw tables
                                           ↓
                                      SQL VIEWs (canonical + CPI-adjusted + validation)
                                           ↓
                                      pd.read_sql → Parquet export
```

```
src/
  etl/
    url_registry.py      # All 36 IRS download URLs (11 Excel/year + 3 PDFs)
    download.py          # Download script + SHA256 manifest (data/manifest.json)
    agi_bins.py          # Canonical 19 AGI bin definitions + text label matching
    db.py                # SQLite connection manager, schema init, insert helpers
    schema.sql           # All DDL: raw tables, canonical views, CPI views, validation views
    parse_table_1x.py    # Tables 1.1-1.4 parser + load_table_*() for SQLite
    parse_table_14a.py   # Table 1.4A parser + load_table_14a() for SQLite
    parse_table_3x.py    # Tables 3.4-3.6 parser + load_table_*() for SQLite
    cpi_adjust.py        # CPI-U adjustment (hardcoded 2020-2022 values) → 2022 dollars
    pipeline.py          # Orchestrator: raw Excel → SQLite → Parquet output
  parameters/
    extract_tax_params.py  # Revenue Procedure PDFs → JSON
  validation/
    reconcile.py         # 6 cross-table validation checks via SQLite views
    report.py            # Text/CSV report generation
data/
  tax_review.db          # SQLite database (gitignored)
  raw/{2020,2021,2022}/  # Downloaded .xls files (gitignored)
  parameters/            # Tax parameter JSON + Revenue Procedure PDFs
  processed/nominal/     # Parquet output in nominal dollars
  processed/real_2022/   # Parquet output in 2022 constant dollars
```

## Data Model (4 canonical tables)

- **AGI_BINS** — 19 income bracket definitions (year, bin_id, lower/upper bounds)
- **RETURNS_AGGREGATE** — return counts, AGI, taxable income, tax, credits, effective rate by bin and filing status
- **CAPITAL_GAINS** — short-term/long-term/total gains and Schedule D counts by bin
- **BRACKET_DISTRIBUTION** — tax generated per marginal rate bracket by filing status

## Key Technical Details

- IRS files are old `.xls` (BIFF format) — must use `xlrd` engine, not `openpyxl`
- All IRS monetary values are reported "in thousands of dollars" — parsers multiply by 1,000
- IRS Excel files have multi-row headers (rows 1-5), footnote markers in cells (`[1]`, `*`), and merged cells
- AGI bin text labels vary between tables — `agi_bins.py` provides centralized fuzzy matching
- Tables 3.4-3.6 are organized by marginal rate (not AGI bin), with filing status as section headers
- CPI-U annual averages are hardcoded (2020: 258.811, 2021: 270.970, 2022: 292.655)

## Key Constraints

- All data sourced exclusively from IRS SOI (https://www.irs.gov/statistics)
- Raw files must retain original filenames; every download needs SHA256 checksum in manifest
- Validation tolerance: cross-table reconciliation must match within <0.05%
- Tax parameters extracted from IRS Revenue Procedures (PDFs) into structured JSON
