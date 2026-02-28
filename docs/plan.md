# Implementation Plan: IRS SOI Data Acquisition & ETL Pipeline

## Context

The project needs a complete Python pipeline to: download 33 IRS Excel files + 3 Revenue Procedure PDFs, parse them into 4 canonical Parquet tables, adjust for inflation, and validate cross-table consistency. Currently only `docs/plan.md` exists — no code, no build system.

## Technology Stack

- **Python 3.11+** with `pyproject.toml` for dependency management
- **xlrd** for `.xls` parsing (IRS files are old-format Excel)
- **pandas + pyarrow** for data manipulation and Parquet output
- **pdfplumber** for Revenue Procedure PDF extraction
- **requests** for downloads
- **pytest** for testing

## Directory Structure to Create

```
pyproject.toml
src/
  __init__.py
  etl/
    __init__.py
    url_registry.py      # All IRS download URLs and metadata
    download.py          # Download script + SHA256 manifest generation
    agi_bins.py          # Canonical AGI bin definitions + text matching
    parse_table_1x.py    # Tables 1.1-1.4 → RETURNS_AGGREGATE
    parse_table_14a.py   # Table 1.4A → CAPITAL_GAINS
    parse_table_3x.py    # Tables 3.2-3.6 → RETURNS_AGGREGATE + BRACKET_DISTRIBUTION
    cpi_adjust.py        # CPI-U inflation adjustment to 2022 dollars
    pipeline.py          # Main orchestrator: raw → Parquet
  parameters/
    __init__.py
    extract_tax_params.py  # Rev Proc PDFs → JSON
  validation/
    __init__.py
    reconcile.py         # 5 cross-table reconciliation checks
    report.py            # Text/CSV report generation
tests/
  __init__.py
  conftest.py
  fixtures/              # Small Excel excerpts for unit tests
  test_agi_bins.py
  test_download.py
  test_parse_table_1x.py
  test_parse_table_14a.py
  test_parse_table_3x.py
  test_cpi_adjust.py
  test_reconcile.py
data/                    # .gitignore'd except parameters/
  raw/{2020,2021,2022}/
  parameters/
  processed/
    nominal/
    real_2022/
```

## Implementation Phases (build order)

### Phase 0: Project Scaffold
- Create `pyproject.toml` with all dependencies
- Create directory structure and `__init__.py` files
- Create `.gitignore` (ignore `data/raw/`, `data/processed/`, `*.xls`, `*.parquet`)
- Create `data/` directories

### Phase 1: Download System
- **`src/etl/url_registry.py`** — Dict mapping `(year, table_id)` to IRS URL. 11 Excel files per year (33 total) + 3 Revenue Procedure PDFs. URLs follow pattern: `https://www.irs.gov/pub/irs-soi/{YY}in{TABLE_SUFFIX}.xls`
- **`src/etl/download.py`** — CLI script: `python -m src.etl.download [--years 2020 2021 2022] [--force] [--verify-only]`. Downloads to `data/raw/{year}/`, PDFs to `data/parameters/`. Generates `data/manifest.json` with URL, filename, year, table_id, sha256, download_date, size_bytes.

### Phase 2: AGI Bin Definitions
- **`src/etl/agi_bins.py`** — Define canonical AGI bin IDs (1-N) with lower/upper bounds. Provide `match_agi_bin(text) -> int | None` that fuzzy-matches the various text representations IRS uses ("Under $5,000", "$1 under $5,000", etc.) to canonical bin IDs. Returns `None` for aggregate/total rows. This is the foundation every parser depends on.

### Phase 3: Excel Parsers
Table-specific, config-driven parsing (hardcoded row/column positions per table — IRS layouts are stable across years):

- **`src/etl/parse_table_1x.py`** — Tables 1.1-1.4. Config dict per table: header_rows, data_start_row, agi_col, column mappings, units_multiplier (1000 for "amounts in thousands"). Shared `_clean_cell()` function strips footnote markers (`[1]`, `*`, `†`), handles dashes/blanks. Outputs feed RETURNS_AGGREGATE.
- **`src/etl/parse_table_14a.py`** — Table 1.4A (capital gains). Same pattern. Outputs feed CAPITAL_GAINS.
- **`src/etl/parse_table_3x.py`** — Tables 3.2-3.3 (AGI-binned, feed RETURNS_AGGREGATE). Tables 3.4-3.6 (bracket tables: rows are marginal rates, not AGI bins; filing status appears as section headers within the table). Outputs feed BRACKET_DISTRIBUTION.

Key parsing challenges:
- `.xls` format requires `xlrd` engine
- Multi-row headers (typically rows 1-5)
- "Money amounts in thousands of dollars" — multiply by 1,000
- Merged cells read as empty except top-left cell
- Footnote markers in numeric cells
- Tables 3.4-3.6: section-based layout with filing status as section headers, not columns

### Phase 4: CPI Adjustment
- **`src/etl/cpi_adjust.py`** — Hardcoded CPI-U annual averages (2020: 258.811, 2021: 270.970, 2022: 292.655). Multiplier = CPI_2022 / CPI_year. Apply to all monetary columns, output as separate "real_2022" Parquet files alongside nominal files.

### Phase 5: Pipeline Orchestrator
- **`src/etl/pipeline.py`** — CLI: `python -m src.etl.pipeline [--years ...]`. For each year: parse all tables → merge into 4 canonical DataFrames → write nominal Parquet → CPI-adjust → write real Parquet. Output to `data/processed/nominal/` and `data/processed/real_2022/`.

### Phase 6: Validation
- **`src/validation/reconcile.py`** — 5 checks per year:
  1. Total AGI across Tables 1.1, 1.4, 3.2
  2. Total income tax across Tables 1.1, 3.3
  3. Sum of bracket tax (3.4-3.6) vs total tax in 3.3 (tolerance <0.05%)
  4. Return counts across all major tables
  5. Capital gain totals
- **`src/validation/report.py`** — Text + CSV report output with PASS/FAIL per check

### Phase 7: Tax Parameters
- **`src/parameters/extract_tax_params.py`** — Parse Revenue Procedure PDFs with `pdfplumber`. Extract ordinary income brackets, capital gains brackets, standard deduction, AMT exemption/phaseout by filing status. Output as `data/parameters/{year}_tax_parameters.json`. Fallback: support manually-curated JSON if PDF extraction is too fragile.

## Verification

1. **Download**: `python -m src.etl.download` — all 36 files downloaded, `data/manifest.json` generated with checksums
2. **Pipeline**: `python -m src.etl.pipeline` — 4 Parquet files in both `nominal/` and `real_2022/`
3. **Validation**: `python -m src.validation.reconcile` — all checks pass (<0.05% variance)
4. **Tests**: `pytest tests/` — all unit tests pass
5. **Parameters**: `python -m src.parameters.extract_tax_params` — 3 JSON files in `data/parameters/`
