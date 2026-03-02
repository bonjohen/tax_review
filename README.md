# Tax Review

A calibrated synthetic microsimulation model of the federal individual income tax system (Tax Years 2018-2022, with 2023 forecast). The model simulates a reform that eliminates preferential treatment for capital gains held less than five years and includes an interactive dashboard for exploring results.

All source data comes from the [IRS Statistics of Income (SOI)](https://www.irs.gov/statistics), Publication 1304 — Individual Income Tax Returns Complete Report.

## Prerequisites

- Python 3.10+
- Windows (batch scripts provided; Python commands work cross-platform)

## Setup

```
setup_env.bat
```

Or manually:

```
python -m venv .venv
.venv\Scripts\activate
pip install -e ".[dev]"
```

## Usage

The pipeline runs in three stages: download, transform, validate.

### 1. Download IRS Data

Downloads 55 Excel files (11 per tax year × 5 years) and 5 Revenue Procedure PDFs from IRS servers. Files are saved to `data/raw/{year}/` with SHA-256 checksums recorded in `data/manifest.json`.

```
download_data.bat
download_data.bat --years 2020          # single year
download_data.bat --force               # re-download all
verify_data.bat                         # check checksums against manifest
```

### 2. Run ETL Pipeline

Parses downloaded Excel files into four canonical Parquet tables, producing both nominal and CPI-adjusted (2022 constant dollar) outputs.

```
run_pipeline.bat
run_pipeline.bat --years 2020           # single year
```

Output is written to:
- `data/processed/nominal/` — raw dollar values
- `data/processed/real_2022/` — adjusted to 2022 dollars using CPI-U

### 3. Extract Tax Parameters

Extracts statutory tax parameters (bracket thresholds, standard deductions, AMT) from IRS Revenue Procedure PDFs into structured JSON.

```
extract_params.bat
```

Output: `data/parameters/{year}_tax_parameters.json`

### 4. Run Validation

Cross-table reconciliation checks that verify parsed data is internally consistent. All checks must pass within a 0.05% tolerance.

```
run_validation.bat
```

Checks performed per tax year:
1. Total AGI across Tables 1.1, 1.4, and 3.2
2. Total income tax across Tables 1.1 and 3.3
3. Bracket tax reconstruction from Tables 3.4-3.6 vs total
4. Return count consistency across all major tables
5. Capital gain totals from Table 1.4A vs Table 1.4

### 5. Generate Dashboard

Exports pipeline and reform model results to JSON and serves the interactive dashboard.

```
python -m src.web.export_data
```

Open `src/web/index.html` in a browser (served via a local HTTP server for fetch to work):

```
cd src/web && python -m http.server 8000
```

The dashboard features:
- **Collapsible chart sections** with 5 consolidated AGI groups (Under $200K, $200K-$1M, $1M-$5M, $5M-$10M, $10M+)
- **Year selector** dropdown (TY2018-2023) with forecast banner for projected years
- **Interactive analysis** with bidirectional links between narrative sections and charts
- **Read Aloud** capability using the Web Speech API
- Charts: burden distribution, revenue impact, filing status breakdown, concentration curve, year-over-year trends, income composition, and tax bracket distribution

### 6. Run Tests

```
run_tests.bat
run_tests.bat -k test_agi_bins          # single test module
run_tests.bat -k test_standard_labels   # single test case
```

## Data Model

The pipeline produces four canonical tables:

| Table | Description | Key Columns |
|-------|-------------|-------------|
| **AGI_BINS** | 19 income bracket definitions | `year`, `agi_bin_id`, `agi_lower_bound`, `agi_upper_bound` |
| **RETURNS_AGGREGATE** | Tax return aggregates by income bracket and filing status | `year`, `agi_bin_id`, `filing_status`, `return_count`, `total_agi`, `total_income_tax`, `effective_tax_rate` |
| **CAPITAL_GAINS** | Capital gains by income bracket | `year`, `agi_bin_id`, `short_term_gain`, `long_term_gain`, `total_gain`, `schedule_d_count` |
| **BRACKET_DISTRIBUTION** | Tax by marginal rate bracket and filing status | `year`, `filing_status`, `marginal_rate`, `bracket_taxable_income`, `bracket_tax`, `bracket_return_count` |

## Project Structure

```
src/
  etl/
    url_registry.py        All 60 IRS download URLs
    download.py            Download script + SHA-256 manifest
    agi_bins.py            Canonical AGI bin definitions + text matching
    parse_table_1x.py      Tables 1.1-1.4 parser
    parse_table_14a.py     Table 1.4A (capital gains) parser
    parse_table_3x.py      Tables 3.2-3.6 parser
    cpi_adjust.py          CPI-U inflation adjustment
    pipeline.py            ETL orchestrator
  parameters/
    extract_tax_params.py  Revenue Procedure PDF extractor
  reform/
    filing_status.py       AGI-share allocation by filing status
  forecast/
    generate_2023.py       Trend-based TY2023 forecast generation
  validation/
    reconcile.py           Cross-table reconciliation checks
    report.py              Report generation (text + CSV)
  web/
    index.html             Interactive dashboard (HTML + embedded JS)
    style.css              Dashboard styles
    export_data.py         Pipeline → dashboard JSON exporter
    data/dashboard.json    Exported dashboard data (not in git)
tests/                     Unit and integration tests
docs/
  old_plan.md              Original data acquisition specification
  plan.md                  Implementation plan
data/
  raw/{2018,2019,2020,2021,2022}/    Downloaded IRS Excel files (not in git)
  parameters/              Tax parameter JSON files
  processed/               Parquet output (not in git)
  validation/              Reconciliation reports
```

## Data Sources

| Tables | Source | Content |
|--------|--------|---------|
| 1.1 | `{yy}in11si.xls` | Selected Income and Tax Items |
| 1.2 | `{yy}in12ms.xls` | Income and Tax Items by Filing Status |
| 1.3 | `{yy}in13ms.xls` | Detailed Income and Tax Items by Filing Status |
| 1.4 | `{yy}in14ar.xls` | Sources of Income by Size of AGI |
| 1.4A | `{yy}in14acg.xls` | Capital Asset Sales (Schedule D) |
| 1.6 | `{yy}in16ag.xls` | Returns by Age, Marital Status, and AGI |
| 3.2 | `{yy}in32tt.xls` | Income Tax as Percentage of AGI |
| 3.3 | `{yy}in33ar.xls` | Tax Liability and Credits by AGI |
| 3.4 | `{yy}in34tr.xls` | Tax by Marginal Rate and Filing Status |
| 3.5 | `{yy}in35tr.xls` | Tax Generated by Rate and AGI |
| 3.6 | `{yy}in36tr.xls` | Taxable Income and Tax by Rate and Filing Status |

Where `{yy}` is `18`, `19`, `20`, `21`, or `22` for Tax Years 2018-2022. All files are downloaded from [https://www.irs.gov/pub/irs-soi/](https://www.irs.gov/pub/irs-soi/).
