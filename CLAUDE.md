# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Synthetic microsimulation model of the federal individual income tax system for Tax Years 2020-2022. The goal is to simulate a reform eliminating preferential treatment for capital gains held less than five years, using IRS Statistics of Income (SOI) data.

## Project Status

Planning phase. The detailed data acquisition plan lives in `docs/plan.md`. No code, build system, or test infrastructure has been implemented yet.

## Planned Architecture

```
/data/
  /raw/{2020,2021,2022}/   # Original IRS Excel downloads with SHA256 manifest
  /parameters/              # {year}_tax_parameters.json (bracket thresholds, std deduction, AMT)
/src/
  /etl/                     # Extraction/parsing scripts (raw Excel -> Parquet/CSV)
  /validation/              # Cross-table reconciliation scripts
/docs/
  plan.md                   # Authoritative data acquisition specification
```

## Data Model (4 canonical tables)

- **AGI_BINS** — income bracket definitions (year, bin_id, lower/upper bounds)
- **RETURNS_AGGREGATE** — return counts, AGI, taxable income, tax, credits, effective rate by bin and filing status
- **CAPITAL_GAINS** — short-term/long-term/total gains and Schedule D counts by bin
- **BRACKET_DISTRIBUTION** — tax generated per marginal rate bracket by filing status

## Key Constraints

- All data sourced exclusively from IRS SOI (https://www.irs.gov/statistics)
- 11 Excel files per tax year from Publication 1304 tables
- Raw files must retain original filenames; every download needs SHA256 checksum in a manifest
- Monetary values: preserve raw nominal values, also produce CPI-adjusted 2022 constant dollars
- Extracted data format: Parquet preferred, CSV acceptable
- Tax parameters extracted from IRS Revenue Procedures (PDFs) into structured JSON
- Validation tolerance: cross-table reconciliation must match within <0.05% rounding tolerance
