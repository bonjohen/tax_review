EMPIRICAL VALIDATION DATA COLLECTION PLAN
Validation of Loss Harvesting Ratios and Holding-Period Fractions
Scope: Most Recent Available SOI Microdata (Primary Target: 2019 PUF)

Purpose
Empirically estimate:

1. Loss Harvesting Ratios by AGI class
2. Holding-Period Fractions (1–5 years vs 5+ years) by AGI class

These estimates will replace or confirm the calibration parameters used in the microsimulation reform model.


Where possible, thread these tasks in with existing pipeline code (example: we need to collect a new attribute from a spreadsheet we already download); don't create a new pipeline, revise the existing one. Make the new work conceptually and stylistically similar to existing work.


---

SECTION 1 — LOSS HARVESTING RATIO VALIDATION
(Data Source: IRS SOI Public Use File)

Primary Source

Entity: Internal Revenue Service

Dataset: SOI Individual Public Use File (PUF)
Access page:
[https://www.irs.gov/statistics/soi-tax-stats-individual-public-use-file](https://www.irs.gov/statistics/soi-tax-stats-individual-public-use-file)

Target Year
Use most recent available PUF (typically 2019 at present).

---

1.1 Download Procedure

1. Navigate to PUF landing page.

2. Download:

   * Data file (CSV or DAT format)
   * Data dictionary (codebook)
   * Documentation PDF

3. Archive to:

/data/raw/puf/{year}/

Record SHA256 checksum in manifest file.

---

1.2 Required Variables (Verify Using Codebook)

Extract the following fields:

* AGI (adjusted gross income)
* Filing status
* Long-term capital gains
* Long-term capital losses
* Net capital gain
* Capital loss carryover
* Capital loss deduction (IRC §1211(b) $3,000 cap)
* Weight variable (return weight)

Variable names differ by year — confirm in documentation before extraction.

---

1.3 Derived Fields

For each return:

Gross_LT_Gain = LT_Gain_Positive
Gross_LT_Loss = LT_Loss_Positive

Net_LT_Gain = Reported net long-term gain

Loss_Harvested = Gross_LT_Gain − Net_LT_Gain

Harvesting_Ratio = Loss_Harvested / Gross_LT_Gain

Apply sampling weight to all aggregations.

---

1.4 Aggregation Procedure

1. Bin returns into AGI ranges matching reform model.
2. Compute weighted totals per bin:

   * Total gross LT gains
   * Total net LT gains
3. Compute empirical harvesting ratio per bin:

Harvesting_Ratio_AGI =
(Total_Gross_LT_Gain − Total_Net_LT_Gain) / Total_Gross_LT_Gain

---

1.5 Validation Outputs

Produce table:

AGI Range
Gross LT Gains
Net LT Gains
Implied Harvesting Ratio

Compare empirical values to assumed ratios (70–90%).

---

SECTION 2 — HOLDING PERIOD FRACTION VALIDATION
(Data Source: SOI Sales of Capital Assets Dataset)

Primary Source

Entity: Internal Revenue Service Statistics of Income Division

Dataset: Sales of Capital Assets Reported on Individual Tax Returns
Access page:
[https://www.irs.gov/statistics/soi-tax-stats-sales-of-capital-assets-reported-on-individual-tax-returns](https://www.irs.gov/statistics/soi-tax-stats-sales-of-capital-assets-reported-on-individual-tax-returns)

---

2.1 Download Procedure

1. Locate most recent Excel tables.
2. Download tables containing:

   * Holding period classification
   * Gross gains by holding duration
   * AGI class distribution

Archive to:

/data/raw/soca/{year}/

Record SHA256 checksum.

---

2.2 Required Fields

Extract:

* Holding period classification
  (short-term vs long-term; if detailed duration exists, capture duration bins)
* Gross gains
* Net gains
* AGI class

If 1–5 year breakdown not directly provided:

Estimate duration structure using:

* Reported mean holding period
* Gain distribution tails
* Treasury elasticity papers (see Section 3)

---

2.3 Derived Metric

If duration bins available:

Holding_Period_Fraction =
(Gains_held_1_to_5_years) / (Total_LT_Gains)

If only short-term vs long-term:

Fit parametric survival model:

1. Assume exponential or Weibull duration distribution.
2. Calibrate parameters using reported turnover rates.
3. Estimate fraction within 1–5 years conditional on long-term.

---

SECTION 3 — TREASURY / JCT CROSS-VALIDATION

Entities:

U.S. Department of the Treasury
Joint Committee on Taxation

Search for:

* Treasury Office of Tax Analysis (OTA) working papers
* JCT revenue estimates involving capital gains reforms

Collect:

* Assumed holding period structures
* Assumed elasticity parameters
* Behavioral adjustment assumptions

Archive PDFs to:

/data/raw/treasury/

---

SECTION 4 — OPTIONAL EXTERNAL VALIDATION

Entity: Federal Reserve

Dataset: Survey of Consumer Finances
[https://www.federalreserve.gov/econres/scfindex.htm](https://www.federalreserve.gov/econres/scfindex.htm)

Extract:

* Portfolio turnover rates
* Asset holding duration by wealth percentile

Use as directional validation only (not tax-based).

---

SECTION 5 — OUTPUT REQUIREMENTS

Produce two calibration tables:

A. Empirical Loss Harvesting Ratios
B. Empirical Holding Period Fractions

Each table must include:

* AGI Range
* Empirical Estimate
* Assumed Model Value
* Absolute Difference
* Percent Difference

---

SECTION 6 — COMPLETION CRITERIA

Validation complete when:

1. Harvesting ratios derived from weighted PUF data.
2. Holding period fractions derived or modeled from SOCA data.
3. Results reconciled with Treasury/JCT published assumptions.
4. Calibration memo written documenting methodology and limitations.

Only after empirical validation may calibration parameters be revised.

#########################################################################################


Additional information listed below based on further AI conversations on how to collect data for, and calculate values of the calibration tables.

Below are **only publicly downloadable sources** and **exact processing instructions** to build both calibration tables with no email requests and no restricted access.

---

# DATA SOURCES (DIRECT DOWNLOAD)

## A. 2020–2022 Net Capital Gains & Loss Structure

Source: Internal Revenue Service
SOI Publication 1304 Excel Tables
Directory:
[https://www.irs.gov/pub/irs-soi/](https://www.irs.gov/pub/irs-soi/)

Download for each year (replace 20/21/22):

### 1. Schedule D Summary (Capital Assets)

* 2020: [https://www.irs.gov/pub/irs-soi/20in14acg.xls](https://www.irs.gov/pub/irs-soi/20in14acg.xls)
* 2021: [https://www.irs.gov/pub/irs-soi/21in14acg.xls](https://www.irs.gov/pub/irs-soi/21in14acg.xls)
* 2022: [https://www.irs.gov/pub/irs-soi/22in14acg.xls](https://www.irs.gov/pub/irs-soi/22in14acg.xls)

### 2. Detailed Income Items (Capital Loss Deduction & Carryover)

* 2020: [https://www.irs.gov/pub/irs-soi/20in13ms.xls](https://www.irs.gov/pub/irs-soi/20in13ms.xls)
* 2021: [https://www.irs.gov/pub/irs-soi/21in13ms.xls](https://www.irs.gov/pub/irs-soi/21in13ms.xls)
* 2022: [https://www.irs.gov/pub/irs-soi/22in13ms.xls](https://www.irs.gov/pub/irs-soi/22in13ms.xls)

### 3. Income by AGI Bin

* 2020: [https://www.irs.gov/pub/irs-soi/20in14ar.xls](https://www.irs.gov/pub/irs-soi/20in14ar.xls)
* 2021: [https://www.irs.gov/pub/irs-soi/21in14ar.xls](https://www.irs.gov/pub/irs-soi/21in14ar.xls)
* 2022: [https://www.irs.gov/pub/irs-soi/22in14ar.xls](https://www.irs.gov/pub/irs-soi/22in14ar.xls)

---

## B. Historical Holding Period Structure (Stand-In)

Source: Internal Revenue Service Statistics of Income Division
Sales of Capital Assets Tables

Landing page:
[https://www.irs.gov/statistics/soi-tax-stats-sales-of-capital-assets-reported-on-individual-tax-returns](https://www.irs.gov/statistics/soi-tax-stats-sales-of-capital-assets-reported-on-individual-tax-returns)

Download the most recent available Excel file (typically 2015 or earlier).

This contains:

* Gains by holding period class
* Gross gains
* Losses
* AGI distributions (in some years)

---

# DIRECTORY STRUCTURE (FOR YOUR CODE PROJECT)

Create:

/data/raw/2020/
/data/raw/2021/
/data/raw/2022/
/data/raw/soca/

Place each XLS file in corresponding folder.

---

# PROCESSING INSTRUCTIONS

## 1️⃣ Build Net Gain Base (2020–2022)

From `in14acg.xls`:

Extract by AGI bin:

* Net long-term gains
* Net short-term gains
* Total net capital gain
* Number of returns with gains

Store as:

`capital_net_{year}.parquet`

Schema:

year
agi_bin
returns_with_schedule_d
net_lt_gain
net_st_gain
net_total_gain

---

## 2️⃣ Estimate Loss Harvesting Lower Bound (2020–2022)

From `in13ms.xls`:

Extract:

* Capital loss deduction amount
* Capital loss carryover (if listed)
* Net capital loss totals

Compute per AGI bin:

Implied_Loss_Ratio =
(Total capital loss deduction + Net losses) /
(Net gains + Total capital loss deduction)

This gives a **minimum harvesting intensity**.

Store as:

`harvest_implied_{year}.parquet`

---

## 3️⃣ Build Holding Period Fraction (Using SOCA)

From SOCA file:

Extract:

* Gains by holding duration

  * Short-term (<1 year)
  * 1–5 years (if separate)
  * 5+ years (if separate)

If 1–5 and 5+ are separate:

Holding_Fraction =
Gains_1_to_5 / Total_LT_Gains

If only LT total available:

Compute:

ST_to_LT_ratio = ST_Gains / LT_Gains

Use that ratio to approximate turnover intensity and scale duration using SOCA proportions.

Store:

`holding_fraction_estimate.parquet`

---

## 4️⃣ Map SOCA Fractions to 2020–2022

Apply SOCA-derived holding fraction by AGI group:

If SOCA provides AGI bins:

Directly map.

If not:

Interpolate fraction by:

* Weighting by AGI concentration of LT gains (from in14acg tables).

Store:

`holding_fraction_{year}.parquet`

---

# FINAL OUTPUT TABLES

## Calibration Table 1 — Harvesting Ratio

year
agi_bin
net_lt_gain
implied_loss_ratio

---

## Calibration Table 2 — Holding Period Fraction

year
agi_bin
holding_fraction_1_to_5

---

# OPTIONAL VALIDATION STEP

Reconstruct implied gross gains:

gross_lt_gain = net_lt_gain / (1 - implied_loss_ratio)

Check that:

Sum(gross_lt_gain) remains economically plausible relative to:

* Total AGI
* Market capitalization growth that year

---

# RESULT

You now have:

• 2020–2022 real tax base
• Empirical lower-bound harvesting estimate
• Empirical holding-period proxy from real IRS duration data
• Fully public datasets
• Fully automatable pipeline

---
