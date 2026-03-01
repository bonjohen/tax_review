-- SQLite schema for tax_review ETL pipeline.
-- Executed by db.init_schema() on first run.

-- Reference Tables --------------------------------------------------------

CREATE TABLE IF NOT EXISTS agi_bins (
    agi_bin_id   INTEGER PRIMARY KEY,
    lower_bound  REAL NOT NULL,
    upper_bound  REAL NOT NULL,
    label        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS cpi_factors (
    year            INTEGER PRIMARY KEY,
    cpi_u_annual    REAL NOT NULL,
    factor_to_2022  REAL NOT NULL
);

-- Raw Parsed Tables -------------------------------------------------------

CREATE TABLE IF NOT EXISTS raw_table_11 (
    year              INTEGER NOT NULL,
    agi_bin_id        INTEGER NOT NULL,
    filing_status     TEXT NOT NULL DEFAULT 'all',
    return_count      REAL,
    total_agi         REAL,
    total_taxable_income REAL,
    total_income_tax  REAL,
    PRIMARY KEY (year, agi_bin_id, filing_status)
);

CREATE TABLE IF NOT EXISTS raw_table_12 (
    year              INTEGER NOT NULL,
    agi_bin_id        INTEGER NOT NULL,
    filing_status     TEXT NOT NULL,
    return_count      REAL,
    total_agi         REAL,
    total_taxable_income REAL,
    total_income_tax  REAL,
    PRIMARY KEY (year, agi_bin_id, filing_status)
);

CREATE TABLE IF NOT EXISTS raw_table_32 (
    year              INTEGER NOT NULL,
    agi_bin_id        INTEGER NOT NULL,
    filing_status     TEXT NOT NULL DEFAULT 'all',
    return_count      REAL,
    total_agi         REAL,
    total_income_tax  REAL,
    effective_tax_rate REAL,
    PRIMARY KEY (year, agi_bin_id, filing_status)
);

CREATE TABLE IF NOT EXISTS raw_table_33 (
    year              INTEGER NOT NULL,
    agi_bin_id        INTEGER NOT NULL,
    filing_status     TEXT NOT NULL DEFAULT 'all',
    return_count      REAL,
    total_credits     REAL,
    total_income_tax  REAL,
    PRIMARY KEY (year, agi_bin_id, filing_status)
);

CREATE TABLE IF NOT EXISTS raw_table_14a (
    year              INTEGER NOT NULL,
    agi_bin_id        INTEGER NOT NULL,
    schedule_d_count  REAL,
    short_term_gain   REAL,
    long_term_gain    REAL,
    total_gain        REAL,
    PRIMARY KEY (year, agi_bin_id)
);

CREATE TABLE IF NOT EXISTS raw_table_14 (
    year               INTEGER NOT NULL,
    agi_bin_id         INTEGER NOT NULL,
    wages              REAL,
    taxable_interest   REAL,
    ordinary_dividends REAL,
    qualified_dividends REAL,
    tax_exempt_interest REAL,
    business_income    REAL,
    capital_gains      REAL,
    partnership_scorp  REAL,
    ira_pension        REAL,
    social_security    REAL,
    rental_royalty     REAL,
    estate_trust       REAL,
    PRIMARY KEY (year, agi_bin_id)
);

CREATE TABLE IF NOT EXISTS raw_table_34 (
    year                   INTEGER NOT NULL,
    filing_status          TEXT NOT NULL,
    marginal_rate          REAL NOT NULL,
    bracket_return_count   REAL,
    bracket_taxable_income REAL,
    bracket_tax            REAL,
    PRIMARY KEY (year, filing_status, marginal_rate)
);

CREATE TABLE IF NOT EXISTS raw_table_36 (
    year                   INTEGER NOT NULL,
    filing_status          TEXT NOT NULL,
    marginal_rate          REAL NOT NULL,
    bracket_return_count   REAL,
    bracket_taxable_income REAL,
    bracket_tax            REAL,
    PRIMARY KEY (year, filing_status, marginal_rate)
);

-- Canonical Views ---------------------------------------------------------

CREATE VIEW IF NOT EXISTS v_returns_aggregate AS
SELECT t12.year, t12.agi_bin_id, t12.filing_status, t12.return_count,
       t12.total_agi, t12.total_taxable_income, t12.total_income_tax,
       t33.total_credits, t32.effective_tax_rate
FROM raw_table_12 t12
LEFT JOIN raw_table_33 t33
  ON t12.year = t33.year AND t12.agi_bin_id = t33.agi_bin_id
LEFT JOIN raw_table_32 t32
  ON t12.year = t32.year AND t12.agi_bin_id = t32.agi_bin_id;

CREATE VIEW IF NOT EXISTS v_capital_gains AS
SELECT year, agi_bin_id, schedule_d_count,
       short_term_gain, long_term_gain, total_gain
FROM raw_table_14a;

CREATE VIEW IF NOT EXISTS v_income_sources AS
SELECT year, agi_bin_id, wages, taxable_interest, ordinary_dividends,
       qualified_dividends, tax_exempt_interest, business_income,
       capital_gains, partnership_scorp, ira_pension, social_security,
       rental_royalty, estate_trust
FROM raw_table_14;

CREATE VIEW IF NOT EXISTS v_bracket_distribution AS
SELECT year, filing_status, marginal_rate,
       bracket_taxable_income, bracket_tax, bracket_return_count
FROM raw_table_36;

CREATE VIEW IF NOT EXISTS v_agi_bins AS
SELECT y.year, b.agi_bin_id, b.lower_bound AS agi_lower_bound,
       b.upper_bound AS agi_upper_bound, b.label
FROM agi_bins b
CROSS JOIN (SELECT DISTINCT year FROM raw_table_12) y;

-- CPI-Adjusted Views ------------------------------------------------------

CREATE VIEW IF NOT EXISTS v_returns_aggregate_real2022 AS
SELECT r.year, r.agi_bin_id, r.filing_status, r.return_count,
       r.total_agi * c.factor_to_2022 AS total_agi,
       r.total_taxable_income * c.factor_to_2022 AS total_taxable_income,
       r.total_income_tax * c.factor_to_2022 AS total_income_tax,
       r.total_credits * c.factor_to_2022 AS total_credits,
       r.effective_tax_rate
FROM v_returns_aggregate r
JOIN cpi_factors c ON r.year = c.year;

CREATE VIEW IF NOT EXISTS v_capital_gains_real2022 AS
SELECT g.year, g.agi_bin_id, g.schedule_d_count,
       g.short_term_gain * c.factor_to_2022 AS short_term_gain,
       g.long_term_gain * c.factor_to_2022 AS long_term_gain,
       g.total_gain * c.factor_to_2022 AS total_gain
FROM v_capital_gains g
JOIN cpi_factors c ON g.year = c.year;

CREATE VIEW IF NOT EXISTS v_bracket_distribution_real2022 AS
SELECT bd.year, bd.filing_status, bd.marginal_rate,
       bd.bracket_taxable_income * c.factor_to_2022 AS bracket_taxable_income,
       bd.bracket_tax * c.factor_to_2022 AS bracket_tax,
       bd.bracket_return_count
FROM v_bracket_distribution bd
JOIN cpi_factors c ON bd.year = c.year;

-- Validation Views --------------------------------------------------------

CREATE VIEW IF NOT EXISTS v_check_agi AS
SELECT t11.year,
       SUM(t11.total_agi) AS agi_table_11,
       SUM(t12.total_agi) AS agi_table_12,
       ABS(SUM(t11.total_agi) - SUM(t12.total_agi))
         / MAX(ABS(SUM(t11.total_agi)), ABS(SUM(t12.total_agi))) AS variance_pct
FROM raw_table_11 t11
JOIN raw_table_12 t12
  ON t11.year = t12.year AND t11.agi_bin_id = t12.agi_bin_id
  AND t12.filing_status = 'all'
GROUP BY t11.year;

CREATE VIEW IF NOT EXISTS v_check_income_tax AS
SELECT t11.year,
       SUM(t11.total_income_tax) AS tax_table_11,
       SUM(t12.total_income_tax) AS tax_table_12,
       ABS(SUM(t11.total_income_tax) - SUM(t12.total_income_tax))
         / MAX(ABS(SUM(t11.total_income_tax)), ABS(SUM(t12.total_income_tax))) AS variance_pct
FROM raw_table_11 t11
JOIN raw_table_12 t12
  ON t11.year = t12.year AND t11.agi_bin_id = t12.agi_bin_id
  AND t12.filing_status = 'all'
GROUP BY t11.year;

CREATE VIEW IF NOT EXISTS v_check_income_tax_33 AS
SELECT t11.year,
       SUM(t11.total_income_tax) AS tax_table_11,
       SUM(t33.total_income_tax) AS tax_table_33,
       ABS(SUM(t11.total_income_tax) - SUM(t33.total_income_tax))
         / MAX(ABS(SUM(t11.total_income_tax)), ABS(SUM(t33.total_income_tax))) AS variance_pct
FROM raw_table_11 t11
JOIN raw_table_33 t33
  ON t11.year = t33.year AND t11.agi_bin_id = t33.agi_bin_id
GROUP BY t11.year;

CREATE VIEW IF NOT EXISTS v_check_bracket_tax AS
SELECT t36.year,
       SUM(t36.bracket_tax) AS bracket_tax,
       t12_agg.total_income_tax AS income_tax,
       CASE WHEN t12_agg.total_income_tax > 0
            THEN SUM(t36.bracket_tax) / t12_agg.total_income_tax
            ELSE 0 END AS ratio
FROM raw_table_36 t36
JOIN (SELECT year, SUM(total_income_tax) AS total_income_tax
      FROM raw_table_12 WHERE filing_status = 'all' GROUP BY year) t12_agg
  ON t36.year = t12_agg.year
WHERE t36.filing_status = 'all'
GROUP BY t36.year, t12_agg.total_income_tax;

CREATE VIEW IF NOT EXISTS v_check_return_counts AS
SELECT t11.year,
       SUM(t11.return_count) AS count_table_11,
       SUM(t12.return_count) AS count_table_12,
       ABS(SUM(t11.return_count) - SUM(t12.return_count))
         / MAX(SUM(t11.return_count), SUM(t12.return_count)) AS variance_pct
FROM raw_table_11 t11
JOIN raw_table_12 t12
  ON t11.year = t12.year AND t11.agi_bin_id = t12.agi_bin_id
  AND t12.filing_status = 'all'
GROUP BY t11.year;

CREATE VIEW IF NOT EXISTS v_check_filing_status AS
SELECT year,
       SUM(CASE WHEN filing_status = 'all' THEN total_agi ELSE 0 END) AS all_agi,
       SUM(CASE WHEN filing_status != 'all' THEN total_agi ELSE 0 END) AS parts_agi,
       SUM(CASE WHEN filing_status = 'all' THEN return_count ELSE 0 END) AS all_count,
       SUM(CASE WHEN filing_status != 'all' THEN return_count ELSE 0 END) AS parts_count
FROM raw_table_12
GROUP BY year;

CREATE VIEW IF NOT EXISTS v_check_capital_gains AS
SELECT year,
       SUM(COALESCE(short_term_gain, 0)) + SUM(COALESCE(long_term_gain, 0)) AS st_plus_lt,
       SUM(COALESCE(total_gain, 0)) AS total
FROM raw_table_14a
GROUP BY year;
