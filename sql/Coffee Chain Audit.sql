/* =======================================================================
   PART 1: LOAD CSV & EXPLORE RAW TABLE
   ======================================================================= */
-- Look at the table
SELECT *
FROM coffee_chain_raw
LIMIT 10;
-- Explore to see how SQL sees the table field names
DESCRIBE coffee_chain_raw;


/* =======================================================================
   PART 2: BUILD ANALYTICS TABLE (single clean+typed table)
   Source: coffee_chain_raw
   New Table: coffee_chain_clean 
   ======================================================================= */

DROP TABLE IF EXISTS coffee_chain_cleaned;

CREATE TABLE coffee_chain_clean AS
SELECT
  -- identifiers / dims
  CAST(`Area Code` AS UNSIGNED)                            AS area_code,
  CAST(`Inventory` AS UNSIGNED)                            AS inventory,
  TRIM(`Market Size`)                                      AS market_size,
  TRIM(`Market`)                                           AS market,
  CAST(`Number of Records` AS UNSIGNED)                    AS number_of_records,
  TRIM(`Product Line`)                                     AS product_line,
  TRIM(`Product Type`)                                     AS product_type,
  TRIM(`Product`)                                          AS product,
  TRIM(`State`)                                            AS state,
  TRIM(`Type`)                                             AS type,

  -- dates (keep both for now; you’ll choose canonical later)
  STR_TO_DATE(`Order Date (MDY)`, '%m/%d/%y')              AS order_date_mdy,
  STR_TO_DATE(`Order Date (DMY)`, '%d/%m/%y')              AS order_date_dmy,

  -- numeric fields (strip $ and commas)
  CAST(NULLIF(REPLACE(REPLACE(`Budget COGS`,   '$',''), ',',''), '') AS DECIMAL(10,2)) AS budget_cogs,
  CAST(NULLIF(REPLACE(REPLACE(`Budget Margin`, '$',''), ',',''), '') AS DECIMAL(10,2)) AS budget_margin,
  CAST(NULLIF(REPLACE(REPLACE(`Budget Profit`, '$',''), ',',''), '') AS DECIMAL(10,2)) AS budget_profit,
  CAST(NULLIF(REPLACE(REPLACE(`Budget Sales`,  '$',''), ',',''), '') AS DECIMAL(10,2)) AS budget_sales,

  CAST(NULLIF(REPLACE(REPLACE(`COGS`,          '$',''), ',',''), '') AS DECIMAL(10,2)) AS cogs,
  CAST(NULLIF(REPLACE(REPLACE(`Margin`,        '$',''), ',',''), '') AS DECIMAL(10,2)) AS margin,
  CAST(NULLIF(REPLACE(REPLACE(`Marketing`,     '$',''), ',',''), '') AS DECIMAL(10,2)) AS marketing,
  CAST(NULLIF(REPLACE(REPLACE(`Profit`,        '$',''), ',',''), '') AS DECIMAL(10,2)) AS profit,
  CAST(NULLIF(REPLACE(REPLACE(`Sales`,         '$',''), ',',''), '') AS DECIMAL(10,2)) AS sales,
  CAST(NULLIF(REPLACE(REPLACE(`Total Expenses`,'$',''), ',',''), '') AS DECIMAL(10,2)) AS total_expenses
FROM coffee_chain_raw
WHERE `Area Code` IS NOT NULL;  -- optional: drop blank rows


/* =========================================================
   Add primary key + canonical month bucket (based on MDY)
   ========================================================= */

ALTER TABLE coffee_chain_clean
  ADD COLUMN row_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY;

ALTER TABLE coffee_chain_clean
  ADD COLUMN order_month DATE;

UPDATE coffee_chain_clean
SET order_month = DATE_FORMAT(order_date_mdy, '%Y-%m-01')
WHERE row_id > 0;

/* =========================================================
   TRIM all text fields 
   ========================================================= */

UPDATE coffee_chain_clean
SET
  market_size   = TRIM(market_size),
  market        = TRIM(market),
  product_line  = TRIM(product_line),
  product_type  = TRIM(product_type),
  product       = TRIM(product),
  state         = TRIM(state),
  type          = TRIM(type)
WHERE row_id > 0;

/* =========================================================
   Quick Checks
   ========================================================= */
-- 1) Smoke test (table exists + returns rows) 
SELECT *
FROM coffee_chain_clean
LIMIT 20;

-- 2) Row count (should match raw table)
SELECT
  (SELECT COUNT(*) FROM coffee_chain_raw) AS raw_rows,
  (SELECT COUNT(*) FROM coffee_chain_clean) AS clean_rows;

-- 3) Content of fields (should be unchanged compared to raw)
SELECT product, COUNT(*) AS n
FROM coffee_chain_clean
GROUP BY product
ORDER BY n DESC;

-- 4)Date parsing shouldn't create NULLs unexpectedly (sholud be zero)
SELECT
  SUM(order_date_mdy IS NULL) AS mdy_nulls,
  SUM(order_date_dmy IS NULL) AS dmy_nulls
FROM coffee_chain_clean;


/* =========================================================
   Authoritative Field Assessment 
   ========================================================= */
/* Authoritative Field Decisions
	•	Use order_date_mdy as canonical order date; ignore order_date_dmy
	•	Treat profit as authoritative; components used only for validation
	•	Use provided margin; do not recompute unless discrepancies appear
	•	Use market as primary geographic field */



/* =======================================================================
   PART 3: CREATE ANALYSIS-READY FACT TABLE
   ======================================================================= */

DROP TABLE IF EXISTS coffee_chain_monthly_fact;

CREATE TABLE coffee_chain_monthly_fact AS
SELECT
  -- Dimensions (grain)
  order_month,
  product_line,
  product_type,
  product,
  market,
  state,
  market_size,
  type,
  -- Base numeric facts (aggregated)
  SUM(sales)         AS sales,
  SUM(cogs)          AS cogs,
  SUM(total_expenses) AS total_expenses,
  SUM(profit)        AS profit,
  SUM(marketing)     AS marketing,
  SUM(budget_sales)  AS budget_sales,
  SUM(budget_cogs)   AS budget_cogs,
  SUM(budget_profit) AS budget_profit
FROM coffee_chain_clean
GROUP BY
  order_month,
  product_line,
  product_type,
  product,
  market,
  state,
  market_size,
  type;
  
  /* =========================================================
   Quick Checks 
   ========================================================= */
-- 1) Smoke test (table exists + returns rows) 
SELECT *
FROM coffee_chain_monthly_fact
LIMIT 20;

-- 2) Row count (should be less than _clean)
SELECT
  (SELECT COUNT(*) FROM coffee_chain_clean)        AS clean_rows,
  (SELECT COUNT(*) FROM coffee_chain_monthly_fact) AS fact_rows;

-- 3) Totals reconciliation (should match _clean exactly)
SELECT
  c.clean_sales, f.fact_sales, (c.clean_sales - f.fact_sales) AS sales_diff,
  c.clean_profit, f.fact_profit, (c.clean_profit - f.fact_profit) AS profit_diff,
  c.clean_cogs, f.fact_cogs, (c.clean_cogs - f.fact_cogs) AS cogs_diff,
  c.clean_total_expenses, f.fact_total_expenses, (c.clean_total_expenses - f.fact_total_expenses) AS total_expenses_diff,
  c.clean_budget_sales, f.fact_budget_sales, (c.clean_budget_sales - f.fact_budget_sales) AS budget_sales_diff,
  c.clean_budget_profit, f.fact_budget_profit, (c.clean_budget_profit - f.fact_budget_profit) AS budget_profit_diff
FROM
  (SELECT
     SUM(sales) AS clean_sales,
     SUM(profit) AS clean_profit,
     SUM(cogs) AS clean_cogs,
     SUM(total_expenses) AS clean_total_expenses,
     SUM(budget_sales) AS clean_budget_sales,
     SUM(budget_profit) AS clean_budget_profit
   FROM coffee_chain_clean) c
CROSS JOIN
  (SELECT
     SUM(sales) AS fact_sales,
     SUM(profit) AS fact_profit,
     SUM(cogs) AS fact_cogs,
     SUM(total_expenses) AS fact_total_expenses,
     SUM(budget_sales) AS fact_budget_sales,
     SUM(budget_profit) AS fact_budget_profit
   FROM coffee_chain_monthly_fact) f;



/* =======================================================================
   PART 4: CREATE VIEWS FOR ANALYSIS
   ======================================================================= */

/* ================================================================
   VIEW 1: Budget vs Actual Variance
   Grain: order_month × product_line
   Purpose: Reveal which product lines drive budget misses over time,
            and whether gaps come from revenue shortfalls or margin/profit inefficiency.
   ================================================================ */

CREATE OR REPLACE VIEW vw_budget_vs_actual_variance AS
SELECT
  -- dimensions
  order_month,
  product_line,

  -- aggregated base metrics
  actual_sales,
  budget_sales,
  actual_profit,
  budget_profit,

  -- variance metrics
  actual_sales - budget_sales            AS sales_variance,
  sales_variance_pct,
  actual_profit - budget_profit          AS profit_variance,
  profit_variance_pct,

  -- margin metrics
  actual_margin,
  budget_margin,
  actual_margin - budget_margin          AS margin_variance

FROM (
  SELECT
    order_month,
    product_line,

    -- base aggregates
    SUM(sales)        AS actual_sales,
    SUM(budget_sales) AS budget_sales,
    SUM(profit)       AS actual_profit,
    SUM(budget_profit) AS budget_profit,

    -- pct variances (computed once)
    CASE
      WHEN SUM(budget_sales) = 0 THEN NULL
      ELSE (SUM(sales) - SUM(budget_sales)) / SUM(budget_sales)
    END AS sales_variance_pct,

    CASE
      WHEN SUM(budget_profit) = 0 THEN NULL
      ELSE (SUM(profit) - SUM(budget_profit)) / SUM(budget_profit)
    END AS profit_variance_pct,

    -- margins (computed once)
    CASE
      WHEN SUM(sales) = 0 THEN NULL
      ELSE SUM(profit) / SUM(sales)
    END AS actual_margin,

    CASE
      WHEN SUM(budget_sales) = 0 THEN NULL
      ELSE SUM(budget_profit) / SUM(budget_sales)
    END AS budget_margin

  FROM coffee_chain_monthly_fact
  GROUP BY
    order_month,
    product_line
) t;

/* ================================================================
   Quick Checks
   ================================================================ */

-- 1) Smoke test: view exists and returns data
SELECT *
FROM vw_budget_vs_actual_variance
LIMIT 10;

-- 2) Row reduction check: view should have fewer rows than fact table
SELECT
  (SELECT COUNT(*) FROM coffee_chain_monthly_fact)        AS fact_rows,
  (SELECT COUNT(*) FROM vw_budget_vs_actual_variance)     AS view_rows;

-- 3) Aggregate reconciliation (should match exactly)
-- Totals for actual/budget sales & profit should be identical between view and fact.
SELECT
  v.view_actual_sales, f.fact_sales,
  (v.view_actual_sales - f.fact_sales) AS actual_sales_diff,

  v.view_budget_sales, f.fact_budget_sales,
  (v.view_budget_sales - f.fact_budget_sales) AS budget_sales_diff,

  v.view_actual_profit, f.fact_profit,
  (v.view_actual_profit - f.fact_profit) AS actual_profit_diff,

  v.view_budget_profit, f.fact_budget_profit,
  (v.view_budget_profit - f.fact_budget_profit) AS budget_profit_diff
FROM
  (SELECT
     SUM(actual_sales)  AS view_actual_sales,
     SUM(budget_sales)  AS view_budget_sales,
     SUM(actual_profit) AS view_actual_profit,
     SUM(budget_profit) AS view_budget_profit
   FROM vw_budget_vs_actual_variance) v
CROSS JOIN
  (SELECT
     SUM(sales)        AS fact_sales,
     SUM(budget_sales) AS fact_budget_sales,
     SUM(profit)       AS fact_profit,
     SUM(budget_profit) AS fact_budget_profit
   FROM coffee_chain_monthly_fact) f;
/* ================================================================
   VIEW 2: Cost Breakdown
   Grain: product_line × market
   Purpose: Identify where costs are disproportionately high and
            whether margin pressure is driven by COGS, marketing,
            or operating expenses — and whether COGS is over plan.
   ================================================================ */

CREATE OR REPLACE VIEW vw_cost_breakdown AS
SELECT
  -- dimensions
  product_line,
  market,

  -- aggregated base metrics
  sales,
  cogs,
  budget_cogs,
  marketing,
  total_expenses,
  profit,

  -- cost ratios
  cogs_pct_sales,
  budget_cogs_pct_sales,
  cogs_variance,
  cogs_variance_pct_sales,
  marketing_pct_sales,
  expense_pct_sales,
  total_cost_pct_sales

FROM (
  SELECT
    product_line,
    market,

    -- base aggregates
    SUM(sales)           AS sales,
    SUM(cogs)            AS cogs,
    SUM(budget_cogs)     AS budget_cogs,
    SUM(marketing)       AS marketing,
    SUM(total_expenses)  AS total_expenses,
    SUM(profit)          AS profit,

    -- ratios (computed once)
    CASE
      WHEN SUM(sales) = 0 THEN NULL
      ELSE SUM(cogs) / SUM(sales)
    END AS cogs_pct_sales,

    CASE
      WHEN SUM(sales) = 0 THEN NULL
      ELSE SUM(budget_cogs) / SUM(sales)
    END AS budget_cogs_pct_sales,

    -- COGS vs Budget (absolute and scaled)
    (SUM(cogs) - SUM(budget_cogs)) AS cogs_variance,

    CASE
      WHEN SUM(sales) = 0 THEN NULL
      ELSE (SUM(cogs) - SUM(budget_cogs)) / SUM(sales)
    END AS cogs_variance_pct_sales,

    CASE
      WHEN SUM(sales) = 0 THEN NULL
      ELSE SUM(marketing) / SUM(sales)
    END AS marketing_pct_sales,

    CASE
      WHEN SUM(sales) = 0 THEN NULL
      ELSE SUM(total_expenses) / SUM(sales)
    END AS expense_pct_sales,

    CASE
      WHEN SUM(sales) = 0 THEN NULL
      ELSE (SUM(cogs) + SUM(marketing) + SUM(total_expenses)) / SUM(sales)
    END AS total_cost_pct_sales

  FROM coffee_chain_monthly_fact
  GROUP BY
    product_line,
    market
) t;

/* ================================================================
   Quick Checks
   ================================================================ */

-- 1) Smoke test: view exists and returns data
SELECT *
FROM vw_cost_breakdown
LIMIT 10;

-- 2) Row reduction check:
-- View should have fewer rows than fact table
SELECT
  (SELECT COUNT(*) FROM coffee_chain_monthly_fact) AS fact_rows,
  (SELECT COUNT(*) FROM vw_cost_breakdown)         AS view_rows;

-- 3) Aggregate reconciliation (should match exactly)
-- Sales, COGS, Marketing, Total Expenses, Profit must reconcile
SELECT
  v.view_sales, f.fact_sales,
  (v.view_sales - f.fact_sales) AS sales_diff,

  v.view_cogs, f.fact_cogs,
  (v.view_cogs - f.fact_cogs) AS cogs_diff,

  v.view_marketing, f.fact_marketing,
  (v.view_marketing - f.fact_marketing) AS marketing_diff,

  v.view_total_expenses, f.fact_total_expenses,
  (v.view_total_expenses - f.fact_total_expenses) AS total_expenses_diff,

  v.view_profit, f.fact_profit,
  (v.view_profit - f.fact_profit) AS profit_diff
FROM
  (SELECT
     SUM(sales)          AS view_sales,
     SUM(cogs)           AS view_cogs,
     SUM(marketing)      AS view_marketing,
     SUM(total_expenses) AS view_total_expenses,
     SUM(profit)         AS view_profit
   FROM vw_cost_breakdown) v
CROSS JOIN
  (SELECT
     SUM(sales)          AS fact_sales,
     SUM(cogs)           AS fact_cogs,
     SUM(marketing)      AS fact_marketing,
     SUM(total_expenses) AS fact_total_expenses,
     SUM(profit)         AS fact_profit
   FROM coffee_chain_monthly_fact) f;

/* ============================================================
   VIEW 3: Monthly Product Line Performance
   Grain: order_month × product_line
   Purpose: Track performance trends and budget variance over time
   ============================================================ */

CREATE OR REPLACE VIEW vw_monthly_product_line_performance AS
SELECT
  -- dimensions
  order_month,
  product_line,

  -- aggregated base metrics
  sales,
  profit,
  budget_sales,
  budget_profit,
  cogs,
  budget_cogs,

  -- margin metrics
  margin,
  budget_margin,

  -- COGS ratios
  cogs_pct_sales,
  budget_cogs_pct_sales,

  -- variance metrics
  sales - budget_sales               AS sales_variance,
  profit - budget_profit             AS profit_variance,
  margin - budget_margin             AS margin_variance,
  cogs - budget_cogs                 AS cogs_variance,
  cogs_pct_sales - budget_cogs_pct_sales AS cogs_pct_variance

FROM (
  SELECT
    order_month,
    product_line,

    -- base aggregates
    SUM(sales)         AS sales,
    SUM(profit)        AS profit,
    SUM(budget_sales)  AS budget_sales,
    SUM(budget_profit) AS budget_profit,
    SUM(cogs)          AS cogs,
    SUM(budget_cogs)   AS budget_cogs,

    -- margin ratios
    CASE
      WHEN SUM(sales) = 0 THEN NULL
      ELSE SUM(profit) / SUM(sales)
    END AS margin,

    CASE
      WHEN SUM(budget_sales) = 0 THEN NULL
      ELSE SUM(budget_profit) / SUM(budget_sales)
    END AS budget_margin,

    -- COGS ratios
    CASE
      WHEN SUM(sales) = 0 THEN NULL
      ELSE SUM(cogs) / SUM(sales)
    END AS cogs_pct_sales,

    CASE
      WHEN SUM(budget_sales) = 0 THEN NULL
      ELSE SUM(budget_cogs) / SUM(budget_sales)
    END AS budget_cogs_pct_sales

  FROM coffee_chain_monthly_fact
  GROUP BY
    order_month,
    product_line
) t;

 /* =========================================================
   Quick Checks 
   ========================================================= */
-- 1) Smoke test
SELECT *
FROM vw_monthly_product_line_performance
LIMIT 10;
-- 2) Rows are reduced
SELECT
  (SELECT COUNT(*) FROM coffee_chain_monthly_fact) AS fact_rows,
  (SELECT COUNT(*) FROM vw_monthly_product_line_performance) AS view_rows;

-- 3) Aggregate reconciliation (should match exactly)
SELECT
  v.view_sales, f.fact_sales, (v.view_sales - f.fact_sales) AS sales_diff,
  v.view_profit, f.fact_profit, (v.view_profit - f.fact_profit) AS profit_diff,
  v.view_budget_sales, f.fact_budget_sales, (v.view_budget_sales - f.fact_budget_sales) AS budget_sales_diff,
  v.view_budget_profit, f.fact_budget_profit, (v.view_budget_profit - f.fact_budget_profit) AS budget_profit_diff
FROM
  (SELECT
     SUM(sales) AS view_sales,
     SUM(profit) AS view_profit,
     SUM(budget_sales) AS view_budget_sales,
     SUM(budget_profit) AS view_budget_profit
   FROM vw_monthly_product_line_performance) v
CROSS JOIN
  (SELECT
     SUM(sales) AS fact_sales,
     SUM(profit) AS fact_profit,
     SUM(budget_sales) AS fact_budget_sales,
     SUM(budget_profit) AS fact_budget_profit
   FROM coffee_chain_monthly_fact) f;


/* ================================================================
   VIEW 4: Monthly Market COGS Performance
   Grain: order_month × product_line × market
   Purpose: Identify whether COGS overruns are systemic or 
            concentrated in specific markets over time.
   ================================================================ */

CREATE OR REPLACE VIEW vw_monthly_market_cogs_performance AS
SELECT
  -- dimensions
  order_month,
  product_line,
  market,

  -- aggregated base metrics
  total_sales,
  total_cogs,
  total_budget_cogs,

  -- cost ratios
  cogs_pct_sales,
  budget_cogs_pct_sales,

  -- variance metrics
  (total_cogs - total_budget_cogs) AS cogs_variance,

  CASE
    WHEN total_sales = 0 THEN NULL
    ELSE (total_cogs - total_budget_cogs) / total_sales
  END AS cogs_variance_pct_sales

FROM (
  SELECT
    order_month,
    product_line,
    market,

    -- base aggregates
    SUM(sales)        AS total_sales,
    SUM(cogs)         AS total_cogs,
    SUM(budget_cogs)  AS total_budget_cogs,

    -- ratios (computed once)
    CASE
      WHEN SUM(sales) = 0 THEN NULL
      ELSE SUM(cogs) / SUM(sales)
    END AS cogs_pct_sales,

    CASE
      WHEN SUM(sales) = 0 THEN NULL
      ELSE SUM(budget_cogs) / SUM(sales)
    END AS budget_cogs_pct_sales

  FROM coffee_chain_monthly_fact
  GROUP BY
    order_month,
    product_line,
    market
) t;

/* ================================================================
   Quick Checks — vw_monthly_market_cogs_performance
   ================================================================ */

-- 1) Smoke test: view exists and returns data
SELECT *
FROM vw_monthly_market_cogs_performance
LIMIT 10;

-- 2) Row reduction / sanity check:
-- View should have <= rows than the fact table (it’s aggregated).
SELECT
  (SELECT COUNT(*) FROM coffee_chain_monthly_fact)        AS fact_rows,
  (SELECT COUNT(*) FROM vw_monthly_market_cogs_performance) AS view_rows;

-- 3) Aggregate reconciliation (must match exactly)
-- Sales, COGS, Budget COGS must reconcile to the fact table.
SELECT
  v.view_sales, f.fact_sales,
  (v.view_sales - f.fact_sales) AS sales_diff,

  v.view_cogs, f.fact_cogs,
  (v.view_cogs - f.fact_cogs) AS cogs_diff,

  v.view_budget_cogs, f.fact_budget_cogs,
  (v.view_budget_cogs - f.fact_budget_cogs) AS budget_cogs_diff
FROM
  (SELECT
     SUM(total_sales)       AS view_sales,
     SUM(total_cogs)        AS view_cogs,
     SUM(total_budget_cogs) AS view_budget_cogs
   FROM vw_monthly_market_cogs_performance) v
CROSS JOIN
  (SELECT
     SUM(sales)       AS fact_sales,
     SUM(cogs)        AS fact_cogs,
     SUM(budget_cogs) AS fact_budget_cogs
   FROM coffee_chain_monthly_fact) f;
/* ================================================================
   VIEW 5:  Monthly Product COGS Performance (for Mix + COGS Overrun)
   Grain: order_month × product_line × product
   Purpose: Identify whether Leaves COGS overruns are concentrated
            in specific products, and whether mix shifts toward
            high-COGS products over time.
   ================================================================ */

DROP VIEW IF EXISTS vw_product_profitability;
CREATE OR REPLACE VIEW vw_monthly_product_cogs_performance AS
SELECT
  -- dimensions
  order_month,
  product_line,
  product,

  -- aggregated base metrics
  total_sales,
  total_budget_sales,
  total_cogs,
  total_budget_cogs,
  total_profit,

  -- derived metrics
  cogs_pct_sales,
  budget_cogs_pct_sales,
  margin,

  -- variance metrics
  (total_cogs - total_budget_cogs) AS cogs_variance

FROM (
  SELECT
    order_month,
    product_line,
    product,

    -- base aggregates
    SUM(sales)        AS total_sales,
    SUM(budget_sales) AS total_budget_sales,
    SUM(cogs)         AS total_cogs,
    SUM(budget_cogs)  AS total_budget_cogs,
    SUM(profit)       AS total_profit,

    -- ratios (computed once; weighted properly via SUMs)
    CASE
      WHEN SUM(sales) = 0 THEN NULL
      ELSE SUM(cogs) / SUM(sales)
    END AS cogs_pct_sales,

    CASE
      WHEN SUM(budget_sales) = 0 THEN NULL
      ELSE SUM(budget_cogs) / SUM(budget_sales)
    END AS budget_cogs_pct_sales,

    CASE
      WHEN SUM(sales) = 0 THEN NULL
      ELSE SUM(profit) / SUM(sales)
    END AS margin

  FROM coffee_chain_monthly_fact
  GROUP BY
    order_month,
    product_line,
    product
) t;


/* ================================================================
   Quick Checks (customized for this view)
   ================================================================ */

-- 1) Smoke test: view exists + returns rows
SELECT *
FROM vw_monthly_product_cogs_performance
LIMIT 10;

-- 2) Row count sanity: should be smaller than fact table
SELECT
  (SELECT COUNT(*) FROM coffee_chain_monthly_fact)                AS fact_rows,
  (SELECT COUNT(*) FROM vw_monthly_product_cogs_performance)      AS view_rows;

-- 3) Totals reconciliation (should match exactly)
-- Sales, Budget Sales, COGS, Budget COGS, Profit must reconcile
SELECT
  v.view_sales, f.fact_sales, (v.view_sales - f.fact_sales) AS sales_diff,
  v.view_budget_sales, f.fact_budget_sales, (v.view_budget_sales - f.fact_budget_sales) AS budget_sales_diff,
  v.view_cogs, f.fact_cogs, (v.view_cogs - f.fact_cogs) AS cogs_diff,
  v.view_budget_cogs, f.fact_budget_cogs, (v.view_budget_cogs - f.fact_budget_cogs) AS budget_cogs_diff,
  v.view_profit, f.fact_profit, (v.view_profit - f.fact_profit) AS profit_diff
FROM
  (SELECT
     SUM(total_sales)        AS view_sales,
     SUM(total_budget_sales) AS view_budget_sales,
     SUM(total_cogs)         AS view_cogs,
     SUM(total_budget_cogs)  AS view_budget_cogs,
     SUM(total_profit)       AS view_profit
   FROM vw_monthly_product_cogs_performance) v
CROSS JOIN
  (SELECT
     SUM(sales)        AS fact_sales,
     SUM(budget_sales) AS fact_budget_sales,
     SUM(cogs)         AS fact_cogs,
     SUM(budget_cogs)  AS fact_budget_cogs,
     SUM(profit)       AS fact_profit
   FROM coffee_chain_monthly_fact) f;





-- DID NOT USE--

/* ================================================================
   VIEW 3: Margin by Market and State
   Grain: market × state
   Purpose: Identify markets/states that underperform on margin
   ================================================================ */

CREATE OR REPLACE VIEW vw_margin_by_market_state AS
SELECT
  -- dimensions
  market,
  state,

  -- aggregated base metrics
  sales,
  profit,
  cogs,
  marketing,
  total_expenses,

  -- derived metrics
  margin,
  cogs_pct_sales,
  marketing_pct_sales,
  expense_pct_sales

FROM (
  SELECT
    market,
    state,

    -- base aggregates
    SUM(sales)          AS sales,
    SUM(profit)         AS profit,
    SUM(cogs)           AS cogs,
    SUM(marketing)      AS marketing,
    SUM(total_expenses) AS total_expenses,

    -- ratios (computed once)
    CASE
      WHEN SUM(sales) = 0 THEN NULL
      ELSE SUM(profit) / SUM(sales)
    END AS margin,

    CASE
      WHEN SUM(sales) = 0 THEN NULL
      ELSE SUM(cogs) / SUM(sales)
    END AS cogs_pct_sales,

    CASE
      WHEN SUM(sales) = 0 THEN NULL
      ELSE SUM(marketing) / SUM(sales)
    END AS marketing_pct_sales,

    CASE
      WHEN SUM(sales) = 0 THEN NULL
      ELSE SUM(total_expenses) / SUM(sales)
    END AS expense_pct_sales

  FROM coffee_chain_monthly_fact
  GROUP BY
    market,
    state
) t;

/* ================================================================
   Quick Checks
   ================================================================ */

-- 1) Smoke test: view exists and returns data
SELECT *
FROM vw_margin_by_market_state
LIMIT 10;

-- 2) Row reduction check: view should have fewer rows than fact table
SELECT
  (SELECT COUNT(*) FROM coffee_chain_monthly_fact) AS fact_rows,
  (SELECT COUNT(*) FROM vw_margin_by_market_state) AS view_rows;

-- 3) Aggregate reconciliation (should match exactly)
SELECT
  v.view_sales, f.fact_sales, (v.view_sales - f.fact_sales) AS sales_diff,
  v.view_profit, f.fact_profit, (v.view_profit - f.fact_profit) AS profit_diff,
  v.view_cogs, f.fact_cogs, (v.view_cogs - f.fact_cogs) AS cogs_diff,
  v.view_marketing, f.fact_marketing, (v.view_marketing - f.fact_marketing) AS marketing_diff,
  v.view_total_expenses, f.fact_total_expenses,
  (v.view_total_expenses - f.fact_total_expenses) AS total_expenses_diff
FROM
  (SELECT
     SUM(sales)          AS view_sales,
     SUM(profit)         AS view_profit,
     SUM(cogs)           AS view_cogs,
     SUM(marketing)      AS view_marketing,
     SUM(total_expenses) AS view_total_expenses
   FROM vw_margin_by_market_state) v
CROSS JOIN
  (SELECT
     SUM(sales)          AS fact_sales,
     SUM(profit)         AS fact_profit,
     SUM(cogs)           AS fact_cogs,
     SUM(marketing)      AS fact_marketing,
     SUM(total_expenses) AS fact_total_expenses
   FROM coffee_chain_monthly_fact) f;



