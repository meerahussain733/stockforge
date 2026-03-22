-- ============================================================
-- StockForge Monitoring Dashboard Queries
-- Run these in Snowflake to check pipeline health
-- Database: ANALYTICS
-- ============================================================

USE DATABASE ANALYTICS;


-- ------------------------------------------------------------
-- 1. ROW COUNT CHECK
--    Are the expected number of rows present in each layer?
-- ------------------------------------------------------------

SELECT 'stg_stock_prices'              AS table_name, COUNT(*) AS row_count FROM staging.stg_stock_prices
UNION ALL
SELECT 'stg_portfolio_transactions',                  COUNT(*) FROM staging.stg_portfolio_transactions
UNION ALL
SELECT 'dim_users',                                   COUNT(*) FROM marts.dim_users
UNION ALL
SELECT 'dim_stocks',                                  COUNT(*) FROM marts.dim_stocks
UNION ALL
SELECT 'fct_daily_portfolio_value',                   COUNT(*) FROM marts.fct_daily_portfolio_value
ORDER BY table_name;


-- ------------------------------------------------------------
-- 2. DATA FRESHNESS
--    When was the most recent record loaded per ticker?
--    Alert if any ticker has not been updated in 2+ days.
-- ------------------------------------------------------------

SELECT
    ticker,
    MAX(price_date)                             AS latest_date,
    DATEDIFF('day', MAX(price_date), CURRENT_DATE) AS days_since_update,
    CASE
        WHEN DATEDIFF('day', MAX(price_date), CURRENT_DATE) > 2 THEN 'STALE ⚠'
        ELSE 'FRESH ✓'
    END                                         AS freshness_status
FROM staging.stg_stock_prices
GROUP BY ticker
ORDER BY ticker;


-- ------------------------------------------------------------
-- 3. MISSING TICKERS
--    Are all 5 expected stocks present in the data?
-- ------------------------------------------------------------

WITH expected AS (
    SELECT value AS ticker
    FROM TABLE(FLATTEN(INPUT => PARSE_JSON('["AAPL","GOOGL","MSFT","AMZN","TSLA"]')))
)
SELECT
    e.ticker,
    CASE WHEN s.ticker IS NOT NULL THEN 'PRESENT ✓' ELSE 'MISSING ✗' END AS status
FROM expected e
LEFT JOIN (SELECT DISTINCT ticker FROM staging.stg_stock_prices) s
    ON e.ticker = s.ticker
ORDER BY e.ticker;


-- ------------------------------------------------------------
-- 4. PRICE ANOMALY CHECK
--    Flag any days where close price moved more than 20%
--    compared to the previous day (potential bad data).
-- ------------------------------------------------------------

WITH daily_change AS (
    SELECT
        price_date,
        ticker,
        close_price,
        LAG(close_price) OVER (PARTITION BY ticker ORDER BY price_date) AS prev_close,
        ROUND(
            (close_price - LAG(close_price) OVER (PARTITION BY ticker ORDER BY price_date))
            / NULLIF(LAG(close_price) OVER (PARTITION BY ticker ORDER BY price_date), 0) * 100
        , 2) AS pct_change
    FROM staging.stg_stock_prices
)
SELECT *
FROM daily_change
WHERE ABS(pct_change) > 20
ORDER BY ABS(pct_change) DESC;


-- ------------------------------------------------------------
-- 5. TOP 5 USERS BY PORTFOLIO VALUE
--    Who has the highest portfolio value on the latest date?
-- ------------------------------------------------------------

SELECT
    user_id,
    value_date,
    total_portfolio_value,
    stocks_held,
    RANK() OVER (ORDER BY total_portfolio_value DESC) AS rank
FROM marts.fct_daily_portfolio_value
WHERE value_date = (SELECT MAX(value_date) FROM marts.fct_daily_portfolio_value)
ORDER BY rank
LIMIT 5;


-- ------------------------------------------------------------
-- 6. DAILY PIPELINE HEALTH
--    For each trading day, how many tickers have data?
--    Should be 5 every day. Flag incomplete days.
-- ------------------------------------------------------------

SELECT
    price_date,
    COUNT(DISTINCT ticker)  AS tickers_loaded,
    CASE
        WHEN COUNT(DISTINCT ticker) = 5 THEN 'COMPLETE ✓'
        ELSE 'INCOMPLETE ✗'
    END                     AS status
FROM staging.stg_stock_prices
GROUP BY price_date
HAVING COUNT(DISTINCT ticker) < 5
ORDER BY price_date DESC
LIMIT 20;


-- ------------------------------------------------------------
-- 7. TRANSACTION SUMMARY BY TYPE
--    Overall BUY vs SELL volume — sanity check on simulator.
-- ------------------------------------------------------------

SELECT
    transaction_type,
    COUNT(*)                        AS trade_count,
    ROUND(SUM(trade_value), 2)      AS total_value,
    ROUND(AVG(trade_value), 2)      AS avg_trade_value,
    ROUND(AVG(shares_traded), 2)    AS avg_shares
FROM staging.stg_portfolio_transactions
GROUP BY transaction_type
ORDER BY transaction_type;
