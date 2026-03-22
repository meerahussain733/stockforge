-- ============================================================
-- StockForge - Snowflake Setup Script
-- Run in Snowflake SQL Editor as ACCOUNTADMIN
-- Execute each STEP block one at a time, verify before next
-- ============================================================

-- STEP 1: Create databases
CREATE DATABASE IF NOT EXISTS RAW;
CREATE DATABASE IF NOT EXISTS ANALYTICS;
CREATE DATABASE IF NOT EXISTS DEV;

-- Verify: should see RAW, ANALYTICS, DEV listed
SHOW DATABASES;

-- ============================================================

-- STEP 2: Create schemas
CREATE SCHEMA IF NOT EXISTS RAW.kafka_staging;
CREATE SCHEMA IF NOT EXISTS ANALYTICS.staging;
CREATE SCHEMA IF NOT EXISTS ANALYTICS.marts;
CREATE SCHEMA IF NOT EXISTS ANALYTICS.monitoring;

-- Verify
SHOW SCHEMAS IN DATABASE RAW;
SHOW SCHEMAS IN DATABASE ANALYTICS;

-- ============================================================

-- STEP 3: Create raw tables
CREATE TABLE IF NOT EXISTS RAW.kafka_staging.stock_prices (
    date        TIMESTAMP,
    ticker      VARCHAR(10),
    open        DECIMAL(10,2),
    high        DECIMAL(10,2),
    low         DECIMAL(10,2),
    close       DECIMAL(10,2),
    volume      INTEGER,
    _loaded_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW.kafka_staging.portfolio_transactions (
    date        TIMESTAMP,
    user_id     VARCHAR(20),
    ticker      VARCHAR(10),
    shares      INTEGER,
    price       DECIMAL(10,2),
    action      VARCHAR(10),   -- BUY or SELL
    value       DECIMAL(15,2),
    _loaded_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Verify: should see all columns listed
DESCRIBE TABLE RAW.kafka_staging.stock_prices;
DESCRIBE TABLE RAW.kafka_staging.portfolio_transactions;

-- ============================================================

-- STEP 4: Configure warehouse auto-suspend (CRITICAL — cost control)
-- Without this: $200+/month. With this: ~$8/month
ALTER WAREHOUSE COMPUTE_WH SET AUTO_SUSPEND   = 10;
ALTER WAREHOUSE COMPUTE_WH SET AUTO_RESUME    = TRUE;
ALTER WAREHOUSE COMPUTE_WH SET WAREHOUSE_SIZE = XSMALL;

-- Verify: look for AUTO_SUSPEND = 10, SIZE = XSMALL
SHOW WAREHOUSES;

-- ============================================================

-- STEP 5: Create roles and grant permissions
CREATE ROLE IF NOT EXISTS analyst;
CREATE ROLE IF NOT EXISTS engineer;
CREATE ROLE IF NOT EXISTS transformer;

GRANT USAGE ON WAREHOUSE COMPUTE_WH              TO ROLE engineer;
GRANT USAGE ON WAREHOUSE COMPUTE_WH              TO ROLE analyst;

GRANT ALL   ON DATABASE RAW                      TO ROLE engineer;
GRANT ALL   ON DATABASE ANALYTICS                TO ROLE engineer;
GRANT ALL   ON DATABASE DEV                      TO ROLE engineer;

GRANT ALL   ON SCHEMA RAW.kafka_staging          TO ROLE engineer;
GRANT ALL   ON ALL TABLES IN SCHEMA RAW.kafka_staging TO ROLE engineer;

GRANT SELECT ON ALL TABLES IN SCHEMA RAW.kafka_staging TO ROLE analyst;
GRANT ALL    ON ALL SCHEMAS IN DATABASE ANALYTICS      TO ROLE engineer;

-- Replace YOUR_USERNAME with your actual Snowflake username
-- GRANT ROLE engineer TO USER YOUR_USERNAME;

-- ============================================================
-- DONE. Now go back to the project and run:
--   source .venv/bin/activate
--   python python/test_snowflake.py
-- Expected: "Connected to Snowflake" with database/schema/warehouse printed
-- ============================================================
