# StockForge — Real-Time Stock Analytics Pipeline

Production-grade data engineering pipeline that ingests, transforms, and validates stock market data end-to-end.

**Stack**: Yahoo Finance → Kafka → Airflow → dbt → Snowflake + Great Expectations
**Cost**: $0 (trial) → ~$8–15/month (production)

---

## Architecture

```
Yahoo Finance (yfinance)
    ↓
Kafka Topics: stock_prices, portfolio_transactions
    ↓
Great Expectations (CSV validation before load)
    ↓
Snowflake RAW — kafka_staging schema
    ↓
Airflow DAG — daily_stock_ingestion (2 AM UTC)
    ↓
dbt Transformations (5 models, 34 tests)
    ├── Staging: stg_stock_prices, stg_portfolio_transactions
    ├── Dimensions: dim_users, dim_stocks
    └── Facts: fct_daily_portfolio_value
    ↓
ANALYTICS schema (Snowflake)
    ↓
Monitoring Dashboard (SQL — freshness, anomalies, health checks)
```

---

## Key Design Decisions

- Kafka KRaft mode (no Zookeeper)
- Snowflake auto-suspend = 10 min
- yfinance for market data ingestion
- dbt staging + marts layer separation
- Great Expectations on raw CSVs before load
- Batch INSERT over write_pandas for Snowflake loading

---

## Data

- **5 stocks**: AAPL, GOOGL, MSFT, AMZN, TSLA
- **2 years** of daily OHLCV prices (2505 rows)
- **3050 simulated transactions** — 50 users, BUY/SELL via 50-day moving average logic
- **34 dbt tests** — all passing

---

## Prerequisites

- Python 3.9+
- Docker Desktop
- Snowflake trial account

---

## Setup

### 1. Clone and install dependencies

```bash
git clone <your-repo-url>
cd stockforge
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure environment variables

```bash
cp .env.example .env
# Fill in your Snowflake credentials
```

### 3. Run Snowflake setup

Run `scripts/snowflake_setup.sql` in the Snowflake UI to create databases, schemas, and tables.

### 4. Fetch and load data

```bash
python python/fetch_stocks.py
python python/generate_transactions.py
python python/load_to_snowflake.py
```

### 5. Validate data quality

```bash
python great_expectations/run_validation.py
```

### 6. Start Kafka

```bash
docker compose up -d
bash kafka/topics.sh
python kafka/producer.py
```

### 7. Run dbt transformations

```bash
dbt run --profiles-dir dbt/ --project-dir dbt/
dbt test --profiles-dir dbt/ --project-dir dbt/
```

### 8. Start Airflow

```bash
docker compose -f airflow/docker-compose-airflow.yml up airflow-init
docker compose -f airflow/docker-compose-airflow.yml up -d
# UI: http://localhost:8080  (airflow / airflow)
```

---

## Project Structure

```
stockforge/
├── python/                  # Ingestion scripts
│   ├── fetch_stocks.py      # Fetch OHLCV data via yfinance
│   ├── generate_transactions.py  # Simulate portfolio trades
│   ├── load_to_snowflake.py # Batch load to Snowflake
│   └── test_snowflake.py    # Connection test
├── kafka/                   # Streaming layer
│   ├── producer.py          # Publish CSV data to Kafka topics
│   └── topics.sh            # Create Kafka topics
├── airflow/                 # Orchestration
│   ├── dags/
│   │   └── daily_ingestion.py  # Main pipeline DAG (2 AM UTC)
│   └── docker-compose-airflow.yml
├── dbt/                     # SQL transformations
│   ├── models/
│   │   ├── staging/         # stg_stock_prices, stg_portfolio_transactions
│   │   ├── marts/           # dim_users, dim_stocks, fct_daily_portfolio_value
│   │   └── tests/           # Custom SQL tests
│   ├── macros/
│   ├── dbt_project.yml
│   └── profiles.yml
├── great_expectations/      # Data quality validation
│   └── run_validation.py
├── monitoring/              # Pipeline health queries
│   └── monitoring_queries.sql
├── scripts/                 # Snowflake setup SQL
│   └── snowflake_setup.sql
├── docker-compose.yml       # Kafka (KRaft mode)
├── requirements.txt
└── .env.example
```

---

## Cost Breakdown

| Component | Monthly |
|---|---|
| Snowflake XSMALL (auto-suspend 10 min) | ~$8 |
| Everything else | $0 |
| **Total** | **~$8** |
