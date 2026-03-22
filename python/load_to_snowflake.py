import snowflake.connector
import pandas as pd
from dotenv import load_dotenv
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()


def get_connection():
    return snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA'),
    )


BATCH_SIZE = 500


def load_stock_prices(csv_path):
    logger.info(f"Reading {csv_path} ...")
    df = pd.read_csv(csv_path)
    df['date'] = pd.to_datetime(df['date'])
    logger.info(f"  Rows to load: {len(df)}")

    conn = get_connection()
    try:
        cursor = conn.cursor()

        # Truncate first to avoid duplicates on re-runs
        cursor.execute("TRUNCATE TABLE RAW.kafka_staging.stock_prices")
        logger.info("  Table truncated")

        # Batch insert — avoids S3 staging entirely
        insert_sql = """
            INSERT INTO RAW.kafka_staging.stock_prices
                (date, ticker, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        rows = [
            (row.date.strftime('%Y-%m-%d %H:%M:%S'), row.ticker, row.open, row.high, row.low, row.close, int(row.volume))
            for row in df.itertuples(index=False)
        ]

        total = 0
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i:i + BATCH_SIZE]
            cursor.executemany(insert_sql, batch)
            total += len(batch)
            logger.info(f"  Inserted {total}/{len(rows)} rows ...")

        conn.commit()

        # Verify
        cursor.execute("SELECT COUNT(*) FROM RAW.kafka_staging.stock_prices")
        count = cursor.fetchone()[0]
        logger.info(f"  Verified: {count} rows in Snowflake")

    finally:
        conn.close()


def load_transactions(csv_path):
    logger.info(f"Reading {csv_path} ...")
    df = pd.read_csv(csv_path, parse_dates=['date'])
    logger.info(f"  Rows to load: {len(df)}")

    conn = get_connection()
    try:
        cursor = conn.cursor()

        cursor.execute("TRUNCATE TABLE RAW.kafka_staging.portfolio_transactions")
        logger.info("  Table truncated")

        insert_sql = """
            INSERT INTO RAW.kafka_staging.portfolio_transactions
                (date, user_id, ticker, shares, price, action, value)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        rows = [
            (row.date.strftime('%Y-%m-%d %H:%M:%S'), row.user_id, row.ticker,
             int(row.shares), float(row.price), row.action, float(row.value))
            for row in df.itertuples(index=False)
        ]

        total = 0
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i:i + BATCH_SIZE]
            cursor.executemany(insert_sql, batch)
            total += len(batch)
            logger.info(f"  Inserted {total}/{len(rows)} rows ...")

        conn.commit()

        cursor.execute("SELECT COUNT(*) FROM RAW.kafka_staging.portfolio_transactions")
        count = cursor.fetchone()[0]
        logger.info(f"  Verified: {count} rows in Snowflake")

    finally:
        conn.close()


if __name__ == "__main__":
    base = os.path.dirname(__file__)

    prices_csv = os.path.join(base, 'stock_prices.csv')
    if not os.path.exists(prices_csv):
        raise FileNotFoundError(f"Run fetch_stocks.py first — {prices_csv} not found")
    load_stock_prices(prices_csv)
    logger.info("Done — stock prices loaded to Snowflake")

    transactions_csv = os.path.join(base, 'transactions.csv')
    if not os.path.exists(transactions_csv):
        raise FileNotFoundError(f"Run generate_transactions.py first — {transactions_csv} not found")
    load_transactions(transactions_csv)
    logger.info("Done — transactions loaded to Snowflake")
