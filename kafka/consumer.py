"""
Kafka Consumer — StockForge
Reads messages from stock_prices and portfolio_transactions topics
and writes them to Snowflake RAW.kafka_staging in batches.

Run from project root:
    python kafka/consumer.py

Stop with Ctrl+C — offsets are committed only after successful Snowflake insert.
"""

import json
import logging
import os
import signal
import sys
from datetime import datetime
from threading import Thread

import snowflake.connector
from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s [%(threadName)s] %(message)s'
)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
BATCH_SIZE  = 100   # insert to Snowflake every N messages
BATCH_SECS  = 5     # or every N seconds, whichever comes first

# Graceful shutdown flag
running = True

def handle_shutdown(signum, frame):
    global running
    logger.info("Shutdown signal received — draining final batch...")
    running = False

signal.signal(signal.SIGINT,  handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


def get_snowflake_conn():
    return snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database='RAW',
        schema='kafka_staging',
    )


def flush_stock_prices(batch: list, cursor):
    if not batch:
        return
    sql = """
        INSERT INTO RAW.kafka_staging.kafka_stock_prices
            (date, ticker, open, high, low, close, volume, consumed_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    rows = [(
        r['date'], r['ticker'],
        r['open'], r['high'], r['low'], r['close'],
        int(r['volume']),
        datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
    ) for r in batch]
    cursor.executemany(sql, rows)
    logger.info(f"  Inserted {len(rows)} stock price rows to Snowflake")


def flush_transactions(batch: list, cursor):
    if not batch:
        return
    sql = """
        INSERT INTO RAW.kafka_staging.kafka_portfolio_transactions
            (date, user_id, ticker, action, shares, price, value, consumed_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    rows = [(
        r['date'], r['user_id'], r['ticker'],
        r['action'], int(r['shares']), float(r['price']),
        float(r['value']),
        datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
    ) for r in batch]
    cursor.executemany(sql, rows)
    logger.info(f"  Inserted {len(rows)} transaction rows to Snowflake")


def consume_topic(topic: str, flush_fn):
    """Consume a single topic and flush to Snowflake in batches."""
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[BOOTSTRAP_SERVERS],
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='earliest',       # start from beginning
        enable_auto_commit=False,           # manual commit after insert
        consumer_timeout_ms=BATCH_SECS * 1000,
        group_id=f'stockforge-{topic}-consumer',
    )

    conn   = get_snowflake_conn()
    cursor = conn.cursor()
    batch  = []

    logger.info(f"Consumer started — topic: {topic}")

    try:
        while running:
            try:
                for message in consumer:
                    batch.append(message.value)

                    if len(batch) >= BATCH_SIZE:
                        flush_fn(batch, cursor)
                        conn.commit()
                        consumer.commit()
                        batch = []

                    if not running:
                        break

            except StopIteration:
                # consumer_timeout_ms elapsed — flush whatever is in the batch
                pass

            # Flush partial batch on timeout or shutdown
            if batch:
                flush_fn(batch, cursor)
                conn.commit()
                consumer.commit()
                batch = []

    finally:
        if batch:
            flush_fn(batch, cursor)
            conn.commit()
            consumer.commit()
        cursor.close()
        conn.close()
        consumer.close()
        logger.info(f"Consumer stopped — topic: {topic}")


if __name__ == "__main__":
    logger.info("Starting StockForge Kafka consumers...")

    t1 = Thread(
        target=consume_topic,
        args=('stock_prices', flush_stock_prices),
        name='stock-prices-consumer',
        daemon=True,
    )
    t2 = Thread(
        target=consume_topic,
        args=('portfolio_transactions', flush_transactions),
        name='transactions-consumer',
        daemon=True,
    )

    t1.start()
    t2.start()

    # Keep main thread alive until shutdown signal
    t1.join()
    t2.join()

    logger.info("All consumers shut down cleanly.")
