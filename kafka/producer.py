import json
import pandas as pd
from kafka import KafkaProducer
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')


def get_producer():
    return KafkaProducer(
        bootstrap_servers=[BOOTSTRAP_SERVERS],
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
        acks='all',           # wait for all replicas to confirm
        retries=3,
    )


def produce_stock_prices(csv_path):
    logger.info(f"Loading {csv_path} ...")
    df = pd.read_csv(csv_path)
    df['date'] = pd.to_datetime(df['date']).astype(str)

    producer = get_producer()
    sent = 0

    for row in df.itertuples(index=False):
        message = {
            'date': row.date, 'ticker': row.ticker,
            'open': row.open, 'high': row.high,
            'low': row.low, 'close': row.close,
            'volume': int(row.volume),
        }
        producer.send('stock_prices', value=message)
        sent += 1
        if sent % 500 == 0:
            logger.info(f"  Sent {sent} messages ...")

    producer.flush()
    logger.info(f"Done — {sent} stock price messages sent to Kafka")


def produce_transactions(csv_path):
    logger.info(f"Loading {csv_path} ...")
    df = pd.read_csv(csv_path)
    df['date'] = pd.to_datetime(df['date']).astype(str)

    producer = get_producer()
    sent = 0

    for row in df.itertuples(index=False):
        message = {
            'date': row.date, 'user_id': row.user_id, 'ticker': row.ticker,
            'shares': int(row.shares), 'price': float(row.price),
            'action': row.action, 'value': float(row.value),
        }
        producer.send('portfolio_transactions', value=message)
        sent += 1
        if sent % 500 == 0:
            logger.info(f"  Sent {sent} messages ...")

    producer.flush()
    logger.info(f"Done — {sent} transaction messages sent to Kafka")


if __name__ == "__main__":
    base = os.path.join(os.path.dirname(__file__), '..', 'python')

    prices_csv = os.path.join(base, 'stock_prices.csv')
    transactions_csv = os.path.join(base, 'transactions.csv')

    if not os.path.exists(prices_csv):
        raise FileNotFoundError(f"Run fetch_stocks.py first — {prices_csv} not found")
    if not os.path.exists(transactions_csv):
        raise FileNotFoundError(f"Run generate_transactions.py first — {transactions_csv} not found")

    produce_stock_prices(prices_csv)
    produce_transactions(transactions_csv)
