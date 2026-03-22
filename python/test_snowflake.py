import snowflake.connector
from dotenv import load_dotenv
import os
import sys

load_dotenv()

def test_connection():
    required = [
        'SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD',
        'SNOWFLAKE_WAREHOUSE', 'SNOWFLAKE_DATABASE', 'SNOWFLAKE_SCHEMA'
    ]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        print(f"Missing .env values: {missing}")
        sys.exit(1)

    try:
        conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA')
        )
        cursor = conn.cursor()

        # Basic connection check
        cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE();")
        db, schema, wh = cursor.fetchone()
        print(f"Connected to Snowflake")
        print(f"  Database : {db}")
        print(f"  Schema   : {schema}")
        print(f"  Warehouse: {wh}")

        # Verify tables exist
        cursor.execute("SHOW TABLES IN RAW.kafka_staging;")
        tables = [row[1] for row in cursor.fetchall()]
        print(f"  Tables in RAW.kafka_staging: {tables}")

        expected = {'STOCK_PRICES', 'PORTFOLIO_TRANSACTIONS'}
        found = set(t.upper() for t in tables)
        if expected.issubset(found):
            print("Both tables found — setup looks correct")
        else:
            missing_tables = expected - found
            print(f"Missing tables: {missing_tables}")
            print("Run scripts/snowflake_setup.sql in Snowflake first")

        conn.close()

    except Exception as e:
        print(f"Connection failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    test_connection()
