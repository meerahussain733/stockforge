"""
Great Expectations validation for StockForge raw data.
Validates CSV files before they are loaded into Snowflake.

Run from project root:
    python great_expectations/run_validation.py
"""

import os
import json
import pandas as pd
from datetime import datetime
from great_expectations.dataset import PandasDataset

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PYTHON_DIR = os.path.join(BASE_DIR, '..', 'python')
REPORT_DIR = os.path.join(BASE_DIR, 'reports')

os.makedirs(REPORT_DIR, exist_ok=True)


def validate_stock_prices(df: pd.DataFrame) -> dict:
    dataset = PandasDataset(df)
    results = []

    checks = [
        dataset.expect_table_row_count_to_be_between(min_value=2000, max_value=4000),
        dataset.expect_column_to_exist("date"),
        dataset.expect_column_to_exist("ticker"),
        dataset.expect_column_to_exist("open"),
        dataset.expect_column_to_exist("high"),
        dataset.expect_column_to_exist("low"),
        dataset.expect_column_to_exist("close"),
        dataset.expect_column_to_exist("volume"),
        dataset.expect_column_values_to_not_be_null("date"),
        dataset.expect_column_values_to_not_be_null("ticker"),
        dataset.expect_column_values_to_not_be_null("close"),
        dataset.expect_column_values_to_not_be_null("volume"),
        dataset.expect_column_values_to_be_in_set("ticker", ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]),
        dataset.expect_column_values_to_be_between("close", min_value=0.01, max_value=100000),
        dataset.expect_column_values_to_be_between("open",  min_value=0.01, max_value=100000),
        dataset.expect_column_values_to_be_between("high",  min_value=0.01, max_value=100000),
        dataset.expect_column_values_to_be_between("low",   min_value=0.01, max_value=100000),
        dataset.expect_column_values_to_be_between("volume", min_value=0),
    ]

    for result in checks:
        results.append({
            "expectation": result.expectation_config.expectation_type,
            "kwargs": result.expectation_config.kwargs,
            "success": result.success,
        })

    return results


def validate_transactions(df: pd.DataFrame) -> dict:
    dataset = PandasDataset(df)
    results = []

    checks = [
        dataset.expect_table_row_count_to_be_between(min_value=2000, max_value=5000),
        dataset.expect_column_to_exist("date"),
        dataset.expect_column_to_exist("user_id"),
        dataset.expect_column_to_exist("ticker"),
        dataset.expect_column_to_exist("action"),
        dataset.expect_column_to_exist("shares"),
        dataset.expect_column_to_exist("price"),
        dataset.expect_column_to_exist("value"),
        dataset.expect_column_values_to_not_be_null("date"),
        dataset.expect_column_values_to_not_be_null("user_id"),
        dataset.expect_column_values_to_not_be_null("ticker"),
        dataset.expect_column_values_to_not_be_null("action"),
        dataset.expect_column_values_to_be_in_set("action", ["BUY", "SELL"]),
        dataset.expect_column_values_to_be_in_set("ticker", ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]),
        dataset.expect_column_values_to_be_between("shares", min_value=1),
        dataset.expect_column_values_to_be_between("price",  min_value=0.01),
        dataset.expect_column_values_to_be_between("value",  min_value=0.01),
    ]

    for result in checks:
        results.append({
            "expectation": result.expectation_config.expectation_type,
            "kwargs": result.expectation_config.kwargs,
            "success": result.success,
        })

    return results


def print_results(suite_name: str, results: list):
    passed = sum(1 for r in results if r["success"])
    total  = len(results)
    status = "PASSED" if passed == total else "FAILED"

    print(f"\n{'='*60}")
    print(f"  {suite_name}")
    print(f"  Status : {status}  ({passed}/{total} checks passed)")
    print(f"{'='*60}")

    for r in results:
        icon = "✓" if r["success"] else "✗"
        col  = r["kwargs"].get("column", r["kwargs"].get("column_A", "table"))
        print(f"  {icon}  {r['expectation']}  [{col}]")


def save_report(all_results: dict):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = os.path.join(REPORT_DIR, f"validation_{timestamp}.json")
    with open(report_path, "w") as f:
        json.dump(all_results, f, indent=2, default=str)
    print(f"\n  Report saved → {report_path}")


if __name__ == "__main__":
    prices_csv       = os.path.join(PYTHON_DIR, "stock_prices.csv")
    transactions_csv = os.path.join(PYTHON_DIR, "transactions.csv")

    all_results = {}
    overall_pass = True

    # --- Stock Prices ---
    if not os.path.exists(prices_csv):
        print(f"ERROR: {prices_csv} not found. Run fetch_stocks.py first.")
    else:
        df = pd.read_csv(prices_csv)
        results = validate_stock_prices(df)
        print_results("STOCK PRICES VALIDATION", results)
        all_results["stock_prices"] = results
        if any(not r["success"] for r in results):
            overall_pass = False

    # --- Transactions ---
    if not os.path.exists(transactions_csv):
        print(f"ERROR: {transactions_csv} not found. Run generate_transactions.py first.")
    else:
        df = pd.read_csv(transactions_csv)
        results = validate_transactions(df)
        print_results("PORTFOLIO TRANSACTIONS VALIDATION", results)
        all_results["transactions"] = results
        if any(not r["success"] for r in results):
            overall_pass = False

    save_report(all_results)

    print(f"\n{'='*60}")
    print(f"  OVERALL: {'ALL CHECKS PASSED ✓' if overall_pass else 'SOME CHECKS FAILED ✗'}")
    print(f"{'='*60}\n")

    exit(0 if overall_pass else 1)
