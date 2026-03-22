import yfinance as yf
import pandas as pd
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

STOCKS = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']
PERIOD = '2y'  # 2 years of daily data


def fetch_stock_data(stocks=STOCKS, period=PERIOD):
    all_data = []

    for stock in stocks:
        try:
            logger.info(f"Fetching {stock} ...")
            ticker = yf.Ticker(stock)
            df = ticker.history(period=period, interval='1d', auto_adjust=True)

            if df.empty:
                logger.warning(f"No data returned for {stock} — skipping")
                continue

            df = df.reset_index()[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
            df.columns = ['date', 'open', 'high', 'low', 'close', 'volume']
            df['ticker'] = stock
            df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)  # strip timezone

            all_data.append(df)
            logger.info(f"  {stock}: {len(df)} records")

        except Exception as e:
            logger.error(f"Error fetching {stock}: {e}")
            continue

    if not all_data:
        raise ValueError("No data fetched for any stock")

    combined = (
        pd.concat(all_data, ignore_index=True)
        .sort_values('date')
        .reset_index(drop=True)
    )
    return combined[['date', 'ticker', 'open', 'high', 'low', 'close', 'volume']]


def quality_checks(df):
    logger.info("Running quality checks ...")
    logger.info(f"  Rows       : {len(df)}")
    logger.info(f"  Stocks     : {sorted(df['ticker'].unique().tolist())}")
    logger.info(f"  Date range : {df['date'].min().date()} → {df['date'].max().date()}")
    logger.info(f"  Nulls      : {df.isnull().sum().sum()}")
    logger.info(f"  Duplicates : {df.duplicated().sum()}")
    logger.info(f"  Rows/stock : {df.groupby('ticker').size().to_dict()}")

    assert df.isnull().sum().sum() == 0, "Nulls found in data"
    assert (df['close'] > 0).all(), "Non-positive close prices found"
    logger.info("  All checks passed")


if __name__ == "__main__":
    df = fetch_stock_data()
    quality_checks(df)

    output_path = os.path.join(os.path.dirname(__file__), 'stock_prices.csv')
    df.to_csv(output_path, index=False)
    logger.info(f"Saved → {output_path} ({len(df)} rows)")
