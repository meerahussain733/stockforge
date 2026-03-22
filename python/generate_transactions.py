import pandas as pd
import numpy as np
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

NUM_USERS = 50
DAYS_TO_SIMULATE = 50
STARTING_CASH = 50_000.0
TRADE_PROBABILITY = 0.3   # 30% chance a user acts on any given day/stock
MA_WINDOW = 50            # 50-day moving average


def generate_transactions(prices_csv):
    logger.info(f"Loading stock prices from {prices_csv} ...")
    prices = pd.read_csv(prices_csv, parse_dates=['date'])
    stocks = sorted(prices['ticker'].unique().tolist())
    trading_days = sorted(prices['date'].unique())[-DAYS_TO_SIMULATE:]

    logger.info(f"  Stocks        : {stocks}")
    logger.info(f"  Trading days  : {len(trading_days)} ({pd.Timestamp(trading_days[0]).date()} → {pd.Timestamp(trading_days[-1]).date()})")
    logger.info(f"  Users         : {NUM_USERS}")

    # Pre-compute 50-day moving average for each stock
    ma = {}
    for stock in stocks:
        stock_prices = prices[prices['ticker'] == stock].sort_values('date').set_index('date')['close']
        ma[stock] = stock_prices.rolling(window=MA_WINDOW, min_periods=1).mean()

    # Initialise portfolios
    users = [f'user_{i:03d}' for i in range(1, NUM_USERS + 1)]
    portfolios = {
        user: {'cash': STARTING_CASH, 'holdings': {s: 0 for s in stocks}}
        for user in users
    }

    transactions = []
    rng = np.random.default_rng(seed=42)  # fixed seed for reproducibility

    for day in trading_days:
        day_prices = prices[prices['date'] == day].set_index('ticker')['close']

        for user in users:
            for stock in stocks:
                if stock not in day_prices.index:
                    continue

                price = day_prices[stock]
                moving_avg = ma[stock].get(day, price)
                portfolio = portfolios[user]
                holdings = portfolio['holdings'][stock]
                cash = portfolio['cash']

                # Skip this trade with 70% probability — not everyone trades every day
                if rng.random() > TRADE_PROBABILITY:
                    continue

                if price < moving_avg and cash >= price * 10:
                    # Price is below MA — BUY signal
                    max_shares = int(cash * 0.1 / price)  # spend max 10% of cash
                    if max_shares < 1:
                        continue
                    shares = int(rng.integers(1, max_shares + 1))
                    value = round(shares * price, 2)
                    transactions.append({
                        'date': day, 'user_id': user, 'ticker': stock,
                        'shares': shares, 'price': round(price, 2),
                        'action': 'BUY', 'value': value
                    })
                    portfolio['holdings'][stock] += shares
                    portfolio['cash'] -= value

                elif price > moving_avg and holdings > 0:
                    # Price is above MA — SELL signal
                    shares = int(rng.integers(1, min(holdings, 50) + 1))
                    value = round(shares * price, 2)
                    transactions.append({
                        'date': day, 'user_id': user, 'ticker': stock,
                        'shares': shares, 'price': round(price, 2),
                        'action': 'SELL', 'value': value
                    })
                    portfolio['holdings'][stock] -= shares
                    portfolio['cash'] += value

    df = pd.DataFrame(transactions)
    return df


def quality_checks(df):
    logger.info("Running quality checks ...")
    logger.info(f"  Total rows   : {len(df)}")
    logger.info(f"  BUY          : {len(df[df['action'] == 'BUY'])}")
    logger.info(f"  SELL         : {len(df[df['action'] == 'SELL'])}")
    logger.info(f"  Unique users : {df['user_id'].nunique()}")
    logger.info(f"  Unique stocks: {df['ticker'].nunique()}")
    logger.info(f"  Date range   : {df['date'].min().date()} → {df['date'].max().date()}")
    logger.info(f"  Nulls        : {df.isnull().sum().sum()}")

    assert df.isnull().sum().sum() == 0, "Nulls found"
    assert set(df['action'].unique()) <= {'BUY', 'SELL'}, "Unexpected action values"
    assert (df['shares'] > 0).all(), "Non-positive share counts found"
    assert (df['price'] > 0).all(), "Non-positive prices found"
    logger.info("  All checks passed")


if __name__ == "__main__":
    prices_csv = os.path.join(os.path.dirname(__file__), 'stock_prices.csv')
    if not os.path.exists(prices_csv):
        raise FileNotFoundError(f"Run fetch_stocks.py first — {prices_csv} not found")

    df = generate_transactions(prices_csv)
    quality_checks(df)

    output_path = os.path.join(os.path.dirname(__file__), 'transactions.csv')
    df.to_csv(output_path, index=False)
    logger.info(f"Saved → {output_path} ({len(df)} rows)")
