import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta

from indicators import rsi_indicator, bollinger_bands

# ---------- CONFIG ----------
DATA_FOLDER = "nifty_data"  # replace with your folder path
DATE_COL = "date"  # change if your file has different column names
OHLC_COLS = ["open", "high", "low", "close"]


# ---------- 1. Load a single stock file ----------
def load_stock_data(file_path, start_date="2021-01-01", end_date="2023-12-31"):
    """
    Loads 1-min OHLC stock data from CSV and filters by date range.
    """
    df = pd.read_csv(file_path)
    df[DATE_COL] = pd.to_datetime(df[DATE_COL])
    df = df.sort_values(DATE_COL)

    # Filter date range
    mask = (df[DATE_COL] >= pd.to_datetime(start_date)) & (df[DATE_COL] <= pd.to_datetime(end_date))
    df = df.loc[mask].reset_index(drop=True)

    return df


# ---------- 2. Get all expiry dates ----------
def get_monthly_expiries(start_date, end_date, df):
    """
    Finds last Thursday of each month between start_date and end_date.
    """
    expiries = []
    current = start_date.replace(day=1)

    while current <= end_date:
        # Go to last day of the month
        next_month = (current.replace(day=28) + timedelta(days=4)).replace(day=1)
        last_day = next_month - timedelta(days=1)

        # Find last Thursday
        last_thursday = last_day
        expiry_found = False
        for i in range(15):  # 3 = Thursday
            if last_thursday.weekday() == 3:
                expiry_found = True
                if not df[df[DATE_COL].dt.date == last_thursday.date()].empty:
                    break
            if expiry_found and not df[df[DATE_COL].dt.date == last_thursday.date()].empty:
                break
            else:
                last_thursday -= timedelta(days=1)

        expiries.append(last_thursday)
        current = next_month

    return expiries


def get_expiry_closes(df, expiries):
    """
    For each expiry date, find the last available close price in the 1-min data.
    Returns a DataFrame with expiry_date and expiry_close.
    """
    results = []

    for exp in expiries:
        # Filter data for this expiry day
        exp_day_data = df[df[DATE_COL].dt.date == exp.date()]

        if exp_day_data.empty:
            continue  # No data for this expiry (e.g., stock not traded or missing file)

        # Last available candle on expiry day
        expiry_close = exp_day_data.iloc[-1]["close"]

        results.append({"expiry": exp, "expiry_close": expiry_close})

    return pd.DataFrame(results)


def add_indicators(df, indicator_funcs):
    """
    Add technical indicators to OHLC dataframe using user-provided functions.

    Parameters
    ----------
    df : pd.DataFrame
        Must contain columns: ['open', 'high', 'low', 'close', 'volume']
    indicator_funcs : list
        A list of functions. Each function must take `df` as input
        and return a pd.DataFrame with indicator columns.

    Returns
    -------
    df : pd.DataFrame
        Original OHLCV + indicator columns
    """
    df = df.copy()

    for func in indicator_funcs:
        df = pd.concat([df, func(df)], axis=1)

    return df


def resample_intraday(df, interval="4H", market_open="09:15", market_close="15:30"):
    """
    Resample 1-min intraday data into custom interval candles.
    Ensures that the last session always ends at market close.

    Parameters:
        df (pd.DataFrame): 1-min OHLCV data with datetime column.
        interval (str): Pandas offset string (e.g. '2H', '3H', '4H').
        market_open (str): Market open time (default: '09:15').
        market_close (str): Market close time (default: '15:30').
    """
    df = df.copy()
    df[DATE_COL] = pd.to_datetime(df[DATE_COL])
    df.set_index(DATE_COL, inplace=True)

    session_data = []

    for date, group in df.groupby(df.index.date):
        day_df = group.between_time(market_open, market_close)
        if day_df.empty:
            continue

        # Generate session boundaries
        start = pd.Timestamp(f"{date} {market_open}")
        end = pd.Timestamp(f"{date} {market_close}")
        bins = pd.date_range(start=start, end=end, freq=interval)

        # Force last bin to align with market close
        if bins[-1] != end:
            bins = bins.append(pd.DatetimeIndex([end]))

        # Split into sessions
        for i in range(len(bins) - 1):
            session = day_df[(day_df.index >= bins[i]) & (day_df.index < bins[i + 1])]
            if session.empty:
                continue

            session_data.append({
                DATE_COL: session.index[-1],  # use close timestamp
                "open": session["open"].iloc[0],
                "high": session["high"].max(),
                "low": session["low"].min(),
                "close": session["close"].iloc[-1],
                "volume": session["volume"].sum(),
                "session_start": bins[i],
                "session_end": bins[i + 1]
            })

    return pd.DataFrame(session_data).sort_values(DATE_COL).reset_index(drop=True)


def summarize_performance(results):
    """
    Summarizes backtest performance.
    """
    summary = {}

    total_trades = len(results)
    total_wins = results["success"].sum()
    win_rate = total_wins / total_trades * 100 if total_trades > 0 else 0

    # Win rate by signal type
    by_signal = results.groupby("signal")["success"].mean() * 100
    by_signal = by_signal.to_dict()

    # Win rate by year
    results["year"] = results["expiry"].dt.year
    by_year = results.groupby("year")["success"].mean() * 100
    by_year = by_year.to_dict()

    summary["total_trades"] = total_trades
    summary["total_wins"] = int(total_wins)
    summary["overall_win_rate"] = round(win_rate, 2)
    summary["win_rate_by_signal"] = {k: round(v, 2) for k, v in by_signal.items()}
    summary["win_rate_by_year"] = {k: round(v, 2) for k, v in by_year.items()}

    return summary
