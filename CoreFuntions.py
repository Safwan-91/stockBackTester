from datetime import timedelta

import pandas as pd

from HelperFunctions import resample_intraday, add_indicators, DATE_COL
from indicators import rsi_indicator, bollinger_bands
from signals import *

bull_signals = [BB_lb_signal_check]
bear_signals = [BB_ub_signal_check]
neutral_signals = []


def generate_signals(df, expiries, resample_period, strike_diff=50):
    """
    Generates mean reversion entry signals with restriction:
    - At most one Bull Put and one Bear Call per expiry cycle.
    - Uses DAILY candles (resampled from 1-min data).
    """

    # --- Resample 1-min into daily OHLC ---
    daily = resample_intraday(df, resample_period)

    # --- Compute indicators ---
    daily = add_indicators(daily, [
        lambda d: rsi_indicator(d, window=14),
        lambda d: bollinger_bands(d, window=20, window_dev=2)
    ])

    signals = []

    # --- Iterate over expiry windows ---
    for i in range(len(expiries) - 1):
        start_date = expiries[i] + timedelta(days=1) if i > 0 else daily[DATE_COL].min()
        end_date = expiries[i + 1]

        cycle_data = daily[(daily[DATE_COL] >= start_date) & (daily[DATE_COL] <= end_date)]

        bull_put_taken = False
        bear_call_taken = False

        for _, row in cycle_data.iterrows():
            # Skip rows without enough data
            if pd.isna(row["rsi"]) or pd.isna(row["sma"]):
                continue

            # --- Bull Put Spread (oversold mean reversion) ---
            if not bull_put_taken:
                for check in bull_signals:
                    if not check(row):
                        break
                else:
                    signals.append({
                        "datetime": row[DATE_COL],
                        "expiry": end_date,
                        "signal": "BULL_PUT",
                        "atm": row["close"]
                    })
                    bull_put_taken = True

            # --- Bear Call Spread (overbought mean reversion) ---
            if not bear_call_taken:
                for check in bear_signals:
                    if not check(row):
                        break
                else:
                    signals.append({
                        "datetime": row[DATE_COL],
                        "expiry": end_date,
                        "signal": "BEAR_CALL",
                        "atm": row["close"]
                    })
                    bear_call_taken = True

            # Stop if both signals taken
            if bull_put_taken and bear_call_taken:
                break

    return pd.DataFrame(signals)


def evaluate_signals(signals, expiry_closes):
    """
    Adds trade outcome (success/failure) to signals based on expiry close.
    """
    results = signals.copy()

    # Merge signals with expiry close data
    results = results.merge(expiry_closes, on="expiry", how="left")

    outcomes = []
    for _, row in results.iterrows():
        success = None

        if row["signal"] == "BULL_PUT":
            success = row["expiry_close"] > row["atm"]

        elif row["signal"] == "BEAR_CALL":
            success = row["expiry_close"] < row["atm"]

        outcomes.append(success)

    results["success"] = outcomes
    return results
