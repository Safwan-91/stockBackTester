import ta
import pandas as pd


def rsi_indicator(df, window=14):
    rsi = ta.momentum.RSIIndicator(close=df["close"], window=window)
    return pd.DataFrame({"rsi": rsi.rsi()}, index=df.index)


def bollinger_bands(df, window=20, window_dev=2):
    bb = ta.volatility.BollingerBands(close=df["close"], window=window, window_dev=window_dev)
    return pd.DataFrame({
        "sma": bb.bollinger_mavg(),
        "upper_bb": bb.bollinger_hband(),
        "lower_bb": bb.bollinger_lband()
    }, index=df.index)
