from __future__ import annotations

from typing import Dict, Any

import numpy as np
import pandas as pd


def ComputeAnalytics(prices: pd.DataFrame) -> pd.DataFrame:
    """
    Takes prices data and creates time-series analytics per ticker.

    Inputs:
      prices columns: ticker, date, close (plus others if available)

    Outputs:
      analytics columns: ticker, date, daily_return, ma20, ma50, vol20
    """
    if prices.empty:
        return pd.DataFrame()

    df = prices.copy()

    # Convert date string -> datetime so sorting and rolling windows work correctly
    df["date"] = pd.to_datetime(df["date"])

    # Sort ensures rolling windows are computed in the correct time order
    df = df.sort_values(["ticker", "date"])

    # pct_change calculates (today_close / yesterday_close - 1)
    df["daily_return"] = df.groupby("ticker")["close"].pct_change()

    # Rolling moving averages (20-day and 50-day)
    df["ma20"] = df.groupby("ticker")["close"].transform(lambda s: s.rolling(20).mean())
    df["ma50"] = df.groupby("ticker")["close"].transform(lambda s: s.rolling(50).mean())

    # Rolling volatility: standard deviation of daily returns over 20 days
    df["vol20"] = df.groupby("ticker")["daily_return"].transform(lambda s: s.rolling(20).std())

    # Select final columns
    out = df[["ticker", "date", "daily_return", "ma20", "ma50", "vol20"]].copy()

    # Store date back to ISO string for SQLite
    out["date"] = out["date"].dt.date.astype(str)

    return out


def ComputeRisk(prices: pd.DataFrame) -> pd.DataFrame:
    """
    Compute per-ticker 'quant-lite' risk metrics as-of the latest date.

    Metrics:
    - VaR 95% (1-day): 5th percentile of historical daily returns
    - Sharpe ratio (annualized): mean(ret)/std(ret) * sqrt(252)
    - Max Drawdown: worst peak-to-trough decline in the price curve

    Output columns:
      ticker, as_of_date, var_95_1d, sharpe, max_drawdown
    """
    if prices.empty:
        return pd.DataFrame()

    df = prices.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["ticker", "date"])

    results = []

    # Process each ticker separately to avoid mixing timeseries
    for ticker, g in df.groupby("ticker"):
        g = g.copy()

        # Compute daily returns
        g["ret"] = g["close"].pct_change()

        # Drop NaNs caused by pct_change on first row
        rets = g["ret"].dropna()
        if len(rets) < 30:
            # Not enough data: skip risk metrics (or you could compute anyway)
            continue

        # VaR 95%: 5th percentile (historical simulation)
        var_95_1d = float(np.percentile(rets, 5))

        # Sharpe: annualized return / annualized vol approximation
        ret_mean = float(rets.mean())
        ret_std = float(rets.std())

        # Avoid division by zero if std is extremely small
        sharpe = float((ret_mean / ret_std) * np.sqrt(252)) if ret_std != 0 else None

        # Max drawdown:
        # 1) compute running max of close prices
        # 2) compute drawdown = (close / running_max - 1)
        running_max = g["close"].cummax()
        drawdown = (g["close"] / running_max) - 1.0
        max_drawdown = float(drawdown.min())  # most negative value

        # as_of_date should be the latest date in the series
        as_of_date = g["date"].iloc[-1].date().isoformat()

        results.append(
            {
                "ticker": ticker,
                "as_of_date": as_of_date,
                "var_95_1d": var_95_1d,
                "sharpe": sharpe,
                "max_drawdown": max_drawdown,
            }
        )

    return pd.DataFrame(results)