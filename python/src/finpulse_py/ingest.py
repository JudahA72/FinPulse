from __future__ import annotations

from typing import List

import pandas as pd
import yfinance as yf


def FetchOhlcv(
    tickers: List[str],
    period: str = "2y",
    interval: str = "1d",
) -> pd.DataFrame:
    """
    Fetch daily OHLCV price data for the tickers.

    Returns a DataFrame with columns:
      ticker, date, open, high, low, close, adj_close, volume

    Notes:
    - I store `date` as an ISO string because SQLite handles strings easily.
    - yfinance does not require API keys.
    """
    # Download data from Yahoo Finance through yfinance
    # For multiple tickers, this returns a DataFrame with MultiIndex columns:
    #   (TICKER, ColumnName)
    raw = yf.download(
        tickers=tickers,
        period=period,
        interval=interval,
        group_by="ticker",
        auto_adjust=False,   # keep raw OHLCV (and adj close separately)
        threads=True,
        progress=False,
    )

    # If nothing came back (network issues, invalid tickers), return empty
    if raw.empty:
        return pd.DataFrame()

    frames = []

    # yfinance behaves differently when tickers list has a single ticker:
    # it returns flat columns (no MultiIndex). We handle both cases.
    if len(tickers) == 1:
        t = tickers[0]
        df = raw.copy()

        # Normalize column names to snake_case
        df.columns = [c.lower().replace(" ", "_") for c in df.columns]

        # Convert index into a normal column
        df = df.reset_index()

        # Add ticker column (since single-ticker case doesn't include it)
        df["ticker"] = t

        frames.append(df)
    else:
        # Multi-ticker case: raw[ticker] gives that ticker's OHLCV block
        for t in tickers:
            # If ticker isn’t present (maybe invalid), skip it
            if t not in raw.columns.get_level_values(0):
                continue

            df = raw[t].copy()
            df.columns = [c.lower().replace(" ", "_") for c in df.columns]
            df = df.reset_index()
            df["ticker"] = t
            frames.append(df)

    # Combine all ticker frames into one DataFrame
    out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    # Standardize yfinance naming differences
    # Sometimes it's "adj close" vs "adj_close" depending on versions
    out = out.rename(columns={"adj close": "adj_close"})

    # Convert date column to ISO string for SQLite storage
    out["date"] = pd.to_datetime(out["Date"]).dt.date.astype(str) if "Date" in out.columns else pd.to_datetime(out["date"]).dt.date.astype(str)

    # yfinance output columns usually: Open High Low Close Adj Close Volume
    # Normalize to our schema names
    rename_map = {
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "adj_close",
        "Volume": "volume",
        "open": "open",
        "high": "high",
        "low": "low",
        "close": "close",
        "adj_close": "adj_close",
        "volume": "volume",
    }
    out = out.rename(columns=rename_map)

    # Some rows can be NaN at the beginning; we require close for calculations
    out = out.dropna(subset=["close"])

    # Keep only the columns our DB expects
    keep_cols = ["ticker", "date", "open", "high", "low", "close", "adj_close", "volume"]
    out = out[keep_cols]

    return out