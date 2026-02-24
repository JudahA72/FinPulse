from __future__ import annotations

from typing import List, Dict, Any, Optional

import pandas as pd

# Import the modules we already built
from finpulse_py.config import Settings
from finpulse_py.ingest import FetchOhlcv
from finpulse_py.transform import ComputeAnalytics, ComputeRisk
from finpulse_py.db import Connect, InitDb, UpsertPrices, UpsertAnalytics, UpsertRisk


def RunPipeline(settings: Settings) -> Dict[str, Any]:
    """
    Orchestrates the whole pipeline:
      1) Connect to DB and ensure schema exists
      2) Fetch OHLCV data for tickers
      3) Upsert prices into DB (idempotent)
      4) Compute analytics (returns, moving averages, volatility)
      5) Upsert analytics into DB
      6) Compute risk metrics (VaR, Sharpe, Max Drawdown)
      7) Upsert risk metrics into DB

    Returns a summary dict which we print in main.py.
    """

    # --- 1) Connect to the database ---
    # This opens (or creates) the SQLite file at settings.db_path
    conn = Connect(settings.db_path)

    try:
        # --- 2) Initialize schema (safe to run every time) ---
        InitDb(conn)

        # --- 3) Fetch raw price data from the provider ---
        # For MVP, I am only support yfinance, but I keep the provider concept
        # so I can swap to Alpha Vantage later with minimal change.
        if settings.data_provider != "yfinance":
            raise ValueError(
                f"Unsupported DATA_PROVIDER={settings.data_provider}. "
                "For MVP, use yfinance."
            )

        prices_df = FetchOhlcv(settings.tickers)

        # If I got no data, return early so I don’t crash on transforms
        if prices_df.empty:
            return {
                "tickers_requested": settings.tickers,
                "tickers_loaded": [],
                "prices_rows_upserted": 0,
                "analytics_rows_upserted": 0,
                "risk_rows_upserted": 0,
                "message": "No price data returned from provider.",
            }

        # --- 4) Convert price DataFrame -> list of dict rows for DB upsert ---
        # records = [{"ticker": "...", "date": "...", ...}, ...]
        price_rows: List[Dict[str, Any]] = prices_df.to_dict(orient="records")

        # --- 5) Upsert prices into the DB (idempotent) ---
        prices_count = UpsertPrices(conn, price_rows)

        # --- 6) Compute analytics (daily return, MA20, MA50, VOL20) ---
        analytics_df = ComputeAnalytics(prices_df)

        # Some columns may be NaN early in the time series (like MA50)
        # We can still store them; SQLite supports NULL values.
        analytics_rows: List[Dict[str, Any]] = (
            analytics_df.to_dict(orient="records") if not analytics_df.empty else []
        )

        analytics_count = UpsertAnalytics(conn, analytics_rows) if analytics_rows else 0

        # --- 7) Compute risk metrics per ticker ---
        # This returns one row per ticker (as-of latest date)
        risk_df = ComputeRisk(prices_df)

        risk_rows: List[Dict[str, Any]] = (
            risk_df.to_dict(orient="records") if not risk_df.empty else []
        )

        risk_count = UpsertRisk(conn, risk_rows) if risk_rows else 0

        # Determine which tickers actually got loaded (some might fail)
        loaded_tickers = sorted(prices_df["ticker"].unique().tolist())

        # Return a nice summary for printing/logging
        return {
            "tickers_requested": settings.tickers,
            "tickers_loaded": loaded_tickers,
            "prices_rows_upserted": prices_count,
            "analytics_rows_upserted": analytics_count,
            "risk_rows_upserted": risk_count,
            "db_path": settings.db_path,
            "message": "Pipeline completed successfully.",
        }

    finally:
        # Always close the DB connection even if something fails
        conn.close()