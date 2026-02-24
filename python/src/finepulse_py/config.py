from __future__ import annotations

import os  # Read environment variables like DB_PATH, TICKERS, etc.
from dataclasses import dataclass  # Clean way to bundle settings together
from typing import List  # Type hints for readability

from dotenv import load_dotenv  # Loads variables from a .env file into os.environ


@dataclass(frozen=True)
class Settings:
    """
    A single object holding all configuration values the app needs.

    frozen=True means: once created, it cannot be modified makes it safer + predictable.
    """
    data_provider: str
    db_path: str
    tickers: List[str]


def ParseTickers(raw: str) -> List[str]:
    """
    Converts a string like "AAPL, MSFT, googl" into:
      ["AAPL", "MSFT", "GOOGL"]

    - strips whitespace
    - forces uppercase
    - removes duplicates while keeping the original order
    """
    # Split by commas, strip whitespace, uppercase each ticker, drop empty pieces
    parts = [t.strip().upper() for t in raw.split(",") if t.strip()]

    # Remove duplicates but preserve order
    seen = set()
    out: List[str] = []
    for t in parts:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def GetSettings() -> Settings:
    """
    Loads .env (if present) and returns a Settings object.

    This is used by the pipeline so config is *centralized* and not scattered.
    """
    # Loads environment variables from a .env file (if it exists).
    # If .env doesn't exist, this just does nothing (safe).
    load_dotenv()

    # Use env var if present, otherwise default to "yfinance"
    data_provider = os.getenv("DATA_PROVIDER", "yfinance").strip()

    # Where the SQLite DB file lives (default is ./data/market.db)
    db_path = os.getenv("DB_PATH", "./data/market.db").strip()

    # Default tickers if user doesn't set TICKERS in .env
    tickers_raw = os.getenv("TICKERS", "AAPL,MSFT,GOOGL,JPM,GS")

    # Convert "AAPL,MSFT" -> ["AAPL","MSFT"]
    tickers = ParseTickers(tickers_raw)

    # Return immutable settings object
    return Settings(
        data_provider=data_provider,
        db_path=db_path,
        tickers=tickers,
    )