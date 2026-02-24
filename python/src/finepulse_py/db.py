from __future__ import annotations

import sqlite3  # Built-in SQLite library (no separate DB server needed)
from typing import Iterable, Dict, Any  # Useful for typed row inputs


# ----------------------------
# SQL SCHEMA (TABLE DEFINITIONS)
# ----------------------------

PRICE_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS prices (
  ticker TEXT NOT NULL,
  date TEXT NOT NULL,         -- stored as ISO string: YYYY-MM-DD
  open REAL,
  high REAL,
  low REAL,
  close REAL,
  adj_close REAL,
  volume INTEGER,

  -- Primary key enforces uniqueness: only one row per (ticker, date)
  PRIMARY KEY (ticker, date)
);
"""

ANALYTICS_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS analytics (
  ticker TEXT NOT NULL,
  date TEXT NOT NULL,
  daily_return REAL,
  ma20 REAL,
  ma50 REAL,
  vol20 REAL,
  PRIMARY KEY (ticker, date)
);
"""

RISK_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS risk (
  ticker TEXT NOT NULL,
  as_of_date TEXT NOT NULL,   -- latest date used to compute risk metrics
  var_95_1d REAL,
  sharpe REAL,
  max_drawdown REAL,
  PRIMARY KEY (ticker, as_of_date)
);
"""


def Connect(db_path: str) -> sqlite3.Connection:
    """
    Opens a connection to the SQLite database file.

    If the file doesn't exist, SQLite creates it automatically.
    """
    conn = sqlite3.connect(db_path)

    # row_factory makes query results behave like dict-like rows
    # (so you can do row["ticker"] instead of row[0])
    conn.row_factory = sqlite3.Row

    # Enforces foreign key constraints as DB's can default to ignoring them (SQLite quirk)
    conn.execute("PRAGMA foreign_keys = ON;")

    # WAL mode improves concurrency + durability for many read/write operations
    conn.execute("PRAGMA journal_mode = WAL;")

    return conn


def InitDb(conn: sqlite3.Connection) -> None:
    """
    Creates required tables if they do not exist.
    Safe to run every time (idempotent).
    """
    conn.execute(PRICE_SCHEMA_SQL)
    conn.execute(ANALYTICS_SCHEMA_SQL)
    conn.execute(RISK_SCHEMA_SQL)
    conn.commit()  # Persist schema changes


def UpsertPrices(conn: sqlite3.Connection, rows: Iterable[Dict[str, Any]]) -> int:
    """
    Inserts or updates price rows.

    Why "upsert"?
    - If you rerun ingestion, you don't want duplicates.
    - You want the latest data to overwrite old values safely.

    PRIMARY KEY (ticker, date) enables ON CONFLICT upsert.
    """
    sql = """
    INSERT INTO prices (ticker, date, open, high, low, close, adj_close, volume)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(ticker, date) DO UPDATE SET
      open=excluded.open,
      high=excluded.high,
      low=excluded.low,
      close=excluded.close,
      adj_close=excluded.adj_close,
      volume=excluded.volume;
    """

    # Convert dict rows into ordered tuples matching the SQL placeholders
    payload = []
    for r in rows:
        payload.append(
            (
                r["ticker"],        # required
                r["date"],          # required ISO string
                r.get("open"),      # optional
                r.get("high"),
                r.get("low"),
                r.get("close"),
                r.get("adj_close"),
                r.get("volume"),
            )
        )

    cur = conn.cursor()          # Cursor executes SQL statements
    cur.executemany(sql, payload) # Efficient bulk insert/update
    conn.commit()                # Save changes to disk

    # rowcount is "best effort" on SQLite; still useful as feedback
    return cur.rowcount if cur.rowcount is not None else 0


def UpsertAnalytics(conn: sqlite3.Connection, rows: Iterable[Dict[str, Any]]) -> int:
    """
    Same concept as prices, but for computed analytics features.
    """
    sql = """
    INSERT INTO analytics (ticker, date, daily_return, ma20, ma50, vol20)
    VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(ticker, date) DO UPDATE SET
      daily_return=excluded.daily_return,
      ma20=excluded.ma20,
      ma50=excluded.ma50,
      vol20=excluded.vol20;
    """

    payload = []
    for r in rows:
        payload.append(
            (
                r["ticker"],
                r["date"],
                r.get("daily_return"),
                r.get("ma20"),
                r.get("ma50"),
                r.get("vol20"),
            )
        )

    cur = conn.cursor()
    cur.executemany(sql, payload)
    conn.commit()
    return cur.rowcount if cur.rowcount is not None else 0


def UpsertRisk(conn: sqlite3.Connection, rows: Iterable[Dict[str, Any]]) -> int:
    """
    Upsert per-ticker risk metrics computed as-of the latest available date.

    This makes the /risk/{ticker} API endpoint fast:
    Java just reads one row from risk per ticker.
    """
    sql = """
    INSERT INTO risk (ticker, as_of_date, var_95_1d, sharpe, max_drawdown)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(ticker, as_of_date) DO UPDATE SET
      var_95_1d=excluded.var_95_1d,
      sharpe=excluded.sharpe,
      max_drawdown=excluded.max_drawdown;
    """

    payload = []
    for r in rows:
        payload.append(
            (
                r["ticker"],
                r["as_of_date"],
                r.get("var_95_1d"),
                r.get("sharpe"),
                r.get("max_drawdown"),
            )
        )

    cur = conn.cursor()
    cur.executemany(sql, payload)
    conn.commit()
    return cur.rowcount if cur.rowcount is not None else 0