import os
import sqlite3
import tempfile

from finpulse_py.db import Connect, InitDb, UpsertPrices


def TestInitDbCreatesTables():
    # Create a temporary DB file
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        conn = Connect(db_path)
        try:
            InitDb(conn)

            # Verify tables exist
            tables = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table';"
            ).fetchall()
            table_names = {t["name"] for t in tables}

            assert "prices" in table_names
            assert "analytics" in table_names
            assert "risk" in table_names
        finally:
            conn.close()


def TestUpsertPricesIsIdempotent():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        conn = Connect(db_path)
        try:
            InitDb(conn)

            row = {
                "ticker": "AAPL",
                "date": "2024-01-01",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "adj_close": 100.5,
                "volume": 1000,
            }

            # Insert once
            UpsertPrices(conn, [row])

            # Insert again with modified close
            row2 = dict(row)
            row2["close"] = 200.0
            UpsertPrices(conn, [row2])

            # Confirm only one row exists for that PK, and it's updated
            result = conn.execute(
                "SELECT close FROM prices WHERE ticker=? AND date=?",
                ("AAPL", "2024-01-01"),
            ).fetchone()

            assert result is not None
            assert result["close"] == 200.0
        finally:
            conn.close()