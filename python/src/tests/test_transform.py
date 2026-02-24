import pandas as pd
import numpy as np

from finpulse_py.transform import ComputeAnalytics, ComputeRisk


def TestComputeAnalyticsBasic():
    # Create a tiny deterministic dataset for one ticker
    prices = pd.DataFrame(
        {
            "ticker": ["AAPL"] * 5,
            "date": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"],
            "close": [100.0, 110.0, 121.0, 133.1, 146.41],  # grows 10% daily
        }
    )

    out = ComputeAnalytics(prices)

    # Should have same number of rows
    assert len(out) == 5

    # daily_return at day2 should be 0.10 (10%)
    day2 = out[out["date"] == "2024-01-02"].iloc[0]
    assert abs(day2["daily_return"] - 0.10) < 1e-9


def TestComputeRiskReturnsOneRowPerTicker():
    # Build enough rows to compute risk (>=30 needed in current implementation)
    dates = pd.date_range("2024-01-01", periods=60, freq="D")
    closes = np.linspace(100, 130, 60)  # simple upward line
    prices = pd.DataFrame(
        {"ticker": ["AAPL"] * 60, "date": dates.date.astype(str), "close": closes}
    )

    risk = ComputeRisk(prices)

    # Expect one row for AAPL
    assert len(risk) == 1
    assert risk.iloc[0]["ticker"] == "AAPL"

    # Ensure required fields exist
    assert "var_95_1d" in risk.columns
    assert "sharpe" in risk.columns
    assert "max_drawdown" in risk.columns