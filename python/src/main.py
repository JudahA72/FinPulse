from __future__ import annotations

import sys  # Needed to read command-line arguments
from typing import List

# Import Settings loader + pipeline runner
from finpulse_py.config import GetSettings
from finpulse_py.pipeline import RunPipeline


def PrintUsage() -> None:
    """
    Prints how to use this script.
    Keeping this helps you (and reviewers) run the project easily.
    """
    print("Usage:")
    print("  python python/src/main.py run       # runs the pipeline (default)")
    print("  python python/src/main.py help      # prints this message")


def Main(argv: List[str]) -> int:
    """
    Our main function returns an exit code:
      0 = success
      non-zero = failure

    This is standard for CLI programs and helps with automation / CI.
    """
    # If user didn’t provide a command, default to "run"
    command = argv[1].lower() if len(argv) > 1 else "run"

    if command in ("help", "-h", "--help"):
        PrintUsage()
        return 0

    if command != "run":
        print(f"Unknown command: {command}")
        PrintUsage()
        return 2

    # Load configuration from .env/environment
    settings = GetSettings()

    # Run the full end-to-end pipeline
    summary = RunPipeline(settings)

    # Print a human-readable summary
    print("\n=== FinPulse Pipeline Summary ===")
    print(f"DB Path: {summary.get('db_path')}")
    print(f"Tickers requested: {summary.get('tickers_requested')}")
    print(f"Tickers loaded:    {summary.get('tickers_loaded')}")
    print(f"Prices upserted:   {summary.get('prices_rows_upserted')}")
    print(f"Analytics upserted:{summary.get('analytics_rows_upserted')}")
    print(f"Risk upserted:     {summary.get('risk_rows_upserted')}")
    print(f"Message:           {summary.get('message')}")
    print("=================================\n")

    # If pipeline says it failed to load anything, treat that as an error
    if summary.get("tickers_loaded") == []:
        return 1

    return 0


if __name__ == "__main__":
    # sys.argv is the list of CLI arguments, including the script name at argv[0]
    raise SystemExit(Main(sys.argv))