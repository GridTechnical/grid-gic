# etl/backfill_historical_weekly.py
"""
One-time script to backfill solar_wind_minute from Jan 1 2024 to Dec 1 2025
in weekly chunks to avoid overloading OMNIWeb.
"""

import os
import sys
from datetime import datetime, timedelta
import pandas as pd
from backfill_solar_wind import main as daily_backfill_main  # reuse your existing logic

# ────────────────────────────────────────────────
# CONFIG
# ────────────────────────────────────────────────

START_DATE = datetime(2024, 1, 1)           # inclusive
END_DATE   = datetime(2025, 12, 1)          # exclusive → up to Nov 30 23:59Z
WEEK_DAYS  = 7                              # chunk size
SLEEP_SEC  = 60                             # delay between weeks (increase if rate-limited)

# ────────────────────────────────────────────────
# MAIN LOOP
# ────────────────────────────────────────────────

def backfill_weekly():
    current_start = START_DATE

    while current_start < END_DATE:
        current_end = min(current_start + timedelta(days=WEEK_DAYS), END_DATE)

        start_iso = current_start.strftime("%Y-%m-%dT00:00:00Z")
        end_iso   = current_end.strftime("%Y-%m-%dT00:00:00Z")

        print(f"\n{'='*60}")
        print(f"Backfilling week: {start_iso} → {end_iso}")
        print(f"{'='*60}")

        # Set environment variables so daily backfill uses them
        os.environ["START_ISO"] = start_iso
        os.environ["END_ISO"]   = end_iso

        try:
            daily_backfill_main()
            print(f"Week completed successfully.")
        except Exception as e:
            print(f"ERROR during week {start_iso} → {end_iso}: {e}")
            print("Stopping. Fix and resume by changing START_DATE.")
            sys.exit(1)

        # Delay to be kind to OMNIWeb server
        print(f"Sleeping {SLEEP_SEC} seconds...")
        import time
        time.sleep(SLEEP_SEC)

        current_start = current_end

    print("\nAll weeks completed. Historical backfill finished.")

if __name__ == "__main__":
    print("Starting weekly historical backfill (2024-01-01 → 2025-12-01)")
    print(f"Chunk size: {WEEK_DAYS} days, sleep: {SLEEP_SEC}s between chunks\n")
    backfill_weekly()
