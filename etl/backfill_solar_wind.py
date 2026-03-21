# etl/backfill_solar_wind.py
import os
import sys
import datetime as dt
import pandas as pd
import numpy as np
from supabase import create_client
from fetch_omni import fetch_omni_range

# Debug prints at the very top
print("=== BACKFILL SCRIPT STARTED ===")
print("Current working directory:", os.getcwd())
print("Python path:", sys.executable)
print("Python version:", sys.version)
print("Args:", sys.argv)
print(f"SUPABASE_URL set? {'yes' if os.getenv('SUPABASE_URL') else 'NO'}")
print(f"SUPABASE_SERVICE_KEY set? {'yes' if os.getenv('SUPABASE_SERVICE_KEY') else 'NO'}")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

def upsert_dataframe(table: str, df: pd.DataFrame, chunk: int = 1000):
    if df.empty:
        print("No data to upsert — skipping.")
        return
    sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    # Clean inf/-inf to None (SQL NULL)
    df = df.replace([np.inf, -np.inf], None)
    # Optional: drop rows where critical columns are NaN
    critical_cols = ['time', 'density', 'speed', 'bz_gsm']  # adjust as needed
    df = df.dropna(subset=critical_cols)
    # Convert time index to string ISO for JSON
    payload = df.copy()
    if not isinstance(payload.index, pd.DatetimeIndex):
        raise ValueError("index must be DatetimeIndex")
    payload.index = payload.index.tz_convert("UTC") if payload.index.tz is not None else payload.index.tz_localize("UTC")
    payload.insert(0, "time", payload.index.strftime("%Y-%m-%dT%H:%M:%SZ"))
    # Replace any remaining NaN with None for JSON
    payload = payload.where(pd.notna(payload), None)
    records = payload.reset_index(drop=True).to_dict(orient="records")
    print(f"Preparing to upsert {len(records)} records in chunks of {chunk}")
    for i in range(0, len(records), chunk):
        chunk_records = records[i:i+chunk]
        try:
            response = sb.table(table).upsert(chunk_records, on_conflict="time").execute()
            print(f"Upsert chunk {i//chunk + 1} succeeded - inserted/updated {len(response.data)} rows")
            print(f"First record response example: {response.data[0] if response.data else 'No rows returned'}")
        except Exception as e:
            print(f"Upsert chunk {i//chunk + 1} failed: {e}")
            print("First record in chunk:", chunk_records[0] if chunk_records else "Empty")
            raise
    print(f"Backfilled {len(records)} rows")

def main():
    # Inputs via env (YYYY-MM-DD or full ISO). Defaults to previous UTC day.
    start_env = os.getenv("START_ISO")
    end_env = os.getenv("END_ISO")
    if start_env and end_env:
        start_iso, end_iso = start_env, end_env
    else:
        utc_today = dt.datetime.utcnow().date()
        start_iso = f"{utc_today - dt.timedelta(days=1)}T00:00:00Z"
        end_iso = f"{utc_today}T00:00:00Z"
    print(f"Backfill range: {start_iso} → {end_iso}")

    # Fetch data first
    print("About to call fetch_omni_range...")
    df = fetch_omni_range(start_iso, end_iso, resample="1min")
    print(f"Fetched raw DF shape: {df.shape}")

    # Make time a column for dropna and payload
    df = df.reset_index()  # <--- THIS FIXES THE KeyError
    print(f"DF shape after reset_index: {df.shape}")

    # Quick check for inf/NaN issues **after** fetch
    if np.any(np.isinf(df.select_dtypes(include=[np.number]))):
        print("Warning: inf values detected — replacing with None")
        df = df.replace([np.inf, -np.inf], None)

    keep = [
        "density","speed","temperature","bx_gsm","by_gsm","bz_gsm","bt",
        "pdyn_npa","bz_south","vbz","clock_angle_rad","newell_proxy"
    ]
    existing = [c for c in keep if c in df.columns]
    df_out = df[existing].dropna(how="all")  # drop fully empty rows

    if df_out.empty:
        print("No valid data after cleaning — skipping upsert")
    else:
        upsert_dataframe("solar_wind_minute", df_out)

if __name__ == "__main__":
    main()
