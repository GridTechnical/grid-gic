# etl/backfill_solar_wind.py
import os
import sys
import datetime as dt
from pathlib import Path
import pandas as pd
import numpy as np
from supabase import create_client

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from fetch_omni import fetch_omni_range

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

def upsert_dataframe(table: str, df: pd.DataFrame, chunk: int = 1000):
    if df.empty:
        print("No data to upsert, skipping.")
        return
    sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

    df = df.replace([np.inf, -np.inf], None)

    if 'time' in df.columns and pd.api.types.is_datetime64_any_dtype(df['time']):
        df['time'] = df['time'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

    df = df.replace({np.nan: None})
    df = df.dropna(how="all")

    records = df.to_dict(orient="records")

    for i in range(0, len(records), chunk):
        chunk_records = records[i:i+chunk]
        try:
            sb.table(table).upsert(chunk_records, on_conflict="time").execute()
        except Exception as e:
            print(f"Upsert chunk {i//chunk + 1} failed: {e}")
            print("First record in chunk:", chunk_records[0] if chunk_records else "Empty")
            raise

    print(f"Backfilled {len(records)} rows")

def main():
    start_env = os.getenv("START_ISO")
    end_env = os.getenv("END_ISO")
    if start_env and end_env:
        start_iso, end_iso = start_env, end_env
    else:
        utc_today = dt.datetime.now(dt.timezone.utc).date()
        start_iso = f"{utc_today - dt.timedelta(days=1)}T00:00:00Z"
        end_iso = f"{utc_today}T00:00:00Z"
    print(f"Backfill range: {start_iso} -> {end_iso}")

    print("About to call fetch_omni_range...")
    df = fetch_omni_range(start_iso, end_iso, resample="1min")
    print(f"Fetched raw DF shape: {df.shape}")

    df = df.reset_index()
    print(f"DF shape after reset_index: {df.shape}")

    numeric = df.select_dtypes(include=[np.number])
    if not numeric.empty and np.isinf(numeric.to_numpy()).any():
        print("Warning: inf values detected, replacing with None")
        df = df.replace([np.inf, -np.inf], None)

    keep = [
        "time", "density","speed","temperature","bx_gsm","by_gsm","bz_gsm","bt",
        "pdyn_npa","bz_south","vbz","clock_angle_rad","newell_proxy"
    ]
    existing = [c for c in keep if c in df.columns]
    df_out = df[existing]

    print(f"DF_out shape before upsert: {df_out.shape}")
    if df_out.empty:
        print("No valid data after cleaning, skipping upsert")
    else:
        print("About to upsert data...")
        upsert_dataframe("solar_wind_minute", df_out)

if __name__ == "__main__":
    main()
