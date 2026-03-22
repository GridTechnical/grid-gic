# etl/ingest_solar_wind.py
import os
import datetime as dt
from pathlib import Path
import pandas as pd
import numpy as np
from supabase import create_client
from fetch_solar_wind import fetch_solar_wind_merged

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

def upsert_dataframe(table: str, df: pd.DataFrame, chunk: int = 500):
    if df.empty:
        print("No data to upsert — skipping.")
        return
    sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    
    print(f"Input DF shape to upsert: {df.shape}")
    print(f"Columns: {df.columns.tolist()}")

    # Clean inf/-inf to None
    df = df.replace([np.inf, -np.inf], None)
    
    # Optional: drop fully empty rows only (all columns NaN)
    df = df.dropna(how="all")
    
    # Convert time to ISO string (if it's still Timestamp)
    if 'time' in df.columns and pd.api.types.is_datetime64_any_dtype(df['time']):
        df['time'] = df['time'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

    # NaN → None for JSON (supabase-py handles NaN as NULL, but this is explicit)
    df = df.replace({np.nan: None})

    records = df.to_dict(orient="records")
    print(f"Preparing to upsert {len(records)} records in chunks of {chunk}")

    for i in range(0, len(records), chunk):
        chunk_records = records[i:i+chunk]
        try:
            response = sb.table(table).upsert(chunk_records, on_conflict="time").execute()
            inserted_updated = len(response.data) if response.data else 0
            print(f"Upsert chunk {i//chunk + 1} succeeded - inserted/updated {inserted_updated} rows")
            if response.data:
                print(f"First record response example: {response.data[0]}")
            else:
                print("Warning: upsert returned 0 rows for chunk - likely duplicates or no changes")
        except Exception as e:
            print(f"Upsert chunk {i//chunk + 1} failed: {e}")
            if chunk_records:
                print("First record in chunk:", chunk_records[0])
            raise  # remove 'raise' if you want to continue on failure

    print(f"Backfilled {len(records)} rows")


def main():
    now = dt.datetime.now(dt.timezone.utc)
    start = now - dt.timedelta(hours=6)

    print(f"Fetching last 6 hours: {start.isoformat()} → {now.isoformat()}")

    # Fetch merged plasma+mag with derived fields
    df = fetch_solar_wind_merged(start, now, resample="1min")
    print(f"Fetched DF shape: {df.shape}")
    print(f"Fetched columns: {df.columns.tolist()}")

    # keep relevant cols if present
    keep = [
        "density","speed","temperature",
        "bx_gsm","by_gsm","bz_gsm","bt",
        "pdyn_npa","bz_south","vbz",
        "clock_angle_rad","newell_proxy"
    ]
    existing = [c for c in keep if c in df.columns]
    df_out = df[existing].dropna(how="all")

    print(f"DF_out shape before upsert: {df_out.shape}")

    # ---- Upsert to Supabase (canonical store)
    upsert_dataframe("solar_wind_minute", df_out)

    # ---- Optional: also publish JSON/CSV for the static site
    out_dir = Path("docs/data")
    out_dir.mkdir(parents=True, exist_ok=True)

    df_out.to_csv(out_dir / "solar_wind_last6h.csv", index_label="time")

    # For the public JSON file we want an ISO time field:
    df_json = df_out.reset_index().rename(columns={"index": "time"})
    if isinstance(df_json["time"], pd.DatetimeIndex):
        df_json["time"] = df_json["time"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        df_json["time"] = df_json["time"].astype(str)

    df_json.to_json(out_dir / "solar_wind_last6h.json", orient="records")

    print(f"Upserted {len(df_out)} rows to Supabase and wrote /docs/data/*")


if __name__ == "__main__":
    main()
