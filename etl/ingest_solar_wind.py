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
    """Upsert a DataFrame into Supabase:
       - ensures 'time' is an ISO-8601 string (UTC, 'Z' suffix)
       - converts NaN -> None so JSON serialization works
    """
    if df.empty:
        print("No data to upsert — skipping.")
        return

    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        raise RuntimeError("Missing SUPABASE_URL / SUPABASE_SERVICE_KEY")

    supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

    print(f"Input DF shape to upsert: {df.shape}")
    print(f"Columns: {df.columns.tolist()}")

    # Build a payload DataFrame with an ISO 'time' column
    payload = df.copy()

    # Ensure index is tz-aware UTC; create ISO time column
    if not isinstance(payload.index, pd.DatetimeIndex):
        raise ValueError("DataFrame index must be DatetimeIndex")

    if payload.index.tz is None:
        payload.index = payload.index.tz_localize("UTC")
    else:
        payload.index = payload.index.tz_convert("UTC")

    payload.insert(
        0,
        "time",
        payload.index.to_series().dt.strftime("%Y-%m-%dT%H:%M:%SZ").values,
    )

    # Replace NaN with None for JSON serialization
    payload = payload.replace({np.nan: None})

    # Convert to list of dict records
    records = payload.reset_index(drop=True).to_dict(orient="records")

    print(f"Preparing to upsert {len(records)} records in chunks of {chunk}")

    for i in range(0, len(records), chunk):
        chunk_records = records[i:i + chunk]
        try:
            response = supabase.table(table).upsert(
                chunk_records, on_conflict="time"
            ).execute()

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
            raise  # re-raise to stop on error (remove if you want to continue)

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
