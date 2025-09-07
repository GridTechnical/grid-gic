# etl/ingest_solar_wind.py
import os
import datetime as dt
from pathlib import Path
import pandas as pd
from supabase import create_client
from fetch_solar_wind import fetch_solar_wind_merged

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

def upsert_dataframe(table: str, df: pd.DataFrame, chunk: int = 500):
    """Upsert a DataFrame into Supabase:
       - ensures 'time' is an ISO-8601 string (UTC, 'Z' suffix)
       - converts NaN -> None so JSON serialization works
    """
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        raise RuntimeError("Missing SUPABASE_URL / SUPABASE_SERVICE_KEY")

    supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

    # Build a payload DataFrame with an ISO 'time' column
    payload = df.copy()

    # Ensure index is tz-aware UTC; create ISO time column
    if not isinstance(payload.index, pd.DatetimeIndex):
        raise ValueError("DataFrame index must be DatetimeIndex")
    if payload.index.tz is None:
        # make it UTC if naive (fetcher already returns tz-aware, this is just a guard)
        payload.index = payload.index.tz_localize("UTC")
    else:
        payload.index = payload.index.tz_convert("UTC")

    # Insert ISO string time (e.g., 2025-09-07T12:34:56Z)
    payload.insert(
        0,
        "time",
        payload.index.to_series().dt.strftime("%Y-%m-%dT%H:%M:%SZ").values,
    )

    # Replace NaN with None for JSON
    payload = payload.where(pd.notna(payload), None)

    # Convert to list of dict records
    records = payload.reset_index(drop=True).to_dict(orient="records")

    # Chunked upsert
    for i in range(0, len(records), chunk):
        _ = supabase.table(table).upsert(records[i : i + chunk], on_conflict="time").execute()

def main():
    now = dt.datetime.now(dt.timezone.utc)
    start = now - dt.timedelta(hours=6)

    # Fetch merged plasma+mag with derived fields
    df = fetch_solar_wind_merged(start, now, resample="1min")

    # keep relevant cols if present
    keep = [
        "density","speed","temperature",
        "bx_gsm","by_gsm","bz_gsm","bt",
        "pdyn_npa","bz_south","vbz",
        "clock_angle_rad","newell_proxy"
    ]
    existing = [c for c in keep if c in df.columns]
    df_out = df[existing].dropna(how="all")

    # ---- Upsert to Supabase (canonical store)
    upsert_dataframe("solar_wind_minute", df_out)

    # ---- Optional: also publish JSON/CSV for the static site
    out_dir = Path("docs/data"); out_dir.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(out_dir / "solar_wind_last6h.csv", index_label="time")
    # For the public JSON file we want an ISO time field:
    df_json = df_out.reset_index().rename(columns={"index":"time"})
    df_json["time"] = pd.to_datetime(df_json["time"], utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    df_json.to_json(out_dir / "solar_wind_last6h.json", orient="records")

    print(f"Upserted {len(df_out)} rows to Supabase and wrote /docs/data/*")

if __name__ == "__main__":
    main()
