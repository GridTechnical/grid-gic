# etl/ingest_solar_wind.py
import os, datetime as dt
from pathlib import Path
import pandas as pd
from fetch_solar_wind import fetch_solar_wind_merged
from supabase import create_client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

def upsert_dataframe(table: str, df: pd.DataFrame, chunk: int = 500):
    supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    records = df.reset_index().rename(columns={"time":"time"}).to_dict(orient="records")
    for i in range(0, len(records), chunk):
        _ = supabase.table(table).upsert(records[i:i+chunk], on_conflict="time").execute()

def main():
    now = dt.datetime.now(dt.timezone.utc)
    start = now - dt.timedelta(hours=6)
    df = fetch_solar_wind_merged(start, now, resample="1min")

    keep = [
        "density","speed","temperature",
        "bx_gsm","by_gsm","bz_gsm","bt",
        "pdyn_npa","bz_south","vbz","clock_angle_rad","newell_proxy"
    ]
    existing = [c for c in keep if c in df.columns]
    df_out = df[existing].dropna(how="all")

    # ---- write to Supabase (canonical store)
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        raise RuntimeError("Missing SUPABASE_URL / SUPABASE_SERVICE_KEY")
    upsert_dataframe("solar_wind_minute", df_out)

    # ---- optional: also publish a thin extract to Pages for the static site
    out_dir = Path("docs/data"); out_dir.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(out_dir / "solar_wind_last6h.csv", index_label="time")
    df_out.reset_index().to_json(out_dir / "solar_wind_last6h.json",
                                 orient="records", date_format="iso")

    print(f"Upserted {len(df_out)} rows to Supabase and wrote /docs/data/*")

if __name__ == "__main__":
    main()
