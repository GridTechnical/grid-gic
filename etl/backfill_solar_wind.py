# etl/backfill_solar_wind.py
import os, datetime as dt
import pandas as pd
from supabase import create_client
from fetch_omni import fetch_omni_range

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

def upsert_dataframe(table: str, df: pd.DataFrame, chunk: int = 1000):
    sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    # ISO time column (UTC Z), NaN -> None
    payload = df.copy()
    if not isinstance(payload.index, pd.DatetimeIndex):
        raise ValueError("index must be DatetimeIndex")
    payload.index = payload.index.tz_convert("UTC") if payload.index.tz is not None else payload.index.tz_localize("UTC")
    payload.insert(0, "time", payload.index.strftime("%Y-%m-%dT%H:%M:%SZ"))
    payload = payload.where(pd.notna(payload), None)
    records = payload.reset_index(drop=True).to_dict(orient="records")
    for i in range(0, len(records), chunk):
        sb.table(table).upsert(records[i:i+chunk], on_conflict="time").execute()

def main():
    # Inputs via env (YYYY-MM-DD or full ISO). Defaults to previous UTC day.
    start_env = os.getenv("START_ISO")
    end_env   = os.getenv("END_ISO")
    if start_env and end_env:
        start_iso, end_iso = start_env, end_env
    else:
        utc_today = dt.datetime.utcnow().date()
        start_iso = f"{utc_today - dt.timedelta(days=1)}T00:00:00Z"
        end_iso   = f"{utc_today}T00:00:00Z"

    df = fetch_omni_range(start_iso, end_iso, resample="1min")
    keep = [
        "density","speed","temperature","bx_gsm","by_gsm","bz_gsm","bt",
        "pdyn_npa","bz_south","vbz","clock_angle_rad","newell_proxy"
    ]
    existing = [c for c in keep if c in df.columns]
    df_out = df[existing].dropna(how="all")

    if not (os.getenv("SUPABASE_URL") and os.getenv("SUPABASE_SERVICE_KEY")):
        raise RuntimeError("Missing Supabase creds")

    upsert_dataframe("solar_wind_minute", df_out)
    print(f"Backfilled {len(df_out)} rows {start_iso} -> {end_iso}")

if __name__ == "__main__":
    main()
