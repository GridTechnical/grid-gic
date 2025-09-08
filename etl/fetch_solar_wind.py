# etl/ingest_solar_wind.py
import os
import json
import datetime as dt
from pathlib import Path
import pandas as pd
import numpy as np

# --- import fetcher from scripts/ ---
import sys, os as _os
REPO_ROOT = _os.path.abspath(_os.path.join(_os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)
from scripts.fetch_solar_wind import fetch_solar_wind_merged
# If you moved the file to etl/, use:
# from fetch_solar_wind import fetch_solar_wind_merged

from supabase import create_client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

def upsert_dataframe(table: str, df: pd.DataFrame, chunk: int = 500):
    """Upsert a DataFrame into Supabase:
       - Ensure UTC ISO time column
       - Convert NaN/±Inf -> None so JSON is valid
    """
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        raise RuntimeError("Missing SUPABASE_URL / SUPABASE_SERVICE_KEY")
    sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError("df.index must be DatetimeIndex")

    idx = df.index
    if idx.tz is None:
        idx = idx.tz_localize("UTC")
    else:
        idx = idx.tz_convert("UTC")

    payload = df.copy()
    payload.insert(0, "time", idx.strftime("%Y-%m-%dT%H:%M:%SZ"))

    # sanitize all columns for JSON (no NaN/Inf)
    for c in payload.columns:
        if c == "time":
            continue
        s = pd.to_numeric(payload[c], errors="coerce")
        s = s.replace([np.inf, -np.inf], np.nan)
        payload[c] = s.astype(object).where(pd.notna(s), None)

    records = payload.reset_index(drop=True).to_dict(orient="records")
    if not records:
        return

    for i in range(0, len(records), chunk):
        sb.table(table).upsert(records[i:i+chunk], on_conflict="time").execute()

def main():
    now = dt.datetime.now(dt.timezone.utc)
    start = now - dt.timedelta(hours=6)

    df = fetch_solar_wind_merged(start, now, resample="1min")

    keep = [
        "density","speed","temperature",
        "bx_gsm","by_gsm","bz_gsm","bt",
        "pdyn_npa","bz_south","vbz",
        "clock_angle_rad","newell_proxy"
    ]
    existing = [c for c in keep if c in df.columns]
    df_out = df[existing].dropna(how="all")

    # --- upsert to Supabase ---
    if len(df_out):
        upsert_dataframe("solar_wind_minute", df_out)
    print(f"Supabase upserted rows: {len(df_out)}")

    # --- publish CSV/JSON for Pages ---
    out_dir = Path("docs/data"); out_dir.mkdir(parents=True, exist_ok=True)
    # CSV
    df_out.to_csv(out_dir / "solar_wind_last6h.csv", index_label="time")

    # Strict JSON (NaN -> null, ISO time field)
    if isinstance(df_out.index, pd.DatetimeIndex):
        idx_iso = (df_out.index.tz_convert("UTC")
                   if df_out.index.tz is not None
                   else df_out.index.tz_localize("UTC")).strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        idx_iso = pd.Index([])

    j = df_out.copy()
    j.insert(0, "time", idx_iso)
    j = j.where(pd.notna(j), None)
    records = j.reset_index(drop=True).to_dict(orient="records")
    with open(out_dir / "solar_wind_last6h.json", "w") as f:
        json.dump(records, f, allow_nan=False)

    print(f"Wrote docs/data/solar_wind_last6h.csv & .json")

if __name__ == "__main__":
    main()
