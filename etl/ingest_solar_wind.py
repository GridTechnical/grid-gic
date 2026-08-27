# etl/ingest_solar_wind.py
import os
import sys
import json
import datetime as dt
from pathlib import Path

import numpy as np
import pandas as pd
from supabase import create_client

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from fetch_solar_wind import fetch_solar_wind_merged

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
KEEP = [
    "density",
    "speed",
    "temperature",
    "bx_gsm",
    "by_gsm",
    "bz_gsm",
    "bt",
    "pdyn_npa",
    "bz_south",
    "vbz",
    "clock_angle_rad",
    "newell_proxy",
]


def _with_time_column(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if isinstance(out.index, pd.DatetimeIndex):
        idx = out.index
        if idx.tz is None:
            idx = idx.tz_localize("UTC")
        else:
            idx = idx.tz_convert("UTC")
        out = out.reset_index(drop=True)
        out.insert(0, "time", idx.strftime("%Y-%m-%dT%H:%M:%SZ"))
        return out
    if "time" in out.columns:
        t = pd.to_datetime(out["time"], utc=True, errors="coerce")
        out["time"] = t.dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return out


def upsert_dataframe(table: str, df: pd.DataFrame, chunk: int = 500):
    if df.empty:
        print("No data to upsert, skipping.")
        return
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        raise RuntimeError("Missing SUPABASE_URL / SUPABASE_SERVICE_KEY")

    sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    payload = _with_time_column(df)
    payload = payload.replace([np.inf, -np.inf], np.nan)
    payload = payload.where(pd.notna(payload), None)
    records = payload.to_dict(orient="records")
    print(f"Preparing to upsert {len(records)} records in chunks of {chunk}")

    for i in range(0, len(records), chunk):
        chunk_records = records[i : i + chunk]
        response = sb.table(table).upsert(chunk_records, on_conflict="time").execute()
        inserted = len(response.data) if response.data else 0
        print(f"Upsert chunk {i // chunk + 1} succeeded - {inserted} rows returned")


def write_pages_snapshot(df: pd.DataFrame, repo_root: Path) -> None:
    out_dir = repo_root / "docs" / "data"
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = _with_time_column(df)
    payload.to_csv(out_dir / "solar_wind_last6h.csv", index=False)
    records = payload.replace([np.inf, -np.inf], np.nan).where(pd.notna(payload), None).to_dict(
        orient="records"
    )
    with open(out_dir / "solar_wind_last6h.json", "w") as f:
        json.dump(records, f, allow_nan=False)
    print(f"Wrote {out_dir / 'solar_wind_last6h.csv'} and .json")


def main():
    now = dt.datetime.now(dt.timezone.utc)
    start = now - dt.timedelta(hours=6)
    print(f"Fetching last 6 hours: {start.isoformat()} -> {now.isoformat()}")

    df = fetch_solar_wind_merged(start, now, resample="1min")
    existing = [c for c in KEEP if c in df.columns]
    df_out = df[existing].dropna(how="all")
    print(f"Fetched DF shape: {df_out.shape} columns: {df_out.columns.tolist()}")

    repo_root = HERE.parent
    write_pages_snapshot(df_out, repo_root)

    if SUPABASE_URL and SUPABASE_SERVICE_KEY:
        upsert_dataframe("solar_wind_minute", df_out)
    else:
        print("SUPABASE_URL / SUPABASE_SERVICE_KEY not set, skipped upsert")


if __name__ == "__main__":
    main()
