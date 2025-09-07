# scripts/fetch_solar_wind.py
from __future__ import annotations
import datetime as dt
from typing import Optional, List
import json, math, io, urllib.request
import pandas as pd

SWPC_BASE = "https://services.swpc.noaa.gov/products/solar-wind"
PLASMA_FEEDS = {
    "2h": f"{SWPC_BASE}/plasma-2-hour.json",
    "6h": f"{SWPC_BASE}/plasma-6-hour.json",
    "3d": f"{SWPC_BASE}/plasma-3-day.json",
    "7d": f"{SWPC_BASE}/plasma-7-day.json",
}
MAG_FEEDS = {
    "2h": f"{SWPC_BASE}/mag-2-hour.json",
    "6h": f"{SWPC_BASE}/mag-6-hour.json",
    "3d": f"{SWPC_BASE}/mag-3-day.json",
    "7d": f"{SWPC_BASE}/mag-7-day.json",
}

def _pick_window(delta: dt.timedelta) -> str:
    h = delta.total_seconds()/3600.0
    if h <= 2:  return "2h"
    if h <= 6:  return "6h"
    if h <= 72: return "3d"
    return "7d"

def _fetch_json_table(url: str) -> pd.DataFrame:
    with urllib.request.urlopen(url, timeout=20) as r:
        data = json.load(io.TextIOWrapper(r, encoding="utf-8"))
    header, rows = data[0], data[1:]
    df = pd.DataFrame(rows, columns=header)

    time_col = next((c for c in df.columns if c.lower() in ("time_tag","time","timestamp","time_tag_gse","time_tag_gsm")), None)
    if not time_col:
        raise ValueError(f"No timestamp column in {url}. Columns: {list(df.columns)}")

    df["time"] = pd.to_datetime(df[time_col], utc=True, errors="coerce")
    for c in df.columns:
        if c not in (time_col, "time"):
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["time"]).drop_duplicates(subset=["time"]).sort_values("time")
    return df.set_index("time")

def _pick(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns: return c
    lower = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in lower: return lower[c.lower()]
    return None

def _compute_derived(plasma: pd.DataFrame, mag: pd.DataFrame) -> pd.DataFrame:
    df = plasma.join(mag, how="outer", lsuffix="_pl", rsuffix="_mag").sort_index()
    col_n    = _pick(df, ["density"])
    col_v    = _pick(df, ["speed"])
    col_temp = _pick(df, ["temperature"])
    col_by   = _pick(df, ["by_gsm","by_gse","by"])
    col_bz   = _pick(df, ["bz_gsm","bz_gse","bz"])
    col_bt   = _pick(df, ["bt"])

    if col_n and col_v:
        df["pdyn_npa"] = 1.6726e-6 * df[col_n] * (df[col_v] ** 2)
    if col_bz:
        df["bz_south"] = df[col_bz].where(df[col_bz] < 0, 0)
    if col_v and col_bz:
        df["vbz"] = df[col_v] * df[col_bz]
    if col_bt and col_by and col_bz:
        import math
        mask = df[col_by].notna() & df[col_bz].notna()
        df["clock_angle_rad"] = pd.NA
        df.loc[mask, "clock_angle_rad"] = df.loc[mask].apply(
            lambda r: math.atan2(r[col_by], r[col_bz]), axis=1
        )
        if col_v:
            def _newell(v, bt, theta):
                if pd.isna(v) or pd.isna(bt) or pd.isna(theta): return pd.NA
                return (max(v,0)**(4/3)) * (max(bt,0)**(2/3)) * (math.sin(abs(theta)/2)**(8/3))
            df["newell_proxy"] = [
                _newell(df[col_v].iat[i], df[col_bt].iat[i], df["clock_angle_rad"].iat[i])
                for i in range(len(df))
            ]
    return df

def fetch_solar_wind_merged(
    start: dt.datetime,
    end: dt.datetime,
    resample: Optional[str] = "1min",
    ffill_limit: int = 5,
) -> pd.DataFrame:
    if start.tzinfo is None:
        start = start.replace(tzinfo=dt.timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=dt.timezone.utc)
    if end <= start:
        raise ValueError("end must be after start")
    if (end - start) > dt.timedelta(days=7):
        raise ValueError("Range > 7 days; use archive fallback for longer windows.")

    window = _pick_window(end - start)
    plasma = _fetch_json_table(PLASMA_FEEDS[window])
    mag = _fetch_json_table(MAG_FEEDS[window])

    plasma = plasma.loc[(plasma.index >= start) & (plasma.index <= end)]
    mag = mag.loc[(mag.index >= start) & (mag.index <= end)]
    merged = _compute_derived(plasma, mag)

    # ensure numeric before resampling
    merged = merged.apply(pd.to_numeric, errors="coerce")

    if resample:
        merged = merged.resample(resample).mean(numeric_only=True)
        if ffill_limit:
            merged = merged.ffill(limit=ffill_limit)

    return merged.loc[(merged.index >= start) & (merged.index <= end)]
