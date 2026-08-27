"""Live L1 solar wind from NOAA SWPC RTSW 1-minute JSON."""
from __future__ import annotations

import datetime as dt
from typing import Optional

import numpy as np
import pandas as pd
import requests

MAG_URL = "https://services.swpc.noaa.gov/json/rtsw/rtsw_mag_1m.json"
WIND_URL = "https://services.swpc.noaa.gov/json/rtsw/rtsw_wind_1m.json"

# n (cm^-3) * v (km/s)^2 -> nPa
PDYN_FACTOR = 1.6726e-6
SOURCE_RANK = {"IMAP": 0, "DSCOVR": 1, "ACE": 2, "SOLAR1": 3}
SENTINELS = [-9999, -999.9, 99999.9, 9999.99, 9999999.9]


def _load(url: str) -> list:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list) or not data:
        raise RuntimeError(f"Empty RTSW payload from {url}")
    return data


def _pick_best(df: pd.DataFrame) -> pd.DataFrame:
    """One row per timestamp: active source first, then preferred spacecraft."""
    out = df.copy()
    src = out["source"] if "source" in out.columns else pd.Series("ZZZ", index=out.index)
    out["_src_rank"] = src.map(lambda s: SOURCE_RANK.get(str(s).upper(), 9) if pd.notna(s) else 9)
    if "active" in out.columns:
        out["_active_rank"] = (~out["active"].fillna(False).astype(bool)).astype(int)
    else:
        out["_active_rank"] = 1
    if "overall_quality" in out.columns:
        out["_qual"] = pd.to_numeric(out["overall_quality"], errors="coerce").fillna(99)
    else:
        out["_qual"] = 99
    out = out.sort_values(["time", "_active_rank", "_qual", "_src_rank"])
    out = out.drop_duplicates(subset=["time"], keep="first")
    return out.drop(columns=["_src_rank", "_active_rank", "_qual"], errors="ignore")


def _add_derived(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    n = pd.to_numeric(out.get("density"), errors="coerce")
    v = pd.to_numeric(out.get("speed"), errors="coerce")
    if "pdyn_npa" not in out.columns:
        out["pdyn_npa"] = PDYN_FACTOR * n * (v ** 2)
    else:
        p = pd.to_numeric(out["pdyn_npa"], errors="coerce")
        out["pdyn_npa"] = p.where(p.notna(), PDYN_FACTOR * n * (v ** 2))
    bz = pd.to_numeric(out.get("bz_gsm"), errors="coerce")
    by = pd.to_numeric(out.get("by_gsm"), errors="coerce")
    speed = pd.to_numeric(out.get("speed"), errors="coerce")
    bt = pd.to_numeric(out.get("bt"), errors="coerce")
    out["bz_south"] = bz.clip(upper=0)
    out["vbz"] = speed * bz
    out["clock_angle_rad"] = np.arctan2(by, bz)
    s = speed.clip(lower=0)
    bt = bt.clip(lower=0)
    th = np.abs(out["clock_angle_rad"])
    out["newell_proxy"] = (s ** (4 / 3)) * (bt ** (2 / 3)) * (np.sin(th / 2) ** (8 / 3))
    return out


def fetch_solar_wind_merged(start, end, resample: Optional[str] = "1min") -> pd.DataFrame:
    """Return 1-minute merged plasma+mag with derived coupling fields.

    Index is UTC DatetimeIndex. Columns match solar_wind_minute / the dashboard.
    """
    mag = pd.DataFrame(_load(MAG_URL))
    wind = pd.DataFrame(_load(WIND_URL))

    mag["time"] = pd.to_datetime(mag["time_tag"], utc=True, errors="coerce")
    wind["time"] = pd.to_datetime(wind["time_tag"], utc=True, errors="coerce")
    mag = mag.dropna(subset=["time"])
    wind = wind.dropna(subset=["time"])

    mag = _pick_best(mag)
    wind = _pick_best(wind)

    mag_keep = mag.set_index("time")[["bx_gsm", "by_gsm", "bz_gsm", "bt"]]
    wind_ren = wind.set_index("time").rename(
        columns={
            "proton_density": "density",
            "proton_speed": "speed",
            "proton_temperature": "temperature",
        }
    )
    wind_keep = wind_ren[["density", "speed", "temperature"]]

    df = mag_keep.join(wind_keep, how="outer")
    df = df.apply(pd.to_numeric, errors="coerce")
    df = df.replace(SENTINELS, np.nan)

    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)
    if start_ts.tzinfo is None:
        start_ts = start_ts.tz_localize("UTC")
    else:
        start_ts = start_ts.tz_convert("UTC")
    if end_ts.tzinfo is None:
        end_ts = end_ts.tz_localize("UTC")
    else:
        end_ts = end_ts.tz_convert("UTC")

    df = df[(df.index >= start_ts) & (df.index <= end_ts)]
    df = _add_derived(df)

    if resample:
        df = df.resample(resample).mean(numeric_only=True)

    cols = [
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
    return df[[c for c in cols if c in df.columns]]


def main() -> None:
    now = dt.datetime.now(dt.timezone.utc)
    start = now - dt.timedelta(hours=6)
    df = fetch_solar_wind_merged(start, now, resample="1min")
    print(f"rows={len(df)} cols={list(df.columns)}")
    if len(df):
        print(df.tail(3).to_string())


if __name__ == "__main__":
    main()
