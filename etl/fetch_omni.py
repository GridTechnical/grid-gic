# etl/fetch_omni.py
from __future__ import annotations
import pandas as pd
from typing import Optional, List, Tuple
from hapiclient import hapi

HAPI_SERVER = "https://cdaweb.gsfc.nasa.gov/hapi"
# 1-min historical solar-wind/IMF dataset
DATASET = "OMNI_HRO2_1MIN"  # swap to OMNI_HRO2_5MIN if volume is too large

# We’ll ask for a superset of fields; HAPI will drop those not available
PARAMS = ",".join([
    "BX_GSM","BY_GSM","BZ_GSM","BT",
    "flow_speed","proton_density","proton_temperature",
    # If proton_* names differ on some spans, HAPI may expose alternates:
    # We'll map later if they come with different names.
])

def _to_df(data, meta) -> pd.DataFrame:
    df = pd.DataFrame(data)
    # Time comes as bytes; ensure datetime index (UTC)
    time_col = "Time" if "Time" in df.columns else "time"
    df[time_col] = pd.to_datetime(df[time_col].astype(str), utc=True, errors="coerce")
    df = df.dropna(subset=[time_col]).set_index(time_col).sort_index()

    # Normalize column names to your convention
    rename = {
        "BX_GSM": "bx_gsm",
        "BY_GSM": "by_gsm",
        "BZ_GSM": "bz_gsm",
        "BT": "bt",
        "flow_speed": "speed",
        "proton_density": "density",
        "proton_temperature": "temperature",
        # sometimes OMNI uses alt capitalization
        "Bt": "bt", "Flow_speed":"speed", "Proton_density":"density", "Proton_temperature":"temperature",
    }
    df = df.rename(columns={k:v for k,v in rename.items() if k in df.columns})

    # numeric coercion
    for c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce")

    # Derived fields
    if "density" in df.columns and "speed" in df.columns:
        df["pdyn_npa"] = 1.6726e-6 * df["density"] * (df["speed"]**2)
    if "bz_gsm" in df.columns:
        df["bz_south"] = df["bz_gsm"].where(df["bz_gsm"] < 0, 0)
    if "speed" in df.columns and "bz_gsm" in df.columns:
        df["vbz"] = df["speed"] * df["bz_gsm"]
    if {"bt","by_gsm","bz_gsm","speed"}.issubset(df.columns):
        import numpy as np
        df["clock_angle_rad"] = np.arctan2(df["by_gsm"], df["bz_gsm"])
        # Newell coupling proxy (scaled, unitless)
        s, bt, th = df["speed"], df["bt"], np.abs(df["clock_angle_rad"])
        df["newell_proxy"] = (s.clip(lower=0)**(4/3)) * (bt.clip(lower=0)**(2/3)) * (np.sin(th/2)**(8/3))

    return df

def fetch_omni_range(start_iso: str, end_iso: str, resample: Optional[str] = "1min") -> pd.DataFrame:
    """Fetch OMNI HRO2 between ISO datetimes and return a 1-min dataframe with derived columns."""
    data, meta = hapi(HAPI_SERVER, DATASET, PARAMS, start_iso, end_iso)  # returns numpy structured array
    df = _to_df(data, meta)
    # Ensure exactly 1-min cadence (fill small gaps if desired)
    if resample:
        df = df.resample(resample).mean(numeric_only=True).ffill(limit=5)
    return df
