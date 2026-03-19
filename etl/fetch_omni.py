# etl/fetch_omni.py
from __future__ import annotations
import pandas as pd
import numpy as np
from typing import Optional
from hapiclient import hapi

HAPI_SERVER = "https://cdaweb.gsfc.nasa.gov/hapi"
DATASET = "OMNI_HRO2_1MIN"  # correct current dataset

# Correct parameter names: lowercase as used in OMNI_HRO2_1MIN
PARAMS_LIST = [
    "Bx_GSM",               # bx_gsm
    "By_GSM",               # by_gsm
    "Bz_GSM",               # bz_gsm
    "Bt",                   # bt
    "flow_speed",           # speed
    "proton_density",       # density
    "proton_temperature",   # temperature
    "Pressure",             # pdyn_npa
]
PARAMS = ",".join(PARAMS_LIST)

def _to_df(data, meta) -> pd.DataFrame:
    df = pd.DataFrame(data)
    
    # Handle time column (HAPI usually returns 'time' in seconds since epoch)
    time_col = "time" if "time" in df.columns else "Time"
    if time_col in df.columns:
        df[time_col] = pd.to_datetime(df[time_col].astype(float), unit="s", utc=True, errors="coerce")
        df = df.dropna(subset=[time_col]).set_index(time_col).sort_index()
    
    # Print received columns for debugging
    print(f"Received columns from HAPI: {list(df.columns)}")
    
    # Rename to match your expected schema (case-insensitive fallback)
    rename = {
        "Bx_GSM": "bx_gsm",
        "By_GSM": "by_gsm",
        "Bz_GSM": "bz_gsm",
        "BT": "bt", "Bt": "bt",
        "flow_speed": "speed", "Flow_speed": "speed",
        "proton_density": "density", "Proton_density": "density",
        "proton_temperature": "temperature", "Proton_temperature": "temperature",
        "Pressure": "pdyn_npa",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
    
    # Coerce to numeric
    for c in df.columns:
        if c != time_col:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    
    # Derived columns (same logic as before, but safer)
    if "density" in df.columns and "speed" in df.columns:
        df["pdyn_npa"] = 1.6726e-6 * df["density"] * (df["speed"] ** 2)
    
    if "bz_gsm" in df.columns:
        df["bz_south"] = df["bz_gsm"].clip(upper=0)
    
    if "speed" in df.columns and "bz_gsm" in df.columns:
        df["vbz"] = df["speed"] * df["bz_gsm"]
    
    if all(col in df.columns for col in ["bt", "by_gsm", "bz_gsm", "speed"]):
        df["clock_angle_rad"] = np.arctan2(df["by_gsm"], df["bz_gsm"])
        s = df["speed"].clip(lower=0)
        bt = df["bt"].clip(lower=0)
        th = np.abs(df["clock_angle_rad"])
        df["newell_proxy"] = (s ** (4/3)) * (bt ** (2/3)) * (np.sin(th / 2) ** (8/3))
    
    return df

def fetch_omni_range(start_iso: str, end_iso: str, resample: Optional[str] = "1min") -> pd.DataFrame:
    print(f"Querying HAPI: dataset={DATASET}, params={PARAMS}")
    print(f"Range: {start_iso} → {end_iso}")
    
    try:
        # Debug: show what parameters are actually available
        param_info = hapi(HAPI_SERVER, "parameters", DATASET)
        available = [p["name"] for p in param_info["parameters"]]
        print(f"Available parameters in {DATASET}: {available}")
        
        data, meta = hapi(HAPI_SERVER, DATASET, PARAMS, start_iso, end_iso)
        df = _to_df(data, meta)
        
        if df.empty:
            print("Warning: No data returned for the requested range.")
        
        if resample:
            df = df.resample(resample).mean(numeric_only=True).ffill(limit=5)
        
        return df
    
    except Exception as e:
        raise RuntimeError(
            f"HAPI query failed: {str(e)}\n"
            f"Dataset: {DATASET}\n"
            f"Requested params: {PARAMS}\n"
            f"Check the printed available parameters and adjust PARAMS_LIST accordingly."
        )
