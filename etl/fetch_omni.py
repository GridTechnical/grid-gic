# etl/fetch_omni.py
from __future__ import annotations
import pandas as pd
import numpy as np
from typing import Optional
from hapiclient import hapi

HAPI_SERVER = "https://cdaweb.gsfc.nasa.gov/hapi"
DATASET = "OMNI_HRO2_1MIN"  # Confirmed current 1-min definitive OMNI (HRO2)

# Updated parameter list using lowercase / descriptive names from HRO2
PARAMS_LIST = [
    "Bx_GSM",              # bx_gsm
    "By_GSM",              # by_gsm
    "Bz_GSM",              # bz_gsm
    "Bt",                  # bt (magnitude)
    "flow_speed",          # speed (km/s)
    "proton_density",      # density (n/cm³)
    "proton_temperature",  # temperature (K)
    "Pressure",            # pdyn_npa (dynamic pressure)
    # Add more if needed: "EY", "Mach_number", "Ratio", etc.
]
PARAMS = ",".join(PARAMS_LIST)

def _to_df(data, meta) -> pd.DataFrame:
    df = pd.DataFrame(data)
    
    # Time column handling (HAPI usually returns 'time' in Unix seconds)
    time_col = "time" if "time" in df.columns else "Time"
    if time_col in df.columns:
        df[time_col] = pd.to_datetime(df[time_col].astype(float), unit="s", utc=True, errors="coerce")
        df = df.dropna(subset=[time_col]).set_index(time_col).sort_index()
    
    # Debug: print actual received columns from server
    print(f"Received columns from HAPI: {list(df.columns)}")
    
    # Normalize column names to your convention (case-insensitive mapping)
    rename = {
        "Bx_GSM": "bx_gsm",
        "By_GSM": "by_gsm",
        "Bz_GSM": "bz_gsm",
        "BT": "bt", "Bt": "bt",
        "flow_speed": "speed", "Flow_speed": "speed",
        "proton_density": "density", "Proton_density": "density",
        "proton_temperature": "temperature", "Proton_temperature": "temperature",
        "Pressure": "pdyn_npa",  # dynamic pressure
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
    
    # Coerce numerics
    for c in df.columns:
        if c != time_col:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    
    # Derived fields (your original logic, made safe)
    if "density" in df.columns and "speed" in df.columns:
        df["pdyn_npa"] = 1.6726e-6 * df["density"] * (df["speed"] ** 2)
    
    if "bz_gsm" in df.columns:
        df["bz_south"] = df["bz_gsm"].clip(upper=0)  # only negative values
    
    if "speed" in df.columns and "bz_gsm" in df.columns:
        df["vbz"] = df["speed"] * df["bz_gsm"]
    
    if {"bt", "by_gsm", "bz_gsm", "speed"}.issubset(df.columns):
        df["clock_angle_rad"] = np.arctan2(df["by_gsm"], df["bz_gsm"])
        s, bt, th = df["speed"], df["bt"], np.abs(df["clock_angle_rad"])
        df["newell_proxy"] = (s.clip(lower=0) ** (4/3)) * \
                             (bt.clip(lower=0) ** (2/3)) * \
                             (np.sin(th / 2) ** (8/3))
    
    return df

def fetch_omni_range(start_iso: str, end_iso: str, resample: Optional[str] = "1min") -> pd.DataFrame:
    """Fetch OMNI HRO2 between ISO datetimes and return a 1-min dataframe with derived columns."""
    print(f"Querying HAPI: dataset={DATASET}, params={PARAMS}")
    print(f"Time range: {start_iso} to {end_iso}")
    
    try:
        # Debug: fetch and print actual available parameters from server
        param_info = hapi(HAPI_SERVER, "parameters", DATASET)
        available_params = [p["name"] for p in param_info["parameters"]]
        print(f"Available parameters in {DATASET}: {available_params}")
        
        # If your desired params aren't there, you'll see it here ↑
        
        data, meta = hapi(HAPI_SERVER, DATASET, PARAMS, start_iso, end_iso)
        df = _to_df(data, meta)
        
        if df.empty:
            print("Warning: No data returned for this range.")
        
        # Resample if requested (1-min is native, but enforce cadence)
        if resample:
            df = df.resample(resample).mean(numeric_only=True).ffill(limit=5)
        
        return df
    
    except Exception as e:
        raise RuntimeError(
            f"HAPI query failed: {str(e)}\n"
            f"Dataset: {DATASET}\n"
            f"Params requested: {PARAMS}\n"
            f"Tip: Check printed available parameters above and adjust PARAMS_LIST."
        )
