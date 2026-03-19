# etl/fetch_omni.py
from __future__ import annotations
import pandas as pd
import numpy as np
import requests
from datetime import datetime
from typing import Optional
from io import StringIO

def fetch_omni_range(start_iso: str, end_iso: str, resample: Optional[str] = "1min") -> pd.DataFrame:
    """Fetch 1-min OMNI data via OMNIWeb CGI (no HAPI) for the given ISO range."""
    print(f"Fetching OMNI via OMNIWeb CGI: {start_iso} → {end_iso}")

    # Parse dates (strip Z if present)
    start_dt = datetime.fromisoformat(start_iso.replace("Z", ""))
    end_dt = datetime.fromisoformat(end_iso.replace("Z", ""))

    url = "https://omniweb.gsfc.nasa.gov/cgi/nx1.cgi"
    payload = {
        'activity': 'list',             # List mode (text output)
        'res': '1',                     # 1-min resolution
        'start_date': start_dt.strftime('%Y%m%d'),
        'end_date': end_dt.strftime('%Y%m%d'),
        'vars': '13,14,17,18,19,23,24,25,26',  # Variable codes:
                                                # 13=Bx_gsm, 14=By_gsm, 17=Bz_gsm, 18=Bt,
                                                # 19=plasma speed (V), 23=proton density (Np),
                                                # 24=proton temp, 25=pdyn, 26=others if needed
        'format': 'ascii'               # Plain text output
    }

    try:
        r = requests.post(url, data=payload, timeout=60)
        r.raise_for_status()
    except requests.RequestException as e:
        raise RuntimeError(f"OMNIWeb request failed: {e}\nResponse: {r.text if 'r' in locals() else 'No response'}")

    text = r.text
    # Find start of data (skip headers until YEAR line)
    lines = text.splitlines()
    data_start = next((i for i, line in enumerate(lines) if line.strip().startswith('YEAR')), None)
    if data_start is None:
        raise RuntimeError("No data block found in OMNIWeb response")

    # Read from data_start as space-delimited (fixed-width-ish)
    df = pd.read_csv(
        StringIO('\n'.join(lines[data_start:])),
        delim_whitespace=True,
        header=None,
        names=[
            'year', 'doy', 'hour', 'min', 'bx_gsm', 'by_gsm', 'bz_gsm', 'bt',
            'speed', 'density', 'temperature', 'pdyn_npa', 'other'  # adjust extra cols
        ],
        na_values=['999.9', '99.99', '9999999.9'],  # OMNI missing values
        on_bad_lines='skip'
    )

    # Create datetime index (UTC)
    df['time'] = pd.to_datetime(
        df['year'].astype(str) + ' ' + df['doy'].astype(str),
        format='%Y %j'
    ) + pd.to_timedelta(df['hour'], unit='h') + pd.to_timedelta(df['min'], unit='min')
    df = df.set_index('time').drop(columns=['year', 'doy', 'hour', 'min', 'other'], errors='ignore')
    df.index = df.index.tz_localize('UTC')

    # Clean numerics
    df = df.apply(pd.to_numeric, errors='coerce')

    # Derived fields (same as your original)
    df["bz_south"] = df["bz_gsm"].clip(upper=0)
    df["vbz"] = df["speed"] * df["bz_gsm"]
    df["clock_angle_rad"] = np.arctan2(df["by_gsm"], df["bz_gsm"])
    s = df["speed"].clip(lower=0)
    bt = df["bt"].clip(lower=0)
    th = np.abs(df["clock_angle_rad"])
    df["newell_proxy"] = (s ** (4/3)) * (bt ** (2/3)) * (np.sin(th / 2) ** (8/3))

    if resample:
        df = df.resample(resample).mean(numeric_only=True).ffill(limit=5)

    print(f"Fetched {len(df)} rows from OMNIWeb")
    return df
