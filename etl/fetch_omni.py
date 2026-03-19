# etl/fetch_omni.py
from __future__ import annotations
import pandas as pd
import numpy as np
import requests
from datetime import datetime, timedelta
from typing import Optional
from io import StringIO

def fetch_omni_range(start_iso: str, end_iso: str, resample: Optional[str] = "1min") -> pd.DataFrame:
    print(f"Fetching OMNI via OMNIWeb CGI: {start_iso} → {end_iso}")

    # Parse dates
    start_dt = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
    end_dt = datetime.fromisoformat(end_iso.replace("Z", "+00:00"))

    # OMNI high-res lags ~3-6 months; clamp end to ~today - 4 months to avoid rejection
    today = datetime.utcnow()
    safe_end = min(end_dt, today - timedelta(days=120))  # adjust lag if needed
    if safe_end < start_dt:
        raise ValueError("Clamped end date is before start — no data possible.")

    print(f"Clamped end date to {safe_end.date()} for availability")

    url = "https://omniweb.gsfc.nasa.gov/cgi/nx1.cgi"
    payload = {
        'activity': 'list',
        'res': 'min',                       # 1-min resolution
        'spacecraft': 'omni_min',
        'start_date': start_dt.strftime('%Y%m%d'),
        'end_date': safe_end.strftime('%Y%m%d'),
        'vars': '13,14,17,18,19,23,24,25',  # Bx,By,Bz,Bt,V,Np,T,P
        'scale': 'Linear',
        'view': '0',
        'table': '0',
        'charsize': '',
        'xstyle': '0',
        'ystyle': '0',
        'symbol': '0',
        'symsize': '',
        'linestyle': 'solid',
        'imagex': '640',
        'imagey': '480',
        'color': '',
        'back': ''
    }

    try:
        r = requests.post(url, data=payload, timeout=90)
        r.raise_for_status()
        text = r.text.strip()
        print(f"Response length: {len(text)} chars")
        print(f"First 500 chars:\n{text[:500]}")  # Debug
    except requests.RequestException as e:
        raise RuntimeError(f"Request failed: {e}")

    # Look for data start
    lines = text.splitlines()
    data_start = None
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith('YEAR') or (stripped and stripped[0].isdigit() and len(stripped.split()) >= 8):
            data_start = i
            break

    if data_start is None:
        # Error response — print full for debug
        print("No data block. Full response:")
        print(text)
        raise RuntimeError(
            "No data block found. Likely causes:\n"
            "1. Date range has no coverage (OMNI lags by months — try earlier end date).\n"
            "2. Server rejected params (check response above).\n"
            "Try shorter range or check https://omniweb.gsfc.nasa.gov/form/omni_min.html for latest data."
        )

    # Parse data
    data_text = '\n'.join(lines[data_start:])
    df = pd.read_csv(
        StringIO(data_text),
        delim_whitespace=True,
        header=None,
        names=[
            'year', 'doy', 'hour', 'min', 'bx_gsm', 'by_gsm', 'bz_gsm', 'bt',
            'speed', 'density', 'temperature', 'pdyn_npa'
        ],
        na_values=['999.9', '99.99', '9999999.9', '9.99E+07', 'NaN'],
        on_bad_lines='skip'
    )

    # Build time
    df['time'] = pd.to_datetime(
        df['year'].astype(str) + ' ' + df['doy'].astype(str),
        format='%Y %j'
    ) + pd.to_timedelta(df['hour'], unit='h') + pd.to_timedelta(df['min'], unit='min')
    df = df.set_index('time').drop(columns=['year', 'doy', 'hour', 'min'], errors='ignore')
    df.index = df.index.tz_localize('UTC')

    df = df.apply(pd.to_numeric, errors='coerce')

    # Derived
    df["bz_south"] = df["bz_gsm"].clip(upper=0)
    df["vbz"] = df["speed"] * df["bz_gsm"]
    df["clock_angle_rad"] = np.arctan2(df["by_gsm"], df["bz_gsm"])
    s = df["speed"].clip(lower=0)
    bt = df["bt"].clip(lower=0)
    th = np.abs(df["clock_angle_rad"])
    df["newell_proxy"] = (s ** (4/3)) * (bt ** (2/3)) * (np.sin(th / 2) ** (8/3))

    if resample:
        df = df.resample(resample).mean(numeric_only=True).ffill(limit=5)

    print(f"Fetched {len(df)} rows")
    return df
from typing import Optional
from io import StringIO

def fetch_omni_range(start_iso: str, end_iso: str, resample: Optional[str] = "1min") -> pd.DataFrame:
    """Fetch 1-min OMNI data via OMNIWeb CGI."""
    print(f"Fetching OMNI via OMNIWeb CGI: {start_iso} → {end_iso}")

    # Parse dates
    start_dt = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
    end_dt = datetime.fromisoformat(end_iso.replace("Z", "+00:00"))

    # OMNI high-res lags; clamp end to ~today - 3 months if needed, but try as-is
    url = "https://omniweb.gsfc.nasa.gov/cgi/nx1.cgi"
    payload = {
        'activity': 'list',                 # List text output
        'res': 'min',                       # 1-min
        'spacecraft': 'omni_min',           # Required for high-res
        'start_date': start_dt.strftime('%Y%m%d'),
        'end_date': end_dt.strftime('%Y%m%d'),
        'vars': '13,14,17,18,19,23,24,25',  # 13=Bx_GSM, 14=By_GSM, 17=Bz_GSM, 18=Bt,
                                            # 19=V (speed), 23=Np (density), 24=T (temp), 25=P (pdyn)
        'scale': 'Linear',
        'view': '0',
        'table': '0',
        'charsize': '',
        'xstyle': '0',
        'ystyle': '0',
        'symbol': '0',
        'symsize': '',
        'linestyle': 'solid',
        'imagex': '640',
        'imagey': '480',
        'color': '',
        'back': ''
    }

    try:
        r = requests.post(url, data=payload, timeout=90)
        r.raise_for_status()
        text = r.text
        print(f"Response length: {len(text)} chars")
        print(f"First 500 chars of response: {text[:500]}")  # Debug: see if error page
    except requests.RequestException as e:
        raise RuntimeError(f"Request failed: {e}")

    # Find data start (after headers, look for line starting with YEAR or numeric year)
    lines = text.splitlines()
    data_start = None
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith('YEAR') or (stripped and stripped[0].isdigit() and len(stripped.split()) > 5):
            data_start = i
            break

    if data_start is None:
        # No data — print response for debug
        print("No data block found. Full response excerpt:")
        print(text[:2000])  # First 2k chars to see error
        raise RuntimeError("No data block found in OMNIWeb response. Check date range (OMNI lags by months) or vars codes. Response may indicate no coverage.")

    # Parse data
    data_text = '\n'.join(lines[data_start:])
    df = pd.read_csv(
        StringIO(data_text),
        delim_whitespace=True,
        header=None,
        names=[
            'year', 'doy', 'hour', 'min', 'bx_gsm', 'by_gsm', 'bz_gsm', 'bt',
            'speed', 'density', 'temperature', 'pdyn_npa'
        ],
        na_values=['999.9', '99.99', '9999999.9', '9.99E+07'],  # OMNI missing markers
        on_bad_lines='skip'
    )

    # Build time index
    df['time'] = pd.to_datetime(
        df['year'].astype(str) + ' ' + df['doy'].astype(str),
        format='%Y %j'
    ) + pd.to_timedelta(df['hour'], unit='h') + pd.to_timedelta(df['min'], unit='min')
    df = df.set_index('time').drop(columns=['year', 'doy', 'hour', 'min'])
    df.index = df.index.tz_localize('UTC')

    df = df.apply(pd.to_numeric, errors='coerce')

    # Derived
    df["bz_south"] = df["bz_gsm"].clip(upper=0)
    df["vbz"] = df["speed"] * df["bz_gsm"]
    df["clock_angle_rad"] = np.arctan2(df["by_gsm"], df["bz_gsm"])
    s = df["speed"].clip(lower=0)
    bt = df["bt"].clip(lower=0)
    th = np.abs(df["clock_angle_rad"])
    df["newell_proxy"] = (s ** (4/3)) * (bt ** (2/3)) * (np.sin(th / 2) ** (8/3))

    if resample:
        df = df.resample(resample).mean(numeric_only=True).ffill(limit=5)

    print(f"Fetched {len(df)} rows")
    return df
