import pandas as pd
import numpy as np
import requests
from datetime import datetime, timedelta, timezone
from typing import Optional
from io import StringIO

def fetch_omni_range(start_iso: str, end_iso: str, resample: Optional[str] = "1min") -> pd.DataFrame:
    print(f"Fetching OMNI via OMNIWeb CGI: {start_iso} → {end_iso}")

    start_dt = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
    end_dt = datetime.fromisoformat(end_iso.replace("Z", "+00:00"))

    today = datetime.now(timezone.utc)
    safe_end = min(end_dt, today - timedelta(days=120))
    if safe_end < start_dt:
        raise ValueError("Clamped end date is before start — no data possible.")

    print(f"Using clamped end date: {safe_end.date()} (UTC)")

    url = "https://omniweb.gsfc.nasa.gov/cgi/nx1.cgi"

    payload = {
        'activity': 'list',
        'res': 'min',
        'spacecraft': 'omni_min',
        'start_date': start_dt.strftime('%Y%m%d'),
        'end_date': safe_end.strftime('%Y%m%d'),
        'vars': '13,14,17,18,19,23,24,25'
    }

    print("Sending payload:", payload)

    try:
        r = requests.post(url, data=payload, timeout=90)
        r.raise_for_status()
        text = r.text.strip()
        print(f"Response length: {len(text)} chars")
        print(f"First 500 chars:\n{text[:500]}\n...")
    except requests.RequestException as e:
        raise RuntimeError(f"Request failed: {e}")

    if '<H1> Error</H1>' in text or 'Wrong value' in text:
        print("Server error page. Full excerpt:")
        print(text[:2000])
        raise RuntimeError("OMNIWeb rejected request. See response above.")

    lines = text.splitlines()

    # Find the first line that starts with a 4-digit year (real data start)
    data_start = None
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped and stripped[0].isdigit() and len(stripped.split()[0]) == 4 and stripped.split()[0].startswith('2025'):  # adjust year if needed
            data_start = i
            break

    if data_start is None:
        print("No data lines found. Full response excerpt:")
        print(text[:2000])
        raise RuntimeError("No data lines detected in response. Likely parsing issue or no coverage.")

    # Take only from the first data line onward
    data_text = '\n'.join(lines[data_start:])

    df = pd.read_csv(
        StringIO(data_text),
        sep=r"\s+",
        header=None,
        names=[
            'year', 'doy', 'hour', 'min', 'bx_gsm', 'by_gsm', 'bz_gsm', 'bt',
            'speed', 'density', 'temperature', 'pdyn_npa'
        ],
        na_values=['999.9', '99.99', '9999999.9', '9.99E+07'],
        on_bad_lines='skip'
    )

    # Build time index
    df['time'] = pd.to_datetime(
        df['year'].astype(str) + ' ' + df['doy'].astype(str),
        format='%Y %j'
    ) + pd.to_timedelta(df['hour'], unit='h') + pd.to_timedelta(df['min'], unit='min')
    df = df.set_index('time').drop(columns=['year', 'doy', 'hour', 'min'], errors='ignore')
    df.index = df.index.tz_localize('UTC')

    df = df.apply(pd.to_numeric, errors='coerce')

    # Derived columns
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
