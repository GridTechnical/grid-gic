"""NOAA/NASA OMNIWeb 1-minute GSM solar wind for training history.

The live dashboard strip stays on the operational RTSW feed (etl/fetch_solar_wind.py).
This module does not forward-fill plasma and does not write secrets.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from io import StringIO

import pandas as pd
import requests

OMNI_URL = "https://omniweb.gsfc.nasa.gov/cgi/nx1.cgi"
COLUMNS = [
    "year", "doy", "hour", "min",
    "bx_gsm", "by_gsm", "bz_gsm", "bt",
    "speed", "density", "temperature", "pdyn_omni",
]


def _parse_omni_text(text: str) -> pd.DataFrame:
    if "<H1> Error</H1>" in text or "Wrong value" in text:
        raise RuntimeError("OMNIWeb rejected the request")
    lines = text.splitlines()
    data_start = None
    for i, line in enumerate(lines):
        parts = line.split()
        if len(parts) >= 5 and parts[0].isdigit() and len(parts[0]) == 4:
            data_start = i
            break
    if data_start is None:
        raise RuntimeError("OMNIWeb returned no data rows")
    data_end = len(lines)
    for i in range(data_start, len(lines)):
        stripped = lines[i].strip()
        if "If you have any questions" in stripped or "</pre>" in stripped or "</BODY>" in stripped:
            data_end = i
            break
    df = pd.read_csv(
        StringIO("\n".join(lines[data_start:data_end])),
        sep=r"\s+",
        header=None,
        names=COLUMNS,
        on_bad_lines="skip",
    )
    time = pd.to_datetime(
        df["year"].astype(str) + " " + df["doy"].astype(str),
        format="%Y %j",
        utc=True,
    ) + pd.to_timedelta(df["hour"], unit="h") + pd.to_timedelta(df["min"], unit="min")
    df = df.set_index(time).drop(columns=["year", "doy", "hour", "min"])
    df.index.name = "time"
    return df.apply(pd.to_numeric, errors="coerce")


def _fetch_chunk(start: datetime, end: datetime, timeout: int = 180) -> pd.DataFrame:
    """OMNI high-res lags by months. `end` is clamped by the caller."""
    payload = [
        ("activity", "retrieve"),
        ("res", "min"),
        ("spacecraft", "omni_min"),
        ("start_date", start.strftime("%Y%m%d%H")),
        ("end_date", end.strftime("%Y%m%d%H")),
        ("vars", "14"),  # Bx GSM
        ("vars", "17"),  # By GSM
        ("vars", "18"),  # Bz GSM
        ("vars", "13"),  # |B|
        ("vars", "21"),  # speed
        ("vars", "25"),  # density
        ("vars", "26"),  # temperature
        ("vars", "27"),  # OMNI flow pressure (ignored; Pdyn is recomputed)
    ]
    r = requests.post(OMNI_URL, data=payload, timeout=timeout)
    r.raise_for_status()
    return _parse_omni_text(r.text)


def fetch_omni(start: str | datetime, end: str | datetime) -> pd.DataFrame:
    """1-minute OMNI from start inclusive to end exclusive, in UTC day chunks.

    The OMNI pressure column is kept as pdyn_omni for audit only. Training uses
    Pdyn recomputed from density and speed, and only when both are real.
    """
    start_dt = pd.Timestamp(start)
    end_dt = pd.Timestamp(end)
    if start_dt.tzinfo is None:
        start_dt = start_dt.tz_localize("UTC")
    else:
        start_dt = start_dt.tz_convert("UTC")
    if end_dt.tzinfo is None:
        end_dt = end_dt.tz_localize("UTC")
    else:
        end_dt = end_dt.tz_convert("UTC")
    start_dt = start_dt.to_pydatetime()
    end_dt = end_dt.to_pydatetime()
    # High-res OMNI is not produced for the last ~120 days.
    safe_end = min(end_dt, datetime.now(timezone.utc) - timedelta(days=120))
    if safe_end <= start_dt:
        raise ValueError(
            f"OMNI high-res is not available for {start_dt.isoformat()} → {end_dt.isoformat()} "
            "(product lags about 120 days)"
        )
    frames = []
    cursor = start_dt
    while cursor < safe_end:
        chunk_end = min(cursor + timedelta(days=1), safe_end)
        print(f"OMNI {cursor.strftime('%Y-%m-%d %H:%M')} → {chunk_end.strftime('%Y-%m-%d %H:%M')} UTC")
        frames.append(_fetch_chunk(cursor, chunk_end))
        cursor = chunk_end
    if not frames:
        return pd.DataFrame(columns=COLUMNS[4:])
    df = pd.concat(frames).sort_index()
    df = df[~df.index.duplicated(keep="last")]
    df = df[(df.index >= pd.Timestamp(start_dt)) & (df.index < pd.Timestamp(safe_end))]
    return df
