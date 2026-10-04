"""Swarm minute |dB/dt| already stored in geomag.swarm_l1m.

This is the heatmap quantity (max |dB/dt| per minute, µT/s along the track).
It is not a ground magnetometer. Orbital motion mixes spatial gradients into dB/dt.
Prefer NRCan when that fetch works. SuperMAG is MISSING without a user id.
"""
from __future__ import annotations

import os

import pandas as pd
import requests

DEFAULT_URL = "https://uajnhkmwfskddeizcpir.supabase.co"


def _creds() -> tuple[str, str]:
    url = os.environ.get("SUPABASE_URL") or os.environ.get("NEXT_PUBLIC_SUPABASE_URL") or DEFAULT_URL
    key = os.environ.get("SUPABASE_SERVICE_KEY") or os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    if not key:
        raise RuntimeError(
            "MISSING SUPABASE_SERVICE_KEY. Swarm labels need the service key already used by etl/. "
            "Refusing to read a key out of a file."
        )
    return url.rstrip("/"), key


def fetch_swarm_minutes(start: str, end: str, page: int = 1000) -> pd.DataFrame:
    url, key = _creds()
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Accept-Profile": "geomag",
    }
    start_iso = pd.Timestamp(start).tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")
    end_iso = pd.Timestamp(end).tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")
    rows: list[dict] = []
    offset = 0
    while True:
        r = requests.get(
            f"{url}/rest/v1/swarm_l1m",
            headers=headers,
            params={
                "select": "minute_ts,sat_id,lat_mean,lon_mean,max_dbdt_utps",
                "minute_ts": [f"gte.{start_iso}", f"lt.{end_iso}"],
                "order": "minute_ts.asc",
                "limit": str(page),
                "offset": str(offset),
            },
            timeout=120,
        )
        if r.status_code >= 300:
            # Do not include the request URL with the key. The key is a header, not the URL.
            raise RuntimeError(f"swarm_l1m read failed HTTP {r.status_code}")
        batch = r.json()
        if not isinstance(batch, list):
            raise RuntimeError("swarm_l1m returned an unexpected payload")
        rows.extend(batch)
        if len(batch) < page:
            break
        offset += page
    if not rows:
        return pd.DataFrame(columns=["time", "station", "lat", "lon", "dbdt_uts"])
    df = pd.DataFrame(rows)
    df["time"] = pd.to_datetime(df["minute_ts"], utc=True)
    df["lat"] = pd.to_numeric(df["lat_mean"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon_mean"], errors="coerce")
    df["dbdt_uts"] = pd.to_numeric(df["max_dbdt_utps"], errors="coerce")
    df["station"] = "SWARM-" + df["sat_id"].astype(str)
    return df.dropna(subset=["time", "lat", "lon", "dbdt_uts"])[
        ["time", "station", "lat", "lon", "dbdt_uts"]
    ]
