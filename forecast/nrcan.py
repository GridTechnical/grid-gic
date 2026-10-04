"""NRCan Canadian magnetic observatories, 1-minute, no API key.

FDSN network C2 via Earthquakes Canada. Data are not redistributed in git.
SuperMAG is not used: it requires a registered user id (MISSING).

NRCan asks that the data not be sold onward. This fetcher writes a local
gitignored file for training and expects acknowledgement of Natural Resources
Canada. It does not ship observatory time series in the repository.
"""
from __future__ import annotations

from io import BytesIO

import numpy as np
import pandas as pd
import requests

STATION_URL = "https://earthquakescanada.nrcan.gc.ca/fdsnws/station/1/query"
DATA_URL = "https://earthquakescanada.nrcan.gc.ca/fdsnws/dataselect/1/query"

# Skip rails and the "02" backup sensors. Primaries cover polar cap through mid-latitudes.
SKIP_STATIONS = {"LRO", "ARF02", "BLC02", "FCC02", "IQA02", "MEA02"}


def list_stations(timeout: int = 60) -> pd.DataFrame:
    r = requests.get(
        STATION_URL,
        params={"network": "C2", "level": "station", "format": "text"},
        timeout=timeout,
    )
    r.raise_for_status()
    rows = []
    for line in r.text.splitlines():
        if not line or line.startswith("#"):
            continue
        parts = line.split("|")
        if len(parts) < 7:
            continue
        net, sta, lat, lon, elev, name, start = parts[:7]
        end = parts[7] if len(parts) > 7 else ""
        rows.append(
            {
                "station": sta,
                "lat": float(lat),
                "lon": float(lon),
                "name": name,
                "end": end.strip(),
            }
        )
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    alive = df["end"].fillna("").eq("")
    primary = ~df["station"].isin(SKIP_STATIONS) & ~df["station"].str.endswith("02")
    named = ~df["name"].str.contains("Secondary", case=False, na=False)
    return df.loc[alive & primary & named].drop_duplicates("station").reset_index(drop=True)


def _read_channel(content: bytes, channel: str) -> pd.Series:
    from obspy import read

    if not content or content[:1] in (b"<", b"E"):
        return pd.Series(dtype="float64")
    st = read(BytesIO(content))
    st = st.select(channel=channel)
    if len(st) == 0:
        return pd.Series(dtype="float64")
    st.merge(method=1, fill_value=None)
    tr = st[0]
    start = pd.Timestamp(tr.stats.starttime.datetime).tz_localize("UTC")
    idx = start + pd.to_timedelta(np.asarray(tr.times(), dtype=float), unit="s")
    idx = pd.DatetimeIndex(idx).round("min")
    s = pd.Series(np.asarray(tr.data, dtype=float), index=idx)
    s = s[~s.index.duplicated(keep="last")].sort_index()
    return s


def _get_channel(session: requests.Session, station: str, channel: str, start: str, end: str) -> pd.Series:
    import time
    params = {
        "network": "C2",
        "station": station,
        "location": "R0",
        "channel": channel,
        "starttime": pd.Timestamp(start).tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%S"),
        "endtime": pd.Timestamp(end).tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%S"),
        "nodata": "404",
    }
    last = None
    for attempt in range(4):
        r = session.get(DATA_URL, params=params, timeout=120)
        last = r
        if r.status_code == 404:
            return pd.Series(dtype="float64")
        if r.status_code >= 500 or r.content[:1] in (b"<", b"E"):
            time.sleep(1.5 * (attempt + 1))
            continue
        r.raise_for_status()
        series = _read_channel(r.content, channel)
        if len(series):
            return series
        time.sleep(1.0 * (attempt + 1))
    code = getattr(last, "status_code", None)
    print(f"  {station} {channel} empty after retries (HTTP {code})")
    return pd.Series(dtype="float64")


def station_dbdt(session, station: str, lat: float, lon: float, start: str, end: str) -> pd.DataFrame:
    """Vector |dB/dt| in nT/s from minute X, Y, Z. A missing component stays missing."""
    parts = {}
    for ch in ("UFX", "UFY", "UFZ"):
        parts[ch] = _get_channel(session, station, ch, start, end)
    missing = [ch for ch, s in parts.items() if len(s) == 0]
    if missing:
        print(f"  {station} missing {missing}; |dB/dt| not invented from a partial vector")
        return pd.DataFrame(columns=["time", "station", "lat", "lon", "dbdt_nts"])
    df = pd.concat(parts, axis=1).sort_index()
    # Only consecutive minutes. A multi-minute gap is not a rate.
    dt = df.index.to_series().diff().dt.total_seconds()
    d = df.diff()
    ok = dt.between(50, 90) & d[["UFX", "UFY", "UFZ"]].notna().all(axis=1)
    dbdt = np.sqrt((d[["UFX", "UFY", "UFZ"]] ** 2).sum(axis=1)) / dt
    dbdt = dbdt.where(ok)
    out = pd.DataFrame(
        {
            "time": dbdt.index,
            "station": station,
            "lat": lat,
            "lon": lon,
            "dbdt_nts": dbdt.to_numpy(),
        }
    )
    return out.dropna(subset=["dbdt_nts"])


def fetch_nrcan_dbdt(start: str, end: str, stations: list[str] | None = None) -> pd.DataFrame:
    meta = list_stations()
    if stations:
        meta = meta[meta["station"].isin(stations)]
    if meta.empty:
        raise RuntimeError("NRCan station list was empty")
    frames = []
    session = requests.Session()
    for row in meta.itertuples(index=False):
        print(f"NRCan {row.station} {start} → {end}")
        try:
            part = station_dbdt(session, row.station, row.lat, row.lon, start, end)
        except (requests.RequestException, OSError, ValueError) as exc:
            print(f"NRCan {row.station} failed: {type(exc).__name__}: {exc}")
            continue
        print(f"  {row.station} minutes with |dB/dt|: {len(part)}")
        if len(part):
            frames.append(part)
    if not frames:
        return pd.DataFrame(columns=["time", "station", "lat", "lon", "dbdt_nts"])
    out = pd.concat(frames, ignore_index=True)
    out["time"] = pd.to_datetime(out["time"], utc=True)
    return out
