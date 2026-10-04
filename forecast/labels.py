"""Bin magnetometer |dB/dt| into magnetic-latitude and MLT bands.

A band with no sample in the label window is missing. It is not a zero.
The label for L1 at t is the max |dB/dt| on [t+30 min, t+90 min).
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from forecast.constants import LABEL_LAG_MINUTES, LABEL_WINDOW_MINUTES, MLAT_BANDS, MLT_SECTORS
from forecast.physics import magnetic_latitude_deg, magnetic_local_time_hours, mlat_band, mlt_sector


def annotate_mag_coords(samples: pd.DataFrame, value_col: str) -> pd.DataFrame:
    out = samples.dropna(subset=["time", "lat", "lon", value_col]).copy()
    out["time"] = pd.to_datetime(out["time"], utc=True)
    mlat = magnetic_latitude_deg(out["lat"].to_numpy(), out["lon"].to_numpy())
    mlt = magnetic_local_time_hours(out["lat"].to_numpy(), out["lon"].to_numpy(), pd.DatetimeIndex(out["time"]))
    out["mlat"] = mlat
    out["mlt"] = mlt
    out["mlat_band"] = mlat_band(mlat)
    out["mlt_sector"] = mlt_sector(mlt)
    return out.dropna(subset=["mlat_band", "mlt_sector"])


def band_minute_max(samples: pd.DataFrame, value_col: str) -> pd.DataFrame:
    """Max |dB/dt| per minute inside each band and MLT sector."""
    ann = annotate_mag_coords(samples, value_col)
    if ann.empty:
        return pd.DataFrame(columns=["time", "mlat_band", "mlt_sector", "dbdt"])
    g = (
        ann.groupby([ann["time"].dt.floor("min"), "mlat_band", "mlt_sector"], observed=True)[value_col]
        .max()
        .rename("dbdt")
        .reset_index()
        .rename(columns={"time": "time"})
    )
    # groupby on a series named time may name the level time already
    if "time" not in g.columns:
        g = g.rename(columns={g.columns[0]: "time"})
    g["time"] = pd.to_datetime(g["time"], utc=True)
    return g


def labels_for_decisions(
    band_minutes: pd.DataFrame,
    decisions: pd.DatetimeIndex,
    threshold: float,
) -> pd.DataFrame:
    """For each decision time and each observed band, max |dB/dt| in the delayed hour.

    Bands that had no magnetometer sample in that hour are omitted (MISSING),
    not filled with zero.
    """
    if band_minutes.empty or len(decisions) == 0:
        return pd.DataFrame(columns=["time", "mlat_band", "mlt_sector", "y_max_dbdt", "y_exceed", "n_minutes"])
    pieces = []
    lag = pd.Timedelta(minutes=LABEL_LAG_MINUTES)
    width = pd.Timedelta(minutes=LABEL_WINDOW_MINUTES)
    decisions = pd.DatetimeIndex(decisions)
    if decisions.tz is None:
        decisions = decisions.tz_localize("UTC")
    for (band, sector), sub in band_minutes.groupby(["mlat_band", "mlt_sector"], observed=True):
        sub = sub.sort_values("time")
        # Force nanoseconds. pandas 3 stores UTC in microseconds, while Timestamp.value is ns.
        times = pd.DatetimeIndex(pd.to_datetime(sub["time"], utc=True)).as_unit("ns").asi8
        vals = sub["dbdt"].to_numpy(dtype=float)
        rows = []
        for t in decisions:
            t0 = pd.Timestamp(t + lag).as_unit("ns")
            t1 = (t0 + width).as_unit("ns")
            i0 = int(np.searchsorted(times, t0.value, side="left"))
            i1 = int(np.searchsorted(times, t1.value, side="left"))
            if i1 <= i0:
                continue
            window = vals[i0:i1]
            finite = window[np.isfinite(window)]
            if finite.size == 0:
                continue
            y = float(finite.max())
            rows.append(
                {
                    "time": t,
                    "mlat_band": band,
                    "mlt_sector": sector,
                    "y_max_dbdt": y,
                    "y_exceed": int(y >= threshold),
                    "n_minutes": int(finite.size),
                }
            )
        if rows:
            pieces.append(pd.DataFrame(rows))
    if not pieces:
        return pd.DataFrame(columns=["time", "mlat_band", "mlt_sector", "y_max_dbdt", "y_exceed", "n_minutes"])
    out = pd.concat(pieces, ignore_index=True)
    out["time"] = pd.to_datetime(out["time"], utc=True)
    # Stable band order for downstream codes.
    out["mlat_band"] = pd.Categorical(out["mlat_band"], categories=MLAT_BANDS, ordered=True)
    out["mlt_sector"] = pd.Categorical(
        out["mlt_sector"], categories=[n for n, _, _ in MLT_SECTORS], ordered=True
    )
    return out.sort_values(["time", "mlat_band", "mlt_sector"]).reset_index(drop=True)
