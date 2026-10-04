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


def along_track_excess(
    samples: pd.DataFrame,
    value_col: str,
    fit_before=None,
    mlat_step: float = 2.0,
    min_bin: int = 30,
) -> pd.DataFrame:
    """Subtract the median |dB/dt| in a 2° magnetic-latitude by MLT-sector bin.

    That median is the along-track spatial background (crustal field, the average
    electrojet, the satellite's path). The residual is the temporal disturbance.
    Minutes at or after ``fit_before`` do not enter the median, so a later hold-out
    does not set its own background. Bins with fewer than ``min_bin`` fit samples
    stay missing rather than inventing a level.
    """
    ann = annotate_mag_coords(samples, value_col)
    if ann.empty:
        ann["dbdt_excess"] = pd.Series(dtype="float64")
        return ann
    ann["mlat_bin"] = np.round(ann["mlat"].to_numpy(dtype=float) / mlat_step) * mlat_step
    fit = ann
    if fit_before is not None:
        cutoff = pd.Timestamp(fit_before)
        if cutoff.tzinfo is None:
            cutoff = cutoff.tz_localize("UTC")
        fit = ann[ann["time"] < cutoff]
        if fit.empty:
            fit = ann
    bg = fit.groupby(["mlat_bin", "mlt_sector"], observed=True)[value_col].agg(
        track_median="median", track_n="size"
    )
    bg.loc[bg["track_n"] < min_bin, "track_median"] = np.nan
    ann = ann.join(bg["track_median"], on=["mlat_bin", "mlt_sector"])
    ann["dbdt_excess"] = ann[value_col] - ann["track_median"]
    return ann


def labels_for_decisions(
    band_minutes: pd.DataFrame,
    decisions: pd.DatetimeIndex,
    threshold: float,
    recent_minutes: int = 60,
) -> pd.DataFrame:
    """For each decision time and each observed band, max |dB/dt| in the delayed hour.

    Bands that had no magnetometer sample in that hour are omitted (MISSING),
    not filled with zero.

    ``recent_dbdt_max_60`` is the max in the same band and sector on [t-60 min, t].
    That window ends at the decision and does not touch the label hour [t+30, t+90).
    """
    empty_cols = ["time", "mlat_band", "mlt_sector", "y_max_dbdt", "y_exceed", "n_minutes", "recent_dbdt_max_60"]
    if band_minutes.empty or len(decisions) == 0:
        return pd.DataFrame(columns=empty_cols)
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
            tb = pd.Timestamp(t).as_unit("ns")
            recent_y = np.nan
            if recent_minutes > 0:
                r0 = (tb - pd.Timedelta(minutes=recent_minutes)).as_unit("ns")
                k0 = int(np.searchsorted(times, r0.value, side="left"))
                k1 = int(np.searchsorted(times, tb.value, side="right"))
                if k1 > k0:
                    recent = vals[k0:k1]
                    recent = recent[np.isfinite(recent)]
                    if recent.size:
                        recent_y = float(recent.max())
            rows.append(
                {
                    "time": t,
                    "mlat_band": band,
                    "mlt_sector": sector,
                    "y_max_dbdt": y,
                    "y_exceed": int(y >= threshold),
                    "n_minutes": int(finite.size),
                    "recent_dbdt_max_60": recent_y,
                }
            )
        if rows:
            pieces.append(pd.DataFrame(rows))
    if not pieces:
        return pd.DataFrame(columns=empty_cols)
    out = pd.concat(pieces, ignore_index=True)
    out["time"] = pd.to_datetime(out["time"], utc=True)
    # Stable band order for downstream codes.
    out["mlat_band"] = pd.Categorical(out["mlat_band"], categories=MLAT_BANDS, ordered=True)
    out["mlt_sector"] = pd.Categorical(
        out["mlt_sector"], categories=[n for n, _, _ in MLT_SECTORS], ordered=True
    )
    return out.sort_values(["time", "mlat_band", "mlt_sector"]).reset_index(drop=True)


def recent_band_max(
    band_minutes: pd.DataFrame,
    decisions: pd.DatetimeIndex,
    minutes: int = 60,
) -> pd.DataFrame:
    """Max |dB/dt| in the magnetic-latitude band on [t-minutes, t], any MLT sector.

    Same rule as the sector recent feature: nothing from the label hour.
    A band with no sample in the lookback is NaN, not zero.
    """
    cols = ["time", "mlat_band", "recent_band_max_60"]
    if band_minutes.empty or len(decisions) == 0:
        return pd.DataFrame(columns=cols)
    stamped = band_minutes.copy()
    stamped["time"] = pd.to_datetime(stamped["time"], utc=True).dt.floor("min")
    g = (
        stamped.groupby(["time", "mlat_band"], observed=True)["dbdt"]
        .max()
        .rename("dbdt")
        .reset_index()
    )
    decisions = pd.DatetimeIndex(decisions)
    if decisions.tz is None:
        decisions = decisions.tz_localize("UTC")
    width = pd.Timedelta(minutes=minutes)
    pieces = []
    for band, sub in g.groupby("mlat_band", observed=True):
        sub = sub.sort_values("time")
        times = pd.DatetimeIndex(pd.to_datetime(sub["time"], utc=True)).as_unit("ns").asi8
        vals = sub["dbdt"].to_numpy(dtype=float)
        rows = []
        for t in decisions:
            tb = pd.Timestamp(t).as_unit("ns")
            r0 = (tb - width).as_unit("ns")
            k0 = int(np.searchsorted(times, r0.value, side="left"))
            k1 = int(np.searchsorted(times, tb.value, side="right"))
            y = np.nan
            if k1 > k0:
                recent = vals[k0:k1]
                recent = recent[np.isfinite(recent)]
                if recent.size:
                    y = float(recent.max())
            rows.append({"time": t, "mlat_band": band, "recent_band_max_60": y})
        if rows:
            pieces.append(pd.DataFrame(rows))
    if not pieces:
        return pd.DataFrame(columns=cols)
    out = pd.concat(pieces, ignore_index=True)
    out["time"] = pd.to_datetime(out["time"], utc=True)
    return out
