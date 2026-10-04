"""Trailing L1 features. Windows are closed on the right: time t sees only L1 at or before t."""
from __future__ import annotations

import numpy as np
import pandas as pd

from forecast.physics import (
    clock_angle_rad,
    cone_angle_rad,
    dynamic_pressure_npa,
    epsilon_proxy,
    half_wave_coupling,
    newell_proxy,
    transit_minutes,
)
from forecast.constants import IMF_ABS_MAX_NT
from forecast.physics import mask_physical

CORE = [
    "bx_gsm",
    "by_gsm",
    "bz_gsm",
    "bt",
    "speed",
    "density",
    "pdyn_npa",
    "clock_angle_rad",
    "clock_sin",
    "clock_cos",
    "cone_angle_rad",
    "newell_proxy",
    "epsilon",
    "half_wave",
]

# name, source column, aggregation, window.
# Clock uses sin/cos means. A linear mean of the angle wraps and is not a direction.
# 15 min catches a sheath/shock; 120 and 180 min are the driving that is already
# inside the magnetosphere when the 30–90 min label opens.
TRAILING = [
    ("bz_min_15", "bz_gsm", "min", "15min"),
    ("speed_max_15", "speed", "max", "15min"),
    ("pdyn_max_15", "pdyn_npa", "max", "15min"),
    ("newell_mean_15", "newell_proxy", "mean", "15min"),
    ("epsilon_mean_15", "epsilon", "mean", "15min"),
    ("bz_mean_30", "bz_gsm", "mean", "30min"),
    ("bz_min_30", "bz_gsm", "min", "30min"),
    ("by_mean_30", "by_gsm", "mean", "30min"),
    ("speed_mean_30", "speed", "mean", "30min"),
    ("speed_max_30", "speed", "max", "30min"),
    ("density_mean_30", "density", "mean", "30min"),
    ("pdyn_mean_30", "pdyn_npa", "mean", "30min"),
    ("pdyn_max_30", "pdyn_npa", "max", "30min"),
    ("clock_sin_mean_30", "clock_sin", "mean", "30min"),
    ("clock_cos_mean_30", "clock_cos", "mean", "30min"),
    ("newell_mean_30", "newell_proxy", "mean", "30min"),
    ("epsilon_mean_30", "epsilon", "mean", "30min"),
    ("half_wave_mean_30", "half_wave", "mean", "30min"),
    ("bt_mean_30", "bt", "mean", "30min"),
    ("bz_mean_60", "bz_gsm", "mean", "60min"),
    ("bz_min_60", "bz_gsm", "min", "60min"),
    ("by_mean_60", "by_gsm", "mean", "60min"),
    ("speed_mean_60", "speed", "mean", "60min"),
    ("speed_max_60", "speed", "max", "60min"),
    ("density_mean_60", "density", "mean", "60min"),
    ("pdyn_mean_60", "pdyn_npa", "mean", "60min"),
    ("pdyn_max_60", "pdyn_npa", "max", "60min"),
    ("clock_sin_mean_60", "clock_sin", "mean", "60min"),
    ("clock_cos_mean_60", "clock_cos", "mean", "60min"),
    ("newell_mean_60", "newell_proxy", "mean", "60min"),
    ("epsilon_mean_60", "epsilon", "mean", "60min"),
    ("half_wave_mean_60", "half_wave", "mean", "60min"),
    ("bt_mean_60", "bt", "mean", "60min"),
    ("bz_mean_120", "bz_gsm", "mean", "120min"),
    ("bz_min_120", "bz_gsm", "min", "120min"),
    ("speed_mean_120", "speed", "mean", "120min"),
    ("pdyn_max_120", "pdyn_npa", "max", "120min"),
    ("newell_mean_120", "newell_proxy", "mean", "120min"),
    ("epsilon_mean_120", "epsilon", "mean", "120min"),
    ("bz_mean_180", "bz_gsm", "mean", "180min"),
    ("bz_min_180", "bz_gsm", "min", "180min"),
    ("speed_mean_180", "speed", "mean", "180min"),
    ("newell_mean_180", "newell_proxy", "mean", "180min"),
    ("epsilon_mean_180", "epsilon", "mean", "180min"),
]

NUMERIC_FEATURES = (
    CORE
    + [name for name, *_ in TRAILING]
    + [
        "coverage_30",
        "coverage_60",
        "coverage_180",
        "ut_sin",
        "ut_cos",
        "doy_sin",
        "doy_cos",
        "ut_hour",
        "doy",
        "tau_min",
        "arr_ut_sin",
        "arr_ut_cos",
    ]
)
# Not in the served L1 vector. They need a magnetometer archive at decision time.
# train_from_table scores them as an ablation and does not put them in the saved model,
# because forecast.predict is L1-only and a mostly-observed feature would treat that
# all-missing case as a rare gap.
RECENT_DBDT_FEATURES = ["recent_dbdt_max_60", "recent_band_max_60"]
CATEGORICAL_FEATURES = ["mlat_band_code", "mlt_sector_code"]


def clean_l1(df: pd.DataFrame) -> pd.DataFrame:
    """Mask OMNI/RTSW fills and recompute Pdyn only from real density and speed.

    Does not forward-fill. A missing plasma minute stays missing.
    """
    out = df.copy()
    if not isinstance(out.index, pd.DatetimeIndex):
        if "time" in out.columns:
            out = out.set_index(pd.to_datetime(out["time"], utc=True))
        else:
            raise ValueError("L1 frame needs a DatetimeIndex or a time column")
    if out.index.tz is None:
        out.index = out.index.tz_localize("UTC")
    else:
        out.index = out.index.tz_convert("UTC")
    out = out.sort_index()
    out = out[~out.index.duplicated(keep="last")]

    for col in ("bx_gsm", "by_gsm", "bz_gsm", "bt"):
        if col not in out.columns:
            out[col] = np.nan
        out[col] = mask_physical(out[col], abs_max=IMF_ABS_MAX_NT, min_valid=None if col != "bt" else 0.0)
    if "speed" not in out.columns:
        out["speed"] = np.nan
    if "density" not in out.columns:
        out["density"] = np.nan
    out["speed"] = mask_physical(out["speed"], abs_max=2500.0, min_valid=150.0)
    out["density"] = mask_physical(out["density"], abs_max=200.0, min_valid=0.05)
    # Drop any precomputed pressure. It is only valid when n and v are real.
    out["pdyn_npa"] = dynamic_pressure_npa(out["density"], out["speed"])
    out["clock_angle_rad"] = clock_angle_rad(out["by_gsm"], out["bz_gsm"])
    out["clock_sin"] = np.sin(out["clock_angle_rad"])
    out["clock_cos"] = np.cos(out["clock_angle_rad"])
    out["cone_angle_rad"] = cone_angle_rad(out["bx_gsm"], out["bt"])
    out["newell_proxy"] = newell_proxy(out["speed"], out["bt"], out["clock_angle_rad"])
    out["epsilon"] = epsilon_proxy(out["speed"], out["bt"], out["clock_angle_rad"])
    out["half_wave"] = half_wave_coupling(out["speed"], out["bz_gsm"])
    # Regular 1-minute grid. Mean of an empty minute is NaN, not 0.
    out = out[CORE].resample("1min").mean()
    return out


def _roll(s: pd.Series, window: str, how: str, min_count: int) -> pd.Series:
    roller = s.rolling(window, min_periods=min_count, closed="right")
    if how == "mean":
        return roller.mean()
    if how == "min":
        return roller.min()
    if how == "max":
        return roller.max()
    raise ValueError(how)


def add_trailing_features(clean: pd.DataFrame) -> pd.DataFrame:
    """Features at each minute. Altering rows after t must not change row t."""
    base = clean_l1(clean)
    out = base[CORE].copy()
    observed = out["bz_gsm"].notna() | out["speed"].notna()
    min_count = {"15min": 5, "30min": 10, "60min": 20, "120min": 40, "180min": 60}
    for name, col, how, window in TRAILING:
        out[name] = _roll(out[col], window, how, min_count[window])
    out["coverage_30"] = observed.rolling("30min", min_periods=1, closed="right").mean()
    out["coverage_60"] = observed.rolling("60min", min_periods=1, closed="right").mean()
    out["coverage_180"] = observed.rolling("180min", min_periods=1, closed="right").mean()
    ut_hour = out.index.hour + out.index.minute / 60.0
    doy = out.index.dayofyear.astype(float)
    out["ut_hour"] = ut_hour
    out["doy"] = doy
    out["ut_sin"] = np.sin(2 * np.pi * ut_hour / 24.0)
    out["ut_cos"] = np.cos(2 * np.pi * ut_hour / 24.0)
    out["doy_sin"] = np.sin(2 * np.pi * doy / 365.25)
    out["doy_cos"] = np.cos(2 * np.pi * doy / 365.25)
    out["tau_min"] = transit_minutes(out["speed"])
    arr_hour = (ut_hour + out["tau_min"] / 60.0) % 24.0
    out["arr_ut_sin"] = np.sin(2 * np.pi * arr_hour / 24.0)
    out["arr_ut_cos"] = np.cos(2 * np.pi * arr_hour / 24.0)
    out.loc[out["tau_min"].isna(), ["arr_ut_sin", "arr_ut_cos"]] = np.nan
    return out


def decision_times(features: pd.DataFrame, every_minutes: int = 5) -> pd.DatetimeIndex:
    idx = features.index
    keep = (idx.minute % every_minutes == 0) & (features["coverage_60"] > 0)
    return idx[keep]
