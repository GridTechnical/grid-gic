"""L1 transit, dynamic pressure, dipole magnetic latitude and MLT.

Geographic longitude is not the forecast target. An MLT sector at a given UT
maps to a geographic meridian; see geographic_lon_for_mlt.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from forecast.constants import (
    DENSITY_MAX_CM3,
    DENSITY_MIN_CM3,
    DIPOLE_POLE_LAT_DEG,
    DIPOLE_POLE_LON_DEG,
    IMF_ABS_MAX_NT,
    L1_DISTANCE_KM,
    MLAT_BANDS,
    MLAT_EDGES,
    MLT_CENTERS,
    MLT_SECTORS,
    PDYN_FACTOR,
    SPEED_MAX_KMS,
    SPEED_MIN_KMS,
    TAU_MAX_MINUTES,
    TAU_MIN_MINUTES,
)


def _series(values, index=None) -> pd.Series:
    if isinstance(values, pd.Series):
        return values
    return pd.Series(values, index=index, dtype="float64")


def mask_physical(values, *, abs_max: float, min_valid: float | None = None) -> pd.Series:
    """Finite values inside a physical range. Fills and non-physical numbers become NaN."""
    s = pd.to_numeric(_series(values), errors="coerce")
    ok = np.isfinite(s.to_numpy(dtype=float))
    v = s.to_numpy(dtype=float)
    ok &= np.abs(v) <= abs_max
    if min_valid is not None:
        ok &= v >= min_valid
    out = s.astype("float64").copy()
    out.iloc[~ok] = np.nan
    return out


def _align(a, b) -> tuple[pd.Series, pd.Series]:
    a_num = pd.to_numeric(a, errors="coerce") if isinstance(a, pd.Series) else pd.to_numeric(pd.Series(np.asarray(a, dtype=float)), errors="coerce")
    if isinstance(a, pd.Series):
        a_num.index = a.index
    if isinstance(b, pd.Series):
        b_num = pd.to_numeric(b, errors="coerce")
        b_num.index = b.index
        if isinstance(a, pd.Series):
            b_num = b_num.reindex(a_num.index)
        else:
            a_num.index = b_num.index
    else:
        b_num = pd.Series(np.asarray(pd.to_numeric(b, errors="coerce"), dtype=float), index=a_num.index)
    return a_num.astype("float64"), b_num.astype("float64")


def dynamic_pressure_npa(density_cm3, speed_kms) -> pd.Series:
    """Pdyn in nPa from proton density and speed. Missing stays missing. Never 0."""
    n_raw, v_raw = _align(density_cm3, speed_kms)
    n = mask_physical(n_raw, abs_max=DENSITY_MAX_CM3, min_valid=DENSITY_MIN_CM3)
    v = mask_physical(v_raw, abs_max=SPEED_MAX_KMS, min_valid=SPEED_MIN_KMS)
    both = n.notna() & v.notna()
    pdyn = PDYN_FACTOR * n * (v ** 2)
    return pdyn.where(both)


def transit_minutes(speed_kms) -> pd.Series:
    """L1-to-Earth transit, clipped to the 30–90 min label horizon. NaN if speed is missing."""
    v = mask_physical(speed_kms, abs_max=SPEED_MAX_KMS, min_valid=SPEED_MIN_KMS)
    tau = L1_DISTANCE_KM / v / 60.0
    return tau.clip(lower=TAU_MIN_MINUTES, upper=TAU_MAX_MINUTES)


def clock_angle_rad(by_gsm, bz_gsm) -> pd.Series:
    by_raw, bz_raw = _align(by_gsm, bz_gsm)
    by = mask_physical(by_raw, abs_max=IMF_ABS_MAX_NT)
    bz = mask_physical(bz_raw, abs_max=IMF_ABS_MAX_NT)
    ang = np.arctan2(by.to_numpy(dtype=float), bz.to_numpy(dtype=float))
    out = pd.Series(ang, index=bz.index)
    return out.where(by.notna() & bz.notna())


def newell_proxy(speed, bt, clock) -> pd.Series:
    """Newell coupling proxy. NaN wherever speed, Bt, or clock is missing."""
    s_raw, b_raw = _align(speed, bt)
    _, th_raw = _align(s_raw, clock)
    s = mask_physical(s_raw, abs_max=SPEED_MAX_KMS, min_valid=SPEED_MIN_KMS)
    b = mask_physical(b_raw, abs_max=IMF_ABS_MAX_NT, min_valid=0.0)
    th = pd.to_numeric(th_raw, errors="coerce")
    proxy = (s.clip(lower=0) ** (4.0 / 3.0)) * (b.clip(lower=0) ** (2.0 / 3.0)) * (
        np.sin(np.abs(th) / 2.0) ** (8.0 / 3.0)
    )
    return proxy.where(s.notna() & b.notna() & th.notna())


def _dipole_lat_lon_rad(lat_deg, lon_deg):
    lat = np.radians(np.asarray(lat_deg, dtype=float))
    lon = np.radians(np.asarray(lon_deg, dtype=float))
    plat = np.radians(DIPOLE_POLE_LAT_DEG)
    plon = np.radians(DIPOLE_POLE_LON_DEG)
    dlon = lon - plon
    sin_mlat = np.sin(lat) * np.sin(plat) + np.cos(lat) * np.cos(plat) * np.cos(dlon)
    sin_mlat = np.clip(sin_mlat, -1.0, 1.0)
    mlat = np.arcsin(sin_mlat)
    y = np.sin(dlon) * np.cos(lat)
    x = np.cos(plat) * np.sin(lat) - np.sin(plat) * np.cos(lat) * np.cos(dlon)
    mlon = np.arctan2(y, x)
    return mlat, mlon


def magnetic_latitude_deg(lat_deg, lon_deg) -> np.ndarray:
    mlat, _ = _dipole_lat_lon_rad(lat_deg, lon_deg)
    return np.degrees(mlat)


def _subsolar(when: pd.DatetimeIndex):
    """Approximate subsolar geographic lat/lon. Equation of time is ignored (~15 min)."""
    t = pd.DatetimeIndex(when)
    if t.tz is None:
        t = t.tz_localize("UTC")
    else:
        t = t.tz_convert("UTC")
    ut_hours = t.hour + t.minute / 60.0 + t.second / 3600.0
    doy = t.dayofyear.to_numpy(dtype=float)
    # Solar declination, degrees. Enough for a dipole-tilt seasonal feature, not a navigator.
    decl = 23.44 * np.sin(2.0 * np.pi * (doy - 81.0) / 365.25)
    lon_sun = ((12.0 - ut_hours.to_numpy(dtype=float)) * 15.0 + 180.0) % 360.0 - 180.0
    return decl, lon_sun, ut_hours.to_numpy(dtype=float)


def magnetic_local_time_hours(lat_deg, lon_deg, when) -> np.ndarray:
    """Dipole MLT in hours [0, 24). `when` is UTC."""
    when = pd.DatetimeIndex(when)
    if len(when) == 1 and np.ndim(lat_deg) > 0 and len(np.atleast_1d(lat_deg)) != 1:
        when = pd.DatetimeIndex(np.repeat(when[0], len(np.atleast_1d(lat_deg))))
    decl, lon_sun, _ = _subsolar(when)
    _, mlon = _dipole_lat_lon_rad(lat_deg, lon_deg)
    _, mlon_sun = _dipole_lat_lon_rad(decl, lon_sun)
    mlt = 12.0 + np.degrees(mlon - mlon_sun) / 15.0
    return np.mod(mlt, 24.0)


def mlat_band(mlat_deg) -> np.ndarray:
    idx = np.digitize(np.asarray(mlat_deg, dtype=float), MLAT_EDGES[1:-1], right=False)
    names = np.asarray(MLAT_BANDS, dtype=object)
    out = names[idx]
    bad = ~np.isfinite(np.asarray(mlat_deg, dtype=float))
    out = out.copy()
    out[bad] = None
    return out


def mlt_sector(mlt_hours) -> np.ndarray:
    h = np.mod(np.asarray(mlt_hours, dtype=float), 24.0)
    out = np.empty(h.shape, dtype=object)
    out[:] = None
    for name, start, end in MLT_SECTORS:
        if start < end:
            mask = (h >= start) & (h < end)
        else:
            mask = (h >= start) | (h < end)
        out[mask] = name
    out[~np.isfinite(np.asarray(mlt_hours, dtype=float))] = None
    return out


def geographic_lon_for_mlt(mlt_hours: float, when, mlat_deg: float) -> float:
    """Geographic longitude (deg E, -180..180) whose dipole MLT is `mlt_hours`.

    Searches longitude at the requested magnetic-latitude's geographic latitude
    is not unique; we search geographic longitude at a fixed geographic latitude
    equal to the dipole latitude target only approximately, by scoring MLT on a
    1-degree longitude grid at geographic latitude = mlat_deg. Good to the
    bin width (6 h of MLT), which is coarser than substorm jitter.
    """
    grid = np.arange(-180.0, 180.0, 1.0)
    lats = np.full(grid.shape, float(mlat_deg))
    when_idx = pd.DatetimeIndex([pd.Timestamp(when).tz_convert("UTC") if pd.Timestamp(when).tzinfo else pd.Timestamp(when).tz_localize("UTC")] * len(grid))
    mlt = magnetic_local_time_hours(lats, grid, when_idx)
    target = float(mlt_hours) % 24.0
    delta = np.abs((mlt - target + 12.0) % 24.0 - 12.0)
    return float(grid[int(np.argmin(delta))])


def band_center_mlat(band: str) -> float:
    i = MLAT_BANDS.index(band)
    return 0.5 * (MLAT_EDGES[i] + MLAT_EDGES[i + 1])


def sector_center_mlt(sector: str) -> float:
    return float(MLT_CENTERS[sector])
