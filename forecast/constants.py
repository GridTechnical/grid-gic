"""Shared constants. Do not treat fill values as physical zeros."""

# n (cm^-3) * v (km/s)^2 -> nPa. Same factor as etl/fetch_solar_wind.py.
PDYN_FACTOR = 1.6726e-6

# L1 halo orbit is ~1.5e6 km sunward of Earth. Transit time is distance/speed.
L1_DISTANCE_KM = 1.5e6
TAU_MIN_MINUTES = 30.0
TAU_MAX_MINUTES = 90.0

# Label window for an L1 sample at t: Earth |dB/dt| on [t+30 min, t+90 min).
LABEL_LAG_MINUTES = 30
LABEL_WINDOW_MINUTES = 60

# Decision cadence. A single minute of L1 is not a feature vector.
DECISION_MINUTES = 5

# Centered dipole north pole (IGRF-13 era ~2020). Not the dip pole, not AACGM.
DIPOLE_POLE_LAT_DEG = 80.6
DIPOLE_POLE_LON_DEG = -72.7

# IMF components above this are OMNI/RTSW fills (9999.99), not storms.
IMF_ABS_MAX_NT = 200.0
SPEED_MIN_KMS = 150.0
SPEED_MAX_KMS = 2500.0
DENSITY_MIN_CM3 = 0.05
DENSITY_MAX_CM3 = 200.0

# Ground |dB/dt| exceedance (nT/s). 0.05 nT/s = 3 nT/min, elevated but not extreme.
# Swarm along-track |dB/dt| is a different unit (µT/s) and is not mixed in.
GROUND_DBDT_THRESHOLD_NTS = 0.05
SWARM_DBDT_THRESHOLD_UTPS = 0.05

MLAT_EDGES = [-90.0, -70.0, -55.0, -40.0, 40.0, 55.0, 70.0, 90.0]
MLAT_BANDS = [
    "s_polar",
    "s_auroral",
    "s_midlat",
    "equatorial",
    "n_midlat",
    "n_auroral",
    "n_polar",
]
# (name, start_hour_inclusive, end_hour_exclusive). Midnight wraps.
MLT_SECTORS = [
    ("midnight", 21.0, 3.0),
    ("dawn", 3.0, 9.0),
    ("noon", 9.0, 15.0),
    ("dusk", 15.0, 21.0),
]
MLT_CENTERS = {"midnight": 0.0, "dawn": 6.0, "noon": 12.0, "dusk": 18.0}

# Active-L1 rule for whole-storm splits. Quiet segments stay intact too.
STORM_BZ_NT = -5.0
STORM_SPEED_KMS = 500.0
STORM_PDYN_NPA = 4.0
STORM_GAP_CLOSE = "3h"
STORM_BREAK = "6h"
