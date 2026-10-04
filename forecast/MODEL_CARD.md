# Model card — L1 to mag-lat / MLT |dB/dt|

## What it is

A first short-horizon model for Solionyx. From L1 solar wind already in hand, estimate where Earth's |dB/dt| is elevated 30–90 minutes later, as a magnetic-latitude band and an MLT sector.

It is not a city GIC model and not a substation pin. Geographic longitude is derived for display: at a given UT, an MLT sector sits on a geographic meridian (`geographic_lon_for_mlt`). Substorm onset still jitters by tens of minutes, so that meridian is a sector, not a timestamp.

## Physics the training path is not allowed to break

- L1 is about 1.5e6 km upstream. Transit is distance/speed: about 60 min at 400 km/s and about 35 min at 700 km/s, clipped to 30–90 min and stored as `tau_min`.
- The label for solar wind at time t is the max |dB/dt| on [t+30 min, t+90 min). Features at t use only L1 at or before t (trailing 30 and 60 min). There is no shuffled-minute split.
- Dynamic pressure is `1.6726e-6 * n * v^2` (nPa) and only when both density and speed are real. OMNI/RTSW fills (9999.99, 99999.9, 999.99, …) become missing. Missing Pdyn stays missing. It is never written as 0.
- A single minute is not a feature vector. Decisions are every 5 minutes and include the trailing half hour and hour, plus UT and day-of-year (dipole-tilt season), as sine/cosine.
- Hold-out is the latest whole active interval. Later segments are dropped so the training set does not see the future of the hold-out storm.

## Coordinates

Magnetic latitude and MLT are a centered dipole, pole 80.6°N, 72.7°W (IGRF-13 era). That is not AACGM and not the magnetic dip pole. Bands:

| band | dipole mlat |
| --- | --- |
| s_polar | < −70° |
| s_auroral | −70° to −55° |
| s_midlat | −55° to −40° |
| equatorial | −40° to 40° |
| n_midlat | 40° to 55° |
| n_auroral | 55° to 70° |
| n_polar | ≥ 70° |

MLT sectors are 6 hours: midnight, dawn, noon, dusk. Equation of time is ignored (about 15 minutes), which is inside the substorm jitter.

## Labels

| source | status |
| --- | --- |
| NRCan network C2, 1-minute X/Y/Z, FDSN, no key | Used when `--labels nrcan` succeeds. Canada only, not a global grid. Not committed to git. Acknowledge Natural Resources Canada. Their terms restrict redistribution and commercial reuse. |
| SuperMAG | **MISSING.** The web service needs a registered user id. No new secret was added. |
| Swarm `geomag.swarm_l1m` `max_dbdt_utps` | Optional `--labels swarm`. This is the heatmap quantity, in µT/s, along the satellite track. Orbital motion mixes spatial structure into dB/dt. It is not ground |dB/dt|. If this mode is used, ground labels are **MISSING**. |

Default ground threshold in code: 0.05 nT/s (3 nT/min) on the vector rate. The smoke run uses 0.30 nT/s (18 nT/min). At 0.15 nT/s the October 28 hold-out (Alert) was already above the cut on about 95% of band-hours, so that cut did not test discrimination. Swarm threshold, if that mode is used: 0.05 µT/s. A band with no sample in the arrival hour is left out of training. It is not a zero.

Clock-angle trailing means are ordinary means, not circular means. Treat them as a rough history, not a precise angle.

## Model

`sklearn.ensemble.HistGradientBoostingClassifier` (balanced) and `HistGradientBoostingRegressor`. Band and MLT sector are categorical inputs. The classifier is P(max |dB/dt| in the arrival hour ≥ threshold). The regressor is the expected max |dB/dt|. The reported range is that prediction plus the 10th and 90th percentile of training residuals. Native missing values are left as NaN so a plasma gap is not a zero.

## Smoke score (committed)

Windows: 2025-10-10 to 2025-10-13 and 2025-10-28 to 2025-10-31, OMNI 1-minute plus NRCan. FDSN answers were incomplete under load: usable vector series were Alert (ALE, polar cap) and Ottawa (OTT, mid-latitude) on the first window, and mostly Alert on the second. Baker Lake, Brandon, Cambridge Bay, Churchill, Meanook, and Resolute Bay are not in this fit because at least one of X/Y/Z came back empty. SuperMAG is MISSING.

Hold-out is the whole later active interval `storm_20251028T0738Z` (898 band-rows). Train is every earlier segment (2098 rows). Threshold 0.30 nT/s.

| | train | hold-out |
| --- | --- | --- |
| positive rate | 0.24 | 0.56 |
| ROC AUC | 0.999 | **0.55** |
| average precision | 0.996 | 0.60 |
| Brier | 0.008 | **0.34** |
| MAE of max \|dB/dt\| | 0.037 nT/s | **0.13 nT/s** |

The in-sample AUC is memorization of a few storms and two stations. The hold-out does not beat a constant forecast (climatology Brier would be about 0.25). This run checks the wiring. It is not a forecast to put on the map.

Numbers are also in `forecast/artifacts/smoke_metrics.json`. The weights for that NRCan smoke fit are `forecast/artifacts/smoke_nrcan_hgb.joblib`. They are not the model `forecast.predict` loads by default.

## Swarm long run (default artifact)

`forecast/artifacts/band_dbdt_hgb.joblib` is this run, not the NRCan smoke. Ground labels are **MISSING**. SuperMAG is **MISSING**. Labels are Swarm `geomag.swarm_l1m` along-track max |dB/dt| in µT/s. Threshold 0.05 µT/s. Decisions every 15 minutes. Hold-out is the latest active segment that lasts at least 6 hours. A 15-minute blip at the end of 4 Nov (`storm_20251104T2251Z`) and the quiet segment after the hold-out were dropped so the test is not that blip.

Swarm coverage queried 2026-10-04: `minute_ts` from 2025-07-01 00:00Z through 2026-09-30 23:59Z, 436,320 rows, 3 satellites, 102 days. Usable with OMNI high-res (the product lags ~120 days, so September 2026 Swarm is out):

| window | joined rows |
| --- | --- |
| 2025-07-01 → 2025-07-31 | 54,640 |
| 2025-08-27 → 2025-09-26 | 55,106 |
| 2025-09-28 → 2025-11-05 | 68,318 |

Holes with no Swarm minutes: 2025-07-31..2025-08-26, 2025-09-26..2025-09-27, 2025-11-05..2026-09-26. Partial days kept (2 of 3 satellites): 2025-09-24, 2025-10-12, 2025-10-13. OMNI minutes 141,120. Swarm samples 419,040. Joined rows 178,064. Positive rate about 0.35. Rebuild with `python -m forecast.build_long_swarm` (cache under gitignored `data/`).

Official hold-out `storm_20251104T0411Z` (2025-11-04 04:15Z–14:00Z, 828 rows). Train 176,503 rows.

| | train | hold-out | band×MLT climatology on the hold-out |
| --- | --- | --- | --- |
| positive rate | 0.35 | 0.35 | train rate by band and sector |
| ROC AUC | 0.916 | **0.911** | **0.904** |
| Brier | 0.120 | **0.122** | **0.120** |
| MAE of max \|dB/dt\| | 0.014 µT/s | **0.014 µT/s** | **0.021 µT/s** |

A constant forecast at the train positive rate has hold-out Brier 0.228. The classifier does **not** beat a lookup of how often each magnetic-latitude band and MLT sector exceeds 0.05 µT/s (AUC 0.911 vs 0.904, Brier 0.122 vs 0.120). South-polar and equatorial along-track |dB/dt| are hot in both train and this hold-out; that map is most of the AUC. The regressor does beat the same lookup (MAE 0.014 vs 0.021 µT/s).

A second split, not the saved hold-out, leaves out the earlier long interval `storm_20251028T0738Z` (28 Oct–3 Nov, 12,809 rows) and trains only on data before it. Hold-out AUC 0.911 vs climatology 0.896, Brier 0.123 vs 0.124, MAE 0.014 vs 0.020 µT/s. Same pattern: a small regression gain, almost no probability skill beyond the orbital band map.

This is not a forecast to put on the map. What would move it: ground |dB/dt| (a complete NRCan set, or SuperMAG once a user id exists), a Swarm label with the along-track spatial gradient taken out, and storms from another season. The archive does not have that season yet.

Numbers are in `forecast/artifacts/swarm_long_metrics.json`.

## Live path

Training history is OMNIWeb 1-minute GSM. The dashboard strip stays on the operational NOAA RTSW feed. This package does not change `docs/index.html`.
