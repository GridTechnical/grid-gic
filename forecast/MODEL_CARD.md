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

Numbers are also in `forecast/artifacts/smoke_metrics.json`.

## Live path

Training history is OMNIWeb 1-minute GSM. The dashboard strip stays on the operational NOAA RTSW feed. This package does not change `docs/index.html`.
