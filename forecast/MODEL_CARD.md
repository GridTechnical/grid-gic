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

### What changed relative to the shallow model

The previous artifact (depth 3, `class_weight='balanced'`, 30 and 60 min L1 only) scored hold-out AUC 0.911 against a band×MLT climatology of 0.904, and Brier 0.122 against 0.120. Almost all of that AUC was the orbital map: south-polar and equatorial along-track |dB/dt| are hot whether or not the solar wind is.

Two changes, in the order they mattered:

1. Capacity, chosen on the previous feature table by 5-fold storm CV (23 storms of at least 6 h, interleaved in time, official hold-out not in a fold). Depth 3 + balanced: mean AUC 0.912 vs climatology 0.901, Brier 0.123 vs 0.121. Depth 6, `min_samples_leaf` 80, no class weight: mean AUC 0.956 vs 0.901, Brier 0.080 vs 0.121. Balanced weights were dropped because they made the probabilities worse, not better. That is the calibration step. There is no second isotonic model. One model with band and MLT as categorical inputs was enough; separate per-band fits were not required once the trees could split on the band.
2. Features, added after that choice and not re-tuned on the hold-out. Trailing windows are 15, 30, 60, 120, and 180 min. Clock angle enters as sin/cos, not a linear mean of a wrapped angle. Also IMF cone angle, the Newell coupling proxy, an epsilon-style proxy `v * Bt^2 * sin^4(|clock|/2)`, half-wave `v * max(-Bz, 0)`, and transit time `tau = L1_distance / v` clipped to 30–90 min. Missing plasma stays missing.

The along-track spatial background is a median in 2° magnetic latitude by MLT sector, fit only on minutes before 2025-10-25 23:59Z (the last 10 days of the archive, which include the hold-out, are excluded). `y_excess_max` is the label-hour max of |dB/dt| minus that median. It is a label, not a feature. Recent Swarm |dB/dt| on [t−60 min, t] in the same band (and in the same band and sector) does not touch [t+30, t+90). Those two columns are about 92% and 99% observed, so they are **not** in the saved model: `forecast.predict` is L1-only and would otherwise score every row as a rare gap. They are an ablation.

### Official hold-out

`storm_20251104T0411Z` (2025-11-04 04:15Z–14:00Z, 828 rows). Train 176,503 rows. Threshold 0.05 µT/s.

| | train | hold-out | band×MLT climatology on the hold-out |
| --- | --- | --- | --- |
| positive rate | 0.35 | 0.35 | train rate by band and sector |
| ROC AUC | 0.963 | **0.957** | **0.904** |
| Brier | 0.076 | **0.080** | **0.120** |
| MAE of max \|dB/dt\| | 0.0098 µT/s | **0.010 µT/s** | **0.021 µT/s** |

ΔAUC = +0.053. Brier is lower by 0.040. A constant forecast at the train positive rate has hold-out Brier 0.228. This is the first long-table fit that beats the orbital map on both probability scores, not only on MAE.

A second classifier, same L1 features and the same capacity, trained on the within-cell residual (1 if the label is at or above that cell's training 75th percentile; cell rates are ~0.25, so the climatology has almost no rank skill):

| label | hold-out AUC | cell climatology AUC | hold-out Brier | cell climatology Brier |
| --- | --- | --- | --- | --- |
| raw max \|dB/dt\| above the cell p75 | 0.881 | 0.446 | 0.148 | 0.196 |
| track-excess max above the cell p75 | 0.906 | 0.464 | 0.138 | 0.196 |

The saved residual head is the track-excess one (`p_above_cell_p75`). The 0.05 µT/s head is still `p_exceed`.

A second split, not used to pick depth, leaves out `storm_20251028T0738Z` (28 Oct–3 Nov, 12,809 rows) and trains only on earlier rows. Hold-out AUC 0.953 vs climatology 0.896, Brier 0.085 vs 0.124, MAE 0.010 vs 0.020 µT/s. Same direction as the official split.

Ablation, not saved: adding the two recent |dB/dt| columns moves the official hold-out to AUC 0.979, Brier 0.054, MAE 0.0083 µT/s. That is a nowcast, and it needs Swarm (or a ground magnetometer) at decision time. It is not what `forecast.predict` runs.

### Ground stations were not expanded

NRCan FDSN station list (network C2, no key) answered on 2026-10-04. Live primaries in the catalog: ALE, ARF, BLC, BRD, CBB, EUA, FCC, IQA, MEA, OTT, RES, SNK, STJ, VIC, YKC. A dataselect probe for 2025-10-10 12:00–14:00Z, location R0, channels UFX/UFY/UFZ, returned **no station with all three components** (ALE missing Z, OTT only Z, and the same pattern at YKC, VIC, STJ, SNK, MEA, FCC, RES, CBB, BLC, IQA, BRD, ARF, EUA). A partial vector is not turned into |dB/dt|. The earlier smoke that kept Alert and Ottawa does not extend today. SuperMAG still needs a registered user id. No new secret was added.

This is still not a ground |dB/dt| forecast and not a city GIC model. It is an L1 forecast of where Swarm's along-track |dB/dt| exceeds 0.05 µT/s, and it now beats the band×MLT map on a later storm and on the late-October storm. Do not paint it on the heatmap until the label is a ground rate.

Numbers are in `forecast/artifacts/swarm_long_metrics.json`.

## Live path

Training history is OMNIWeb 1-minute GSM. The dashboard strip stays on the operational NOAA RTSW feed. This package does not change `docs/index.html`.
