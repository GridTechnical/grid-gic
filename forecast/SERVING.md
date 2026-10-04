# Serving later (not live)

The heatmap and the L1 RTSW strip do not read this model. Do not wire it in until a hold-out on more than one storm is boringly stable. The default artifact is Swarm along-track |dB/dt| (µT/s), not ground |dB/dt|. On the 4 Nov 2025 hold-out its exceedance probabilities do not beat a magnetic-latitude / MLT climatology. See the model card.

Suggested table, not created:

```text
forecast.band_hour
  issue_time        timestamptz   -- L1 time the features were closed
  valid_start       timestamptz   -- issue_time + 30 min
  valid_end         timestamptz   -- issue_time + 90 min
  mlat_band         text
  mlt_sector        text
  p_exceed          float
  dbdt_p10          float
  dbdt_p50          float
  dbdt_p90          float
  threshold         float
  unit              text          -- nT/s for NRCan, uT/s for Swarm
  model             text
```

A later job can score the latest RTSW hour with `forecast.predict` and upsert that table. Training history stays OMNI (or the cleaned `solar_wind_minute` archive). The live strip stays the operational RTSW feed.

Rules for that job:

- Close features at the last complete L1 minute. Never fill missing density, speed, or Pdyn with 0.
- Score every mag-lat band and MLT sector. Bands the training set never saw stay flagged unsupported.
- Map an MLT sector to geographic longitude only for display (`geographic_lon_for_mlt`). The model does not emit a city or a substation.
- Say that substorm onset jitters by tens of minutes. The product is a 60-minute arrival window, not an onset alarm.
