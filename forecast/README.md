# Forecast — L1 solar wind to mag-lat / MLT |dB/dt|

Short-horizon model path for Solionyx. Read [MODEL_CARD.md](MODEL_CARD.md) before trusting a number. Serving notes: [SERVING.md](SERVING.md). The heatmap is unchanged.

```bash
pip install -r forecast/requirements.txt

# Unit checks (no network): Pdyn never fills 0, trailing windows ignore the future,
# labels sit on [t+30, t+90), storms are not split mid-event.
python -m unittest forecast.tests.test_forecast

# OMNI + NRCan ground |dB/dt| for two storm windows. Writes data/ (gitignored).
python -m forecast.build_dataset \
  --window 2025-10-10T00:00:00Z,2025-10-13T00:00:00Z \
  --window 2025-10-28T00:00:00Z,2025-10-31T00:00:00Z \
  --labels nrcan \
  --stations ALE,RES,CBB,BLC,IQA,FCC,MEA,OTT,BRD \
  --threshold 0.30 \
  --out data/forecast/smoke.parquet

python -m forecast.train --table data/forecast/smoke.parquet \
  --out forecast/artifacts/band_dbdt_hgb.joblib

python -m forecast.predict \
  --features forecast/examples/l1_decision_sample.parquet
```

Swarm heatmap quantity instead of ground (marks ground labels MISSING; needs `SUPABASE_URL` and `SUPABASE_SERVICE_KEY` in the environment, same as etl). The long run uses every Swarm day that OMNI high-res can cover. See the model card before reading the AUC.

```bash
python -m forecast.build_dataset --window 2025-10-10T00:00:00Z,2025-10-12T00:00:00Z \
  --labels swarm --out data/forecast/swarm.parquet

python -m forecast.build_long_swarm
python -m forecast.train --table data/forecast/swarm_long.parquet \
  --metrics forecast/artifacts/swarm_long_metrics.json --min-hold-hours 6
```

`forecast/examples/l1_decision_sample.parquet` is L1 features only, so predict runs without the gitignored table and without NRCan samples.

SuperMAG is not fetched. It needs a registered user id.
