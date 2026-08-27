# grid-gic

GIC watch: L1 solar wind plus ESA Swarm magnetometer data, stored in Supabase, shown on GitHub Pages.

## What runs

- **Solar wind (every 10 min):** `etl/ingest_solar_wind.py` pulls NOAA SWPC RTSW 1-minute mag+plasma, derives coupling fields, upserts `public.solar_wind_minute`, and writes `docs/data/solar_wind_last6h.json` for the dashboard.
- **Solar wind history (daily 02:10 UTC):** `etl/backfill_solar_wind.py` loads NASA OMNIWeb (lags ~120 days).
- **Swarm (daily 04:37 UTC):** `etl/ingest_swarm_test.py` via VirES into `geomag.swarm_l1b`, then drops 1 Hz older than 7 days.
- **Minute rollup (daily 06:10 UTC):** calls Supabase RPC `geomag.rollup_yesterday`.

## Secrets (GitHub Actions)

- `SUPABASE_URL`
- `SUPABASE_SERVICE_KEY`
- `VIRES_TOKEN` (Swarm only)

## Local

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python etl/fetch_solar_wind.py          # fetch only, no Supabase
python etl/ingest_solar_wind.py         # also writes docs/data; upserts if env is set
```

Pages: `docs/index.html` (Solionyx heatmap) and `docs/risk.html` (GIC Watch).
