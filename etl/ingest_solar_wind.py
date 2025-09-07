# etl/ingest_solar_wind.py
import datetime as dt
from pathlib import Path
import pandas as pd
from fetch_solar_wind import fetch_solar_wind_merged

def main():
    now = dt.datetime.now(dt.timezone.utc)
    start = now - dt.timedelta(hours=6)

    df = fetch_solar_wind_merged(start, now, resample="1min")

    # keep relevant columns if they exist
    keep = [
        "density","speed","temperature",
        "bx_gsm","by_gsm","bz_gsm","bt",
        "pdyn_npa","bz_south","vbz",
        "clock_angle_rad","newell_proxy"
    ]
    existing = [c for c in keep if c in df.columns]
    df_out = df[existing].dropna(how="all")

    # output into docs/data (so Pages can serve it)
    out_dir = Path("docs/data")
    out_dir.mkdir(parents=True, exist_ok=True)

    df_out.to_csv(out_dir / "solar_wind_last6h.csv", index_label="time")
    df_out.reset_index().to_json(
        out_dir / "solar_wind_last6h.json",
        orient="records",
        date_format="iso"
    )

    print(f"Wrote {len(df_out)} rows to docs/data/solar_wind_last6h.*")

if __name__ == "__main__":
    main()
