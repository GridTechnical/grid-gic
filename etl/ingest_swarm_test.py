#!/usr/bin/env python3
import os
from datetime import datetime, timezone
import pandas as pd
import numpy as np

# Data sources
from viresclient import SwarmRequest

# Supabase REST (schema-aware)
from postgrest import Client as PostgrestClient

# ---- Env ----
SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
START = os.environ.get("START", "2014-01-01T00:00:00Z")
END   = os.environ.get("END",   datetime.now(timezone.utc).strftime("%Y-%m-%dT00:00:00Z"))

# PostgREST client targeting the geomag schema
pg = PostgrestClient(
    f"{SUPABASE_URL}/rest/v1",
    headers={
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Profile": "geomag",   # write to geomag schema
        "Accept-Profile": "geomag",    # read from geomag schema
        "Prefer": "resolution=merge-duplicates"  # upsert behavior
    },
)

# ---- Helpers ----
def upsert(table: str, df: pd.DataFrame, pk_cols):
    """Batch upsert into a table within the geomag schema."""
    if df.empty:
        print(f"[{table}] nothing to upsert")
        return
    records = df.to_dict(orient="records")
    BATCH = 1000
    for i in range(0, len(records), BATCH):
        batch = records[i:i+BATCH]
        pg.from_(table).upsert(batch, on_conflict=",".join(pk_cols)).execute()
        print(f"[{table}] upserted {len(batch)} rows")

# ---- Swarm fetch ----
def fetch_swarm_l1b(collection: str, start_iso: str, end_iso: str) -> pd.DataFrame:
    """Fetch 1 Hz L1B NEC vectors + position for a Swarm spacecraft."""
    req = SwarmRequest()
    # public VirES server
    req.set_server("https://vires.services")
    req.set_collection(collection)  # e.g., SW_OPER_MAGA_LR_1B

    data = req.get_between(
        start_time=start_iso,
        end_time=end_iso,
        measurements=["B_NEC", "Latitude", "Longitude", "Radius", "Spacecraft"],
    )
    df = data.as_dataframe()
    if df.empty:
        return pd.DataFrame()

    # nT -> µT
    for c in ["B_N", "B_E", "B_Z"]:
        df[c] = df[c] / 1000.0

    # altitude (km): Radius (m) - Earth mean radius (6371 km)
    df["alt_km"] = (df["Radius"] / 1000.0) - 6371.0

    out = pd.DataFrame({
        "ts": pd.to_datetime(df.index).tz_convert("UTC"),
        "sat_id": df["Spacecraft"].map({"A":"A","B":"B","C":"C"}).fillna("A"),
        "lat_deg": df["Latitude"].astype(float),
        "lon_deg": df["Longitude"].astype(float),
        "bn_ut": df["B_N"].astype(float),
        "be_ut": df["B_E"].astype(float),
        "bd_ut": df["B_Z"].astype(float),
    }).sort_values(["sat_id","ts"])

    # simple |dB/dt| over 60 s window per sat
    def dbdt(group: pd.DataFrame) -> pd.Series:
        diff = group[["bn_ut","be_ut","bd_ut"]].diff(60)
        return np.sqrt((diff**2).sum(axis=1)) / 60.0

    out["dbdt_utps"] = out.groupby("sat_id", group_keys=False).apply(dbdt).values
    return out.dropna(subset=["ts"])

# ---- Main ----
def main():
    # seed satellites table (A/B/C)
    sats = pd.DataFrame([
        {"sat_id":"A","name":"Swarm Alpha","agency":"ESA"},
        {"sat_id":"B","name":"Swarm Bravo","agency":"ESA"},
        {"sat_id":"C","name":"Swarm Charlie","agency":"ESA"},
    ])
    upsert("satellites", sats, ["sat_id"])

    total = 0
    for coll in ["SW_OPER_MAGA_LR_1B", "SW_OPER_MAGB_LR_1B", "SW_OPER_MAGC_LR_1B"]:
        try:
            df = fetch_swarm_l1b(coll, START, END)
        except Exception as e:
            print(f"[{coll}] error: {e}")
            df = pd.DataFrame()
        if not df.empty:
            upsert("swarm_l1b", df, ["ts","sat_id"])
            print(f"[{coll}] rows: {len(df)}")
            total += len(df)
        else:
            print(f"[{coll}] no data for {START} → {END}")

    print(f"DONE. total rows inserted: {total}")

if __name__ == "__main__":
    main()
