#!/usr/bin/env python3
import os
from datetime import datetime, timezone
import pandas as pd
import numpy as np

# pip deps: supabase, viresclient, pandas, numpy, requests
from supabase import create_client, Client
from viresclient import SwarmRequest

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
START = os.environ.get("START", "2014-01-01T00:00:00Z")
END   = os.environ.get("END",   datetime.now(timezone.utc).strftime("%Y-%m-%dT00:00:00Z"))

sb: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

def upsert(table: str, df: pd.DataFrame, pk_cols):
    if df.empty:
        print(f"[{table}] nothing to upsert")
        return
    data = df.to_dict(orient="records")
    BATCH = 1000
    for i in range(0, len(data), BATCH):
        batch = data[i:i+BATCH]
        sb.table(f"geomag.{table}").upsert(batch, on_conflict=",".join(pk_cols)).execute()
        print(f"[{table}] upserted {len(batch)} rows")

def fetch_swarm_l1b(collection: str, start_iso: str, end_iso: str) -> pd.DataFrame:
    req = SwarmRequest()
    # Public VirES server (no token needed for MAG L1B LR)
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

    # altitude km (Radius m - Earth mean radius 6371 km)
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

    # simple |dB/dt| over 60s window
    def dbdt(group: pd.DataFrame) -> pd.Series:
        diff = group[["bn_ut","be_ut","bd_ut"]].diff(60)
        mag = np.sqrt((diff**2).sum(axis=1)) / 60.0
        return mag

    out["dbdt_utps"] = out.groupby("sat_id", group_keys=False).apply(dbdt).values
    return out.dropna(subset=["ts"])

def main():
    # seed satellites
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
            total += len(df)
            print(f"[{coll}] rows: {len(df)}")
        else:
            print(f"[{coll}] no data for {START} → {END}")

    print(f"DONE. total rows inserted: {total}")

if __name__ == "__main__":
    main()
