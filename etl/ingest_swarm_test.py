#!/usr/bin/env python3
import os, json
from datetime import datetime, timezone
import pandas as pd
import numpy as np
import requests
from viresclient import SwarmRequest

# ---- Env ----
SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
START = os.environ.get("START", "2014-01-01T00:00:00Z")
END   = os.environ.get("END",   datetime.now(timezone.utc).strftime("%Y-%m-%dT00:00:00Z"))
VIRES_TOKEN = os.environ["VIRES_TOKEN"]


# ---- Simple Supabase REST helper (schema-aware) ----
class SupabaseREST:
    def __init__(self, base_url: str, service_key: str, schema: str = "geomag"):
        self.base = f"{base_url}/rest/v1"
        self.headers = {
            "apikey": service_key,
            "Authorization": f"Bearer {service_key}",
            "Content-Profile": schema,
            "Accept-Profile": schema,
            "Prefer": "resolution=merge-duplicates,return=minimal",
            "Content-Type": "application/json",
        }

    def upsert(self, table: str, records: list[dict], on_conflict: str, batch_size: int = 1000):
        if not records:
            print(f"[{table}] nothing to upsert")
            return
        for i in range(0, len(records), batch_size):
            chunk = records[i:i+batch_size]
            url = f"{self.base}/{table}?on_conflict={on_conflict}"
            r = requests.post(url, headers=self.headers, data=json.dumps(chunk), timeout=180)
            if r.status_code >= 300:
                raise RuntimeError(f"Upsert {table} failed {r.status_code}: {r.text}")
            print(f"[{table}] upserted {len(chunk)} rows")

pg = SupabaseREST(SUPABASE_URL, SUPABASE_SERVICE_KEY, schema="geomag")

# ---- Swarm fetch ----
def fetch_swarm_l1b(collection: str, start_iso: str, end_iso: str) -> pd.DataFrame:
    VIRES_URL = os.environ.get("VIRES_URL", "https://vires.services/ows")
    req = SwarmRequest(VIRES_URL)   # pass URL explicitly
    req.set_token(VIRES_TOKEN)
    req.set_collection(collection)  # SW_OPER_MAGA_LR_1B / MAGB / MAGC

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
    # altitude (km): radius(m) -> km minus Earth mean radius
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

    # simple |dB/dt| over 60 s window
    def dbdt(group: pd.DataFrame) -> pd.Series:
        diff = group[["bn_ut","be_ut","bd_ut"]].diff(60)
        return np.sqrt((diff**2).sum(axis=1)) / 60.0

    out["dbdt_utps"] = out.groupby("sat_id", group_keys=False).apply(dbdt).values
    return out.dropna(subset=["ts"])

def main():
    # seed satellites A/B/C
    sats = pd.DataFrame([
        {"sat_id":"A","name":"Swarm Alpha","agency":"ESA"},
        {"sat_id":"B","name":"Swarm Bravo","agency":"ESA"},
        {"sat_id":"C","name":"Swarm Charlie","agency":"ESA"},
    ])
    pg.upsert("satellites", sats.to_dict(orient="records"), on_conflict="sat_id")

    total = 0
    for coll in ["SW_OPER_MAGA_LR_1B", "SW_OPER_MAGB_LR_1B", "SW_OPER_MAGC_LR_1B"]:
        try:
            df = fetch_swarm_l1b(coll, START, END)
        except Exception as e:
            print(f"[{coll}] error: {e}")
            df = pd.DataFrame()

        if not df.empty:
            pg.upsert("swarm_l1b", df.to_dict(orient="records"), on_conflict="ts,sat_id")
            print(f"[{coll}] rows: {len(df)}")
            total += len(df)
        else:
            print(f"[{coll}] no data for {START} → {END}")

    print(f"DONE. total rows inserted: {total}")

if __name__ == "__main__":
    main()
