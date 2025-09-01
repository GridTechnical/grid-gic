#!/usr/bin/env python3
import os, json
from datetime import datetime, timezone
import pandas as pd
import numpy as np
import requests
from viresclient import SwarmRequest
import time

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
        self.session = requests.Session()

    def _warm_table(self, table: str):
        # Best-effort: hit a tiny SELECT to force PostgREST to load the schema
        try:
            url = f"{self.base}/{table}?select=1&limit=1"
            self.session.get(url, headers=self.headers, timeout=15)
        except requests.RequestException:
            pass  # warming is best-effort

    def upsert(self, table: str, records: list[dict], on_conflict: str, batch_size: int = 1000):
        if not records:
            print(f"[{table}] nothing to upsert")
            return
        self._warm_table(table)

        for i in range(0, len(records), batch_size):
            chunk = records[i:i+batch_size]
            url = f"{self.base}/{table}?on_conflict={on_conflict}"

            # retry on transient network/503 for PostgREST cache warm-up
            for attempt in range(6):  # ~ 0s,1s,2s,4s,8s,16s
                try:
                    r = self.session.post(
                        url, headers=self.headers,
                        data=json.dumps(chunk, default=str),
                        timeout=180
                    )
                except requests.RequestException as e:
                    if attempt == 5:
                        raise RuntimeError(f"Upsert {table} request error: {e}")
                    time.sleep(2 ** attempt)
                    continue

                if r.status_code >= 500 or r.status_code == 503:
                    if attempt == 5:
                        raise RuntimeError(f"Upsert {table} failed {r.status_code}: {r.text}")
                    time.sleep(2 ** attempt)
                    continue

                if r.status_code >= 300:
                    raise RuntimeError(f"Upsert {table} failed {r.status_code}: {r.text}")

                print(f"[{table}] upserted {len(chunk)} rows")
                break

pg = SupabaseREST(SUPABASE_URL, SUPABASE_SERVICE_KEY, schema="geomag")

# ---- Swarm fetch ----
def fetch_swarm_l1b(collection: str, start_iso: str, end_iso: str) -> pd.DataFrame:
    """Fetch 1 Hz L1B NEC vectors + position for a Swarm spacecraft via VirES.
       Handles multiple possible column namings for B_NEC."""
    VIRES_URL = os.environ.get("VIRES_URL", "https://vires.services/ows")
    req = SwarmRequest(url=VIRES_URL, token=os.environ["VIRES_TOKEN"])

    req.set_collection(collection)
    req.set_products(
        measurements=["B_NEC"],                          # vector field
        auxiliaries=["Spacecraft", "Latitude", "Longitude", "Radius"],
    )

    data = req.get_between(start_time=start_iso, end_time=end_iso)
    df = data.as_dataframe()
    if df.empty:
        return pd.DataFrame()

    # --- Extract B_NEC components regardless of column naming ---
    bn, be, bd = None, None, None

    # Case 1: split columns like B_NEC_N/E/C
    if {"B_NEC_N","B_NEC_E","B_NEC_C"}.issubset(df.columns):
        bn = df["B_NEC_N"] / 1000.0
        be = df["B_NEC_E"] / 1000.0
        bd = df["B_NEC_C"] / 1000.0

    # Case 2: split columns like B_NEC_X/Y/Z
    elif {"B_NEC_X","B_NEC_Y","B_NEC_Z"}.issubset(df.columns):
        bn = df["B_NEC_X"] / 1000.0
        be = df["B_NEC_Y"] / 1000.0
        bd = df["B_NEC_Z"] / 1000.0

    # Case 3: indexed columns like B_NEC_0/1/2
    elif {"B_NEC_0","B_NEC_1","B_NEC_2"}.issubset(df.columns):
        bn = df["B_NEC_0"] / 1000.0
        be = df["B_NEC_1"] / 1000.0
        bd = df["B_NEC_2"] / 1000.0

    # Case 4: single column "B_NEC" containing arrays
    elif "B_NEC" in df.columns:
        vecs = df["B_NEC"].apply(
            lambda v: v if hasattr(v, "__len__") and len(v) == 3 else [np.nan, np.nan, np.nan]
        )
        vecs = pd.DataFrame(vecs.tolist(), index=df.index, columns=["N","E","C"])
        bn = vecs["N"] / 1000.0
        be = vecs["E"] / 1000.0
        bd = vecs["C"] / 1000.0
    else:
        # If none of the above matched, show the columns we got for quick debug
        raise KeyError(f"No recognizable B_NEC columns in DataFrame: {list(df.columns)}")

    # Altitude (km): Radius is meters from Earth center; subtract ~6371 km
    alt_km = (df["Radius"] / 1000.0) - 6371.0

    # Normalize timestamps to UTC (handle tz-naive index)
    ts = pd.to_datetime(df.index)
    if getattr(ts, "tz", None) is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")

    out = pd.DataFrame({
        "ts": ts,
        "sat_id": df["Spacecraft"].map({"A":"A","B":"B","C":"C"}).fillna("A"),
        "lat_deg": df["Latitude"].astype(float),
        "lon_deg": df["Longitude"].astype(float),
        "alt_km": alt_km.astype(float),
        "bn_ut": bn.astype(float),
        "be_ut": be.astype(float),
        "bd_ut": bd.astype(float),
    }).sort_values(["sat_id","ts"])


    # simple |dB/dt| over 60 s window (1 Hz data)
    diff = out.groupby("sat_id")[["bn_ut","be_ut","bd_ut"]].diff(60)
    out["dbdt_utps"] = np.sqrt((diff**2).sum(axis=1)) / 60.0
    return out.dropna(subset=["ts"])


def main():
    # seed satellites A/B/C (best-effort; skip if PostgREST still warming)
    sats = pd.DataFrame([
        {"sat_id":"A","name":"Swarm Alpha","agency":"ESA"},
        {"sat_id":"B","name":"Swarm Bravo","agency":"ESA"},
        {"sat_id":"C","name":"Swarm Charlie","agency":"ESA"},
    ])
    try:
        pg.upsert("satellites", sats.to_dict(orient="records"), on_conflict="sat_id")
    except Exception as e:
        print(f"[satellites] warning: {e} (continuing)")
        
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
    if total == 0:
        raise SystemExit(2)

if __name__ == "__main__":
    main()
