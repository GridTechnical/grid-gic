#!/usr/bin/env python3
import os, sys, json, time, datetime as dt, requests

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
SOURCE_URL = os.environ.get("SWPC_SOLARWIND_URL", "https://services.swpc.noaa.gov/products/summary/solar-wind.json")
SOURCE_URL = os.environ.get("SWPC_SOLARWIND_URL") or \
             "https://services.swpc.noaa.gov/products/summary/solar-wind.json"
def upsert(rows):
    if not rows: return
    url = f"{SUPABASE_URL}/rest/v1/solar_wind_5m?on_conflict=utc_ts"
    headers = {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Profile": "watch",
        "Accept-Profile": "watch",
        "Prefer": "resolution=merge-duplicates,return=minimal",
        "Content-Type": "application/json",
    }
    r = requests.post(url, headers=headers, data=json.dumps(rows, default=str), timeout=30)
    if r.status_code >= 300:
        raise SystemExit(f"Upsert failed {r.status_code}: {r.text}")
    print(f"upserted {len(rows)} row(s)")

def parse_records(payload):
    """
    Expect SWPC 'summary/solar-wind.json' schema:
    [ ["time_tag","density","speed","bt","bz_gsm"], ... ]
    Keep last ~3 hours at 5-min stride (downsample by time bucket).
    """
    if not payload or not isinstance(payload, list) or not payload[0]:
        return []
    header, *rows = payload
    idx = {k:i for i,k in enumerate(header)}
    out = {}
    now = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
    earliest = now - dt.timedelta(hours=6)  # keep 6h just in case
    for r in rows:
        try:
            t = dt.datetime.fromisoformat(r[idx["time_tag"]].replace("Z","+00:00"))
        except Exception:
            continue
        if t.tzinfo is None:
            t = t.replace(tzinfo=dt.timezone.utc)
        if t < earliest: 
            continue
        # 5-min bucket key
        bucket = t.replace(minute=(t.minute//5)*5, second=0, microsecond=0)
        try:
            n = float(r[idx.get("density", -1)]) if idx.get("density", -1) != -1 else None
            v = float(r[idx.get("speed",   -1)]) if idx.get("speed",   -1) != -1 else None
            bt= float(r[idx.get("bt",      -1)]) if idx.get("bt",      -1) != -1 else None
            bz= float(r[idx.get("bz_gsm",  -1)]) if idx.get("bz_gsm",  -1) != -1 else None
        except Exception:
            continue
        ey = None
        if v is not None and bz is not None:
            ey = -(v * bz) / 1000.0  # mV/m
        out[bucket] = {
            "utc_ts": bucket.isoformat().replace("+00:00","Z"),
            "bz_nt": bz, "bt_nt": bt, "vx_kmps": v, "n_cm3": n,
            "ey_mVpm": ey, "pd_nPa": None,
            "src": "SWPC summary/solar-wind.json",
        }
    return list(out.values())

def main():
    try:
        resp = requests.get(SOURCE_URL, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
    except Exception as e:
        raise SystemExit(f"Fetch error: {e}")
    rows = parse_records(payload)
    upsert(rows)

if __name__ == "__main__":
    main()
