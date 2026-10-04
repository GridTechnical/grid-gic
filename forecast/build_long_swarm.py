"""OMNI + Swarm training table over every OMNI-usable Swarm coverage block.

Swarm ``geomag.swarm_l1m`` on 2026-10-04:

- min minute_ts 2025-07-01 00:00Z, max 2026-09-30 23:59Z
- 436,320 rows, 3 satellites, 102 distinct UTC days

Days are full (4320 rows = 3 sats x 1440) except 2025-09-24, 2025-10-12, and
2025-10-13 (2 sats, 2880 rows). Large holes:

- 2025-07-31 through 2025-08-26
- 2025-09-26 and 2025-09-27
- 2025-11-05 through 2026-09-26

2026-09-27 through 2026-09-30 is in Swarm but OMNI 1-minute high-res lags about
120 days, so those four days are not usable for this L1 history. They are not
downloaded.

Decisions are every 15 minutes (thinner than the 5-minute smoke) so the long
table is not just copies of the same hour. Labels stay on [t+30, t+90).
Cache lands in ``data/forecast/cache/`` (gitignored). The table is gitignored.
Do not commit the Swarm series or any NRCan series.

  python -m forecast.build_long_swarm
  python -m forecast.train --table data/forecast/swarm_long.parquet \\
      --metrics forecast/artifacts/swarm_long_metrics.json --min-hold-hours 6
"""
from __future__ import annotations

import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from forecast.constants import SWARM_DBDT_THRESHOLD_UTPS
from forecast.dataset import build_training_table, write_table
from forecast.omni import fetch_omni
from forecast.swarm import fetch_swarm_minutes

# Inclusive start, exclusive end. Each block is contiguous Swarm coverage that
# also sits inside the OMNI high-res lag.
USABLE_WINDOWS: list[tuple[str, str]] = [
    ("2025-07-01T00:00:00Z", "2025-07-31T00:00:00Z"),
    ("2025-08-27T00:00:00Z", "2025-09-26T00:00:00Z"),
    ("2025-09-28T00:00:00Z", "2025-11-05T00:00:00Z"),
]

COVERAGE_NOTES = [
    "Ground magnetometer labels: MISSING.",
    "SuperMAG: MISSING (needs a registered user id; no new secret was added).",
    "Labels are Swarm along-track max |dB/dt| (heatmap quantity, uT/s). "
    "Satellite motion folds spatial gradients into dB/dt. Not ground dB/dt.",
    "geomag.swarm_l1m queried 2026-10-04: min 2025-07-01T00:00Z, max 2026-09-30T23:59Z, "
    "436320 rows, 3 sats, 102 days.",
    "Coverage holes with no Swarm minutes: 2025-07-31..2025-08-26, 2025-09-26..2025-09-27, "
    "2025-11-05..2026-09-26.",
    "2026-09-27..2026-09-30 has Swarm but OMNI 1-minute high-res lags ~120 days "
    "(safe end before 2026-06-06 on this run date), so that block is excluded.",
    "Partial Swarm days kept: 2025-09-24, 2025-10-12, 2025-10-13 (2 of 3 satellites).",
    "Decision cadence is 15 min, not 5, so neighboring rows are less redundant.",
    "Pdyn is recomputed only when density and speed are real. Features are trailing 30 and 60 min. "
    "No future L1.",
    f"Exceedance threshold is the code default {SWARM_DBDT_THRESHOLD_UTPS} uT/s. "
    "On a 2025-07-01..04 probe the band-hour max exceeded 0.05 on about 34% of rows, "
    "so the cut is not saturated.",
]

CACHE = Path("data/forecast/cache")


def _days(start: str, end: str) -> list[datetime]:
    a = pd.Timestamp(start).tz_convert("UTC").to_pydatetime()
    b = pd.Timestamp(end).tz_convert("UTC").to_pydatetime()
    out = []
    cursor = a
    while cursor < b:
        out.append(cursor)
        cursor = cursor + timedelta(days=1)
    return out


def _all_days() -> list[datetime]:
    days: list[datetime] = []
    for start, end in USABLE_WINDOWS:
        days.extend(_days(start, end))
    return days


def _retry(label: str, fn, attempts: int = 4):
    last: Exception | None = None
    for i in range(attempts):
        try:
            return fn()
        except Exception as exc:  # network flake, not a secret
            last = exc
            wait = 2 * (i + 1)
            print(f"retry {label} attempt {i + 1}: {type(exc).__name__} {exc}", flush=True)
            time.sleep(wait)
    assert last is not None
    raise last


def _cache_omni_day(day: datetime) -> tuple[str, int]:
    stamp = day.strftime("%Y%m%d")
    path = CACHE / "omni" / f"{stamp}.parquet"
    if path.exists() and path.stat().st_size > 0:
        n = len(pd.read_parquet(path))
        return stamp, n
    end = day + timedelta(days=1)

    def pull():
        return fetch_omni(day.isoformat(), end.isoformat())

    df = _retry(f"omni {stamp}", pull)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".parquet.tmp")
    df.to_parquet(tmp)
    tmp.replace(path)
    return stamp, len(df)


def _cache_swarm_day(day: datetime) -> tuple[str, int]:
    stamp = day.strftime("%Y%m%d")
    path = CACHE / "swarm" / f"{stamp}.parquet"
    if path.exists() and path.stat().st_size > 0:
        n = len(pd.read_parquet(path))
        return stamp, n
    end = day + timedelta(days=1)
    start_s = day.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_s = end.strftime("%Y-%m-%dT%H:%M:%SZ")

    def pull():
        return fetch_swarm_minutes(start_s, end_s)

    df = _retry(f"swarm {stamp}", pull)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".parquet.tmp")
    df.to_parquet(tmp)
    tmp.replace(path)
    return stamp, len(df)


def _load_omni(days: list[datetime]) -> pd.DataFrame:
    frames = []
    for day in days:
        path = CACHE / "omni" / f"{day.strftime('%Y%m%d')}.parquet"
        df = pd.read_parquet(path)
        if df.empty:
            continue
        if "time" in df.columns:
            df = df.set_index(pd.to_datetime(df["time"], utc=True))
        else:
            df.index = pd.to_datetime(df.index, utc=True)
        frames.append(df)
    raw = pd.concat(frames).sort_index()
    raw = raw[~raw.index.duplicated(keep="last")]
    return raw


def _load_swarm(days: list[datetime]) -> pd.DataFrame:
    frames = []
    for day in days:
        path = CACHE / "swarm" / f"{day.strftime('%Y%m%d')}.parquet"
        df = pd.read_parquet(path)
        if df.empty:
            continue
        frames.append(df)
    samples = pd.concat(frames, ignore_index=True)
    samples["time"] = pd.to_datetime(samples["time"], utc=True)
    return samples


def main() -> None:
    os.environ.setdefault("SUPABASE_URL", "https://uajnhkmwfskddeizcpir.supabase.co")
    days = _all_days()
    print(f"usable UTC days={len(days)} windows={USABLE_WINDOWS}", flush=True)
    workers = 3
    omni_counts: dict[str, int] = {}
    swarm_counts: dict[str, int] = {}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = []
        for day in days:
            futs.append(("omni", pool.submit(_cache_omni_day, day)))
            futs.append(("swarm", pool.submit(_cache_swarm_day, day)))
        done = 0
        for kind, fut in futs:
            stamp, n = fut.result()
            (omni_counts if kind == "omni" else swarm_counts)[stamp] = n
            done += 1
            if done % 10 == 0 or done == len(futs):
                print(f"cached {done}/{len(futs)}", flush=True)
    empty_omni = sorted(k for k, n in omni_counts.items() if n == 0)
    empty_swarm = sorted(k for k, n in swarm_counts.items() if n == 0)
    print(
        f"OMNI days={len(omni_counts)} rows={sum(omni_counts.values())} empty={empty_omni}",
        flush=True,
    )
    print(
        f"Swarm days={len(swarm_counts)} rows={sum(swarm_counts.values())} empty={empty_swarm}",
        flush=True,
    )
    raw = _load_omni(days)
    samples = _load_swarm(days)
    print(f"loaded L1={len(raw)} swarm samples={len(samples)}", flush=True)
    table, features = build_training_table(
        raw,
        samples,
        "dbdt_uts",
        SWARM_DBDT_THRESHOLD_UTPS,
        every_minutes=15,
    )
    if table.empty:
        raise SystemExit("no joined rows")
    # Per-window row counts use the decision time, which sits inside the L1 window.
    window_rows = []
    for start, end in USABLE_WINDOWS:
        a = pd.Timestamp(start)
        b = pd.Timestamp(end)
        n = int(((table["time"] >= a) & (table["time"] < b)).sum())
        window_rows.append({"start": start, "end": end, "rows": n})
    storms = (
        table.groupby(["storm_id", "storm_kind"], observed=True)
        .agg(rows=("time", "size"), t0=("time", "min"), t1=("time", "max"))
        .reset_index()
        .sort_values("t0")
    )
    meta = {
        "label_source": "swarm",
        "unit": "uT/s",
        "threshold": SWARM_DBDT_THRESHOLD_UTPS,
        "value_col": "dbdt_uts",
        "windows": USABLE_WINDOWS,
        "window_rows": window_rows,
        "l1": "omni",
        "every_minutes": 15,
        "n_rows": int(len(table)),
        "n_l1": int(len(raw)),
        "n_label_samples": int(len(samples)),
        "notes": COVERAGE_NOTES + ([f"OMNI days with zero rows: {empty_omni}"] if empty_omni else []),
        "supermag": "MISSING",
        "ground_labels": "MISSING",
        "positive_rate": float(table["y_exceed"].mean()),
        "bands": sorted(table["mlat_band"].astype(str).unique()),
        "n_storm_segments": int((storms["storm_kind"] == "storm").sum()),
        "n_quiet_segments": int((storms["storm_kind"] == "quiet").sum()),
        "built_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    out = "data/forecast/swarm_long.parquet"
    write_table(table, features, out, meta)
    summary = {
        "n_rows": meta["n_rows"],
        "positive_rate": meta["positive_rate"],
        "window_rows": window_rows,
        "n_l1": meta["n_l1"],
        "n_label_samples": meta["n_label_samples"],
        "n_storm_segments": meta["n_storm_segments"],
        "y_max_p50": float(table["y_max_dbdt"].median()),
        "y_max_p90": float(table["y_max_dbdt"].quantile(0.9)),
    }
    print(json.dumps(summary, indent=2), flush=True)
    print("storm segments (id, kind, rows, start, end):", flush=True)
    for rec in storms.itertuples(index=False):
        print(
            f"  {rec.storm_id} {rec.storm_kind} rows={rec.rows} "
            f"{pd.Timestamp(rec.t0).strftime('%Y-%m-%dT%H:%MZ')} → "
            f"{pd.Timestamp(rec.t1).strftime('%Y-%m-%dT%H:%MZ')}",
            flush=True,
        )


if __name__ == "__main__":
    main()
