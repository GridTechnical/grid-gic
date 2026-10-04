"""Join trailing L1 features to delayed band labels. No future L1 in X."""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from forecast.constants import (
    DECISION_MINUTES,
    GROUND_DBDT_THRESHOLD_NTS,
    SWARM_DBDT_THRESHOLD_UTPS,
)
from forecast.features import NUMERIC_FEATURES, add_trailing_features, decision_times
from forecast.labels import band_minute_max, labels_for_decisions
from forecast.nrcan import fetch_nrcan_dbdt
from forecast.omni import fetch_omni
from forecast.storms import assign_storm_ids
from forecast.swarm import fetch_swarm_minutes


def _windows_from_args(window: list[str] | None, start: str | None, end: str | None) -> list[tuple[str, str]]:
    windows: list[tuple[str, str]] = []
    for item in window or []:
        if "," not in item:
            raise SystemExit(f"--window must be START,END (got {item})")
        a, b = item.split(",", 1)
        windows.append((a.strip(), b.strip()))
    if start or end:
        if not (start and end):
            raise SystemExit("pass both --start and --end, or only --window")
        windows.append((start, end))
    if not windows:
        raise SystemExit("pass --window START,END at least once")
    return windows


def load_l1(source: str, windows: list[tuple[str, str]]) -> pd.DataFrame:
    frames = []
    for start, end in windows:
        if source == "omni":
            frames.append(fetch_omni(start, end))
        elif source == "parquet":
            raise SystemExit("use --l1-parquet for a local file")
        else:
            raise SystemExit(f"unknown L1 source {source}")
    raw = pd.concat(frames).sort_index()
    raw = raw[~raw.index.duplicated(keep="last")]
    return raw


def load_l1_parquet(path: str) -> pd.DataFrame:
    df = pd.read_parquet(path)
    if "time" in df.columns:
        df = df.set_index(pd.to_datetime(df["time"], utc=True))
    return df.sort_index()


def load_labels(kind: str, windows: list[tuple[str, str]], stations: list[str] | None):
    frames = []
    notes = []
    if kind == "nrcan":
        value_col = "dbdt_nts"
        unit = "nT/s"
        threshold = GROUND_DBDT_THRESHOLD_NTS
        for start, end in windows:
            # Labels run 90 min after the last L1 decision. Fetch that hour too.
            end_l = (pd.Timestamp(end) + pd.Timedelta(minutes=90)).strftime("%Y-%m-%dT%H:%M:%SZ")
            part = fetch_nrcan_dbdt(start, end_l, stations=stations)
            frames.append(part)
        source = "nrcan"
        notes.append("Ground labels: NRCan network C2, Canada only. Not a global grid.")
        notes.append("SuperMAG: MISSING (download requires a registered user id; no new secret was added).")
    elif kind == "swarm":
        value_col = "dbdt_uts"
        unit = "uT/s"
        threshold = SWARM_DBDT_THRESHOLD_UTPS
        for start, end in windows:
            end_l = (pd.Timestamp(end) + pd.Timedelta(minutes=90)).strftime("%Y-%m-%dT%H:%M:%SZ")
            frames.append(fetch_swarm_minutes(start, end_l))
        source = "swarm"
        notes.append("Ground magnetometer labels: MISSING.")
        notes.append("SuperMAG: MISSING (needs a user id).")
        notes.append(
            "Labels are Swarm along-track max |dB/dt| (heatmap quantity, µT/s). "
            "Satellite motion folds spatial gradients into dB/dt. Not ground dB/dt."
        )
    else:
        raise SystemExit("--labels must be nrcan or swarm")
    samples = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return samples, value_col, unit, threshold, source, notes


def build_training_table(
    l1_raw: pd.DataFrame,
    samples: pd.DataFrame,
    value_col: str,
    threshold: float,
    *,
    every_minutes: int = DECISION_MINUTES,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    features = add_trailing_features(l1_raw)
    storms = assign_storm_ids(features)
    decisions = decision_times(features, every_minutes=every_minutes)
    band_minutes = band_minute_max(samples, value_col) if len(samples) else pd.DataFrame()
    labels = labels_for_decisions(band_minutes, decisions, threshold)
    if labels.empty:
        return labels, features
    feat = features.loc[labels["time"]].reset_index(drop=True)
    # features.loc on a repeated time index repeats rows in lockstep with labels.
    storm = storms.loc[labels["time"]].reset_index(drop=True)
    table = pd.concat(
        [
            labels.reset_index(drop=True),
            feat[list(NUMERIC_FEATURES)].reset_index(drop=True),
            storm.reset_index(drop=True),
        ],
        axis=1,
    )
    return table, features


def write_table(table: pd.DataFrame, features: pd.DataFrame, path: str, meta: dict) -> None:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    table.to_parquet(out, index=False)
    meta_path = out.with_suffix(".meta.json")
    meta_path.write_text(json.dumps(meta, indent=2, default=str))
    # A small L1-only sample so predict can run without the gitignored table.
    sample_path = Path("forecast/examples/l1_decision_sample.parquet")
    sample_path.parent.mkdir(parents=True, exist_ok=True)
    decisions = decision_times(features, every_minutes=5)
    if len(decisions):
        take = decisions[:12]
        sample = features.loc[take, list(NUMERIC_FEATURES)].copy().reset_index()
        first = sample.columns[0]
        if first != "time":
            sample = sample.rename(columns={first: "time"})
        sample.to_parquet(sample_path, index=False)
        print(f"wrote {sample_path} rows={len(sample)} (L1 features only, no ground series)")
    print(f"wrote {out} rows={len(table)}")
