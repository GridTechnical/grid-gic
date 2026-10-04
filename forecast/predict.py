"""Load a saved model and emit band probabilities for the arrival hour.

  python -m forecast.predict \\
    --features forecast/examples/l1_decision_sample.parquet \\
    --at 2025-10-10T01:00:00Z

The feature row at --at must already be trailing-only L1 (see forecast.features).
This does not read future solar wind. It does not update the heatmap.
"""
from __future__ import annotations

import argparse
import json

import numpy as np
import pandas as pd

from forecast.constants import MLAT_BANDS
from forecast.features import NUMERIC_FEATURES
from forecast.modeling import load_bundle, matrix
from forecast.physics import band_center_mlat, geographic_lon_for_mlt, sector_center_mlt


def _load_features(path: str) -> pd.DataFrame:
    df = pd.read_parquet(path)
    if "time" not in df.columns:
        df = df.reset_index()
        df = df.rename(columns={df.columns[0]: "time"})
    df["time"] = pd.to_datetime(df["time"], utc=True)
    return df.sort_values("time")


def predict_frame(bundle: dict, features: pd.DataFrame) -> list[dict]:
    clf = bundle["classifier"]
    reg = bundle["regressor"]
    q10 = float(bundle["residual_q10"])
    q90 = float(bundle["residual_q90"])
    trained = set(bundle.get("trained_bands") or [])
    meta = bundle.get("meta") or {}
    unit = meta.get("unit")
    threshold = meta.get("threshold")
    records = []
    for i in range(len(features)):
        row = features.iloc[i]
        when = pd.Timestamp(row["time"])
        if when.tzinfo is None:
            when = when.tz_localize("UTC")
        else:
            when = when.tz_convert("UTC")
        expanded = []
        for band in MLAT_BANDS:
            for sector in bundle["mlt_sectors"]:
                item = {col: row[col] if col in row.index else np.nan for col in NUMERIC_FEATURES}
                item["mlat_band"] = band
                item["mlt_sector"] = sector
                item["mlat_band_code"] = MLAT_BANDS.index(band)
                item["mlt_sector_code"] = list(bundle["mlt_sectors"]).index(sector)
                expanded.append(item)
        x = matrix(pd.DataFrame(expanded))
        dbdt = np.clip(reg.predict(x), 0, None)
        if clf is None:
            prob = np.full(len(x), np.nan)
        else:
            prob = clf.predict_proba(x)[:, 1]
        bands = []
        for j, item in enumerate(expanded):
            center = sector_center_mlt(item["mlt_sector"])
            mlat = band_center_mlat(item["mlat_band"])
            # Geographic longitude is a lookup from UT + MLT, not the training target.
            try:
                glon = geographic_lon_for_mlt(center, when + pd.Timedelta(minutes=60), mlat)
            except Exception:
                glon = None
            lo = max(0.0, float(dbdt[j] + q10))
            hi = max(lo, float(dbdt[j] + q90))
            bands.append(
                {
                    "mlat_band": item["mlat_band"],
                    "mlt_sector": item["mlt_sector"],
                    "mlt_center_hours": center,
                    "geo_lon_center_deg": glon,
                    "supported_by_training": item["mlat_band"] in trained,
                    "p_exceed": None if not np.isfinite(prob[j]) else float(prob[j]),
                    "dbdt_expected": float(dbdt[j]),
                    "dbdt_range": [lo, hi],
                }
            )
        tau = row["tau_min"] if "tau_min" in row.index else None
        records.append(
            {
                "issue_time": when.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "label_window_start": (when + pd.Timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "label_window_end": (when + pd.Timedelta(minutes=90)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "tau_min_from_speed": None if pd.isna(tau) else float(tau),
                "threshold": threshold,
                "unit": unit,
                "label_source": meta.get("label_source"),
                "supermag": "MISSING",
                "substorm_onset": "jitters by tens of minutes; this window is not an onset clock",
                "bands": bands,
            }
        )
    return records


def main() -> None:
    p = argparse.ArgumentParser(description="Predict band |dB/dt| probabilities from saved L1 features")
    p.add_argument("--model", default="forecast/artifacts/band_dbdt_hgb.joblib")
    p.add_argument("--features", default="forecast/examples/l1_decision_sample.parquet")
    p.add_argument("--at", help="ISO UTC time. Default: last row in --features")
    p.add_argument("--out", help="optional JSON path")
    args = p.parse_args()
    bundle = load_bundle(args.model)
    feat = _load_features(args.features)
    if args.at:
        at = pd.Timestamp(args.at)
        if at.tzinfo is None:
            at = at.tz_localize("UTC")
        else:
            at = at.tz_convert("UTC")
        # Nearest decision, not a future row past the requested time.
        prior = feat[feat["time"] <= at]
        if prior.empty:
            raise SystemExit(f"no feature row at or before {at.isoformat()}")
        feat = prior.tail(1)
    else:
        feat = feat.tail(1)
    payload = predict_frame(bundle, feat)
    text = json.dumps(payload[0] if len(payload) == 1 else payload, indent=2)
    print(text)
    if args.out:
        from pathlib import Path
        Path(args.out).write_text(text)


if __name__ == "__main__":
    main()
