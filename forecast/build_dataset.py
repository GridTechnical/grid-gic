"""Build a feature + label table. Writes under data/ (gitignored).

Examples:
  python -m forecast.build_dataset \\
    --window 2025-10-10T00:00:00Z,2025-10-13T00:00:00Z \\
    --window 2025-10-28T00:00:00Z,2025-10-31T00:00:00Z \\
    --labels nrcan --out data/forecast/smoke.parquet
"""
from __future__ import annotations

import argparse
import json

import pandas as pd

from forecast.dataset import _windows_from_args, build_training_table, load_labels, load_l1, load_l1_parquet, write_table


def main() -> None:
    p = argparse.ArgumentParser(description="Build L1 features and delayed |dB/dt| band labels")
    p.add_argument("--window", action="append", default=[], help="START,END (repeatable)")
    p.add_argument("--start")
    p.add_argument("--end")
    p.add_argument("--l1", choices=["omni", "parquet"], default="omni")
    p.add_argument("--l1-parquet", help="local OMNI-like parquet when --l1 parquet")
    p.add_argument("--labels", choices=["nrcan", "swarm"], default="nrcan")
    p.add_argument("--stations", default="", help="comma-separated NRCan station codes (default: all primaries)")
    p.add_argument("--out", default="data/forecast/training.parquet")
    p.add_argument("--every-minutes", type=int, default=5)
    p.add_argument("--threshold", type=float, default=None, help="override |dB/dt| exceedance cut in the label unit")
    args = p.parse_args()
    windows = _windows_from_args(args.window, args.start, args.end)
    if args.l1 == "parquet":
        if not args.l1_parquet:
            raise SystemExit("--l1 parquet needs --l1-parquet")
        raw = load_l1_parquet(args.l1_parquet)
    else:
        raw = load_l1("omni", windows)
    stations = [s.strip() for s in args.stations.split(",") if s.strip()] or None
    samples, value_col, unit, threshold, source, notes = load_labels(args.labels, windows, stations)
    if args.threshold is not None:
        threshold = float(args.threshold)
        notes.append(f"Threshold override: {threshold} {unit}")
    print(f"L1 rows={len(raw)} label samples={len(samples)} source={source} unit={unit}")
    if samples.empty:
        notes.append("LABELS MISSING: no magnetometer samples in the requested windows.")
        print("\n".join(notes))
        raise SystemExit(2)
    table, features = build_training_table(
        raw, samples, value_col, threshold, every_minutes=args.every_minutes
    )
    meta = {
        "label_source": source,
        "unit": unit,
        "threshold": threshold,
        "value_col": value_col,
        "windows": windows,
        "l1": args.l1,
        "n_rows": int(len(table)),
        "n_l1": int(len(raw)),
        "n_label_samples": int(len(samples)),
        "notes": notes,
        "supermag": "MISSING",
        "positive_rate": float(table["y_exceed"].mean()) if len(table) else None,
        "bands": sorted(table["mlat_band"].astype(str).unique()) if len(table) else [],
    }
    if table.empty:
        notes.append("LABELS MISSING after the 30–90 min join (no band samples inside label windows).")
        print(json.dumps(meta, indent=2))
        raise SystemExit(2)
    write_table(table, features, args.out, meta)
    print(json.dumps({k: meta[k] for k in ("label_source", "unit", "threshold", "n_rows", "positive_rate", "bands", "supermag")}, indent=2))
    for line in notes:
        print(line)


if __name__ == "__main__":
    main()
