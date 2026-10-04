"""Train the band |dB/dt| model on a table from forecast.build_dataset.

  python -m forecast.train --table data/forecast/smoke.parquet
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from forecast.modeling import save_bundle, train_from_table


def main() -> None:
    p = argparse.ArgumentParser(description="Train a storm-holdout |dB/dt| band model")
    p.add_argument("--table", default="data/forecast/smoke.parquet")
    p.add_argument("--out", default="forecast/artifacts/band_dbdt_hgb.joblib")
    p.add_argument("--metrics", default=None, help="metrics JSON path (default: forecast/artifacts/smoke_metrics.json)")
    p.add_argument(
        "--min-hold-hours",
        type=float,
        default=0.0,
        help="hold out the latest active storm at least this long; shorter tail segments are dropped",
    )
    args = p.parse_args()
    table_path = Path(args.table)
    if not table_path.exists():
        raise SystemExit(f"missing {table_path}. Run python -m forecast.build_dataset first.")
    table = pd.read_parquet(table_path)
    meta_path = table_path.with_suffix(".meta.json")
    meta = json.loads(meta_path.read_text()) if meta_path.exists() else {}
    span = pd.Timedelta(hours=args.min_hold_hours) if args.min_hold_hours > 0 else None
    trained = train_from_table(table, meta, min_hold_span=span)
    save_bundle(trained["bundle"], args.out, metrics_path=args.metrics)
    report = trained["report"]
    print(json.dumps({k: report[k] for k in ("holdout_storms", "n_train", "n_holdout", "train", "holdout", "unit", "threshold", "label_source")}, indent=2))


if __name__ == "__main__":
    main()
