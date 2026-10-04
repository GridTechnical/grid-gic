"""Gradient-boosted P(band exceeds |dB/dt|) and an expected range.

One HistGradientBoosting model with magnetic-latitude band and MLT sector as
categorical features. Split is by whole storm, never by random minutes.
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    mean_absolute_error,
    roc_auc_score,
)

from forecast.constants import MLAT_BANDS, MLT_SECTORS
from forecast.features import CATEGORICAL_FEATURES, NUMERIC_FEATURES
from forecast.storms import train_holdout_ids

SECTORS = [name for name, _, _ in MLT_SECTORS]


def encode_bands(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    band_map = {name: i for i, name in enumerate(MLAT_BANDS)}
    sector_map = {name: i for i, name in enumerate(SECTORS)}
    out["mlat_band_code"] = out["mlat_band"].astype(str).map(band_map).astype("int64")
    out["mlt_sector_code"] = out["mlt_sector"].astype(str).map(sector_map).astype("int64")
    if out["mlat_band_code"].isna().any() or out["mlt_sector_code"].isna().any():
        unknown = sorted(set(out.loc[out["mlat_band_code"].isna(), "mlat_band"].astype(str)))
        unknown += sorted(set(out.loc[out["mlt_sector_code"].isna(), "mlt_sector"].astype(str)))
        raise ValueError(f"unknown band or sector: {unknown}")
    return out


def matrix(df: pd.DataFrame) -> pd.DataFrame:
    cols = list(NUMERIC_FEATURES) + list(CATEGORICAL_FEATURES)
    x = df[cols].copy()
    for col in NUMERIC_FEATURES:
        x[col] = pd.to_numeric(x[col], errors="coerce")
    return x


def _fit_models(train: pd.DataFrame):
    x = matrix(train)
    cat = [x.columns.get_loc(c) for c in CATEGORICAL_FEATURES]
    y_cls = train["y_exceed"].astype(int).to_numpy()
    y_reg = train["y_max_dbdt"].astype(float).to_numpy()
    leaf = 20 if len(train) >= 400 else 8
    common = dict(
        max_depth=3,
        max_iter=200,
        learning_rate=0.06,
        min_samples_leaf=leaf,
        l2_regularization=1.0,
        early_stopping=False,
        random_state=0,
        categorical_features=cat,
    )
    clf = None
    if len(np.unique(y_cls)) >= 2:
        clf = HistGradientBoostingClassifier(class_weight="balanced", **common)
        clf.fit(x, y_cls)
    reg = HistGradientBoostingRegressor(**common)
    reg.fit(x, y_reg)
    pred = reg.predict(x)
    resid = y_reg - pred
    q10, q90 = np.nanquantile(resid, [0.1, 0.9])
    return clf, reg, float(q10), float(q90)


def _scores(clf, reg, frame: pd.DataFrame) -> dict:
    if frame.empty:
        return {"n": 0}
    x = matrix(frame)
    y = frame["y_exceed"].astype(int).to_numpy()
    y_reg = frame["y_max_dbdt"].astype(float).to_numpy()
    out = {
        "n": int(len(frame)),
        "positive_rate": float(np.mean(y)) if len(y) else None,
        "mae_dbdt": float(mean_absolute_error(y_reg, reg.predict(x))),
    }
    if clf is None or len(np.unique(y)) < 2:
        out["roc_auc"] = None
        out["average_precision"] = None
        out["brier"] = None
        out["note"] = "classifier skipped or hold-out has one class"
        if clf is not None:
            p = clf.predict_proba(x)[:, 1]
            out["brier"] = float(brier_score_loss(y, p))
        return out
    p = clf.predict_proba(x)[:, 1]
    out["roc_auc"] = float(roc_auc_score(y, p))
    out["average_precision"] = float(average_precision_score(y, p))
    out["brier"] = float(brier_score_loss(y, p))
    return out


def train_from_table(table: pd.DataFrame, meta: dict | None = None) -> dict:
    table = encode_bands(table.dropna(subset=["y_max_dbdt", "y_exceed"]).copy())
    table["time"] = pd.to_datetime(table["time"], utc=True)
    storms = table[["storm_id", "storm_kind"]].copy()
    storms.index = table["time"]
    train_ids, hold_ids, dropped = train_holdout_ids(storms)
    train = table[table["storm_id"].isin(train_ids)].copy()
    hold = table[table["storm_id"].isin(hold_ids)].copy()
    if train.empty or hold.empty:
        raise RuntimeError("train or hold-out split is empty")
    clf, reg, q10, q90 = _fit_models(train)
    report = {
        "model": "sklearn.HistGradientBoosting",
        "task": "P(mlat/MLT band max |dB/dt| exceeds threshold on [t+30min, t+90min)) plus expected |dB/dt|",
        "train_storms": train_ids,
        "holdout_storms": hold_ids,
        "dropped_future_storms": dropped,
        "n_train": int(len(train)),
        "n_holdout": int(len(hold)),
        "train": _scores(clf, reg, train),
        "holdout": _scores(clf, reg, hold),
        "residual_q10": q10,
        "residual_q90": q90,
        "threshold": meta.get("threshold") if meta else None,
        "unit": meta.get("unit") if meta else None,
        "label_source": meta.get("label_source") if meta else None,
        "notes": (meta or {}).get("notes", []),
        "leakage": "Features are trailing L1 only. Hold-out is one whole later storm. Segments after that storm are dropped.",
    }
    bundle = {
        "classifier": clf,
        "regressor": reg,
        "residual_q10": q10,
        "residual_q90": q90,
        "feature_columns": list(NUMERIC_FEATURES) + list(CATEGORICAL_FEATURES),
        "mlat_bands": list(MLAT_BANDS),
        "mlt_sectors": list(SECTORS),
        "trained_bands": sorted(train["mlat_band"].astype(str).unique()),
        "trained_sectors": sorted(train["mlt_sector"].astype(str).unique()),
        "meta": report,
    }
    return {"bundle": bundle, "report": report}


def save_bundle(bundle: dict, path: str) -> None:
    import joblib

    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(bundle, out)
    metrics = out.with_name("smoke_metrics.json")
    metrics.write_text(json.dumps(bundle["meta"], indent=2))
    print(f"wrote {out}")
    print(f"wrote {metrics}")


def load_bundle(path: str) -> dict:
    import joblib

    return joblib.load(path)
