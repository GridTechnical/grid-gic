"""Gradient-boosted P(band exceeds |dB/dt|) and an expected range.

One HistGradientBoosting model with magnetic-latitude band and MLT sector as
categorical features. Split is by whole storm, never by random minutes.

On the long Swarm table the shallow balanced model redraws the band×MLT map
and does not beat that climatology on Brier. Storm-fold CV selected a deeper
unweighted model. A second head is trained on the within-cell upper quartile
so skill is also reported after the orbital climatology is removed.
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
from forecast.features import CATEGORICAL_FEATURES, NUMERIC_FEATURES, RECENT_DBDT_FEATURES
from forecast.storms import train_holdout_ids

SECTORS = [name for name, _, _ in MLT_SECTORS]
CELL_QUANTILE = 0.75


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


def matrix(df: pd.DataFrame, extra: list[str] | None = None) -> pd.DataFrame:
    cols = list(NUMERIC_FEATURES) + list(extra or []) + list(CATEGORICAL_FEATURES)
    x = df.reindex(columns=cols)
    for col in cols:
        if col in CATEGORICAL_FEATURES:
            continue
        x[col] = pd.to_numeric(x[col], errors="coerce")
    return x


def _hparams(n_rows: int) -> tuple[dict, str | None]:
    """Capacity from 5-fold storm CV on the long table, not from the official hold-out.

    depth 3 + class_weight balanced (the previous artifact): mean AUC 0.912 vs
    band×MLT 0.901, Brier 0.123 vs 0.121. No probability win.
    depth 6, min_samples_leaf 80, no class weight: mean AUC 0.956 vs 0.901,
    Brier 0.080 vs 0.121. That is the long-table fit. Small tables stay shallow
    and balanced; they do not have enough storms to grow the trees.
    """
    if n_rows >= 80000:
        common = dict(
            max_depth=6,
            max_iter=200,
            learning_rate=0.05,
            min_samples_leaf=80,
            l2_regularization=1.0,
            early_stopping=False,
            random_state=0,
        )
        return common, None
    leaf = 20 if n_rows >= 400 else 8
    common = dict(
        max_depth=3,
        max_iter=200,
        learning_rate=0.06,
        min_samples_leaf=leaf,
        l2_regularization=1.0,
        early_stopping=False,
        random_state=0,
    )
    return common, "balanced"


def _fit_pair(train: pd.DataFrame, y_cls: str = "y_exceed", y_reg: str = "y_max_dbdt", extra: list[str] | None = None):
    x = matrix(train, extra=extra)
    cat = [x.columns.get_loc(c) for c in CATEGORICAL_FEATURES]
    common, class_weight = _hparams(len(train))
    common = dict(common)
    common["categorical_features"] = cat
    y_c = train[y_cls].astype(int).to_numpy()
    y_r = train[y_reg].astype(float).to_numpy()
    clf = None
    if len(np.unique(y_c)) >= 2:
        kw = dict(common)
        if class_weight:
            kw["class_weight"] = class_weight
        clf = HistGradientBoostingClassifier(**kw)
        clf.fit(x, y_c)
    reg = HistGradientBoostingRegressor(**common)
    reg.fit(x, y_r)
    resid = y_r - reg.predict(x)
    q10, q90 = np.nanquantile(resid, [0.1, 0.9])
    return clf, reg, float(q10), float(q90), extra or []


def _cell_keys(frame: pd.DataFrame) -> pd.Series:
    return frame["mlat_band"].astype(str) + "|" + frame["mlt_sector"].astype(str)


def _mark_above_cell(train: pd.DataFrame, frame: pd.DataFrame, value_col: str, q: float = CELL_QUANTILE):
    """1 when value_col is at or above the train quantile of that band×sector. NaN if either is missing."""
    tr = train.loc[train[value_col].notna(), ["mlat_band", "mlt_sector", value_col]].copy()
    tr["key"] = _cell_keys(tr)
    qv = tr.groupby("key")[value_col].quantile(q)

    def mark(df: pd.DataFrame) -> np.ndarray:
        keys = _cell_keys(df)
        thr = keys.map(qv).to_numpy(dtype=float)
        y = pd.to_numeric(df[value_col], errors="coerce").to_numpy(dtype=float)
        out = np.full(len(df), np.nan)
        ok = np.isfinite(y) & np.isfinite(thr)
        out[ok] = (y[ok] >= thr[ok]).astype(float)
        return out

    return mark(train), mark(frame)


def band_sector_climatology(train: pd.DataFrame, frame: pd.DataFrame, y_col: str = "y_exceed", y_reg: str | None = "y_max_dbdt") -> dict:
    """Lookup of train rate (and mean, if y_reg is set) by band and MLT sector.

    Swarm along-track |dB/dt| is large in some bands even in quiet L1, so a
    high AUC can be that map rather than a solar-wind forecast.
    """
    if frame.empty or train.empty or y_col not in frame.columns:
        return {}
    y = frame[y_col].astype(int).to_numpy()
    p = float(train[y_col].mean())
    tr = train.assign(b=train["mlat_band"].astype(str), s=train["mlt_sector"].astype(str))
    rate = tr.groupby(["b", "s"])[y_col].mean()
    clim_p = []
    for b, s in zip(frame["mlat_band"].astype(str), frame["mlt_sector"].astype(str)):
        key = (b, s)
        clim_p.append(float(rate.loc[key]) if key in rate.index else p)
    clim_p_a = np.asarray(clim_p, dtype=float)
    out = {
        "constant_positive_rate": p,
        "constant_brier": float(brier_score_loss(y, np.full(len(y), p))),
        "band_sector_brier": float(brier_score_loss(y, clim_p_a)),
        "band_sector_roc_auc": None,
    }
    if y_reg is not None and y_reg in frame.columns and y_reg in train.columns:
        mu = tr.groupby(["b", "s"])[y_reg].mean()
        fallback_mu = float(train[y_reg].mean())
        clim_y = []
        for b, s in zip(frame["mlat_band"].astype(str), frame["mlt_sector"].astype(str)):
            key = (b, s)
            clim_y.append(float(mu.loc[key]) if key in mu.index else fallback_mu)
        out["band_sector_mae"] = float(mean_absolute_error(frame[y_reg].astype(float), np.asarray(clim_y, dtype=float)))
    if len(np.unique(y)) >= 2 and np.unique(np.round(clim_p_a, 6)).size >= 2:
        out["band_sector_roc_auc"] = float(roc_auc_score(y, clim_p_a))
    return out


def _scores(clf, reg, frame: pd.DataFrame, y_col: str = "y_exceed", y_reg: str = "y_max_dbdt", extra: list[str] | None = None) -> dict:
    if frame.empty:
        return {"n": 0}
    x = matrix(frame, extra=extra)
    y = frame[y_col].astype(int).to_numpy()
    out = {
        "n": int(len(frame)),
        "positive_rate": float(np.mean(y)) if len(y) else None,
    }
    if reg is not None and y_reg in frame.columns:
        out["mae_dbdt"] = float(mean_absolute_error(frame[y_reg].astype(float), reg.predict(x)))
    if clf is None or len(np.unique(y)) < 2:
        out["roc_auc"] = None
        out["average_precision"] = None
        out["brier"] = None
        out["note"] = "classifier skipped or hold-out has one class"
        if clf is not None and len(y):
            p = clf.predict_proba(x)[:, 1]
            out["brier"] = float(brier_score_loss(y, p))
        return out
    p = clf.predict_proba(x)[:, 1]
    out["roc_auc"] = float(roc_auc_score(y, p))
    out["average_precision"] = float(average_precision_score(y, p))
    out["brier"] = float(brier_score_loss(y, p))
    return out


def _residual_head(train: pd.DataFrame, hold: pd.DataFrame, value_col: str) -> tuple[dict | None, object | None]:
    """Classifier for 'above this cell's train upper quartile' of value_col.

    The band×MLT climatology of this label is nearly flat (about 1-q in every
    cell), so its AUC is not the orbital map. Train quantiles only.
    """
    if value_col not in train.columns or train[value_col].notna().mean() < 0.5:
        return None, None
    y_tr, y_ho = _mark_above_cell(train, hold, value_col)
    tr = train.copy()
    ho = hold.copy()
    tr["_hot"] = y_tr
    ho["_hot"] = y_ho
    tr = tr.dropna(subset=["_hot"])
    ho = ho.dropna(subset=["_hot"])
    if tr.empty or ho.empty or tr["_hot"].nunique() < 2:
        return None, None
    clf, _, _, _, _ = _fit_pair(tr, y_cls="_hot", y_reg=value_col)
    report = {
        "value": value_col,
        "rule": f"train band×MLT quantile {CELL_QUANTILE:.2f}; label is 1 at or above that cell threshold",
        "train": _scores(clf, None, tr, y_col="_hot"),
        "holdout": _scores(clf, None, ho, y_col="_hot"),
        "holdout_climatology": band_sector_climatology(tr, ho, y_col="_hot", y_reg=None),
    }
    return report, clf


def _recent_ablation(train: pd.DataFrame, hold: pd.DataFrame) -> dict | None:
    """Same primary label, plus recent |dB/dt| that ends at t. Not the saved model.

    forecast.predict has L1 only. A feature that is usually observed would send
    every served row down the missing branch. The ablation is the skill if a
    Swarm or ground nowcast is in hand at decision time.
    """
    present = [c for c in RECENT_DBDT_FEATURES if c in train.columns and train[c].notna().mean() > 0.05]
    if not present:
        return None
    clf, reg, _, _, _ = _fit_pair(train, extra=present)
    return {
        "features": present,
        "non_null_rate_train": {c: float(train[c].notna().mean()) for c in present},
        "holdout": _scores(clf, reg, hold, extra=present),
        "note": "Not saved. Serving is L1-only; these columns would be missing at predict time.",
    }


def _earlier_storm_sensitivity(table: pd.DataFrame, hold_ids: list[str]) -> dict | None:
    """Repeat the fit with storm_20251028T0738Z held out, training only on earlier rows."""
    sid = "storm_20251028T0738Z"
    if sid not in set(table["storm_id"].astype(str)) or sid in hold_ids:
        return None
    t0 = table.loc[table["storm_id"].astype(str) == sid, "time"].min()
    earlier = table[table["time"] < t0]
    hold = table[table["storm_id"].astype(str) == sid]
    if earlier.empty or hold.empty:
        return None
    clf, reg, _, _, _ = _fit_pair(earlier)
    return {
        "storm": sid,
        "n_train": int(len(earlier)),
        "n_holdout": int(len(hold)),
        "holdout": _scores(clf, reg, hold),
        "holdout_climatology": band_sector_climatology(earlier, hold),
    }


def train_from_table(
    table: pd.DataFrame,
    meta: dict | None = None,
    min_hold_span: pd.Timedelta | None = None,
) -> dict:
    table = encode_bands(table.dropna(subset=["y_max_dbdt", "y_exceed"]).copy())
    table["time"] = pd.to_datetime(table["time"], utc=True)
    storms = table[["storm_id", "storm_kind"]].copy()
    storms.index = table["time"]
    train_ids, hold_ids, dropped = train_holdout_ids(storms, min_hold_span=min_hold_span)
    train = table[table["storm_id"].isin(train_ids)].copy()
    hold = table[table["storm_id"].isin(hold_ids)].copy()
    if train.empty or hold.empty:
        raise RuntimeError("train or hold-out split is empty")
    clf, reg, q10, q90, _ = _fit_pair(train)
    common, class_weight = _hparams(len(train))
    above_raw, above_clf = _residual_head(train, hold, "y_max_dbdt")
    above_excess, excess_clf = _residual_head(train, hold, "y_excess_max")
    # Prefer the spatially detrended head when it exists. It is the temporal residual.
    residual_clf = excess_clf if excess_clf is not None else above_clf
    residual_name = "y_excess_max" if excess_clf is not None else ("y_max_dbdt" if above_clf is not None else None)
    report = {
        "model": "sklearn.HistGradientBoosting",
        "task": "P(mlat/MLT band max |dB/dt| exceeds threshold on [t+30min, t+90min)) plus expected |dB/dt|",
        "capacity": {
            "max_depth": common["max_depth"],
            "max_iter": common["max_iter"],
            "learning_rate": common["learning_rate"],
            "min_samples_leaf": common["min_samples_leaf"],
            "class_weight": class_weight,
            "selection": (
                "Long-table depth and class weight were chosen by 5-fold storm CV "
                "(storms of at least 6 h, interleaved in time, official hold-out not in the folds). "
                "depth 6 / leaf 80 / no class_weight beat depth 3 / balanced on both AUC and Brier "
                "versus band×MLT climatology. Balanced weights raised Brier."
            ),
        },
        "train_storms": train_ids,
        "holdout_storms": hold_ids,
        "dropped_future_storms": dropped,
        "n_train": int(len(train)),
        "n_holdout": int(len(hold)),
        "train": _scores(clf, reg, train),
        "holdout": _scores(clf, reg, hold),
        "holdout_climatology": band_sector_climatology(train, hold),
        "holdout_above_cell_p75": above_raw,
        "holdout_excess_above_cell_p75": above_excess,
        "holdout_with_recent_dbdt": _recent_ablation(train, hold),
        "earlier_storm_sensitivity": _earlier_storm_sensitivity(table, hold_ids),
        "residual_q10": q10,
        "residual_q90": q90,
        "threshold": meta.get("threshold") if meta else None,
        "unit": meta.get("unit") if meta else None,
        "label_source": meta.get("label_source") if meta else None,
        "notes": (meta or {}).get("notes", []),
        "leakage": (
            "Features are trailing L1 only. Recent |dB/dt| used in the ablation ends at the decision "
            "and does not enter [t+30min, t+90min). The along-track median excludes the last 10 days "
            "of a long archive. Hold-out is one whole later storm. Segments after that storm are dropped. "
            "Cell quantiles for the residual head are fit on training rows only."
        ),
        "min_hold_span": None if min_hold_span is None else str(min_hold_span),
        "background_fit_before": (meta or {}).get("background_fit_before"),
    }
    bundle = {
        "classifier": clf,
        "regressor": reg,
        "residual_classifier": residual_clf,
        "residual_label": residual_name,
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


def save_bundle(bundle: dict, path: str, metrics_path: str | None = None) -> None:
    import joblib

    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(bundle, out)
    # Default stays smoke_metrics.json so the two-storm NRCan command is unchanged.
    metrics = Path(metrics_path) if metrics_path else out.with_name("smoke_metrics.json")
    metrics.write_text(json.dumps(bundle["meta"], indent=2))
    print(f"wrote {out}")
    print(f"wrote {metrics}")


def load_bundle(path: str) -> dict:
    import joblib

    return joblib.load(path)
