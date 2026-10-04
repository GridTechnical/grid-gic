"""Whole-storm ids. Minutes from one storm never land on both sides of a split."""
from __future__ import annotations

import pandas as pd

from forecast.constants import STORM_BREAK, STORM_BZ_NT, STORM_GAP_CLOSE, STORM_PDYN_NPA, STORM_SPEED_KMS


def active_minutes(l1: pd.DataFrame) -> pd.Series:
    bz = l1["bz_gsm"]
    speed = l1["speed"]
    pdyn = l1["pdyn_npa"]
    active = (bz < STORM_BZ_NT) | ((speed > STORM_SPEED_KMS) & (speed < 2500)) | (pdyn > STORM_PDYN_NPA)
    return active.fillna(False)


def _close_short_gaps(active: pd.Series, max_gap: pd.Timedelta) -> pd.Series:
    """Fill inactive runs that sit between active samples and are shorter than max_gap."""
    vals = active.to_numpy(dtype=bool).copy()
    times = active.index
    n = len(vals)
    i = 0
    while i < n:
        if vals[i]:
            i += 1
            continue
        j = i
        while j < n and not vals[j]:
            j += 1
        if i > 0 and j < n:
            span = times[j] - times[i - 1]
            hole = times[min(j, n - 1)] - times[i]
            # Do not bridge a download gap. Only close a quiet lull inside continuous data.
            if span <= max_gap and (j == i or hole <= max_gap):
                vals[i:j] = True
        i = j if j > i else i + 1
    return pd.Series(vals, index=active.index)


def assign_storm_ids(l1: pd.DataFrame) -> pd.DataFrame:
    """Return a frame indexed like l1 with storm_id and storm_kind.

    A hole longer than STORM_BREAK starts a new segment, so two downloaded
    windows do not become one storm.
    """
    if l1.empty:
        return pd.DataFrame(columns=["storm_id", "storm_kind"], index=l1.index)
    active = _close_short_gaps(active_minutes(l1), pd.Timedelta(STORM_GAP_CLOSE))
    break_after = pd.Timedelta(STORM_BREAK)
    ids: list[str] = []
    kinds: list[str] = []
    current: str | None = None
    current_kind: str | None = None
    prev = None
    for t, flag in active.items():
        if prev is not None and (t - prev) > break_after:
            current = None
        kind = "storm" if bool(flag) else "quiet"
        if current is None or current_kind != kind:
            stamp = pd.Timestamp(t).strftime("%Y%m%dT%H%MZ")
            current = f"{kind}_{stamp}"
            current_kind = kind
        ids.append(current)
        kinds.append(kind)
        prev = t
    return pd.DataFrame({"storm_id": ids, "storm_kind": kinds}, index=l1.index)


def train_holdout_ids(storms: pd.DataFrame) -> tuple[list[str], list[str], list[str]]:
    """Earlier segments train. The latest active storm is the hold-out.

    Segments that start after the hold-out are dropped so training does not
    see the future of the hold-out storm. Returns (train_ids, holdout_ids, dropped_ids).
    """
    if storms.empty:
        raise ValueError("no storms")
    tmp = storms.copy()
    tmp["t"] = tmp.index
    first = tmp.groupby("storm_id")["t"].min().sort_values()
    kinds = tmp.groupby("storm_id")["storm_kind"].first()
    active_ids = [i for i in first.index if kinds.loc[i] == "storm"]
    if len(active_ids) >= 1:
        # Latest active storm by its start time.
        hold = max(active_ids, key=lambda i: first.loc[i])
    else:
        if len(first) < 2:
            raise ValueError("need at least two segments, or one active storm plus other data")
        hold = first.index[-1]
    hold_start = first.loc[hold]
    train = [i for i in first.index if first.loc[i] < hold_start]
    dropped = [i for i in first.index if first.loc[i] > hold_start]
    if not train:
        raise ValueError(
            f"hold-out storm {hold} is the earliest segment; fetch an earlier window so train is non-empty"
        )
    return train, [hold], dropped
