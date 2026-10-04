"""Physics and leakage checks. No network."""
from __future__ import annotations

import unittest

import numpy as np
import pandas as pd

from forecast.features import add_trailing_features, clean_l1
from forecast.labels import along_track_excess, band_minute_max, labels_for_decisions
from forecast.physics import (
    cone_angle_rad,
    dynamic_pressure_npa,
    epsilon_proxy,
    half_wave_coupling,
    magnetic_latitude_deg,
    magnetic_local_time_hours,
)
from forecast.storms import assign_storm_ids, train_holdout_ids


def _minutes(start: str, periods: int, **cols) -> pd.DataFrame:
    idx = pd.date_range(start, periods=periods, freq="1min", tz="UTC")
    data = {k: np.full(periods, v) if np.isscalar(v) else v for k, v in cols.items()}
    return pd.DataFrame(data, index=idx)


class PhysicsTests(unittest.TestCase):
    def test_pdyn_missing_is_not_zero(self):
        n = pd.Series([4.0, np.nan, 4.0, 0.0, 999.99])
        v = pd.Series([400.0, 400.0, np.nan, 400.0, 99999.9])
        p = dynamic_pressure_npa(n, v)
        self.assertTrue(np.isfinite(p.iloc[0]))
        self.assertGreater(p.iloc[0], 0)
        for i in (1, 2, 3, 4):
            self.assertTrue(np.isnan(p.iloc[i]), i)
            self.assertNotEqual(p.iloc[i], 0)

    def test_clean_l1_masks_omni_fills(self):
        raw = _minutes(
            "2025-10-11T00:00:00Z",
            3,
            bx_gsm=[1.0, 9999.99, 1.0],
            by_gsm=[1.0, 1.0, 1.0],
            bz_gsm=[-2.0, -2.0, 9999.99],
            bt=[3.0, 3.0, 3.0],
            speed=[400.0, 99999.9, 400.0],
            density=[5.0, 5.0, 999.99],
            temperature=[1e5, 1e5, 1e5],
            pdyn_omni=[1.0, 1.0, 1.0],
        )
        clean = clean_l1(raw)
        self.assertTrue(np.isnan(clean["speed"].iloc[1]))
        self.assertTrue(np.isnan(clean["pdyn_npa"].iloc[1]))
        self.assertTrue(np.isnan(clean["pdyn_npa"].iloc[2]))
        self.assertTrue(np.isfinite(clean["pdyn_npa"].iloc[0]))
        self.assertFalse((clean["pdyn_npa"].fillna(1) == 0).any())

    def test_trailing_features_ignore_the_future(self):
        raw = _minutes(
            "2025-10-11T00:00:00Z",
            180,
            bx_gsm=1.0,
            by_gsm=2.0,
            bz_gsm=-1.0,
            bt=3.0,
            speed=400.0,
            density=5.0,
        )
        a = add_trailing_features(raw)
        future = raw.copy()
        future.iloc[-1, future.columns.get_loc("bz_gsm")] = -80.0
        future.iloc[-1, future.columns.get_loc("speed")] = 900.0
        b = add_trailing_features(future)
        t = a.index[60]
        pd.testing.assert_series_equal(a.loc[t], b.loc[t])
        # The last row is allowed to see that new sample.
        self.assertLess(b.iloc[-1]["bz_min_60"], a.iloc[-1]["bz_min_60"])
        self.assertLess(b.iloc[-1]["bz_min_15"], a.iloc[-1]["bz_min_15"])

    def test_label_window_is_delayed(self):
        t0 = pd.Timestamp("2025-10-11T12:00:00Z")
        times = [t0 + pd.Timedelta(minutes=m) for m in (0, 10, 45, 89, 90, 120)]
        samples = pd.DataFrame(
            {
                "time": times,
                "lat": [65.0] * len(times),
                "lon": [-100.0] * len(times),
                "dbdt_nts": [9.0, 8.0, 0.2, 0.4, 7.0, 6.0],
            }
        )
        minutes = band_minute_max(samples, "dbdt_nts")
        decisions = pd.DatetimeIndex([t0])
        labels = labels_for_decisions(minutes, decisions, threshold=0.05)
        self.assertEqual(len(labels), 1)
        # Only +45 and +89 are inside [t+30, t+90). Max is 0.4, not the 9 at t or 7 at +90.
        self.assertAlmostEqual(labels.iloc[0]["y_max_dbdt"], 0.4)
        self.assertGreater(labels.iloc[0]["time"] + pd.Timedelta(minutes=30), labels.iloc[0]["time"])

    def test_storms_stay_whole(self):
        idx = pd.date_range("2025-10-10", periods=60 * 20, freq="1min", tz="UTC")
        bz = np.zeros(len(idx))
        # Two active blocks separated by 5 quiet hours: must not merge (gap close is 3 h).
        bz[0:240] = -8
        bz[240 + 300 : 240 + 300 + 240] = -8
        df = pd.DataFrame(
            {"bz_gsm": bz, "speed": 350.0, "pdyn_npa": 1.0},
            index=idx,
        )
        storms = assign_storm_ids(df)
        active = storms[storms["storm_kind"] == "storm"]["storm_id"].unique()
        self.assertEqual(len(active), 2)
        train, hold, dropped = train_holdout_ids(storms)
        self.assertEqual(len(hold), 1)
        self.assertNotIn(hold[0], train)
        # Every minute of the hold-out id is on one side only.
        self.assertTrue(set(train).isdisjoint(hold))
        hold_times = storms.index[storms["storm_id"].isin(hold)]
        train_times = storms.index[storms["storm_id"].isin(train)]
        self.assertLess(train_times.max(), hold_times.min())

    def test_short_tail_is_not_the_holdout_when_a_minimum_span_is_set(self):
        idx = pd.date_range("2025-07-01", periods=60 * 30, freq="1min", tz="UTC")
        bz = np.zeros(len(idx))
        # 4 h storm, 6 h quiet, 4 h storm, 6 h quiet, 20 min blip.
        bz[0:240] = -8
        bz[240 + 360 : 240 + 360 + 240] = -8
        bz[-20:] = -8
        df = pd.DataFrame({"bz_gsm": bz, "speed": 350.0, "pdyn_npa": 1.0}, index=idx)
        storms = assign_storm_ids(df)
        train, hold, dropped = train_holdout_ids(storms, min_hold_span=pd.Timedelta(hours=2))
        self.assertEqual(len(hold), 1)
        hold_times = storms.index[storms["storm_id"].isin(hold)]
        self.assertGreaterEqual(hold_times.max() - hold_times.min(), pd.Timedelta(hours=2))
        # The 20 min tail starts after the hold-out and is dropped, not trained.
        tail_id = storms.iloc[-1]["storm_id"]
        self.assertIn(tail_id, dropped)
        self.assertNotIn(tail_id, train)
        self.assertNotIn(tail_id, hold)
        train_times = storms.index[storms["storm_id"].isin(train)]
        self.assertLess(train_times.max(), hold_times.min())

    def test_ottawa_is_northern_not_equatorial(self):
        mlat = magnetic_latitude_deg([45.4], [-75.55])[0]
        self.assertGreater(mlat, 50.0)
        self.assertLess(mlat, 65.0)
        when = pd.date_range("2025-10-11", periods=4, freq="6h", tz="UTC")
        mlt = magnetic_local_time_hours(np.full(4, 45.4), np.full(4, -75.55), when)
        self.assertEqual(len(mlt), 4)
        self.assertTrue(np.all((mlt >= 0) & (mlt < 24)))

    def test_coupling_proxies_stay_missing(self):
        speed = pd.Series([400.0, np.nan, 400.0])
        bt = pd.Series([5.0, 5.0, np.nan])
        clock = pd.Series([np.pi, np.pi, np.pi])
        bx = pd.Series([0.0, 1.0, 1.0])
        bz = pd.Series([2.0, -4.0, np.nan])
        eps = epsilon_proxy(speed, bt, clock)
        self.assertTrue(np.isfinite(eps.iloc[0]))
        self.assertGreater(eps.iloc[0], 0)
        self.assertTrue(np.isnan(eps.iloc[1]))
        self.assertTrue(np.isnan(eps.iloc[2]))
        cone = cone_angle_rad(bx, bt)
        self.assertAlmostEqual(cone.iloc[0], np.pi / 2)
        self.assertTrue(np.isnan(cone.iloc[2]))
        hw = half_wave_coupling(speed, bz)
        # Northward Bz is a real zero, not a fill. Missing speed or Bz stays missing.
        self.assertEqual(hw.iloc[0], 0.0)
        self.assertTrue(np.isnan(hw.iloc[1]))
        self.assertTrue(np.isnan(hw.iloc[2]))

    def test_recent_dbdt_does_not_enter_the_label_hour(self):
        t0 = pd.Timestamp("2025-10-11T12:00:00Z")
        samples = pd.DataFrame(
            {
                "time": [
                    t0 - pd.Timedelta(minutes=10),
                    t0 + pd.Timedelta(minutes=20),
                    t0 + pd.Timedelta(minutes=50),
                ],
                "lat": [65.0, 65.0, 65.0],
                "lon": [-100.0, -100.0, -100.0],
                "dbdt_nts": [1.5, 9.0, 0.3],
            }
        )
        minutes = band_minute_max(samples, "dbdt_nts")
        labels = labels_for_decisions(minutes, pd.DatetimeIndex([t0]), threshold=0.05)
        self.assertEqual(len(labels), 1)
        # +50 is inside [t+30, t+90). +20 is in the gap. -10 is the recent window.
        self.assertAlmostEqual(labels.iloc[0]["y_max_dbdt"], 0.3)
        self.assertAlmostEqual(labels.iloc[0]["recent_dbdt_max_60"], 1.5)

    def test_along_track_excess_removes_the_bin_median(self):
        t0 = pd.Timestamp("2025-07-01T00:00:00Z")
        times = [t0 + pd.Timedelta(minutes=i) for i in range(40)]
        quiet = pd.DataFrame(
            {
                "time": times,
                "lat": [65.0] * 40,
                "lon": [-100.0] * 40,
                "dbdt_uts": [0.04] * 40,
            }
        )
        spike = quiet.iloc[[10]].copy()
        spike["dbdt_uts"] = 0.20
        samples = pd.concat([quiet.iloc[:10], spike, quiet.iloc[11:]], ignore_index=True)
        # A later hold-out-sized spike must not move the fit median.
        later = quiet.iloc[[0]].copy()
        later["time"] = t0 + pd.Timedelta(days=30)
        later["dbdt_uts"] = 0.90
        samples = pd.concat([samples, later], ignore_index=True)
        out = along_track_excess(samples, "dbdt_uts", fit_before=t0 + pd.Timedelta(days=20), min_bin=10)
        early = out[out["time"] < t0 + pd.Timedelta(days=1)]
        self.assertAlmostEqual(float(early["dbdt_excess"].median()), 0.0, places=6)
        self.assertGreater(float(early["dbdt_excess"].max()), 0.1)
        held = out[out["time"] >= t0 + pd.Timedelta(days=20)]
        # Background stayed at 0.04, so the held-out 0.90 is excess 0.86, not absorbed into the median.
        self.assertGreater(float(held["dbdt_excess"].iloc[0]), 0.8)


if __name__ == "__main__":
    unittest.main()
