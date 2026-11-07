"""
Calibrate the physical model to the current LUT (ground truth for now).

Grid-searches the model's free settings so its daily heating minutes
(run 1 + run 2, capped at 180 like the LUT) match the LUT minutes
over the saved days in stored_data/.

Run from the repo root:  python3 tools/calibrate_lut.py
"""

import itertools
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tools"))

import boiler_model as bm  # noqa: E402
import backtest_stored as bt  # noqa: E402
import glob  # noqa: E402

CAP = 180
GRID = {
    "k": [4, 5, 6, 7, 8, 9, 10, 11, 12],
    "t_start": [30, 35, 40, 45, 50, 55],
    "t_min_use": [35, 40, 45, 50, 55],
    "refill": [0.2, 0.3, 0.4, 0.5],
}


def load_days():
    p = bm.ModelParams(latitude=bt.LAT, longitude=bt.LON)
    days = []
    for path in sorted(glob.glob(os.path.join(ROOT, "stored_data", "*.csv"))):
        hours = bt.load(path)
        if not hours:
            continue
        sunrise, sunset = bt.sun_times(hours[0].time.date(), p)
        lut = bt.old_minutes(hours, sunrise, sunset)
        if lut is not None:
            days.append((hours, sunrise, sunset, lut))
    return days


def model_minutes(day, k, t_start, t_min, refill):
    hours, sunrise, sunset, _ = day
    p = bm.ModelParams(latitude=bt.LAT, longitude=bt.LON, k=k,
                       heater=bm.HeaterParams(t_min_use=t_min),
                       schedule=bm.ScheduleParams(refill_fraction=refill, t_amb=hours[-1].temp))
    plan = bm.plan_day(sunrise, hours, sunrise, sunset, t_start, p)
    m = (plan.run1.minutes if plan.run1 else 0) + (plan.run2.minutes if plan.run2 else 0)
    return min(m, CAP), plan


def score(days, combo):
    errs = []
    for d in days:
        m, _ = model_minutes(d, *combo)
        errs.append(m - d[3])
    mae = sum(abs(e) for e in errs) / len(errs)
    bias = sum(errs) / len(errs)
    return mae, bias


def main():
    days = load_days()
    print(f"{len(days)} days with LUT minutes")
    results = []
    for combo in itertools.product(*GRID.values()):
        mae, bias = score(days, combo)
        results.append((mae, bias, combo))
    results.sort(key=lambda r: r[0])

    print("\nTop 8 fits (k, t_start, t_min_use, refill):")
    print(" MAE min  bias min   k  t_start  t_min  refill")
    for mae, bias, c in results[:8]:
        print(f"  {mae:6.1f}   {bias:+6.1f}  {c[0]:3d}  {c[1]:6d}  {c[2]:5d}   {c[3]:.0%}")

    best = results[0][2]
    print(f"\nBest fit day by day: k={best[0]}, t_start={best[1]}, t_min_use={best[2]}, refill={best[3]:.0%}")
    print("date        clouds  temp  dusk   LUT  model  diff")
    big = 0
    for d in days:
        m, plan = model_minutes(d, *best)
        hours = d[0]
        cc = sum(h.cloud_cover for h in hours) / len(hours)
        tm = sum(h.temp for h in hours) / len(hours)
        diff = m - d[3]
        big += abs(diff) > 45
        print(f"{hours[0].time.date()}  {cc:5.0f}%  {tm:4.1f}  {plan.t_end_solar:4.1f}  {d[3]:4d}  {m:5d}  {diff:+5d}")
    print(f"\nDays off by more than 45 min: {big} of {len(days)}")


if __name__ == "__main__":
    main()
