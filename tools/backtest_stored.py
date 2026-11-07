"""
Backtest: run the new model over the saved daily CSVs in stored_data/
and compare with the current app's minutes (boiler_duration LUT).

Saved files hold daylight hours only, with temperature and cloud cover
(no radiation), so solar comes from the cloud-cover estimate and the
refill water temperature uses the last saved hour.

Run from the repo root:  python3 tools/backtest_stored.py [--t-start 35]
"""

import argparse
import csv
import glob
import os
import sys
from datetime import datetime, timedelta

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import boiler_model as bm  # noqa: E402

LAT, LON = 32.72, 35.13


def old_minutes(hours, sunrise, sunset):
    """Current app logic: mean T and CC in [sunrise+2, sunset-1] -> LUT minutes."""
    import contextlib
    import io
    os.chdir(ROOT)
    with contextlib.redirect_stdout(io.StringIO()):
        import main as old
        w = [{"time": h.time, "temp": h.temp, "clouds": h.cloud_cover} for h in hours]
        t, cc = old.daily_mean_T_CC(w, sunrise.hour, sunset.hour)
        return None if t is None else old.boiler_duration(t, cc)


def sun_times(day, p):
    """Sunrise / sunset from the model's solar elevation, 1-minute search."""
    start = datetime.combine(day, datetime.min.time())
    mins = [start + timedelta(minutes=m) for m in range(24 * 60)]
    up = [m for m in mins if bm.solar_elevation_deg(m, LAT, LON, p.utc_offset_hours) > -0.83]
    return up[0], up[-1]


def load(path):
    out = []
    with open(path) as f:
        for row in csv.DictReader(f):
            out.append(bm.WeatherHour(time=datetime.fromisoformat(row["time"]),
                                      temp=float(row["temp"]), cloud_cover=float(row["clouds"])))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--t-start", type=float, default=35.0)
    args = ap.parse_args()

    p = bm.ModelParams(latitude=LAT, longitude=LON)
    rows = []
    for path in sorted(glob.glob(os.path.join(ROOT, "stored_data", "*.csv"))):
        hours = load(path)
        if not hours:
            continue
        day = hours[0].time.date()
        sunrise, sunset = sun_times(day, p)
        p.schedule.t_amb = hours[-1].temp          # last saved hour as refill proxy
        plan = bm.plan_day(sunrise, hours, sunrise, sunset, args.t_start, p)
        old = old_minutes(hours, sunrise, sunset)
        new_min = (plan.run1.minutes if plan.run1 else 0) + (plan.run2.minutes if plan.run2 else 0)
        cc = sum(h.cloud_cover for h in hours) / len(hours)
        rows.append((day, cc, plan.t_end_solar, plan.run1.minutes if plan.run1 else 0,
                     plan.run2.minutes if plan.run2 else 0, new_min, old, plan.total_kwh))

    print("date        clouds  dusk  run1  run2  new_min  old_min  kWh")
    for r in rows:
        print(f"{r[0]}  {r[1]:5.0f}%  {r[2]:4.1f}  {r[3]:4d}  {r[4]:4d}  {r[5]:7d}  {r[6]!s:>7}  {r[7]:4.1f}")
    n = len(rows)
    print(f"\n{n} days | mean kWh {sum(r[7] for r in rows) / n:.1f} | "
          f"mean new min {sum(r[5] for r in rows) / n:.0f} | "
          f"mean old min {sum(r[6] or 0 for r in rows) / n:.0f}")


if __name__ == "__main__":
    main()
