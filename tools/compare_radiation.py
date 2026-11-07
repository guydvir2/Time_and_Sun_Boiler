"""
Compare the cloud-cover radiation estimate with real radiation, day by day.

Needs stored_data_rad/ from tools/backfill_radiation.py.
For each day prints the daily radiation on the panel (kWh/m2) from:
  - real GTI (archive)
  - real GHI tilted by the model
  - cloud cover only (clear-sky x Kasten-Czeplak, tilted)
and the dusk temperature and heating minutes the model gives for each,
with the LUT minutes as a sanity check.

Run from the repo root:  python3 tools/compare_radiation.py [--t-start 35] [--plot out.png]
"""

import argparse
import csv
import glob
import os
import sys
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tools"))

import boiler_model as bm  # noqa: E402
import backtest_stored as bt  # noqa: E402

RAD_DIR = os.path.join(ROOT, "stored_data_rad")


def load_rad(path):
    out = []
    with open(path) as f:
        for row in csv.DictReader(f):
            def num(k):
                v = row.get(k)
                return None if v in (None, "", "None") else float(v)
            out.append(bm.WeatherHour(time=datetime.fromisoformat(row["time"]), temp=num("temp"),
                                      cloud_cover=num("clouds"), ghi=num("ghi"), gti=num("gti")))
    return out


def variant(hours, kind):
    if kind == "gti":
        return hours
    if kind == "ghi":
        return [bm.WeatherHour(h.time, h.temp, h.cloud_cover, h.ghi, None) for h in hours]
    return [bm.WeatherHour(h.time, h.temp, h.cloud_cover, None, None) for h in hours]


def daily_plane_kwh(hours, sunrise, sunset, p):
    tot = 0.0
    for h in hours:
        if h.time + bm.timedelta(hours=1) > sunrise and h.time < sunset:
            g = bm.plane_irradiance(h, p)
            tot += (g or 0.0) * bm._fraction_of_hour_in_daylight(h, sunrise, sunset)
    return tot / 1000.0


def minutes(plan):
    return (plan.run1.minutes if plan.run1 else 0) + (plan.run2.minutes if plan.run2 else 0)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--t-start", type=float, default=35.0)
    ap.add_argument("--plot", default=None, help="save a PNG chart to this path")
    args = ap.parse_args()

    files = sorted(glob.glob(os.path.join(RAD_DIR, "*.csv")))
    if not files:
        raise SystemExit("no data in stored_data_rad/ - run tools/backfill_radiation.py first")

    p = bm.ModelParams(latitude=bt.LAT, longitude=bt.LON)
    rows = []
    for path in files:
        hours = load_rad(path)
        day = hours[0].time.date()
        sunrise, sunset = bt.sun_times(day, p)
        r = {"day": day}
        for kind in ("gti", "ghi", "clouds"):
            hv = variant(hours, kind)
            plan = bm.plan_day(sunrise, hv, sunrise, sunset, args.t_start, p)
            r[kind] = (daily_plane_kwh(hv, sunrise, sunset, p), plan.t_end_solar, minutes(plan))
        lut = bt.old_minutes(hours, sunrise, sunset)
        r["lut"] = lut
        rows.append(r)

    print("date        panel kWh/m2: real  tiltedGHI  clouds | dusk C: real clouds | minutes: real clouds LUT")
    for r in rows:
        g, h, c = r["gti"], r["ghi"], r["clouds"]
        print(f"{r['day']}        {g[0]:5.2f}   {h[0]:6.2f}   {c[0]:6.2f} |  {g[1]:5.1f} {c[1]:5.1f} |"
              f"  {g[2]:4d} {c[2]:4d} {r['lut']!s:>4}")

    n = len(rows)
    ratio = [r["clouds"][0] / r["gti"][0] for r in rows if r["gti"][0] > 0.2]
    tilt_ratio = [r["ghi"][0] / r["gti"][0] for r in rows if r["gti"][0] > 0.2]
    mae_cloud = sum(abs(r["clouds"][2] - r["gti"][2]) for r in rows) / n
    mae_lut = sum(abs(r["gti"][2] - (r["lut"] or 0)) for r in rows) / n
    print(f"\n{n} days")
    print(f"cloud estimate / real panel radiation: mean {sum(ratio) / len(ratio):.2f} "
          f"(min {min(ratio):.2f}, max {max(ratio):.2f})")
    print(f"model tilt of GHI / real GTI: mean {sum(tilt_ratio) / len(tilt_ratio):.2f}")
    print(f"minutes, clouds vs real radiation: mean diff {mae_cloud:.0f} min/day")
    print(f"minutes, real radiation vs LUT (sanity): mean diff {mae_lut:.0f} min/day")

    if args.plot:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        x = range(n)
        lab = [r["day"].strftime("%d %b") for r in rows]
        fig, (a1, a2) = plt.subplots(2, 1, figsize=(16, 8), sharex=True)
        a1.plot(x, [r["gti"][0] for r in rows], "-o", ms=3, color="#BA7517", label="real (GTI)")
        a1.plot(x, [r["clouds"][0] for r in rows], "-o", ms=3, color="#888780", label="from clouds")
        a1.set_ylabel("Sun on panel, kWh/m2/day"); a1.legend(); a1.grid(alpha=.3)
        a2.plot(x, [r["lut"] or 0 for r in rows], "-o", ms=3, color="#E24B4A", label="LUT (sanity)")
        a2.plot(x, [r["gti"][2] for r in rows], "-o", ms=3, color="#378ADD", label="model, real radiation")
        a2.plot(x, [r["clouds"][2] for r in rows], "--", color="#378ADD", alpha=.5, label="model, clouds only")
        a2.set_ylabel("Heating minutes"); a2.legend(); a2.grid(alpha=.3)
        a2.set_xticks(list(x)[::3]); a2.set_xticklabels(lab[::3], rotation=45, ha="right", fontsize=8)
        plt.tight_layout(); plt.savefig(args.plot, dpi=130)
        print(f"chart saved to {args.plot}")


if __name__ == "__main__":
    main()
