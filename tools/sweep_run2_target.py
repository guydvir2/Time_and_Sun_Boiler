"""
How much does the run-2 target temperature matter?
For each target, runs the model over the saved days and prints the average
electric energy per day and how many days run 2 is on.

Run from the repo root:
  python3 tools/sweep_run2_target.py [--dir stored_data_rad]   (or --dir stored_data for cloud-only data)
"""
import argparse, glob, os, sys
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT); sys.path.insert(0, os.path.join(ROOT, "tools"))
import boiler_model as bm  # noqa: E402
import backtest_stored as bt  # noqa: E402
import compare_radiation as cr  # noqa: E402

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default="stored_data_rad")
    ap.add_argument("--t-start", type=float, default=35.0)
    a = ap.parse_args()
    files = sorted(glob.glob(os.path.join(ROOT, a.dir, "*.csv")))
    days = []
    for f in files:
        h = cr.load_rad(f)
        if h:
            days.append(h)
    print(f"{len(days)} days from {a.dir}")
    print("run2 target   kWh/day   min/day   days with run2   days with any run")
    for tgt in (None, 65, 60, 55, 50, 45):
        p = bm.ModelParams(latitude=bt.LAT, longitude=bt.LON,
                           heater=bm.HeaterParams(t_target_run2=tgt))
        kwh = mins = r2 = anyr = 0
        for h in days:
            sr, ss = bt.sun_times(h[0].time.date(), p)
            pl = bm.plan_day(sr, h, sr, ss, a.t_start, p)
            kwh += pl.total_kwh; mins += cr.minutes(pl)
            r2 += pl.run2 is not None; anyr += pl.run1 is not None or pl.run2 is not None
        n = len(days)
        print(f"{'max (65)' if tgt in (None, 65) else tgt!s:>10}   {kwh/n:6.2f}   {mins/n:7.0f}   {r2:10d}   {anyr:12d}")

if __name__ == "__main__":
    main()
