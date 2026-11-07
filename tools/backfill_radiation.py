"""
Backfill real (reanalysis) radiation for the dates in stored_data/.

Fetches hourly temperature, cloud cover, horizontal radiation (GHI) and
radiation on the panel plane (GTI, tilt 50 deg, facing south) from the
Open-Meteo historical archive, and writes one CSV per day to
stored_data_rad/YYYY-MM-DD.csv  (columns: time,temp,clouds,ghi,gti).

Run on a machine that can reach Open-Meteo, from the repo root:
    python3 tools/backfill_radiation.py [--log daily_log.csv] [--tilt 50] [--azimuth 0]
"""

import argparse
import configparser
import csv
import glob
import os
from datetime import date

import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ARCHIVE = "https://archive-api.open-meteo.com/v1/archive"
OUT_DIR = os.path.join(ROOT, "stored_data_rad")


def location():
    cfg = configparser.ConfigParser()
    cfg.read(os.path.join(ROOT, "config.ini"))
    if cfg.has_section("location"):
        return float(cfg["location"]["latitude"]), float(cfg["location"]["longitude"])
    return 32.72, 35.13


def log_dates(path):
    with open(path) as f:
        return sorted(date.fromisoformat(r["date"]) for r in csv.DictReader(f))


def stored_dates():
    names = glob.glob(os.path.join(ROOT, "stored_data", "*.csv"))
    return sorted(date.fromisoformat(os.path.basename(n)[:10]) for n in names)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tilt", type=float, default=50.0)
    ap.add_argument("--azimuth", type=float, default=0.0)
    ap.add_argument("--log", default=None, help="app daily_log.csv: take the dates from it")
    args = ap.parse_args()

    days = log_dates(args.log) if args.log else stored_dates()
    if not days:
        raise SystemExit("no files in stored_data/")
    lat, lon = location()
    r = requests.get(ARCHIVE, timeout=60, params={
        "latitude": lat, "longitude": lon,
        "start_date": days[0].isoformat(), "end_date": days[-1].isoformat(),
        "hourly": "temperature_2m,cloud_cover,shortwave_radiation,global_tilted_irradiance",
        "tilt": args.tilt, "azimuth": args.azimuth,
        "timezone": "auto",
    })
    r.raise_for_status()
    h = r.json()["hourly"]

    wanted = {d.isoformat() for d in days}
    by_day = {}
    for i, t in enumerate(h["time"]):
        d = t[:10]
        if d in wanted:
            by_day.setdefault(d, []).append(
                (t, h["temperature_2m"][i], h["cloud_cover"][i],
                 h["shortwave_radiation"][i], h["global_tilted_irradiance"][i]))

    os.makedirs(OUT_DIR, exist_ok=True)
    for d, rows in by_day.items():
        with open(os.path.join(OUT_DIR, f"{d}.csv"), "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["time", "temp", "clouds", "ghi", "gti"])
            w.writerows(rows)
    print(f"wrote {len(by_day)} days to {OUT_DIR} ({lat:.2f}, {lon:.2f}, tilt {args.tilt}, azimuth {args.azimuth})")


if __name__ == "__main__":
    main()
