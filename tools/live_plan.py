"""
Live test: fetch today's Open-Meteo forecast and print the boiler day plan.
Nothing is sent to Home Assistant.

Run from the repo root:  python3 tools/live_plan.py [--t-start 35] [--model A|B] [--vacation]
"""

import argparse
import configparser
import os
import sys
from datetime import datetime, timedelta

import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import boiler_model as bm  # noqa: E402

URL = "https://api.open-meteo.com/v1/forecast"


def load_location():
    cfg = configparser.ConfigParser()
    cfg.read(os.path.join(ROOT, "config.ini"))
    if cfg.has_section("location"):
        return float(cfg["location"]["latitude"]), float(cfg["location"]["longitude"])
    sys.exit("config.ini with a [location] section is required")


def fetch(lat, lon, tilt, azimuth):
    r = requests.get(URL, timeout=20, params={
        "latitude": lat, "longitude": lon,
        "hourly": "temperature_2m,cloud_cover,shortwave_radiation,global_tilted_irradiance",
        "daily": "sunrise,sunset",
        "tilt": tilt, "azimuth": azimuth,
        "timezone": "auto", "forecast_days": 2,
    })
    r.raise_for_status()
    return r.json()


def to_hours(data, day):
    h = data["hourly"]
    out = []
    for i, t in enumerate(h["time"]):
        ts = datetime.fromisoformat(t)
        if ts.date() != day:
            continue
        out.append(bm.WeatherHour(
            time=ts,
            temp=h["temperature_2m"][i],
            cloud_cover=h["cloud_cover"][i],
            ghi=h["shortwave_radiation"][i],
            gti=h["global_tilted_irradiance"][i],
        ))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--t-start", type=float, default=35.0, help="tank temperature at dawn, C")
    ap.add_argument("--model", default="A", choices=["A", "B"])
    ap.add_argument("--vacation", action="store_true")
    args = ap.parse_args()

    lat, lon = load_location()
    p = bm.ModelParams(latitude=lat, longitude=lon, model=args.model,
                       schedule=bm.ScheduleParams(vacation=args.vacation))
    data = fetch(lat, lon, p.collector.tilt_deg, p.collector.azimuth_deg)
    p.utc_offset_hours = data.get("utc_offset_seconds", 7200) / 3600.0

    today = datetime.now().date()
    daily = data["daily"]
    idx = daily["time"].index(today.isoformat())
    sunrise = datetime.fromisoformat(daily["sunrise"][idx])
    sunset = datetime.fromisoformat(daily["sunset"][idx])
    hours = to_hours(data, today)

    h_day = sum(h.gti or 0 for h in hours) / 1000.0
    plan = bm.plan_day(datetime.now(), hours, sunrise, sunset, args.t_start, p)

    print(f"Location {lat:.2f}, {lon:.2f} | {today} | sun {sunrise:%H:%M}-{sunset:%H:%M} "
          f"| radiation on panel {h_day:.2f} kWh/m2")
    for line in bm.plan_summary(plan):
        print(line)
    print(f"publish to HA: {bm.should_publish(plan)} | replan due now: {bm.replan_due(datetime.now(), plan)}")


if __name__ == "__main__":
    main()
