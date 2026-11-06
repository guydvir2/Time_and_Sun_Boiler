"""Weather + sun times from Open-Meteo, and the daily CSV cache."""
import os
import csv
import requests
from datetime import datetime, date, timedelta


def get_weather(s, start_hour, end_hour, days_back=0):
    """Hourly temperature and cloud cover for one day, limited to [start_hour, end_hour].
    Returns (rows, ok). Rows are dicts: time, temp, clouds."""
    print(f"🌤️  Fetching weather ({s.latitude}, {s.longitude}) hours {start_hour}-{end_hour}, days_back={days_back}")
    target = date.today() - timedelta(days=days_back)
    now = datetime.now()
    try:
        r = requests.get(s.weather_url, timeout=s.request_timeout, params={
            "latitude": s.latitude, "longitude": s.longitude,
            "hourly": "temperature_2m,cloudcover", "past_days": days_back, "timezone": "auto"})
        r.raise_for_status()
        hourly = r.json()["hourly"]
        rows = []
        for t, temp, clouds in zip(hourly["time"], hourly["temperature_2m"], hourly["cloudcover"]):
            ts = datetime.fromisoformat(t)
            if ts.date() != target or not (start_hour <= ts.hour <= end_hour):
                continue
            if days_back == 0 and ts > now:      # skip future hours when looking at today
                continue
            rows.append({"time": ts, "temp": temp, "clouds": clouds})
        print(f"✅ {len(rows)} weather points")
        return rows, len(rows) > 0
    except Exception as e:
        print(f"❌ Weather fetch failed: {e}")
        return [], False


def get_sun_times(s, days_back=0, default_sunrise=7, default_sunset=17):
    """Sunrise and sunset as whole hours (rounded). Falls back to defaults on any failure."""
    target = date.today() - timedelta(days=days_back)
    try:
        r = requests.get("https://api.open-meteo.com/v1/forecast", timeout=s.request_timeout, params={
            "latitude": s.latitude, "longitude": s.longitude, "daily": "sunrise,sunset",
            "timezone": "auto", "past_days": days_back})
        r.raise_for_status()
        daily = r.json()["daily"]
        for dt, sunrise, sunset in zip(daily["time"], daily["sunrise"], daily["sunset"]):
            if dt == str(target):
                a, b = datetime.fromisoformat(sunrise), datetime.fromisoformat(sunset)
                res = {"sunrise": round(a.hour + a.minute / 60) % 24,
                       "sunset": round(b.hour + b.minute / 60) % 24}
                print(f"🌅 Sunrise {a:%H:%M} → {res['sunrise']}h, sunset {b:%H:%M} → {res['sunset']}h")
                return res
        print(f"⚠️  No sun data for {target}, using defaults")
    except Exception as e:
        print(f"❌ Sun times failed: {e} — using defaults")
    return {"sunrise": default_sunrise, "sunset": default_sunset}


def csv_path(s, days_back):
    return os.path.join(s.stored_dir, f"{date.today() - timedelta(days=days_back)}.csv")


def save_csv(rows, filename):
    if not rows:
        return
    try:
        os.makedirs(os.path.dirname(os.path.abspath(filename)), exist_ok=True)
        with open(filename, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["time", "temp", "clouds"])
            for e in rows:
                w.writerow([e["time"].isoformat(), e["temp"], e["clouds"]])
        print(f"💾 Saved {len(rows)} points to {filename}")
    except OSError as e:
        print(f"❌ Could not save weather CSV: {e}")


def load_csv(filename):
    try:
        with open(filename, newline="") as f:
            rows = [{"time": datetime.fromisoformat(r["time"]), "temp": float(r["temp"]),
                     "clouds": float(r["clouds"])} for r in csv.DictReader(f)]
        print(f"📂 Loaded {len(rows)} points from {filename}")
        return rows, len(rows) > 0
    except FileNotFoundError:
        print(f"❌ File not found: {filename}")
    except Exception as e:
        print(f"❌ Could not load {filename}: {e}")
    return [], False
