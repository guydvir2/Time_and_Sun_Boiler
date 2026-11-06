"""Real sunshine for past days (Open-Meteo archive), to check whether 'cloud cover' is a good stand-in for sun.

sun_pct = measured sunshine duration / daylight duration (0..100). The archive lags ~5 days behind.
Cached in stored_data/sunshine.csv; only missing days are fetched.
"""
import csv
import os
from datetime import date, timedelta
from statistics import mean

import requests

ARCHIVE_LAG_DAYS = 5


def load(s):
    """{date: {'sun_pct', 'sun_h', 'rad_mj'}} from the cache file."""
    out = {}
    try:
        with open(s.sunshine_file, newline="") as f:
            for r in csv.DictReader(f):
                try:
                    out[r["date"]] = {"sun_pct": float(r["sun_pct"]), "sun_h": float(r["sun_h"]), "rad_mj": float(r["rad_mj"])}
                except (KeyError, ValueError):
                    continue
    except (FileNotFoundError, OSError):
        pass
    return out


def _save(s, data):
    os.makedirs(os.path.dirname(s.sunshine_file), exist_ok=True)
    tmp = s.sunshine_file + ".tmp"
    with open(tmp, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date", "sun_pct", "sun_h", "rad_mj"])
        for d in sorted(data):
            v = data[d]
            w.writerow([d, v["sun_pct"], v["sun_h"], v["rad_mj"]])
    os.replace(tmp, s.sunshine_file)


def fetch_missing(s, dates, today=None):
    """Download sunshine for `dates` that are not cached yet. Returns (added_count, message)."""
    today = today or date.today()
    newest_ok = str(today - timedelta(days=ARCHIVE_LAG_DAYS))
    have = load(s)
    want = sorted(d for d in set(dates) if d not in have and d <= newest_ok)
    if not want:
        return 0, "Sunshine data is up to date"
    r = requests.get(s.archive_url, timeout=max(s.request_timeout, 30), params={
        "latitude": s.latitude, "longitude": s.longitude, "start_date": want[0], "end_date": want[-1],
        "daily": "sunshine_duration,daylight_duration,shortwave_radiation_sum", "timezone": "auto"})
    r.raise_for_status()
    j = r.json()["daily"]
    added = 0
    for d, sun, light, rad in zip(j["time"], j["sunshine_duration"], j["daylight_duration"], j["shortwave_radiation_sum"]):
        if d in want and None not in (sun, light, rad) and light > 0:
            have[d] = {"sun_pct": round(100 * sun / light, 1), "sun_h": round(sun / 3600, 2), "rad_mj": round(rad, 2)}
            added += 1
    _save(s, have)
    return added, f"Added sunshine for {added} days"


def _pearson(xs, ys):
    mx, my = mean(xs), mean(ys)
    sx = sum((x - mx) ** 2 for x in xs) ** .5
    sy = sum((y - my) ** 2 for y in ys) ** .5
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / (sx * sy) if sx and sy else 0.0


def verdict(days):
    """How well forecast clouds tracked real sun on the saved days. days = replay rows that have sun_pct."""
    pts = [(r["clouds"], r["sun_pct"]) for r in days if "sun_pct" in r]
    if len(pts) < 10:
        return {"n": len(pts), "level": "grey", "text": "Not enough days with sunshine data yet (need 10)"}
    r = _pearson([p[0] for p in pts], [p[1] for p in pts])
    # expected sun if clouds were perfect: 100 - clouds. Biggest misses = where the formula was misled most.
    misses = sorted(({"date": d["date"], "clouds": d["clouds"], "sun_pct": d["sun_pct"],
                      "gap": round(d["sun_pct"] - (100 - d["clouds"]))} for d in days if "sun_pct" in d),
                    key=lambda m: -abs(m["gap"]))[:3]
    if r <= -0.7:
        level, text = "green", "Clouds track real sunshine well"
    elif r <= -0.4:
        level, text = "yellow", "Clouds track real sunshine only roughly"
    else:
        level, text = "red", "Clouds do NOT track real sunshine here"
    return {"n": len(pts), "r": round(r, 2), "level": level, "text": text, "misses": misses,
            "points": [[c, sp] for c, sp in pts]}
