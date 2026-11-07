"""'Is everything linked and working?' One light per service, for the dashboard.

Checks are read-only (nothing is switched) and run in parallel, so the whole round takes a few seconds.
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from .state import load_heartbeat, load_auto
from .drivers import REGISTRY

LINKS = (("homeassistant", lambda s: s.ha_ip), ("mqtt_tasmota", lambda s: s.mqtt_host))


def engine_row(s, now=None):
    now = now or datetime.now()
    hb = load_heartbeat(s)
    if not hb:
        return {"name": "Engine", "level": "red", "text": "Not running (start main.py)"}
    try:
        age = (now - datetime.fromisoformat(hb)).total_seconds() / 60
    except ValueError:
        return {"name": "Engine", "level": "red", "text": "Unknown"}
    if age <= 2 * s.sleep_minutes + 1:
        return {"name": "Engine", "level": "green", "text": f"Running · last check {hb[11:16]}"}
    return {"name": "Engine", "level": "red", "text": f"Stopped? Last check {hb[5:10]} {hb[11:16]}"}


def auto_row(s):
    a = load_auto(s)
    if a["enabled"]:
        return {"name": "Auto-run", "level": "green", "text": "On · refines hourly, final after sunset"}
    since = f" since {a['since'][5:10]} {a['since'][11:16]}" if a.get("since") else ""
    return {"name": "Auto-run", "level": "yellow", "text": f"Paused{since}"}


def weather_row(forecast):
    if not forecast or forecast.get("error"):
        return {"name": "Weather", "level": "red", "text": (forecast or {}).get("error", "No data")}
    if forecast.get("stale"):
        return {"name": "Weather", "level": "yellow", "text": "Open-Meteo: using older data"}
    return {"name": "Weather", "level": "green", "text": "Open-Meteo OK"}


def _link_rows(s, name):
    try:
        rows = REGISTRY[name](s).check()
    except Exception as e:                             # a check must never break the page
        rows = [{"name": name, "level": "red", "text": f"Check failed ({type(e).__name__})"}]
    if name not in s.driver_names:                     # configured, but not used to switch the boiler
        for r in rows:
            r["text"] += " · spare"
            if r["level"] == "red":
                r["level"] = "grey"
    return rows


def check_all(s, forecast):
    """Engine, auto-run, weather, then every configured link (HA, MQTT + plug), problems first."""
    names = [n for n, configured in LINKS if n in s.driver_names or configured(s)]
    if "dryrun" in s.driver_names:
        names.append("dryrun")
    with ThreadPoolExecutor(max_workers=4) as ex:
        links = [r for rows in ex.map(lambda n: _link_rows(s, n), names) for r in rows]
    rows = [engine_row(s), auto_row(s), weather_row(forecast)] + links
    order = {"red": 0, "yellow": 1, "grey": 2, "green": 3}
    return sorted(rows, key=lambda r: order.get(r["level"], 9))
