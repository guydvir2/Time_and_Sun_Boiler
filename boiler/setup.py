"""Settings that the web UI may read and change (Settings tab + first-run wizard).

Only the keys listed in FIELDS can be written, each with its own check. Changes go to settings.json
(git-ignored), which overrides config.ini / config.local.ini. Secret values are never sent back to the
page: it only learns whether a secret is set.
"""
import json
import os
import re

import requests

from .drivers import REGISTRY

HHMM = re.compile(r"^([01]\d|2[0-3]):[0-5]\d$")


def _str(maxlen, pattern=None, allow_empty=True):
    def check(v):
        if not isinstance(v, str) or len(v) > maxlen or (not allow_empty and not v.strip()):
            raise ValueError(f"text up to {maxlen} characters")
        v = v.strip()
        if v and pattern and not re.match(pattern, v):
            raise ValueError("invalid format")
        return v
    return check


def _num(lo, hi, integer=False):
    def check(v):
        if isinstance(v, bool) or not isinstance(v, (int, float)) or not lo <= v <= hi or (integer and int(v) != v):
            raise ValueError(f"number from {lo} to {hi}")
        return int(v) if integer else float(v)
    return check


def _hhmm(v):
    if not isinstance(v, str) or not HHMM.match(v):
        raise ValueError("time as HH:MM")
    return v


def _drivers(v):
    if not isinstance(v, list) or not 1 <= len(v) <= 3 or len(set(v)) != len(v) or any(x not in REGISTRY for x in v):
        raise ValueError(f"1 to 3 of: {', '.join(REGISTRY)}")
    return ",".join(v)


def _full_topic(v):
    v = _str(100)(v)
    if "%prefix%" not in v or "%topic%" not in v:
        raise ValueError("must contain %prefix% and %topic%")
    return v


def _password(v):
    if not isinstance(v, str) or not 6 <= len(v) <= 128:
        raise ValueError("6 to 128 characters")
    return v


HOST = r"^[A-Za-z0-9._:-]+$"
ENTITY = r"^[A-Za-z0-9_.]+$"
# ui key: (ini section, ini key, check, attribute on Settings, secret?)
FIELDS = {
    "location.name":      ("location", "name", _str(80), "place_name", False),
    "location.latitude":  ("location", "latitude", _num(-90, 90), "latitude", False),
    "location.longitude": ("location", "longitude", _num(-180, 180), "longitude", False),
    "min_run":            ("parameters", "MIN_RUN", _num(0, 60, True), "min_run", False),
    "energy.boiler_kw":   ("energy", "boiler_kw", _num(0.1, 20), "boiler_kw", False),
    "energy.price_kwh":   ("energy", "price_kwh", _num(0, 10), "price_kwh", False),
    "run1_pct":           ("parameters", "RUN1_PERCENT", _num(10, 100, True), "run1_pct", False),
    "run2_at":            ("parameters", "RUN2_AT", lambda v: v if v == "" else _hhmm(v), "run2_at", False),
    "drivers":            ("output", "drivers", _drivers, "driver_names", False),
    "mqtt.host":          ("mqtt", "host", _str(100, HOST), "mqtt_host", False),
    "mqtt.port":          ("mqtt", "port", _num(1, 65535, True), "mqtt_port", False),
    "mqtt.username":      ("mqtt", "username", _str(100), "mqtt_user", False),
    "mqtt.password":      ("mqtt", "password", _str(200), "mqtt_password", True),
    "tasmota.topic":      ("tasmota", "topic", _str(100, r"^[^#+\s]+$"), "tasmota_topic", False),
    "tasmota.full_topic": ("tasmota", "full_topic", _full_topic, "tasmota_full_topic", False),
    "tasmota.relay":      ("tasmota", "relay", _num(1, 8, True), "tasmota_relay", False),
    "ha.ip":              ("homeassistant", "ip", _str(100, HOST), "ha_ip", False),
    "ha.port":            ("homeassistant", "port", _num(1, 65535, True), "ha_port", False),
    "ha.token":           ("homeassistant", "token", _str(1000), "ha_token", True),
    "ha.slider_1":        ("homeassistant", "BOILER_1ST_ON_ENTITY_ID", _str(120, ENTITY), "ha_slider_1", False),
    "ha.slider_2":        ("homeassistant", "BOILER_2ND_ON_ENTITY_ID", _str(120, ENTITY), "ha_slider_2", False),
    "ha.script_1":        ("homeassistant", "RUN_SCRIPT_1ST_START_BOILER_ENTITY_ID", _str(120, ENTITY), "ha_script_1", False),
    "ha.script_2":        ("homeassistant", "RUN_SCRIPT_2ND_START_BOILER_ENTITY_ID", _str(120, ENTITY), "ha_script_2", False),
    "ha.max_per_boiler":  ("homeassistant", "MAX_DURATION_MINUTES_BOILER", _num(1, 600, True), "ha_max_per_boiler", False),
    "power.on_watts":     ("power", "on_watts", _num(1, 10000), "power_on_watts", False),
    "power.scale":        ("power", "scale", _num(0.01, 1000), "power_scale", False),
    "power.settle_seconds": ("power", "settle_seconds", _num(0, 600), "power_settle_seconds", False),
    "safety.max_unplanned_on": ("safety", "max_unplanned_on_minutes", _num(0, 1440, True), "max_unplanned_on", False),
    "max_manual_minutes": ("dashboard", "max_manual_minutes", _num(10, 600, True), "max_manual_minutes", False),
    "dashboard.password": ("dashboard", "password", _password, "dashboard_password", True),
}


ENV_NAMES = {"mqtt.password": "MQTT_PASSWORD", "ha.token": "HA_TOKEN", "dashboard.password": "DASHBOARD_PASSWORD"}


def values(s):
    """Current values for the page. Secrets -> {'set': bool} only."""
    out = {}
    for key, (_, _, _, attr, secret) in FIELDS.items():
        v = getattr(s, attr)
        out[key] = {"set": bool(v), "env": ENV_NAMES[key]} if secret else v
    out["needs_setup"] = s.needs_setup
    out["drivers_available"] = {"mqtt_tasmota": "Tasmota plug (MQTT)", "homeassistant": "Home Assistant",
                                "dryrun": "Test mode (switches nothing)"}
    return out


def clean(changes):
    """Untrusted {ui_key: value} -> {(section, key): str}. Raises ValueError naming the field."""
    if not isinstance(changes, dict) or len(changes) > len(FIELDS):
        raise ValueError("changes must be an object")
    out = {}
    for k, v in changes.items():
        if k not in FIELDS:
            raise ValueError(f"'{k}' cannot be changed here")
        sec, key, check, _, secret = FIELDS[k]
        if secret:
            raise ValueError(f"{k}: is set in the server's environment ({ENV_NAMES[k]}), not here")
        try:
            out[(sec, key)] = str(check(v))
        except ValueError as e:
            raise ValueError(f"{k}: {e}")
    return out


def _read_json(path):
    try:
        with open(path) as f:
            d = json.load(f)
        return d if isinstance(d, dict) else {}
    except (FileNotFoundError, ValueError, OSError):
        return {}


def save(s, cleaned):
    """Merge into settings.json (atomic write, private file mode)."""
    path = s.settings_json
    if not path:
        raise OSError("this instance has no settings file")
    data = _read_json(path)
    for (sec, key), v in cleaned.items():
        data.setdefault(sec, {})[key] = v
    tmp = path + ".tmp"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)      # holds secrets: owner only
    with os.fdopen(fd, "w") as f:
        json.dump(data, f, indent=1)
    os.replace(tmp, path)


def preview_settings(s, cleaned):
    """A Settings object with unsaved changes applied (for 'Test connection')."""
    import copy
    t = copy.copy(s)
    for k, (sec, key, check, attr, _) in FIELDS.items():
        if (sec, key) in cleaned:
            v = cleaned[(sec, key)]
            cur = getattr(s, attr)
            if attr == "driver_names":
                v = v.split(",")
            elif isinstance(cur, bool):
                v = v.lower() == "true"
            elif isinstance(cur, int):
                v = int(float(v))
            elif isinstance(cur, float) or cur is None and attr in ("latitude", "longitude"):
                v = float(v)
            setattr(t, attr, v)
    return t


def geocode(q, timeout=10):
    """City search (Open-Meteo geocoding, free, no key). [{name, country, admin, lat, lon}]"""
    q = (q or "").strip()
    if not 2 <= len(q) <= 80:
        return []
    r = requests.get("https://geocoding-api.open-meteo.com/v1/search", timeout=timeout,
                     params={"name": q, "count": 6, "language": "en", "format": "json"})
    r.raise_for_status()
    return [{"name": x.get("name", ""), "country": x.get("country", ""), "admin": x.get("admin1", ""),
             "lat": x["latitude"], "lon": x["longitude"]} for x in r.json().get("results", [])]
