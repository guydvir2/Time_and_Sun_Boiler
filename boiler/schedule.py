"""When things run: 'ready by' time for the sun-based run, and weekly presets (extra timed runs).

stored_data/schedule.json (edited on the Schedule tab):
    {"ready_by": "18:30" | "",                 # "" = start right after the daily decision
     "presets": [{"id", "name", "days": [0..6 Mon=0], "time": "06:00", "minutes": 30, "enabled": true}]}
"""
import json
import os
import re
import secrets
from datetime import datetime, date, timedelta, time

HHMM = re.compile(r"^([01]\d|2[0-3]):[0-5]\d$")
DAY_NAMES = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
PRESET_GRACE_MIN = 15          # engine was down at preset time: still run if at most this late, else skip
MAX_PRESETS = 20


def default_schedule():
    return {"ready_by": "", "presets": []}


def parse_hhmm(v):
    h, m = v.split(":")
    return time(int(h), int(m))


def validate(d, max_minutes):
    """Untrusted (web) input -> clean schedule dict. Raises ValueError with a readable message."""
    if not isinstance(d, dict):
        raise ValueError("schedule must be an object")
    rb = d.get("ready_by", "") or ""
    if rb and not (isinstance(rb, str) and HHMM.match(rb)):
        raise ValueError("Ready-by must be HH:MM (or empty)")
    presets = d.get("presets", [])
    if not isinstance(presets, list) or len(presets) > MAX_PRESETS:
        raise ValueError(f"Up to {MAX_PRESETS} presets")
    clean = []
    for p in presets:
        if not isinstance(p, dict):
            raise ValueError("bad preset")
        name = str(p.get("name", "") or "Preset").strip()[:30] or "Preset"
        days = p.get("days", [])
        if not isinstance(days, list) or not days or any(isinstance(x, bool) or x not in range(7) for x in days):
            raise ValueError(f"{name}: pick at least one day")
        t = p.get("time", "")
        if not (isinstance(t, str) and HHMM.match(t)):
            raise ValueError(f"{name}: time must be HH:MM")
        m = p.get("minutes")
        if isinstance(m, bool) or not isinstance(m, (int, float)) or int(m) != m or not 1 <= m <= max_minutes:
            raise ValueError(f"{name}: minutes must be 1 to {max_minutes}")
        pid = str(p.get("id") or "")
        if not re.match(r"^[A-Za-z0-9_-]{1,24}$", pid):
            pid = secrets.token_hex(4)
        clean.append({"id": pid, "name": name, "days": sorted(set(days)), "time": t, "minutes": int(m),
                      "enabled": bool(p.get("enabled", True))})
    return {"ready_by": rb, "presets": clean}


def load(s):
    try:
        with open(s.schedule_file) as f:
            return validate(json.load(f), 10 ** 6)
    except FileNotFoundError:
        return default_schedule()
    except (ValueError, OSError) as e:
        print(f"⚠️  {s.schedule_file} unreadable ({e}); no presets, no ready-by")
        return default_schedule()


def save(s, sched):
    os.makedirs(os.path.dirname(s.schedule_file), exist_ok=True)
    tmp = s.schedule_file + ".tmp"
    with open(tmp, "w") as f:
        json.dump(sched, f, indent=1)
    os.replace(tmp, s.schedule_file)


def plan_start(decided_at: datetime, minutes: int, ready_by: str):
    """(start_at, ready_at). Ready time later today, or tomorrow if it is already past.
    If there is not enough time left, start now (ready_at then shows the real, later time)."""
    if not ready_by or minutes <= 0:
        return decided_at, decided_at + timedelta(minutes=minutes)
    ready = datetime.combine(decided_at.date(), parse_hhmm(ready_by))
    if ready <= decided_at:
        ready += timedelta(days=1)
    start = ready - timedelta(minutes=minutes)
    if start < decided_at:
        return decided_at, decided_at + timedelta(minutes=minutes)
    return start, ready


def due_presets(sched, now: datetime, done: dict):
    """Presets that should start now: today is one of its days, time reached, not later than the grace
    period, not done today. done = {preset_id: 'YYYY-MM-DD'}. Returns list of (preset, late_skip: bool)."""
    out = []
    today = str(now.date())
    for p in sched["presets"]:
        if not p["enabled"] or now.weekday() not in p["days"] or done.get(p["id"]) == today:
            continue
        at = datetime.combine(now.date(), parse_hhmm(p["time"]))
        if now < at:
            continue
        out.append((p, now > at + timedelta(minutes=PRESET_GRACE_MIN)))
    return out


def upcoming(sched, now: datetime, plan=None, limit=3):
    """Next scheduled starts (planned auto run + presets), soonest first: [{'at','what','minutes'}]."""
    ev = []
    if plan and plan.get("status") == "planned":
        ev.append({"at": plan["start_at"], "what": "Auto (sun)", "minutes": plan["minutes"]})
    for p in sched["presets"]:
        if not p["enabled"]:
            continue
        for i in range(8):
            d = now.date() + timedelta(days=i)
            at = datetime.combine(d, parse_hhmm(p["time"]))
            if d.weekday() in p["days"] and at > now:
                ev.append({"at": at.isoformat(timespec="minutes"), "what": p["name"], "minutes": p["minutes"]})
                break
    return sorted(ev, key=lambda e: e["at"])[:limit]
