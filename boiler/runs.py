"""Operation log: how many minutes the boiler really ran, by source (auto / preset / manual).

stored_data/runs.jsonl, one line per event: {"time", "source", "minutes"}. A run that is stopped early,
or replaced by a new start before it ended, gets a negative line for the part that did not run.
"""
import json
import os
import threading
from collections import defaultdict
from datetime import datetime, timedelta

from .state import load_running, save_running, clear_running

_lock = threading.Lock()
SOURCES = ("auto", "preset", "manual")


def _append(s, source, minutes, note=""):
    os.makedirs(os.path.dirname(s.runs_file), exist_ok=True)
    rec = {"time": datetime.now().isoformat(timespec="seconds"), "source": source, "minutes": round(minutes, 1)}
    if note:
        rec["note"] = note
    with open(s.runs_file, "a") as f:
        f.write(json.dumps(rec) + "\n")


def _cut_current(s, now, note):
    r = load_running(s, now)
    if r:
        left = (datetime.fromisoformat(r["until"]) - now).total_seconds() / 60
        if left > 0:
            _append(s, r.get("source", "manual"), -left, note)


def record_start(s, minutes, source):
    """Call after a driver ACCEPTED a start of `minutes` (the device timer restarts from now)."""
    with _lock:
        now = datetime.now()
        _cut_current(s, now, "replaced by a new start")
        if minutes > 0:
            _append(s, source, minutes)
            save_running(s, minutes, source)


def record_stop(s):
    with _lock:
        _cut_current(s, datetime.now(), "stopped")
        clear_running(s)


def read(s):
    try:
        with open(s.runs_file) as f:
            return [json.loads(x) for x in f if x.strip()]
    except (FileNotFoundError, OSError, ValueError):
        return []


def cost_of(s, minutes):
    """(kWh, cost) of `minutes` on time: an estimate from the heater's rated power and the price per kWh."""
    kwh = max(0.0, minutes) / 60 * s.boiler_kw
    return kwh, kwh * s.price_kwh


def totals(s, now=None, days=14):
    """{'today': {auto, preset, manual, total}, 'week': total of last 7 days, 'per_day': [{date, auto, preset, manual}]}
    Minutes still to come in a running run are counted (the device will run them)."""
    now = now or datetime.now()
    per = defaultdict(lambda: defaultdict(float))
    for r in read(s):
        try:
            per[r["time"][:10]][r["source"]] += float(r["minutes"])
        except (KeyError, ValueError, TypeError):
            continue
    rows = []
    for i in range(days - 1, -1, -1):
        d = str((now - timedelta(days=i)).date())
        row = {"date": d, **{k: max(0, round(per[d][k])) for k in SOURCES}}
        row["kwh"], row["cost"] = (round(x, 2) for x in cost_of(s, sum(row[k] for k in SOURCES)))
        rows.append(row)
    t = rows[-1]
    week_min = sum(sum(r[k] for k in SOURCES) for r in rows[-7:])
    return {"today": {**{k: t[k] for k in SOURCES}, "total": sum(t[k] for k in SOURCES), "kwh": t["kwh"], "cost": t["cost"]},
            "week": week_min, "week_cost": round(sum(r["cost"] for r in rows[-7:]), 2), "per_day": rows}


def segments(s, day, now=None):
    """ON periods that started on `day` (YYYY-MM-DD): [{'start','end','source'}] as ISO minutes. A cut
    (stop / replaced) shortens the segment it belongs to."""
    now = now or datetime.now()
    segs = []
    for r in read(s):
        try:
            t, m = datetime.fromisoformat(r["time"]), float(r["minutes"])
        except (KeyError, ValueError, TypeError):
            continue
        if r["time"][:10] != day:
            continue
        if m > 0:
            segs.append({"start": t, "end": t + timedelta(minutes=m), "source": r.get("source", "manual")})
        elif segs:
            segs[-1]["end"] = min(segs[-1]["end"], t)
    out = []
    for x in segs:
        if x["end"] <= x["start"]:
            continue
        mins = (x["end"] - x["start"]).total_seconds() / 60
        kwh, cost = cost_of(s, mins)
        out.append({"start": x["start"].isoformat(timespec="minutes"), "end": x["end"].isoformat(timespec="minutes"),
                    "source": x["source"], "minutes": round(mins), "kwh": round(kwh, 2), "cost": round(cost, 2)})
    return out
