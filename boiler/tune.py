"""Replay a formula over every saved day (stored_data/YYYY-MM-DD.csv). Read-only: nothing is switched or saved."""
import contextlib
import glob
import io
import os
import re
import threading
from statistics import mean

from .formula import daily_mean, compute_duration
from .weather import load_csv

DAY_FILE = re.compile(r"^(\d{4}-\d{2}-\d{2})\.csv$")
_cache = {"key": None, "days": []}
_lock = threading.Lock()


def saved_days(s):
    """[{date, rows, sunrise, sunset}] for every usable saved day, oldest first. Cached until files change."""
    paths = sorted(p for p in glob.glob(os.path.join(s.stored_dir, "*.csv")) if DAY_FILE.match(os.path.basename(p)))
    key = tuple((p, os.path.getmtime(p)) for p in paths)
    with _lock:
        if key == _cache["key"]:
            return _cache["days"]
        days = []
        for p in paths:
            with contextlib.redirect_stdout(io.StringIO()):
                rows, ok = load_csv(p)
            if ok and len(rows) >= 3:
                hours = [r["time"].hour for r in rows]
                days.append({"date": DAY_FILE.match(os.path.basename(p)).group(1), "rows": rows,
                             "sunrise": min(hours), "sunset": max(hours)})
        _cache.update(key=key, days=days)
        return days


def replay(s, formula, sunshine=None):
    """What `formula` gives on each saved day: [{date, temp, clouds, minutes, sun_pct?}]."""
    out = []
    for d in saved_days(s):
        t, cc = daily_mean(d["rows"], d["sunrise"], d["sunset"], formula)
        if t is None:
            continue
        row = {"date": d["date"], "temp": round(t, 1), "clouds": round(cc), "minutes": compute_duration(t, cc, formula)}
        if sunshine and d["date"] in sunshine:
            row["sun_pct"] = sunshine[d["date"]]["sun_pct"]
        out.append(row)
    return out


def compare(current, new):
    """Summary of new vs current replays (same days)."""
    if not current:
        return {"days": 0}
    by = {r["date"]: r["minutes"] for r in current}
    diffs = [r["minutes"] - by[r["date"]] for r in new if r["date"] in by]
    return {"days": len(diffs), "avg_current": round(mean(by.values())), "avg_new": round(mean(r["minutes"] for r in new)),
            "changed": sum(1 for x in diffs if x), "more": sum(1 for x in diffs if x > 0),
            "less": sum(1 for x in diffs if x < 0), "max_up": max(diffs + [0]), "max_down": min(diffs + [0])}
