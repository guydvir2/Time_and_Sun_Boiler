"""Runs that were due but did NOT start (engine was off, paused, or the device refused). The app never fires
these late on its own: it keeps them here and the dashboard asks 'Heat now?'.  Cleared at midnight.

state.json 'missed': [{id, kind: run|preset|extra, label, minutes, planned_at, source}]
"""
from datetime import datetime, timedelta

from .state import _update, _read_state, _lock, load_running, load_plan, save_plan
from .activity import log_event
from .drivers import start_boiler
from . import runs

LATE_GRACE_MIN = 10          # a start that is more than this late is not fired by itself any more: we ask


def load(s, now=None):
    now = now or datetime.now()
    today = str(now.date())
    items = [m for m in _read_state(s).get("missed", []) if str(m.get("planned_at", ""))[:10] == today]
    r = load_running(s, now)
    if r:                                   # already heating for at least that long: nothing to ask
        left = (datetime.fromisoformat(r["until"]) - now).total_seconds() / 60
        items = [m for m in items if left < m["minutes"]]
    return items


def add(s, mid, kind, label, minutes, planned_at, source):
    with _lock:
        cur = [m for m in _read_state(s).get("missed", []) if m["id"] != mid]
        cur.append({"id": mid, "kind": kind, "label": label, "minutes": int(minutes),
                    "planned_at": planned_at.isoformat(timespec="minutes"), "source": source})
        _update(s, missed=cur)
    log_event(s, source, f"{label} ({int(minutes)} min) planned for {planned_at:%H:%M} did not start: waiting for your OK", False)


def _remove(s, mid):
    with _lock:
        _update(s, missed=[m for m in _read_state(s).get("missed", []) if m["id"] != mid] or None)


def answer(s, drivers, mid, action):
    """action 'fire' or 'skip'. Returns (ok, message)."""
    item = next((m for m in _read_state(s).get("missed", []) if m["id"] == mid), None)
    if not item:
        return False, "Already handled"
    if action == "skip":
        _remove(s, mid)
        log_event(s, "manual", f"Skipped {item['label']} ({item['minutes']} min): not heated late", True)
        return True, "Skipped"
    ok, used = start_boiler(drivers, item["minutes"])
    if not ok:
        log_event(s, "manual", f"Heat now {item['label']} {item['minutes']} min FAILED", False)
        return False, "The boiler did not accept it. Try again."
    runs.record_start(s, item["minutes"], item["source"])
    log_event(s, item["source"], f"{item['label']} {item['minutes']} min started late, on your OK, via {used}", True)
    if item["kind"] == "run":                                           # keep today's plan truthful
        plan = load_plan(s)
        if plan:
            for r in plan.get("runs", []):
                if f"run{r['n']}:" + plan["date"] == mid:
                    now = datetime.now()
                    r["status"], r["start_at"] = "started", now.isoformat(timespec="minutes")
                    r["ready_at"] = (now + timedelta(minutes=item["minutes"])).isoformat(timespec="minutes")
            save_plan(s, plan)
    _remove(s, mid)
    return True, f"Heating for {item['minutes']} min"
