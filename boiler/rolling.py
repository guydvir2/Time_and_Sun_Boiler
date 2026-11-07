"""The rolling daily decision.

Every hour the engine re-estimates today's minutes: hours already past use measured weather, hours still
ahead use the forecast. The number refines through the day and locks once the sun window is over (the last
hour that counts is sunset - window_end_offset). After that it is a calculation, not a prediction.

The day's minutes are split in two runs:
  run 1  share = run1_pct of the day, timed to be hot by 'ready by' (or right at the lock if none is set).
         With a 'ready by' it may start before the lock, on the estimate of that moment.
  run 2  the rest, at run2_at (never before the lock). It also tops up whatever run 1 missed because it
         started on an estimate that later went up.
Neither run is longer than the max per run.
"""
from datetime import datetime, time, timedelta

from . import schedule

ESTIMATE_EVERY_MIN = 60
MIN_RUN = 3                       # shorter runs are skipped


def split(total, pct, cap):
    """(run1, run2) minutes. Run 1 gets pct % of the day, both capped; what does not fit goes to run 2."""
    total = max(0, int(total))
    r1 = min(int(round(total * pct / 100)), cap)
    r2 = min(total - r1, cap)
    return r1, max(0, r2)


def lock_time(day, sunset, f):
    """When the number stops changing: the end of the last hour in the sun window."""
    hi = round(sunset.hour + sunset.minute / 60) % 24 - f.window_end_offset
    return datetime.combine(day, time(0)) + timedelta(hours=hi + 1)


def new_plan(day):
    return {"date": str(day), "status": "estimate", "minutes": None, "runs": [], "trail": []}


def apply_estimate(plan, minutes, temp, clouds, lock_at, now, final):
    plan.update(minutes=int(minutes), temp=round(temp, 1), clouds=round(clouds), lock_at=lock_at.isoformat(timespec="minutes"),
                updated=now.isoformat(timespec="minutes"), status="final" if final else "estimate")
    trail = plan.setdefault("trail", [])
    if not trail or trail[-1][1] != int(minutes) or final:
        trail.append([now.strftime("%H:%M"), int(minutes)])
        del trail[:-24]


def _run(plan, n):
    for r in plan["runs"]:
        if r["n"] == n:
            return r
    r = {"n": n, "minutes": 0, "status": "planned", "start_at": None, "ready_at": None}
    plan["runs"].append(r)
    plan["runs"].sort(key=lambda x: x["n"])
    return r


def _iso(t):
    return t.isoformat(timespec="minutes")


def sync_runs(plan, s, ready_by, now):
    """(Re)compute minutes and times of the runs that have not started yet. Safe to call every tick."""
    if plan.get("minutes") is None:
        return plan
    cap, total = s.max_manual_minutes, plan["minutes"]
    lock_at = datetime.fromisoformat(plan["lock_at"])
    final = plan["status"] == "final"
    r1_target, _ = split(total, s.run1_pct, cap)

    r1 = _run(plan, 1)
    due = r1["status"] == "planned" and r1.get("start_at") and now >= datetime.fromisoformat(r1["start_at"]) and r1["minutes"] >= MIN_RUN
    if due:                                   # its start time has come: keep it, the engine starts it now
        r1["minutes"] = max(r1["minutes"], min(r1_target, cap)) if r1_target >= MIN_RUN else r1["minutes"]
    elif r1["status"] in ("planned", "skipped"):
        r1["minutes"] = r1_target
        if r1_target < MIN_RUN:
            r1["status"] = "skipped" if final or r1_target == 0 else "planned"
        else:
            r1["status"] = "planned"
        if ready_by:
            st, rd = schedule.plan_start(now, r1_target, ready_by)
            if rd.date() > now.date():                    # ready-by already passed today: run at the lock
                st = max(now, lock_at)
                rd = st + timedelta(minutes=r1_target)
            r1["start_at"], r1["ready_at"] = _iso(st), _iso(rd)
        else:
            st = max(lock_at, now) if final else lock_at
            r1["start_at"], r1["ready_at"] = _iso(st), _iso(st + timedelta(minutes=r1_target))

    ran1 = r1["minutes"] if r1["status"] not in ("skipped",) else 0
    r2 = _run(plan, 2)
    if r2["status"] in ("planned", "skipped"):
        m2 = min(max(0, total - ran1), cap)
        r2["minutes"] = m2
        r1_end = datetime.fromisoformat(r1["ready_at"]) if r1.get("ready_at") else lock_at
        at = lock_at
        if s.run2_at:
            hh, mm = (int(x) for x in s.run2_at.split(":"))
            at = max(at, datetime.combine(now.date(), time(hh, mm)))
        at = max(at, r1_end)
        if final:
            at = max(at, now)
        r2["start_at"], r2["ready_at"] = _iso(at), _iso(at + timedelta(minutes=m2))
        r2["status"] = "planned" if m2 >= MIN_RUN else "skipped"
    return plan


def startable(plan, run):
    """Run 1 may go on an estimate (it has a ready-by to meet); run 2 only on the final number."""
    return run["status"] in ("planned", "failed") and run["minutes"] >= MIN_RUN and (run["n"] == 1 or plan["status"] == "final")
