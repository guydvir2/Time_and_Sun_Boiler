"""The engine. Every 30 s it checks:
  1. Today's estimate, refined every hour (measured weather so far + forecast), locked after the sun window.
  2. Run 1 / run 2 start times reached -> switch on (see rolling.py).
  3. Weekly presets (extra timed runs) due -> switch on.
Pause (dashboard) stops 2-3; the estimate keeps refining. Extra hot water and Stop always work.
"""
import sys
from time import sleep, monotonic
from datetime import datetime, date, time, timedelta

from . import APP_VERSION
from .settings import Settings
from .formula import Formula, daily_mean, explain, load_formula
from .weather import get_day, csv_path, save_csv
from .state import (load_last_update, save_last_update, save_attempt, log_decision, save_heartbeat,
                    auto_enabled, save_plan, load_plan, preset_done, mark_preset_done, load_running,
                    load_later, clear_later)
from .activity import log_event, trim
from .drivers import build_drivers, start_boiler
from . import schedule, runs, rolling

TICK_SECONDS = 30
START_RETRY_MIN = 2          # a failed start is retried every 2 min ...
START_GIVE_UP_MIN = 120      # ... for up to 2 h after its planned time


def estimate(s, formula, now):
    """Today's minutes from the whole day's weather (measured so far + forecast for the rest).
    Returns (minutes, temp, clouds, lock_at, rows) or None when there is no data."""
    day = get_day(s, now.date())
    if not day:
        return None
    rise = round(day["sunrise"].hour + day["sunrise"].minute / 60) % 24
    sets = round(day["sunset"].hour + day["sunset"].minute / 60) % 24
    t, cc = daily_mean(day["rows"], rise, sets, formula)
    if t is None:
        return None
    info = explain(t, cc, formula)
    return info["result_min"], t, cc, rolling.lock_time(now.date(), day["sunset"], formula), day["rows"]


def start_now(s, drivers, minutes, source, label):
    ok, used = start_boiler(drivers, minutes)
    if ok:
        runs.record_start(s, minutes, source)
    log_event(s, source, f"{label} {minutes} min via {used}" if ok else f"{label} {minutes} min: no driver accepted it", ok)
    return ok, used


def update_estimate(s, formula, now):
    """One refinement of today's plan. Returns False when the weather could not be read (retry later)."""
    today = str(now.date())
    plan = load_plan(s)
    if not plan or plan.get("date") != today or "runs" not in plan:
        if plan and plan.get("date") == today:           # an old-style plan from before the upgrade: leave today alone
            return True
        plan = rolling.new_plan(now.date())
    if plan["status"] == "final":
        return True
    res = estimate(s, formula, now)
    if res is None:
        save_attempt(s, False, "No weather data (will retry)")
        return False
    minutes, t, cc, lock_at, rows = res
    final = now >= lock_at
    before = plan.get("minutes")
    rolling.apply_estimate(plan, minutes, t, cc, lock_at, now, final)
    rolling.sync_runs(plan, s, schedule.load(s)["ready_by"], now)
    save_plan(s, plan)
    if final:
        save_last_update(s, now.date())
        log_decision(s, t, cc, minutes, "final", True)
        if s.save_data:
            save_csv([r for r in rows if r["time"] <= now], csv_path(s, 0))
        save_attempt(s, True, f"Final: {minutes} min ({t:.1f}°C, {cc:.0f}% clouds)")
        log_event(s, "auto", f"Final for today: {minutes} min ({t:.1f}°C, {cc:.0f}% clouds)")
    else:
        save_attempt(s, True, f"Estimate {minutes} min, final after {lock_at:%H:%M}")
        if before is None:
            log_event(s, "auto", f"First estimate today: {minutes} min, final after {lock_at:%H:%M}")
    print(f"{'🔒' if final else '🔁'} {minutes} min ({t:.1f}°C, {cc:.0f}% clouds){'' if final else f', final after {lock_at:%H:%M}'}")
    return True


class Engine:
    """One tick = one look at the clock. Kept as a class so tests can drive it without sleeping."""

    def __init__(self, s, drivers):
        self.s, self.drivers = s, drivers
        self.next_decision_try = 0.0          # monotonic: next estimate (hourly; after a failure every SLEEP_MINUTES)
        self.last_try = {}                    # start retries: key -> monotonic

    def _formula(self):
        f = load_formula(self.s)               # picks up a formula saved on /tune, no restart needed
        return Formula() if f.blocking_problems() else f

    def _may_try(self, key, every_min):
        t = monotonic()
        if t - self.last_try.get(key, -1e9) < every_min * 60:
            return False
        self.last_try[key] = t
        return True

    def _later(self, now):
        s, later = self.s, load_later(self.s)
        if not later:
            return
        at = datetime.fromisoformat(later["at"])
        if now < at:
            return
        if now > at + timedelta(minutes=START_GIVE_UP_MIN):
            clear_later(s)
            log_event(s, "manual", f"Gave up the extra {later['minutes']} min set for {at:%H:%M}", False)
        elif self._may_try("later", START_RETRY_MIN):
            ok, _ = start_now(s, self.drivers, later["minutes"], "manual", "Extra hot water")
            if ok:
                clear_later(s)

    def tick(self, now=None):
        s, now = self.s, now or datetime.now()
        save_heartbeat(s)
        self._later(now)                       # a user-set extra run goes ahead even when paused
        today = str(now.date())

        # 1. refine today's estimate (hourly, and at once when the lock time is passed)
        plan = load_plan(s)
        pending_lock = (plan and plan.get("date") == today and plan.get("status") == "estimate"
                        and plan.get("lock_at") and now >= datetime.fromisoformat(plan["lock_at"]))
        if monotonic() >= self.next_decision_try or pending_lock:
            ok = update_estimate(s, self._formula(), now)
            self.next_decision_try = monotonic() + 60 * (rolling.ESTIMATE_EVERY_MIN if ok else s.sleep_minutes)

        if not auto_enabled(s):
            return "paused"

        # 2. runs: keep their times in step with the estimate, start the ones that are due
        plan = load_plan(s)
        if plan and plan.get("date") == today and "runs" in plan and plan.get("minutes") is not None:
            rolling.sync_runs(plan, s, schedule.load(s)["ready_by"], now)
            for run in plan["runs"]:
                if not rolling.startable(plan, run, s.min_run):
                    continue
                start_at = datetime.fromisoformat(run["start_at"])
                if now < start_at:
                    continue
                if now > start_at + timedelta(minutes=START_GIVE_UP_MIN):
                    run["status"] = "missed"
                    log_event(s, "auto", f"Gave up run {run['n']} ({run['minutes']} min) planned for {start_at:%H:%M}", False)
                elif self._may_try(f"run{run['n']}", START_RETRY_MIN):
                    label = f"Run {run['n']}" + ("" if plan["status"] == "final" else " (on estimate)")
                    ok, used = start_now(s, self.drivers, run["minutes"], "auto", label)
                    run["status"] = "started" if ok else "failed"
                    if ok:
                        run["start_at"] = now.isoformat(timespec="minutes")
                        run["ready_at"] = (now + timedelta(minutes=run["minutes"])).isoformat(timespec="minutes")
            save_plan(s, plan)

        # 3. presets
        sched = schedule.load(s)
        for p, too_late in schedule.due_presets(sched, now, preset_done(s)):
            if too_late:
                mark_preset_done(s, p["id"], today)
                log_event(s, "preset", f"Skipped '{p['name']}' {p['time']}: the engine was not running then", False)
                continue
            r = load_running(s, now)
            if r and (datetime.fromisoformat(r["until"]) - now).total_seconds() / 60 >= p["minutes"]:
                mark_preset_done(s, p["id"], today)
                log_event(s, "preset", f"'{p['name']}' not needed: already ON until {r['until'][11:16]}")
                continue
            if self._may_try("preset:" + p["id"], START_RETRY_MIN):
                ok, _ = start_now(s, self.drivers, p["minutes"], "preset", f"Preset '{p['name']}'")
                if ok:
                    mark_preset_done(s, p["id"], today)
        return "ok"


def main():
    from .app import App
    app = App()
    s = app.s
    for p in app.problems:
        print(f"⚠️  {p}")
    trim(s)
    if s.legacy_secrets:
        print(f"⚠️  Secrets found in a config file and IGNORED: {', '.join(s.legacy_secrets)}. "
              f"Run migrate_secrets.py, then restart (see README: Secrets).")
    log_event(s, "service", f"Service started v{APP_VERSION}" + (f", drivers: {', '.join(d.name for d in app.drivers)}"
                                                                 if app.drivers else ", waiting for setup"))
    if s.dashboard_enabled:
        try:                                    # a UI problem must never stop the boiler job
            from .web import start_in_thread
            start_in_thread(s, app.drivers, app=app)
        except Exception as e:
            print(f"⚠️  Dashboard could not start: {e}")
    elif not app.drivers:
        sys.exit("❌ No usable driver and the dashboard is off. Fix config.ini (or set [output] drivers = dryrun).")

    print("=" * 60)
    print(f"🚀 BoilerMind v{APP_VERSION}  drivers: {', '.join(d.name for d in app.drivers) or 'none yet'}")
    if s.needs_setup or not app.drivers:
        print(f"   👉 Finish setup in the dashboard: http://<this-machine>:{s.dashboard_port}/#setup")
    print("=" * 60)

    eng = Engine(s, app.drivers)
    last_note, seen = None, app.version
    while True:
        try:
            app.maybe_reload()
            if app.version != seen:              # settings saved on the dashboard: use them now
                seen = app.version
                eng = Engine(app.s, app.drivers)
                print(f"🔄 settings reloaded, drivers: {', '.join(d.name for d in app.drivers) or 'none'}")
            state = eng.tick() if app.drivers else "waiting for setup"
        except Exception as e:                   # one bad tick must not kill the engine
            print(f"❌ tick error: {e}")
            state = "error"
        if state != last_note:
            print(f"⏱️  [{datetime.now():%H:%M:%S}] engine {state}")
            last_note = state
        sleep(TICK_SECONDS)
