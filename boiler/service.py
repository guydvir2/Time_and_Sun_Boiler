"""The engine. Every 30 s it checks three things:
  1. Daily decision (after UPDATE_AFTER): weather -> formula -> minutes -> a plan (start now, or later so the
     water is hot by the 'ready by' time).
  2. The plan's start time reached -> switch on.
  3. Weekly presets (extra timed runs) due -> switch on.
Pause (dashboard) stops 1-3. Manual Run/Stop on the dashboard always work.
"""
import sys
from time import sleep, monotonic
from datetime import datetime, date, time, timedelta

from . import APP_VERSION
from .settings import Settings
from .formula import Formula, daily_mean, explain, load_formula
from .weather import get_weather, get_sun_times, csv_path, save_csv, load_csv
from .state import (load_last_update, save_last_update, save_attempt, log_decision, save_heartbeat,
                    auto_enabled, save_plan, load_plan, preset_done, mark_preset_done, load_running,
                    load_later, clear_later)
from .activity import log_event, trim
from .drivers import build_drivers, start_boiler
from . import schedule, runs

TICK_SECONDS = 30
START_RETRY_MIN = 2          # a failed start is retried every 2 min ...
START_GIVE_UP_MIN = 120      # ... for up to 2 h after its planned time


def decide(s, formula):
    """Weather -> minutes. Returns (minutes, temp, clouds) or None (no data, will retry)."""
    sun = get_sun_times(s, s.days_back)
    path = csv_path(s, s.days_back)
    if s.read_from_file:
        rows, ok = load_csv(path)
    else:
        rows, ok = get_weather(s, sun["sunrise"], sun["sunset"], s.days_back)
        if ok and s.save_data:
            save_csv(rows, path)
    if not ok:
        return None, "No weather data (will retry)"
    t, cc = daily_mean(rows, sun["sunrise"], sun["sunset"], formula)
    if t is None:
        return None, "No weather points in the analysis window (will retry)"
    info = explain(t, cc, formula)
    print(f"🔥 mean {t:.1f}°C, clouds {cc:.0f}% → base {info['base_min']} min, "
          f"cloud +{info['cloud_alpha'] * 100:.0f}% → {info['result_min']} min")
    return (info["result_min"], t, cc), ""


def start_now(s, drivers, minutes, source, label):
    ok, used = start_boiler(drivers, minutes)
    if ok:
        runs.record_start(s, minutes, source)
    log_event(s, source, f"{label} {minutes} min via {used}" if ok else f"{label} {minutes} min: no driver accepted it", ok)
    return ok, used


def run_daily_update(s, drivers, formula, now=None):
    """The daily decision. Returns True once today's plan is made. A failed start is retried by the
    engine (plan status 'failed'), not by deciding again."""
    now = now or datetime.now()
    print("\n" + "=" * 40 + f"\nDAILY UPDATE  {now:%Y-%m-%d %H:%M:%S}\n" + "=" * 40)
    result, why = decide(s, formula)
    if result is None:
        print("❌ " + why)
        save_attempt(s, False, why)
        log_event(s, "auto", why, False)
        return False
    minutes, t, cc = result
    start_at, ready_at = schedule.plan_start(now, minutes, schedule.load(s)["ready_by"])
    plan = {"date": str(now.date()), "minutes": minutes, "temp": round(t, 1), "clouds": round(cc),
            "start_at": start_at.isoformat(timespec="minutes"), "ready_at": ready_at.isoformat(timespec="minutes"),
            "status": "planned"}
    if minutes <= 0:
        plan["status"] = "skipped"
        save_plan(s, plan)
        log_decision(s, t, cc, 0, "", True)
        save_attempt(s, True, "0 min needed today (warm and sunny)")
        log_event(s, "auto", f"No run needed today ({t:.1f}°C, {cc:.0f}% clouds)")
        return True
    if start_at > now:
        save_plan(s, plan)
        log_decision(s, t, cc, minutes, "planned", True)
        save_attempt(s, True, f"{minutes} min planned: start {start_at:%H:%M}, ready {ready_at:%H:%M}")
        log_event(s, "auto", f"Planned {minutes} min: start {start_at:%H:%M}, hot by {ready_at:%H:%M} ({t:.1f}°C, {cc:.0f}% clouds)")
        print(f"🕒 planned: start {start_at:%H:%M}, ready by {ready_at:%H:%M}")
        return True
    ok, used = start_now(s, drivers, minutes, "auto", "Auto run")
    plan["status"] = "started" if ok else "failed"
    save_plan(s, plan)
    log_decision(s, t, cc, minutes, used, ok)
    save_attempt(s, ok, f"{minutes} min via {used}" if ok else "No driver accepted the run (will retry)")
    print("✅ DAILY UPDATE COMPLETED" if ok else "❌ No driver accepted the run — will retry")
    return True


class Engine:
    """One tick = one look at the clock. Kept as a class so tests can drive it without sleeping."""

    def __init__(self, s, drivers):
        self.s, self.drivers = s, drivers
        self.hh, self.mm = (int(x) for x in s.update_after.split(":"))
        self.debug_pending = s.debug_ignore_time
        self.next_decision_try = 0.0          # monotonic; weather failures retry every SLEEP_MINUTES
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
        if not auto_enabled(s):
            return "paused"
        today = str(now.date())

        # 1. daily decision
        due = now.time() >= time(self.hh, self.mm) or self.debug_pending
        if due and (load_last_update(s) != today or self.debug_pending) and monotonic() >= self.next_decision_try:
            if run_daily_update(s, self.drivers, self._formula(), now):
                save_last_update(s, now.date())
                if (load_plan(s) or {}).get("status") == "failed":
                    self.last_try["plan"] = monotonic()  # it just failed: next try in START_RETRY_MIN
                self.debug_pending = False
            else:
                self.next_decision_try = monotonic() + 60 * s.sleep_minutes

        # 2. planned start (also retries a failed immediate start)
        plan = load_plan(s)
        if plan and plan.get("status") in ("planned", "failed"):
            start_at = datetime.fromisoformat(plan["start_at"])
            if now >= start_at:
                if now > start_at + timedelta(minutes=START_GIVE_UP_MIN):
                    plan["status"] = "missed"
                    save_plan(s, plan)
                    log_event(s, "auto", f"Gave up the {plan['minutes']} min run planned for {start_at:%H:%M}", False)
                elif self._may_try("plan", START_RETRY_MIN):
                    ok, used = start_now(s, self.drivers, plan["minutes"], "auto", "Auto run")
                    plan["status"] = "started" if ok else "failed"
                    save_plan(s, plan)
                    if ok:
                        log_decision(s, plan["temp"], plan["clouds"], plan["minutes"], used, True)

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
                eng.next_decision_try = 0.0
                print(f"🔄 settings reloaded, drivers: {', '.join(d.name for d in app.drivers) or 'none'}")
            state = eng.tick() if app.drivers else "waiting for setup"
        except Exception as e:                   # one bad tick must not kill the engine
            print(f"❌ tick error: {e}")
            state = "error"
        if state != last_note:
            print(f"⏱️  [{datetime.now():%H:%M:%S}] engine {state}")
            last_note = state
        sleep(TICK_SECONDS)
