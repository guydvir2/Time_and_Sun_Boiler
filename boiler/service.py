"""The daily job: get weather, apply the formula, hand the result to the drivers."""
import sys
from time import sleep
from datetime import datetime, date, time

from . import APP_VERSION
from .settings import Settings
from .formula import Formula, daily_mean, explain, load_formula
from .weather import get_weather, get_sun_times, csv_path, save_csv, load_csv
from .state import (load_last_update, save_last_update, save_attempt, save_running, log_decision,
                    save_heartbeat, auto_enabled)
from .activity import log_event, trim
from .drivers import build_drivers, start_boiler


def run_daily_update(s, drivers, formula):
    """One full attempt. Returns True only if a driver accepted the run."""
    print("\n" + "=" * 40)
    print(f"DAILY UPDATE  {datetime.now():%Y-%m-%d %H:%M:%S}")
    print("=" * 40)

    sun = get_sun_times(s, s.days_back)
    path = csv_path(s, s.days_back)
    if s.read_from_file:
        rows, ok = load_csv(path)
    else:
        rows, ok = get_weather(s, sun["sunrise"], sun["sunset"], s.days_back)
        if ok and s.save_data:
            save_csv(rows, path)
    if not ok:
        print("❌ No weather data — will retry")
        save_attempt(s, False, "No weather data (will retry)")
        log_event(s, "auto", "No weather data (will retry)", False)
        return False

    t, cc = daily_mean(rows, sun["sunrise"], sun["sunset"], formula)
    if t is None:
        print("❌ No weather points inside the analysis window — will retry")
        save_attempt(s, False, "No weather points in the analysis window (will retry)")
        log_event(s, "auto", "No weather points in the analysis window (will retry)", False)
        return False

    info = explain(t, cc, formula)
    minutes = info["result_min"]
    print(f"🔥 mean {t:.1f}°C, clouds {cc:.0f}% → base {info['base_min']} min, "
          f"cloud +{info['cloud_alpha'] * 100:.0f}% → {minutes} min")

    ok, used = start_boiler(drivers, minutes)
    log_decision(s, t, cc, minutes, used, ok)
    save_attempt(s, ok, f"{minutes} min via {used}" if ok else "No driver accepted the run (will retry)")
    if ok:
        save_running(s, minutes, "auto")
    log_event(s, "auto", (f"Auto run {minutes} min via {used} ({t:.1f}°C, {cc:.0f}% clouds)" if ok
                          else f"Auto run {minutes} min: no driver accepted it (will retry)"), ok)
    print("✅ DAILY UPDATE COMPLETED" if ok else "❌ No driver accepted the run — will retry")
    return ok


def main():
    s = Settings()
    drivers, problems = build_drivers(s)
    for p in problems:
        print(f"⚠️  {p}")
    if not drivers:
        sys.exit("❌ No usable driver. Fix the config above (or set [output] drivers = dryrun to test).")

    formula = load_formula(s)
    bad = formula.blocking_problems()
    if bad:
        print("⚠️  Saved formula has problems (" + "; ".join(bad) + "); using the default")
        formula = Formula()

    hh, mm = (int(x) for x in s.update_after.split(":"))
    trim(s)
    log_event(s, "service", f"Service started v{APP_VERSION}, drivers: {', '.join(d.name for d in drivers)}")

    if s.dashboard_enabled:
        try:                                    # a UI problem must never stop the boiler job
            from .web import start_in_thread
            start_in_thread(s, drivers, formula)
        except Exception as e:
            print(f"⚠️  Dashboard could not start: {e}")

    print("=" * 60)
    print(f"🚀 BOILER CONTROL v{APP_VERSION}  drivers: {', '.join(d.name for d in drivers)}")
    print(f"   update after {s.update_after}, check every {s.sleep_minutes} min, debug={s.debug_ignore_time}")
    print("=" * 60)

    debug_pending = s.debug_ignore_time         # debug: run once right now, then behave normally
    while True:
        now = datetime.now()
        save_heartbeat(s)                        # the dashboard shows "Engine: running" from this
        if not auto_enabled(s):
            print(f"⏸️  [{now:%H:%M:%S}] auto-run is PAUSED (dashboard) — not running today's update")
            sleep(60 * s.sleep_minutes)
            continue
        # "done today" lives on disk, so a restart after the update time does not repeat it
        done_today = load_last_update(s) == str(date.today())
        due = now.time() >= time(hh, mm) or debug_pending

        if due and (not done_today or debug_pending):
            formula = load_formula(s)            # picks up a formula saved on /tune, no restart needed
            if formula.blocking_problems():
                formula = Formula()
            if run_daily_update(s, drivers, formula):
                save_last_update(s, date.today())
                debug_pending = False           # keep running (and serving the dashboard) afterwards
        else:
            print(f"⏸️  [{now:%H:%M:%S}] waiting (updated today: {done_today}, update after {s.update_after})")
        sleep(60 * s.sleep_minutes)
