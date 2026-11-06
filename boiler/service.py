"""The daily job: get weather, apply the formula, hand the result to the drivers."""
import sys
from time import sleep
from datetime import datetime, date, time

from . import APP_VERSION
from .settings import Settings
from .formula import Formula, daily_mean, explain
from .weather import get_weather, get_sun_times, csv_path, save_csv, load_csv
from .state import load_last_update, save_last_update, log_decision
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
        return False

    t, cc = daily_mean(rows, sun["sunrise"], sun["sunset"], formula)
    if t is None:
        print("❌ No weather points inside the analysis window — will retry")
        return False

    info = explain(t, cc, formula)
    minutes = info["result_min"]
    print(f"🔥 mean {t:.1f}°C, clouds {cc:.0f}% → base {info['base_min']} min, "
          f"cloud +{info['cloud_alpha'] * 100:.0f}% → {minutes} min")

    ok, used = start_boiler(drivers, minutes)
    log_decision(s, t, cc, minutes, used, ok)
    print("✅ DAILY UPDATE COMPLETED" if ok else "❌ No driver accepted the run — will retry")
    return ok


def main():
    s = Settings()
    drivers, problems = build_drivers(s)
    for p in problems:
        print(f"⚠️  {p}")
    if not drivers:
        sys.exit("❌ No usable driver. Fix the config above (or set [output] drivers = dryrun to test).")

    formula = Formula()
    bad = formula.validate()
    if bad:
        sys.exit("❌ Formula problems: " + "; ".join(bad))

    hh, mm = (int(x) for x in s.update_after.split(":"))
    print("=" * 60)
    print(f"🚀 BOILER CONTROL v{APP_VERSION}  drivers: {', '.join(d.name for d in drivers)}")
    print(f"   update after {s.update_after}, check every {s.sleep_minutes} min, debug={s.debug_ignore_time}")
    print("=" * 60)

    while True:
        now = datetime.now()
        # "done today" lives on disk, so a restart after the update time does not repeat it
        done_today = load_last_update(s) == str(date.today())
        due = now.time() >= time(hh, mm) or s.debug_ignore_time

        if due and (not done_today or s.debug_ignore_time):
            if run_daily_update(s, drivers, formula):
                save_last_update(s, date.today())
                if s.debug_ignore_time:
                    break                       # debug: one run, then stop
        else:
            print(f"⏸️  [{now:%H:%M:%S}] waiting (updated today: {done_today}, update after {s.update_after})")
        sleep(60 * s.sleep_minutes)
