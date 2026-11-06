"""Fill the history from weather files already saved in stored_data/ (one CSV per day).

For each saved day it works out what the CURRENT formula would have decided and appends it to
decisions.csv, marked driver='replay'. These are NOT real runs: the dashboard shows them lighter.
Safe to run any number of times: it only appends days that are missing, never edits anything.
"""
import contextlib
import csv
import glob
import io
import os
import re
from datetime import date, datetime

from .formula import Formula, daily_mean, compute_duration
from .state import DECISION_COLUMNS
from .weather import load_csv

DAY_FILE = re.compile(r"^(\d{4}-\d{2}-\d{2})\.csv$")


def _known_days(s):
    days = set()
    try:
        with open(s.decisions_file, newline="") as f:
            for r in csv.DictReader(f):
                days.add(r["timestamp"][:10])
    except (FileNotFoundError, KeyError, OSError):
        pass
    return days


def backfill(s, formula=None, today=None):
    """Append replayed decisions for saved days that have none. Returns the list of added rows."""
    formula = formula or Formula()
    today = str(today or date.today())
    known = _known_days(s)
    added = []
    for path in sorted(glob.glob(os.path.join(s.stored_dir, "*.csv"))):
        m = DAY_FILE.match(os.path.basename(path))
        if not m or m.group(1) in known or m.group(1) >= today:    # skip decisions.csv, known days, today
            continue
        with contextlib.redirect_stdout(io.StringIO()):
            rows, ok = load_csv(path)
        if not ok or len(rows) < 3:
            continue
        hours = [r["time"].hour for r in rows]
        sunrise, sunset = min(hours), max(hours)         # the saved window is exactly [sunrise, sunset]
        t, cc = daily_mean(rows, sunrise, sunset, formula)
        if t is None:
            continue
        added.append([f"{m.group(1)}T17:02:00", round(t, 1), round(cc, 1), compute_duration(t, cc, formula), "replay", True])
    if added:
        os.makedirs(os.path.dirname(s.decisions_file), exist_ok=True)
        new = not os.path.exists(s.decisions_file)
        with open(s.decisions_file, "a", newline="") as f:
            w = csv.writer(f)
            if new:
                w.writerow(DECISION_COLUMNS)
            w.writerows(added)
    return added


def main():
    from .settings import Settings
    s = Settings()
    added = backfill(s)
    if added:
        print(f"✅ Added {len(added)} replayed days ({added[0][0][:10]} to {added[-1][0][:10]}) to {s.decisions_file}")
    else:
        print("Nothing to add: every saved day already has an entry (or there are no saved days).")


if __name__ == "__main__":
    main()
