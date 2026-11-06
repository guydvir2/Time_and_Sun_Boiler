"""Small persistent bits: 'was today done?' and the log of every decision."""
import os
import csv
import json
from datetime import datetime


def load_last_update(s):
    """Date string (YYYY-MM-DD) of the last successful update, or None."""
    try:
        with open(s.state_file) as f:
            return json.load(f).get("last_update_date")
    except (FileNotFoundError, ValueError):
        return None


def save_last_update(s, day):
    try:
        with open(s.state_file, "w") as f:
            json.dump({"last_update_date": str(day)}, f)
    except OSError as e:
        print(f"⚠️  Could not save state: {e}")


DECISION_COLUMNS = ["timestamp", "mean_temp", "mean_clouds", "duration_min", "driver", "ok"]


def log_decision(s, mean_t, mean_cc, duration, driver, ok):
    """Append inputs + result, so the formula can be reviewed (and backtested) later."""
    try:
        os.makedirs(os.path.dirname(s.decisions_file), exist_ok=True)
        new = not os.path.exists(s.decisions_file)
        with open(s.decisions_file, "a", newline="") as f:
            w = csv.writer(f)
            if new:
                w.writerow(DECISION_COLUMNS)
            w.writerow([datetime.now().isoformat(timespec="seconds"), round(mean_t, 1),
                        round(mean_cc, 1), duration, driver or "", ok])
    except OSError as e:
        print(f"⚠️  Could not write decision log: {e}")
