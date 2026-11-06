"""Small persistent bits: 'was today done?' and the log of every decision."""
import os
import csv
import json
from datetime import datetime


def load_last_update(s):
    """Date string (YYYY-MM-DD) of the last successful update, or None."""
    return _read_state(s).get("last_update_date")


def _read_state(s):
    try:
        with open(s.state_file) as f:
            return json.load(f)
    except (FileNotFoundError, ValueError):
        return {}


def _write_state(s, data):
    try:
        with open(s.state_file, "w") as f:
            json.dump(data, f)
    except OSError as e:
        print(f"⚠️  Could not save state: {e}")


def save_last_update(s, day):
    data = _read_state(s)
    data["last_update_date"] = str(day)
    _write_state(s, data)


def save_attempt(s, ok, message):
    """Remember the latest attempt (success or failure) so the dashboard can show problems."""
    data = _read_state(s)
    data["last_attempt"] = {"time": datetime.now().isoformat(timespec="seconds"),
                            "ok": bool(ok), "message": message}
    _write_state(s, data)


def load_attempt(s):
    return _read_state(s).get("last_attempt")


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
