"""Small persistent bits: 'was today done?', the latest attempt, 'boiler running until', and the
log of every decision. The service and the dashboard share this process, so writes are locked."""
import os
import csv
import json
import shutil
import threading
from datetime import datetime, timedelta

_lock = threading.RLock()


def _load_json(path):
    with open(path) as f:
        d = json.load(f)
    if not isinstance(d, dict):
        raise ValueError("not an object")
    return d


def _read_state(s):
    """state.json, or its last good copy (.bak) if the main file is damaged (e.g. power cut mid-write)."""
    try:
        return _load_json(s.state_file)
    except FileNotFoundError:
        return {}
    except (ValueError, OSError):
        try:
            d = _load_json(s.state_file + ".bak")
            print("⚠️  state.json was damaged: using the last good copy")
            return d
        except (ValueError, OSError):
            print("⚠️  state.json was damaged and there is no good copy: starting empty")
            return {}


def _write_state(s, data):
    """Write to a temp file, then swap it in: a crash can never leave a half-written state.json."""
    tmp = s.state_file + ".tmp"
    try:
        with open(tmp, "w") as f:
            json.dump(data, f)
            f.flush()
            os.fsync(f.fileno())
        try:
            _load_json(s.state_file)                                   # only a good file becomes the backup
            shutil.copyfile(s.state_file, s.state_file + ".bak")
        except (OSError, ValueError):
            pass
        os.replace(tmp, s.state_file)
    except OSError as e:
        print(f"⚠️  Could not save state: {e}")


def _update(s, **changes):
    with _lock:
        data = _read_state(s)
        for k, v in changes.items():
            if v is None:
                data.pop(k, None)
            else:
                data[k] = v
        _write_state(s, data)


def load_last_update(s):
    """Date string (YYYY-MM-DD) of the last successful update, or None."""
    return _read_state(s).get("last_update_date")


def save_last_update(s, day):
    _update(s, last_update_date=str(day))


def save_attempt(s, ok, message):
    """Remember the latest attempt (success or failure) so the dashboard can show problems."""
    _update(s, last_attempt={"time": datetime.now().isoformat(timespec="seconds"),
                             "ok": bool(ok), "message": message})


def load_attempt(s):
    return _read_state(s).get("last_attempt")


def save_running(s, minutes, source):
    """The boiler was told to run `minutes` from now. (The device times itself out; this is for display.)"""
    until = datetime.now() + timedelta(minutes=minutes)
    _update(s, running={"until": until.isoformat(timespec="seconds"), "minutes": minutes, "source": source})


def clear_running(s):
    _update(s, running=None)


def load_running(s, now=None):
    """{'until','minutes','source'} while the run is still in progress, else None."""
    r = _read_state(s).get("running")
    if not r:
        return None
    try:
        if datetime.fromisoformat(r["until"]) > (now or datetime.now()):
            return r
    except (KeyError, ValueError):
        pass
    return None


def save_heartbeat(s):
    """The service loop is alive (written every loop)."""
    _update(s, heartbeat=datetime.now().isoformat(timespec="seconds"))


def load_heartbeat(s):
    return _read_state(s).get("heartbeat")


def auto_enabled(s):
    """Automatic daily runs on/off (holiday mode). Default on."""
    return _read_state(s).get("auto", {}).get("enabled", True)


def set_auto(s, enabled):
    _update(s, auto={"enabled": bool(enabled), "since": datetime.now().isoformat(timespec="seconds")})


def load_auto(s):
    return dict({"enabled": True, "since": None}, **_read_state(s).get("auto", {}))


def save_plan(s, plan):
    """Today's sun-based run: {date, minutes, start_at, ready_at, status: planned|started|failed|skipped, ...}"""
    _update(s, plan=plan)


def load_plan(s):
    return _read_state(s).get("plan")


def save_later(s, at, minutes):
    """One pending 'extra hot water' run for later today: {at: ISO minute, minutes}."""
    _update(s, later={"at": at.isoformat(timespec="minutes"), "minutes": int(minutes)})


def clear_later(s):
    _update(s, later=None)


def load_later(s):
    return _read_state(s).get("later")


WX_KEEP = 12


def record_weather(s, kind, ok):
    """Remember the outcome of every weather / sun-time fetch (last WX_KEEP), for the confidence meter."""
    with _lock:
        d = _read_state(s)
        log = d.get("wx_log", [])[-(WX_KEEP - 1):]
        log.append({"t": datetime.now().isoformat(timespec="seconds"), "kind": kind, "ok": bool(ok)})
        d["wx_log"] = log
        if ok:
            d["wx_last_ok"] = log[-1]["t"]
        _write_state(s, d)


def load_weather_log(s):
    d = _read_state(s)
    return d.get("wx_log", []), d.get("wx_last_ok")


def preset_done(s):
    return _read_state(s).get("preset_done", {})


def mark_preset_done(s, pid, day):
    with _lock:
        d = _read_state(s)
        d.setdefault("preset_done", {})[pid] = str(day)
        _write_state(s, d)


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
