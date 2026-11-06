"""Activity log: one JSON line per event ('auto run failed', 'manual run 60 min', ...).
Read by the dashboard. Separate from the console output on purpose: structured and short."""
import json
import os
import threading
from datetime import datetime

_lock = threading.Lock()
KEEP = 300


def log_event(s, kind, message, ok=True):
    rec = {"time": datetime.now().isoformat(timespec="seconds"), "kind": kind, "ok": bool(ok), "message": message}
    try:
        os.makedirs(s.stored_dir, exist_ok=True)
        with _lock, open(s.activity_file, "a") as f:
            f.write(json.dumps(rec) + "\n")
    except OSError as e:
        print(f"⚠️  Could not write activity log: {e}")


def read_events(s, limit=20):
    """Newest first."""
    try:
        with open(s.activity_file) as f:
            lines = f.read().splitlines()[-limit:]
    except (FileNotFoundError, OSError):
        return []
    out = []
    for ln in lines:
        try:
            out.append(json.loads(ln))
        except ValueError:
            continue
    return list(reversed(out))


def trim(s):
    """Keep the file small: called once at startup."""
    try:
        with _lock, open(s.activity_file) as f:
            lines = f.read().splitlines()
        if len(lines) > KEEP * 2:
            with _lock, open(s.activity_file, "w") as f:
                f.write("\n".join(lines[-KEEP:]) + "\n")
    except (FileNotFoundError, OSError):
        pass
