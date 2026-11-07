"""How much to trust today's numbers: a 0-5 score from recent weather fetches.

Each failure among the last 6 fetches costs a point, old data costs one or two more, and a sun-time
fetch that fell back to the default hours costs one. A score of 2 or less gets a plain-language warning.
"""
from datetime import datetime

from .state import load_weather_log

RECENT = 6


def score(s, now=None):
    now = now or datetime.now()
    log, last_ok = load_weather_log(s)
    recent = log[-RECENT:]
    fails = sum(1 for e in recent if not e["ok"])
    sun_default = any(e["kind"] == "sun" and not e["ok"] and e["t"][:10] == str(now.date()) for e in log)

    age_h = None
    if last_ok:
        try:
            age_h = (now - datetime.fromisoformat(last_ok)).total_seconds() / 3600
        except ValueError:
            pass

    level = 5 - fails
    if age_h is None:
        level -= 2
    elif age_h > 12:
        level -= 2
    elif age_h > 3:
        level -= 1
    if sun_default:
        level -= 1
    level = max(0, min(5, level))

    reasons = []
    if age_h is None:
        reasons.append("no weather data received yet")
    elif age_h > 3:
        reasons.append(f"weather data is {age_h:.0f} h old")
    if fails:
        reasons.append(f"{fails} of the last {len(recent)} updates failed")
    if sun_default:
        reasons.append("sunrise/sunset fell back to default hours")
    warning = None
    if level <= 2 and reasons:
        warning = (reasons[0][0].upper() + reasons[0][1:] + (f" ({', '.join(reasons[1:])})" if reasons[1:] else "")
                   + ". Today's minutes may be off.")
    label = "high" if level >= 4 else "fair" if level == 3 else "low"
    return {"level": level, "label": label, "warning": warning, "fails": fails, "checked": len(recent)}
