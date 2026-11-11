"""Dashboard: one page (page.html) + a small JSON API. Standard library only.

Runs as a thread inside the service (so one process), or alone:  python dashboard.py  (view-only)
Reading and operating the boiler (extra hot water, Run / Stop, heat-now) are open on your network.
Admin actions (settings, schedule, formula, pause) need the dashboard password (env DASHBOARD_PASSWORD);
with no password set, they are off.
The API never exposes secrets.
"""
import csv
import hmac
import json
import os
import secrets
import threading
import time as _time
from datetime import datetime, date, time, timedelta
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import APP_VERSION
from .formula import Formula, explain, load_formula, compute_duration, base_minutes
from .state import (mark_pulse_check, load_last_update, load_attempt, load_running, load_auto, set_auto, load_plan,
                    record_weather, save_later, clear_later, load_later)
from . import confidence
from .activity import log_event, read_events
from . import power as power_mod
from .drivers import start_boiler, REGISTRY
from . import runs, schedule, missed as missed_mod

PAGE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "page.html")
STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
TUNE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tune.html")
COOKIE = "boiler_session"
# Operating the boiler needs no password (anyone on the network can use the buttons); everything else is admin.
OPEN_PATHS = ("/api/run", "/api/stop", "/api/later/cancel", "/api/missed")
CSRF_HEADER = ("X-Requested-With", "boiler-ui")      # a cross-site page cannot send this without our consent
FORECAST_TTL = 1800                                  # seconds
SERVICES_TTL = 60                                    # seconds between live link checks


# ----------------------------------------------------------------------------- data

def _read_csv(path):
    try:
        with open(path, newline="") as f:
            return list(csv.DictReader(f))
    except (FileNotFoundError, OSError):
        return []


def _history(s, days=30):
    """One entry per day (the last decision of that day), newest last."""
    per_day = {}
    for r in _read_csv(s.decisions_file):
        try:
            per_day[r["timestamp"][:10]] = {
                "date": r["timestamp"][:10], "minutes": int(float(r["duration_min"])),
                "temp": float(r["mean_temp"]), "clouds": float(r["mean_clouds"]),
                "driver": r.get("driver", ""), "ok": r.get("ok", "").strip().lower() == "true",
                "replay": r.get("driver", "") == "replay"}
        except (KeyError, ValueError):
            continue
    return [per_day[d] for d in sorted(per_day)][-days:]


def _today_weather(s, today):
    rows = []
    for r in _read_csv(os.path.join(s.stored_dir, f"{today}.csv")):
        try:
            rows.append({"hour": int(r["time"][11:13]), "temp": float(r["temp"]), "clouds": float(r["clouds"])})
        except (KeyError, ValueError):
            continue
    return rows


def health(s, now, last_update, attempt):
    """(level, text). level is green / yellow / red. Problems win over everything else."""
    today = str(now.date())
    auto = load_auto(s)
    if not auto["enabled"]:
        return "yellow", "Schedule is paused" + (f" (since {auto['since'][5:10]})" if auto.get("since") else "")
    plan = load_plan(s)
    if plan and plan.get("date") == today and "runs" in plan:
        for r in plan["runs"]:
            if r["status"] == "missed":
                return "red", f"Run {r['n']} ({r['start_at'][11:16]}) could not start"
            if r["status"] == "failed":
                return "red", f"Run {r['n']} did not start yet: retrying"
        if plan.get("minutes") is not None:
            if plan["status"] == "final":
                return "green", f"Final for today: {plan['minutes']} min"
            if attempt and not attempt.get("ok") and str(attempt.get("time", ""))[:10] == today:
                return "yellow", f"Weather unavailable: keeping the last estimate ({plan['minutes']} min)"
            return "green", f"Estimate {plan['minutes']} min, final after {plan['lock_at'][11:16]}"
    if attempt and not attempt.get("ok") and str(attempt.get("time", ""))[:10] == today:
        return "red", attempt.get("message", "Weather unavailable")
    return "yellow", "No estimate yet today"


def build_status(s, driver_names, formula=None, now=None, power=None):
    formula = formula or Formula()
    now = now or datetime.now()
    today = str(now.date())
    last_update = load_last_update(s)
    attempt = load_attempt(s)
    level, text = health(s, now, last_update, attempt)
    hist = _history(s)
    todays = hist[-1] if hist and hist[-1]["date"] == today else None
    done_today = last_update == today


    base = None
    if todays:
        base = explain(todays["temp"], todays["clouds"], formula)
        base["result_min"] = todays["minutes"]   # the recorded value wins (inputs here are rounded)

    return {
        "version": APP_VERSION,
        "now": now.isoformat(timespec="seconds"),
        "health": {"level": level, "text": text},
        "running": load_running(s, now),
        "auto": load_auto(s),
        "plan": load_plan(s),
        "totals": power_mod.apply_measured(s, runs.totals(s, now), now, power) if s.power_enabled else runs.totals(s, now),
        "missed": missed_mod.load(s, now),
        "power": {"live": power.snapshot(now) if power else None, **power_mod.totals(s, now, monitor=power)}
                 if s.power_enabled else None,
        "upcoming": schedule.upcoming(schedule.load(s), now, load_plan(s)),
        "ready_by": schedule.load(s)["ready_by"],
        "segments": runs.segments(s, today, now),
        "today": todays, "base": base,
        "last": hist[-1] if hist else None,
        "check_every_min": s.sleep_minutes, "run1_pct": s.run1_pct, "run2_at": s.run2_at, "min_run": s.min_run,
        "drivers": list(driver_names),
        "history": hist,
        "weather": _today_weather(s, today),
        "activity": read_events(s, 15),
        "confidence": confidence.score(s, now),
        "later": load_later(s),
        "formula": {"points": sorted([float(k), v] for k, v in formula.temp_lut.items()),
                    "cloud_k": formula.cloud_k, "max_on": formula.max_on, "full": formula.to_dict()},
    }


# ----------------------------------------------------------------------------- auth

class Auth:
    """Password -> random session token (kept in memory, 12 h). Failed logins are rate-limited."""
    TTL = 12 * 3600
    MAX_FAILS, WINDOW = 5, 60

    def __init__(self, password):
        self._pw = password if callable(password) else (lambda: password or "")   # callable: follows Settings
        self._last = self._pw() or ""
        self.sessions = {}
        self.fails = []
        self.lock = threading.Lock()

    @property
    def password(self):
        pw = self._pw() or ""
        if pw != self._last:                  # password changed: everyone has to log in again
            self._last, self.sessions = pw, {}
        return pw

    def grant(self):
        """A session without a password check (right after the first password was created)."""
        self.password                             # sync to the new password first (else it clears this session)
        with self.lock:
            token = secrets.token_urlsafe(32)
            self.sessions[token] = _time.time() + self.TTL
            return token

    @property
    def enabled(self):
        return bool(self.password)

    def login(self, supplied):
        """Returns (token or None, http_status, message)."""
        with self.lock:
            now = _time.time()
            self.fails = [t for t in self.fails if now - t < self.WINDOW]
            if len(self.fails) >= self.MAX_FAILS:
                return None, 429, "Too many wrong attempts. Wait a minute."
            if not self.password or not hmac.compare_digest(str(supplied).encode(), self.password.encode()):
                self.fails.append(now)
                return None, 401, "Wrong password"
            token = secrets.token_urlsafe(32)
            self.sessions[token] = now + self.TTL
            return token, 200, "ok"

    def valid(self, token):
        self.password                             # clears sessions if the password changed
        with self.lock:
            now = _time.time()
            self.sessions = {t: e for t, e in self.sessions.items() if e > now}
            return bool(token) and token in self.sessions

    def logout(self, token):
        with self.lock:
            self.sessions.pop(token, None)


class Ctx:
    def __init__(self, s, drivers, formula, app=None):
        self.app = app                                  # live settings + drivers (see app.py), or fixed ones
        self._s, self._drivers = s, list(drivers)
        self.auth = Auth(lambda: self.s.dashboard_password)
        self.action_lock = threading.Lock()          # one switch command at a time
        self.fc_lock = threading.Lock()
        self.fc = {"t": 0.0, "value": None}
        self.sv_lock = threading.Lock()
        self.sv = {"t": 0.0, "value": None}

    @property
    def s(self):
        if self.app:
            self.app.maybe_reload()
            return self.app.s
        return self._s

    @property
    def drivers(self):
        return list(self.app.drivers) if self.app else self._drivers

    def settings_changed(self):
        """Called after a save: reload now and drop cached checks."""
        if self.app:
            self.app.reload()
        self.fc["t"] = self.sv["t"] = 0.0

    def services(self):
        from .services import check_all
        with self.sv_lock:                            # one check round at a time, cached for a minute
            if self.sv["value"] is None or _time.time() - self.sv["t"] > SERVICES_TTL:
                self.sv = {"t": _time.time(), "value": check_all(self.s, self.forecast(), self.app.power if self.app else None)}
            return self.sv["value"]

    @property
    def formula(self):
        """Always the saved one (stored_data/formula.json), so /tune changes show immediately."""
        f = load_formula(self.s)
        return f if not f.blocking_problems() else Formula()

    @property
    def names(self):
        return [d.name for d in self.drivers]

    def forecast(self):
        from .weather import forecast_prediction
        if self.s.latitude is None:
            return {"error": "Set your location in Settings"}
        with self.fc_lock:
            if self.fc["value"] is not None and _time.time() - self.fc["t"] < FORECAST_TTL:
                return self.fc["value"]
            try:
                self.fc = {"t": _time.time(), "value": forecast_prediction(self.s, self.formula)}
                record_weather(self.s, "forecast", True)
                return self.fc["value"]
            except Exception as e:
                record_weather(self.s, "forecast", False)
                if self.fc["value"] is not None:     # serve the last good one, marked stale
                    return dict(self.fc["value"], stale=True)
                return {"error": f"Forecast unavailable ({type(e).__name__})"}


# ----------------------------------------------------------------------------- server

class _Handler(BaseHTTPRequestHandler):
    ctx = None   # set by make_server

    # -- helpers
    def _send(self, code, body, ctype="application/json", headers=None):
        data = body if isinstance(body, bytes) else body.encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        if "Cache-Control" not in (headers or {}):
            self.send_header("Cache-Control", "no-store")
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(data)

    def _json(self, code, obj, headers=None):
        self._send(code, json.dumps(obj), "application/json", headers)

    def _token(self):
        try:
            c = SimpleCookie(self.headers.get("Cookie", ""))
            return c[COOKIE].value if COOKIE in c else ""
        except Exception:
            return ""

    def _controls(self):
        c = self.ctx
        return {"available": bool(c.drivers), "password_set": c.auth.enabled, "needs_setup": c.s.needs_setup,
                "logged_in": c.auth.valid(self._token()), "max_minutes": c.s.max_manual_minutes,
                "can_stop": any(d.supports_stop for d in c.drivers)}

    # -- GET
    def do_GET(self):
        path = self.path.split("?")[0]
        c = self.ctx
        if path in ("/", "/index.html", "/tune"):
            with open(TUNE_FILE if path == "/tune" else PAGE_FILE, "rb") as f:
                self._send(200, f.read(), "text/html; charset=utf-8")
        elif path.startswith("/static/"):
            name = os.path.basename(path)                # no sub-folders, no "..": just a file name
            fp = os.path.join(STATIC_DIR, name)
            if not name.endswith(".woff2") or not os.path.isfile(fp):
                return self._send(404, "not found", "text/plain")
            with open(fp, "rb") as f:
                self._send(200, f.read(), "font/woff2", {"Cache-Control": "max-age=604800"})
        elif path == "/api/tune":
            try:
                self._json(200, self._tune_data())
            except Exception as e:
                self._json(500, {"error": str(e)})
        elif path == "/api/status":
            try:
                st = build_status(c.s, c.names, c.formula, power=c.app.power if c.app else None)
                st["controls"] = self._controls()
                self._json(200, st)
            except Exception as e:                      # never crash the thread on bad data
                self._json(500, {"error": str(e)})
        elif path == "/api/services":
            try:
                self._json(200, {"services": c.services(), "checked": datetime.now().isoformat(timespec="seconds")})
            except Exception as e:
                self._json(500, {"error": str(e)})
        elif path == "/api/settings":
            if c.auth.enabled and not c.auth.valid(self._token()):
                return self._json(401, {"ok": False, "message": "Locked. Enter the password first."})
            from . import setup
            self._json(200, dict(setup.values(c.s), controls=self._controls()))
        elif path == "/api/geocode":
            from urllib.parse import urlparse, parse_qs
            from . import setup
            try:
                q = parse_qs(urlparse(self.path).query).get("q", [""])[0]
                self._json(200, {"results": setup.geocode(q)})
            except Exception as e:
                self._json(502, {"results": [], "error": f"City search unavailable ({type(e).__name__})"})
        elif path == "/api/schedule":
            self._json(200, {"schedule": schedule.load(c.s), "max_minutes": c.s.max_manual_minutes,
                             "run1_pct": c.s.run1_pct, "run2_at": c.s.run2_at, "controls": self._controls()})
        elif path == "/api/forecast":
            self._json(200, c.forecast())
        elif path == "/healthz":
            self._send(200, "ok", "text/plain")
        else:
            self._send(404, "not found", "text/plain")

    # -- POST (switching things): CSRF header + password session
    def do_POST(self):
        path = self.path.split("?")[0]
        c = self.ctx
        if self.headers.get(CSRF_HEADER[0]) != CSRF_HEADER[1]:
            return self._json(403, {"ok": False, "message": "Forbidden"})
        try:
            n = int(self.headers.get("Content-Length", "0"))
            body = json.loads(self.rfile.read(min(n, 16384)) or b"{}") if n > 0 else {}
            if not isinstance(body, dict):
                body = {}
        except (ValueError, OSError):
            return self._json(400, {"ok": False, "message": "Bad request"})

        if path == "/api/login":
            if not c.auth.enabled:
                return self._json(403, {"ok": False, "message": "Controls are off: no dashboard password is set"})
            token, code, msg = c.auth.login(body.get("password", ""))
            if token is None:
                return self._json(code, {"ok": False, "message": msg})
            cookie = f"{COOKIE}={token}; HttpOnly; SameSite=Strict; Path=/; Max-Age={Auth.TTL}"
            return self._json(200, {"ok": True, "message": "Unlocked"}, {"Set-Cookie": cookie})

        if path == "/api/logout":
            c.auth.logout(self._token())
            return self._json(200, {"ok": True, "message": "Locked"},
                              {"Set-Cookie": f"{COOKIE}=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0"})

        if path == "/api/setup/password":             # first run only: create the dashboard password
            return self._first_password(body)
        if path == "/api/tune/preview":               # read-only what-if: no password needed
            return self._preview(body)
        if path == "/api/sunshine":                   # fetch public data into the cache: no password needed
            return self._sunshine()

        if path not in ("/api/run", "/api/stop", "/api/auto", "/api/later/cancel", "/api/formula", "/api/schedule",
                        "/api/settings", "/api/settings/test", "/api/missed"):
            return self._json(404, {"ok": False, "message": "Not found"})
        if path not in OPEN_PATHS:                    # admin actions: dashboard password required
            if not c.auth.enabled:
                return self._json(403, {"ok": False, "message": "Controls are off: no dashboard password is set"})
            if not c.auth.valid(self._token()):
                return self._json(401, {"ok": False, "message": "Locked. Enter the password first."})
        if path == "/api/auto":                       # pause/resume needs no driver
            return self._auto(body)
        if path == "/api/later/cancel":
            later = load_later(c.s)
            clear_later(c.s)
            if later:
                log_event(c.s, "manual", f"Cancelled extra {later['minutes']} min at {later['at'][11:16]}", True)
            return self._json(200, {"ok": True, "message": "Cancelled"})
        if path == "/api/formula":
            return self._save_formula(body)
        if path == "/api/schedule":
            return self._save_schedule(body)
        if path == "/api/settings":
            return self._save_settings(body)
        if path == "/api/settings/test":
            return self._test_settings(body)
        if not c.drivers:
            return self._json(503, {"ok": False, "message": "View-only mode: no driver is attached"})
        # short wait: the previous request may have answered but not yet released the lock
        if not c.action_lock.acquire(timeout=0.5):
            return self._json(409, {"ok": False, "message": "Another command is still in progress"})
        try:
            if path == "/api/missed":
                ok, msg = missed_mod.answer(c.s, c.drivers, str(body.get("id", "")), body.get("action"))
                self._json(200 if ok else 409, {"ok": ok, "message": msg})
            elif path == "/api/run":
                self._run(body)
            else:
                self._stop()
        finally:
            c.action_lock.release()

    def _run(self, body):
        c, s = self.ctx, self.ctx.s
        m = body.get("minutes")
        if isinstance(m, bool) or not isinstance(m, (int, float)) or int(m) != m or not 1 <= int(m) <= s.max_manual_minutes:
            return self._json(400, {"ok": False, "message": f"Minutes must be a whole number from 1 to {s.max_manual_minutes}"})
        m = int(m)
        at = body.get("at")
        if at:                                          # "HH:MM" later today: the engine starts it
            try:
                hh, mm = (int(x) for x in str(at).split(":"))
                when = datetime.combine(date.today(), time(hh, mm))
            except ValueError:
                return self._json(400, {"ok": False, "message": "Start time must be HH:MM"})
            if when <= datetime.now():
                return self._json(400, {"ok": False, "message": "Start time has already passed"})
            save_later(s, when, m)
            log_event(s, "manual", f"Extra {m} min set for {when:%H:%M}", True)
            return self._json(200, {"ok": True, "message": f"Set: {m} min at {when:%H:%M}"})
        ok, used = start_boiler(c.drivers, m, s)
        if ok:
            runs.record_start(s, m, "manual")
            log_event(s, "manual", f"Manual run {m} min via {used}", True)
            return self._json(200, {"ok": True, "message": f"Running for {m} min (via {used})"})
        log_event(s, "manual", f"Manual run {m} min FAILED: no driver accepted it", False)
        return self._json(502, {"ok": False, "message": "No driver accepted the run. Check the connection."})

    # -- formula tuning
    def _tune_data(self):
        from . import tune, sunshine
        s, f = self.ctx.s, self.ctx.formula
        sun = sunshine.load(s)
        days = tune.replay(s, f, sun)
        return {"saved": f.to_dict(), "default": Formula().to_dict(), "days": days,
                "sunshine": sunshine.verdict(days), "controls": self._controls()}

    def _preview(self, body):
        from . import tune
        from .formula import parse_formula
        try:
            f = parse_formula(body.get("formula"))
        except (ValueError, TypeError) as e:
            return self._json(400, {"ok": False, "message": str(e)})
        s = self.ctx.s
        current, new = tune.replay(s, self.ctx.formula), tune.replay(s, f)
        fc = self.ctx.forecast()
        ahead = [{"date": d["date"], "minutes": compute_duration(d["mean_temp"], d["mean_clouds"], f),
                  "now": compute_duration(d["mean_temp"], d["mean_clouds"], self.ctx.formula),
                  "base": round(base_minutes(d["mean_temp"], f)), "temp": d["mean_temp"], "clouds": d["mean_clouds"]}
                 for d in (fc.get("days") or [])]
        return self._json(200, {"ok": True, "days": [{"date": r["date"], "minutes": r["minutes"]} for r in new],
                                "forecast": ahead,
                                "summary": tune.compare(current, new), "problems": f.validate(),
                                "blocking": f.blocking_problems()})

    def _save_formula(self, body):
        from .formula import parse_formula, save_formula
        try:
            f = parse_formula(body.get("formula"))
        except (ValueError, TypeError) as e:
            return self._json(400, {"ok": False, "message": str(e)})
        bad = f.blocking_problems()
        if bad:
            return self._json(400, {"ok": False, "message": "Not saved: " + "; ".join(bad)})
        s = self.ctx.s
        try:
            save_formula(s, f)
        except OSError as e:
            return self._json(500, {"ok": False, "message": f"Could not save: {e}"})
        self.ctx.fc["t"] = 0.0                         # forecast "expected run" uses the new formula
        log_event(s, "manual", "Formula changed on /tune (applies from the next estimate)", True)
        return self._json(200, {"ok": True, "message": "Saved. Applies from the next estimate (within the hour)."})

    # -- settings + setup
    def _first_password(self, body):
        return self._json(403, {"ok": False, "message": "The dashboard password is set with DASHBOARD_PASSWORD in "
                                                        "the server's environment file, not here."})

    def _save_settings(self, body):
        from . import setup
        c = self.ctx
        try:
            cleaned = setup.clean(body.get("changes"))
            if not cleaned:
                return self._json(400, {"ok": False, "message": "Nothing to save"})
            setup.save(c.s, cleaned)
        except ValueError as e:
            return self._json(400, {"ok": False, "message": str(e)})
        except OSError as e:
            return self._json(500, {"ok": False, "message": f"Could not save: {e}"})
        c.settings_changed()
        names = ", ".join(sorted({k.split(".")[0] for k in body["changes"]}))
        log_event(c.s, "manual", f"Settings changed ({names})", True)
        s = c.s
        msg = "Saved" + ("" if not s.needs_setup else ". Setup still needs a location and a way to switch the boiler.")
        return self._json(200, {"ok": True, "message": msg, "needs_setup": s.needs_setup,
                                "problems": c.app.problems if c.app else []})

    def _test_settings(self, body):
        """Run the link checks with the unsaved values (nothing is saved, nothing is switched)."""
        from . import setup
        from .services import _link_rows
        try:
            t = setup.preview_settings(self.ctx.s, setup.clean(body.get("changes") or {}))
        except ValueError as e:
            return self._json(400, {"ok": False, "message": str(e)})
        rows = [r for n in t.driver_names if n in REGISTRY for r in _link_rows(t, n)]
        return self._json(200, {"ok": True, "services": rows})

    def _save_schedule(self, body):
        s = self.ctx.s
        try:
            sched = schedule.validate(body.get("schedule"), s.max_manual_minutes)
        except (ValueError, TypeError) as e:
            return self._json(400, {"ok": False, "message": str(e)})
        from .state import save_plan
        from . import rolling
        plan = load_plan(s)
        if plan and plan.get("date") == str(date.today()) and "runs" in plan:   # re-time today's runs now
            save_plan(s, rolling.sync_runs(plan, s, sched["ready_by"], datetime.now()))
        try:
            schedule.save(s, sched)
        except OSError as e:
            return self._json(500, {"ok": False, "message": f"Could not save: {e}"})
        on = sum(p["enabled"] for p in sched["presets"])
        log_event(s, "manual", f"Schedule saved: ready by {sched['ready_by'] or 'right away'}, {on} preset(s) on", True)
        return self._json(200, {"ok": True, "message": "Schedule saved", "schedule": sched})

    def _sunshine(self):
        from . import tune, sunshine
        s = self.ctx.s
        if not self.ctx.action_lock.acquire(blocking=False):
            return self._json(409, {"ok": False, "message": "Busy, try again"})
        try:
            n, msg = sunshine.fetch_missing(s, [d["date"] for d in tune.saved_days(s)])
            return self._json(200, {"ok": True, "message": msg, "added": n})
        except Exception as e:
            return self._json(502, {"ok": False, "message": f"Could not reach Open-Meteo archive ({type(e).__name__})"})
        finally:
            self.ctx.action_lock.release()

    def _auto(self, body):
        s, en = self.ctx.s, body.get("enabled")
        if not isinstance(en, bool):
            return self._json(400, {"ok": False, "message": "enabled must be true or false"})
        set_auto(s, en)
        self.ctx.sv["t"] = 0.0                         # refresh the lights now
        msg = "Auto-run resumed" if en else "Auto-run paused (manual Run still works)"
        log_event(s, "manual", msg, True)
        return self._json(200, {"ok": True, "message": msg})

    def _stop(self):
        c, s = self.ctx, self.ctx.s
        capable = [d for d in c.drivers if d.supports_stop]
        if not capable:
            return self._json(501, {"ok": False, "message": "None of the active drivers can stop the boiler"})
        for d in capable:
            if d.stop():
                runs.record_stop(s)
                mark_pulse_check(s, datetime.now())          # the engine verifies PulseTime == 0 next tick
                log_event(s, "manual", f"Manual stop via {d.name}", True)
                return self._json(200, {"ok": True, "message": f"Stopped (via {d.name})"})
        log_event(s, "manual", "Manual stop FAILED", False)
        return self._json(502, {"ok": False, "message": "Stop was not confirmed. Check the boiler!"})

    def log_message(self, *args):                       # keep the service log clean
        pass


def make_server(s, drivers, formula=None, app=None):
    handler = type("Handler", (_Handler,), {"ctx": Ctx(s, drivers, formula, app)})
    srv = ThreadingHTTPServer((s.dashboard_host, s.dashboard_port), handler)
    srv.daemon_threads = True
    srv.ctx = handler.ctx
    return srv


def _shown_host(s):
    return "localhost" if s.dashboard_host in ("0.0.0.0", "") else s.dashboard_host


def start_in_thread(s, drivers, formula=None, app=None):
    srv = make_server(s, drivers, formula, app)
    threading.Thread(target=srv.serve_forever, name="dashboard", daemon=True).start()
    mode = ("setup needed: open this page" if s.needs_setup else
            "controls ON (password)" if srv.ctx.auth.enabled else "open the page to create a password")
    print(f"🖥️  Dashboard: http://{_shown_host(s)}:{s.dashboard_port}  ({mode})")
    return srv


def main():
    """View-only mode: python dashboard.py  (no drivers attached, so nothing can be switched)."""
    from .settings import Settings
    from .app import App
    app = App(view_only=True)
    s = app.s
    srv = make_server(s, [], app=app)
    print(f"🖥️  Dashboard: http://{_shown_host(s)}:{s.dashboard_port}  (view-only, Ctrl+C to stop)")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
