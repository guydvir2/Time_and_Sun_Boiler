"""Read-only dashboard: one page + one JSON endpoint. Standard library only.

Runs as a thread inside the service (so one process), or alone:  python -m boiler.web
It never exposes secrets (no tokens/passwords), and it cannot change anything.
"""
import csv
import json
import os
import threading
from datetime import datetime, date, time, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import APP_VERSION
from .formula import Formula, explain
from .state import load_last_update, load_attempt


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
                "driver": r.get("driver", ""), "ok": r.get("ok", "").strip().lower() == "true"}
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
    hh, mm = (int(x) for x in s.update_after.split(":"))
    due = now.time() >= time(hh, mm)
    today = str(now.date())
    done_today = last_update == today
    stale_days = None
    if last_update:
        try:
            stale_days = (now.date() - date.fromisoformat(last_update)).days
        except ValueError:
            pass
    failed_today = bool(attempt) and not attempt.get("ok") and str(attempt.get("time", ""))[:10] == today

    if done_today:
        return "green", "Today's run is done" + (f": {attempt['message']}" if attempt and attempt.get("ok") else "")
    if failed_today:
        return "red", f"Today's run failed: {attempt.get('message', '')}"
    if stale_days is not None and stale_days >= 2:
        return "red", f"No successful run since {last_update}"
    if due:
        return "yellow", "Due now: will run at the next check"
    if last_update is None:
        return "yellow", f"No run yet. First run after {s.update_after}"
    return "green", f"Waiting. Next run after {s.update_after}"


def build_status(s, driver_names, formula=None, now=None):
    formula = formula or Formula()
    now = now or datetime.now()
    today = str(now.date())
    last_update = load_last_update(s)
    attempt = load_attempt(s)
    level, text = health(s, now, last_update, attempt)
    hist = _history(s)
    todays = hist[-1] if hist and hist[-1]["date"] == today else None
    done_today = last_update == today

    hh, mm = (int(x) for x in s.update_after.split(":"))
    if done_today:
        nxt = f"{now.date() + timedelta(days=1)} {s.update_after}"
    elif now.time() >= time(hh, mm):
        nxt = "next check"
    else:
        nxt = f"{today} {s.update_after}"

    base = None
    if todays:
        base = explain(todays["temp"], todays["clouds"], formula)
        base["result_min"] = todays["minutes"]   # the recorded value wins (inputs here are rounded)

    return {
        "version": APP_VERSION,
        "now": now.isoformat(timespec="seconds"),
        "health": {"level": level, "text": text},
        "today": todays, "base": base,
        "last": hist[-1] if hist else None,
        "next_run": nxt, "update_after": s.update_after, "check_every_min": s.sleep_minutes,
        "drivers": list(driver_names),
        "history": hist,
        "weather": _today_weather(s, today),
        "formula": {"points": sorted([float(k), v] for k, v in formula.temp_lut.items()),
                    "cloud_k": formula.cloud_k, "max_on": formula.max_on},
    }


# ----------------------------------------------------------------------------- server

class _Handler(BaseHTTPRequestHandler):
    ctx = None   # (settings, driver_names, formula), set by make_server

    def _send(self, code, body, ctype):
        data = body if isinstance(body, bytes) else body.encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        path = self.path.split("?")[0]
        if path in ("/", "/index.html"):
            self._send(200, PAGE, "text/html; charset=utf-8")
        elif path == "/api/status":
            try:
                body = json.dumps(build_status(*self.ctx))
            except Exception as e:                      # never crash the thread on bad data
                self._send(500, json.dumps({"error": str(e)}), "application/json")
                return
            self._send(200, body, "application/json")
        elif path == "/healthz":
            self._send(200, "ok", "text/plain")
        else:
            self._send(404, "not found", "text/plain")

    def log_message(self, *args):                       # keep the service log clean
        pass


def make_server(s, driver_names, formula=None):
    handler = type("Handler", (_Handler,), {"ctx": (s, list(driver_names), formula or Formula())})
    srv = ThreadingHTTPServer((s.dashboard_host, s.dashboard_port), handler)
    srv.daemon_threads = True
    return srv


def start_in_thread(s, driver_names, formula=None):
    srv = make_server(s, driver_names, formula)
    threading.Thread(target=srv.serve_forever, name="dashboard", daemon=True).start()
    shown = "localhost" if s.dashboard_host in ("0.0.0.0", "") else s.dashboard_host
    print(f"🖥️  Dashboard: http://{shown}:{s.dashboard_port}  (read-only)")
    return srv


def main():
    """View-only mode: python -m boiler.web  (does not run the boiler job)."""
    from .settings import Settings
    s = Settings()
    srv = make_server(s, s.driver_names)
    shown = "localhost" if s.dashboard_host in ("0.0.0.0", "") else s.dashboard_host
    print(f"🖥️  Dashboard: http://{shown}:{s.dashboard_port}  (Ctrl+C to stop)")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


# ----------------------------------------------------------------------------- page

PAGE = r"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Boiler</title>
<style>
:root{--bg:#f6f7f9;--card:#fff;--fg:#1c2330;--mute:#6b7585;--line:#e3e7ed;--green:#1a9850;--yellow:#c99700;--red:#d73027;--bar:#3b7ddd}
@media (prefers-color-scheme:dark){:root{--bg:#10141b;--card:#1a212c;--fg:#e8ecf2;--mute:#93a0b3;--line:#2a3342;--green:#3fbf74;--yellow:#e3b53a;--red:#ff6a5f;--bar:#6aa3f0}}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--fg);font:16px/1.4 system-ui,-apple-system,Segoe UI,Roboto,sans-serif}
main{max-width:640px;margin:0 auto;padding:16px}
.top{display:flex;justify-content:space-between;align-items:baseline;color:var(--mute);font-size:13px;margin-bottom:10px}
.top b{color:var(--fg);font-size:18px}
.card{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:16px;margin-bottom:12px}
.status{display:flex;gap:12px;align-items:center;border-left:8px solid var(--yellow)}
.status.green{border-left-color:var(--green)}.status.yellow{border-left-color:var(--yellow)}.status.red{border-left-color:var(--red)}
.status .dot{font-size:26px}.status .t{font-size:17px;font-weight:600}
.hero{text-align:center}
.hero .n{font-size:64px;font-weight:700;line-height:1}
.hero .n small{font-size:22px;color:var(--mute);font-weight:500}
.hero .s{color:var(--mute);margin-top:6px}
h2{font-size:13px;text-transform:uppercase;letter-spacing:.06em;color:var(--mute);margin:0 0 8px}
svg{width:100%;height:auto;display:block}
.axis{stroke:var(--line);stroke-width:1}.lbl{fill:var(--mute);font-size:10px}
.bar{fill:var(--bar)}.bar.bad{fill:var(--red)}
details summary{cursor:pointer;color:var(--mute);font-size:14px}
.kv{display:grid;grid-template-columns:auto 1fr;gap:4px 14px;margin:10px 0;font-size:14px}.kv span:nth-child(odd){color:var(--mute)}
.empty{color:var(--mute);text-align:center;padding:18px 0}
</style></head><body><main>
<div class="top"><b>Boiler</b><span id="meta"></span></div>
<noscript>This page needs JavaScript.</noscript>

<div id="status" class="card status"><span class="dot">⏳</span><span class="t">Loading…</span></div>

<div class="card hero" id="hero"></div>

<div class="card"><h2>Last 30 days · minutes</h2><div id="hist"></div></div>

<div class="card"><details><summary>Details</summary>
  <div class="kv" id="kv"></div>
  <h2 style="margin-top:14px">Today's weather</h2><div id="wx"></div>
  <h2 style="margin-top:14px">Formula · temperature → minutes</h2><div id="fx"></div>
</details></div>
</main>
<script>
const $ = id => document.getElementById(id);
const NS = "http://www.w3.org/2000/svg";
const el = (tag, attrs = {}, text) => { const e = document.createElementNS(NS, tag);
  for (const k in attrs) e.setAttribute(k, attrs[k]); if (text !== undefined) e.textContent = text; return e; };
const h = (tag, cls, text) => { const e = document.createElement(tag); if (cls) e.className = cls;
  if (text !== undefined) e.textContent = text; return e; };
const DOT = {green: "🟢", yellow: "🟡", red: "🔴"};

function renderStatus(d) {
  const s = $("status"); s.className = "card status " + d.health.level;
  s.replaceChildren(h("span", "dot", DOT[d.health.level]), h("span", "t", d.health.text));
}

function renderHero(d) {
  const box = $("hero"); box.replaceChildren();
  const t = d.today, shown = t || d.last;
  if (!shown) { box.append(h("div", "empty", "No runs recorded yet")); return; }
  const n = h("div", "n"); n.append(document.createTextNode(shown.minutes), h("small", "", " min")); box.append(n);
  const via = shown.driver ? ` · via ${shown.driver}` : "";
  box.append(h("div", "s", `${shown.temp.toFixed(1)}°C · ${shown.clouds.toFixed(0)}% clouds${via}`));
  if (!t) box.append(h("div", "s", `Last run: ${shown.date}. Nothing yet today.`));
  else if (!t.ok) box.append(h("div", "s", "⚠️ Not accepted by the driver"));
}

function renderHistory(d) {
  const box = $("hist"); box.replaceChildren();
  const rows = d.history;
  if (!rows.length) { box.append(h("div", "empty", "History appears after the first run")); return; }
  const W = 600, H = 150, pad = 18, top = d.formula.max_on;
  const maxY = Math.max(top, ...rows.map(r => r.minutes));
  const svg = el("svg", {viewBox: `0 0 ${W} ${H + 18}`, role: "img", "aria-label": "Minutes per day"});
  for (const v of [0, maxY / 2, maxY]) {
    const y = H - (v / maxY) * (H - pad) ; svg.append(el("line", {x1: 0, x2: W, y1: y, y2: y, class: "axis"}));
    svg.append(el("text", {x: 2, y: y - 3, class: "lbl"}, Math.round(v)));
  }
  const slot = (W - 18) / Math.max(rows.length, 30), bw = slot * 0.7;
  rows.forEach((r, i) => {
    const bh = (r.minutes / maxY) * (H - pad), x = i * slot + (slot - bw) / 2 + 14;
    const rect = el("rect", {x: x, y: H - bh, width: bw - 1, height: Math.max(bh, 1), rx: 2, class: "bar" + (r.ok ? "" : " bad")});
    rect.append(el("title", {}, `${r.date}: ${r.minutes} min, ${r.temp.toFixed(1)}°C, ${r.clouds.toFixed(0)}% clouds` + (r.ok ? "" : " (failed)")));
    svg.append(rect);
  });
  svg.append(el("text", {x: 14, y: H + 13, class: "lbl"}, rows[0].date.slice(5)));
  svg.append(el("text", {x: W - 2, y: H + 13, class: "lbl", "text-anchor": "end"}, rows[rows.length - 1].date.slice(5)));
  box.append(svg);
}

function spark(values, label, color) {
  const W = 600, H = 56; const min = Math.min(...values), max = Math.max(...values), span = (max - min) || 1;
  const svg = el("svg", {viewBox: `0 0 ${W} ${H + 14}`});
  const pts = values.map((v, i) => `${(i / Math.max(values.length - 1, 1)) * (W - 8) + 4},${H - ((v - min) / span) * (H - 8) - 2}`).join(" ");
  svg.append(el("polyline", {points: pts, fill: "none", stroke: color, "stroke-width": 2.5, "stroke-linejoin": "round"}));
  svg.append(el("text", {x: 4, y: H + 12, class: "lbl"}, `${label}: ${min.toFixed(0)} – ${max.toFixed(0)}`));
  return svg;
}

function renderWeather(d) {
  const box = $("wx"); box.replaceChildren();
  if (!d.weather.length) { box.append(h("div", "empty", "No weather saved for today yet")); return; }
  const hrs = d.weather;
  box.append(spark(hrs.map(r => r.temp), `Temperature °C, ${hrs[0].hour}:00–${hrs[hrs.length - 1].hour}:00`, "var(--bar)"),
             spark(hrs.map(r => r.clouds), "Clouds %", "var(--mute)"));
}

function renderFormula(d) {
  const box = $("fx"); box.replaceChildren();
  const pts = d.formula.points; if (!pts.length) return;
  const W = 600, H = 150, x0 = 30, maxY = d.formula.max_on;
  const tmin = pts[0][0], tmax = pts[pts.length - 1][0];
  const X = t => x0 + ((t - tmin) / ((tmax - tmin) || 1)) * (W - x0 - 10), Y = v => H - (v / maxY) * (H - 14);
  const svg = el("svg", {viewBox: `0 0 ${W} ${H + 18}`});
  svg.append(el("line", {x1: x0, x2: W, y1: H, y2: H, class: "axis"}));
  svg.append(el("polyline", {points: pts.map(p => `${X(p[0])},${Y(p[1])}`).join(" "), fill: "none", stroke: "var(--bar)", "stroke-width": 2.5}));
  svg.append(el("text", {x: x0, y: H + 13, class: "lbl"}, `${tmin}°C`));
  svg.append(el("text", {x: W - 10, y: H + 13, class: "lbl", "text-anchor": "end"}, `${tmax}°C`));
  svg.append(el("text", {x: 2, y: 6, class: "lbl"}, maxY + " min"));
  if (d.today) {
    const cx = Math.min(Math.max(X(d.today.temp), x0), W - 10), cy = Y(d.today.minutes);
    svg.append(el("circle", {cx: cx, cy: cy, r: 5, fill: "var(--red)"}));
    svg.append(el("text", {x: Math.min(cx + 8, W - 60), y: cy - 6, class: "lbl"}, "today"));
  }
  box.append(svg, h("div", "s", `Line = temperature only. The dot sits above it when clouds add time (cloud factor ${d.formula.cloud_k}).`));
  box.lastChild.style.cssText = "color:var(--mute);font-size:12px;margin-top:4px";
}

function renderDetails(d) {
  const kv = $("kv"); kv.replaceChildren();
  const rows = [["Drivers", d.drivers.join(" → ") || "none"], ["Next run", d.next_run],
                ["Checks every", d.check_every_min + " min"], ["Version", d.version]];
  if (d.base) rows.splice(1, 0, ["Today's math", `${d.base.base_min} min base, clouds +${(d.base.cloud_alpha * 100).toFixed(0)}% → ${d.base.result_min} min`]);
  for (const [k, v] of rows) kv.append(h("span", "", k), h("span", "", String(v)));
}

async function refresh() {
  try {
    const r = await fetch("/api/status", {cache: "no-store"}); if (!r.ok) throw new Error(r.status);
    const d = await r.json();
    renderStatus(d); renderHero(d); renderHistory(d); renderWeather(d); renderFormula(d); renderDetails(d);
    $("meta").textContent = "updated " + d.now.slice(11, 16);
  } catch (e) {
    $("status").className = "card status red";
    $("status").replaceChildren(h("span", "dot", "🔴"), h("span", "t", "Can't reach the boiler service"));
  }
}
refresh(); setInterval(refresh, 60000);
</script></body></html>
"""


if __name__ == "__main__":
    main()
