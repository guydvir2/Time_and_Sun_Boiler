"""Power sensor: listens to the Tasmota plug's own energy readings (same MQTT topic as the relay).

Step A only LISTENS and LOGS; it never switches anything and never changes a decision.
  * live: watts, relay state, 'heating now'
  * log:  one line per heating session in stored_data/power.jsonl  {start, end, minutes, kwh, peak_w}
Readings arrive two ways: the plug's periodic tele/<topic>/SENSOR, and a Status 8 query we send every
30 s so 'heating now' is never more than half a minute old.
Heating = watts >= [power] on_watts (the element draws ~2 kW; a standby plug draws almost nothing).
"""
import json
import os
import threading
import time as _time
from datetime import datetime, timedelta

import paho.mqtt.client as mqtt

from .drivers.mqtt_tasmota import TasmotaMqttDriver

POLL_SECONDS = 30
STALE_SECONDS = 150          # no reading for this long = sensor offline


def _energy(obj):
    """Find the ENERGY block in a SENSOR / STATUS8 payload. Returns dict or None."""
    if not isinstance(obj, dict):
        return None
    if isinstance(obj.get("ENERGY"), dict):
        return obj["ENERGY"]
    for v in obj.values():
        if isinstance(v, dict):
            e = _energy(v)
            if e:
                return e
    return None


class PowerMonitor:
    def __init__(self, s):
        self.s = s
        self._drv = TasmotaMqttDriver(s)                 # only used to build topics the Tasmota way
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._client = None
        self.connected = False
        self.reading = {"watts": None, "volts": None, "amps": None, "at": None}
        self.relay = None                                # "ON" / "OFF" / None
        self.relay_since = None                          # when WE saw it change (None = state known only from start-up)
        self.drew = False                                # any real draw since the relay last went ON
        self.lwt = None
        self.first_seen = None
        self._sess = None                                # open session: {start, last, last_w, kwh, peak}

    # ---- config -------------------------------------------------------------------------
    @property
    def configured(self):
        return bool(self.s.power_enabled and self.s.mqtt_host and self.s.tasmota_topic)

    # ---- lifecycle ----------------------------------------------------------------------
    def start(self):
        if not self.configured:
            return False
        threading.Thread(target=self._run, name="power-monitor", daemon=True).start()
        return True

    def stop(self):
        self._stop.set()
        c = self._client
        if c:
            try:
                c.loop_stop()
                c.disconnect()
            except Exception:
                pass
        self._close_session(datetime.now())

    def _make_client(self):
        s = self.s
        try:                                             # paho-mqtt >= 2.0
            c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="time-and-sun-boiler-power")
        except AttributeError:                           # paho-mqtt 1.x
            c = mqtt.Client(client_id="time-and-sun-boiler-power")
        if s.mqtt_user:
            c.username_pw_set(s.mqtt_user, s.mqtt_password)
        return c

    def _run(self):
        s, t = self.s, self._drv.topic
        n = s.tasmota_relay
        topics = [(t("tele", "SENSOR"), 0), (t("stat", "STATUS8"), 0), (t("tele", "LWT"), 0),
                  (t("tele", "STATE"), 0), (t("stat", "POWER"), 0), (t("stat", f"POWER{n}"), 0),
                  (t("stat", "RESULT"), 0)]

        def on_connect(c, userdata, flags, rc, *extra):
            ok = (not rc.is_failure) if getattr(rc, "is_failure", None) is not None else rc == 0
            self.connected = ok
            if ok:
                c.subscribe(topics)
                c.publish(t("cmnd", "Status"), "8")
                c.publish(t("cmnd", f"Power{n}"), "")      # empty = query: reports the relay, switches nothing

        def on_disconnect(c, *a):
            self.connected = False

        c = self._make_client()
        c.on_connect, c.on_message, c.on_disconnect = on_connect, self._on_message, on_disconnect
        c.reconnect_delay_set(min_delay=2, max_delay=60)
        self._client = c
        try:
            c.connect_async(s.mqtt_host, s.mqtt_port, keepalive=30)
            c.loop_start()
        except Exception as e:
            print(f"⚠️  Power monitor: {e}")
            return
        while not self._stop.wait(POLL_SECONDS):
            try:
                if self.connected:
                    c.publish(t("cmnd", "Status"), "8")
                self._tick(datetime.now())
            except Exception as e:                        # must never raise out of the thread
                print(f"⚠️  Power monitor tick: {e}")

    # ---- incoming -----------------------------------------------------------------------
    def _on_message(self, c, userdata, msg):
        try:
            topic, body = msg.topic, msg.payload.decode(errors="replace").strip()
            n = self.s.tasmota_relay
            if topic.endswith("/LWT"):
                self.lwt = body
                return
            if body.upper() in ("ON", "OFF") and topic.upper().endswith(("/POWER", f"/POWER{n}")):
                self._set_relay(body.upper())
                return
            try:
                obj = json.loads(body)
            except ValueError:
                return
            for key in (f"POWER{n}", "POWER"):
                if isinstance(obj, dict) and str(obj.get(key, "")).upper() in ("ON", "OFF"):
                    self._set_relay(str(obj[key]).upper())
            e = _energy(obj)
            if e and e.get("Power") is not None:
                k = self.s.power_scale                      # this plug's voltage (so power) reads 1/10: correct it
                volts = None if e.get("Voltage") is None else float(e["Voltage"]) * k
                self._reading(float(e["Power"]) * k, volts, e.get("Current"), datetime.now())
        except Exception as ex:
            print(f"⚠️  Power monitor message: {ex}")

    def _set_relay(self, new):
        with self._lock:
            if new != self.relay:
                first = self.relay is None
                self.relay = new
                self.relay_since = None if first else datetime.now()      # start-up: we do not know since when
                self.drew = False

    def _since_switch(self, now):
        """Seconds since the relay was switched, or None if unknown / long ago."""
        return None if self.relay_since is None else (now - self.relay_since).total_seconds()

    def _window(self):
        """Lag allowed after an ON/OFF: the setting plus one sampling step (readings only come every POLL_SECONDS)."""
        z = self.s.power_settle_seconds
        return z + POLL_SECONDS if z > 0 else 0

    def _settling(self, now):
        """True while the sensor is still catching up with the last ON/OFF."""
        d = self._since_switch(now)
        return d is not None and 0 <= d < self._window()

    def _reading(self, watts, volts, amps, now):
        with self._lock:
            self.reading = {"watts": watts, "volts": volts, "amps": amps, "at": now}
            self.first_seen = self.first_seen or now
            on = watts >= self.s.power_on_watts
            if on:
                self.drew = True
            stale_off = self.relay == "OFF" and self._settling(now)    # relay already OFF: this is the sensor's lag
            sess = self._sess
            if sess:
                dt = (now - sess["last"]).total_seconds()
                if 0 < dt < 600 and not stale_off:         # trapezoid; a long gap, or the OFF lag, is not integrated
                    sess["kwh"] += (sess["last_w"] + watts) / 2 * dt / 3600 / 1000
                sess["last"], sess["last_w"] = now, watts
                sess["peak"] = max(sess["peak"], watts)
            if on and not sess:
                start = now
                if self.relay == "ON" and self._settling(now):          # it was really on since the relay switched
                    start = self.relay_since
                self._sess = {"start": start, "last": now, "last_w": watts, "kwh": 0.0, "peak": watts}
            elif not on and sess:
                end = now
                if self.relay == "OFF" and self.relay_since and self.relay_since >= sess["start"] \
                        and (now - self.relay_since).total_seconds() <= self._window():
                    end = self.relay_since                                # it was really off since the relay switched
                self._close_session(end, locked=True)

    def _tick(self, now):
        """No readings for a while: close an open session at its last reading (the sensor went quiet)."""
        with self._lock:
            sess = self._sess
            if sess and (now - sess["last"]).total_seconds() > STALE_SECONDS:
                self._close_session(sess["last"], locked=True)

    def _close_session(self, end, locked=False):
        def go():
            sess, self._sess = self._sess, None
            if not sess:
                return
            minutes = (end - sess["start"]).total_seconds() / 60
            if minutes < 0.5:                              # a blip, not a heating run
                return
            rec = {"start": sess["start"].isoformat(timespec="seconds"), "end": end.isoformat(timespec="seconds"),
                   "minutes": round(minutes, 1), "kwh": round(sess["kwh"], 3), "peak_w": round(sess["peak"])}
            try:
                os.makedirs(self.s.stored_dir, exist_ok=True)
                with open(self.s.power_file, "a") as f:
                    f.write(json.dumps(rec) + "\n")
            except OSError as e:
                print(f"⚠️  Could not write power log: {e}")
        if locked:
            go()
        else:
            with self._lock:
                go()

    # ---- what the dashboard shows ----------------------------------------------------------
    def snapshot(self, now=None):
        """{'state': heating | idle | tank_hot | switching_on | switching_off | no_draw | draw_while_off | offline | waiting,
        'watts', 'relay', 'age_s'}"""
        now = now or datetime.now()
        with self._lock:
            r, relay = dict(self.reading), self.relay
        at = r["at"]
        age = (now - at).total_seconds() if at else None
        if not self.connected and (age is None or age > STALE_SECONDS):
            state = "offline"
        elif age is None:
            state = "waiting"
        elif age > STALE_SECONDS:
            state = "offline"
        else:
            on = r["watts"] >= self.s.power_on_watts
            if self._settling(now):     # inside the lag after an ON/OFF: no alarm, just say what we wait for
                state = "switching_on" if relay == "ON" and not on else "switching_off" if relay == "OFF" and on else \
                        "heating" if on else "idle"
            elif on:
                state = "draw_while_off" if relay == "OFF" else "heating"
            elif relay == "ON":
                # no draw although ON: normal if it was heating before (thermostat cut off = tank hot),
                # a fault if it never drew since we switched it ON
                state = "no_draw" if self.relay_since is not None and not self.drew else "tank_hot"
            else:
                state = "idle"
        return {"state": state, "watts": None if r["watts"] is None else round(r["watts"]), "relay": relay,
                "age_s": None if age is None else round(age)}


def read_log(s):
    try:
        with open(s.power_file) as f:
            lines = f.read().splitlines()
    except (FileNotFoundError, OSError):
        return []
    out = []
    for ln in lines:
        try:
            out.append(json.loads(ln))
        except ValueError:
            continue
    return out


def totals(s, now=None, days=14, monitor=None):
    """Measured (not commanded) heating: {'today': {minutes, kwh}, 'per_day': [{date, minutes, kwh}]}.
    An open session counts up to now."""
    now = now or datetime.now()
    per = {}
    for r in read_log(s):
        try:
            d = r["start"][:10]
            per.setdefault(d, [0.0, 0.0])
            per[d][0] += float(r["minutes"])
            per[d][1] += float(r["kwh"])
        except (KeyError, ValueError, TypeError):
            continue
    if monitor and monitor._sess:
        sess = monitor._sess
        d = sess["start"].isoformat()[:10]
        per.setdefault(d, [0.0, 0.0])
        per[d][0] += (now - sess["start"]).total_seconds() / 60
        per[d][1] += sess["kwh"]
    rows = []
    for i in range(days - 1, -1, -1):
        d = str((now - timedelta(days=i)).date())
        m, k = per.get(d, [0.0, 0.0])
        rows.append({"date": d, "minutes": round(m), "kwh": round(k, 2)})
    return {"today": {"minutes": rows[-1]["minutes"], "kwh": rows[-1]["kwh"]}, "per_day": rows}


def row(s, monitor):
    """One light for the Connections card."""
    if not s.power_enabled:
        return None
    if monitor is None or not monitor.configured:
        return {"name": "Power sensor", "level": "grey", "text": "Not active here (needs MQTT host + plug topic)"}
    snap = monitor.snapshot()
    st = snap["state"]
    if st == "offline":
        return {"name": "Power sensor", "level": "red", "text": "No readings (is the plug online, has it an energy sensor?)"}
    if st == "waiting":
        return {"name": "Power sensor", "level": "yellow", "text": "Connected, waiting for the first reading"}
    if st == "no_draw":
        return {"name": "Power sensor", "level": "red", "text": "Relay is ON but nothing is drawn: check the element / plug"}
    if st == "draw_while_off":
        return {"name": "Power sensor", "level": "red", "text": f"Drawing {snap['watts']} W while the relay is OFF"}
    return {"name": "Power sensor", "level": "green", "text": f"{snap['watts']} W"}


def apply_measured(s, t, now, monitor):
    """Replace the commanded minutes of runs.totals() by the minutes the plug really drew power, wherever the
    sensor was listening. Per-source split is kept in proportion (extra heating nobody commanded -> 'manual')."""
    firsts = [r["start"][:10] for r in read_log(s) if "start" in r]
    if monitor and monitor.first_seen:
        firsts.append(str(monitor.first_seen.date()))
    if not firsts:
        return t
    since, today = min(firsts), str(now.date())
    live = bool(monitor) and monitor.snapshot(now)["state"] not in ("offline", "waiting")
    pt = totals(s, now, days=len(t["per_day"]), monitor=monitor)["per_day"]
    rows = []
    for row, m in zip(t["per_day"], pt):
        if row["date"] < since or (row["date"] == today and not live):
            rows.append(row)
            continue
        com, meas = sum(row[k] for k in ("auto", "preset", "manual")), m["minutes"]
        new = {k: round(row[k] * meas / com) for k in ("auto", "preset", "manual")} if com else {"auto": 0, "preset": 0, "manual": meas}
        rows.append({**row, **new, "measured": True})
    last = rows[-1]
    return {"today": {**{k: last[k] for k in ("auto", "preset", "manual")},
                      "total": sum(last[k] for k in ("auto", "preset", "manual")), "measured": bool(last.get("measured"))},
            "week": sum(sum(r[k] for k in ("auto", "preset", "manual")) for r in rows[-7:]), "per_day": rows}
