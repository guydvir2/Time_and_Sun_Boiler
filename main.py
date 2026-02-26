import os
import sys
import csv
import json
import time
import shutil
import logging
import threading
import requests
import configparser
from datetime import datetime, timedelta, date

import tkinter as tk
from tkinter import ttk, messagebox
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg


# ==========================================================
# LOGGING
# ==========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("boiler.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger(__name__)


# ==========================================================
# CONFIG — load + validate
# ==========================================================
REQUIRED_CONFIG = {
    "homeassistant": [
        "HA_IP", "HA_PORT", "token",
        "BOILER_1ST_ON_ENTITY_ID",
        "BOILER_2ND_ON_ENTITY_ID",
        "RUN_SCRIPT_1ST_START_BOILER_ENTITY_ID"
    ],
    "location":    ["latitude", "longitude"],
    "weather":     ["weather_url"],
    "parameters":  ["max_first_run", "sunset_offset_minutes", "init_hour", "poll_interval_minutes"],
    "temperature_duration_lut": ["temp_lut"]
}

config = configparser.ConfigParser()
config.read("config.ini")

missing = []
for section, keys in REQUIRED_CONFIG.items():
    for key in keys:
        if not config.has_option(section, key):
            missing.append(f"[{section}] → {key}")
if missing:
    raise SystemExit(
        "config.ini is missing required entries:\n  " + "\n  ".join(missing)
    )

HA_IP    = config["homeassistant"]["HA_IP"]
HA_PORT  = config["homeassistant"]["HA_PORT"]

# ── Security: prefer env var over config for the token ──────────────────
# Set HA_TOKEN=your_token in your environment to avoid storing secrets in
# config.ini (useful when the project folder is under version control).
HA_TOKEN = os.environ.get("HA_TOKEN") or config["homeassistant"]["token"]
if not HA_TOKEN:
    raise SystemExit("HA token not found. Set HA_TOKEN env var or add token to config.ini.")

BOILER_1ST_ON_ENTITY_ID        = config["homeassistant"]["BOILER_1ST_ON_ENTITY_ID"]
BOILER_2ND_ON_ENTITY_ID        = config["homeassistant"]["BOILER_2ND_ON_ENTITY_ID"]
RUN_SCRIPT_1ST_START_ENTITY_ID = config["homeassistant"]["RUN_SCRIPT_1ST_START_BOILER_ENTITY_ID"]

LAT           = float(config["location"]["latitude"])
LON           = float(config["location"]["longitude"])
WEATHER_URL   = config["weather"]["weather_url"]
MAX_FIRST_RUN         = int(config["parameters"]["max_first_run"])
SUNSET_OFFSET_MINUTES = int(config["parameters"]["sunset_offset_minutes"])
INIT_HOUR             = int(config["parameters"]["init_hour"])
POLL_INTERVAL_MINUTES = int(config["parameters"]["poll_interval_minutes"])

TEMP_LUT = {int(k): v for k, v in json.loads(
    config["temperature_duration_lut"]["temp_lut"]
).items()}

# ── LUT empty guard ──────────────────────────────────────────────────────
if not TEMP_LUT:
    raise SystemExit("TEMP_LUT is empty. Check [temperature_duration_lut] in config.ini.")

HA_URL  = f"http://{HA_IP}:{HA_PORT}"
HEADERS = {
    "Authorization": f"Bearer {HA_TOKEN}",
    "Content-Type":  "application/json"
}

DATA_FOLDER    = "stored_data"
ARCHIVE_FOLDER = os.path.join(DATA_FOLDER, "archive")
os.makedirs(DATA_FOLDER, exist_ok=True)

# ── CSV write lock — prevents races between GUI and any cron/scheduler ──
_csv_lock = threading.Lock()


# ==========================================================
# RETRY HELPER
# ==========================================================
def with_retries(fn, retries=3, delay=5, label="operation"):
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except Exception as e:
            if attempt == retries:
                log.error(f"{label} failed after {retries} attempts: {e}")
                raise
            log.warning(f"{label} attempt {attempt}/{retries} failed: {e}. Retrying in {delay}s...")
            time.sleep(delay)


# ==========================================================
# HOME ASSISTANT
# ==========================================================
def check_ha_reachable():
    r = requests.get(f"{HA_URL}/api/", headers=HEADERS, timeout=5)
    r.raise_for_status()
    log.info("HA is reachable.")


def ha_set_slider(entity_id, value):
    url     = f"{HA_URL}/api/services/input_number/set_value"
    payload = {"entity_id": entity_id, "value": value}
    r = requests.post(url, headers=HEADERS, json=payload, timeout=10)
    r.raise_for_status()
    log.info(f"  ✓ Set {entity_id} = {value} min")


def ha_run_script(script_name):
    entity_id = f"script.{script_name}"
    url       = f"{HA_URL}/api/services/script/turn_on"
    payload   = {"entity_id": entity_id}
    r = requests.post(url, headers=HEADERS, json=payload, timeout=10)
    r.raise_for_status()
    log.info(f"  ✓ Triggered {entity_id}")


def send_to_ha(first_run, second_run):
    log.info("Sending values to Home Assistant...")
    try:
        with_retries(lambda: check_ha_reachable(),                            label="HA health-check")
        with_retries(lambda: ha_set_slider(BOILER_1ST_ON_ENTITY_ID, first_run), label=f"set slider1")
        with_retries(lambda: ha_set_slider(BOILER_2ND_ON_ENTITY_ID, second_run),label=f"set slider2")
        with_retries(lambda: ha_run_script(RUN_SCRIPT_1ST_START_ENTITY_ID),   label=f"run script")
        log.info("HA execution complete.")
        return True
    except requests.exceptions.Timeout:
        log.error("HA request timed out.")
    except requests.exceptions.ConnectionError:
        log.error(f"Could not connect to HA at {HA_URL}.")
    except requests.exceptions.HTTPError as e:
        log.error(f"HA returned HTTP {e.response.status_code}: {e.response.text}")
    except requests.exceptions.RequestException as e:
        log.error(f"Unexpected HA error: {e}")
    return False


# ==========================================================
# WEATHER + CALC
# ==========================================================
def get_sun_times():
    """
    Lightweight API call — fetches only daily sunrise/sunset for today.
    Used at INIT_HOUR to seed the scheduler. No hourly data fetched.
    """
    def _fetch():
        r = requests.get(
            WEATHER_URL,
            params={
                "latitude":  LAT, "longitude": LON,
                "daily":     "sunrise,sunset",
                "timezone":  "auto",
                "forecast_days": 1
            },
            timeout=10
        )
        r.raise_for_status()
        return r.json()

    try:
        data = with_retries(_fetch, label="sun times fetch")
    except Exception as e:
        raise RuntimeError(f"Could not fetch sun times: {e}") from e

    try:
        sunrise = datetime.fromisoformat(data["daily"]["sunrise"][0])
        sunset  = datetime.fromisoformat(data["daily"]["sunset"][0])
    except (KeyError, IndexError, ValueError) as e:
        raise RuntimeError(f"Unexpected API response for sun times: {e}") from e

    log.info(f"Sun times → sunrise={sunrise.strftime('%H:%M')}  sunset={sunset.strftime('%H:%M')}")
    return sunrise, sunset


def get_weather():
    def _fetch():
        r = requests.get(
            WEATHER_URL,
            params={
                "latitude":  LAT, "longitude": LON,
                "hourly":    "temperature_2m,cloudcover",
                "daily":     "sunrise,sunset",
                "timezone":  "auto"
            },
            timeout=10
        )
        r.raise_for_status()
        return r.json()

    try:
        data = with_retries(_fetch, label="weather API fetch")
    except requests.exceptions.Timeout:
        raise RuntimeError("Weather API timed out after all retries.") from None
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Weather API failed: {e}") from e

    today = date.today()
    try:
        sunrise = datetime.fromisoformat(data["daily"]["sunrise"][0])
        sunset  = datetime.fromisoformat(data["daily"]["sunset"][0])
    except (KeyError, IndexError, ValueError) as e:
        raise RuntimeError(f"Unexpected weather API response: {e}") from e

    now     = datetime.now()
    weather = []
    for t, temp, clouds in zip(
        data["hourly"]["time"],
        data["hourly"]["temperature_2m"],
        data["hourly"]["cloudcover"]
    ):
        try:
            ts = datetime.fromisoformat(t)
        except ValueError:
            log.warning(f"Skipping unparseable timestamp {t!r}")
            continue
        # Only include hours that have already passed (measured, not forecast)
        # Hourly data is on the hour — include up to and including current hour
        if ts.date() == today and sunrise <= ts <= now:
            weather.append({"time": ts, "temp": temp, "clouds": clouds})

    log.info(f"Collected {len(weather)} measured hour(s) between {sunrise.strftime('%H:%M')} and {now.strftime('%H:%M')}")
    return sunrise, sunset, weather


def weighted_average(weather, sunrise, sunset):
    """
    Weight measured hours by their position in the day:
      - First 3hrs after sunrise  → weight 0.5  (morning warm-up, less representative)
      - Middle of the day         → weight 1.0  (peak representative hours)
      - Last 2hrs before sunset   → weight 0.5  (cooling down)

    Since we only pass measured hours (sunrise → now), the weights
    naturally reflect however much of the day has been recorded.
    """
    total_weight = temp_sum = cloud_sum = 0
    for w in weather:
        t = w["time"]
        if   sunrise <= t < sunrise + timedelta(hours=3):
            weight = 0.5
        elif sunrise + timedelta(hours=3) <= t <= sunset - timedelta(hours=2):
            weight = 1.0
        elif sunset - timedelta(hours=2) < t <= sunset:
            weight = 0.5
        else:
            weight = 0
        if weight > 0:
            temp_sum     += w["temp"]   * weight
            cloud_sum    += w["clouds"] * weight
            total_weight += weight

    if total_weight == 0:
        raise ValueError("No measured weather data available for weighting.")
    return temp_sum / total_weight, cloud_sum / total_weight


def calculate_duration(temp):
    temps = sorted(TEMP_LUT.keys())
    if temp <= temps[0]:  return TEMP_LUT[temps[0]]
    if temp >= temps[-1]: return TEMP_LUT[temps[-1]]
    for i in range(len(temps) - 1):
        t0, t1 = temps[i], temps[i + 1]
        if t0 <= temp <= t1:
            v0, v1 = TEMP_LUT[t0], TEMP_LUT[t1]
            return int(round(v0 + (temp - t0) / (t1 - t0) * (v1 - v0)))
    raise ValueError(f"Temperature {temp}°C not matched in TEMP_LUT.")


def calculate_sliders(duration):
    if duration <= MAX_FIRST_RUN:
        return duration, 0
    return MAX_FIRST_RUN, duration - MAX_FIRST_RUN


# ==========================================================
# DATA MANAGER  — all file I/O in one place
# ==========================================================
REQUIRED_FIELDS = {
    "date", "sunrise", "sunset",
    "avg_temp", "avg_cloud",
    "duration", "first_run", "second_run"
}

LEGACY_COLUMN_MAP = {
    "on_time":   "duration",
    "script_ok": "ha_status",
}

CSV_FIELDNAMES = [
    "date", "sunrise", "sunset",
    "avg_temp", "avg_cloud", "effective_temp",
    "duration", "first_run", "second_run",
    "execute_time", "ha_status"
]


def _safe_num(value, cast=float, fallback=None):
    """Safely cast a CSV string value to int/float; return fallback if it's '—' or invalid."""
    try:
        return cast(value)
    except (ValueError, TypeError):
        return fallback


class DataManager:
    """
    Handles all reading/writing of daily CSV summaries.
    Provides a cache so the disk is only scanned once per session.
    Use invalidate() after any write to force a fresh read.
    """

    def __init__(self):
        self._cache      = None
        self._warned_fmt = set()

    def invalidate(self):
        self._cache = None

    # ----------------------------------------------------------
    def execute_today(self, sunrise, sunset):
        """
        Part 3 — full execution using already-known sunrise/sunset from scheduler.
        Fetches measured hourly weather (sunrise→now), calculates, sends to HA, saves CSV.
        Returns the saved row dict, or None on failure.
        """
        try:
            _, _, weather = get_weather()
        except Exception as e:
            log.error(f"Could not fetch measured weather data: {e}")
            return None

        avg_temp, avg_cloud      = weighted_average(weather, sunrise, sunset)
        effective_temp           = round(avg_temp - (avg_cloud / 100) * 3, 2)
        duration                 = calculate_duration(effective_temp)
        first_run, second_run    = calculate_sliders(duration)

        log.info(
            f"Calculated → effective_temp={effective_temp}°C | "
            f"duration={duration} min | slider1={first_run} | slider2={second_run}"
        )

        ha_ok        = send_to_ha(first_run, second_run)
        execute_time = datetime.now().strftime("%H:%M:%S")
        ha_status    = "OK" if ha_ok else "FAILED"

        row = {
            "date":           str(date.today()),
            "sunrise":        sunrise.strftime("%H:%M"),
            "sunset":         sunset.strftime("%H:%M"),
            "avg_temp":       round(avg_temp, 2),
            "avg_cloud":      round(avg_cloud, 2),
            "effective_temp": effective_temp,
            "duration":       duration,
            "first_run":      first_run,
            "second_run":     second_run,
            "execute_time":   execute_time,
            "ha_status":      ha_status
        }

        filename = os.path.join(DATA_FOLDER, f"{date.today()}.csv")
        self._write_csv(filename, row)
        log.info(f"[{execute_time}] CSV saved → HA status: {ha_status}")
        self.invalidate()
        return row

    # ----------------------------------------------------------
    def retry_ha(self):
        """Re-send today's saved values to HA without re-fetching weather.
        Used by the GUI Retry HA button."""
        filename = os.path.join(DATA_FOLDER, f"{date.today()}.csv")
        if not os.path.exists(filename):
            log.warning("No CSV found for today — cannot retry HA.")
            return False, "No data for today"

        with _csv_lock:
            with open(filename, newline="") as f:
                rows = list(csv.DictReader(f))

        if not rows:
            return False, "Today's CSV is empty"

        row        = rows[0]
        first_run  = _safe_num(row.get("first_run"),  cast=int, fallback=0)
        second_run = _safe_num(row.get("second_run"), cast=int, fallback=0)

        ha_ok     = send_to_ha(first_run, second_run)
        ha_status = "OK" if ha_ok else "FAILED"

        row["ha_status"]    = ha_status
        row["execute_time"] = datetime.now().strftime("%H:%M:%S")
        self._write_csv(filename, row)

        log.info(f"Retry HA complete → {ha_status}")
        self.invalidate()
        return ha_ok, ha_status

    # ----------------------------------------------------------
    def all_records(self):
        """Return cached, date-sorted list of all valid daily summary dicts."""
        if self._cache is not None:
            return self._cache

        all_rows       = []
        archived_count = 0

        for file in sorted(os.listdir(DATA_FOLDER)):
            if not file.endswith(".csv"):
                continue

            filepath = os.path.join(DATA_FOLDER, file)
            try:
                with _csv_lock:
                    with open(filepath, newline="") as f:
                        reader     = csv.DictReader(f)
                        fieldnames = set(reader.fieldnames or [])
                        remapped   = {LEGACY_COLUMN_MAP.get(fn, fn) for fn in fieldnames}

                        if not REQUIRED_FIELDS.issubset(remapped):
                            fmt_key = frozenset(fieldnames)
                            if fmt_key not in self._warned_fmt:
                                log.warning(
                                    f"Unrecognised columns {sorted(fieldnames)} "
                                    f"(e.g. {file}). Moving to archive/."
                                )
                                self._warned_fmt.add(fmt_key)
                            rows_data = None
                        else:
                            migrated = any(fn in LEGACY_COLUMN_MAP for fn in fieldnames)
                            if migrated:
                                log.info(f"Migrating legacy format: {file}")
                            rows_data = [self._migrate_row(r) for r in reader]

                if rows_data is None:
                    self._archive(filepath, file)
                    archived_count += 1
                else:
                    all_rows.extend(rows_data)

            except Exception as e:
                log.warning(f"Skipping {file}: {e}")

        if archived_count:
            log.info(f"Archived {archived_count} unrecognised file(s) to archive/")

        try:
            all_rows.sort(key=lambda x: datetime.strptime(x["date"], "%Y-%m-%d"))
        except ValueError as e:
            log.warning(f"Could not sort rows by date: {e}")

        self._cache = all_rows
        return self._cache

    # ----------------------------------------------------------
    @staticmethod
    def _write_csv(filename, row):
        """Thread-safe single-row CSV write."""
        fieldnames = list(row.keys())
        with _csv_lock:
            with open(filename, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerow(row)

    @staticmethod
    def _migrate_row(row):
        new_row = {LEGACY_COLUMN_MAP.get(k, k): v for k, v in row.items()}
        new_row.setdefault("effective_temp", "—")
        new_row.setdefault("execute_time",   "—")
        new_row.setdefault("ha_status",      "—")
        new_row.setdefault("first_run",      "—")
        new_row.setdefault("second_run",     "—")
        return new_row

    @staticmethod
    def _archive(filepath, file):
        os.makedirs(ARCHIVE_FOLDER, exist_ok=True)
        dest = os.path.join(ARCHIVE_FOLDER, file)
        try:
            shutil.move(filepath, dest)
            log.info(f"Archived: {file} → archive/")
        except Exception as e:
            log.warning(f"Could not archive {file}: {e}")



# ==========================================================
# SCHEDULER  — four-state daily loop, runs in a background thread
# ==========================================================
# States
# ──────
# WAIT_INIT    : sleeping until INIT_HOUR — no API calls
# FETCH_SUN    : it is INIT_HOUR → fetch sunrise+sunset, move to WAIT_TRIGGER
# WAIT_TRIGGER : polling every POLL_INTERVAL_MINUTES until sunset+offset
# EXECUTE      : trigger time reached → run full execution, move to WAIT_INIT
#
# Mid-day startup:
#   - If start time is before INIT_HOUR            → WAIT_INIT
#   - If start time is between INIT_HOUR & trigger → fetch sun times, WAIT_TRIGGER
#   - If start time is past trigger & no CSV today → execute immediately
#   - If start time is past trigger & CSV exists   → WAIT_INIT (already done)

class Scheduler:

    def __init__(self, data_manager: "DataManager", on_state_change=None):
        self.dm              = data_manager
        self.on_state_change = on_state_change   # optional GUI callback
        self.state           = None
        self.sunrise         = None
        self.sunset          = None
        self.trigger_time    = None
        self._stop           = threading.Event()
        self._thread         = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop.set()

    # ----------------------------------------------------------
    def _set_state(self, state):
        self.state = state
        log.info(f"[Scheduler] → {state}")
        if self.on_state_change:
            self.on_state_change(state)

    # ----------------------------------------------------------
    def _run(self):
        """Main scheduler loop. Runs entirely in a background thread."""
        self._determine_startup_state()

        while not self._stop.is_set():

            if self.state == "WAIT_INIT":
                self._wait_for_init()

            elif self.state == "FETCH_SUN":
                self._fetch_sun()

            elif self.state == "WAIT_TRIGGER":
                self._wait_for_trigger()

            elif self.state == "EXECUTE":
                self._execute()

            else:
                # Unknown state — reset to safe default
                log.warning(f"[Scheduler] Unknown state {self.state!r} — resetting to WAIT_INIT")
                self._set_state("WAIT_INIT")

    # ----------------------------------------------------------
    def _determine_startup_state(self):
        """
        Decide which state to enter on startup based on current time
        and whether today's CSV already exists.
        """
        now       = datetime.now()
        today_csv = os.path.join(DATA_FOLDER, f"{date.today()}.csv")

        if os.path.exists(today_csv):
            # Already executed today regardless of time
            log.info("[Scheduler] Startup: today already done → WAIT_INIT")
            self._set_state("WAIT_INIT")
            return

        if now.hour < INIT_HOUR:
            # Too early even for init
            log.info(f"[Scheduler] Startup: before {INIT_HOUR:02d}:00 → WAIT_INIT")
            self._set_state("WAIT_INIT")
            return

        # It is past INIT_HOUR — we need sun times before deciding further
        log.info("[Scheduler] Startup: past init hour, fetching sun times...")
        try:
            self.sunrise, self.sunset = get_sun_times()
            self.trigger_time = self.sunset + timedelta(minutes=SUNSET_OFFSET_MINUTES)
        except Exception as e:
            log.error(f"[Scheduler] Startup sun times fetch failed: {e} → WAIT_INIT")
            self._set_state("WAIT_INIT")
            return

        if now >= self.trigger_time:
            # Past trigger, no CSV → execute immediately
            log.info("[Scheduler] Startup: past trigger time, no CSV → EXECUTE")
            self._set_state("EXECUTE")
        else:
            # Between INIT_HOUR and trigger → wait for trigger
            log.info(f"[Scheduler] Startup: waiting for trigger at {self.trigger_time.strftime('%H:%M')} → WAIT_TRIGGER")
            self._set_state("WAIT_TRIGGER")

    # ----------------------------------------------------------
    def _wait_for_init(self):
        """Sleep in POLL_INTERVAL_MINUTES chunks until INIT_HOUR on the next day."""
        while not self._stop.is_set():
            now = datetime.now()
            # Build today's (or tomorrow's) init time
            init_today = now.replace(hour=INIT_HOUR, minute=0, second=0, microsecond=0)
            if now >= init_today:
                # Already past today's init hour — target tomorrow
                init_today += timedelta(days=1)

            secs_to_init = (init_today - now).total_seconds()
            log.info(f"[Scheduler] WAIT_INIT: next init at {init_today.strftime('%Y-%m-%d %H:%M')} "
                     f"({secs_to_init/3600:.1f}h away)")

            # Sleep in chunks so we can respond to stop() promptly
            sleep_secs = POLL_INTERVAL_MINUTES * 60
            slept = 0
            while slept < secs_to_init and not self._stop.is_set():
                chunk = min(sleep_secs, secs_to_init - slept)
                time.sleep(chunk)
                slept += chunk

            if not self._stop.is_set():
                self._set_state("FETCH_SUN")
                return

    # ----------------------------------------------------------
    def _fetch_sun(self):
        """Part 1 — fetch sunrise/sunset at INIT_HOUR."""
        try:
            self.sunrise, self.sunset = get_sun_times()
            self.trigger_time = self.sunset + timedelta(minutes=SUNSET_OFFSET_MINUTES)
            log.info(f"[Scheduler] FETCH_SUN: trigger set to {self.trigger_time.strftime('%H:%M')}")
            self._set_state("WAIT_TRIGGER")
        except Exception as e:
            log.error(f"[Scheduler] FETCH_SUN failed: {e} — retrying in {POLL_INTERVAL_MINUTES} min")
            # Stay in FETCH_SUN — retry on next loop iteration after a short sleep
            time.sleep(POLL_INTERVAL_MINUTES * 60)

    # ----------------------------------------------------------
    def _wait_for_trigger(self):
        """Part 2 — sleep in POLL_INTERVAL_MINUTES chunks until trigger_time."""
        while not self._stop.is_set():
            now  = datetime.now()
            diff = (self.trigger_time - now).total_seconds()

            if diff <= 0:
                self._set_state("EXECUTE")
                return

            log.info(f"[Scheduler] WAIT_TRIGGER: trigger at {self.trigger_time.strftime('%H:%M')} "
                     f"({diff/60:.0f} min away)")
            sleep_secs = min(POLL_INTERVAL_MINUTES * 60, diff)
            time.sleep(sleep_secs)

    # ----------------------------------------------------------
    def _execute(self):
        """Part 3 — full execution: fetch measured weather, calculate, send to HA, save CSV."""
        try:
            row = self.dm.execute_today(self.sunrise, self.sunset)
            if row:
                log.info("[Scheduler] EXECUTE: done for today → WAIT_INIT")
            else:
                log.warning("[Scheduler] EXECUTE: execute_today returned None → WAIT_INIT")
        except Exception as e:
            log.error(f"[Scheduler] EXECUTE failed: {e} → WAIT_INIT")
        self._set_state("WAIT_INIT")


# ==========================================================
# GUI
# ==========================================================
class BoilerApp:

    def __init__(self, root, data_manager: DataManager, scheduler: "Scheduler"):

        self.root         = root
        self.dm           = data_manager
        self.scheduler    = scheduler
        self.startup_time = datetime.now()
        self._graph_canvas = None          # track canvas for redraw
        self._state_var    = tk.StringVar(value="STARTING...")

        self.root.title("Boiler Control Dashboard")
        self.root.geometry("1500x680")

        # ── Dark theme palette ───────────────────────────────
        BG     = "#1e1e1e"
        BG2    = "#2b2b2b"
        BG3    = "#3c3f41"
        FG     = "#e0e0e0"
        FG_DIM = "#aaaaaa"
        ACCENT = "#4e9de0"
        SEP    = "#555555"

        self.root.configure(bg=BG)
        self._clr = dict(BG=BG, BG2=BG2, BG3=BG3, FG=FG,
                         FG_DIM=FG_DIM, ACCENT=ACCENT, SEP=SEP)

        style = ttk.Style()
        style.theme_use("clam")

        style.configure("TNotebook",     background=BG,  borderwidth=0)
        style.configure("TNotebook.Tab", background=BG3, foreground=FG,
                        padding=[12, 4], font=("Segoe UI", 10))
        style.map("TNotebook.Tab",
                  background=[("selected", BG2)],
                  foreground=[("selected", ACCENT)])

        style.configure("TFrame", background=BG)

        style.configure("Treeview",
                        background=BG2, foreground=FG,
                        fieldbackground=BG2,
                        rowheight=28, font=("Segoe UI", 10),
                        borderwidth=0)
        style.configure("Treeview.Heading",
                        background=BG3, foreground=ACCENT,
                        font=("Segoe UI", 10, "bold"),
                        relief="flat", borderwidth=0)
        style.map("Treeview",
                  background=[("selected", ACCENT)],
                  foreground=[("selected", "#ffffff")])

        style.configure("Vertical.TScrollbar",
                        background=BG3, troughcolor=BG2,
                        borderwidth=0, arrowcolor=FG_DIM)

        style.configure("TButton",
                        background=BG3, foreground=FG,
                        font=("Segoe UI", 10), relief="flat",
                        borderwidth=0, padding=[10, 4])
        style.map("TButton",
                  background=[("active", "#4e5254")],
                  foreground=[("active", "#ffffff")])

        self.notebook = ttk.Notebook(root)
        self.notebook.pack(fill="both", expand=True)

        self.tab_table = ttk.Frame(self.notebook)
        self.tab_graph = ttk.Frame(self.notebook)

        self.notebook.add(self.tab_table, text="Daily Log")
        self.notebook.add(self.tab_graph, text="Graph")

        self.create_table()
        self.load_data()
        self.draw_graph()
        self.create_status_bar()
        self._tick()

    # ------------------------------------------------------
    def create_table(self):
        columns = (
            "#", "date", "sunrise", "sunset",
            "avg_temp", "avg_cloud", "effective_temp",
            "duration", "first_run", "second_run",
            "execute_time", "ha_status"
        )

        self.tree = ttk.Treeview(self.tab_table, columns=columns, show="headings")

        col_widths = {
            "#": 35, "date": 105, "sunrise": 70, "sunset": 70,
            "avg_temp": 95, "avg_cloud": 95, "effective_temp": 115,
            "duration": 90, "first_run": 125, "second_run": 135,
            "execute_time": 105, "ha_status": 90
        }
        col_labels = {
            "#": "#", "date": "Date", "sunrise": "Sunrise", "sunset": "Sunset",
            "avg_temp": "Avg Temp (°C)", "avg_cloud": "Avg Cloud (%)",
            "effective_temp": "Eff. Temp (°C)", "duration": "Total (min)",
            "first_run": "1st Timeout (min)", "second_run": "2nd Timeout (min)",
            "execute_time": "Executed At", "ha_status": "HA Status",
        }

        for col in columns:
            self.tree.heading(col, text=col_labels.get(col, col))
            self.tree.column(col, width=col_widths.get(col, 90), anchor="center")

        scrollbar = ttk.Scrollbar(self.tab_table, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        self.tree.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        self.tree.tag_configure("ha_ok_odd",  background="#1e3a2f", foreground="#c8f0d8")
        self.tree.tag_configure("ha_ok_even", background="#162e24", foreground="#b0e8c8")
        self.tree.tag_configure("ha_failed",  background="#3a1e1e", foreground="#f5a0a0")
        self.tree.tag_configure("ha_unknown", background="#2a2a2e", foreground="#aaaaaa")

    # ------------------------------------------------------
    def load_data(self):
        """Reload table from DataManager and redraw graph."""
        self.dm.invalidate()
        self.tree.delete(*self.tree.get_children())

        for i, row in enumerate(self.dm.all_records(), 1):
            status = row["ha_status"]
            if   status == "FAILED": tag = "ha_failed"
            elif status == "—":      tag = "ha_unknown"
            elif i % 2 == 0:         tag = "ha_ok_even"
            else:                    tag = "ha_ok_odd"

            self.tree.insert("", "end", values=(
                i,
                row["date"],        row["sunrise"],   row["sunset"],
                row["avg_temp"],    row["avg_cloud"],  row["effective_temp"],
                row["duration"],    row["first_run"],  row["second_run"],
                row["execute_time"],row["ha_status"]
            ), tags=(tag,))

        # Redraw graph so it stays in sync with the table
        self.draw_graph()

    # ------------------------------------------------------
    def retry_ha(self):
        """Run HA retry in a background thread so the GUI stays responsive."""
        def _run():
            ok, status = self.dm.retry_ha()
            # Update UI back on the main thread
            self.root.after(0, lambda: self._retry_done(ok, status))

        # Disable button while running
        self._set_retry_btn_state("disabled")
        threading.Thread(target=_run, daemon=True).start()

    def _retry_done(self, ok, status):
        self._set_retry_btn_state("normal")
        if ok:
            messagebox.showinfo("Retry HA", f"HA updated successfully.\nStatus: {status}")
        else:
            messagebox.showerror("Retry HA", f"HA call failed.\nStatus: {status}\nCheck boiler.log.")
        self.load_data()

    def _set_retry_btn_state(self, state):
        if hasattr(self, "_retry_btn"):
            self._retry_btn.config(state=state)

    # ------------------------------------------------------
    def draw_graph(self):
        """Clear and redraw the graph. Safe to call multiple times."""
        # Destroy previous canvas if it exists
        if self._graph_canvas is not None:
            self._graph_canvas.get_tk_widget().destroy()
            self._graph_canvas = None

        records = self.dm.all_records()
        if not records:
            return

        # Safe conversion — "—" values in migrated rows are skipped per-series
        dates_all   = []
        durations   = []
        temps       = []
        first_runs  = []
        second_runs = []
        eff_dates   = []
        eff_temps   = []

        for r in records:
            d = _safe_num(r.get("date", ""), cast=lambda v: datetime.strptime(v, "%Y-%m-%d"))
            if d is None:
                continue
            dates_all.append(d)

            durations.append(  _safe_num(r.get("duration"),    cast=int,   fallback=0))
            temps.append(      _safe_num(r.get("avg_temp"),    cast=float, fallback=0))
            first_runs.append( _safe_num(r.get("first_run"),   cast=int,   fallback=0))
            second_runs.append(_safe_num(r.get("second_run"),  cast=int,   fallback=0))

            et = _safe_num(r.get("effective_temp"), cast=float)
            if et is not None:
                eff_dates.append(d)
                eff_temps.append(et)

        if not dates_all:
            return

        fig = plt.Figure(figsize=(10, 5), facecolor="#1e1e1e")
        ax  = fig.add_subplot(111)
        ax.set_facecolor("#2b2b2b")
        ax.tick_params(colors="#aaaaaa")
        ax.xaxis.label.set_color("#aaaaaa")
        ax.yaxis.label.set_color("#aaaaaa")
        ax.title.set_color("#e0e0e0")
        for spine in ax.spines.values():
            spine.set_edgecolor("#555555")

        ax.plot(dates_all, durations,   label="Total Duration (min)", linewidth=2)
        ax.plot(dates_all, temps,       label="Avg Temp (°C)",        linewidth=2)
        ax.plot(dates_all, first_runs,  label="1st Timeout (min)",    linestyle="--")
        ax.plot(dates_all, second_runs, label="2nd Timeout (min)",    linestyle=":")
        if eff_temps:
            ax.plot(eff_dates, eff_temps, label="Eff. Temp (°C)",
                    linestyle="-.", linewidth=1.5, color="#a070e0")

        ax.axhline(MAX_FIRST_RUN, color="#555555", linewidth=0.8,
                   linestyle="-.", label=f"{MAX_FIRST_RUN} min cap")

        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        fig.autofmt_xdate(rotation=45)

        ax.set_title("Duration, Temperature & Slider Values")
        ax.set_xlabel("Date")
        ax.set_ylabel("Value")
        legend = ax.legend(facecolor="#2b2b2b", edgecolor="#555555", labelcolor="#e0e0e0")
        ax.grid(True, linestyle=":", alpha=0.4, color="#555555")

        canvas = FigureCanvasTkAgg(fig, master=self.tab_graph)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True)
        self._graph_canvas = canvas

    # ------------------------------------------------------
    def create_status_bar(self):
        c   = self._clr
        bar = tk.Frame(self.root, bg=c["BG2"], height=36)
        bar.pack(side="bottom", fill="x")
        bar.pack_propagate(False)

        tk.Frame(bar, bg=c["SEP"], height=1).place(relx=0, rely=0, relwidth=1)

        self.lbl_clock = tk.Label(
            bar, text="", bg=c["BG2"], fg=c["FG"],
            font=("Segoe UI", 10, "bold"), padx=12
        )
        self.lbl_clock.pack(side="left", pady=4)

        tk.Label(bar, text="│", bg=c["BG2"], fg=c["SEP"],
                 font=("Segoe UI", 11)).pack(side="left")

        self.lbl_uptime = tk.Label(
            bar, text="", bg=c["BG2"], fg=c["FG_DIM"],
            font=("Segoe UI", 10), padx=12
        )
        self.lbl_uptime.pack(side="left", pady=4)

        tk.Label(bar, text="│", bg=c["BG2"], fg=c["SEP"],
                 font=("Segoe UI", 11)).pack(side="left")

        # Scheduler state indicator
        self.lbl_state = tk.Label(
            bar, textvariable=self._state_var,
            bg=c["BG2"], fg="#f0c060",
            font=("Segoe UI", 10, "bold"), padx=12
        )
        self.lbl_state.pack(side="left", pady=4)

        btn_style = {
            "bg": c["BG3"], "fg": c["FG"],
            "activebackground": "#4e5254", "activeforeground": "#ffffff",
            "relief": "flat", "font": ("Segoe UI", 10),
            "padx": 14, "pady": 5, "cursor": "hand2", "bd": 0
        }

        self._retry_btn = tk.Button(
            bar, text="⚡  Retry HA (today)",
            command=self.retry_ha, **btn_style
        )
        self._retry_btn.pack(side="right", padx=(4, 12), pady=4)

        tk.Button(
            bar, text="⟳  Refresh",
            command=self.load_data, **btn_style
        ).pack(side="right", padx=4, pady=4)

    # ------------------------------------------------------
    def on_scheduler_state(self, state):
        """Called from the Scheduler thread — must use root.after to touch GUI."""
        state_icons = {
            "WAIT_INIT":    "💤  WAIT INIT",
            "FETCH_SUN":    "🌅  FETCH SUN",
            "WAIT_TRIGGER": "⏳  WAIT TRIGGER",
            "EXECUTE":      "⚡  EXECUTING",
        }
        label = state_icons.get(state, state)
        self.root.after(0, lambda: self._state_var.set(label))

        # Auto-refresh table and graph after execution completes
        if state == "WAIT_INIT":
            self.root.after(500, self.load_data)

    # ------------------------------------------------------
    def _tick(self):
        now     = datetime.now()
        elapsed = now - self.startup_time
        h, rem  = divmod(int(elapsed.total_seconds()), 3600)
        m, s    = divmod(rem, 60)
        self.lbl_clock.config(text=f"🕐  {now.strftime('%H:%M:%S')}")
        self.lbl_uptime.config(text=f"Uptime: {h:02d}:{m:02d}:{s:02d}")
        self.root.after(1000, self._tick)


# ==========================================================
# ENTRY
# ==========================================================
if __name__ == "__main__":

    dm        = DataManager()
    root      = tk.Tk()

    # Scheduler needs to call back into the GUI — pass the callback after app is built
    scheduler = Scheduler(dm)
    app       = BoilerApp(root, dm, scheduler)

    # Now wire the state-change callback and start the scheduler thread
    scheduler.on_state_change = app.on_scheduler_state
    scheduler.start()

    root.mainloop()
    scheduler.stop()   # clean shutdown when window is closed