import os
import sys
import csv
import json
import time
import logging
import logging.handlers
import threading
import requests
import configparser
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Any, Tuple

import tkinter as tk
from tkinter import ttk, messagebox
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

# Import runtime settings manager
from runtime_settings import RuntimeSettings


# ==========================================================
# LOGGING
# ==========================================================
_file_handler = logging.handlers.RotatingFileHandler(
    "boiler.log", maxBytes=2_000_000, backupCount=5
)
_file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

_console_handler = logging.StreamHandler(sys.stdout)
_console_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

logging.basicConfig(level=logging.INFO, handlers=[_file_handler, _console_handler])
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
    "parameters":  [
        "max_first_run", "sunset_offset_minutes",
        "init_hour", "poll_interval_minutes",
        "cloud_penalty_factor"
    ],
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

# Prefer env var over config for the token
HA_TOKEN = os.environ.get("HA_TOKEN") or config["homeassistant"]["token"]
if not HA_TOKEN:
    raise SystemExit("HA token not found. Set HA_TOKEN env var or add token to config.ini.")

BOILER_1ST_ON_ENTITY_ID        = config["homeassistant"]["BOILER_1ST_ON_ENTITY_ID"]
BOILER_2ND_ON_ENTITY_ID        = config["homeassistant"]["BOILER_2ND_ON_ENTITY_ID"]
RUN_SCRIPT_1ST_START_ENTITY_ID = config["homeassistant"]["RUN_SCRIPT_1ST_START_BOILER_ENTITY_ID"]

LAT                  = float(config["location"]["latitude"])
LON                  = float(config["location"]["longitude"])
WEATHER_URL          = config["weather"]["weather_url"]
MAX_FIRST_RUN        = int(config["parameters"]["max_first_run"])
SUNSET_OFFSET_MINUTES= int(config["parameters"]["sunset_offset_minutes"])
INIT_HOUR            = int(config["parameters"]["init_hour"])
POLL_INTERVAL_MINUTES= int(config["parameters"]["poll_interval_minutes"])
CLOUD_PENALTY_FACTOR = float(config["parameters"]["cloud_penalty_factor"])

# Cast LUT values to int
TEMP_LUT = {int(k): int(v) for k, v in json.loads(
    config["temperature_duration_lut"]["temp_lut"]
).items()}

if not TEMP_LUT:
    raise SystemExit("TEMP_LUT is empty. Check [temperature_duration_lut] in config.ini.")

# Initialize runtime settings (loads from runtime_settings.json or uses config.ini defaults)
runtime_settings = RuntimeSettings({
    "temp_lut": TEMP_LUT,
    "cloud_penalty_factor": CLOUD_PENALTY_FACTOR
})

# Override globals with runtime settings (user customizations take precedence)
TEMP_LUT = runtime_settings.get_temp_lut()
CLOUD_PENALTY_FACTOR = runtime_settings.get_cloud_penalty()
FIRST_RUN_TARGET_TIME = runtime_settings.get_target_time()

log.info(f"Runtime settings loaded: target={FIRST_RUN_TARGET_TIME}, cloud_penalty={CLOUD_PENALTY_FACTOR}")

HA_URL  = f"http://{HA_IP}:{HA_PORT}"
HEADERS = {
    "Authorization": f"Bearer {HA_TOKEN}",
    "Content-Type":  "application/json"
}

# CSV write lock — prevents races between GUI thread and scheduler thread
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
        with_retries(lambda: check_ha_reachable(),                             label="HA health-check")
        with_retries(lambda: ha_set_slider(BOILER_1ST_ON_ENTITY_ID, first_run),  label="set slider1")
        with_retries(lambda: ha_set_slider(BOILER_2ND_ON_ENTITY_ID, second_run), label="set slider2")
        with_retries(lambda: ha_run_script(RUN_SCRIPT_1ST_START_ENTITY_ID),    label="run script")
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
    Lightweight fetch — daily sunrise/sunset only.
    Called once at INIT_HOUR to seed the scheduler.
    """
    def _fetch():
        r = requests.get(
            WEATHER_URL,
            params={
                "latitude": LAT, "longitude": LON,
                "daily":    "sunrise,sunset",
                "timezone": "auto",
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


def get_measured_weather(sunrise, sunset):
    """
    Fetch hourly weather data from sunrise to sunset (measured hours only).
    Returns list of dicts: {"hour": datetime, "temp": float, "cloud": int}
    """
    def _fetch():
        r = requests.get(
            WEATHER_URL,
            params={
                "latitude":  LAT, "longitude": LON,
                "hourly":    "temperature_2m,cloudcover",
                "timezone":  "auto"
            },
            timeout=10
        )
        r.raise_for_status()
        return r.json()

    try:
        data = with_retries(_fetch, label="hourly weather fetch")
    except requests.exceptions.Timeout:
        raise RuntimeError("Weather API timed out after all retries.") from None
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Weather API failed: {e}") from e

    today = date.today()
    now   = datetime.now()
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
        
        # Only measured hours: today, from sunrise to sunset (or now if before sunset)
        end_time = min(now, sunset)
        if ts.date() == today and sunrise <= ts <= end_time:
            weather.append({"hour": ts, "temp": temp, "cloud": clouds})

    log.info(f"Collected {len(weather)} measured hour(s) between "
             f"{sunrise.strftime('%H:%M')} and {end_time.strftime('%H:%M')}")
    return weather


def weighted_average(weather, sunrise, sunset):
    """
    Weight measured hours by position in the day:
      - First 3hrs after sunrise       → 0.5  (morning warm-up)
      - Middle (sunrise+3h → sunset-2h) → 1.0  (peak hours)
      - Last 2hrs before sunset        → 0.5  (cooling)
    """
    total_weight = temp_sum = cloud_sum = 0
    for w in weather:
        t = w["hour"]
        if   sunrise <= t < sunrise + timedelta(hours=3):
            weight = 0.5
        elif sunrise + timedelta(hours=3) <= t <= sunset - timedelta(hours=2):
            weight = 1.0
        elif sunset - timedelta(hours=2) < t <= sunset:
            weight = 0.5
        else:
            weight = 0
        if weight > 0:
            temp_sum     += w["temp"]  * weight
            cloud_sum    += w["cloud"] * weight
            total_weight += weight

    if total_weight == 0:
        raise ValueError("No measured weather data available for weighting.")
    return temp_sum / total_weight, cloud_sum / total_weight


def calculate_duration(temp):
    """Linear interpolation in TEMP_LUT."""
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
    """Split duration into first_run and second_run."""
    if duration <= MAX_FIRST_RUN:
        return duration, 0
    return MAX_FIRST_RUN, duration - MAX_FIRST_RUN


# ==========================================================
# HELPER: SAFE NUMBER CONVERSION
# ==========================================================
def _safe_num(value, cast=float, fallback=None):
    """Safely cast a CSV string value; return fallback if '—' or invalid."""
    try:
        return cast(value) if value not in (None, "", "—", "N/A") else fallback
    except (ValueError, TypeError):
        return fallback


# ==========================================================
# DATA MANAGER — Single CSV file: daily_log.csv
# ==========================================================
class DataManager:
    """
    Single-file CSV manager: daily_log.csv
    One row per day with hourly temp/cloud columns (dawn→dusk only)
    """
    
    DAILY_LOG_FILE = "daily_log.csv"
    
    # Fixed metadata columns
    METADATA_COLS = [
        "date", "dawn", "dusk", "avg_temp", "avg_cloud", "effective_temp",
        "duration", "first_run", "second_run", "trigger_time", "ha_status"
    ]
    
    def __init__(self):
        self._cache = None
        self._ensure_file_exists()
    
    def _ensure_file_exists(self):
        """Create daily_log.csv with header if it doesn't exist."""
        if not os.path.exists(self.DAILY_LOG_FILE):
            with _csv_lock:
                with open(self.DAILY_LOG_FILE, 'w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(self.METADATA_COLS)
            log.info(f"Created {self.DAILY_LOG_FILE}")
    
    def invalidate(self):
        """Clear cache - forces reload on next read."""
        self._cache = None
    
    def execute_today(self, sunrise, sunset):
        """
        Main execution flow:
        1. Fetch measured weather
        2. Calculate duration
        3. Send to HA
        4. Save to CSV
        """
        try:
            weather = get_measured_weather(sunrise, sunset)
        except Exception as e:
            log.error(f"Could not fetch measured weather: {e}")
            return None

        avg_temp, avg_cloud = weighted_average(weather, sunrise, sunset)
        effective_temp      = round(avg_temp - (avg_cloud / 100) * CLOUD_PENALTY_FACTOR, 2)
        duration            = calculate_duration(effective_temp)
        first_run, second_run = calculate_sliders(duration)

        log.info(
            f"Calculated → effective_temp={effective_temp}°C | "
            f"duration={duration} min | slider1={first_run} | slider2={second_run}"
        )

        ha_ok        = send_to_ha(first_run, second_run)
        execute_time = datetime.now().strftime("%H:%M:%S")
        ha_status    = "OK" if ha_ok else "FAILED"

        row_data = {
            "date":           str(date.today()),
            "dawn":           sunrise.strftime("%H:%M"),
            "dusk":           sunset.strftime("%H:%M"),
            "avg_temp":       round(avg_temp, 2),
            "avg_cloud":      round(avg_cloud, 2),
            "effective_temp": effective_temp,
            "duration":       duration,
            "first_run":      first_run,
            "second_run":     second_run,
            "trigger_time":   execute_time,
            "ha_status":      ha_status
        }

        self.save_record(row_data, weather)
        log.info(f"[{execute_time}] CSV saved → HA status: {ha_status}")
        self.invalidate()
        return row_data
    
    def save_record(self, row_data: Dict[str, Any], hourly_weather: List[Dict]) -> bool:
        """
        Save one day's record to daily_log.csv.
        """
        try:
            full_row = self._build_row_with_hourly(row_data, hourly_weather)
            
            with _csv_lock:
                existing_rows = self._read_all_rows()
                
                # Remove today's row if exists
                today_str = row_data["date"]
                existing_rows = [r for r in existing_rows if r.get("date") != today_str]
                
                # Append new row
                existing_rows.append(full_row)
                
                # Write back atomically
                self._write_all_rows(existing_rows)
            
            log.info(f"Saved record for {today_str} to {self.DAILY_LOG_FILE}")
            return True
            
        except Exception as e:
            log.error(f"Failed to save record: {e}")
            return False
    
    def _build_row_with_hourly(self, row_data: Dict, hourly_weather: List[Dict]) -> Dict[str, str]:
        """Build CSV row dict with hourly temp/cloud columns."""
        row = {k: str(row_data.get(k, "")) for k in self.METADATA_COLS}
        
        # Add hourly data (dawn to dusk only)
        for entry in hourly_weather:
            hour_dt = entry["hour"]  # datetime object
            hour_str = hour_dt.strftime("%H")  # "06", "07", etc.
            
            row[f"temp_{hour_str}"] = str(round(entry["temp"], 1))
            row[f"cloud_{hour_str}"] = str(int(entry["cloud"]))
        
        return row
    
    def _read_all_rows(self) -> List[Dict[str, str]]:
        """Read all rows from daily_log.csv."""
        if not os.path.exists(self.DAILY_LOG_FILE):
            return []
        
        with open(self.DAILY_LOG_FILE, 'r', newline='') as f:
            reader = csv.DictReader(f)
            return list(reader)
    
    def _write_all_rows(self, rows: List[Dict[str, str]]):
        """Write all rows back to daily_log.csv atomically."""
        if not rows:
            with open(self.DAILY_LOG_FILE, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(self.METADATA_COLS)
            return
        
        # Collect all unique column names
        all_cols = set()
        for row in rows:
            all_cols.update(row.keys())
        
        # Sort: metadata first, then hourly columns sorted
        hourly_cols = sorted([c for c in all_cols if c.startswith(("temp_", "cloud_"))])
        ordered_cols = self.METADATA_COLS + hourly_cols
        
        # Atomic write via temp file
        tmp_file = self.DAILY_LOG_FILE + ".tmp"
        with open(tmp_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=ordered_cols)
            writer.writeheader()
            for row in rows:
                full_row = {col: row.get(col, "") for col in ordered_cols}
                writer.writerow(full_row)
        
        os.replace(tmp_file, self.DAILY_LOG_FILE)  # Atomic on Linux
    
    def all_records(self) -> List[Dict[str, Any]]:
        """Return all daily records (metadata only), sorted by date."""
        if self._cache is not None:
            return self._cache
        
        rows = self._read_all_rows()
        
        # Extract only metadata columns
        records = []
        for row in rows:
            record = {col: row.get(col, "—") for col in self.METADATA_COLS}
            records.append(record)
        
        # Sort by date
        try:
            records.sort(key=lambda x: datetime.strptime(x["date"], "%Y-%m-%d"))
        except ValueError as e:
            log.warning(f"Could not sort by date: {e}")
        
        self._cache = records
        return self._cache
    
    def get_hourly_data(self, date_str: str) -> Optional[List[Dict[str, Any]]]:
        """
        Get hourly temp/cloud data for a specific date.
        Returns list of dicts: {"hour": str, "temp": float, "cloud": int}
        """
        rows = self._read_all_rows()
        
        for row in rows:
            if row.get("date") == date_str:
                hourly = []
                hour_keys = sorted([k for k in row.keys() if k.startswith("temp_")])
                
                for temp_key in hour_keys:
                    hour_str = temp_key.replace("temp_", "")
                    cloud_key = f"cloud_{hour_str}"
                    
                    temp_val = _safe_num(row.get(temp_key), cast=float)
                    cloud_val = _safe_num(row.get(cloud_key), cast=int)
                    
                    if temp_val is not None and cloud_val is not None:
                        hourly.append({
                            "hour": hour_str,
                            "temp": temp_val,
                            "cloud": cloud_val
                        })
                
                return hourly if hourly else None
        
        return None
    
    def retry_ha(self):
        """Re-send today's values to HA."""
        today_str = str(date.today())
        rows = self._read_all_rows()
        
        today_row = None
        for row in rows:
            if row.get("date") == today_str:
                today_row = row
                break
        
        if not today_row:
            log.warning("No record found for today — cannot retry HA.")
            return False, "No data for today"
        
        first_run = _safe_num(today_row.get("first_run"), cast=int, fallback=0)
        second_run = _safe_num(today_row.get("second_run"), cast=int, fallback=0)
        
        ha_ok = send_to_ha(first_run, second_run)
        ha_status = "OK" if ha_ok else "FAILED"
        
        # Update status and trigger time
        today_row["ha_status"] = ha_status
        today_row["trigger_time"] = datetime.now().strftime("%H:%M:%S")
        
        # Write back
        with _csv_lock:
            all_rows = self._read_all_rows()
            updated_rows = [r if r.get("date") != today_str else today_row for r in all_rows]
            self._write_all_rows(updated_rows)
        
        log.info(f"Retry HA complete → {ha_status}")
        self.invalidate()
        return ha_ok, ha_status


# ==========================================================
# SCHEDULER — Backward calculation from target time
# ==========================================================
class Scheduler:
    """
    New flow:
    1. At 16:45 (FIRST_CHECK_HOUR): Fetch weather, calculate duration, compute trigger time
    2. Wait until trigger time
    3. Execute (send to HA)
    4. Sleep until next day
    """
    
    # Check time: 2 hours before target (if target is 18:45 → check at 16:45)
    HOURS_BEFORE_TARGET = 2
    
    def __init__(self, data_manager: DataManager):
        self.dm = data_manager
        self.thread = None
        self.running = False
        self.on_state_change = None  # Callback for GUI
        
        # Parse target time
        target_parts = FIRST_RUN_TARGET_TIME.split(":")
        self.target_hour = int(target_parts[0])
        self.target_minute = int(target_parts[1])
        
        # Calculate first check time (2 hours before target)
        check_time = datetime.now().replace(
            hour=self.target_hour, minute=self.target_minute, second=0, microsecond=0
        ) - timedelta(hours=self.HOURS_BEFORE_TARGET)
        self.check_hour = check_time.hour
        self.check_minute = check_time.minute
        
        log.info(f"Scheduler initialized: target={FIRST_RUN_TARGET_TIME}, "
                 f"first_check={self.check_hour:02d}:{self.check_minute:02d}")
    
    def start(self):
        """Start scheduler thread."""
        if self.running:
            return
        self.running = True
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()
        log.info("Scheduler started.")
    
    def stop(self):
        """Stop scheduler thread."""
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
        log.info("Scheduler stopped.")
    
    def _set_state(self, state: str):
        """Update state and notify GUI."""
        if self.on_state_change:
            self.on_state_change(state)
    
    def _loop(self):
        """Main scheduler loop."""
        while self.running:
            try:
                now = datetime.now()
                today_str = str(date.today())
                
                # Check if already executed today
                records = self.dm.all_records()
                already_done = any(r["date"] == today_str for r in records)
                
                if already_done:
                    # Already executed today - wait until tomorrow
                    self._set_state("WAIT_NEXT_DAY")
                    tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
                    sleep_seconds = (tomorrow - now).total_seconds()
                    log.info(f"Already executed today. Sleeping until tomorrow ({sleep_seconds/3600:.1f}h)")
                    time.sleep(min(sleep_seconds, 300))  # Wake up every 5 min to check
                    continue
                
                # Calculate next check time
                check_today = now.replace(
                    hour=self.check_hour, minute=self.check_minute, second=0, microsecond=0
                )
                
                if now < check_today:
                    # Wait until first check time
                    self._set_state("WAIT_FIRST_CHECK")
                    sleep_seconds = (check_today - now).total_seconds()
                    log.info(f"Waiting until first check at {check_today.strftime('%H:%M')} "
                             f"({sleep_seconds/3600:.1f}h)")
                    time.sleep(min(sleep_seconds, 300))
                    continue
                
                # Time to check/execute
                log.info("=== Starting daily calculation ===")
                self._set_state("FETCH_SUN")
                
                try:
                    sunrise, sunset = get_sun_times()
                except Exception as e:
                    log.error(f"Failed to fetch sun times: {e}. Retrying in 10 min.")
                    time.sleep(600)
                    continue
                
                # Fetch weather and calculate
                self._set_state("CALCULATING")
                try:
                    weather = get_measured_weather(sunrise, sunset)
                    avg_temp, avg_cloud = weighted_average(weather, sunrise, sunset)
                    effective_temp = round(avg_temp - (avg_cloud / 100) * CLOUD_PENALTY_FACTOR, 2)
                    duration = calculate_duration(effective_temp)
                    first_run, second_run = calculate_sliders(duration)
                    
                    log.info(f"Calculated: duration={duration}min, first_run={first_run}min")
                    
                except Exception as e:
                    log.error(f"Calculation failed: {e}. Retrying in 10 min.")
                    time.sleep(600)
                    continue
                
                # Calculate trigger time (backwards from target)
                target_today = now.replace(
                    hour=self.target_hour, minute=self.target_minute, second=0, microsecond=0
                )
                trigger_time = target_today - timedelta(minutes=first_run)
                
                log.info(f"Target ready time: {target_today.strftime('%H:%M')}")
                log.info(f"Calculated trigger time: {trigger_time.strftime('%H:%M')}")
                
                # Check if trigger time is in the past
                now = datetime.now()
                if trigger_time <= now:
                    log.warning(f"Trigger time is in the past! Executing immediately.")
                    self._execute_now(sunrise, sunset)
                    continue
                
                # Wait until trigger time
                sleep_seconds = (trigger_time - now).total_seconds()
                self._set_state("WAIT_TRIGGER")
                log.info(f"Waiting until trigger time {trigger_time.strftime('%H:%M')} "
                         f"({sleep_seconds/60:.1f} min)")
                
                # Sleep in small chunks to allow clean shutdown
                while self.running and datetime.now() < trigger_time:
                    time.sleep(min(60, (trigger_time - datetime.now()).total_seconds()))
                
                if not self.running:
                    break
                
                # Execute!
                self._execute_now(sunrise, sunset)
                
            except Exception as e:
                log.error(f"Scheduler error: {e}", exc_info=True)
                time.sleep(300)  # Sleep 5 min on error
    
    def _execute_now(self, sunrise, sunset):
        """Execute the boiler control."""
        self._set_state("EXECUTE")
        log.info("=== EXECUTING BOILER CONTROL ===")
        try:
            self.dm.execute_today(sunrise, sunset)
            log.info("=== EXECUTION COMPLETE ===")
        except Exception as e:
            log.error(f"Execution failed: {e}", exc_info=True)
        self._set_state("WAIT_NEXT_DAY")


# ==========================================================
# GUI — Tooltip for hourly data
# ==========================================================
class HourlyTooltip:
    """Shows hourly data tooltip on hover over date cell."""
    
    def __init__(self, tree, dm: DataManager):
        self.tree = tree
        self.dm = dm
        self.tooltip_window = None
        self.current_item = None
        
        # Bind hover events
        tree.bind('<Motion>', self._on_motion)
        tree.bind('<Leave>', self._hide_tooltip)
    
    def _on_motion(self, event):
        """Show tooltip when hovering over date column."""
        item = self.tree.identify_row(event.y)
        column = self.tree.identify_column(event.x)
        
        # Show tooltip only on date column (column #1)
        if item and column == "#1":
            if item != self.current_item:
                self.current_item = item
                self._show_tooltip(item, event)
        else:
            self._hide_tooltip()
    
    def _show_tooltip(self, item, event):
        """Display tooltip with hourly data."""
        self._hide_tooltip()
        
        # Get date from row
        values = self.tree.item(item, 'values')
        if not values or len(values) < 2:
            return
        
        date_str = values[1]  # Column index 1 is date
        
        # Fetch hourly data
        hourly = self.dm.get_hourly_data(date_str)
        if not hourly:
            return
        
        # Create tooltip window
        self.tooltip_window = tw = tk.Toplevel(self.tree)
        tw.wm_overrideredirect(True)
        tw.wm_geometry(f"+{event.x_root+10}+{event.y_root+10}")
        
        # Dark theme
        bg_color = "#2b2b2b"
        fg_color = "#e0e0e0"
        
        frame = tk.Frame(tw, background=bg_color, relief="solid", borderwidth=1)
        frame.pack()
        
        # Title
        title = tk.Label(frame, text=f"Hourly Data — {date_str}", 
                        font=("Consolas", 10, "bold"),
                        background=bg_color, foreground="#f0c060", padx=10, pady=5)
        title.pack()
        
        # Header
        header = tk.Label(frame, text="Hour  │ Temp   │ Cloud",
                         font=("Consolas", 9, "bold"),
                         background=bg_color, foreground=fg_color, padx=10, pady=2)
        header.pack()
        
        # Data rows
        for entry in hourly:
            text = f"{entry['hour']}:00 │ {entry['temp']:5.1f}° │ {entry['cloud']:3d}%"
            label = tk.Label(frame, text=text, font=("Consolas", 9),
                           background=bg_color, foreground=fg_color, padx=10, pady=1)
            label.pack()
    
    def _hide_tooltip(self, event=None):
        """Hide tooltip."""
        if self.tooltip_window:
            self.tooltip_window.destroy()
            self.tooltip_window = None
        self.current_item = None


# ==========================================================
# GUI — Main Application
# ==========================================================
class BoilerApp:
    def __init__(self, root, data_manager: DataManager, scheduler: Scheduler):
        self.root = root
        self.dm = data_manager
        self.scheduler = scheduler
        self.startup_time = datetime.now()
        
        # State variable for status bar
        self._state_var = tk.StringVar(value="💤 INITIALIZING")
        
        # Dark color scheme
        self._clr = {
            "BG":     "#1e1e1e",
            "BG2":    "#252526",
            "BG3":    "#3e3e42",
            "FG":     "#e0e0e0",
            "FG_DIM": "#a0a0a0",
            "SEP":    "#555555",
            "ACCENT": "#007acc"
        }
        
        self.root.title("Boiler Control System")
        self.root.geometry("1200x700")
        self.root.configure(bg=self._clr["BG"])
        
        # Create notebook (tabs)
        self.notebook = ttk.Notebook(root)
        self.notebook.pack(fill="both", expand=True, padx=5, pady=5)
        
        # Tab 1: Data table
        self.tab_data = tk.Frame(self.notebook, bg=self._clr["BG"])
        self.notebook.add(self.tab_data, text="  📊  Daily Records  ")
        
        # Tab 2: Settings
        self.tab_settings = tk.Frame(self.notebook, bg=self._clr["BG"])
        self.notebook.add(self.tab_settings, text="  ⚙️  Settings  ")
        
        # Build tabs
        self.create_data_table()
        self.create_settings_tab()
        self.create_status_bar()
        
        # Load initial data
        self.load_data()
        
        # Start clock
        self._tick()
    
    # ------------------------------------------------------
    # TAB 1: DATA TABLE
    # ------------------------------------------------------
    def create_data_table(self):
        # Treeview
        cols = (
            "#", "Date", "Dawn", "Dusk", "Avg Temp", "Avg Cloud", "Eff Temp",
            "Duration", "1st Run", "2nd Run", "Trigger", "HA Status"
        )
        
        self.tree = ttk.Treeview(self.tab_data, columns=cols, show="headings", height=20)
        
        widths = [40, 90, 60, 60, 80, 80, 80, 80, 70, 70, 80, 90]
        for col, w in zip(cols, widths):
            self.tree.heading(col, text=col)
            self.tree.column(col, width=w, anchor="center")
        
        # Scrollbar
        vsb = ttk.Scrollbar(self.tab_data, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)
        
        self.tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")
        
        # Row colors
        self.tree.tag_configure("ha_ok_odd",  background="#1a2e1a", foreground="#a0e8a0")
        self.tree.tag_configure("ha_ok_even", background="#162e24", foreground="#b0e8c8")
        self.tree.tag_configure("ha_failed",  background="#3a1e1e", foreground="#f5a0a0")
        self.tree.tag_configure("ha_unknown", background="#2a2a2e", foreground="#aaaaaa")
        
        # Attach tooltip
        self.tooltip = HourlyTooltip(self.tree, self.dm)
    
    def load_data(self):
        """Load data into table."""
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
                row["date"],         row["dawn"],          row["dusk"],
                row["avg_temp"],     row["avg_cloud"],     row["effective_temp"],
                row["duration"],     row["first_run"],     row["second_run"],
                row["trigger_time"], row["ha_status"]
            ), tags=(tag,))
    
    # ------------------------------------------------------
    # TAB 2: SETTINGS
    # ------------------------------------------------------
    def create_settings_tab(self):
        """Create settings tab with compact layout - graph dominates."""
        c = self._clr
        
        # Main container
        main_frame = tk.Frame(self.tab_settings, bg=c["BG"])
        main_frame.pack(fill="both", expand=True, padx=10, pady=10)
        
        # ========== TOP: COMPACT CONTROLS (Target Time + Cloud Penalty in ONE ROW) ==========
        top_frame = tk.Frame(main_frame, bg=c["BG2"], relief="solid", borderwidth=1)
        top_frame.pack(fill="x", pady=(0, 10))
        
        # Left side: Target time
        tk.Label(
            top_frame, text="First run ready by:",
            bg=c["BG2"], fg=c["FG"], font=("Segoe UI", 9)
        ).grid(row=0, column=0, sticky="e", padx=(15, 5), pady=10)
        
        target_parts = FIRST_RUN_TARGET_TIME.split(":")
        self.target_hour_var = tk.StringVar(value=target_parts[0])
        self.target_min_var = tk.StringVar(value=target_parts[1])
        
        time_frame = tk.Frame(top_frame, bg=c["BG2"])
        time_frame.grid(row=0, column=1, sticky="w", pady=10)
        
        tk.Entry(
            time_frame, textvariable=self.target_hour_var, width=3,
            font=("Consolas", 11), justify="center", bg=c["BG3"], fg=c["FG"]
        ).pack(side="left")
        tk.Label(time_frame, text=":", bg=c["BG2"], fg=c["FG"],
                font=("Consolas", 11, "bold")).pack(side="left", padx=2)
        tk.Entry(
            time_frame, textvariable=self.target_min_var, width=3,
            font=("Consolas", 11), justify="center", bg=c["BG3"], fg=c["FG"]
        ).pack(side="left")
        
        # Separator
        tk.Label(top_frame, text="│", bg=c["BG2"], fg=c["SEP"],
                font=("Segoe UI", 14)).grid(row=0, column=2, padx=20)
        
        # Right side: Cloud penalty
        tk.Label(
            top_frame, text="Cloud penalty:",
            bg=c["BG2"], fg=c["FG"], font=("Segoe UI", 9)
        ).grid(row=0, column=3, sticky="e", padx=(0, 5), pady=10)
        
        self.cloud_penalty_var = tk.StringVar(value=str(CLOUD_PENALTY_FACTOR))
        tk.Entry(
            top_frame, textvariable=self.cloud_penalty_var, width=5,
            font=("Consolas", 11), justify="center", bg=c["BG3"], fg=c["FG"]
        ).grid(row=0, column=4, sticky="w", pady=10, padx=(0, 15))
        
        # ========== MIDDLE: LUT TABLE + GRAPH SPLIT ==========
        middle_frame = tk.Frame(main_frame, bg=c["BG"])
        middle_frame.pack(fill="both", expand=True)
        
        # LEFT: LUT Table (30% width)
        lut_frame = tk.LabelFrame(
            middle_frame, text="  Temperature LUT  ",
            bg=c["BG"], fg=c["FG"], font=("Segoe UI", 10, "bold"),
            padx=10, pady=10
        )
        lut_frame.pack(side="left", fill="both", padx=(0, 5))
        
        # LUT Treeview (max 10 rows visible)
        lut_cols = ("Temp (°C)", "Duration (min)")
        self.lut_tree = ttk.Treeview(
            lut_frame, columns=lut_cols, show="headings", height=10
        )
        
        self.lut_tree.heading("Temp (°C)", text="Temp (°C)")
        self.lut_tree.heading("Duration (min)", text="Duration (min)")
        self.lut_tree.column("Temp (°C)", width=80, anchor="center")
        self.lut_tree.column("Duration (min)", width=100, anchor="center")
        
        lut_scroll = ttk.Scrollbar(lut_frame, orient="vertical", command=self.lut_tree.yview)
        self.lut_tree.configure(yscrollcommand=lut_scroll.set)
        
        self.lut_tree.pack(side="left", fill="both", expand=True)
        lut_scroll.pack(side="right", fill="y")
        
        # Load current LUT
        self._load_lut_to_tree()
        
        # LUT buttons (VERTICAL at bottom)
        btn_frame = tk.Frame(lut_frame, bg=c["BG"])
        btn_frame.pack(fill="x", pady=(10, 0))
        
        btn_style = {
            "bg": c["BG3"], "fg": c["FG"], "font": ("Segoe UI", 9),
            "relief": "flat", "padx": 8, "pady": 4, "cursor": "hand2", "width": 18
        }
        
        tk.Button(btn_frame, text="➕ Add Row", command=self._add_lut_row, **btn_style).pack(fill="x", pady=2)
        tk.Button(btn_frame, text="✏️ Edit Selected", command=self._edit_lut_row, **btn_style).pack(fill="x", pady=2)
        tk.Button(btn_frame, text="🗑️ Delete Selected", command=self._delete_lut_row, **btn_style).pack(fill="x", pady=2)
        
        # RIGHT: Graph (70% width)
        graph_frame = tk.LabelFrame(
            middle_frame, text="  Cloud Penalty Impact Visualization  ",
            bg=c["BG"], fg=c["FG"], font=("Segoe UI", 10, "bold"),
            padx=10, pady=10
        )
        graph_frame.pack(side="right", fill="both", expand=True)
        
        # Example input (compact, horizontal)
        example_frame = tk.Frame(graph_frame, bg=c["BG2"], relief="solid", borderwidth=1)
        example_frame.pack(fill="x", pady=(0, 8))
        
        tk.Label(
            example_frame, text="Test:",
            bg=c["BG2"], fg=c["FG"], font=("Segoe UI", 8, "bold")
        ).pack(side="left", padx=(8, 5), pady=6)
        
        # Temp
        tk.Label(example_frame, text="Temp:", bg=c["BG2"], fg=c["FG"],
                font=("Segoe UI", 8)).pack(side="left", padx=(0, 2))
        self.example_temp_var = tk.StringVar(value="12")
        tk.Entry(example_frame, textvariable=self.example_temp_var, width=4,
                font=("Consolas", 9), justify="center", bg=c["BG3"], fg=c["FG"]).pack(side="left")
        tk.Label(example_frame, text="°C", bg=c["BG2"], fg=c["FG_DIM"],
                font=("Segoe UI", 8)).pack(side="left", padx=(1, 10))
        
        # Cloud
        tk.Label(example_frame, text="Clouds:", bg=c["BG2"], fg=c["FG"],
                font=("Segoe UI", 8)).pack(side="left", padx=(0, 2))
        self.example_cloud_var = tk.StringVar(value="50")
        tk.Entry(example_frame, textvariable=self.example_cloud_var, width=4,
                font=("Consolas", 9), justify="center", bg=c["BG3"], fg=c["FG"]).pack(side="left")
        tk.Label(example_frame, text="%", bg=c["BG2"], fg=c["FG_DIM"],
                font=("Segoe UI", 8)).pack(side="left", padx=(1, 10))
        
        # Update button
        tk.Button(
            example_frame, text="📊 Update",
            command=self._update_lut_graph,
            bg="#007acc", fg="#ffffff", font=("Segoe UI", 8, "bold"),
            relief="flat", padx=8, pady=2, cursor="hand2"
        ).pack(side="left", padx=5)
        
        # Calculation results (compact)
        self.calc_result_frame = tk.Frame(graph_frame, bg=c["BG2"], relief="solid", borderwidth=1)
        self.calc_result_frame.pack(fill="x", pady=(0, 8))
        
        self.lbl_calc_result = tk.Label(
            self.calc_result_frame, text="Click 'Update' to see calculation",
            bg=c["BG2"], fg=c["FG_DIM"], font=("Consolas", 8),
            justify="left", padx=8, pady=5
        )
        self.lbl_calc_result.pack(fill="x")
        
        # Graph canvas (takes most space)
        self.graph_container = graph_frame
        self.lut_graph_canvas = None
        self._update_lut_graph()
        
        # ========== BOTTOM: SAVE BUTTON ==========
        save_frame = tk.Frame(main_frame, bg=c["BG"])
        save_frame.pack(fill="x", pady=(10, 0))
        
        tk.Button(
            save_frame, text="💾  Save All Settings",
            command=self._save_settings,
            bg="#007acc", fg="#ffffff", font=("Segoe UI", 11, "bold"),
            relief="flat", padx=30, pady=10, cursor="hand2"
        ).pack(side="left")
        
        tk.Label(
            save_frame, text="Changes apply immediately",
            bg=c["BG"], fg=c["FG_DIM"], font=("Segoe UI", 9, "italic")
        ).pack(side="left", padx=15)
    
    # ------------------------------------------------------
    # LUT EDITOR METHODS
    # ------------------------------------------------------
    def _load_lut_to_tree(self):
        """Load current TEMP_LUT into treeview."""
        self.lut_tree.delete(*self.lut_tree.get_children())
        
        for temp in sorted(TEMP_LUT.keys()):
            duration = TEMP_LUT[temp]
            self.lut_tree.insert("", "end", values=(temp, duration))
    
    def _add_lut_row(self):
        """Add new temperature point."""
        dialog = tk.Toplevel(self.root)
        dialog.title("Add Temperature Point")
        dialog.geometry("320x180")
        dialog.configure(bg=self._clr["BG"])
        dialog.transient(self.root)
        dialog.grab_set()
        dialog.resizable(False, False)
        
        # Center dialog
        dialog.update_idletasks()
        x = (dialog.winfo_screenwidth() // 2) - (320 // 2)
        y = (dialog.winfo_screenheight() // 2) - (180 // 2)
        dialog.geometry(f"+{x}+{y}")
        
        main_frame = tk.Frame(dialog, bg=self._clr["BG"], padx=20, pady=15)
        main_frame.pack(fill="both", expand=True)
        
        # Temperature
        tk.Label(main_frame, text="Temperature (°C):", bg=self._clr["BG"], 
                fg=self._clr["FG"], font=("Segoe UI", 10)).pack(anchor="w", pady=(0, 5))
        temp_var = tk.StringVar()
        temp_entry = tk.Entry(main_frame, textvariable=temp_var, width=15, 
                             font=("Consolas", 11), bg=self._clr["BG3"], fg=self._clr["FG"])
        temp_entry.pack(fill="x", pady=(0, 15))
        temp_entry.focus()
        
        # Duration
        tk.Label(main_frame, text="Duration (minutes):", bg=self._clr["BG"], 
                fg=self._clr["FG"], font=("Segoe UI", 10)).pack(anchor="w", pady=(0, 5))
        dur_var = tk.StringVar()
        dur_entry = tk.Entry(main_frame, textvariable=dur_var, width=15, 
                            font=("Consolas", 11), bg=self._clr["BG3"], fg=self._clr["FG"])
        dur_entry.pack(fill="x", pady=(0, 20))
        
        def save():
            try:
                temp = int(temp_var.get())
                duration = int(dur_var.get())
                if duration <= 0:
                    raise ValueError("Duration must be positive")
                
                TEMP_LUT[temp] = duration
                self._load_lut_to_tree()
                self._update_lut_graph()
                dialog.destroy()
            except ValueError as e:
                messagebox.showerror("Invalid Input", f"Error: {e}", parent=dialog)
        
        # Buttons
        btn_frame = tk.Frame(main_frame, bg=self._clr["BG"])
        btn_frame.pack(fill="x")
        
        tk.Button(btn_frame, text="Add", command=save, bg="#007acc", fg="#ffffff",
                 font=("Segoe UI", 10, "bold"), relief="flat", padx=20, pady=8,
                 cursor="hand2", width=10).pack(side="left", padx=(0, 10))
        tk.Button(btn_frame, text="Cancel", command=dialog.destroy, bg=self._clr["BG3"],
                 fg=self._clr["FG"], font=("Segoe UI", 10), relief="flat", padx=20,
                 pady=8, cursor="hand2", width=10).pack(side="left")
        
        # Enter key to save
        dialog.bind('<Return>', lambda e: save())
    
    def _edit_lut_row(self):
        """Edit selected temperature point."""
        selected = self.lut_tree.selection()
        if not selected:
            messagebox.showwarning("No Selection", "Please select a row to edit")
            return
        
        item = selected[0]
        values = self.lut_tree.item(item, 'values')
        old_temp = int(values[0])
        old_duration = int(values[1])
        
        dialog = tk.Toplevel(self.root)
        dialog.title("Edit Temperature Point")
        dialog.geometry("320x180")
        dialog.configure(bg=self._clr["BG"])
        dialog.transient(self.root)
        dialog.grab_set()
        dialog.resizable(False, False)
        
        # Center dialog
        dialog.update_idletasks()
        x = (dialog.winfo_screenwidth() // 2) - (320 // 2)
        y = (dialog.winfo_screenheight() // 2) - (180 // 2)
        dialog.geometry(f"+{x}+{y}")
        
        main_frame = tk.Frame(dialog, bg=self._clr["BG"], padx=20, pady=15)
        main_frame.pack(fill="both", expand=True)
        
        # Temperature
        tk.Label(main_frame, text="Temperature (°C):", bg=self._clr["BG"], 
                fg=self._clr["FG"], font=("Segoe UI", 10)).pack(anchor="w", pady=(0, 5))
        temp_var = tk.StringVar(value=str(old_temp))
        temp_entry = tk.Entry(main_frame, textvariable=temp_var, width=15, 
                             font=("Consolas", 11), bg=self._clr["BG3"], fg=self._clr["FG"])
        temp_entry.pack(fill="x", pady=(0, 15))
        temp_entry.focus()
        temp_entry.select_range(0, tk.END)
        
        # Duration
        tk.Label(main_frame, text="Duration (minutes):", bg=self._clr["BG"], 
                fg=self._clr["FG"], font=("Segoe UI", 10)).pack(anchor="w", pady=(0, 5))
        dur_var = tk.StringVar(value=str(old_duration))
        dur_entry = tk.Entry(main_frame, textvariable=dur_var, width=15, 
                            font=("Consolas", 11), bg=self._clr["BG3"], fg=self._clr["FG"])
        dur_entry.pack(fill="x", pady=(0, 20))
        
        def save():
            try:
                new_temp = int(temp_var.get())
                new_duration = int(dur_var.get())
                if new_duration <= 0:
                    raise ValueError("Duration must be positive")
                
                # Remove old entry if temp changed
                if new_temp != old_temp:
                    del TEMP_LUT[old_temp]
                
                TEMP_LUT[new_temp] = new_duration
                self._load_lut_to_tree()
                self._update_lut_graph()
                dialog.destroy()
            except ValueError as e:
                messagebox.showerror("Invalid Input", f"Error: {e}", parent=dialog)
        
        # Buttons
        btn_frame = tk.Frame(main_frame, bg=self._clr["BG"])
        btn_frame.pack(fill="x")
        
        tk.Button(btn_frame, text="Save", command=save, bg="#007acc", fg="#ffffff",
                 font=("Segoe UI", 10, "bold"), relief="flat", padx=20, pady=8,
                 cursor="hand2", width=10).pack(side="left", padx=(0, 10))
        tk.Button(btn_frame, text="Cancel", command=dialog.destroy, bg=self._clr["BG3"],
                 fg=self._clr["FG"], font=("Segoe UI", 10), relief="flat", padx=20,
                 pady=8, cursor="hand2", width=10).pack(side="left")
        
        # Enter key to save
        dialog.bind('<Return>', lambda e: save())
    
    def _delete_lut_row(self):
        """Delete selected temperature point."""
        selected = self.lut_tree.selection()
        if not selected:
            messagebox.showwarning("No Selection", "Please select a row to delete")
            return
        
        if len(TEMP_LUT) <= 2:
            messagebox.showerror("Cannot Delete", "LUT must have at least 2 points")
            return
        
        item = selected[0]
        values = self.lut_tree.item(item, 'values')
        temp = int(values[0])
        
        if messagebox.askyesno("Confirm Delete", f"Delete temperature point {temp}°C?"):
            del TEMP_LUT[temp]
            self._load_lut_to_tree()
            self._update_lut_graph()
    
    def _update_lut_graph(self):
        """Redraw LUT preview graph with example calculation."""
        if self.lut_graph_canvas:
            self.lut_graph_canvas.get_tk_widget().destroy()
        
        if not TEMP_LUT:
            return
        
        # Get example values
        try:
            example_temp = float(self.example_temp_var.get())
            example_cloud = float(self.example_cloud_var.get())
            penalty = float(self.cloud_penalty_var.get())
        except (ValueError, AttributeError):
            example_temp = 12.0
            example_cloud = 50.0
            penalty = CLOUD_PENALTY_FACTOR
        
        # Calculate durations for example
        base_duration = calculate_duration(example_temp)
        effective_temp = example_temp - (example_cloud / 100) * penalty
        duration_with_penalty = calculate_duration(effective_temp)
        
        # Update calculation display
        diff = duration_with_penalty - base_duration
        diff_percent = (diff / base_duration * 100) if base_duration > 0 else 0
        
        calc_text = (
            f"Temp={example_temp:.1f}°C, Clouds={example_cloud:.0f}%  →  "
            f"Without: {base_duration}min | With: {duration_with_penalty}min | "
            f"Diff: +{diff}min ({diff_percent:+.1f}%)"
        )
        
        if hasattr(self, 'lbl_calc_result'):
            self.lbl_calc_result.config(text=calc_text)
        
        # Create figure (larger for better visibility)
        fig = plt.Figure(figsize=(7, 5), facecolor=self._clr["BG"])
        ax = fig.add_subplot(111)
        ax.set_facecolor(self._clr["BG2"])
        
        # Plot LUT curves
        temps = sorted(TEMP_LUT.keys())
        durations = [TEMP_LUT[t] for t in temps]
        
        # Generate smooth curve
        if len(temps) >= 2:
            import numpy as np
            temp_range = np.linspace(min(temps), max(temps), 100)
            duration_curve_base = []
            
            for t in temp_range:
                for i in range(len(temps) - 1):
                    if temps[i] <= t <= temps[i + 1]:
                        t0, t1 = temps[i], temps[i + 1]
                        d0, d1 = durations[i], durations[i + 1]
                        d = d0 + (t - t0) / (t1 - t0) * (d1 - d0)
                        duration_curve_base.append(d)
                        break
                else:
                    if t <= temps[0]:
                        duration_curve_base.append(durations[0])
                    else:
                        duration_curve_base.append(durations[-1])
            
            # Plot BASE curve (without cloud penalty)
            ax.plot(temp_range, duration_curve_base, color="#00cc88", linewidth=3, 
                   label="WITHOUT Cloud Penalty", zorder=3)
            
            # Plot WITH PENALTY curve
            temp_range_shifted = temp_range - (example_cloud / 100) * penalty
            duration_curve_penalty = []
            
            for t_shifted in temp_range_shifted:
                for i in range(len(temps) - 1):
                    if temps[i] <= t_shifted <= temps[i + 1]:
                        t0, t1 = temps[i], temps[i + 1]
                        d0, d1 = durations[i], durations[i + 1]
                        d = d0 + (t_shifted - t0) / (t1 - t0) * (d1 - d0)
                        duration_curve_penalty.append(d)
                        break
                else:
                    if t_shifted <= temps[0]:
                        duration_curve_penalty.append(durations[0])
                    else:
                        duration_curve_penalty.append(durations[-1])
            
            ax.plot(temp_range, duration_curve_penalty, color="#ff6b6b", linewidth=3,
                   linestyle="--", label=f"WITH Cloud Penalty ({example_cloud:.0f}% clouds)", zorder=3)
            
            # Mark the example point
            ax.scatter([example_temp], [base_duration], color="#00cc88", s=200, 
                      zorder=5, edgecolors="#ffffff", linewidths=2.5, marker='o')
            ax.scatter([example_temp], [duration_with_penalty], color="#ff6b6b", s=200,
                      zorder=5, edgecolors="#ffffff", linewidths=2.5, marker='s')
            
            # Arrow showing difference
            if abs(diff) > 2:
                ax.annotate('', xy=(example_temp, duration_with_penalty), 
                           xytext=(example_temp, base_duration),
                           arrowprops=dict(arrowstyle='<->', color='#ffaa00', lw=2.5))
                
                mid_y = (base_duration + duration_with_penalty) / 2
                ax.text(example_temp + 0.5, mid_y, f'+{diff}min', 
                       color='#ffaa00', fontweight='bold', fontsize=10,
                       bbox=dict(boxstyle='round,pad=0.4', facecolor=self._clr["BG2"], 
                                edgecolor='#ffaa00', linewidth=2))
        
        # Plot LUT points
        ax.scatter(temps, durations, color="#007acc", s=70, zorder=4, 
                  edgecolors="#ffffff", linewidths=1, alpha=0.7)
        
        # Styling
        ax.set_xlabel("Temperature (°C)", color=self._clr["FG"], fontweight='bold', fontsize=11)
        ax.set_ylabel("Duration (minutes)", color=self._clr["FG"], fontweight='bold', fontsize=11)
        ax.set_title("Cloud Penalty Impact", color=self._clr["FG"], 
                    fontweight="bold", fontsize=12)
        ax.tick_params(colors=self._clr["FG_DIM"], labelsize=9)
        ax.grid(True, linestyle=":", alpha=0.3, color=self._clr["SEP"])
        ax.legend(facecolor=self._clr["BG2"], edgecolor=self._clr["SEP"], 
                 labelcolor=self._clr["FG"], fontsize=9, loc='best')
        
        for spine in ax.spines.values():
            spine.set_edgecolor(self._clr["SEP"])
        
        # Embed in tkinter
        canvas = FigureCanvasTkAgg(fig, master=self.graph_container)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True)
        self.lut_graph_canvas = canvas
    
    def _save_settings(self):
        """Save all settings to runtime_settings.json."""
        global FIRST_RUN_TARGET_TIME, CLOUD_PENALTY_FACTOR, TEMP_LUT

        try:
            # Validate and save target time
            hour = self.target_hour_var.get().strip()
            minute = self.target_min_var.get().strip()
            time_str = f"{hour.zfill(2)}:{minute.zfill(2)}"
            
            if not runtime_settings.set_target_time(time_str):
                messagebox.showerror("Invalid Time", "Time must be in HH:MM format (00:00 to 23:59)")
                return
            
            # Validate and save cloud penalty
            try:
                penalty = float(self.cloud_penalty_var.get())
                if not runtime_settings.set_cloud_penalty(penalty):
                    messagebox.showerror("Invalid Penalty", "Cloud penalty must be a positive number")
                    return
            except ValueError:
                messagebox.showerror("Invalid Penalty", "Cloud penalty must be a number")
                return
            
            # Validate and save LUT
            if len(TEMP_LUT) < 2:
                messagebox.showerror("Invalid LUT", "LUT must have at least 2 temperature points")
                return
            
            if not runtime_settings.set_temp_lut(TEMP_LUT):
                messagebox.showerror("Invalid LUT", "LUT validation failed")
                return
            
            # Update globals
            FIRST_RUN_TARGET_TIME = time_str
            CLOUD_PENALTY_FACTOR = penalty
            TEMP_LUT = runtime_settings.get_temp_lut()
            
            # Reinitialize scheduler with new target time
            target_parts = time_str.split(":")
            self.scheduler.target_hour = int(target_parts[0])
            self.scheduler.target_minute = int(target_parts[1])
            
            check_time = datetime.now().replace(
                hour=self.scheduler.target_hour, 
                minute=self.scheduler.target_minute, 
                second=0, microsecond=0
            ) - timedelta(hours=self.scheduler.HOURS_BEFORE_TARGET)
            self.scheduler.check_hour = check_time.hour
            self.scheduler.check_minute = check_time.minute
            
            log.info(f"Settings updated: target={time_str}, cloud_penalty={penalty}, "
                    f"check_time={self.scheduler.check_hour:02d}:{self.scheduler.check_minute:02d}")
            
            messagebox.showinfo(
                "Settings Saved",
                f"Settings saved successfully!\n\n"
                f"Target ready time: {time_str}\n"
                f"First check at: {self.scheduler.check_hour:02d}:{self.scheduler.check_minute:02d}\n"
                f"Cloud penalty: {penalty}\n"
                f"LUT points: {len(TEMP_LUT)}\n\n"
                f"Changes will apply to next calculation."
            )
            
        except Exception as e:
            log.error(f"Failed to save settings: {e}")
            messagebox.showerror("Save Failed", f"Error saving settings: {e}")
    
    # ------------------------------------------------------
    # STATUS BAR
    # ------------------------------------------------------
    def create_status_bar(self):
        c = self._clr
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
    
    def retry_ha(self):
        """Retry HA in background thread."""
        def _run():
            ok, status = self.dm.retry_ha()
            self.root.after(0, lambda: self._retry_done(ok, status))
        
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
    
    def on_scheduler_state(self, state):
        """Called from Scheduler thread."""
        state_icons = {
            "WAIT_FIRST_CHECK": "💤  WAIT CHECK",
            "FETCH_SUN":        "🌅  FETCH SUN",
            "CALCULATING":      "🧮  CALCULATING",
            "WAIT_TRIGGER":     "⏳  WAIT TRIGGER",
            "EXECUTE":          "⚡  EXECUTING",
            "WAIT_NEXT_DAY":    "😴  DONE TODAY",
        }
        self.root.after(0, lambda: self._state_var.set(state_icons.get(state, state)))
        
        # Auto-refresh after execution
        if state == "WAIT_NEXT_DAY":
            self.root.after(500, self.load_data)
    
    def _tick(self):
        """Update clock and uptime."""
        now = datetime.now()
        elapsed = now - self.startup_time
        h, rem = divmod(int(elapsed.total_seconds()), 3600)
        m, s = divmod(rem, 60)
        self.lbl_clock.config(text=f"🕐  {now.strftime('%H:%M:%S')}")
        self.lbl_uptime.config(text=f"Uptime: {h:02d}:{m:02d}:{s:02d}")
        self.root.after(1000, self._tick)


# ==========================================================
# ENTRY POINT
# ==========================================================
if __name__ == "__main__":
    dm = DataManager()
    root = tk.Tk()
    scheduler = Scheduler(dm)
    app = BoilerApp(root, dm, scheduler)
    
    scheduler.on_state_change = app.on_scheduler_state
    scheduler.start()
    
    root.mainloop()
    scheduler.stop()
