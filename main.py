import os
import csv
import json
import time
import logging
import requests
import configparser
from datetime import datetime, timedelta, date
import tkinter as tk
from tkinter import ttk, messagebox
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg


# ==========================================================
# LOGGING  (file + console, timestamped)
# ==========================================================
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("boiler.log"),
        logging.StreamHandler(sys.stdout)   # stdout = normal color in PyCharm
    ]
)
log = logging.getLogger(__name__)


# ==========================================================
# CONFIG  — load + validate before anything else
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
    "parameters":  ["max_first_run"],
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
        "config.ini is missing the following required entries:\n  " +
        "\n  ".join(missing)
    )

HA_IP    = config["homeassistant"]["HA_IP"]
HA_PORT  = config["homeassistant"]["HA_PORT"]
HA_TOKEN = config["homeassistant"]["token"]

BOILER_1ST_ON_ENTITY_ID        = config["homeassistant"]["BOILER_1ST_ON_ENTITY_ID"]
BOILER_2ND_ON_ENTITY_ID        = config["homeassistant"]["BOILER_2ND_ON_ENTITY_ID"]
RUN_SCRIPT_1ST_START_ENTITY_ID = config["homeassistant"]["RUN_SCRIPT_1ST_START_BOILER_ENTITY_ID"]

LAT         = float(config["location"]["latitude"])
LON         = float(config["location"]["longitude"])
WEATHER_URL = config["weather"]["weather_url"]

# Now lives in config.ini under [parameters] max_first_run
MAX_FIRST_RUN = int(config["parameters"]["max_first_run"])

TEMP_LUT = {int(k): v for k, v in json.loads(
    config["temperature_duration_lut"]["temp_lut"]
).items()}

HA_URL  = f"http://{HA_IP}:{HA_PORT}"
HEADERS = {
    "Authorization": f"Bearer {HA_TOKEN}",
    "Content-Type":  "application/json"
}

DATA_FOLDER = "stored_data"
os.makedirs(DATA_FOLDER, exist_ok=True)


# ==========================================================
# RETRY HELPER
# ==========================================================
def with_retries(fn, retries=3, delay=5, label="operation"):
    """
    Call fn() up to `retries` times, waiting `delay` seconds between
    attempts. Raises the last exception if all attempts fail.
    """
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except Exception as e:
            if attempt == retries:
                log.error(f"{label} failed after {retries} attempts: {e}")
                raise
            log.warning(
                f"{label} attempt {attempt}/{retries} failed: {e}. "
                f"Retrying in {delay}s..."
            )
            time.sleep(delay)


# ==========================================================
# HOME ASSISTANT
# ==========================================================
def check_ha_reachable():
    """Quick health-check before doing anything else."""
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
    """
    Full HA execution sequence with per-call retries:
      1. Set slider 1  → first_run
      2. Set slider 2  → second_run  (0 when duration ≤ MAX_FIRST_RUN)
      3. Fire 1st script → HA starts boiler immediately.
         HA handles the 2nd script via slider 2 internally.
    """
    log.info("Sending values to Home Assistant...")

    try:
        with_retries(lambda: check_ha_reachable(),
                     label="HA health-check")

        with_retries(lambda: ha_set_slider(BOILER_1ST_ON_ENTITY_ID, first_run),
                     label=f"set slider1 ({BOILER_1ST_ON_ENTITY_ID})")

        with_retries(lambda: ha_set_slider(BOILER_2ND_ON_ENTITY_ID, second_run),
                     label=f"set slider2 ({BOILER_2ND_ON_ENTITY_ID})")

        with_retries(lambda: ha_run_script(RUN_SCRIPT_1ST_START_ENTITY_ID),
                     label=f"run script ({RUN_SCRIPT_1ST_START_ENTITY_ID})")

        log.info("HA execution complete.")
        return True

    except requests.exceptions.Timeout:
        log.error("HA request timed out. Is HA reachable?")
    except requests.exceptions.ConnectionError:
        log.error(f"Could not connect to HA at {HA_URL}. Check HA_IP / HA_PORT.")
    except requests.exceptions.HTTPError as e:
        log.error(f"HA returned HTTP {e.response.status_code}: {e.response.text}")
    except requests.exceptions.RequestException as e:
        log.error(f"Unexpected HA request error: {e}")

    return False


# ==========================================================
# WEATHER + CALC
# ==========================================================
def get_weather():

    def _fetch():
        r = requests.get(
            WEATHER_URL,
            params={
                "latitude":  LAT,
                "longitude": LON,
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
        raise RuntimeError(f"Weather API failed after all retries: {e}") from e

    today = date.today()

    try:
        sunrise = datetime.fromisoformat(data["daily"]["sunrise"][0])
        sunset  = datetime.fromisoformat(data["daily"]["sunset"][0])
    except (KeyError, IndexError, ValueError) as e:
        raise RuntimeError(f"Unexpected weather API response format: {e}") from e

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

        if ts.date() == today:
            weather.append({"time": ts, "temp": temp, "clouds": clouds})

    return sunrise, sunset, weather


def weighted_average(weather, sunrise, sunset):

    total_weight = 0
    temp_sum     = 0
    cloud_sum    = 0

    for w in weather:
        t = w["time"]

        if sunrise <= t < sunrise + timedelta(hours=3):
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
        raise ValueError(
            "No weather data fell within the weighted sunrise/sunset "
            "window for today. Cannot compute averages."
        )

    return temp_sum / total_weight, cloud_sum / total_weight


def calculate_duration(temp):

    temps = sorted(TEMP_LUT.keys())

    if temp <= temps[0]:
        return TEMP_LUT[temps[0]]
    if temp >= temps[-1]:
        return TEMP_LUT[temps[-1]]

    for i in range(len(temps) - 1):
        t0, t1 = temps[i], temps[i + 1]
        if t0 <= temp <= t1:
            v0, v1 = TEMP_LUT[t0], TEMP_LUT[t1]
            ratio  = (temp - t0) / (t1 - t0)
            return int(round(v0 + ratio * (v1 - v0)))

    raise ValueError(
        f"Temperature {temp}°C could not be matched in TEMP_LUT. "
        "Check for gaps in the lookup table."
    )


def calculate_sliders(duration):
    """
    Examples:
      duration=90  → first_run=90,  second_run=0
      duration=180 → first_run=120, second_run=60
    """
    if duration <= MAX_FIRST_RUN:
        return duration, 0
    return MAX_FIRST_RUN, duration - MAX_FIRST_RUN


# ==========================================================
# MAIN DAILY LOGIC
# ==========================================================
def save_daily_summary():
    """
    Fetch weather → calculate → send to HA → save CSV.
    No-op if today's file already exists.
    Returns a dict of today's values (for GUI retry use), or None if skipped.
    """
    filename = os.path.join(DATA_FOLDER, f"{date.today()}.csv")

    if os.path.exists(filename):
        log.info(f"Today's summary already saved ({filename}). Skipping.")
        return None

    sunrise, sunset, weather = get_weather()
    avg_temp, avg_cloud      = weighted_average(weather, sunrise, sunset)
    effective_temp           = round(avg_temp - (avg_cloud / 100) * 3, 2)
    duration                 = calculate_duration(effective_temp)
    first_run, second_run    = calculate_sliders(duration)

    log.info(
        f"Calculated → effective_temp={effective_temp}°C | "
        f"duration={duration} min | "
        f"slider1={first_run} min | slider2={second_run} min"
    )

    ha_ok        = send_to_ha(first_run, second_run)
    execute_time = datetime.now().strftime("%H:%M:%S")
    ha_status    = "OK" if ha_ok else "FAILED"

    row_data = {
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

    with open(filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(row_data.keys()))
        writer.writeheader()
        writer.writerow(row_data)

    log.info(f"[{execute_time}] CSV saved → HA status: {ha_status}")
    invalidate_records_cache()
    return row_data


def retry_ha_for_today():
    """
    Re-send slider values to HA using today's saved CSV,
    without re-fetching weather. Updates ha_status in the file.
    Used by the GUI 'Retry HA' button.
    """
    filename = os.path.join(DATA_FOLDER, f"{date.today()}.csv")

    if not os.path.exists(filename):
        log.warning("No CSV found for today — cannot retry HA.")
        return False, "No data for today"

    with open(filename, newline="") as f:
        reader = csv.DictReader(f)
        rows   = list(reader)

    if not rows:
        return False, "Today's CSV is empty"

    row       = rows[0]
    first_run = int(row["first_run"])
    second_run= int(row["second_run"])

    ha_ok     = send_to_ha(first_run, second_run)
    ha_status = "OK" if ha_ok else "FAILED"

    # Update ha_status in the saved file
    row["ha_status"]    = ha_status
    row["execute_time"] = datetime.now().strftime("%H:%M:%S")

    with open(filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(row.keys()))
        writer.writeheader()
        writer.writerow(row)

    log.info(f"Retry HA complete → {ha_status}")
    invalidate_records_cache()
    return ha_ok, ha_status


# ==========================================================
# GUI
# ==========================================================
REQUIRED_FIELDS = {
    "date", "sunrise", "sunset",
    "avg_temp", "avg_cloud",
    "duration", "first_run", "second_run"
}

# Old column names → current names.  Any file whose columns can be
# fully remapped via this dict is migrated automatically instead of skipped.
LEGACY_COLUMN_MAP = {
    "on_time":   "duration",
    "script_ok": "ha_status",
}


def _migrate_row(row):
    """Remap legacy column names and back-fill any missing optional columns."""
    new_row = {LEGACY_COLUMN_MAP.get(k, k): v for k, v in row.items()}
    new_row.setdefault("effective_temp", "—")
    new_row.setdefault("execute_time",   "—")
    new_row.setdefault("ha_status",      "—")
    new_row.setdefault("first_run",      "—")
    new_row.setdefault("second_run",     "—")
    return new_row


# Cache — folder is scanned only once per session.
# Call invalidate_records_cache() after writing new data.
_records_cache = None

ARCHIVE_FOLDER = os.path.join(DATA_FOLDER, "archive")


def invalidate_records_cache():
    global _records_cache
    _records_cache = None


def _archive_file(filepath, file):
    """Move an unrecognised file to stored_data/archive/ silently."""
    import shutil
    os.makedirs(ARCHIVE_FOLDER, exist_ok=True)
    dest = os.path.join(ARCHIVE_FOLDER, file)
    try:
        shutil.move(filepath, dest)
        log.info(f"Archived unrecognised file: {file} → archive/")
    except Exception as e:
        log.warning(f"Could not archive {file}: {e}")


def read_all_records():
    global _records_cache
    if _records_cache is not None:
        return _records_cache

    all_rows        = []
    archived_count  = 0
    warned_formats  = set()   # suppress duplicate warnings for same column signature

    for file in sorted(os.listdir(DATA_FOLDER)):
        if not file.endswith(".csv"):
            continue

        filepath = os.path.join(DATA_FOLDER, file)
        try:
            with open(filepath, newline="") as f:
                reader     = csv.DictReader(f)
                fieldnames = set(reader.fieldnames or [])

                remapped = {LEGACY_COLUMN_MAP.get(fn, fn) for fn in fieldnames}

                if not REQUIRED_FIELDS.issubset(remapped):
                    fmt_key = frozenset(fieldnames)
                    if fmt_key not in warned_formats:
                        log.warning(
                            f"Found files with unrecognised columns "
                            f"{sorted(fieldnames)} (e.g. {file}). "
                            f"These look like raw hourly weather data and cannot "
                            f"be displayed as daily summaries. "
                            f"Moving them to {ARCHIVE_FOLDER}/ automatically."
                        )
                        warned_formats.add(fmt_key)
                    # close file before moving
                    f.close()
                    _archive_file(filepath, file)
                    archived_count += 1
                    continue

                if any(fn in LEGACY_COLUMN_MAP for fn in fieldnames):
                    log.info(f"Migrating legacy format: {file}")

                for row in reader:
                    all_rows.append(_migrate_row(row))

        except Exception as e:
            log.warning(f"Skipping {file}: {e}")

    if archived_count:
        log.info(f"Archived {archived_count} unrecognised file(s) to {ARCHIVE_FOLDER}/")

    try:
        all_rows.sort(key=lambda x: datetime.strptime(x["date"], "%Y-%m-%d"))
    except ValueError as e:
        log.warning(f"Could not sort rows by date: {e}")

    _records_cache = all_rows
    return _records_cache


class BoilerApp:

    def __init__(self, root):

        self.root         = root
        self.startup_time = datetime.now()
        self.root.title("Boiler Control Dashboard")
        self.root.geometry("1500x680")

        # ── Dark theme palette ──────────────────────────────
        BG       = "#1e1e1e"   # main background
        BG2      = "#2b2b2b"   # slightly lighter panels / status bar
        BG3      = "#3c3f41"   # button / header background
        FG       = "#e0e0e0"   # primary text
        FG_DIM   = "#aaaaaa"   # secondary text
        ACCENT   = "#4e9de0"   # highlight (headings, selection)
        SEP      = "#555555"   # separator

        self.root.configure(bg=BG)

        style = ttk.Style()
        style.theme_use("clam")

        # Notebook / tabs
        style.configure("TNotebook",       background=BG,  borderwidth=0)
        style.configure("TNotebook.Tab",   background=BG3, foreground=FG,
                        padding=[12, 4],   font=("Segoe UI", 10))
        style.map("TNotebook.Tab",
                  background=[("selected", BG2)],
                  foreground=[("selected", ACCENT)])

        # Frames
        style.configure("TFrame", background=BG)

        # Treeview (table)
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

        # Scrollbar
        style.configure("Vertical.TScrollbar",
                        background=BG3, troughcolor=BG2,
                        borderwidth=0, arrowcolor=FG_DIM)

        # Buttons
        style.configure("TButton",
                        background=BG3, foreground=FG,
                        font=("Segoe UI", 10), relief="flat",
                        borderwidth=0, padding=[10, 4])
        style.map("TButton",
                  background=[("active", "#4e5254")],
                  foreground=[("active", "#ffffff")])

        # Store for use in other methods
        self._clr = dict(BG=BG, BG2=BG2, BG3=BG3, FG=FG,
                         FG_DIM=FG_DIM, ACCENT=ACCENT, SEP=SEP)

        notebook = ttk.Notebook(root)
        notebook.pack(fill="both", expand=True)

        self.tab_table = ttk.Frame(notebook)
        self.tab_graph = ttk.Frame(notebook)

        notebook.add(self.tab_table, text="Daily Log")
        notebook.add(self.tab_graph, text="Graph")

        self.create_table()
        self.load_data()
        self.create_graph()
        self.create_status_bar()
        self._tick()             # start clock loop

    # ------------------------------------------------------
    def create_table(self):

        columns = (
            "#", "date", "sunrise", "sunset",
            "avg_temp", "avg_cloud", "effective_temp",
            "duration", "first_run", "second_run",
            "execute_time", "ha_status"
        )

        self.tree = ttk.Treeview(
            self.tab_table, columns=columns, show="headings"
        )

        col_widths = {
            "#": 35, "date": 105, "sunrise": 70, "sunset": 70,
            "avg_temp": 95, "avg_cloud": 95, "effective_temp": 115,
            "duration": 90, "first_run": 125, "second_run": 135,
            "execute_time": 105, "ha_status": 90
        }

        # Human-readable heading labels — column IDs stay unchanged for data binding
        col_labels = {
            "#":             "#",
            "date":          "Date",
            "sunrise":       "Sunrise",
            "sunset":        "Sunset",
            "avg_temp":      "Avg Temp (°C)",
            "avg_cloud":     "Avg Cloud (%)",
            "effective_temp":"Eff. Temp (°C)",
            "duration":      "Total (min)",
            "first_run":     "1st Timeout (min)",
            "second_run":    "2nd Timeout (min)",
            "execute_time":  "Executed At",
            "ha_status":     "HA Status",
        }

        for col in columns:
            self.tree.heading(col, text=col_labels.get(col, col))
            self.tree.column(col, width=col_widths.get(col, 90), anchor="center")

        scrollbar = ttk.Scrollbar(
            self.tab_table, orient="vertical", command=self.tree.yview
        )
        self.tree.configure(yscrollcommand=scrollbar.set)
        self.tree.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        # Row coloring — dark palette
        # OK rows:  alternating dark teal shades  (easy on the eye)
        # FAILED:   deep red tint
        # Unknown:  neutral grey
        self.tree.tag_configure("ha_ok_odd",
                                background="#1e3a2f", foreground="#c8f0d8")   # dark green
        self.tree.tag_configure("ha_ok_even",
                                background="#162e24", foreground="#b0e8c8")   # slightly darker green
        self.tree.tag_configure("ha_failed",
                                background="#3a1e1e", foreground="#f5a0a0")   # dark red
        self.tree.tag_configure("ha_unknown",
                                background="#2a2a2e", foreground="#aaaaaa")   # neutral grey

    # ------------------------------------------------------
    def create_status_bar(self):
        """
        Dark bottom bar visible at all times (outside the notebook).
        Contains: clock | uptime | spacer | Refresh button | Retry HA button
        """
        c   = self._clr
        bar = tk.Frame(self.root, bg=c["BG2"], height=36)
        bar.pack(side="bottom", fill="x")
        bar.pack_propagate(False)

        # separator line at the top of the bar
        tk.Frame(bar, bg=c["SEP"], height=1).place(relx=0, rely=0, relwidth=1)

        # -- clock (live) --
        self.lbl_clock = tk.Label(
            bar, text="", bg=c["BG2"], fg=c["FG"],
            font=("Segoe UI", 10, "bold"), padx=12
        )
        self.lbl_clock.pack(side="left", pady=4)

        tk.Label(bar, text="│", bg=c["BG2"], fg=c["SEP"],
                 font=("Segoe UI", 11)).pack(side="left")

        # -- uptime (live) --
        self.lbl_uptime = tk.Label(
            bar, text="", bg=c["BG2"], fg=c["FG_DIM"],
            font=("Segoe UI", 10), padx=12
        )
        self.lbl_uptime.pack(side="left", pady=4)

        # -- buttons on the right --
        btn_style = {
            "bg": c["BG3"], "fg": c["FG"],
            "activebackground": "#4e5254", "activeforeground": "#ffffff",
            "relief": "flat", "font": ("Segoe UI", 10),
            "padx": 14, "pady": 5, "cursor": "hand2",
            "bd": 0
        }

        tk.Button(
            bar, text="⚡  Retry HA (today)",
            command=self.retry_ha, **btn_style
        ).pack(side="right", padx=(4, 12), pady=4)

        tk.Button(
            bar, text="⟳  Refresh",
            command=self.load_data, **btn_style
        ).pack(side="right", padx=4, pady=4)

    # ------------------------------------------------------
    def _tick(self):
        """Update clock and uptime labels every second."""
        now     = datetime.now()
        elapsed = now - self.startup_time

        hours, rem   = divmod(int(elapsed.total_seconds()), 3600)
        minutes, secs = divmod(rem, 60)

        self.lbl_clock.config(text=f"🕐  {now.strftime('%H:%M:%S')}")
        self.lbl_uptime.config(text=f"Uptime: {hours:02d}:{minutes:02d}:{secs:02d}")

        self.root.after(1000, self._tick)

    # ------------------------------------------------------
    def load_data(self):

        self.tree.delete(*self.tree.get_children())

        for i, row in enumerate(read_all_records(), 1):

            status = row["ha_status"]
            if status == "FAILED":
                tag = "ha_failed"
            elif status == "—":
                tag = "ha_unknown"
            elif i % 2 == 0:
                tag = "ha_ok_even"
            else:
                tag = "ha_ok_odd"

            self.tree.insert(
                "", "end",
                values=(
                    i,
                    row["date"],
                    row["sunrise"],
                    row["sunset"],
                    row["avg_temp"],
                    row["avg_cloud"],
                    row["effective_temp"],
                    row["duration"],
                    row["first_run"],
                    row["second_run"],
                    row["execute_time"],
                    row["ha_status"]
                ),
                tags=(tag,)
            )

    # ------------------------------------------------------
    def retry_ha(self):
        """Retry HA send for today without re-fetching weather."""
        ok, status = retry_ha_for_today()

        if ok:
            messagebox.showinfo("Retry HA", f"HA updated successfully.\nStatus: {status}")
        else:
            messagebox.showerror("Retry HA", f"HA call failed.\nStatus: {status}\nCheck boiler.log for details.")

        # Refresh table so ha_status column reflects the new result
        self.load_data()

    # ------------------------------------------------------
    def create_graph(self):

        records = read_all_records()
        if not records:
            return

        # Use proper datetime objects so matplotlib spaces dates correctly
        dates       = [datetime.strptime(r["date"], "%Y-%m-%d") for r in records]
        durations   = [int(r["duration"])    for r in records]
        temps       = [float(r["avg_temp"])  for r in records]
        first_runs  = [int(r["first_run"])   for r in records]
        second_runs = [int(r["second_run"])  for r in records]

        # effective_temp may be "—" in older files — skip gracefully
        eff_dates = []
        eff_temps = []
        for r, d in zip(records, dates):
            if r["effective_temp"] != "—":
                eff_dates.append(d)
                eff_temps.append(float(r["effective_temp"]))

        fig = plt.Figure(figsize=(10, 5), facecolor="#1e1e1e")
        ax  = fig.add_subplot(111)
        ax.set_facecolor("#2b2b2b")
        ax.tick_params(colors="#aaaaaa")
        ax.xaxis.label.set_color("#aaaaaa")
        ax.yaxis.label.set_color("#aaaaaa")
        ax.title.set_color("#e0e0e0")
        for spine in ax.spines.values():
            spine.set_edgecolor("#555555")

        ax.plot(dates, durations,   label="Total Duration (min)", linewidth=2)
        ax.plot(dates, temps,       label="Avg Temp (°C)",        linewidth=2)
        ax.plot(dates, first_runs,  label=f"Slider 1 – first_run (min)",  linestyle="--")
        ax.plot(dates, second_runs, label="Slider 2 – second_run (min)", linestyle=":")

        if eff_temps:
            ax.plot(eff_dates, eff_temps, label="Effective Temp (°C)",
                    linestyle="-.", linewidth=1.5, color="purple")

        ax.axhline(MAX_FIRST_RUN, color="gray", linewidth=0.8,
                   linestyle="-.", label=f"{MAX_FIRST_RUN} min cap")

        # Proper date axis — no overlapping labels
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        fig.autofmt_xdate(rotation=45)

        ax.set_title("Duration, Temperature & Slider Values")
        ax.set_xlabel("Date")
        ax.set_ylabel("Value")
        ax.legend()
        ax.grid(True, linestyle=":", alpha=0.5)

        canvas = FigureCanvasTkAgg(fig, master=self.tab_graph)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True)


# ==========================================================
# ENTRY
# ==========================================================
if __name__ == "__main__":

    try:
        save_daily_summary()
    except Exception as e:
        log.error(f"Could not complete today's summary: {e}")

    root = tk.Tk()
    app  = BoilerApp(root)
    root.mainloop()