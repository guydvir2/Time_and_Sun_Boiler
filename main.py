import csv
import json
import requests
import configparser
from time import sleep
from statistics import mean
from datetime import datetime, timedelta, time, date
import tkinter as tk
from tkinter import ttk, messagebox

from daily_logger import init_logger, log_event, save_summary, get_summary_html_path

# ================= CONFIG =================
config = configparser.ConfigParser()
config.read('config.ini')

# Home Assistant
TOKEN = config['homeassistant']['token']
HA_IP = config['homeassistant']['HA_IP']
HA_PORT = config['homeassistant']['HA_PORT']
BOILER_ENTITY_ID = config["homeassistant"]["BOILER_ENTITY_ID"]
BOILER_1ST_ON_ENTITY_ID = config["homeassistant"]["BOILER_1ST_ON_ENTITY_ID"]
BOILER_2ND_ON_ENTITY_ID = config["homeassistant"]["BOILER_2ND_ON_ENTITY_ID"]
RUN_SCRIPT_1ST_START_BOILER_ENTITY_ID = config["homeassistant"]["RUN_SCRIPT_1ST_START_BOILER_ENTITY_ID"]

# Location & Weather
latitude = float(config['location']['latitude'])
longitude = float(config['location']['longitude'])
weather_url = config['weather']['weather_url']

# Parameters
SAVE_DATA = config['parameters']['SAVE_DATA'].lower() == "true"
SLEEP_MINUTES = float(config["parameters"]["SLEEP_MINUTES"])
READ_DATA_FROM_FILE = config["parameters"]["READ_DATA_FROM_FILE"].lower() == "false"
debug_mode_ignore_update_time = config["parameters"]["debug_mode_ignore_update_time"].lower() == "true"
DAYS_BACK_WEATHER_DATA = int(config["parameters"]["DAYS_BACK_WEATHER_DATA"])

MAX_DURATION_MINUTES_BOILER = 120
APP_VERSION = "2.0.0"

# HA URLs
HA_URL = f"http://{HA_IP}:{HA_PORT}"
HA_URL_SERVICES = f"{HA_URL}/api/services"
HA_URL_STATES = f"{HA_URL}/api/states"
headers = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

update_for_today = False
weather_data_today = []

# ================= WEATHER FUNCTIONS =================
def get_weather(location, start_dt, end_dt, days_back=0):
    lat, lon = location
    target_date = date.today() - timedelta(days=days_back)
    now = datetime.now()

    try:
        r = requests.get(
            weather_url,
            params={
                "latitude": lat,
                "longitude": lon,
                "hourly": "temperature_2m,cloudcover",
                "past_days": days_back,
                "timezone": "auto"
            }, timeout=10
        )
        r.raise_for_status()
        weather = []

        hourly = r.json()["hourly"]
        for t, temp, clouds in zip(hourly["time"], hourly["temperature_2m"], hourly["cloudcover"]):
            ts = datetime.fromisoformat(t)
            if ts.date() != target_date:
                continue
            if not (start_dt <= ts <= end_dt):
                continue
            if days_back == 0 and ts > now:
                continue
            weather.append({"timestamp": ts, "temperature": temp, "cloud_cover": clouds})
        return weather, len(weather) > 0
    except Exception as e:
        print(f"❌ Error fetching weather: {e}")
        return [], False

def get_sun_times(location, days_back=0, default_sunrise=6, default_sunset=17):
    lat, lon = location
    target_date = date.today() - timedelta(days=days_back)
    try:
        r = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": lat,
                "longitude": lon,
                "daily": "sunrise,sunset",
                "timezone": "auto",
                "past_days": days_back
            }, timeout=10
        )
        r.raise_for_status()
        daily = r.json()["daily"]
        for dt, sunrise, sunset in zip(daily["time"], daily["sunrise"], daily["sunset"]):
            if dt == str(target_date):
                sunrise_dt = datetime.fromisoformat(sunrise)
                sunset_dt = datetime.fromisoformat(sunset)
                return {"sunrise_dt": sunrise_dt, "sunset_dt": sunset_dt}
    except Exception as e:
        print(f"❌ Error fetching sun times: {e}")
    # defaults
    now = datetime.now()
    sunrise_dt = now.replace(hour=default_sunrise, minute=0, second=0, microsecond=0)
    sunset_dt  = now.replace(hour=default_sunset, minute=0, second=0, microsecond=0)
    return {"sunrise_dt": sunrise_dt, "sunset_dt": sunset_dt}

def save_weather_to_csv(weather, filename):
    if not weather:
        return
    try:
        with open(filename, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["timestamp", "temperature", "cloud_cover"])
            for entry in weather:
                writer.writerow([entry["timestamp"].isoformat(), entry["temperature"], entry["cloud_cover"]])
    except Exception as e:
        print(f"❌ Error saving weather data: {e}")

def load_weather_from_csv(filename):
    weather = []
    try:
        with open(filename, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                weather.append({"timestamp": datetime.fromisoformat(row["timestamp"]),
                                "temperature": float(row["temperature"]),
                                "cloud_cover": float(row["cloud_cover"])})
        return weather, True
    except Exception as e:
        print(f"❌ Error loading weather data: {e}")
        return [], False

def generate_filename_csv(date_back=0):
    return f"stored_data/{date.today() - timedelta(days=date_back)}.csv"

# ================= HOME ASSISTANT FUNCTIONS =================
def ha_get_entity_state(entity_id):
    url = HA_URL_STATES + f"/{entity_id}"
    r = requests.get(url, headers=headers, timeout=10)
    r.raise_for_status()
    return r.json()["state"]

def ha_set_slide_value(entity_id, value=5):
    value = max(0, min(value, MAX_DURATION_MINUTES_BOILER))
    entity_json = {"entity_id": entity_id, "value": value}
    try:
        r = requests.post(HA_URL_SERVICES + "/input_number/set_value", headers=headers, json=entity_json, timeout=10)
        return r.ok
    except Exception as e:
        print(f"❌ Error updating slider: {e}")
        return False

def ha_run_script(script_id):
    script_entity = script_id if script_id.startswith("script.") else f"script.{script_id}"
    try:
        r = requests.post(f"{HA_URL_SERVICES}/script/turn_on",
                          headers=headers, json={"entity_id": script_entity}, timeout=10)
        return r.ok
    except Exception as e:
        print(f"❌ Error running script {script_entity}: {e}")
        return False

def ha_update_duration_entities(dur):
    remaining = max(dur - MAX_DURATION_MINUTES_BOILER, 0)
    first = ha_set_slide_value(BOILER_1ST_ON_ENTITY_ID, dur - remaining)
    second = ha_set_slide_value(BOILER_2ND_ON_ENTITY_ID, remaining)
    return first and second

def ha_execute_boiler_start_script(script_id):
    state_before = ha_get_entity_state(BOILER_ENTITY_ID)
    if state_before != "off":
        print("⚠️ Boiler already ON, skipping script")
        return False
    ha_run_script(script_id)
    start = datetime.now()
    while datetime.now() < start + timedelta(minutes=10):
        sleep(2)
        if ha_get_entity_state(BOILER_ENTITY_ID) == "on":
            return True
    print("❌ Failed to activate boiler")
    return False

# ================= DURATION CALCULATION =================
def get_lut():
    return {int(k): v for k, v in json.loads(config["temperature_duration_lut"]["temp_lut"]).items()}

def calc_duration(T, CC, k=0.6, MAX_ON=180):
    temp_lut = get_lut()
    temps = sorted(temp_lut.keys())
    if T <= temps[0]:
        base = temp_lut[temps[0]]
    elif T >= temps[-1]:
        base = temp_lut[temps[-1]]
    else:
        for i in range(len(temps)-1):
            t0, t1 = temps[i], temps[i+1]
            if t0 <= T <= t1:
                v0, v1 = temp_lut[t0], temp_lut[t1]
                ratio = (T - t0) / (t1 - t0)
                base = v0 + ratio*(v1-v0)
                break
    ST = max(0.0, min(1.0, (T - min(temps)) / (max(temps) - min(temps))))
    alpha = k * (CC / 100.0) * ST
    return min(int(round(base*(1+alpha))), MAX_ON)

def calc_daily_weighted_mean(weather_data, sunrise_dt, sunset_dt):
    if not weather_data:
        return None, None
    peak_start = sunrise_dt + timedelta(hours=3)
    peak_end   = sunset_dt - timedelta(hours=2)
    weighted_temp_sum = 0
    weighted_cc_sum   = 0
    total_weight      = 0
    for entry in weather_data:
        ts   = entry["timestamp"]
        temp = entry["temperature"]
        cc   = entry["cloud_cover"]
        if sunrise_dt <= ts <= sunset_dt:
            if peak_start <= ts <= peak_end:
                weight = 1.3
            elif ts < peak_start:
                weight = 0.7
            else:
                weight = 0.8
            weighted_temp_sum += temp * weight
            weighted_cc_sum   += cc * weight
            total_weight      += weight
    if total_weight == 0:
        return None, None
    return weighted_temp_sum/total_weight, weighted_cc_sum/total_weight

# ================= GUI =================
class BoilerGUI:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title(f"Boiler Control v{APP_VERSION}")
        self.status_frame = ttk.Frame(self.root, padding=10)
        self.status_frame.pack(fill="both", expand=True)
        self.dot = tk.Canvas(self.status_frame, width=20, height=20)
        self.dot.grid(row=0, column=0)
        self.label = ttk.Label(self.status_frame, text="Waiting for update...")
        self.label.grid(row=0, column=1)
        self.dot.bind("<Double-1>", self.show_weather_details)
        self.weather_details = []

    def update_status(self, msg, success=True):
        self.label.config(text=msg)
        self.dot.delete("all")
        color = "green" if success else "red"
        self.dot.create_oval(2,2,18,18, fill=color)

    def show_weather_details(self, event=None):
        if not self.weather_details:
            messagebox.showinfo("Weather Details", "No data")
            return
        s = "\n".join([f"{e['timestamp'].strftime('%H:%M')} | {e['temperature']}°C | {e['cloud_cover']}%" for e in self.weather_details])
        messagebox.showinfo("Weather Details", s)

    def run(self):
        self.root.mainloop()

gui = BoilerGUI()

# ================= MAIN LOOP =================
def main_run():
    global update_for_today, weather_data_today

    while True:
        now = datetime.now()
        sun_time = get_sun_times((latitude, longitude))
        sunset_trigger = sun_time["sunset_dt"] + timedelta(minutes=10)

        # Trigger daily update
        if (now >= sunset_trigger and not update_for_today) or debug_mode_ignore_update_time:
            gui.update_status(f"Updating... {now.strftime('%H:%M')}")
            log_event("info", "Daily update started")
            if READ_DATA_FROM_FILE:
                weather_data_today, ok = load_weather_from_csv(generate_filename_csv())
            else:
                weather_data_today, ok = get_weather((latitude, longitude),
                                                     sun_time["sunrise_dt"],
                                                     sun_time["sunset_dt"])
                if ok and SAVE_DATA:
                    save_weather_to_csv(weather_data_today, generate_filename_csv())
            gui.weather_details = weather_data_today
            if ok:
                daily_t, daily_cc = calc_daily_weighted_mean(weather_data_today,
                                                             sun_time["sunrise_dt"],
                                                             sun_time["sunset_dt"])
                if daily_t is not None:
                    duration = calc_duration(daily_t, daily_cc)
                    if ha_update_duration_entities(duration):
                        if ha_execute_boiler_start_script(RUN_SCRIPT_1ST_START_BOILER_ENTITY_ID):
                            gui.update_status(f"Boiler ON: {duration} min")
                            log_event("info", "Boiler started")
                            save_summary(status="success")
                            update_for_today = True
                        else:
                            gui.update_status("Script failed", success=False)
                            save_summary(status="failed", error="Script fail")
                    else:
                        gui.update_status("HA update failed", success=False)
                        save_summary(status="failed", error="HA update fail")
                else:
                    gui.update_status("No data to calc duration", success=False)
                    save_summary(status="failed", error="No data")
            else:
                gui.update_status("Weather fetch failed", success=False)
                save_summary(status="failed", error="Weather fetch fail")

        # Reset flag at 7 AM
        elif time(7,0) <= now.time() < time(14,0) and update_for_today:
            update_for_today = False
            gui.update_status("Reset flag 7AM")
            log_event("info", "update_for_today flag reset")

        sleep(int(SLEEP_MINUTES*60))

# ================= ENTRY POINT =================
if __name__ == "__main__":
    init_logger()
    gui.update_status("Waiting for next trigger...")
    from threading import Thread
    Thread(target=main_run, daemon=True).start()
    gui.run()


























# import csv
# import json
# import requests
# import configparser
# from time import sleep
# from statistics import mean
# from datetime import datetime, timedelta, time, date
# from daily_logger import init_logger, log_event, save_summary, get_summary_html_path
#
# config = configparser.ConfigParser()
# config.read('config.ini')
#
# # Read base values from config
# TOKEN = config['homeassistant']['token']
# HA_IP = config['homeassistant']['HA_IP']
# HA_PORT = config['homeassistant']['HA_PORT']
#
# BOILER_ENTITY_ID = config["homeassistant"]["BOILER_ENTITY_ID"]
# BOILER_1ST_ON_ENTITY_ID = config["homeassistant"]["BOILER_1ST_ON_ENTITY_ID"]
# BOILER_2ND_ON_ENTITY_ID = config["homeassistant"]["BOILER_2ND_ON_ENTITY_ID"]
# RUN_SCRIPT_1ST_START_BOILER_ENTITY_ID = config["homeassistant"]["RUN_SCRIPT_1ST_START_BOILER_ENTITY_ID"]
# RUN_SCRIPT_2ND_START_BOILER_ENTITY_ID = config["homeassistant"]["RUN_SCRIPT_2ND_START_BOILER_ENTITY_ID"]
#
# latitude = float(config['location']['latitude'])
# longitude = float(config['location']['longitude'])
# weather_url = config['weather']['weather_url']
#
# SAVE_DATA = config['parameters']['SAVE_DATA'].lower() == "true"
# SLEEP_MINUTES = float(config["parameters"]["SLEEP_MINUTES"])
# READ_DATA_FROM_FILE = config["parameters"]["READ_DATA_FROM_FILE"].lower() == "true"
# debug_mode_ignore_update_time = config["parameters"]["debug_mode_ignore_update_time"].lower() == "true"
# DAYS_BACK_WEATHER_DATA = int(config["parameters"]["DAYS_BACK_WEATHER_DATA"])
#
# APP_VERSION = "1.0.3"
#
# # Build URLs in Python (where f-strings work)
# HA_URL = f"http://{HA_IP}:{HA_PORT}"
# HA_URL_SERVICES = f"{HA_URL}/api/services"
# HA_URL_STATES = f"{HA_URL}/api/states"
# HA_URL_WS = f"ws://{HA_IP}:{HA_PORT}/api/websocket"
# HA_URL_HISTORY = f"{HA_URL}/api/history/period"
# HA_URL_STAT = f"{HA_URL}/api/history/statistics"
#
# headers = {
#     "Authorization": f"Bearer {TOKEN}",
#     "Content-Type": "application/json",
# }
#
# MAX_DURATION_MINUTES_BOILER = 120
#
# update_for_today = False
#
#
# ##### Get Climatic data from Open-Meteo API and related functions #####
# def get_weather(location, start_hour, end_hour, days_back=0):
#     """Fetch weather data from Open-Meteo API."""
#     print("\n" + "=" * 60)
#     print("🌤️  FETCHING WEATHER DATA")
#     print("=" * 60)
#
#     lat, lon = location
#     target_date = date.today() - timedelta(days=days_back)
#     now = datetime.now()
#
#     print(f"📍 Location: {lat}, {lon}")
#     print(f"📅 Target date: {target_date}")
#     print(f"⏰ Hours window: {start_hour}:00–{end_hour}:00")
#
#     try:
#         r = requests.get(
#             weather_url,
#             params={
#                 "latitude": lat,
#                 "longitude": lon,
#                 "hourly": "temperature_2m,cloudcover",
#                 "past_days": days_back,
#                 "timezone": "auto"
#             }, timeout=10
#         )
#
#         r.raise_for_status()
#         weather = []
#
#         if r.status_code != 200:
#             print("❌ Failed to fetch weather data")
#             return weather, False
#         else:
#             hourly = r.json()["hourly"]
#
#             for t, temp, clouds in zip(
#                     hourly["time"],
#                     hourly["temperature_2m"],
#                     hourly["cloudcover"]
#             ):
#                 ts = datetime.fromisoformat(t)
#
#                 if ts.date() != target_date:
#                     continue
#                 if not (start_hour <= ts.hour <= end_hour):
#                     continue
#
#                 # Skip future hours when looking at today
#                 if days_back == 0 and ts > now:
#                     continue
#
#                 weather.append({
#                     "time": ts,
#                     "temp": temp,
#                     "clouds": clouds
#                 })
#
#                 print(f"   {ts.strftime('%H:%M')} | 🌡️  {temp}°C | ☁️  {clouds}%")
#
#             print(f"✅ Successfully fetched {len(weather)} weather data points")
#             return weather, len(weather) > 0
#
#     except Exception as e:
#         print(f"❌ Error fetching weather: {e}")
#         return [], False
#
#
# def get_sun_times(location, days_back=0, default_sunrise=6, default_sunset=17):
#     """
#     Get sunrise and sunset times for a location, rounded to nearest hour.
#
#     Args:
#         location: tuple of (latitude, longitude)
#         days_back: number of days back from today (0 = today, 1 = yesterday, etc.)
#         default_sunrise: default sunrise hour if retrieval fails (default: 7)
#         default_sunset: default sunset hour if retrieval fails (default: 17)
#
#     Returns:
#         dict with 'sunrise' and 'sunset' as integers (hour 0-23)
#     """
#     print("\n" + "=" * 60)
#     print("🌅 FETCHING SUNRISE/SUNSET TIMES")
#     print("=" * 60)
#
#     lat, lon = location
#     target_date = date.today() - timedelta(days=days_back)
#
#     print(f"📍 Location: {lat}, {lon}")
#     print(f"📅 Target date: {target_date}")
#
#     try:
#         r = requests.get(
#             "https://api.open-meteo.com/v1/forecast",
#             params={
#                 "latitude": lat,
#                 "longitude": lon,
#                 "daily": "sunrise,sunset",
#                 "timezone": "auto",
#                 "past_days": days_back
#             }, timeout=10
#         )
#
#         r.raise_for_status()
#
#         daily = r.json()["daily"]
#
#         # Find the target date in the response
#         for dt, sunrise, sunset in zip(
#                 daily["time"],
#                 daily["sunrise"],
#                 daily["sunset"]
#         ):
#             if dt == str(target_date):
#                 sunrise_dt = datetime.fromisoformat(sunrise)
#                 sunset_dt = datetime.fromisoformat(sunset)
#
#                 # Round to nearest hour
#                 sunrise_hour = round(sunrise_dt.hour + sunrise_dt.minute / 60)
#                 sunset_hour = round(sunset_dt.hour + sunset_dt.minute / 60)
#
#                 # Handle edge case where rounding gives 24
#                 sunrise_hour = sunrise_hour % 24
#                 sunset_hour = sunset_hour % 24
#
#                 print(f"🌅 Sunrise: {sunrise_dt.strftime('%H:%M')} → hour {sunrise_hour}")
#                 print(f"🌇 Sunset:  {sunset_dt.strftime('%H:%M')} → hour {sunset_hour}")
#                 print("✅ Successfully fetched sun times")
#
#                 return {
#                     "sunrise": sunrise_hour,
#                     "sunset": sunset_hour
#                 }
#
#         print(f"⚠️  No data found for {target_date}, using defaults")
#
#     except Exception as e:
#         print(f"❌ Error fetching sun times: {e}")
#         print(f"⚠️  Using defaults")
#
#     # Return defaults if anything fails
#     print(f"🌅 Default sunrise: {default_sunrise}:00")
#     print(f"🌇 Default sunset:  {default_sunset}:00")
#     return {
#         "sunrise": default_sunrise,
#         "sunset": default_sunset
#     }
#
#
# def get_past_days_data_and_save(days):
#     """Download weather data for specified number of days."""
#     print("\n" + "=" * 60)
#     print(f"📥 DOWNLOADING WEATHER DATA FOR {days} DAY(S)")
#     print("=" * 60)
#
#     for i in range(days):
#         print(f"\n📅 Processing day {i} ({date.today() - timedelta(days=i)})")
#         file_name_csv = generate_filename_csv(i)
#         sun_time = get_sun_times((latitude, longitude), i)
#         w, got_weather_flag = get_weather((latitude, longitude), sun_time["sunrise"], sun_time["sunset"], i)
#         save_weather_to_csv(w, file_name_csv)
#
#     print("\n" + "=" * 60)
#     print("✅ DOWNLOAD COMPLETE")
#     print("=" * 60)
#
#
# def generate_filename_csv(date_back):
#     """Generate CSV filename based on date."""
#     filename = "stored_data/" + f"{date.today() - timedelta(days=date_back)}.csv"
#     return filename
#
#
# def save_weather_to_csv(weather, filename):
#     """Save weather data to CSV file."""
#     print("\n" + "=" * 30)
#     print("💾 SAVING WEATHER DATA TO CSV")
#     print("=" * 30)
#
#     if not weather:
#         print("⚠️  No weather data to save")
#         return
#
#     try:
#         with open(filename, "w", newline="") as f:
#             writer = csv.writer(f)
#             writer.writerow(["time", "temp", "clouds"])  # header
#
#             for entry in weather:
#                 writer.writerow([
#                     entry["time"].isoformat(),
#                     entry["temp"],
#                     entry["clouds"]
#                 ])
#
#         print(f"📁 Filename: {filename}")
#         print(f"📊 Data points saved: {len(weather)}")
#         print("✅ Weather data saved successfully")
#
#     except Exception as e:
#         print(f"❌ Error saving weather data: {e}")
#
#
# def load_weather_from_csv(filename):
#     """Load weather data from CSV file."""
#     print("\n" + "=" * 60)
#     print("📂 LOADING WEATHER DATA FROM CSV")
#     print("=" * 60)
#     print(f"📁 Filename: {filename}")
#
#     weather = []
#     try:
#         with open(filename, newline="") as f:
#             reader = csv.DictReader(f)
#             for row in reader:
#                 weather.append({
#                     "time": datetime.fromisoformat(row["time"]),
#                     "temp": float(row["temp"]),
#                     "clouds": float(row["clouds"])
#                 })
#
#         print(f"📊 Data points loaded: {len(weather)}")
#         print("✅ Weather data loaded successfully")
#         return weather, True
#
#     except FileNotFoundError:
#         print(f"❌ File not found: {filename}")
#         return [], False
#     except Exception as e:
#         print(f"❌ Error loading weather data: {e}")
#         return [], False
#
#
# #### Functions to interact with Home Assistant API #####
# def ha_get_entity_state(entity_id):
#     """Get state of Home Assistant entity."""
#     print(f"\n🔍 Getting state for entity: {entity_id}")
#     response = None
#     url = HA_URL_STATES + f"/{entity_id}"
#
#     try:
#         response = requests.get(url, headers=headers, timeout=10)
#
#         if response.status_code != 200:
#             print(f"❌ Error: {response.status_code} - {response.text}")
#             raise Exception(f"Error: {response.status_code} - {response.text}")
#
#         print("✅ Entity state retrieved successfully")
#         return response.json()
#
#     except Exception as e:
#         print(f"❌ Failed to get entity state: {e}")
#         raise
#
#
# def ha_get_slide_value(entity_id):
#     """Get current value of Home Assistant slider."""
#     slider_data_json = ha_get_entity_state(entity_id)
#     current_value = int(float(slider_data_json["state"]))
#     print(f"📊 Current value: {current_value}")
#     return current_value
#
#
# def ha_set_slide_value(entity_id, value=5, min_value=0, max_value=MAX_DURATION_MINUTES_BOILER):
#     """Set value of Home Assistant slider."""
#     print(f"\n📈 Setting slider value for: {entity_id}")
#
#     # Send POST request
#     try:
#         # Try to convert to integer
#         value = int(value)
#
#         # Clamp to range
#         value = max(min_value, min(value, max_value))
#
#         print(f"🎯 Target value: {value} (range: {min_value}-{max_value})")
#
#     except (ValueError, TypeError) as e:
#         print(f"⚠️  Invalid value '{value}'. Using min_value={min_value}. Error: {e}")
#         value = min_value
#
#     entity_json = {"entity_id": entity_id, "value": value if min_value <= value <= max_value else min_value}
#
#     try:
#         response = requests.post(HA_URL_SERVICES + f"/input_number/set_value", headers=headers, json=entity_json, timeout=10)
#
#         # Check response
#         if response.ok:
#             print(f"✅ Slider updated to {value}")
#             return True
#         else:
#             print(f"❌ Failed to update slider: {response.text}")
#             return False
#
#     except Exception as e:
#         print(f"❌ Error updating slider: {e}")
#         return False
#
#
# def ha_run_script(script_entity_or_name, timeout=10):
#     """
#     Run Home Assistant script. Accepts either 'script.xyz' or 'xyz'.
#     Returns True on success, False on error.
#     """
#     entity_id = script_entity_or_name if str(script_entity_or_name).startswith(
#         "script.") else f"script.{script_entity_or_name}"
#     response = None
#     try:
#         response = requests.post(
#             f"{HA_URL}/api/services/script/turn_on",
#             headers=headers,
#             json={"entity_id": entity_id},
#             timeout=timeout
#         )
#     except Exception as e:
#         print(f"❌ Error calling script {entity_id}: {e}")
#         return False
#
#     if not getattr(response, "ok", False):
#         print(
#             f"❌ Script call failed {entity_id}: {getattr(response, 'status_code', '')} {getattr(response, 'text', '')}")
#         return False
#
#     return True
#
#
# def ha_update_duration_entities(dur, entity1, entity2):
#     """Update Home Assistant boiler entities with calculated duration."""
#     print("\n" + "=" * 60)
#     print("🏠 UPDATING HOME ASSISTANT")
#     print("=" * 60)
#     print(f"⏱️  Total duration to set: {dur} minutes")
#     print(f"📊 Max per boiler: {MAX_DURATION_MINUTES_BOILER} minutes")
#
#     remaining = 0
#     print(f"   Boiler 1: {MAX_DURATION_MINUTES_BOILER} minutes")
#
#     if dur > MAX_DURATION_MINUTES_BOILER:
#         remaining = dur - MAX_DURATION_MINUTES_BOILER
#         print(f"   Boiler 2: {remaining} minutes")
#     else:
#         print(f"   Boiler 2: 0 minutes")
#
#     a = ha_set_slide_value(entity1, dur - remaining) and ha_set_slide_value(entity2, remaining)
#     return a
#
#
# def ha_execute_boiler_start_script(script_id):
#     state_before = ha_get_entity_state(BOILER_ENTITY_ID)["state"]
#     state_after: bool = False
#     if state_before == "off":
#         ha_run_script(script_id)
#         now = datetime.now()
#
#         while state_after != "on" and datetime.now() < now + timedelta(minutes=10):
#             sleep(2)
#             state_after = ha_get_entity_state(BOILER_ENTITY_ID)["state"]
#         if state_after == "on":
#             print("✅ First Boiler duration set and activated")
#             print("=" * 60)
#             return True
#         else:
#             print("❌ Failed to activate boiler after setting duration")
#             print("=" * 60)
#             return False
#     else:
#         print("⚠️  Boiler already ON, skipping activation script")
#         print("=" * 60)
#         return False
#
#
# ### Core logic functions: calculating mean temperature/cloud cover and boiler duration ##
# def get_lut():
#     """Get the temperature to duration lookup table from config."""
#     return {
#         int(k): v
#         for k, v in json.loads(config["temperature_duration_lut"]["temp_lut"]).items()
#     }
#
#
# def calc_daily_mean_T_CC(data, sunrise, sunset, start_offset=2, end_offset=1):
#     """
#     Computes mean temperature and cloud cover in the window:
#     [sunrise + start_offset, sunset - end_offset]
#
#     Returns (None, None) if insufficient data.
#     """
#     print("\n" + "=" * 60)
#     print("📊 CALCULATING DAILY MEAN TEMPERATURE & CLOUD COVER")
#     print("=" * 60)
#
#     if not data:
#         print("⚠️  No data available")
#         return None, None
#
#     data = sorted(data, key=lambda x: x["time"])
#
#     start_hour = sunrise + start_offset
#     end_hour = sunset - end_offset
#
#     print(f"🌅 Sunrise: {sunrise}:00 (+ {start_offset}h offset = {start_hour}:00)")
#     print(f"🌇 Sunset:  {sunset}:00 (- {end_offset}h offset = {end_hour}:00)")
#     print(f"⏰ Analysis window: {start_hour}:00 - {end_hour}:00")
#
#     filtered = [
#         d for d in data
#         if start_hour <= d["time"].hour <= end_hour
#     ]
#
#     if not filtered:
#         print("⚠️  No data points in the analysis window")
#         return None, None
#
#     mean_T = mean(d["temp"] for d in filtered)
#     mean_CC = mean(d["clouds"] for d in filtered)
#
#     print(f"📊 Data points used: {len(filtered)}")
#     print(f"🌡️  Mean Temperature: {mean_T:.1f}°C")
#     print(f"☁️  Mean Cloud Cover: {mean_CC:.1f}%")
#     print("✅ Calculation complete")
#
#     return mean_T, mean_CC
#
#
# def calc_duration(T: float, CC: float, k: float = 0.6, MAX_ON=180) -> int:
#     """
#     Compute boiler ON duration (minutes) from outdoor temperature (T) and cloud coverage (CC).
#
#     T: temperature in °C
#     CC: cloud coverage in %
#     k: max cloud impact factor (default 0.6)
#     """
#     print("\n" + "=" * 60)
#     print("🔥 CALCULATING BOILER DURATION")
#     print("=" * 60)
#     print(f"🌡️  Input Temperature: {T:.1f}°C")
#     print(f"☁️  Input Cloud Cover: {CC:.1f}%")
#     print(f"⚙️  Cloud impact factor (k): {k}")
#
#     temp_lut = get_lut()
#
#     MIN_TEMP_SUN_RELEVANT = min(temp_lut)
#     MAX_TEMP_SUN_RELEVANT = max(temp_lut)
#     # Sort LUT keys
#     temps = sorted(temp_lut.keys())
#
#     # --- handle bounds ---
#     if T <= temps[0]:
#         base = temp_lut[temps[0]]
#     elif T >= temps[-1]:
#         base = temp_lut[temps[-1]]
#     else:
#         # --- linear interpolation between bounds ---
#         for i in range(len(temps) - 1):
#             t0, t1 = temps[i], temps[i + 1]
#             if t0 <= T <= t1:
#                 v0, v1 = temp_lut[t0], temp_lut[t1]
#                 ratio = (T - t0) / (t1 - t0)
#                 base = v0 + ratio * (v1 - v0)
#                 break
#
#     print(f"📐 Base duration (temp only): {base:.0f} minutes")
#
#     # === 2) CLOUD CORRECTION α(CC, T) ===
#     # Temperature sensitivity: 0 at <=10°C, 1 at >=24°C
#     ST = (T - MIN_TEMP_SUN_RELEVANT) / (MAX_TEMP_SUN_RELEVANT - MIN_TEMP_SUN_RELEVANT)
#     ST = max(0.0, min(1.0, ST))  # clamp 0..1
#
#     # Full alpha
#     alpha = k * (CC / 100.0) * ST
#
#     print(f"🌤️  Sun sensitivity factor: {ST:.2f}")
#     print(f"☁️  Cloud correction (α): {alpha:.3f}")
#
#     # === 3) Apply cloud correction ===
#     result = min(int(round(base * (1 + alpha))), MAX_ON)
#
#     print(f"🔥 Final duration: {result} minutes (max: {MAX_ON})")
#     print("✅ Calculation complete")
#
#     return result
#
#
# def read_data_from_saved_file(days_back=DAYS_BACK_WEATHER_DATA):
#     file_name_csv = generate_filename_csv(days_back)
#     w, got_weather_flag = load_weather_from_csv(file_name_csv)
#
#     if got_weather_flag:
#         log_event("info", "Weather data ready", data={"points": len(w)})
#         return w, True
#     else:
#         print(f"❌ Failed to load weather from file {file_name_csv}")
#         log_event("error", "Failed to obtain weather data", code="WEATHER_FETCH_FAIL")
#         return [], False
#
#
# def read_data_from_website():
#     w, got_weather_flag = get_weather((latitude, longitude), 0, 23, DAYS_BACK_WEATHER_DATA)
#     log_event("info", "Weather data fetched from website", data={"points": len(w)})
#
#     if got_weather_flag:
#         log_event("info", "Weather data ready", data={"points": len(w)})
#         if SAVE_DATA:
#             file_name_csv = generate_filename_csv(DAYS_BACK_WEATHER_DATA)
#             save_weather_to_csv(w, file_name_csv)
#             log_event("info", "Wether data saved to file", data={"points": len(w)})
#         return w, True
#     else:
#         print(f"❌ Failed to load weather from website")
#         log_event("error", "Failed to obtain weather data", code="WEATHER_FETCH_FAIL")
#         return [], False
#
#
# def main_headers():
#     print("\n" + "=" * 60)
#     print(f"🚀 BOILER CONTROL SYSTEM STARTED v{APP_VERSION}")
#     print("=" * 60)
#     print(f"🕐 Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
#     print(f"🐛 Debug mode: {debug_mode_ignore_update_time}")
#     print(f"💾 Save data: {SAVE_DATA}")
#     print(f"📂 Read from file: {READ_DATA_FROM_FILE}")
#     print(f"⏰ Sleep interval: {SLEEP_MINUTES} minutes")
#     print("=" * 60)
#
#
# def main_run():
#     global update_for_today
#
#     log_event("info", "Main run started", data={"app_version": APP_VERSION})
#     # try:
#     while True:
#         current_time = datetime.now().time()
#         sun_time = get_sun_times((latitude, longitude), DAYS_BACK_WEATHER_DATA)
#
#         # Daily update check
#         if (current_time >= time(sun_time["sunset"],
#                                  int(2 * SLEEP_MINUTES)) and not update_for_today) or debug_mode_ignore_update_time:
#             print("\n" + "=" * 30)
#             print("STARTING DAILY UPDATE CYCLE")
#             log_event("info", "Starting daily update cycle",
#                       data={"date": str(date.today()), "trigger": datetime.now().isoformat()})
#
#             print("=" * 30)
#             print(f"⏰ Trigger time: {datetime.now().strftime('%H:%M:%S')}")
#             print(f"📅 Date: {date.today()}")
#
#             # Load from file
#             if READ_DATA_FROM_FILE:
#                 w, success = read_data_from_saved_file()
#                 if not success:
#                     sleep(int(60 * SLEEP_MINUTES))
#                     continue
#             # Fetch from API
#             else:
#                 w, success = read_data_from_website()
#                 if not success:
#                     sleep(int(60 * SLEEP_MINUTES))
#                     continue
#
#             daily_t, daily_cc = calc_daily_mean_T_CC(w, int(sun_time["sunrise"]), int(sun_time["sunset"]))
#             log_event("info", "Daily means calculated", data={"mean_T": daily_t, "mean_CC": daily_cc})
#
#             # Calculate boiler duration and update Home Assistant
#             if daily_t is not None and daily_cc is not None:
#                 duration = calc_duration(daily_t, daily_cc)
#                 log_event("info", "Calculated duration", data={"duration_min": duration})
#                 flg = ha_update_duration_entities(duration, BOILER_1ST_ON_ENTITY_ID,
#                                                   BOILER_2ND_ON_ENTITY_ID)
#                 if flg:
#                     b = False
#                     if debug_mode_ignore_update_time:
#                         b = True
#                     else:
#                         b = ha_execute_boiler_start_script(RUN_SCRIPT_1ST_START_BOILER_ENTITY_ID)
#                     if b:
#                         log_event("info", "Boiler start script executed successfully",
#                                   data={"script": RUN_SCRIPT_1ST_START_BOILER_ENTITY_ID})
#                         log_event("info", "Home Assistant update OK", data={"duration": duration})
#                         # finalize and persist summary as success
#                         save_summary(status="success")
#                         update_for_today = True
#                         print("\n" + "=" * 40)
#                         print("DAILY UPDATE COMPLETED SUCCESSFULLY")
#                         print("=" * 40)
#                     else:
#                         log_event("error", "Failed to execute boiler start script",
#                                   code="SCRIPT_EXEC_FAIL",
#                                   data={"script": RUN_SCRIPT_1ST_START_BOILER_ENTITY_ID})
#                         save_summary(status="failed", error="Failed to execute boiler start script")
#
#                         print("\n" + "=" * 40)
#                         print("DAILY UPDATE FAILED - BOILER START SCRIPT ERROR")
#                         print("=" * 40)
#                         sleep(int(60 * SLEEP_MINUTES))
#                 else:
#                     log_event("error", "Home Assistant update failed", code="HA_UPDATE_FAIL")
#                     save_summary(status="failed", error="HA update failed")
#
#                     print("\n" + "=" * 40)
#                     print("DAILY UPDATE FAILED - HOME ASSISTANT ERROR")
#                     print("=" * 40)
#                     sleep(int(60 * SLEEP_MINUTES))
#             else:
#                 log_event("warning", "Data fetch failed", code="API failure")
#                 save_summary(status="failed", error="insufficient data")
#
#                 print("\n" + "=" * 40)
#                 print("DAILY UPDATE FAILED - COULD NOT CALCULATE DURATION")
#                 print("=" * 40)
#                 sleep(int(60 * SLEEP_MINUTES))
#
#
#         ############ Reset update flag at start of new day ########################################
#         elif (time(7, 0) <= current_time < time(14, 0) and update_for_today) and not debug_mode_ignore_update_time:
#             update_for_today = False
#             log_event("info", "Reset update_for_today flag for new day")
#
#             print("\n" + "=" * 30)
#             print("🌅 NEW DAY - RESETTING UPDATE FLAG")
#             print(f"⏰ Time: {datetime.now().strftime('%H:%M:%S')}")
#             print("=" * 30)
#
#         ###################### Sleep and wait for next check ############################
#         else:
#             print(f"\n⏸️  [{datetime.now().strftime('%H:%M:%S')}] Waiting... (next check in {SLEEP_MINUTES} min)")
#             print(f"   Current time: {current_time.strftime('%H:%M')}")
#             print(f"   Update window: 17:00+")
#             print(f"   Updated today: {update_for_today}")
#             print(f"   Debug mode: {debug_mode_ignore_update_time}")
#
#             sleep(int(60 * SLEEP_MINUTES))
#             # except Exception as e:
#             #     log_event("error", "Unhandled exception in main loop", code="UNHANDLED_EXCEPTION", data={"exception": str(e)})
#             #     save_summary(status="failed", error=str(e))
#             #     sleep(int(60 * SLEEP_MINUTES))
#
#
# # Main execution
# if __name__ == "__main__":
#     init_logger()
#     main_headers()
#     main_run()
