import os
import sys
import csv
import json
import requests
import configparser
from time import sleep
from statistics import mean
from datetime import datetime, timedelta, time, date

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

config = configparser.ConfigParser()
# config.ini holds non-secret settings (tracked in git).
# config.local.ini is optional, git-ignored, and overrides config.ini (put the token here).
config.read([os.path.join(BASE_DIR, 'config.ini'), os.path.join(BASE_DIR, 'config.local.ini')])

# Read base values from config. The token never lives in git:
# environment variable HA_TOKEN wins, otherwise config.local.ini [homeassistant] token.
TOKEN = os.environ.get("HA_TOKEN") or config.get('homeassistant', 'token', fallback="")
if not TOKEN:
    sys.exit("❌ No Home Assistant token. Set env var HA_TOKEN or add it to config.local.ini "
             "(see config.local.ini.example).")
HA_IP = config['homeassistant']['ip']
HA_PORT = config['homeassistant']['port']
BOILER_1ST_ON_ENTITY_ID = config["homeassistant"]["BOILER_1ST_ON_ENTITY_ID"]
BOILER_2ND_ON_ENTITY_ID = config["homeassistant"]["BOILER_2ND_ON_ENTITY_ID"]
RUN_SCRIPT_1ST_START_BOILER_ENTITY_ID = config["homeassistant"]["RUN_SCRIPT_1ST_START_BOILER_ENTITY_ID"]
RUN_SCRIPT_2ND_START_BOILER_ENTITY_ID = config["homeassistant"]["RUN_SCRIPT_2ND_START_BOILER_ENTITY_ID"]

latitude = float(config['location']['latitude'])
longitude = float(config['location']['longitude'])
weather_url = config['weather']['weather_url']

SAVE_DATA = config['parameters']['SAVE_DATA'].lower() == "true"
SLEEP_MINUTES = int(config["parameters"]["SLEEP_MINUTES"])
READ_DATA_FROM_FILE = config["parameters"]["READ_DATA_FROM_FILE"].lower() == "true"
debug_mode_ignore_update_time = config["parameters"]["debug_mode_ignore_update_time"].lower() == "true"
DAYS_BACK_WEATHER_DATA = int(config["parameters"]["DAYS_BACK_WEATHER_DATA"])

APP_VERSION = "1.1.0"

REQUEST_TIMEOUT = 15          # seconds; no network call may hang forever
HA_RETRIES = 3                # attempts per Home Assistant call
HA_RETRY_DELAY = 5            # seconds between attempts
STATE_FILE = os.path.join(BASE_DIR, "state.json")           # survives restarts
DECISIONS_FILE = os.path.join(BASE_DIR, "stored_data", "decisions.csv")  # what was set, and why


# Build URLs in Python (where f-strings work)
HA_URL = f"http://{HA_IP}:{HA_PORT}"
HA_URL_SERVICES = f"{HA_URL}/api/services"
HA_URL_STATES = f"{HA_URL}/api/states"
HA_URL_WS = f"ws://{HA_IP}:{HA_PORT}/api/websocket"
HA_URL_HISTORY = f"{HA_URL}/api/history/period"
HA_URL_STAT = f"{HA_URL}/api/history/statistics"


headers = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json",
}

THRESHOLD_TEMPERATURE = 22  # Celsius. past this temperature, the boiler is less needed.
MAX_DURATION_MINUTES_BOILER = 120

update_for_today = False


def get_weather(location, start_hour, end_hour, days_back=0):
    """Fetch weather data from Open-Meteo API."""
    print("\n" + "=" * 60)
    print("🌤️  FETCHING WEATHER DATA")
    print("=" * 60)

    lat, lon = location
    target_date = date.today() - timedelta(days=days_back)
    now = datetime.now()

    print(f"📍 Location: {lat}, {lon}")
    print(f"📅 Target date: {target_date}")
    print(f"⏰ Hours window: {start_hour}:00–{end_hour}:00")

    try:
        r = requests.get(
            weather_url,
            params={
                "latitude": lat,
                "longitude": lon,
                "hourly": "temperature_2m,cloudcover",
                "past_days": days_back,
                "timezone": "auto"
            },
            timeout=REQUEST_TIMEOUT
        )

        r.raise_for_status()
        weather = []

        if r.status_code != 200:
            print("❌ Failed to fetch weather data")
            return weather, False
        else:
            hourly = r.json()["hourly"]

            for t, temp, clouds in zip(
                    hourly["time"],
                    hourly["temperature_2m"],
                    hourly["cloudcover"]
            ):
                ts = datetime.fromisoformat(t)

                if ts.date() != target_date:
                    continue
                if not (start_hour <= ts.hour <= end_hour):
                    continue

                # Skip future hours when looking at today
                if days_back == 0 and ts > now:
                    continue

                weather.append({
                    "time": ts,
                    "temp": temp,
                    "clouds": clouds
                })

                print(f"   {ts.strftime('%H:%M')} | 🌡️  {temp}°C | ☁️  {clouds}%")

            print(f"✅ Successfully fetched {len(weather)} weather data points")
            return weather, len(weather) > 0

    except Exception as e:
        print(f"❌ Error fetching weather: {e}")
        return [], False


def get_sun_times(location, days_back=0, default_sunrise=7, default_sunset=17):
    """
    Get sunrise and sunset times for a location, rounded to nearest hour.

    Args:
        location: tuple of (latitude, longitude)
        days_back: number of days back from today (0 = today, 1 = yesterday, etc.)
        default_sunrise: default sunrise hour if retrieval fails (default: 7)
        default_sunset: default sunset hour if retrieval fails (default: 17)

    Returns:
        dict with 'sunrise' and 'sunset' as integers (hour 0-23)
    """
    print("\n" + "=" * 60)
    print("🌅 FETCHING SUNRISE/SUNSET TIMES")
    print("=" * 60)

    lat, lon = location
    target_date = date.today() - timedelta(days=days_back)

    print(f"📍 Location: {lat}, {lon}")
    print(f"📅 Target date: {target_date}")

    try:
        r = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": lat,
                "longitude": lon,
                "daily": "sunrise,sunset",
                "timezone": "auto",
                "past_days": days_back
            },
            timeout=REQUEST_TIMEOUT
        )

        r.raise_for_status()

        daily = r.json()["daily"]

        # Find the target date in the response
        for dt, sunrise, sunset in zip(
                daily["time"],
                daily["sunrise"],
                daily["sunset"]
        ):
            if dt == str(target_date):
                sunrise_dt = datetime.fromisoformat(sunrise)
                sunset_dt = datetime.fromisoformat(sunset)

                # Round to nearest hour
                sunrise_hour = round(sunrise_dt.hour + sunrise_dt.minute / 60)
                sunset_hour = round(sunset_dt.hour + sunset_dt.minute / 60)

                # Handle edge case where rounding gives 24
                sunrise_hour = sunrise_hour % 24
                sunset_hour = sunset_hour % 24

                print(f"🌅 Sunrise: {sunrise_dt.strftime('%H:%M')} → hour {sunrise_hour}")
                print(f"🌇 Sunset:  {sunset_dt.strftime('%H:%M')} → hour {sunset_hour}")
                print("✅ Successfully fetched sun times")

                return {
                    "sunrise": sunrise_hour,
                    "sunset": sunset_hour
                }

        print(f"⚠️  No data found for {target_date}, using defaults")

    except Exception as e:
        print(f"❌ Error fetching sun times: {e}")
        print(f"⚠️  Using defaults")

    # Return defaults if anything fails
    print(f"🌅 Default sunrise: {default_sunrise}:00")
    print(f"🌇 Default sunset:  {default_sunset}:00")
    return {
        "sunrise": default_sunrise,
        "sunset": default_sunset
    }


def save_weather_to_csv(weather, filename):
    """Save weather data to CSV file."""
    print("\n" + "=" * 30)
    print("💾 SAVING WEATHER DATA TO CSV")
    print("=" * 30)

    if not weather:
        print("⚠️  No weather data to save")
        return

    try:
        os.makedirs(os.path.dirname(os.path.abspath(filename)), exist_ok=True)
        with open(filename, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["time", "temp", "clouds"])  # header

            for entry in weather:
                writer.writerow([
                    entry["time"].isoformat(),
                    entry["temp"],
                    entry["clouds"]
                ])

        print(f"📁 Filename: {filename}")
        print(f"📊 Data points saved: {len(weather)}")
        print("✅ Weather data saved successfully")

    except Exception as e:
        print(f"❌ Error saving weather data: {e}")


def load_weather_from_csv(filename):
    """Load weather data from CSV file."""
    print("\n" + "=" * 60)
    print("📂 LOADING WEATHER DATA FROM CSV")
    print("=" * 60)
    print(f"📁 Filename: {filename}")

    weather = []
    try:
        with open(filename, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                weather.append({
                    "time": datetime.fromisoformat(row["time"]),
                    "temp": float(row["temp"]),
                    "clouds": float(row["clouds"])
                })

        print(f"📊 Data points loaded: {len(weather)}")
        print("✅ Weather data loaded successfully")
        return weather, True

    except FileNotFoundError:
        print(f"❌ File not found: {filename}")
        return [], False
    except Exception as e:
        print(f"❌ Error loading weather data: {e}")
        return [], False


def generate_filename_csv(date_back):
    """Generate CSV filename based on date."""
    return os.path.join(BASE_DIR, "stored_data", f"{date.today() - timedelta(days=date_back)}.csv")


def get_entity_state(entity_id):
    """Get state of Home Assistant entity."""
    print(f"\n🔍 Getting state for entity: {entity_id}")

    url = HA_URL_STATES + f"/{entity_id}"

    try:
        response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)

        if response.status_code != 200:
            print(f"❌ Error: {response.status_code} - {response.text}")
            raise Exception(f"Error: {response.status_code} - {response.text}")

        print("✅ Entity state retrieved successfully")
        return response.json()

    except Exception as e:
        print(f"❌ Failed to get entity state: {e}")
        raise


def get_slide_value(entity_id):
    """Get current value of Home Assistant slider."""
    slider_data_json = get_entity_state(entity_id)
    current_value = int(float(slider_data_json["state"]))
    print(f"📊 Current value: {current_value}")
    return current_value


def set_slide_value(entity_id, value=5, min_value=0, max_value=MAX_DURATION_MINUTES_BOILER):
    """Set value of Home Assistant slider."""
    print(f"\n📈 Setting slider value for: {entity_id}")

    # Send POST request
    try:
        # Try to convert to integer
        value = int(value)

        # Clamp to range
        value = max(min_value, min(value, max_value))

        print(f"🎯 Target value: {value} (range: {min_value}-{max_value})")

    except (ValueError, TypeError) as e:
        print(f"⚠️  Invalid value '{value}'. Using min_value={min_value}. Error: {e}")
        value = min_value

    entity_json = {"entity_id": entity_id, "value": value}

    return _ha_post(HA_URL_SERVICES + "/input_number/set_value", entity_json,
                    f"slider {entity_id} -> {value}")


def _ha_post(url, payload, what):
    """POST to Home Assistant with timeout and a few retries. Returns True on success."""
    for attempt in range(1, HA_RETRIES + 1):
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=REQUEST_TIMEOUT)
            if response.ok:
                print(f"✅ {what} (attempt {attempt})")
                return True
            print(f"❌ {what} failed (attempt {attempt}): {response.status_code} {response.text}")
            if response.status_code in (401, 403, 404):
                return False  # retrying will not help (bad token / wrong entity)
        except Exception as e:
            print(f"❌ {what} error (attempt {attempt}): {e}")
        if attempt < HA_RETRIES:
            sleep(HA_RETRY_DELAY)
    return False


def run_ha_script(script_name):
    return _ha_post(f"{HA_URL}/api/services/script/turn_on",
                    {"entity_id": f"script.{script_name}"},
                    f"run script {script_name}")


def daily_mean_T_CC(data, sunrise, sunset, start_offset=2, end_offset=1):
    """
    Computes mean temperature and cloud cover in the window:
    [sunrise + start_offset, sunset - end_offset]

    Returns (None, None) if insufficient data.
    """
    print("\n" + "=" * 60)
    print("📊 CALCULATING DAILY MEAN TEMPERATURE & CLOUD COVER")
    print("=" * 60)

    if not data:
        print("⚠️  No data available")
        return None, None

    data = sorted(data, key=lambda x: x["time"])

    start_hour = sunrise + start_offset
    end_hour = sunset - end_offset

    print(f"🌅 Sunrise: {sunrise}:00 (+ {start_offset}h offset = {start_hour}:00)")
    print(f"🌇 Sunset:  {sunset}:00 (- {end_offset}h offset = {end_hour}:00)")
    print(f"⏰ Analysis window: {start_hour}:00 - {end_hour}:00")

    filtered = [
        d for d in data
        if start_hour <= d["time"].hour <= end_hour
    ]

    if not filtered:
        print("⚠️  No data points in the analysis window")
        return None, None

    mean_T = mean(d["temp"] for d in filtered)
    mean_CC = mean(d["clouds"] for d in filtered)

    print(f"📊 Data points used: {len(filtered)}")
    print(f"🌡️  Mean Temperature: {mean_T:.1f}°C")
    print(f"☁️  Mean Cloud Cover: {mean_CC:.1f}%")
    print("✅ Calculation complete")

    return mean_T, mean_CC


def boiler_duration(T: float, CC: float, k: float = 0.6, MAX_ON=180) -> int:
    """
    Compute boiler ON duration (minutes) from outdoor temperature (T) and cloud coverage (CC).

    T: temperature in °C
    CC: cloud coverage in %
    k: max cloud impact factor (default 0.6)
    """
    print("\n" + "=" * 60)
    print("🔥 CALCULATING BOILER DURATION")
    print("=" * 60)
    print(f"🌡️  Input Temperature: {T:.1f}°C")
    print(f"☁️  Input Cloud Cover: {CC:.1f}%")
    print(f"⚙️  Cloud impact factor (k): {k}")

    MIN_TEMP_SUN_RELEVANT = 10
    MAX_TEMP_SUN_RELEVANT = 20

    # === 1) TEMP → BASE DURATION LUT ===
    temp_lut = {
        6: 180, 8: 180, 10: 160,
        12: 150, 14: 130, 16: 100,
        18: 60, 20: 40, 22: 20,
        24: 10, 26: 0, 28: 0, 30: 0
    }

    # Sort LUT keys
    temps = sorted(temp_lut.keys())

    # --- handle bounds ---
    if T <= temps[0]:
        base = temp_lut[temps[0]]
    elif T >= temps[-1]:
        base = temp_lut[temps[-1]]
    else:
        # --- linear interpolation between bounds ---
        for i in range(len(temps) - 1):
            t0, t1 = temps[i], temps[i + 1]
            if t0 <= T <= t1:
                v0, v1 = temp_lut[t0], temp_lut[t1]
                ratio = (T - t0) / (t1 - t0)
                base = v0 + ratio * (v1 - v0)
                break

    print(f"📐 Base duration (temp only): {base:.0f} minutes")

    # === 2) CLOUD CORRECTION α(CC, T) ===
    # Temperature sensitivity: 0 at <=10°C, 1 at >=20°C (MIN/MAX_TEMP_SUN_RELEVANT)
    ST = (T - MIN_TEMP_SUN_RELEVANT) / (MAX_TEMP_SUN_RELEVANT - MIN_TEMP_SUN_RELEVANT)
    ST = max(0.0, min(1.0, ST))  # clamp 0..1

    # Full alpha
    alpha = k * (CC / 100.0) * ST

    print(f"🌤️  Sun sensitivity factor: {ST:.2f}")
    print(f"☁️  Cloud correction (α): {alpha:.3f}")

    # === 3) Apply cloud correction ===
    result = min(int(round(base * (1 + alpha))), MAX_ON)

    print(f"🔥 Final duration: {result} minutes (max: {MAX_ON})")
    print("✅ Calculation complete")

    return result


def update_homeassistant(dur, entity1, entity2):
    """Update Home Assistant boiler entities with calculated duration."""
    print("\n" + "=" * 60)
    print("🏠 UPDATING HOME ASSISTANT")
    print("=" * 60)
    print(f"⏱️  Total duration to set: {dur} minutes")
    print(f"📊 Max per boiler: {MAX_DURATION_MINUTES_BOILER} minutes")

    remaining = 0
    print(f"   Boiler 1: {MAX_DURATION_MINUTES_BOILER} minutes")

    if dur > MAX_DURATION_MINUTES_BOILER:
        remaining = dur - MAX_DURATION_MINUTES_BOILER
        print(f"   Boiler 2: {remaining} minutes")
    else:
        print(f"   Boiler 2: 0 minutes")

    ok = set_slide_value(entity1, dur - remaining) and set_slide_value(entity2, remaining)
    if ok:
        ok = run_ha_script(RUN_SCRIPT_1ST_START_BOILER_ENTITY_ID)
        if ok:
            print("✅ First Boiler duration set and activated")
        # The 2nd run is only needed when the total exceeds one boiler cycle.
        if ok and remaining > 0:
            ok = run_ha_script(RUN_SCRIPT_2ND_START_BOILER_ENTITY_ID)
            if ok:
                print("✅ Second Boiler activated")
    if not ok:
        print("❌ Failed to set/activate Boiler durations")

    print("=" * 60)
    return ok


def load_state():
    """Return the date (YYYY-MM-DD) of the last successful update, or None."""
    try:
        with open(STATE_FILE) as f:
            return json.load(f).get("last_update_date")
    except (FileNotFoundError, ValueError):
        return None


def save_state(day):
    try:
        with open(STATE_FILE, "w") as f:
            json.dump({"last_update_date": str(day)}, f)
    except OSError as e:
        print(f"⚠️  Could not save state: {e}")


def log_decision(mean_t, mean_cc, duration, ha_ok):
    """Append what was decided (inputs + result) so the formula can be reviewed later."""
    try:
        os.makedirs(os.path.dirname(DECISIONS_FILE), exist_ok=True)
        new_file = not os.path.exists(DECISIONS_FILE)
        with open(DECISIONS_FILE, "a", newline="") as f:
            writer = csv.writer(f)
            if new_file:
                writer.writerow(["timestamp", "mean_temp", "mean_clouds", "duration_min", "ha_ok"])
            writer.writerow([datetime.now().isoformat(timespec="seconds"),
                             round(mean_t, 1), round(mean_cc, 1), duration, ha_ok])
    except OSError as e:
        print(f"⚠️  Could not write decision log: {e}")


def download_data(days):
    """Download weather data for specified number of days."""
    print("\n" + "=" * 60)
    print(f"📥 DOWNLOADING WEATHER DATA FOR {days} DAY(S)")
    print("=" * 60)

    for i in range(days):
        print(f"\n📅 Processing day {i} ({date.today() - timedelta(days=i)})")
        file_name_csv = generate_filename_csv(i)
        sun_time = get_sun_times((latitude, longitude), i)
        w, got_weather_flag = get_weather((latitude, longitude), sun_time["sunrise"], sun_time["sunset"], i)
        save_weather_to_csv(w, file_name_csv)

    print("\n" + "=" * 60)
    print("✅ DOWNLOAD COMPLETE")
    print("=" * 60)

def main_header():
    print("\n" + "=" * 60)
    print(f"🚀 BOILER CONTROL SYSTEM STARTED v{APP_VERSION}")
    print("=" * 60)
    print(f"🕐 Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🐛 Debug mode: {debug_mode_ignore_update_time}")
    print(f"💾 Save data: {SAVE_DATA}")
    print(f"📂 Read from file: {READ_DATA_FROM_FILE}")
    print(f"⏰ Sleep interval: {SLEEP_MINUTES} minutes")
    print("=" * 60)

def run_daily_update():
    """One full update attempt. Returns True only if Home Assistant was updated."""
    print("\n" + "=" * 30)
    print("STARTING DAILY UPDATE CYCLE")
    print("=" * 30)
    print(f"⏰ Trigger time: {datetime.now().strftime('%H:%M:%S')}")
    print(f"📅 Date: {date.today()}")

    sun_time = get_sun_times((latitude, longitude), DAYS_BACK_WEATHER_DATA)
    file_name_csv = generate_filename_csv(DAYS_BACK_WEATHER_DATA)

    if READ_DATA_FROM_FILE:
        w, got_weather_flag = load_weather_from_csv(file_name_csv)
    else:
        w, got_weather_flag = get_weather((latitude, longitude), sun_time["sunrise"], sun_time["sunset"],
                                          DAYS_BACK_WEATHER_DATA)
        if got_weather_flag and SAVE_DATA:
            save_weather_to_csv(w, file_name_csv)

    if not got_weather_flag:
        print("\n❌ DAILY UPDATE FAILED - NO WEATHER DATA (will retry)")
        return False

    daily_t, daily_cc = daily_mean_T_CC(w, sun_time["sunrise"], sun_time["sunset"])
    if daily_t is None or daily_cc is None:
        print("\n❌ DAILY UPDATE FAILED - INSUFFICIENT DATA (will retry)")
        return False

    duration = boiler_duration(daily_t, daily_cc)
    ha_ok = update_homeassistant(duration, BOILER_1ST_ON_ENTITY_ID, BOILER_2ND_ON_ENTITY_ID)
    log_decision(daily_t, daily_cc, duration, ha_ok)

    if ha_ok:
        print("\n" + "=" * 40)
        print("DAILY UPDATE COMPLETED SUCCESSFULLY")
        print("=" * 40)
    else:
        print("\n❌ DAILY UPDATE FAILED - HOME ASSISTANT NOT UPDATED (will retry)")
    return ha_ok


# Main execution
if __name__ == "__main__":
    main_header()

    while True:
        now = datetime.now()
        # "Done today" is kept on disk, so a restart after 17:01 does not repeat the update.
        done_today = load_state() == str(date.today())
        due = now.time() >= time(17, 1) or debug_mode_ignore_update_time

        if due and (not done_today or debug_mode_ignore_update_time):
            if run_daily_update():
                save_state(date.today())
                if debug_mode_ignore_update_time:
                    break  # debug: one run, then stop
            # on failure: fall through to the sleep below and try again next cycle
        else:
            print(f"\n⏸️  [{now.strftime('%H:%M:%S')}] Waiting... (next check in {SLEEP_MINUTES} min)")
            print(f"   Update window: 17:01+")
            print(f"   Updated today: {done_today}")
            print(f"   Debug mode: {debug_mode_ignore_update_time}")

        sleep(60 * SLEEP_MINUTES)
