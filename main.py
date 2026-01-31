import csv
import requests
import configparser
from time import sleep
from statistics import mean
from datetime import datetime, timedelta, time, date

from scipy.stats import false_discovery_control

config = configparser.ConfigParser()
config.read('config.ini')

# Read base values from config
TOKEN = config['homeassistant']['token']
HA_IP = config['homeassistant']['ip']
HA_PORT = config['homeassistant']['port']
BOILER_1ST_ON_ENTITY_ID = config["homeassistant"]["BOILER_1ST_ON_ENTITY_ID"]
BOILER_2ND_ON_ENTITY_ID = config["homeassistant"]["BOILER_2ND_ON_ENTITY_ID"]

latitude = float(config['location']['latitude'])
longitude = float(config['location']['longitude'])
weather_url = config['weather']['weather_url']

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
    lat, lon = location
    target_date = date.today() - timedelta(days=days_back)
    now = datetime.now()  # Add this line

    print("Fetching Open-Meteo weather")
    print(f"Location: {lat}, {lon}")
    print(f"Target date: {target_date}")
    print(f"Hours window: {start_hour}:00–{end_hour}:00")

    r = requests.get(
        weather_url,
        params={
            "latitude": lat,
            "longitude": lon,
            "hourly": "temperature_2m,cloudcover",
            "past_days": days_back,
            "timezone": "auto"
        }
    )
    weather = []

    r.raise_for_status()
    if r.status_code != 200:
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
                continue  # Return False if future data encountered

            weather.append({
                "time": ts,
                "temp": temp,
                "clouds": clouds
            })

            print(f"{ts} | T={temp}°C | Clouds={clouds}%")

        return weather, len(weather) > 0


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
    lat, lon = location
    target_date = date.today() - timedelta(days=days_back)

    # print(f"Fetching sunrise/sunset for {target_date}")
    # print(f"Location: {lat}, {lon}")

    try:
        r = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": lat,
                "longitude": lon,
                "daily": "sunrise,sunset",
                "timezone": "auto",
                "past_days": days_back
            }
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

                print(f"Sunrise: {sunrise_dt.strftime('%H:%M')} → hour {sunrise_hour}")
                print(f"Sunset:  {sunset_dt.strftime('%H:%M')} → hour {sunset_hour}")

                return {
                    "sunrise": sunrise_hour,
                    "sunset": sunset_hour
                }

        print(f"No data found for {target_date}, using defaults")

    except Exception as e:
        print(f"Error fetching sun times: {e}, using defaults")

    # Return defaults if anything fails
    print(f"Using default values: sunrise={default_sunrise}, sunset={default_sunset}")
    return {
        "sunrise": default_sunrise,
        "sunset": default_sunset
    }


def save_weather_to_csv(weather, filename):
    if not weather:
        print("No weather data to save.")
        return

    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["time", "temp", "clouds"])  # header

        for entry in weather:
            writer.writerow([
                entry["time"].isoformat(),
                entry["temp"],
                entry["clouds"]
            ])

    print(f"Weather data saved to {filename}")


def load_weather_from_csv(filename):
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
        print(f"Loaded weather from {filename}")
        return weather, True

    except FileNotFoundError:
        print(f"File not found: {filename}")
        return [], False


def generate_filename_csv(date_back):
    filename = "stored_data/" + f"{date.today() - timedelta(days=date_back)}.csv"
    return filename


def get_entity_state(entity_id):
    url = HA_URL_STATES + f"/{entity_id}"
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        raise Exception(f"Error: {response.status_code} - {response.text}")

    return response.json()


def get_slide_value(entity_id):
    slider_data_json = get_entity_state(entity_id)
    current_value = int(float(slider_data_json["state"]))
    # print(f"    📈 Current value of '{entity_id}': {current_value}")
    return current_value


def set_slide_value(entity_id, value=5, min_value=0, max_value=MAX_DURATION_MINUTES_BOILER):
    # Send POST request
    try:
        # Try to convert to integer
        value = int(value)

        # Clamp to range
        value = max(min_value, min(value, max_value))

    except (ValueError, TypeError) as e:
        print(f"Warning: Invalid value '{value}' for {entity_id}. Using min_value={min_value}. Error: {e}")
        value = min_value

    entity_json = {"entity_id": entity_id, "value": value if min_value <= value <= max_value else min_value}
    response = requests.post(HA_URL_SERVICES + f"/input_number/set_value", headers=headers, json=entity_json)

    # Check response
    if response.ok:
        # print(f"    📈 Slider '{entity_id}' updated to {value}")
        return True
    else:
        print(f"Failed to update slider: {response.text}")
        return False


def daily_mean_T_CC(data, sunrise, sunset, start_offset=2, end_offset=1):
    """
    Computes mean temperature and cloud cover in the window:
    [sunrise + start_offset, sunset - end_offset]

    Returns (None, None) if insufficient data.
    """
    if not data:
        return None, None

    data = sorted(data, key=lambda x: x["time"])

    start_hour = sunrise + start_offset
    end_hour = sunset - end_offset

    filtered = [
        d for d in data
        if start_hour <= d["time"].hour <= end_hour
    ]

    if not filtered:
        return None, None

    mean_T = mean(d["temp"] for d in filtered)
    mean_CC = mean(d["clouds"] for d in filtered)

    return mean_T, mean_CC


def boiler_duration(T: float, CC: float, k: float = 0.6, MAX_ON=180) -> int:
    """
    Compute boiler ON duration (minutes) from outdoor temperature (T) and cloud coverage (CC).

    T: temperature in °C
    CC: cloud coverage in %
    k: max cloud impact factor (default 0.3)
    """

    MIN_TEMP_SUN_RELEVANT = 10
    MAX_TEMP_SUN_RELEVANT = 20

    # === 1) TEMP → BASE DURATION LUT ===
    # (from your graph)
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
    print("Temperature only calculation: {}".format(base))
    # === 2) CLOUD CORRECTION α(CC, T) ===
    # Temperature sensitivity: 0 at <=10°C, 1 at >=24°C
    ST = (T - MIN_TEMP_SUN_RELEVANT) / (MAX_TEMP_SUN_RELEVANT - MIN_TEMP_SUN_RELEVANT)
    ST = max(0.0, min(1.0, ST))  # clamp 0..1

    # Full alpha
    alpha = k * (CC / 100.0) * ST

    # === 3) Apply cloud correction ===
    print(base)
    result = min(int(round(base * (1 + alpha))), MAX_ON)

    return result


def update_homeassistant(dur, entity1, entity2):

    set_slide_value(entity1, dur)
    if dur > MAX_DURATION_MINUTES_BOILER:
        a= set_slide_value(entity2, dur - MAX_DURATION_MINUTES_BOILER)
        return a
    else:
        a = set_slide_value(entity2, 0)
        return a




def download_data(days):
    for i in range(days):
        file_name_csv = generate_filename_csv(i)
        sun_time = get_sun_times((latitude, longitude), i)
        w, got_weather_flag = get_weather((latitude, longitude), sun_time["sunrise"], sun_time["sunset"], i)
        save_weather_to_csv(w, file_name_csv)


SAVE_DATA = True
SLEEP_MINUTES = 10
READ_DATA_FROM_FILE = False
debug_mode_ignore_update_time = True
DAYS_BACK_WEATHER_DATA = 0  # 0 = today, 1 = yesterday, 2 = the day before yesterday

# Main execution
if __name__ == "__main__":
    print("Current time is {}".format(datetime.now()))

    while True:
        if (datetime.now().time() >= time(17, 0) or debug_mode_ignore_update_time) and not update_for_today:
            sun_time = get_sun_times((latitude, longitude), DAYS_BACK_WEATHER_DATA)  # get sunrise and sunset

            if READ_DATA_FROM_FILE:
                file_name_csv = generate_filename_csv(DAYS_BACK_WEATHER_DATA)
                read_weather_flag, succeed_read_file = load_weather_from_csv(file_name_csv)
                if read_weather_flag:
                    w, got_weather_flag = load_weather_from_csv(file_name_csv)
                else:
                    print("Failed to load weather from file {}".format(file_name_csv))
                    break
            else:
                w, got_weather_flag = get_weather((latitude, longitude), sun_time["sunrise"], sun_time["sunset"],
                                                  DAYS_BACK_WEATHER_DATA)
                if got_weather_flag and SAVE_DATA:
                    file_name_csv = generate_filename_csv(DAYS_BACK_WEATHER_DATA)
                    save_weather_to_csv(w, file_name_csv)

            daily_t, daily_cc = daily_mean_T_CC(w, sun_time["sunrise"], sun_time["sunset"])
            print(f"Daily mean T: {daily_t} °C, Daily mean CC: {daily_cc} %")
            duration = boiler_duration(daily_t, daily_cc)
            print(f"Calculated boiler duration: {duration} minutes")

            update_homeassistant(duration, BOILER_1ST_ON_ENTITY_ID, BOILER_2ND_ON_ENTITY_ID)
            update_for_today = True

        elif (time(7, 0) <= datetime.now().time() < time(17,
                                                         0) and update_for_today) and not debug_mode_ignore_update_time:
            update_for_today = False
            print("After midnight, resetting for the next day")
        else:
            print("nothing to do yet...")
            sleep(60 * SLEEP_MINUTES)
