"""All configuration in one place. Nothing else in the package reads files or env directly.

Precedence (later wins): config.ini  <  config.local.ini (git-ignored, secrets)  <  settings.json
(written by the Settings tab / setup wizard, git-ignored)  <  environment.
Secrets: HA_TOKEN (Home Assistant), MQTT_PASSWORD (broker).
"""
import os
import json
import configparser

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _bool(v) -> bool:
    return str(v).strip().lower() in ("1", "true", "yes", "on")


class Settings:
    def __init__(self, ini_paths=None, environ=None, json_path=None):
        env = os.environ if environ is None else environ
        # interpolation=None: '%' is literal (Tasmota topics use %topic%, and passwords may contain '%')
        cp = configparser.ConfigParser(interpolation=None)
        cp.read(ini_paths or [os.path.join(BASE_DIR, "config.ini"),
                              os.path.join(BASE_DIR, "config.local.ini")])
        # the UI's settings file: default location only when using the default ini files (tests pass their own)
        self.settings_json = json_path or (None if ini_paths else os.path.join(BASE_DIR, "settings.json"))
        if self.settings_json:
            try:
                with open(self.settings_json) as f:
                    cp.read_dict({sec: {k: str(v) for k, v in vals.items()} for sec, vals in json.load(f).items()})
            except FileNotFoundError:
                pass
            except (ValueError, OSError, AttributeError) as e:
                print(f"⚠️  {self.settings_json} unreadable ({e}); ignoring it")
        self._cp = cp

        g = cp.get
        # --- general behaviour ---
        self.save_data = _bool(g("parameters", "SAVE_DATA", fallback="True"))
        self.sleep_minutes = int(g("parameters", "SLEEP_MINUTES", fallback="15"))
        self.read_from_file = _bool(g("parameters", "READ_DATA_FROM_FILE", fallback="False"))
        self.debug_ignore_time = _bool(g("parameters", "debug_mode_ignore_update_time", fallback="False"))
        self.days_back = int(g("parameters", "DAYS_BACK_WEATHER_DATA", fallback="0"))
        self.update_after = g("parameters", "UPDATE_AFTER", fallback="17:01")   # HH:MM local
        self.request_timeout = int(g("parameters", "REQUEST_TIMEOUT", fallback="15"))

        # --- location / weather ---
        lat, lon = g("location", "latitude", fallback=""), g("location", "longitude", fallback="")
        self.latitude = float(lat) if lat.strip() else None          # None = not set up yet (wizard)
        self.longitude = float(lon) if lon.strip() else None
        self.place_name = g("location", "name", fallback="")
        self.weather_url = g("weather", "weather_url", fallback="https://api.open-meteo.com/v1/forecast")

        # --- output: ordered list of drivers; the next one is tried only if the previous failed ---
        names = g("output", "drivers", fallback="")
        self.driver_names = [n.strip() for n in names.split(",") if n.strip()]

        # --- Home Assistant driver ---
        self.ha_ip = g("homeassistant", "ip", fallback=g("homeassistant", "HA_IP", fallback=""))
        self.ha_port = g("homeassistant", "port", fallback=g("homeassistant", "HA_PORT", fallback="8123"))
        self.ha_token = env.get("HA_TOKEN") or g("homeassistant", "token", fallback="")
        self.ha_slider_1 = g("homeassistant", "BOILER_1ST_ON_ENTITY_ID", fallback="")
        self.ha_slider_2 = g("homeassistant", "BOILER_2ND_ON_ENTITY_ID", fallback="")
        self.ha_script_1 = g("homeassistant", "RUN_SCRIPT_1ST_START_BOILER_ENTITY_ID", fallback="")
        self.ha_script_2 = g("homeassistant", "RUN_SCRIPT_2ND_START_BOILER_ENTITY_ID", fallback="")
        self.ha_max_per_boiler = int(g("homeassistant", "MAX_DURATION_MINUTES_BOILER", fallback="120"))
        self.ha_retries = int(g("homeassistant", "RETRIES", fallback="3"))
        self.ha_retry_delay = int(g("homeassistant", "RETRY_DELAY", fallback="5"))

        # --- MQTT / Tasmota driver ---
        self.mqtt_host = g("mqtt", "host", fallback="")
        self.mqtt_port = int(g("mqtt", "port", fallback="1883"))
        self.mqtt_user = g("mqtt", "username", fallback="")
        self.mqtt_password = env.get("MQTT_PASSWORD") or g("mqtt", "password", fallback="")
        self.tasmota_topic = g("tasmota", "topic", fallback="")        # the device's %topic%
        # Tasmota's own "Full Topic" field (Configuration > MQTT). Default is Tasmota's default.
        self.tasmota_full_topic = g("tasmota", "full_topic", fallback="%prefix%/%topic%/")
        self.tasmota_relay = int(g("tasmota", "relay", fallback="1"))
        self.tasmota_confirm = _bool(g("tasmota", "confirm", fallback="True"))
        self.tasmota_confirm_timeout = int(g("tasmota", "confirm_timeout", fallback="10"))

        # --- dashboard (read-only web page; shows no secrets) ---
        self.dashboard_enabled = _bool(g("dashboard", "enabled", fallback="True"))
        self.dashboard_host = g("dashboard", "host", fallback="0.0.0.0")   # 127.0.0.1 = this machine only
        self.dashboard_port = int(g("dashboard", "port", fallback="8080"))
        # Manual buttons (Run/Stop) only work when a password is set. Keep it in config.local.ini or env.
        self.dashboard_password = env.get("DASHBOARD_PASSWORD") or g("dashboard", "password", fallback="")
        self.max_manual_minutes = int(g("dashboard", "max_manual_minutes", fallback="180"))

        # --- paths ---
        self.state_file = os.path.join(BASE_DIR, "state.json")
        self.stored_dir = os.path.join(BASE_DIR, "stored_data")
        self.decisions_file = os.path.join(self.stored_dir, "decisions.csv")
        self.activity_file = os.path.join(self.stored_dir, "activity.jsonl")
        self.schedule_file = os.path.join(self.stored_dir, "schedule.json")        # ready-by + presets
        self.runs_file = os.path.join(self.stored_dir, "runs.jsonl")               # minutes really run
        self.formula_file = os.path.join(self.stored_dir, "formula.json")          # tuned on /tune
        self.formula_history = os.path.join(self.stored_dir, "formula_history.jsonl")
        self.sunshine_file = os.path.join(self.stored_dir, "sunshine.csv")         # real sun (archive)
        self.archive_url = g("weather", "archive_url", fallback="https://archive-api.open-meteo.com/v1/archive")

    @property
    def needs_setup(self):
        """True until there is a location and at least one driver (the first-run wizard does both)."""
        return self.latitude is None or self.longitude is None or not self.driver_names
