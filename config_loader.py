"""
Config Loader — reads config.ini and runtime_settings.json.
Static config (config.ini) and user settings (runtime_settings.json) are
kept separate; runtime_settings is the single source of truth for
everything the user can change in the GUI.
"""
import logging
import os
import configparser
from runtime_settings import RuntimeSettings

log = logging.getLogger(__name__)

_REQUIRED = {
    "homeassistant": ["HA_IP", "HA_PORT", "token",
                      "BOILER_1ST_ON_ENTITY_ID",
                      "RUN_SCRIPT_1ST_START_BOILER_ENTITY_ID",
                      "RUN_SCRIPT_2ND_START_BOILER_ENTITY_ID"],
    "location":      ["latitude", "longitude", "timezone"],
    "weather":       ["weather_url"],
    "parameters":    ["max_first_run", "sunset_offset_minutes",
                      "init_hour", "poll_interval_minutes",
                      "cloud_penalty_factor"],
}


class ConfigLoader:
    def __init__(self, config_file=None):
        if config_file is None:
            from data_directory import DataDirectoryManager
            config_file = DataDirectoryManager.get_config_path()
        self.config_file = config_file
        self.config: configparser.ConfigParser = None

        # HA
        self.ha_ip = self.ha_port = self.ha_token = None
        self.boiler_1st_on_entity_id = None
        self.run_script_1st_entity_id = self.run_script_2nd_entity_id = None
        self.ha_url = self.ha_headers = None

        # Location / weather
        self.lat = self.lon = self.timezone = self.weather_url = None

        # Parameters
        self.max_first_run = self.sunset_offset_minutes = None
        self.init_hour = self.poll_interval_minutes = None
        self.cloud_penalty_factor = None

        # MQTT (optional section)
        self.mqtt_broker_ip    = None
        self.mqtt_broker_port  = 1883
        self.mqtt_username     = ""
        self.mqtt_password     = ""
        self.mqtt_tasmota_topic = "boiler"
        self.mqtt_topic_format  = "device_first"

        # Populated from RuntimeSettings after load()
        self.temp_lut              = None
        self.first_run_target_time = None
        self.runtime_settings: RuntimeSettings = None

    def load(self):
        self._load_ini()
        self._load_runtime()
        self._build_ha()
        log.info("Configuration loaded")

    def _load_ini(self):
        self.config = configparser.ConfigParser()
        self.config.read(self.config_file)

        missing = [f"[{s}] {k}" for s, keys in _REQUIRED.items()
                   for k in keys if not self.config.has_option(s, k)]
        if missing:
            raise SystemExit("config.ini missing: " + ", ".join(missing))

        ha = self.config["homeassistant"]
        self.ha_ip    = ha["HA_IP"]
        self.ha_port  = ha["HA_PORT"]
        self.ha_token = os.environ.get("HA_TOKEN") or ha["token"]
        if not self.ha_token:
            raise SystemExit("HA token not found")

        self.boiler_1st_on_entity_id    = ha["BOILER_1ST_ON_ENTITY_ID"]
        self.run_script_1st_entity_id   = ha["RUN_SCRIPT_1ST_START_BOILER_ENTITY_ID"]
        self.run_script_2nd_entity_id   = ha["RUN_SCRIPT_2ND_START_BOILER_ENTITY_ID"]

        loc = self.config["location"]
        self.lat      = float(loc["latitude"])
        self.lon      = float(loc["longitude"])
        self.timezone = loc["timezone"]

        self.weather_url = self.config["weather"]["weather_url"]

        p = self.config["parameters"]
        self.max_first_run          = int(p["max_first_run"])
        self.sunset_offset_minutes  = int(p["sunset_offset_minutes"])
        self.init_hour              = int(p["init_hour"])
        self.poll_interval_minutes  = int(p["poll_interval_minutes"])
        self.cloud_penalty_factor   = float(p["cloud_penalty_factor"])

        if self.config.has_section("mqtt"):
            m = self.config["mqtt"]
            self.mqtt_broker_ip     = m.get("broker_ip", "")
            self.mqtt_broker_port   = int(m.get("broker_port", 1883))
            self.mqtt_username      = m.get("username", "")
            self.mqtt_password      = m.get("password", "")
            self.mqtt_tasmota_topic = m.get("tasmota_topic", "boiler")
            self.mqtt_topic_format  = m.get("topic_format", "device_first")

    def _load_runtime(self):
        default_lut = {10: 180, 12: 140, 14: 90, 16: 45, 18: 30, 20: 15, 22: 5, 24: 0}
        self.runtime_settings = RuntimeSettings({
            "temp_lut": default_lut,
            "cloud_penalty_factor": self.cloud_penalty_factor,
        })
        self.temp_lut              = self.runtime_settings.get_temp_lut()
        self.cloud_penalty_factor  = self.runtime_settings.get_cloud_penalty()
        self.first_run_target_time = self.runtime_settings.get_target_time()
        if len(self.temp_lut) < 2:
            raise SystemExit("TEMP_LUT must have at least 2 points")
        log.info(f"Runtime: target={self.first_run_target_time}, "
                 f"mode={self.runtime_settings.get_execution_mode()}, "
                 f"lut={len(self.temp_lut)}pts")

    def _build_ha(self):
        self.ha_url = f"http://{self.ha_ip}:{self.ha_port}"
        self.ha_headers = {
            "Authorization": f"Bearer {self.ha_token}",
            "Content-Type": "application/json",
        }
