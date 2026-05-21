"""
config.py — all configuration in one place.
  • DataDirectoryManager  — directory/file paths
  • RuntimeSettings       — user-editable JSON settings
  • AppConfig             — loads config.ini, wires everything together
"""
import configparser
import json
import logging
import os
import shutil
from datetime import datetime
from typing import Any, Dict, Optional

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# Directory / path management
# ─────────────────────────────────────────────────────────────

class DataDirectoryManager:
    DATA_DIR   = "data"
    CONFIG_DIR = os.path.join(DATA_DIR, "config")
    LOGS_DIR   = os.path.join(DATA_DIR, "logs")
    CSV_DIR    = os.path.join(DATA_DIR, "csv")

    CONFIG_INI       = os.path.join(CONFIG_DIR, "config.ini")
    RUNTIME_SETTINGS = os.path.join(CONFIG_DIR, "runtime_settings.json")
    BOILER_LOG       = os.path.join(LOGS_DIR,   "boiler.log")
    DAILY_CSV        = os.path.join(CSV_DIR,    "daily_log.csv")

    @classmethod
    def setup(cls):
        os.makedirs(cls.CONFIG_DIR, exist_ok=True)
        os.makedirs(cls.LOGS_DIR,   exist_ok=True)
        os.makedirs(cls.CSV_DIR,    exist_ok=True)

    @classmethod
    def migrate_old_files(cls):
        migrations = [
            ("config.ini",            cls.CONFIG_INI),
            ("runtime_settings.json", cls.RUNTIME_SETTINGS),
            ("boiler.log",            cls.BOILER_LOG),
            ("daily_log.csv",         cls.DAILY_CSV),
        ]
        for old, new in migrations:
            if os.path.exists(old) and not os.path.exists(new):
                shutil.move(old, new)
                log.info(f"Migrated: {old} → {new}")
        for i in range(1, 6):
            old = f"boiler.log.{i}"
            if os.path.exists(old):
                new = os.path.join(cls.LOGS_DIR, f"boiler.log.{i}")
                if not os.path.exists(new):
                    shutil.move(old, new)

    @classmethod
    def get_config_path(cls):        return cls.CONFIG_INI
    @classmethod
    def get_runtime_settings_path(cls): return cls.RUNTIME_SETTINGS
    @classmethod
    def get_log_path(cls):           return cls.BOILER_LOG
    @classmethod
    def get_csv_path(cls):           return cls.DAILY_CSV


# ─────────────────────────────────────────────────────────────
# Runtime settings (user-editable, persisted to JSON)
# ─────────────────────────────────────────────────────────────

class RuntimeSettings:
    def __init__(self, config_defaults: Dict[str, Any],
                 settings_file: Optional[str] = None):
        self.SETTINGS_FILE = settings_file or DataDirectoryManager.get_runtime_settings_path()
        self._defaults     = config_defaults
        self.settings      = self._load()

    # ── Persistence ──────────────────────────────────────────

    def _load(self) -> Dict[str, Any]:
        if os.path.exists(self.SETTINGS_FILE):
            try:
                with open(self.SETTINGS_FILE) as f:
                    data = json.load(f)
                log.info(f"Loaded runtime settings from {self.SETTINGS_FILE}")
                return data
            except Exception as e:
                log.warning(f"Could not load {self.SETTINGS_FILE}: {e} — using defaults")
        return {
            "first_run_target_time": "18:45",
            "cloud_penalty_factor":  self._defaults["cloud_penalty_factor"],
            "temp_lut":              self._defaults["temp_lut"],
        }

    def save(self) -> bool:
        self.settings["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            with open(self.SETTINGS_FILE, "w") as f:
                json.dump(self.settings, f, indent=2)
            return True
        except IOError as e:
            log.error(f"Failed to save runtime settings: {e}")
            return False

    # ── Helpers ──────────────────────────────────────────────

    def _get(self, key, default):       return self.settings.get(key, default)
    def _set(self, key, value) -> bool:
        self.settings[key] = value
        return self.save()

    @staticmethod
    def _valid_time(t: str) -> bool:
        try:
            h, m = t.split(":")
            return 0 <= int(h) <= 23 and 0 <= int(m) <= 59
        except Exception:
            return False

    # ── Solar ────────────────────────────────────────────────

    def get_target_time(self) -> str:      return self._get("first_run_target_time", "18:45")
    def get_second_run_time(self) -> str:  return self._get("second_run_target_time", "20:00")
    def get_solar_active(self) -> bool:    return self._get("solar_active", True)
    def get_cloud_penalty(self) -> float:  return float(self._get("cloud_penalty_factor", 3.0))
    def get_temp_lut(self) -> Dict[int, int]:
        return {int(k): int(v) for k, v in self._get("temp_lut", {}).items()}

    def set_target_time(self, t: str) -> bool:
        return self._set("first_run_target_time", t) if self._valid_time(t) else False
    def set_second_run_time(self, t: str) -> bool:
        return self._set("second_run_target_time", t) if self._valid_time(t) else False
    def set_solar_active(self, v: bool) -> bool:   return self._set("solar_active", v)
    def set_cloud_penalty(self, v: float) -> bool:
        return self._set("cloud_penalty_factor", v) if v > 0 else False
    def set_temp_lut(self, lut: Dict[int, int]) -> bool:
        return self._set("temp_lut", lut) if lut and len(lut) >= 2 else False

    # ── Execution ────────────────────────────────────────────

    def get_execution_mode(self) -> str:   return self._get("execution_mode", "HA")
    def get_mqtt_active(self) -> bool:     return self._get("mqtt_active", False)
    def set_execution_mode(self, m: str) -> bool:
        return self._set("execution_mode", m) if m in ("HA", "MQTT") else False
    def set_mqtt_active(self, v: bool) -> bool: return self._set("mqtt_active", v)

    # ── Weekly ───────────────────────────────────────────────

    def get_weekly_presets(self) -> list:
        return self._get("weekly_presets", [
            {"id": i, "active": False, "start_time": "08:00", "duration": 30, "days": []}
            for i in range(1, 4)
        ])
    def set_weekly_presets(self, presets: list) -> bool:
        return self._set("weekly_presets", presets)

    # ── One-shot ─────────────────────────────────────────────

    def get_one_shot(self) -> dict:
        return self._get("one_shot",
                         {"start_time": "08:00", "duration": 30, "armed": False})
    def set_one_shot(self, start_time: str, duration: int, armed: bool = False) -> bool:
        return self._set("one_shot",
                         {"start_time": start_time, "duration": duration, "armed": armed})

    # ── Vacation ─────────────────────────────────────────────

    def get_vacation_mode(self) -> bool:  return self._get("vacation_mode", False)
    def set_vacation_mode(self, v: bool) -> bool: return self._set("vacation_mode", v)


# ─────────────────────────────────────────────────────────────
# Static config (config.ini)
# ─────────────────────────────────────────────────────────────

_REQUIRED = {
    "location":      ["latitude", "longitude", "timezone"],
    "weather":       ["weather_url"],
    "parameters":    ["max_first_run", "cloud_penalty_factor"],
}

# HA is optional — only validated if the section is present
_HA_REQUIRED = ["HA_IP", "HA_PORT", "token",
                "BOILER_1ST_ON_ENTITY_ID",
                "RUN_SCRIPT_1ST_START_BOILER_ENTITY_ID"]


class AppConfig:
    def __init__(self, config_file=None):
        self.config_file = config_file or DataDirectoryManager.get_config_path()

        # HA
        self.ha_ip = self.ha_port = self.ha_url = self.ha_headers = None
        self.boiler_entity_id = ""
        self.boiler_1st_on_entity_id = None
        self.run_script_1st_entity_id = None

        # Location / weather
        self.lat = self.lon = self.timezone = None
        self.weather_url = None
        self.max_first_run = 120
        self.cloud_penalty_factor = 3.0
        self.first_run_target_time = "18:45"

        # MQTT
        self.mqtt_broker_ip = ""
        self.mqtt_broker_port = 1883
        self.mqtt_username = self.mqtt_password = ""
        self.mqtt_tasmota_topic = "boiler"
        self.mqtt_topic_format = "device_first"
        self.mqtt_cmd_enabled = False
        self.mqtt_cmd_adhoc = self.mqtt_cmd_oneshot = self.mqtt_cmd_weekly = ""

        # HA availability flag (set after load())
        self.ha_configured = False

        # Runtime (populated after load())
        self.runtime_settings: Optional[RuntimeSettings] = None
        self.temp_lut: Dict[int, int] = {}

    # ── Public ───────────────────────────────────────────────

    def load(self):
        self.config = configparser.ConfigParser()
        self.config.read(self.config_file)
        self._check_required()
        if self.config.has_section("homeassistant"):
            self._load_ha()
            self.ha_configured = True
        self._load_location()
        self._load_weather()
        self._load_params()
        self._load_mqtt()
        self._load_runtime()
        log.info("Configuration loaded")

    # ── Private ──────────────────────────────────────────────

    def _check_required(self):
        missing = []
        for section, keys in _REQUIRED.items():
            if not self.config.has_section(section):
                missing.append(f"[{section}]")
                continue
            for k in keys:
                if k not in self.config[section]:
                    missing.append(f"[{section}].{k}")
        # HA is optional but if section is present all its keys must be there
        if self.config.has_section("homeassistant"):
            for k in _HA_REQUIRED:
                if k not in self.config["homeassistant"]:
                    missing.append(f"[homeassistant].{k}")
        if missing:
            raise SystemExit("config.ini missing: " + ", ".join(missing))

    def _load_ha(self):
        ha = self.config["homeassistant"]
        self.ha_ip                    = ha["HA_IP"]
        self.ha_port                  = int(ha["HA_PORT"])
        self.ha_url                   = f"http://{self.ha_ip}:{self.ha_port}"
        self.ha_headers               = {"Authorization": f"Bearer {ha['token']}",
                                         "Content-Type": "application/json"}
        self.boiler_entity_id         = ha.get("BOILER_ENTITY_ID", "")
        self.boiler_1st_on_entity_id  = ha["BOILER_1ST_ON_ENTITY_ID"]
        self.run_script_1st_entity_id = ha["RUN_SCRIPT_1ST_START_BOILER_ENTITY_ID"]

    def _load_location(self):
        loc = self.config["location"]
        self.lat      = float(loc["latitude"])
        self.lon      = float(loc["longitude"])
        self.timezone = loc["timezone"]

    def _load_weather(self):
        self.weather_url = self.config["weather"]["weather_url"]

    def _load_params(self):
        p = self.config["parameters"]
        self.max_first_run        = int(p["max_first_run"])
        self.cloud_penalty_factor = float(p["cloud_penalty_factor"])

    def _load_mqtt(self):
        if not self.config.has_section("mqtt"):
            return
        m = self.config["mqtt"]
        self.mqtt_broker_ip    = m.get("broker_ip",    "")
        self.mqtt_broker_port  = int(m.get("broker_port", 1883))
        self.mqtt_username     = m.get("username",     "")
        self.mqtt_password     = m.get("password",     "")
        self.mqtt_tasmota_topic = m.get("tasmota_topic", "boiler")
        self.mqtt_topic_format  = m.get("topic_format",  "device_first")
        self.mqtt_cmd_enabled   = m.get("cmd_enabled",   "false").lower() == "true"
        self.mqtt_cmd_adhoc     = m.get("cmd_adhoc",     "")
        self.mqtt_cmd_oneshot   = m.get("cmd_oneshot",   "")
        self.mqtt_cmd_weekly    = m.get("cmd_weekly",    "")

    def _load_runtime(self):
        default_lut = {10: 180, 12: 140, 14: 90, 16: 45,
                       18: 30,  20: 15,  22: 5,  24: 0}
        self.runtime_settings = RuntimeSettings({
            "temp_lut":              default_lut,
            "cloud_penalty_factor":  self.cloud_penalty_factor,
        })
        self.temp_lut              = self.runtime_settings.get_temp_lut()
        self.cloud_penalty_factor  = self.runtime_settings.get_cloud_penalty()
        self.first_run_target_time = self.runtime_settings.get_target_time()
        if len(self.temp_lut) < 2:
            raise SystemExit("TEMP_LUT must have at least 2 points")
        log.info(f"Runtime: target={self.first_run_target_time}, "
                 f"mode={self.runtime_settings.get_execution_mode()}, "
                 f"lut={len(self.temp_lut)}pts")
