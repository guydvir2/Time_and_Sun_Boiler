"""
Config Loader
Reads and validates config.ini, loads runtime settings
"""

import os
import json
import logging
import configparser
from runtime_settings import RuntimeSettings

# ==========================================================
# Files names
# ==========================================================
CONFIG_INI="config.ini"



log = logging.getLogger(__name__)


class ConfigLoader:
    """Loads and validates configuration from config.ini and runtime_settings.json"""
    
    REQUIRED_CONFIG = {
        "homeassistant": [
            "HA_IP", "HA_PORT", "token",
            "BOILER_1ST_ON_ENTITY_ID",
            "BOILER_2ND_ON_ENTITY_ID",
            "RUN_SCRIPT_1ST_START_BOILER_ENTITY_ID"
        ],
        "location": ["latitude", "longitude"],
        "weather": ["weather_url"],
        "parameters": [
            "max_first_run", "sunset_offset_minutes",
            "init_hour", "poll_interval_minutes",
            "cloud_penalty_factor"
        ],
        "temperature_duration_lut": ["temp_lut"]
    }
    
    def __init__(self, config_file=CONFIG_INI):
        self.config_file = config_file
        self.config = None
        self.runtime_settings = None
        
        # Loaded values
        self.ha_ip = None
        self.ha_port = None
        self.ha_token = None
        self.boiler_1st_on_entity_id = None
        self.boiler_2nd_on_entity_id = None
        self.run_script_1st_start_entity_id = None
        
        self.lat = None
        self.lon = None
        self.weather_url = None
        
        self.max_first_run = None
        self.sunset_offset_minutes = None
        self.init_hour = None
        self.poll_interval_minutes = None
        
        # These can be overridden by runtime_settings
        self.cloud_penalty_factor = None
        self.temp_lut = None
        self.first_run_target_time = None
        
        self.ha_url = None
        self.ha_headers = None
    
    def load(self):
        """Load configuration from config.ini and runtime_settings.json"""
        self._load_config_ini()
        self._load_runtime_settings()
        self._build_ha_connection()
        log.info("Configuration loaded successfully")
    
    def _load_config_ini(self):
        """Load and validate config.ini"""
        self.config = configparser.ConfigParser()
        self.config.read(self.config_file)
        
        # Validate required fields
        missing = []
        for section, keys in self.REQUIRED_CONFIG.items():
            for key in keys:
                if not self.config.has_option(section, key):
                    missing.append(f"[{section}] → {key}")
        
        if missing:
            raise SystemExit(
                f"config.ini is missing required entries:\n  " + "\n  ".join(missing)
            )
        
        # Load Home Assistant config
        self.ha_ip = self.config["homeassistant"]["HA_IP"]
        self.ha_port = self.config["homeassistant"]["HA_PORT"]
        self.ha_token = os.environ.get("HA_TOKEN") or self.config["homeassistant"]["token"]
        
        if not self.ha_token:
            raise SystemExit("HA token not found. Set HA_TOKEN env var or add to config.ini.")
        
        self.boiler_1st_on_entity_id = self.config["homeassistant"]["BOILER_1ST_ON_ENTITY_ID"]
        self.boiler_2nd_on_entity_id = self.config["homeassistant"]["BOILER_2ND_ON_ENTITY_ID"]
        self.run_script_1st_start_entity_id = self.config["homeassistant"]["RUN_SCRIPT_1ST_START_BOILER_ENTITY_ID"]
        
        # Load location
        self.lat = float(self.config["location"]["latitude"])
        self.lon = float(self.config["location"]["longitude"])
        
        # Load weather
        self.weather_url = self.config["weather"]["weather_url"]
        
        # Load parameters
        self.max_first_run = int(self.config["parameters"]["max_first_run"])
        self.sunset_offset_minutes = int(self.config["parameters"]["sunset_offset_minutes"])
        self.init_hour = int(self.config["parameters"]["init_hour"])
        self.poll_interval_minutes = int(self.config["parameters"]["poll_interval_minutes"])
        self.cloud_penalty_factor = float(self.config["parameters"]["cloud_penalty_factor"])
        
        # Load LUT
        self.temp_lut = {
            int(k): int(v) for k, v in json.loads(
                self.config["temperature_duration_lut"]["temp_lut"]
            ).items()
        }
        
        if not self.temp_lut:
            raise SystemExit("TEMP_LUT is empty. Check [temperature_duration_lut] in config.ini.")
    
    def _load_runtime_settings(self):
        """Load runtime settings (overrides config.ini defaults)"""
        self.runtime_settings = RuntimeSettings({
            "temp_lut": self.temp_lut,
            "cloud_penalty_factor": self.cloud_penalty_factor
        })
        
        # Override with runtime settings
        self.temp_lut = self.runtime_settings.get_temp_lut()
        self.cloud_penalty_factor = self.runtime_settings.get_cloud_penalty()
        self.first_run_target_time = self.runtime_settings.get_target_time()
        
        log.info(f"Runtime overrides: target={self.first_run_target_time}, "
                f"cloud_penalty={self.cloud_penalty_factor}")
    
    def _build_ha_connection(self):
        """Build HA URL and headers"""
        self.ha_url = f"http://{self.ha_ip}:{self.ha_port}"
        self.ha_headers = {
            "Authorization": f"Bearer {self.ha_token}",
            "Content-Type": "application/json"
        }
