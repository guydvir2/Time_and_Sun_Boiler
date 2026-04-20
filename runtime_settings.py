"""
Runtime Settings — user-editable overrides persisted to runtime_settings.json.
Never modifies config.ini.
"""
import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, Optional

log = logging.getLogger(__name__)


class RuntimeSettings:
    def __init__(self, config_defaults: Dict[str, Any], settings_file: Optional[str] = None):
        if settings_file is None:
            try:
                from data_directory import DataDirectoryManager
                settings_file = DataDirectoryManager.get_runtime_settings_path()
            except ImportError:
                settings_file = "runtime_settings.json"
        self.SETTINGS_FILE = settings_file
        self._defaults = config_defaults
        self.settings  = self._load()

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

    # ── Accessors ────────────────────────────────────────────

    def _get(self, key, default):
        return self.settings.get(key, default)

    def _set(self, key, value) -> bool:
        self.settings[key] = value
        return self.save()

    # Solar
    def get_target_time(self) -> str:          return self._get("first_run_target_time", "18:45")
    def get_second_run_time(self) -> str:      return self._get("second_run_target_time", "20:00")
    def get_solar_active(self) -> bool:        return self._get("solar_active", True)
    def get_cloud_penalty(self) -> float:      return float(self._get("cloud_penalty_factor", 3.0))
    def get_temp_lut(self) -> Dict[int, int]:
        return {int(k): int(v) for k, v in self._get("temp_lut", {}).items()}

    def set_target_time(self, t: str) -> bool:
        return self._set("first_run_target_time", t) if self._valid_time(t) else False
    def set_second_run_time(self, t: str) -> bool:
        return self._set("second_run_target_time", t) if self._valid_time(t) else False
    def set_solar_active(self, v: bool) -> bool:  return self._set("solar_active", v)
    def set_cloud_penalty(self, v: float) -> bool:
        return self._set("cloud_penalty_factor", v) if v > 0 else False
    def set_temp_lut(self, lut: Dict[int, int]) -> bool:
        if not lut or len(lut) < 2:
            return False
        return self._set("temp_lut", lut)

    # Execution
    def get_execution_mode(self) -> str:   return self._get("execution_mode", "HA")
    def get_mqtt_active(self) -> bool:     return self._get("mqtt_active", False)
    def set_execution_mode(self, m: str) -> bool:
        return self._set("execution_mode", m) if m in ("HA", "MQTT") else False
    def set_mqtt_active(self, v: bool) -> bool: return self._set("mqtt_active", v)

    # Weekly
    def get_weekly_presets(self) -> list:
        return self._get("weekly_presets", [
            {"id": i, "active": False, "start_time": "08:00", "duration": 30, "days": []}
            for i in range(1, 4)
        ])
    def set_weekly_presets(self, presets: list) -> bool:
        return self._set("weekly_presets", presets)

    # One-shot
    def get_one_shot(self) -> dict:
        return self._get("one_shot", {"start_time": "08:00", "duration": 30, "armed": False})
    def set_one_shot(self, start_time: str, duration: int, armed: bool = False) -> bool:
        return self._set("one_shot", {"start_time": start_time, "duration": duration, "armed": armed})

    # ── Validation ───────────────────────────────────────────

    @staticmethod
    def _valid_time(t: str) -> bool:
        try:
            h, m = t.split(":")
            return 0 <= int(h) <= 23 and 0 <= int(m) <= 59
        except Exception:
            return False
