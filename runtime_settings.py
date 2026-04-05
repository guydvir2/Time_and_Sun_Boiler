"""
Runtime Settings Manager
Handles user customizations that override config.ini defaults.
Saves to runtime_settings.json without modifying config.ini.
"""

import json
import os
import logging
from datetime import datetime
from typing import Dict, Any, Optional

SETTING_FILE_JSON = "runtime_settings.json"
log = logging.getLogger(__name__)


class RuntimeSettings:
    """
    Manages runtime overrides for:
    - first_run_target_time (when first run should complete)
    - cloud_penalty_factor
    - temp_lut (temperature → duration lookup table)
    """
    
    SETTINGS_FILE = SETTING_FILE_JSON
    
    def __init__(self, config_defaults: Dict[str, Any]):
        """
        Args:
            config_defaults: Default values from config.ini containing:
                - temp_lut: dict[int, int]
                - cloud_penalty_factor: float
        """
        self.config_defaults = config_defaults
        self.settings = self._load_or_create()
    
    def _load_or_create(self) -> Dict[str, Any]:
        """Load from JSON file, or create with defaults if doesn't exist."""
        if os.path.exists(self.SETTINGS_FILE):
            try:
                with open(self.SETTINGS_FILE, 'r') as f:
                    data = json.load(f)
                    log.info(f"Loaded runtime settings from {self.SETTINGS_FILE}")
                    return data
            except (json.JSONDecodeError, IOError) as e:
                log.warning(f"Could not load {self.SETTINGS_FILE}: {e}. Using defaults.")
        
        # Create defaults
        defaults = {
            "first_run_target_time": "18:45",  # Default target ready time
            "cloud_penalty_factor": self.config_defaults["cloud_penalty_factor"],
            "temp_lut": self.config_defaults["temp_lut"],
            "last_updated": None
        }
        return defaults
    
    def save(self) -> bool:
        """Save current settings to JSON file."""
        self.settings["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            with open(self.SETTINGS_FILE, 'w') as f:
                json.dump(self.settings, f, indent=2)
            log.info(f"Saved runtime settings to {self.SETTINGS_FILE}")
            return True
        except IOError as e:
            log.error(f"Failed to save runtime settings: {e}")
            return False
    
    def get_target_time(self) -> str:
        """Get first run target ready time (HH:MM format)."""
        return self.settings.get("first_run_target_time", "18:45")
    
    def set_target_time(self, time_str: str) -> bool:
        """
        Set first run target ready time.
        Args:
            time_str: Time in HH:MM format (e.g., "18:45")
        Returns:
            True if valid and saved, False otherwise
        """
        if not self._validate_time_format(time_str):
            log.error(f"Invalid time format: {time_str}. Expected HH:MM")
            return False
        
        self.settings["first_run_target_time"] = time_str
        return self.save()
    
    def get_cloud_penalty(self) -> float:
        """Get cloud penalty factor."""
        return float(self.settings.get("cloud_penalty_factor", 3.0))
    
    def set_cloud_penalty(self, factor: float) -> bool:
        """
        Set cloud penalty factor.
        Args:
            factor: Positive float value
        Returns:
            True if valid and saved, False otherwise
        """
        if factor <= 0:
            log.error(f"Cloud penalty must be positive, got: {factor}")
            return False
        
        self.settings["cloud_penalty_factor"] = factor
        return self.save()
    
    def get_temp_lut(self) -> Dict[int, int]:
        """Get temperature LUT. Returns dict with int keys and values."""
        lut = self.settings.get("temp_lut", {})
        # Ensure keys are integers (JSON serializes dict keys as strings)
        return {int(k): int(v) for k, v in lut.items()}
    
    def set_temp_lut(self, lut: Dict[int, int]) -> bool:
        """
        Set temperature LUT.
        Args:
            lut: Dict mapping temperature (int) to duration (int)
        Returns:
            True if valid and saved, False otherwise
        """
        if not self._validate_lut(lut):
            return False
        
        self.settings["temp_lut"] = lut
        return self.save()
    
    @staticmethod
    def _validate_time_format(time_str: str) -> bool:
        """Validate HH:MM format (00:00 to 23:59)."""
        try:
            parts = time_str.split(":")
            if len(parts) != 2:
                return False
            hour, minute = int(parts[0]), int(parts[1])
            return 0 <= hour <= 23 and 0 <= minute <= 59
        except (ValueError, AttributeError):
            return False
    
    @staticmethod
    def _validate_lut(lut: Dict[int, int]) -> bool:
        """Validate LUT: keys and values must be positive integers."""
        if not lut:
            log.error("LUT cannot be empty")
            return False
        
        try:
            for temp, duration in lut.items():
                if not isinstance(temp, int) or not isinstance(duration, int):
                    log.error(f"LUT entries must be integers: {temp} -> {duration}")
                    return False
                if duration <= 0:
                    log.error(f"Duration must be positive: {temp} -> {duration}")
                    return False
            return True
        except (TypeError, AttributeError) as e:
            log.error(f"Invalid LUT structure: {e}")
            return False
