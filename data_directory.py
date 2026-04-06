"""
Data Directory Manager
Organizes all data and config files into data/ folder
"""

import os
import shutil
import logging

log = logging.getLogger(__name__)


class DataDirectoryManager:
    """
    Manages the data/ directory structure:
    
    data/
    ├── config/
    │   ├── config.ini
    │   └── runtime_settings.json
    ├── logs/
    │   └── boiler.log (+ rotated logs)
    └── csv/
        └── daily_log.csv
    """
    
    DATA_DIR = "data"
    CONFIG_DIR = os.path.join(DATA_DIR, "config")
    LOGS_DIR = os.path.join(DATA_DIR, "logs")
    CSV_DIR = os.path.join(DATA_DIR, "csv")
    
    # File paths
    CONFIG_INI = os.path.join(CONFIG_DIR, "config.ini")
    RUNTIME_SETTINGS = os.path.join(CONFIG_DIR, "runtime_settings.json")
    BOILER_LOG = os.path.join(LOGS_DIR, "boiler.log")
    DAILY_CSV = os.path.join(CSV_DIR, "daily_log.csv")
    
    @classmethod
    def setup(cls):
        """Create directory structure"""
        os.makedirs(cls.CONFIG_DIR, exist_ok=True)
        os.makedirs(cls.LOGS_DIR, exist_ok=True)
        os.makedirs(cls.CSV_DIR, exist_ok=True)
        log.info(f"Data directory structure created: {cls.DATA_DIR}/")
    
    @classmethod
    def migrate_old_files(cls):
        """
        Migrate files from root to data/ structure (one-time migration)
        """
        migrations = [
            ("config.ini", cls.CONFIG_INI),
            ("runtime_settings.json", cls.RUNTIME_SETTINGS),
            ("boiler.log", cls.BOILER_LOG),
            ("daily_log.csv", cls.DAILY_CSV)
        ]
        
        for old_file, new_file in migrations:
            if os.path.exists(old_file) and not os.path.exists(new_file):
                shutil.move(old_file, new_file)
                log.info(f"Migrated: {old_file} → {new_file}")
        
        # Migrate rotated log files
        for i in range(1, 6):
            old_log = f"boiler.log.{i}"
            if os.path.exists(old_log):
                new_log = os.path.join(cls.LOGS_DIR, f"boiler.log.{i}")
                if not os.path.exists(new_log):
                    shutil.move(old_log, new_log)
                    log.info(f"Migrated: {old_log} → {new_log}")
    
    @classmethod
    def get_config_path(cls):
        """Get path to config.ini"""
        return cls.CONFIG_INI
    
    @classmethod
    def get_runtime_settings_path(cls):
        """Get path to runtime_settings.json"""
        return cls.RUNTIME_SETTINGS
    
    @classmethod
    def get_log_path(cls):
        """Get path to boiler.log"""
        return cls.BOILER_LOG
    
    @classmethod
    def get_csv_path(cls):
        """Get path to daily_log.csv"""
        return cls.DAILY_CSV
