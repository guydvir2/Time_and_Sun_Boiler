"""
Boiler Control System - Main Entry Point
Modular architecture with separate services
"""

import sys
import logging
import logging.handlers
import tkinter as tk

from utils import init_timezone
from utils import now_local
from data_directory import DataDirectoryManager
from config_loader import ConfigLoader
from weather_service import WeatherService
from ha_service import HAService
from data_manager import DataManager
from scheduler import Scheduler



# ==========================================================
# DATA DIRECTORY SETUP
# ==========================================================
# Create data/ directory structure
DataDirectoryManager.setup()
DataDirectoryManager.migrate_old_files()


# ==========================================================
# LOGGING - Using data/logs/ directory
# ==========================================================
_file_handler = logging.handlers.RotatingFileHandler(
    DataDirectoryManager.get_log_path(),
    maxBytes=2_000_000,
    backupCount=5
)
_file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

_console_handler = logging.StreamHandler(sys.stdout)
_console_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

logging.basicConfig(level=logging.INFO, handlers=[_file_handler, _console_handler])
log = logging.getLogger(__name__)

# ==========================================================
# ENTRY POINT
# ==========================================================
if __name__ == "__main__":
    log.info("=== Boiler Control System Starting ===")
    
    # Load configuration
    config = ConfigLoader()
    config.load()

    init_timezone(config.timezone)  # "Asia/Jerusalem"
    t = now_local()

    # Initialize services
    weather_service = WeatherService(
        weather_url=config.weather_url,
        lat=config.lat,
        lon=config.lon,
        temp_lut=config.temp_lut,
        cloud_penalty_factor=config.cloud_penalty_factor,
        max_first_run=config.max_first_run
    )
    
    ha_service = HAService(
        ha_url=config.ha_url,
        headers=config.ha_headers,
        boiler_1st_entity=config.boiler_1st_on_entity_id,
        boiler_2nd_entity=config.boiler_2nd_on_entity_id,
        script_entity=config.run_script_1st_start_entity_id
    )
    
    data_manager = DataManager()
    
    scheduler = Scheduler(
        weather_service=weather_service,
        ha_service=ha_service,
        data_manager=data_manager,
        target_time_str=config.first_run_target_time
    )
    
    # Import and create GUI
    from gui.main_window import BoilerApp
    
    root = tk.Tk()
    app = BoilerApp(
        root=root,
        config=config,
        weather_service=weather_service,
        ha_service=ha_service,
        data_manager=data_manager,
        scheduler=scheduler
    )
    
    scheduler.on_state_change = app.on_scheduler_state
    scheduler.start()
    
    log.info("=== System Ready ===")
    root.mainloop()
    
    scheduler.stop()
    log.info("=== System Shutdown ===")
