"""
Boiler Control System — entry point
"""
import logging
import logging.handlers
import sys
import tkinter as tk

from data_directory import DataDirectoryManager
from utils import init_timezone

DataDirectoryManager.setup()
DataDirectoryManager.migrate_old_files()

_fh = logging.handlers.RotatingFileHandler(
    DataDirectoryManager.get_log_path(), maxBytes=2_000_000, backupCount=5)
_fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
_ch = logging.StreamHandler(sys.stdout)
_ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logging.basicConfig(level=logging.INFO, handlers=[_fh, _ch])
log = logging.getLogger(__name__)

if __name__ == "__main__":
    log.info("=== Boiler Control System Starting ===")

    from config_loader import ConfigLoader
    cfg = ConfigLoader()
    cfg.load()
    init_timezone(cfg.timezone)

    from weather_service import WeatherService
    from ha_service import HAService
    from mqtt_service import MQTTService
    from data_manager import DataManager
    from scheduler import Scheduler

    weather = WeatherService(
        weather_url=cfg.weather_url, lat=cfg.lat, lon=cfg.lon,
        temp_lut=cfg.temp_lut,
        cloud_penalty_factor=cfg.cloud_penalty_factor,
        max_first_run=cfg.max_first_run,
    )
    ha = HAService(
        ha_url=cfg.ha_url, headers=cfg.ha_headers,
        boiler_1st_entity=cfg.boiler_1st_on_entity_id,
        script_1st_entity=cfg.run_script_1st_entity_id,
        script_2nd_entity=cfg.run_script_2nd_entity_id,
    )
    mqtt = MQTTService(
        broker_ip=cfg.mqtt_broker_ip or "localhost",
        broker_port=cfg.mqtt_broker_port,
        tasmota_topic=cfg.mqtt_tasmota_topic,
        username=cfg.mqtt_username,
        password=cfg.mqtt_password,
        topic_format=cfg.mqtt_topic_format,
    ) if cfg.mqtt_broker_ip else None
    if mqtt:
        mqtt.cmd_enabled = cfg.mqtt_cmd_enabled
        mqtt.cmd_adhoc   = cfg.mqtt_cmd_adhoc
        mqtt.cmd_oneshot = cfg.mqtt_cmd_oneshot
        mqtt.cmd_weekly  = cfg.mqtt_cmd_weekly

    rs  = cfg.runtime_settings
    dm  = DataManager()
    sch = Scheduler(weather_service=weather, ha_service=ha,
                    data_manager=dm, runtime_settings=rs, mqtt_service=mqtt)

    from gui.main_window import BoilerApp
    root = tk.Tk()
    app  = BoilerApp(root=root, config=cfg, weather_service=weather,
                     ha_service=ha, data_manager=dm,
                     scheduler=sch, mqtt_service=mqtt)

    # Wire HA reachability → status bar
    # NOTE: mqtt callbacks (on_connect_change, on_status_change, on_tele) are
    # owned by app._wire_mqtt_callbacks() — do NOT reassign here.
    ha.on_reachability_change = lambda ok: root.after(0, lambda: app.notify_ha_state(ok))
    if mqtt:
        mqtt.active = rs.get_mqtt_active()
        if rs.get_execution_mode() == "MQTT":
            mqtt.active = True
            rs.set_mqtt_active(True)

    sch.on_state_change = app.on_scheduler_state
    sch.start()

    log.info("=== System Ready ===")
    root.mainloop()
    sch.stop()
    log.info("=== System Shutdown ===")
