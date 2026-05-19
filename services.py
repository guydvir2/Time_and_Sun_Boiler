"""
services.py — external service integrations.
  • HAService   — Home Assistant REST API
  • MQTTService — Tasmota MQTT broker
"""
import logging
import threading
import time
from typing import Callable, List, Optional

import paho.mqtt.client as mqtt
import requests

from utils import with_retries

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# Home Assistant
# ─────────────────────────────────────────────────────────────

class HAService:
    def __init__(self, ha_url: str, headers: dict,
                 boiler_1st_entity: str, script_1st_entity: str):
        self.ha_url               = ha_url
        self.headers              = headers
        self.boiler_1st_on_entity_id  = boiler_1st_entity
        self.run_script_1st_entity_id = script_1st_entity

        # Callback fired after every operation: fn(ok: bool)
        self.on_reachability_change: Optional[Callable[[bool], None]] = None

    # ── Public ───────────────────────────────────────────────

    def check_reachable(self):
        r = requests.get(f"{self.ha_url}/api/", headers=self.headers, timeout=5)
        r.raise_for_status()

    def send_run(self, duration: int, run_number: int = 1) -> bool:
        """Set slider duration then trigger the boiler script.
        run_number is 1 or 2 — same entities used, kept for logging."""
        label = "1st run" if run_number == 1 else "2nd run"
        log.info(f"HA {label}: {duration}min")
        try:
            with_retries(self.check_reachable,
                         label="HA ping")
            with_retries(lambda: self._set_slider(self.boiler_1st_on_entity_id, duration),
                         label="slider")
            with_retries(lambda: self._run_script(self.run_script_1st_entity_id),
                         label=f"script-{run_number}")
            log.info(f"HA {label} complete")
            self._notify(True)
            return True
        except Exception as e:
            log.error(f"HA {label} failed: {e}")
            self._notify(False)
            return False

    # Legacy wrapper — keep so data_manager.retry_ha still works
    def send_first_run(self, duration: int) -> bool:
        return self.send_run(duration, run_number=1)

    # ── Private ──────────────────────────────────────────────

    def _set_slider(self, entity_id: str, value: int):
        r = requests.post(
            f"{self.ha_url}/api/services/input_number/set_value",
            headers=self.headers,
            json={"entity_id": entity_id, "value": value},
            timeout=10)
        r.raise_for_status()
        log.info(f"Set {entity_id} = {value} min")

    def _run_script(self, script_name: str):
        r = requests.post(
            f"{self.ha_url}/api/services/script/turn_on",
            headers=self.headers,
            json={"entity_id": f"script.{script_name}"},
            timeout=10)
        r.raise_for_status()
        log.info(f"Triggered script.{script_name}")

    def _notify(self, ok: bool):
        if self.on_reachability_change:
            self.on_reachability_change(ok)


# ─────────────────────────────────────────────────────────────
# MQTT / Tasmota
# ─────────────────────────────────────────────────────────────

class MQTTService:
    def __init__(self, broker_ip: str, broker_port: int,
                 tasmota_topic: str,
                 username: str = "", password: str = "",
                 topic_format: str = "device_first",
                 cmd_enabled: bool = False,
                 cmd_adhoc: str = "", cmd_oneshot: str = "", cmd_weekly: str = ""):

        self.broker_ip    = broker_ip
        self.broker_port  = broker_port
        self.tasmota_topic = tasmota_topic
        self.username     = username
        self.password     = password
        self.topic_format = topic_format

        # Command listener topics (set before connect)
        self.cmd_enabled  = cmd_enabled
        self.cmd_adhoc    = cmd_adhoc
        self.cmd_oneshot  = cmd_oneshot
        self.cmd_weekly   = cmd_weekly

        # Callbacks — assigned by main_window after build
        self.on_status_change:  Optional[Callable[[str],        None]] = None
        self.on_connect_change: Optional[Callable[[bool],       None]] = None
        self.on_tele:           Optional[Callable[[str, str],   None]] = None
        self.on_command:        Optional[Callable[[str, str],   None]] = None

        self.active: bool = False

        self._client:       Optional[mqtt.Client] = None
        self._connected     = False
        self._last_status:  Optional[str]         = None
        self._off_timer:    Optional[threading.Timer] = None
        self._tele_log:     List[tuple]            = []

        self._build_topics()

    # ── Topic layout ─────────────────────────────────────────

    def _build_topics(self):
        t = self.tasmota_topic
        if self.topic_format == "device_first":
            self.cmd_topic  = f"{t}/cmnd/POWER"
            self.stat_topic = f"{t}/stat/POWER"
            self.tele_sub   = f"{t}/tele/#"
        else:
            self.cmd_topic  = f"cmnd/{t}/POWER"
            self.stat_topic = f"stat/{t}/POWER"
            self.tele_sub   = f"tele/{t}/#"

    # ── Connection ───────────────────────────────────────────

    def connect(self) -> bool:
        try:
            self._client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
            if self.username:
                self._client.username_pw_set(self.username, self.password)
            self._client.on_connect    = self._on_connect
            self._client.on_disconnect = self._on_disconnect
            self._client.on_message    = self._on_message
            self._client.connect(self.broker_ip, self.broker_port, keepalive=60)
            self._client.loop_start()
            log.info(f"MQTT topics — cmd:{self.cmd_topic} "
                     f"stat:{self.stat_topic} tele:{self.tele_sub}")
            return True
        except Exception as e:
            log.error(f"MQTT connect failed: {e}")
            return False

    def disconnect(self):
        if self._client:
            self._client.loop_stop()
            self._client.disconnect()
            self._connected = False

    def is_connected(self) -> bool:
        return self._connected

    def reconnect(self):
        self.disconnect()
        time.sleep(1)
        self.connect()

    # ── Commands ─────────────────────────────────────────────

    def turn_on(self) -> bool:
        return self._publish(self.cmd_topic, "ON")

    def turn_off(self) -> bool:
        self._cancel_off_timer()
        return self._publish(self.cmd_topic, "OFF")

    def turn_on_for(self, minutes: int) -> bool:
        if not self.turn_on():
            return False
        self._cancel_off_timer()
        self._off_timer = threading.Timer(minutes * 60, self._auto_off)
        self._off_timer.daemon = True
        self._off_timer.start()
        log.info(f"MQTT timed ON: {minutes}min")
        return True

    def send_cmd(self, subtopic: str, payload: str) -> bool:
        base = self.tasmota_topic
        if self.topic_format == "device_first":
            topic = f"{base}/cmnd/{subtopic}"
        else:
            topic = f"cmnd/{base}/{subtopic}"
        return self._publish(topic, payload)

    # ── State ────────────────────────────────────────────────

    def get_last_status(self) -> Optional[str]:
        return self._last_status

    def get_tele_log(self) -> List[tuple]:
        return list(self._tele_log)

    # ── Internal ─────────────────────────────────────────────

    def _publish(self, topic: str, payload: str) -> bool:
        if not self._connected or not self._client:
            log.warning(f"MQTT publish skipped — not connected ({topic}={payload})")
            return False
        result = self._client.publish(topic, payload)
        ok = result.rc == mqtt.MQTT_ERR_SUCCESS
        if not ok:
            log.error(f"MQTT publish failed: {topic}={payload} rc={result.rc}")
        return ok

    def _auto_off(self):
        log.info("MQTT auto-off timer fired")
        self.turn_off()

    def _cancel_off_timer(self):
        if self._off_timer:
            self._off_timer.cancel()
            self._off_timer = None

    # ── Callbacks ────────────────────────────────────────────

    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        if reason_code == 0:
            self._connected = True
            log.info(f"MQTT connected to {self.broker_ip}:{self.broker_port}")
            client.subscribe(self.stat_topic)
            client.subscribe(self.tele_sub)
            if self.cmd_enabled:
                for topic in (self.cmd_adhoc, self.cmd_oneshot, self.cmd_weekly):
                    if topic:
                        client.subscribe(topic)
                        log.info(f"MQTT subscribed: {topic}")
            # Query current boiler state so status bar is not gray on startup
            client.publish(self.cmd_topic, "")
            if self.on_connect_change:
                self.on_connect_change(True)
        else:
            log.error(f"MQTT connect refused: {reason_code}")
            self._connected = False
            if self.on_connect_change:
                self.on_connect_change(False)

    def _on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties=None):
        self._connected = False
        log.warning(f"MQTT disconnected: {reason_code}")
        if self.on_connect_change:
            self.on_connect_change(False)

    def _on_message(self, client, userdata, message):
        topic   = message.topic
        payload = message.payload.decode(errors="replace").strip()

        # Tasmota POWER status
        if topic == self.stat_topic:
            self._last_status = payload
            log.info(f"MQTT stat: {payload}")
            if self.on_status_change:
                self.on_status_change(payload)
            return

        # Telemetry
        if topic.startswith(self.tasmota_topic):
            subtopic = topic.split("/")[-1]
            ts = __import__("utils").now_local().strftime("%H:%M:%S")
            self._tele_log.append((ts, subtopic, payload))
            if len(self._tele_log) > 200:
                self._tele_log.pop(0)
            if self.on_tele:
                self.on_tele(subtopic, payload)
            return

        # Inbound commands
        if not self.cmd_enabled:
            return
        if topic == self.cmd_adhoc and self.on_command:
            self.on_command("adhoc", payload)
        elif topic == self.cmd_oneshot and self.on_command:
            self.on_command("oneshot", payload)
        elif topic == self.cmd_weekly and self.on_command:
            self.on_command("weekly", payload)
