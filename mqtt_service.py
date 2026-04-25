"""
MQTT Service — Tasmota direct control

Topic formats (config.ini topic_format):
  device_first : <topic>/cmnd/X   <topic>/stat/X   <topic>/tele/#
  standard     : cmnd/<topic>/X   stat/<topic>/X   tele/<topic>/#
"""
import logging
import threading
import time
from typing import Callable, List, Optional

log = logging.getLogger(__name__)

try:
    import paho.mqtt.client as mqtt
    MQTT_AVAILABLE = True
except ImportError:
    MQTT_AVAILABLE = False
    log.warning("paho-mqtt not installed — MQTT mode unavailable")


class MQTTService:
    MAX_TELE_LINES = 100

    def __init__(self, broker_ip: str, broker_port: int, tasmota_topic: str,
                 username: str = "", password: str = "",
                 topic_format: str = "device_first"):
        self.broker_ip    = broker_ip
        self.broker_port  = broker_port
        self.tasmota_topic = tasmota_topic
        self.username     = username
        self.password     = password
        self.topic_format = topic_format

        # If True, connect on startup
        self.active: bool = False

        self._client: Optional["mqtt.Client"] = None
        self._connected = False
        self._last_status: Optional[str] = None
        self._off_timer: Optional[threading.Timer] = None
        self._tele_log: List[tuple] = []

        # Command listener topics (from Telegram / external clients)
        self.cmd_enabled  = False
        self.cmd_adhoc    = ""
        self.cmd_oneshot  = ""
        self.cmd_weekly   = ""

        # Callbacks — set by caller
        self.on_connect_change: Optional[Callable[[bool], None]]         = None
        self.on_status_change:  Optional[Callable[[str], None]]          = None
        self.on_tele:           Optional[Callable[[str, str], None]]      = None
        self.on_command:        Optional[Callable[[str, str], None]]      = None
        # on_command(cmd_type, payload)
        # cmd_type: "adhoc" | "oneshot" | "weekly"
        # adhoc   payload: "<minutes>"            e.g. "45"
        # oneshot payload: "<HH:MM>,<minutes>"    e.g. "20:00,60"
        # weekly  payload: "<id>,<HH:MM>,<min>,<days>"  e.g. "1,08:00,45,Mon,Wed,Fri"

        self._build_topics()

    # ── Topics ──────────────────────────────────────────────

    def _build_topics(self):
        t = self.tasmota_topic
        if self.topic_format == "device_first":
            self.cmd_topic   = f"{t}/cmnd/POWER"
            self.stat_topic  = f"{t}/stat/POWER"
            self.tele_prefix = f"{t}/tele/"
            self.tele_sub    = f"{t}/tele/#"
        else:
            self.cmd_topic   = f"cmnd/{t}/POWER"
            self.stat_topic  = f"stat/{t}/POWER"
            self.tele_prefix = f"tele/{t}/"
            self.tele_sub    = f"tele/{t}/#"
        log.info(f"MQTT topics — cmd:{self.cmd_topic} stat:{self.stat_topic} tele:{self.tele_sub}")

    def set_topic_format(self, fmt: str):
        self.topic_format = fmt
        self._build_topics()

    # ── Connection ───────────────────────────────────────────

    def connect(self) -> bool:
        if not MQTT_AVAILABLE:
            log.error("paho-mqtt not installed")
            return False
        try:
            self._client = mqtt.Client()
            if self.username:
                self._client.username_pw_set(self.username, self.password)
            self._client.on_connect    = self._on_connect
            self._client.on_disconnect = self._on_disconnect
            self._client.on_message    = self._on_message
            self._client.connect(self.broker_ip, self.broker_port, keepalive=60)
            self._client.loop_start()
            for _ in range(50):
                if self._connected:
                    return True
                time.sleep(0.1)
            log.error(f"MQTT connect timeout to {self.broker_ip}:{self.broker_port}")
            return False
        except Exception as e:
            log.error(f"MQTT connect failed: {e}")
            return False

    def disconnect(self):
        self._cancel_off_timer()
        if self._client:
            self._client.loop_stop()
            self._client.disconnect()
        self._connected = False
        log.info("MQTT disconnected")

    def is_connected(self) -> bool:
        return self._connected

    # ── Commands ─────────────────────────────────────────────

    def turn_on(self) -> bool:
        return self._publish(self.cmd_topic, "ON")

    def turn_off(self) -> bool:
        self._cancel_off_timer()
        return self._publish(self.cmd_topic, "OFF")

    def turn_on_for(self, duration_minutes: int) -> bool:
        if not self.turn_on():
            return False
        self._cancel_off_timer()
        self._off_timer = threading.Timer(duration_minutes * 60, self._auto_off)
        self._off_timer.daemon = True
        self._off_timer.start()
        log.info(f"MQTT ON — auto-OFF in {duration_minutes} min")
        return True

    def send_cmd(self, subtopic: str, payload: str) -> bool:
        t = self.tasmota_topic
        topic = f"{t}/cmnd/{subtopic}" if self.topic_format == "device_first" \
                else f"cmnd/{t}/{subtopic}"
        return self._publish(topic, payload)

    def get_status(self) -> Optional[str]:
        return self._last_status

    def get_tele_log(self) -> List[tuple]:
        return list(self._tele_log)

    def clear_tele_log(self):
        self._tele_log.clear()

    # ── Internal ─────────────────────────────────────────────

    def _publish(self, topic: str, payload: str) -> bool:
        if not self._connected or not self._client:
            log.error("MQTT not connected — cannot publish")
            return False
        result = self._client.publish(topic, payload)
        ok = result.rc == 0
        log.info(f"MQTT {topic} = {payload} → {'OK' if ok else 'FAILED'}")
        return ok

    def _auto_off(self):
        log.info("MQTT auto-OFF fired")
        self._publish(self.cmd_topic, "OFF")

    def _cancel_off_timer(self):
        if self._off_timer and self._off_timer.is_alive():
            self._off_timer.cancel()
        self._off_timer = None

    def _fire(self, cb, *args):
        if cb:
            cb(*args)

    # ── paho callbacks ───────────────────────────────────────

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self._connected = True
            client.subscribe(self.stat_topic)
            client.subscribe(self.tele_sub)
            if self.cmd_enabled:
                for t in [self.cmd_adhoc, self.cmd_oneshot, self.cmd_weekly]:
                    if t:
                        client.subscribe(t)
                        log.info(f"MQTT cmd topic subscribed: {t}")
            log.info(f"MQTT connected → subscribed stat+tele+cmds")
            self._fire(self.on_connect_change, True)
            # Query current POWER state
            client.publish(self.cmd_topic, "")
        else:
            log.error(f"MQTT connect error rc={rc}")
            self._fire(self.on_connect_change, False)

    def _on_disconnect(self, client, userdata, rc):
        self._connected = False
        log.warning(f"MQTT disconnected rc={rc}")
        self._fire(self.on_connect_change, False)

    def _on_message(self, client, userdata, msg):
        try:
            topic   = msg.topic
            payload = msg.payload.decode().strip()
            if topic == self.stat_topic:
                self._last_status = payload.upper()
                log.info(f"MQTT stat: {payload}")
                self._fire(self.on_status_change, self._last_status)
            elif topic.startswith(self.tele_prefix):
                subtopic = topic[len(self.tele_prefix):]
                self._tele_log.append((subtopic, payload))
                if len(self._tele_log) > self.MAX_TELE_LINES:
                    self._tele_log.pop(0)
                self._fire(self.on_tele, subtopic, payload)
            elif self.cmd_enabled and topic == self.cmd_adhoc:
                log.info(f"MQTT cmd adhoc: {payload}")
                self._fire(self.on_command, "adhoc", payload)
            elif self.cmd_enabled and topic == self.cmd_oneshot:
                log.info(f"MQTT cmd oneshot: {payload}")
                self._fire(self.on_command, "oneshot", payload)
            elif self.cmd_enabled and topic == self.cmd_weekly:
                log.info(f"MQTT cmd weekly: {payload}")
                self._fire(self.on_command, "weekly", payload)
        except Exception as e:
            log.error(f"MQTT message error: {e}")
