"""MQTT driver for a Tasmota relay: no Home Assistant needed.

start() sends two commands:
    <cmnd>/PulseTime<n>  <value>   -> the DEVICE turns itself off after the time
    <cmnd>/Power<n>      ON
Because the shut-off timer lives in the Tasmota device, the boiler still turns off on time even if
this app, the VM, the broker or the network dies right after starting it. That is deliberate.
stop() sends Power<n> OFF.

Tasmota PulseTime encoding: 1..111 = 0.1 s steps, 112..64900 = (value - 100) seconds (max 18 h).
Topic layout follows the device's Full Topic (see topic()).
"""
import threading
import paho.mqtt.client as mqtt

from .base import Driver

MAX_SECONDS = 64900 - 100


def pulsetime_value(minutes: int) -> int:
    """Tasmota PulseTime payload for a run of `minutes` (whole minutes, capped at 18 h)."""
    seconds = min(max(int(minutes), 0) * 60, MAX_SECONDS)
    return 100 + seconds if seconds > 11 else seconds * 10


class TasmotaMqttDriver(Driver):
    name = "mqtt_tasmota"
    supports_stop = True

    def __init__(self, s):
        self.s = s

    def topic(self, prefix, suffix):
        """Build a topic the way Tasmota does: Full Topic with %prefix% and %topic% filled in.
        Default layout  cmnd/<topic>/Power1 ; with Full Topic '%topic%/%prefix%/' it is <topic>/cmnd/Power1."""
        full = self.s.tasmota_full_topic.replace("%prefix%", prefix).replace("%topic%", self.s.tasmota_topic)
        if not full.endswith("/"):
            full += "/"
        return full + suffix

    def problems(self):
        p = []
        if "%topic%" not in self.s.tasmota_full_topic or "%prefix%" not in self.s.tasmota_full_topic:
            p.append("Tasmota: full_topic must contain both %prefix% and %topic% (copy it from the device's "
                     "Configuration > MQTT > Full Topic)")
        if not self.s.mqtt_host:
            p.append("MQTT: no [mqtt] host in config")
        if not self.s.tasmota_topic:
            p.append("MQTT: no [tasmota] topic in config (the device's %topic%)")
        return p

    def _client(self):
        s = self.s
        try:                                           # paho-mqtt >= 2.0
            c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="time-and-sun-boiler")
        except AttributeError:                         # paho-mqtt 1.x
            c = mqtt.Client(client_id="time-and-sun-boiler")
        if s.mqtt_user:
            c.username_pw_set(s.mqtt_user, s.mqtt_password)
        return c

    def _exchange(self, commands, expect):
        """Publish commands (in order) and wait for the device to report Power<n> == expect."""
        s = self.s
        n = s.tasmota_relay
        stat_all = self.topic("stat", "#")
        connected, subscribed, confirmed = threading.Event(), threading.Event(), threading.Event()

        def on_connect(c, userdata, flags, rc, *extra):
            if getattr(rc, "is_failure", None) is not None:
                ok = not rc.is_failure
            else:
                ok = rc == 0
            if ok:
                c.subscribe(stat_all, qos=1)
                connected.set()
            else:
                print(f"❌ MQTT: broker refused connection ({rc})")

        def on_subscribe(c, userdata, mid, *extra):
            subscribed.set()

        def on_message(c, userdata, msg):
            body = msg.payload.decode(errors="replace").upper()
            topic = msg.topic.upper()
            if topic.endswith(("/POWER", f"/POWER{n}")) and body == expect:
                confirmed.set()
            elif topic.endswith("/RESULT"):
                flat = body.replace(" ", "")
                if f'"POWER{n}":"{expect}"' in flat or f'"POWER":"{expect}"' in flat:
                    confirmed.set()

        c = self._client()
        c.on_connect, c.on_subscribe, c.on_message = on_connect, on_subscribe, on_message
        try:
            c.connect(s.mqtt_host, s.mqtt_port, keepalive=30)
            c.loop_start()
            if not (connected.wait(s.request_timeout) and subscribed.wait(s.request_timeout)):
                print("❌ MQTT: could not connect/subscribe in time")
                return False
            for topic, payload in commands:            # order matters (PulseTime before Power ON)
                info = c.publish(topic, payload, qos=1)
                info.wait_for_publish(s.request_timeout)
                if not info.is_published():
                    print(f"❌ MQTT: publish to {topic} not acknowledged")
                    return False
            if not s.tasmota_confirm:
                return True
            if confirmed.wait(s.tasmota_confirm_timeout):
                print(f"✅ MQTT: Tasmota confirmed the relay is {expect}")
                return True
            print(f"❌ MQTT: commands sent but the device did not confirm {expect} within "
                  f"{s.tasmota_confirm_timeout}s (is the topic right? is it online?)")
            return False
        except Exception as e:
            print(f"❌ MQTT error: {e}")
            return False
        finally:
            c.loop_stop()
            try:
                c.disconnect()
            except Exception:
                pass

    def start(self, minutes: int) -> bool:
        s = self.s
        if minutes <= 0:
            print("ℹ️  MQTT: duration is 0 — nothing to switch on")
            return True
        n = s.tasmota_relay
        cmd_pulse, cmd_power = self.topic("cmnd", f"PulseTime{n}"), self.topic("cmnd", f"Power{n}")
        value = pulsetime_value(minutes)
        print(f"📡 MQTT {s.mqtt_host}:{s.mqtt_port}: {cmd_pulse} {value} ({minutes} min), then {cmd_power} ON")
        # arm the self-shutoff timer BEFORE switching on
        return self._exchange([(cmd_pulse, str(value)), (cmd_power, "ON")], "ON")

    def stop(self) -> bool:
        s = self.s
        cmd_power = self.topic("cmnd", f"Power{s.tasmota_relay}")
        print(f"📡 MQTT {s.mqtt_host}:{s.mqtt_port}: {cmd_power} OFF")
        return self._exchange([(cmd_power, "OFF")], "OFF")
