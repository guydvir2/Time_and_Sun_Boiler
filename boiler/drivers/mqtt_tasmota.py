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

    def _exchange(self, commands, expect=None, match=None, force_confirm=False):
        """Publish commands (in order) and wait for the device to confirm: either Power<n> == expect, or
        match(flat_result_text) is true for a /RESULT message (used to verify PulseTime)."""
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
            if expect and topic.endswith(("/POWER", f"/POWER{n}")) and body == expect:
                confirmed.set()
            elif topic.endswith("/RESULT"):
                flat = body.replace(" ", "")
                if expect and (f'"POWER{n}":"{expect}"' in flat or f'"POWER":"{expect}"' in flat):
                    confirmed.set()
                elif match and match(flat):
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
            if not s.tasmota_confirm and not force_confirm:
                return True
            what = expect or "the PulseTime reply"
            if confirmed.wait(s.tasmota_confirm_timeout):
                print(f"✅ MQTT: Tasmota confirmed {what if not expect else 'the relay is ' + expect}")
                return True
            print(f"❌ MQTT: commands sent but the device did not confirm {what} within "
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

    def check(self, wait=4):
        """Two lights: the broker, and the plug. The plug is asked for its relay state with an empty
        Power<n> command (a query in Tasmota: it reports, it does not switch). Also reads the retained LWT."""
        s = self.s
        issues = self.problems()
        if issues:
            return [{"name": "MQTT broker", "level": "red", "text": issues[0].split(": ", 1)[-1]}]
        n = s.tasmota_relay
        lwt_topic = self.topic("tele", "LWT")
        connected, replied = threading.Event(), threading.Event()
        seen = {"lwt": None, "power": None}

        def on_connect(c, userdata, flags, rc, *extra):
            ok = (not rc.is_failure) if getattr(rc, "is_failure", None) is not None else rc == 0
            if ok:
                c.subscribe([(self.topic("stat", "#"), 0), (lwt_topic, 0)])
                connected.set()
            else:
                seen["refused"] = str(rc)
                connected.set()

        def on_subscribe(c, userdata, mid, *extra):
            c.publish(self.topic("cmnd", f"Power{n}"), "", qos=0)      # empty payload = query only

        def on_message(c, userdata, msg):
            body = msg.payload.decode(errors="replace").strip()
            t = msg.topic.upper()
            if msg.topic == lwt_topic:
                seen["lwt"] = body
            elif t.endswith(("/POWER", f"/POWER{n}")):
                seen["power"] = body.upper(); replied.set()
            elif t.endswith("/RESULT"):
                flat = body.upper().replace(" ", "")
                for key in (f'"POWER{n}":"', '"POWER":"'):
                    if key in flat:
                        seen["power"] = flat.split(key, 1)[1].split('"', 1)[0]; replied.set()

        c = self._client()
        c.on_connect, c.on_subscribe, c.on_message = on_connect, on_subscribe, on_message
        broker = f"{s.mqtt_host}:{s.mqtt_port}"
        try:
            c.connect(s.mqtt_host, s.mqtt_port, keepalive=15)
            c.loop_start()
            if not connected.wait(wait) or seen.get("refused"):
                why = f"refused ({seen['refused']})" if seen.get("refused") else "no answer"
                return [{"name": "MQTT broker", "level": "red", "text": f"{broker}: {why}"}]
            replied.wait(wait)
        except Exception as e:
            return [{"name": "MQTT broker", "level": "red", "text": f"{broker}: not reachable ({type(e).__name__})"}]
        finally:
            c.loop_stop()
            try:
                c.disconnect()
            except Exception:
                pass
        rows = [{"name": "MQTT broker", "level": "green", "text": f"Connected ({broker})"}]
        plug = f"Plug {s.tasmota_topic}"
        if seen["power"]:
            rows.append({"name": plug, "level": "green", "text": f"Online · relay {seen['power']}", "relay": seen["power"]})
        elif (seen["lwt"] or "").lower() == "offline":
            rows.append({"name": plug, "level": "red", "text": "Offline (device reports Offline)"})
        else:
            rows.append({"name": plug, "level": "red", "text": "No reply (check topic / power to the plug)"})
        return rows

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
        cmd_pulse = self.topic("cmnd", f"PulseTime{s.tasmota_relay}")
        print(f"📡 MQTT {s.mqtt_host}:{s.mqtt_port}: {cmd_power} OFF (+ PulseTime cleared)")
        return self._exchange([(cmd_power, "OFF"), (cmd_pulse, "0")], "OFF")

    def arm(self, minutes: int) -> bool:
        """Set the device's self-shut-off timer WITHOUT switching anything (the backup for a run that
        another driver, e.g. Home Assistant, performs). True when the device answered."""
        n = self.s.tasmota_relay
        cmd = self.topic("cmnd", f"PulseTime{n}")
        value = pulsetime_value(minutes)
        print(f"📡 MQTT: {cmd} {value} ({minutes} min backup timeout)")
        return self._exchange([(cmd, str(value))], match=lambda flat: f'"PULSETIME{n}"' in flat, force_confirm=True)

    def clear_pulse(self) -> bool:
        """Set PulseTime to 0 and VERIFY the device reports it as 0 (so a later switch-ON, from anywhere,
        is never cut short by an old run's timer)."""
        n = self.s.tasmota_relay
        cmd = self.topic("cmnd", f"PulseTime{n}")
        print(f"📡 MQTT: {cmd} 0 (verify it reads 0)")
        return self._exchange([(cmd, "0")], match=lambda flat: f'"PULSETIME{n}":{{"SET":0' in flat, force_confirm=True)
