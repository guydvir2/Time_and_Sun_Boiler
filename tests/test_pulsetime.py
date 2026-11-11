"""PulseTime handling: HA runs get a backup timeout, every run books a check, the engine clears + verifies it."""
import os
import tempfile
from datetime import datetime, timedelta
from types import SimpleNamespace as NS

from boiler import service, state
from boiler.drivers import mqtt_tasmota as mt, start_boiler


class FakeHA:
    name, supports_stop = "homeassistant", False

    def start(self, minutes):
        return True


def make(monkeypatch):
    d = tempfile.mkdtemp()
    s = NS(mqtt_host="b", mqtt_port=1883, mqtt_user="", mqtt_password="", tasmota_topic="WH",
           tasmota_full_topic="%topic%/%prefix%/", tasmota_relay=1, tasmota_confirm=True, tasmota_confirm_timeout=1,
           request_timeout=1, state_file=os.path.join(d, "state.json"))
    sent = []
    monkeypatch.setattr(mt.TasmotaMqttDriver, "_exchange",
                        lambda self, commands, expect=None, match=None, force_confirm=False: sent.append(commands) or True)
    return s, sent


def test_ha_run_gets_backup_timeout_and_a_check(monkeypatch):
    s, sent = make(monkeypatch)
    ok, used = start_boiler([FakeHA()], 10, s)
    assert ok and sent == [[("WH/cmnd/PulseTime1", "820")]]        # 10 min + 2 min margin = 100 + 720 s
    due = state.load_pulse_check(s)
    assert 12 < (due - datetime.now()).total_seconds() / 60 <= 13


def test_engine_clears_after_the_run(monkeypatch):
    s, sent = make(monkeypatch)
    start_boiler([FakeHA()], 10, s)
    sent.clear()
    monkeypatch.setattr(service, "log_event", lambda *a, **k: None)
    eng = service.Engine.__new__(service.Engine)
    eng.s, eng.last_try = s, {}
    eng._pulse_guard(datetime.now())                                 # run not over yet
    assert sent == []
    eng._pulse_guard(datetime.now() + timedelta(minutes=14))
    assert sent == [[("WH/cmnd/PulseTime1", "0")]] and state.load_pulse_check(s) is None


def test_tasmota_stop_clears_pulsetime(monkeypatch):
    s, sent = make(monkeypatch)
    mt.TasmotaMqttDriver(s).stop()
    assert sent == [[("WH/cmnd/Power1", "OFF"), ("WH/cmnd/PulseTime1", "0")]]
