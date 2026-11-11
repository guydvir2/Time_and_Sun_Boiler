"""Safety watchdog: relay ON with no run of ours active gets switched OFF after the limit."""
import os
import tempfile
import unittest
from datetime import datetime, timedelta

from boiler import service, state, runs
from boiler.settings import Settings
from boiler.drivers.base import Driver


class FakeDriver(Driver):
    name = "fake"
    supports_stop = True

    def __init__(self):
        self.stops = 0

    def start(self, minutes):
        return True

    def stop(self):
        self.stops += 1
        return True


class FakePower:
    def __init__(self, relay):
        self.relay = relay


class WatchdogTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.s = Settings(ini_paths=[os.path.join(here, "config.ini")], environ={}, json_path=os.path.join(self.tmp, "x.json"))
        self.s.stored_dir = self.tmp
        for k in ("state_file", "decisions_file", "activity_file", "schedule_file", "runs_file"):
            setattr(self.s, k, os.path.join(self.tmp, os.path.basename(getattr(self.s, k))))
        self.s.max_unplanned_on = 60
        self.drv, self.pw = FakeDriver(), FakePower("ON")
        self.eng = service.Engine(self.s, [self.drv], self.pw)
        self.t0 = datetime(2026, 11, 10, 12, 0)

    def test_switches_off_after_limit(self):
        self.eng._watchdog(self.t0)
        self.eng._watchdog(self.t0 + timedelta(minutes=59))
        self.assertEqual(self.drv.stops, 0)
        self.eng._watchdog(self.t0 + timedelta(minutes=61))
        self.assertEqual(self.drv.stops, 1)

    def test_relay_off_resets(self):
        self.eng._watchdog(self.t0)
        self.pw.relay = "OFF"
        self.eng._watchdog(self.t0 + timedelta(minutes=30))
        self.pw.relay = "ON"
        self.eng._watchdog(self.t0 + timedelta(minutes=40))
        self.eng._watchdog(self.t0 + timedelta(minutes=90))      # only 50 min since it came back
        self.assertEqual(self.drv.stops, 0)

    def test_own_run_is_left_alone(self):
        runs.record_start(self.s, 300, "manual")                  # a 5 h run of ours (until is far ahead)
        self.eng._watchdog(datetime.now())
        self.eng._watchdog(datetime.now() + timedelta(minutes=120))
        self.assertEqual(self.drv.stops, 0)

    def test_off_when_disabled_or_no_sensor(self):
        self.s.max_unplanned_on = 0
        self.eng._watchdog(self.t0)
        self.eng._watchdog(self.t0 + timedelta(minutes=500))
        self.assertEqual(self.drv.stops, 0)
        self.s.max_unplanned_on = 60
        self.eng.power = None
        self.eng._watchdog(self.t0)
        self.eng._watchdog(self.t0 + timedelta(minutes=500))
        self.assertEqual(self.drv.stops, 0)


if __name__ == "__main__":
    unittest.main()
