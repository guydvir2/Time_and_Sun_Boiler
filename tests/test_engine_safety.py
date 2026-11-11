"""Replays the failures found in the code review: run overlap, weather outage at lock time, damaged state.json."""
import json
import os
import tempfile
import unittest
from datetime import datetime, date, time, timedelta

from boiler import service, state, runs
from boiler.settings import Settings
from boiler.drivers import build_drivers

D = date(2026, 11, 10)


class FakeDT(datetime):
    cur = None

    @classmethod
    def now(cls, tz=None):
        return cls.cur


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        sj = os.path.join(self.tmp, "settings.json")
        json.dump({"location": {"latitude": "32.7", "longitude": "35.1"}, "output": {"drivers": "dryrun"},
                   "parameters": {"RUN1_PERCENT": "60", "RUN2_AT": "21:00"}, "power": {"enabled": "False"}}, open(sj, "w"))
        here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.s = Settings(ini_paths=[os.path.join(here, "config.ini")], environ={}, json_path=sj)
        self.s.stored_dir = self.tmp
        for k in ("state_file", "decisions_file", "activity_file", "schedule_file", "runs_file", "formula_file", "formula_history"):
            setattr(self.s, k, os.path.join(self.tmp, os.path.basename(getattr(self.s, k))))
        self.drivers, _ = build_drivers(self.s)
        self.online, self.fetches, self.clock = True, 0, 0.0
        for mod in (state, runs, service):
            mod.datetime = FakeDT
        service.get_day = self._day
        service.monotonic = lambda: self.clock

    def tearDown(self):
        for mod in (state, runs, service):
            mod.datetime = datetime

    def _day(self, s, day=None):
        self.fetches += 1
        if not self.online:
            return None
        rows = [{"time": datetime.combine(D, time(h)), "temp": 14.0, "clouds": 40.0} for h in range(24)]
        return {"rows": rows, "sunrise": datetime.combine(D, time(6, 5)), "sunset": datetime.combine(D, time(16, 45))}

    def run_day(self, start, end, hook=None):
        eng = service.Engine(self.s, self.drivers)
        now = start
        while now <= end:
            FakeDT.cur = now
            self.clock = (now - datetime.combine(D, time(0))).total_seconds()
            if hook:
                hook(now, eng)
            eng.tick(now)
            now += timedelta(seconds=30)
        return eng

    def at(self, h, m=0):
        return datetime.combine(D, time(h, m))


class OverlapTest(Base):
    def test_new_start_does_not_cut_a_longer_run(self):
        def hook(now, eng):
            if now == self.at(20, 50):
                runs.record_start(self.s, 120, "manual")
        self.run_day(self.at(5), self.at(21, 5), hook)
        r = state.load_running(self.s, self.at(21, 5))
        self.assertEqual(r["until"][:16], "2026-11-10T22:50")


class WeatherOutageTest(Base):
    def test_outage_does_not_hammer_and_run2_still_happens(self):
        def hook(now, eng):
            if now.time() >= time(15, 0):
                self.online = False
        self.run_day(self.at(5), self.at(23, 30), hook)
        plan = state.load_plan(self.s)
        self.assertEqual(plan["status"], "final")
        self.assertEqual([r["status"] for r in plan["runs"]], ["started", "started"])
        self.assertLess(self.fetches, 120)                      # was 799 before the fix


class StateFileTest(Base):
    def test_truncated_state_falls_back_to_backup(self):
        state.set_auto(self.s, False)
        state.save_heartbeat(self.s)                            # second write: first one becomes the backup
        open(self.s.state_file, "w").close()                    # what a crash mid-write used to leave
        self.assertFalse(state.auto_enabled(self.s))

    def test_no_half_written_file_left_behind(self):
        state.set_auto(self.s, True)
        self.assertFalse(os.path.exists(self.s.state_file + ".tmp"))


if __name__ == "__main__":
    unittest.main()
