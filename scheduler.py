"""
Scheduler — single background thread, 30s poll.
Modes (coexist): SOLAR, WEEKLY, ONE-SHOT.
Execution routed to HA or MQTT per runtime_settings.
"""
import logging
import threading
import time
from datetime import date, datetime, timedelta
from typing import Optional

log = logging.getLogger(__name__)

_WEEKDAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def _weekday() -> str:
    return _WEEKDAYS[datetime.now().weekday()]


class Scheduler:
    POLL = 30  # seconds

    def __init__(self, weather_service, ha_service, data_manager,
                 runtime_settings, mqtt_service=None):
        self.weather      = weather_service
        self.ha           = ha_service
        self.dm           = data_manager
        self.rs           = runtime_settings
        self.mqtt         = mqtt_service

        self.on_state_change = None   # GUI callback(state: str)

        self._thread: Optional[threading.Thread] = None
        self._running = False

        # Solar state
        self._solar_done_today    = False
        self._solar_done_date     = None
        self._second_run_duration = 0
        self._second_run_fired    = False

        # Weekly state
        self._weekly_fired: set   = set()
        self._weekly_date         = None

        # One-shot state
        self._oneshot_fired       = False
        self._oneshot_date        = None

        self._parse_target(self.rs.get_target_time())
        log.info(f"Scheduler ready — target {self.rs.get_target_time()}, "
                 f"check {self._check_h:02d}:{self._check_m:02d}")

    # ── Public ───────────────────────────────────────────────

    def start(self):
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        log.info("Scheduler started")

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        log.info("Scheduler stopped")

    def update_target_time(self, t: str):
        self._parse_target(t)

    # ── Internal ─────────────────────────────────────────────

    def _parse_target(self, t: str):
        h, m = int(t[:2]), int(t[3:])
        self._target_h, self._target_m = h, m
        chk = datetime.now().replace(hour=h, minute=m, second=0, microsecond=0) \
              - timedelta(hours=2)
        self._check_h, self._check_m = chk.hour, chk.minute

    def _state(self, s: str):
        if self.on_state_change:
            self.on_state_change(s)

    def _mode(self) -> str:
        return self.rs.get_execution_mode()

    def _execute(self, duration: int, label: str) -> bool:
        mode = self._mode()
        log.info(f"EXECUTE [{label}] {duration}min via {mode}")
        self._state("EXECUTE")
        if mode == "MQTT":
            if not self.mqtt:
                log.error("MQTT mode but no mqtt_service")
                return False
            if not self.mqtt.is_connected() and not self.mqtt.connect():
                log.error("MQTT connect failed")
                return False
            return self.mqtt.turn_on_for(duration)
        else:
            return self.ha.send_first_run(duration)

    def _execute_2nd(self, duration: int) -> bool:
        mode = self._mode()
        log.info(f"EXECUTE [2ND] {duration}min via {mode}")
        self._state("EXECUTE")
        if mode == "MQTT":
            if not self.mqtt:
                return False
            if not self.mqtt.is_connected():
                self.mqtt.connect()
            return self.mqtt.turn_on_for(duration)
        else:
            return self.ha.send_second_run(duration)

    # ── Main loop ────────────────────────────────────────────

    def _loop(self):
        while self._running:
            try:
                self._tick()
            except Exception as e:
                log.error(f"Scheduler error: {e}", exc_info=True)
            time.sleep(self.POLL)

    def _tick(self):
        now   = datetime.now()
        today = date.today()

        # Reset daily trackers on new day
        if self._weekly_date != today:
            self._weekly_fired = set()
            self._weekly_date  = today
        if self._solar_done_date != today:
            self._solar_done_today    = False
            self._second_run_duration = 0
            self._second_run_fired    = False
            self._solar_done_date     = today
        if self._oneshot_date != today:
            self._oneshot_fired = False

        self._tick_solar(now, today)
        self._tick_weekly(now)
        self._tick_oneshot(now, today)

    # ── Solar ────────────────────────────────────────────────

    def _tick_solar(self, now: datetime, today: date):
        if not self.rs.get_solar_active():
            self._state("SOLAR_INACTIVE")
            return

        if not self._solar_done_today:
            already = any(r["date"] == str(today) for r in self.dm.all_records())
            if already:
                self._solar_done_today = True
                self._state("WAIT_NEXT_DAY")
            else:
                check_time = now.replace(hour=self._check_h, minute=self._check_m,
                                         second=0, microsecond=0)
                if now >= check_time:
                    self._solar_calc_and_fire(now, str(today))
                else:
                    self._state("WAIT_FIRST_CHECK")

        # 2nd run
        if self._solar_done_today and not self._second_run_fired \
                and self._second_run_duration > 0:
            t2 = self.rs.get_second_run_time()
            h, m = int(t2[:2]), int(t2[3:])
            if now >= now.replace(hour=h, minute=m, second=0, microsecond=0):
                log.info(f"Solar 2nd run {self._second_run_duration}min")
                self._execute_2nd(self._second_run_duration)
                self._second_run_fired = True

    def _solar_calc_and_fire(self, now: datetime, today_str: str):
        self._state("FETCH_SUN")
        try:
            sunrise, sunset = self.weather.get_sun_times()
        except Exception as e:
            log.error(f"Sun times failed: {e}")
            return

        self._state("CALCULATING")
        try:
            calc = self.weather.calculate_all(sunrise, sunset)
        except Exception as e:
            log.error(f"Calc failed: {e}")
            return

        first_run  = calc["first_run"]
        second_run = calc["second_run"]
        self._second_run_duration = second_run

        target = now.replace(hour=self._target_h, minute=self._target_m,
                             second=0, microsecond=0)
        trigger = target - timedelta(minutes=first_run)

        if trigger > now:
            log.info(f"Solar waiting until {trigger.strftime('%H:%M')}")
            self._state("WAIT_TRIGGER")
            while self._running and datetime.now() < trigger:
                time.sleep(min(30, (trigger - datetime.now()).total_seconds()))
            if not self._running:
                return

        ok = self._execute(first_run, "SOLAR-1ST")
        self.dm.save_record({
            "date":           today_str,
            "dawn":           sunrise.strftime("%H:%M"),
            "dusk":           sunset.strftime("%H:%M"),
            "avg_temp":       calc["avg_temp"],
            "avg_cloud":      calc["avg_cloud"],
            "effective_temp": calc["effective_temp"],
            "duration":       calc["duration"],
            "first_run":      first_run,
            "second_run":     second_run,
            "trigger_time":   datetime.now().strftime("%H:%M:%S"),
            "ha_status":      "OK" if ok else "FAILED",
        })
        self._solar_done_today = True
        self._state("WAIT_NEXT_DAY" if ok else "ERROR")

    # ── Weekly ───────────────────────────────────────────────

    def _tick_weekly(self, now: datetime):
        today_day = _weekday()
        for p in self.rs.get_weekly_presets():
            pid = p.get("id")
            if not p.get("active") or pid in self._weekly_fired:
                continue
            if today_day not in p.get("days", []):
                continue
            h, m = int(p["start_time"][:2]), int(p["start_time"][3:])
            fire = now.replace(hour=h, minute=m, second=0, microsecond=0)
            if 0 <= (now - fire).total_seconds() < self.POLL * 2:
                self._execute(int(p.get("duration", 30)), f"WEEKLY-{pid}")
                self._weekly_fired.add(pid)

    # ── One-shot ─────────────────────────────────────────────

    def _tick_oneshot(self, now: datetime, today: date):
        if self._oneshot_fired:
            return
        os = self.rs.get_one_shot()
        if not os.get("armed") or int(os.get("duration", 0)) <= 0:
            return
        st = os["start_time"]
        h, m = int(st[:2]), int(st[3:])
        fire = now.replace(hour=h, minute=m, second=0, microsecond=0)
        if 0 <= (now - fire).total_seconds() < self.POLL * 2:
            self._execute(int(os["duration"]), "ONE-SHOT")
            self._oneshot_fired = True
            self._oneshot_date  = today
            self.rs.set_one_shot(st, int(os["duration"]), armed=False)
