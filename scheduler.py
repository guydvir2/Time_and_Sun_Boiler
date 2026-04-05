"""
Scheduler
Backward calculation from target time
MINIMAL LOGGING - only key events
"""

import logging
import time
import threading
from datetime import datetime, timedelta, date

log = logging.getLogger(__name__)


class Scheduler:
    """
    Backward calculation scheduler.
    Checks 2 hours before target, calculates trigger time, executes.
    """
    
    HOURS_BEFORE_TARGET = 2
    
    def __init__(self, weather_service, ha_service, data_manager, target_time_str):
        self.weather_service = weather_service
        self.ha_service = ha_service
        self.data_manager = data_manager
        
        self.thread = None
        self.running = False
        self.on_state_change = None  # Callback for GUI
        
        # Parse target time
        parts = target_time_str.split(":")
        self.target_hour = int(parts[0])
        self.target_minute = int(parts[1])
        
        # Calculate check time (2 hours before)
        check_time = datetime.now().replace(
            hour=self.target_hour, minute=self.target_minute, second=0, microsecond=0
        ) - timedelta(hours=self.HOURS_BEFORE_TARGET)
        self.check_hour = check_time.hour
        self.check_minute = check_time.minute
        
        log.info(f"Scheduler: target={target_time_str}, check={self.check_hour:02d}:{self.check_minute:02d}")
    
    def start(self):
        """Start scheduler thread"""
        if self.running:
            return
        self.running = True
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()
        log.info("Scheduler started")
    
    def stop(self):
        """Stop scheduler thread"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
        log.info("Scheduler stopped")
    
    def update_target_time(self, target_time_str):
        """Update target time (called from GUI)"""
        parts = target_time_str.split(":")
        self.target_hour = int(parts[0])
        self.target_minute = int(parts[1])
        
        check_time = datetime.now().replace(
            hour=self.target_hour, minute=self.target_minute, second=0, microsecond=0
        ) - timedelta(hours=self.HOURS_BEFORE_TARGET)
        self.check_hour = check_time.hour
        self.check_minute = check_time.minute
        
        log.info(f"Target updated: {target_time_str}, check={self.check_hour:02d}:{self.check_minute:02d}")
    
    def _set_state(self, state: str):
        """Update state and notify GUI"""
        if self.on_state_change:
            self.on_state_change(state)
    
    def _loop(self):
        """Main scheduler loop - MINIMAL LOGGING"""
        while self.running:
            try:
                now = datetime.now()
                today_str = str(date.today())
                
                # Check if already executed
                records = self.data_manager.all_records()
                already_done = any(r["date"] == today_str for r in records)
                
                if already_done:
                    self._set_state("WAIT_NEXT_DAY")
                    tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
                    sleep_seconds = (tomorrow - now).total_seconds()
                    time.sleep(min(sleep_seconds, 300))  # Wake every 5 min
                    continue
                
                # Calculate next check time
                check_today = now.replace(
                    hour=self.check_hour, minute=self.check_minute, second=0, microsecond=0
                )
                
                if now < check_today:
                    self._set_state("WAIT_FIRST_CHECK")
                    sleep_seconds = (check_today - now).total_seconds()
                    time.sleep(min(sleep_seconds, 300))
                    continue
                
                # Time to calculate/execute
                log.info("=== Daily execution starting ===")
                self._set_state("FETCH_SUN")
                
                try:
                    sunrise, sunset = self.weather_service.get_sun_times()
                except Exception as e:
                    log.error(f"Failed to fetch sun times: {e}")
                    time.sleep(600)  # Retry in 10 min
                    continue
                
                self._set_state("CALCULATING")
                try:
                    calc_result = self.weather_service.calculate_all(sunrise, sunset)
                    first_run = calc_result["first_run"]
                    second_run = calc_result["second_run"]
                except Exception as e:
                    log.error(f"Calculation failed: {e}")
                    time.sleep(600)
                    continue
                
                # Calculate trigger time (backwards)
                target_today = now.replace(
                    hour=self.target_hour, minute=self.target_minute, second=0, microsecond=0
                )
                trigger_time = target_today - timedelta(minutes=first_run)
                
                log.info(f"Target: {target_today.strftime('%H:%M')}, Trigger: {trigger_time.strftime('%H:%M')}")
                
                # Check if in past
                now = datetime.now()
                if trigger_time <= now:
                    log.warning("Trigger time is in past - executing immediately")
                    self._execute_now(sunrise, sunset, calc_result)
                    continue
                
                # Wait until trigger
                self._set_state("WAIT_TRIGGER")
                while self.running and datetime.now() < trigger_time:
                    time.sleep(min(60, (trigger_time - datetime.now()).total_seconds()))
                
                if not self.running:
                    break
                
                # Execute
                self._execute_now(sunrise, sunset, calc_result)
                
            except Exception as e:
                log.error(f"Scheduler error: {e}", exc_info=True)
                time.sleep(300)
    
    def _execute_now(self, sunrise, sunset, calc_result):
        """Execute boiler control"""
        self._set_state("EXECUTE")
        log.info("=== EXECUTING ===")
        
        try:
            # Send to HA
            ha_ok = self.ha_service.send_boiler_commands(
                calc_result["first_run"],
                calc_result["second_run"]
            )
            
            # Save to CSV
            row_data = {
                "date": str(date.today()),
                "dawn": sunrise.strftime("%H:%M"),
                "dusk": sunset.strftime("%H:%M"),
                "avg_temp": calc_result["avg_temp"],
                "avg_cloud": calc_result["avg_cloud"],
                "effective_temp": calc_result["effective_temp"],
                "duration": calc_result["duration"],
                "first_run": calc_result["first_run"],
                "second_run": calc_result["second_run"],
                "trigger_time": datetime.now().strftime("%H:%M:%S"),
                "ha_status": "OK" if ha_ok else "FAILED"
            }
            
            self.data_manager.save_record(row_data)
            log.info(f"=== COMPLETE: HA status={row_data['ha_status']} ===")
            
        except Exception as e:
            log.error(f"Execution failed: {e}", exc_info=True)
        
        self._set_state("WAIT_NEXT_DAY")
