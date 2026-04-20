"""
Home Assistant Service
"""
import logging
import time
import requests

log = logging.getLogger(__name__)


def with_retries(fn, retries=3, delay=5, label="operation"):
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except Exception as e:
            if attempt == retries:
                log.error(f"{label} failed after {retries} attempts: {e}")
                raise
            log.warning(f"{label} attempt {attempt}/{retries} failed: {e}. Retrying in {delay}s...")
            time.sleep(delay)


class HAService:
    def __init__(self, ha_url, headers, boiler_1st_entity, script_1st_entity, script_2nd_entity):
        self.ha_url = ha_url
        self.headers = headers
        self.boiler_1st_on_entity_id = boiler_1st_entity
        self.run_script_1st_entity_id = script_1st_entity
        self.run_script_2nd_entity_id = script_2nd_entity

        # Fired after every HA operation: callback(ok: bool)
        self.on_reachability_change = None

    def check_reachable(self):
        r = requests.get(f"{self.ha_url}/api/", headers=self.headers, timeout=5)
        r.raise_for_status()

    def _set_slider(self, entity_id, value):
        r = requests.post(
            f"{self.ha_url}/api/services/input_number/set_value",
            headers=self.headers,
            json={"entity_id": entity_id, "value": value},
            timeout=10
        )
        r.raise_for_status()
        log.info(f"Set {entity_id} = {value} min")

    def _run_script(self, script_name):
        r = requests.post(
            f"{self.ha_url}/api/services/script/turn_on",
            headers=self.headers,
            json={"entity_id": f"script.{script_name}"},
            timeout=10
        )
        r.raise_for_status()
        log.info(f"Triggered script.{script_name}")

    def _notify(self, ok: bool):
        if self.on_reachability_change:
            self.on_reachability_change(ok)

    def send_first_run(self, duration: int) -> bool:
        log.info(f"HA 1st run: {duration}min")
        try:
            with_retries(self.check_reachable, label="HA ping")
            with_retries(lambda: self._set_slider(self.boiler_1st_on_entity_id, duration), label="slider1")
            with_retries(lambda: self._run_script(self.run_script_1st_entity_id), label="script1")
            log.info("HA 1st run complete")
            self._notify(True)
            return True
        except Exception as e:
            log.error(f"HA 1st run failed: {e}")
            self._notify(False)
            return False

    def send_second_run(self, duration: int) -> bool:
        log.info(f"HA 2nd run: {duration}min")
        try:
            with_retries(self.check_reachable, label="HA ping")
            with_retries(lambda: self._run_script(self.run_script_2nd_entity_id), label="script2")
            log.info("HA 2nd run complete")
            return True
        except Exception as e:
            log.error(f"HA 2nd run failed: {e}")
            return False

    def get_boiler_state(self, entity_id: str) -> str:
        try:
            r = requests.get(
                f"{self.ha_url}/api/states/{entity_id}",
                headers=self.headers, timeout=5
            )
            r.raise_for_status()
            return r.json().get("state", "unknown")
        except Exception as e:
            log.error(f"Failed to get boiler state: {e}")
            return "unknown"
