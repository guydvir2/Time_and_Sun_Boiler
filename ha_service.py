"""
Home Assistant Service
Handles all interactions with Home Assistant API
"""

import logging
import time
import requests

log = logging.getLogger(__name__)


def with_retries(fn, retries=3, delay=5, label="operation"):
    """Retry helper for network operations"""
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
    """Home Assistant API client"""
    
    def __init__(self, ha_url, headers, boiler_1st_entity, boiler_2nd_entity, script_entity):
        self.ha_url = ha_url
        self.headers = headers
        self.boiler_1st_on_entity_id = boiler_1st_entity
        self.boiler_2nd_on_entity_id = boiler_2nd_entity
        self.run_script_entity_id = script_entity
    
    def check_reachable(self):
        """Test HA connectivity"""
        r = requests.get(f"{self.ha_url}/api/", headers=self.headers, timeout=5)
        r.raise_for_status()
        log.info("HA is reachable")
    
    def set_slider(self, entity_id, value):
        """Set input_number value"""
        url = f"{self.ha_url}/api/services/input_number/set_value"
        payload = {"entity_id": entity_id, "value": value}
        r = requests.post(url, headers=self.headers, json=payload, timeout=10)
        r.raise_for_status()
        log.info(f"Set {entity_id} = {value} min")
    
    def run_script(self, script_name):
        """Trigger a script"""
        entity_id = f"script.{script_name}"
        url = f"{self.ha_url}/api/services/script/turn_on"
        payload = {"entity_id": entity_id}
        r = requests.post(url, headers=self.headers, json=payload, timeout=10)
        r.raise_for_status()
        log.info(f"Triggered {entity_id}")
    
    def send_boiler_commands(self, first_run, second_run):
        """Send all boiler control commands to HA"""
        log.info(f"Sending to HA: first_run={first_run}min, second_run={second_run}min")
        
        try:
            with_retries(lambda: self.check_reachable(), label="HA health-check")
            with_retries(lambda: self.set_slider(self.boiler_1st_on_entity_id, first_run), label="set slider1")
            with_retries(lambda: self.set_slider(self.boiler_2nd_on_entity_id, second_run), label="set slider2")
            with_retries(lambda: self.run_script(self.run_script_entity_id), label="run script")
            log.info("HA execution complete")
            return True
            
        except requests.exceptions.Timeout:
            log.error("HA request timed out")
        except requests.exceptions.ConnectionError:
            log.error(f"Could not connect to HA at {self.ha_url}")
        except requests.exceptions.HTTPError as e:
            log.error(f"HA returned HTTP {e.response.status_code}: {e.response.text}")
        except requests.exceptions.RequestException as e:
            log.error(f"Unexpected HA error: {e}")
        
        return False
