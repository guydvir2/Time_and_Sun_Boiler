"""Home Assistant driver (the original behaviour).

HA's input_number slider caps at max_per_boiler minutes, so a longer run is split over two
sliders/scripts. The 2nd script is only started when there is overflow.
"""
import requests
from time import sleep

from .base import Driver


class HomeAssistantDriver(Driver):
    name = "homeassistant"

    def __init__(self, s):
        self.s = s
        self.base = f"http://{s.ha_ip}:{s.ha_port}/api/services"
        self.headers = {"Authorization": f"Bearer {s.ha_token}", "Content-Type": "application/json"}

    def problems(self):
        s, p = self.s, []
        if not s.ha_token:
            p.append("Home Assistant: no token (set env HA_TOKEN or [homeassistant] token in config.local.ini)")
        if not s.ha_ip:
            p.append("Home Assistant: no ip in config")
        for label, val in (("BOILER_1ST_ON_ENTITY_ID", s.ha_slider_1), ("BOILER_2ND_ON_ENTITY_ID", s.ha_slider_2),
                           ("RUN_SCRIPT_1ST_START_BOILER_ENTITY_ID", s.ha_script_1),
                           ("RUN_SCRIPT_2ND_START_BOILER_ENTITY_ID", s.ha_script_2)):
            if not val:
                p.append(f"Home Assistant: {label} missing in config")
        return p

    def _post(self, path, payload, what):
        s = self.s
        for attempt in range(1, s.ha_retries + 1):
            try:
                r = requests.post(f"{self.base}/{path}", headers=self.headers, json=payload,
                                  timeout=s.request_timeout)
                if r.ok:
                    print(f"✅ HA: {what} (attempt {attempt})")
                    return True
                print(f"❌ HA: {what} failed (attempt {attempt}): {r.status_code} {r.text}")
                if r.status_code in (401, 403, 404):
                    return False                      # retrying will not help
            except Exception as e:
                print(f"❌ HA: {what} error (attempt {attempt}): {e}")
            if attempt < s.ha_retries:
                sleep(s.ha_retry_delay)
        return False

    def _set_slider(self, entity_id, value):
        value = max(0, min(int(value), self.s.ha_max_per_boiler))
        return self._post("input_number/set_value", {"entity_id": entity_id, "value": value},
                          f"slider {entity_id} -> {value}")

    def _run_script(self, name):
        return self._post("script/turn_on", {"entity_id": f"script.{name}"}, f"script {name}")

    def start(self, minutes: int) -> bool:
        s = self.s
        first = min(minutes, s.ha_max_per_boiler)
        rest = minutes - first
        print(f"🏠 HA: boiler 1 = {first} min, boiler 2 = {rest} min")
        ok = self._set_slider(s.ha_slider_1, first) and self._set_slider(s.ha_slider_2, rest)
        ok = ok and self._run_script(s.ha_script_1)
        if ok and rest > 0:
            ok = self._run_script(s.ha_script_2)
        return ok
