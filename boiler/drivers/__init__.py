"""Driver registry. Add a new driver = one file + one line here."""
from .dryrun import DryRunDriver
from .homeassistant import HomeAssistantDriver
from .mqtt_tasmota import TasmotaMqttDriver

REGISTRY = {
    "dryrun": DryRunDriver,
    "homeassistant": HomeAssistantDriver,
    "mqtt_tasmota": TasmotaMqttDriver,
}


def build_drivers(s):
    """Instantiate the configured drivers in order. Returns (drivers, problems)."""
    drivers, problems = [], []
    for name in s.driver_names:
        cls = REGISTRY.get(name)
        if cls is None:
            problems.append(f"Unknown driver '{name}' (available: {', '.join(REGISTRY)})")
            continue
        d = cls() if cls is DryRunDriver else cls(s)
        issues = d.problems()
        if issues:
            problems.extend(issues)
        else:
            drivers.append(d)
    return drivers, problems


PULSE_MARGIN_MIN = 2         # backup timeout for a non-Tasmota run = run + 2 min (the normal driver ends first)
PULSE_CHECK_AFTER_MIN = 1    # verify PulseTime == 0 this long after the run (and its margin) should be over


def tasmota_if_defined(s):
    """A Tasmota helper when MQTT + a Tasmota topic are configured (even if it is not a selected driver)."""
    if s is None:
        return None
    t = TasmotaMqttDriver(s)
    return t if not t.problems() else None


def start_boiler(drivers, minutes, s=None):
    """Try drivers in order; the next one is used only if the previous one failed.
    With MQTT defined, the Tasmota timeout is used for EVERY real driver (Home Assistant too), and a check
    that PulseTime is back to 0 is booked for after the run (the engine does it, see service.py).
    Returns (ok, driver_name_that_worked_or_None)."""
    tas = tasmota_if_defined(s)
    for d in drivers:
        print(f"▶️  Driver '{d.name}': start {minutes} min")
        if tas and d.name != "dryrun":
            from datetime import datetime, timedelta
            from ..state import mark_pulse_check
            extra = 0 if d.name == "mqtt_tasmota" else PULSE_MARGIN_MIN
            if extra and not tas.arm(minutes + extra):
                print("⚠️  Tasmota backup timeout could not be set — the run goes ahead without it")
            mark_pulse_check(s, datetime.now() + timedelta(minutes=minutes + extra + PULSE_CHECK_AFTER_MIN))
        if d.start(minutes):
            return True, d.name
        print(f"⚠️  Driver '{d.name}' failed" + (" — trying next" if d is not drivers[-1] else ""))
    return False, None
