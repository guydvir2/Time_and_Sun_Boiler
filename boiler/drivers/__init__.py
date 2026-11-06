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


def start_boiler(drivers, minutes):
    """Try drivers in order; the next one is used only if the previous one failed.
    Returns (ok, driver_name_that_worked_or_None)."""
    for d in drivers:
        print(f"▶️  Driver '{d.name}': start {minutes} min")
        if d.start(minutes):
            return True, d.name
        print(f"⚠️  Driver '{d.name}' failed" + (" — trying next" if d is not drivers[-1] else ""))
    return False, None
