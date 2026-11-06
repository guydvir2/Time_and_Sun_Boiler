# Time & Sun Boiler

Decides each day how long the boiler should run, from the day's temperature and cloud cover,
and tells a driver (MQTT/Tasmota, Home Assistant, or dry-run) to run it.

## Run

```
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python main.py          # job + dashboard
.venv/bin/python dashboard.py      # dashboard only (view history, nothing is switched)
```

Dashboard: `http://<this-machine>:8080` (read-only, shows no secrets).

## Config

- `config.ini`: settings (tracked). Pick drivers under `[output] drivers = ...`
  (`dryrun`, `mqtt_tasmota`, `homeassistant`; a list means "next one only if the previous failed").
- `config.local.ini` (git-ignored) or env vars: secrets (`HA_TOKEN`, `MQTT_PASSWORD`).
  See `config.local.ini.example`.

## Safe first runs

1. `drivers = dryrun`: does everything except switch anything.
2. `mqtt_tasmota` with a lamp on the Tasmota plug.
3. The real boiler.
