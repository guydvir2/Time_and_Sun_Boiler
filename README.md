# BoilerMind (Time & Sun Boiler)

Decides each day how long the boiler should run, from the day's temperature and cloud cover,
and tells a driver (MQTT/Tasmota, Home Assistant, or dry-run) to run it.

How the day works:
- Every hour it re-estimates today's minutes: measured weather for the hours already past, the forecast
  for the rest. The number locks once the sun window is over (around sunset); after that it is final.
- The minutes are split into run 1 (`RUN1_PERCENT`, timed to be hot by the Schedule's "hot by") and
  run 2 (the rest, at `RUN2_AT`, never before the number is final). If run 1 had to start on an estimate
  that later went up, run 2 tops up the difference. No run is longer than "max on per run".
- `UPDATE_AFTER` is no longer used.

## Run

```
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python main.py          # engine + dashboard
```

Open `http://<this-machine>:8080`. On a fresh install a **setup wizard** opens: create a password, pick your town,
choose how the boiler is switched (Tasmota over MQTT, Home Assistant, or test mode), and the "hot by" time.
Everything can be changed later on the **Settings** tab (saved to `settings.json`, git-ignored, applied live).

Tabs: **Home** (status, today's minutes, Run/Stop, pause, cloud boost, services) · **Schedule** ("hot by" time and
weekly presets) · **History** (charts, weather, activity) · **Settings** · **Tune** (formula sliders).

`dashboard.py` runs the page alone, view-only (it never switches anything).

## Power sensor (current sensor on the plug)

If the Tasmota plug has an energy sensor, the app listens to it (same MQTT topic, `[power]` in config.ini).
Home shows *Heating now · 1980 W* and the measured minutes / kWh today; every heating session is logged to
`stored_data/power.jsonl`. "On, no draw" means the relay is on but the element's thermostat has cut off.
Listen and log only: it changes no decision.

## Tune the formula

`http://<this-machine>:8080/tune`: move sliders and see what every saved day would have got.
Nothing changes until **Save** (password). Saved to `stored_data/formula.json`; every change is kept in
`formula_history.jsonl`. **Clouds vs real sunshine** downloads measured sunshine (Open-Meteo archive) to
check that cloud cover is a fair stand-in for sun.

## History from saved weather

`python backfill.py` fills the history chart from the weather files already in `stored_data/`
(what the current formula would have decided). Those days show faded, marked "replayed". Safe to repeat.

## Config

- `config.ini`: settings (tracked). Pick drivers under `[output] drivers = ...`
  (`dryrun`, `mqtt_tasmota`, `homeassistant`; a list means "next one only if the previous failed").
- `config.local.ini` (git-ignored) or env vars: secrets (`HA_TOKEN`, `MQTT_PASSWORD`).
  See `config.local.ini.example`.

## Safe first runs

1. `drivers = dryrun`: does everything except switch anything.
2. `mqtt_tasmota` with a lamp on the Tasmota plug.
3. The real boiler.
