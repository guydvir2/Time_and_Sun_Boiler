# BoilerMind (Time & Sun Boiler)

Decides each day how long the boiler should run, from the day's temperature and cloud cover,
and tells a driver (MQTT/Tasmota, Home Assistant, or dry-run) to run it.

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
