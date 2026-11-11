"""One-time move to 'secrets only in the environment'. Run it on the server BEFORE switching branches.

    python3 migrate_secrets.py            # writes boiler.env (owner-only) + keeps your location/addresses in settings.json
    python3 migrate_secrets.py --clean    # same, then removes the secrets from the old files

It looks for secrets in config.local.ini, config.ini and settings.json. It never prints them.
"""
import configparser
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SECRETS = {("homeassistant", "token"): "HA_TOKEN", ("mqtt", "password"): "MQTT_PASSWORD",
           ("dashboard", "password"): "DASHBOARD_PASSWORD"}
# personal values that used to live in config.ini (now blank there) -> keep them in settings.json
PERSONAL = [("location", "latitude"), ("location", "longitude"), ("homeassistant", "ip"), ("homeassistant", "port"),
            ("output", "drivers")]


def main(clean):
    ini = configparser.ConfigParser(interpolation=None)
    ini.read([os.path.join(HERE, "config.ini"), os.path.join(HERE, "config.local.ini")])
    sj_path = os.path.join(HERE, "settings.json")
    try:
        with open(sj_path) as f:
            sj = json.load(f)
    except (FileNotFoundError, ValueError):
        sj = {}
    env_path = os.path.join(HERE, "boiler.env")
    have = {}
    if os.path.exists(env_path):
        for ln in open(env_path).read().splitlines():
            if "=" in ln and not ln.lstrip().startswith("#"):
                k, v = ln.split("=", 1)
                have[k.strip()] = v
    found = {}
    for (sec, key), name in SECRETS.items():
        v = (sj.get(sec, {}) or {}).get(key) or ini.get(sec, key, fallback="").strip()
        if v and name not in have:
            found[name] = v
    if found:
        have.update(found)
        fd = os.open(env_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as f:
            f.write("# written by migrate_secrets.py. Keep it owner-only, never commit it.\n")
            for k, v in have.items():
                f.write(f"{k}={v}\n")
        print(f"boiler.env written (owner-only) with: {', '.join(found)}")
    else:
        print("No new secrets to move.")
    kept = []
    for sec, key in PERSONAL:
        v = ini.get(sec, key, fallback="").strip()
        if v and key not in (sj.get(sec, {}) or {}):
            sj.setdefault(sec, {})[key] = v
            kept.append(f"{sec}.{key}")
    if clean:
        for (sec, key) in SECRETS:
            (sj.get(sec) or {}).pop(key, None)
    if kept or clean:
        tmp = sj_path + ".tmp"
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as f:
            json.dump(sj, f, indent=1)
        os.replace(tmp, sj_path)
        print(f"settings.json updated; kept your values: {', '.join(kept) or 'none new'}")
    if clean:
        lp = os.path.join(HERE, "config.local.ini")
        if os.path.exists(lp):
            os.remove(lp)
            print("config.local.ini removed (it is retired).")
        print("NOTE: a secret in config.ini itself (if any) must be removed by hand; the new config.ini has none.")
    print("Next: point the service at boiler.env (docker --env-file / systemd EnvironmentFile) and restart.")


if __name__ == "__main__":
    main("--clean" in sys.argv)
