# BoilerMind: install on the server (Docker)

Target: Debian VM `192.168.30.12`, user `guyd`, folder `~/pythons/dockers/Time_and_Sun_Boiler`.
Run every command in a terminal **on the VM**, one line at a time, in the folder named in each phase.

## Progress (updated only after a phase is confirmed on the VM)

| Phase | What | Status |
|---|---|---|
| 0 | Before you start | 🟢 Docker works. No old boiler container on this VM (only `MQTT_Cockpit`, a different app: leave it) |
| 1 | Get the code, right branch | 🟢 Done: on `fix/engine-safety`, Dockerfile is the new one (python:3.12-slim) |
| 2 | Secrets file `boiler.env` | 🔴 Missing in the install folder (an earlier `cp` ran in another folder): create it now |
| 3 | Carry over data from the old container | ⬜ Skipped for now: no old container found here. Fresh start |
| 4 | Build the image | 🟢 Done: `docker build -t boilermind .` finished (11/11 steps, 12.7 s) |
| 5 | Start it | ⬜ Not done |
| 6 | Check it is alive | ⬜ Not done |
| 7 | First-run setup in the browser | ⬜ Not done |

Key: 🟢 done and confirmed · 🟡 started or partly done · ⬜ not done

---

## Phase 0: Before you start

- Docker must work: `docker --version`.
- This VM's Docker has **no `docker compose`**. Use the plain `docker` commands below.
- Find the old container, so you can stop it later (two copies must never run: both would switch the boiler):
  ```
  docker ps --format '{{.Names}}  {{.Image}}'
  ```
  Write its name down. Called `<OLD>` below.

## Phase 1: Get the code, on the right branch

```
mkdir -p ~/pythons/dockers && cd ~/pythons/dockers
git clone -b fix/engine-safety https://github.com/guydvir2/Time_and_Sun_Boiler
cd Time_and_Sun_Boiler
```

Check it is the new web app, not the old desktop one:
```
git branch --show-current
head -3 Dockerfile
```
Expected: `fix/engine-safety`, and a Dockerfile starting `FROM python:3.12-slim` (no `python3-tk`).

Already have the folder? Switch it instead:
```
git fetch origin
git checkout fix/engine-safety
git pull
```

## Phase 2: Secrets file

Secrets live only in `boiler.env` (never in git, never in the app's settings).
```
cp boiler.env.example boiler.env
chmod 600 boiler.env
nano boiler.env
```
Set:
- `DASHBOARD_PASSWORD=` a long password (needed for Run/Stop and Settings)
- `MQTT_PASSWORD=` the **new** MQTT password
- `HA_TOKEN=` only if Home Assistant is used as a driver

Save in nano: Ctrl+O, Enter, Ctrl+X.

## Phase 3: Carry over data from the old container (skip for a fresh start)

Stop the old container first, then copy its files into `data/`:
```
OLD=<name from phase 0>
W=$(docker inspect -f '{{.Config.WorkingDir}}' $OLD); echo $W
docker stop $OLD
mkdir -p data
docker cp $OLD:$W/settings.json data/
docker cp $OLD:$W/state.json data/
docker cp $OLD:$W/stored_data data/
```
"no such file" on a line means that file lived elsewhere: note it in the troubleshooting table.

## Phase 4: Build the image

```
docker build -t boilermind .
```
Keep the dot at the end. About a minute or two.

## Phase 5: Start it

```
docker run -d --name boilermind --restart unless-stopped -p 8080:8080 --env-file boiler.env -e TZ=Asia/Jerusalem -v "$PWD/data:/data" boilermind
```
One long line. `data/` is where settings, state and history live, so updates never lose them.
Change `TZ` to your own time zone if it is not Asia/Jerusalem.

## Phase 6: Check it is alive

```
docker logs --tail 20 boilermind
curl http://localhost:8080/healthz
```
Expected: log lines with "BoilerMind", and `ok` from the second command.

## Phase 7: First-run setup in the browser

Open `http://192.168.30.12:8080`. A fresh install shows the setup wizard (password check, town, how the
boiler is switched, "hot by" time). With data copied in phase 3 it opens straight to the home screen.
Before trusting it with the real boiler: try a lamp on the plug, or "Test mode (switches nothing)" first.

---

## Update to a newer version

```
cd ~/pythons/dockers/Time_and_Sun_Boiler
git pull
docker build -t boilermind .
docker rm -f boilermind
```
then the phase 5 `docker run` line again. `data/` is untouched.

## Roll back to the old app

```
docker rm -f boilermind
docker start <OLD>
```

## Troubleshooting (things that already went wrong)

| What you see | Cause | Fix |
|---|---|---|
| `open boiler.env: no such file` / `grep: boiler.env: No such file` | `boiler.env` was created in another folder | Phase 2 in the folder you build from; check with `ls -a` |
| `unknown shorthand flag: 'd' in -d` | This Docker has no `compose` add-on | Use the plain `docker build` / `docker run` lines (or `sudo apt install docker-compose-v2`) |
| `open Dockerfile: no such file or directory` | Wrong folder, or an old checkout without the Dockerfile | `pwd`, `ls`; then phase 1 switch commands |
| Build shows `python3-tk` | Folder is on old branch `v_2.0` (desktop app) | `git checkout fix/engine-safety && git pull` |
| `port is already allocated` | Old container still on 8080 | `docker stop <OLD>` |
| Dashboard says setup needed after phase 3 | `settings.json` not copied | Repeat the `docker cp` lines, then `docker restart boilermind` |
| Run/Stop buttons locked | `DASHBOARD_PASSWORD` empty in `boiler.env` | Set it, then `docker rm -f boilermind` and the phase 5 line |

## Notes

- Repo moved: the address is now `github.com/guydvir2/Time_and_Sun_Boiler` (capital letters). The old one redirects.
- Every command above was checked against this repo's files. Commands marked in the Progress table as
  confirmed were run on the VM; the rest are still untested there.
