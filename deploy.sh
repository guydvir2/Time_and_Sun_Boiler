#!/usr/bin/env bash
# Deploy BoilerMind on the VM: back up the data, update the code, rebuild the image, restart.
#
#   ./deploy.sh              # update the branch that is checked out now
#   ./deploy.sh <branch>     # switch to <branch> first
#
# Settings (override with env vars if your VM differs):
#   CONTAINER=time-and-sun-boiler   IMAGE=time-sun-boiler:latest   RUN_SCRIPT=./run_this.sh
#   APP_IN_CONTAINER=/app           BACKUP_DIR=~/boiler-backups    KEEP=10
set -euo pipefail

cd "$(dirname "$(readlink -f "$0")")"
CONTAINER="${CONTAINER:-time-and-sun-boiler}"
IMAGE="${IMAGE:-time-sun-boiler:latest}"
RUN_SCRIPT="${RUN_SCRIPT:-./run_this.sh}"
APP_IN_CONTAINER="${APP_IN_CONTAINER:-/app}"
BACKUP_DIR="${BACKUP_DIR:-$HOME/boiler-backups}"
KEEP="${KEEP:-10}"
DATA=(settings.json config.local.ini state.json stored_data)
STAMP="$(date +%F_%H%M%S)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

running() { [ -n "$(docker ps -q -f "name=^/${CONTAINER}$")" ]; }

echo "🟡 1/4  Backing up data"
mkdir -p "$BACKUP_DIR" "$WORK/host" "$WORK/container"
for f in "${DATA[@]}"; do [ -e "$f" ] && cp -a "$f" "$WORK/host/"; done
if running; then                       # data that lives only inside the container is copied out too
    for f in "${DATA[@]}"; do docker cp "$CONTAINER:$APP_IN_CONTAINER/$f" "$WORK/container/" 2>/dev/null || true; done
fi
if [ -z "$(ls -A "$WORK/host" "$WORK/container" 2>/dev/null)" ]; then
    echo "🔴 Nothing found to back up (no data in this folder or in the running container). Stopping." >&2
    echo "   If this is a first install, run with FIRST_INSTALL=1." >&2
    [ "${FIRST_INSTALL:-0}" = "1" ] || exit 1
fi
BACKUP="$BACKUP_DIR/boiler-$STAMP.tgz"
tar czf "$BACKUP" -C "$WORK" host container
echo "🟢 Backup: $BACKUP ($(du -h "$BACKUP" | cut -f1))"

echo "🟡 2/4  Updating the code"
git fetch origin
[ -n "${1:-}" ] && git checkout "$1"
git pull --ff-only origin "$(git rev-parse --abbrev-ref HEAD)"
echo "🟢 Now on $(git rev-parse --abbrev-ref HEAD) @ $(git rev-parse --short HEAD)"

echo "🟡 3/4  Rebuilding the image"
docker build -t "$IMAGE" .

echo "🟡 4/4  Restarting"
running && docker stop "$CONTAINER" >/dev/null
"$RUN_SCRIPT"
sleep 5

# The container is started with --rm and may have no data volume: then the new one starts empty.
# Put the saved data back in that case, then restart so the app reads it.
if running && ! docker inspect -f '{{range .Mounts}}{{.Destination}} {{end}}' "$CONTAINER" | grep -q "$APP_IN_CONTAINER"; then
    if [ -n "$(ls -A "$WORK/container" 2>/dev/null)" ]; then
        echo "🟡 No data volume: restoring the data into the new container"
        for f in "${DATA[@]}"; do
            [ -e "$WORK/container/$f" ] && docker cp "$WORK/container/$f" "$CONTAINER:$APP_IN_CONTAINER/"
        done
        docker restart "$CONTAINER" >/dev/null
        sleep 5
    fi
    echo "🟡 Tip: mount the data folders in run_this.sh (-v) so they survive a restart."
fi

# keep the newest $KEEP backups
ls -1t "$BACKUP_DIR"/boiler-*.tgz 2>/dev/null | tail -n +$((KEEP + 1)) | xargs -r rm -f

if running; then
    echo "🟢 Running. Last log lines:"; docker logs --tail 15 "$CONTAINER"
else
    echo "🔴 The container is NOT running. Logs:"; docker logs --tail 40 "$CONTAINER" 2>&1 || true
    echo "   Your backup is safe: $BACKUP"; exit 1
fi
