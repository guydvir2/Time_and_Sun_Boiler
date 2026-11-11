#!/usr/bin/env bash
# ONE command updates BoilerMind on the VM: pull the newest code (this script included), back up,
# rebuild, recreate the container, show the result. Plain docker (no compose). Run it from anywhere:
#
#   ~/pythons/dockers/Time_and_Sun_Boiler/deploy.sh              # update the branch checked out now
#   ~/pythons/dockers/Time_and_Sun_Boiler/deploy.sh <branch>     # switch to <branch> first
#
# Env overrides: NAME=boilermind  PORT=8080  TZ_NAME=Asia/Jerusalem  BACKUP_DIR=~/boiler-backups  KEEP=10
set -euo pipefail

SELF="$(readlink -f "$0")"
cd "$(dirname "$SELF")"
NAME="${NAME:-boilermind}"
PORT="${PORT:-8080}"
TZ_NAME="${TZ_NAME:-Asia/Jerusalem}"
BACKUP_DIR="${BACKUP_DIR:-$HOME/boiler-backups}"
KEEP="${KEEP:-10}"
STAMP="$(date +%F_%H%M%S)"

if [ -z "${DEPLOY_REEXEC:-}" ]; then            # step 1: get the newest code, then re-run the (maybe new) script
    echo "🟡 Updating the code"
    git fetch origin
    [ -n "${1:-}" ] && git checkout "$1"
    git pull --ff-only origin "$(git rev-parse --abbrev-ref HEAD)"
    echo "🟢 Now on $(git rev-parse --abbrev-ref HEAD) @ $(git rev-parse --short HEAD)"
    DEPLOY_REEXEC=1 exec "$SELF" "$@"
fi

[ -f boiler.env ] || { echo "🔴 boiler.env not found here (secrets file). Run from the app folder." >&2; exit 1; }
[ -d data ] || { echo "🔴 data/ not found here. Run from the app folder." >&2; exit 1; }

echo "🟡 1/3  Backing up data/ and boiler.env"
mkdir -p "$BACKUP_DIR"
BACKUP="$BACKUP_DIR/boiler-$STAMP.tgz"
# the container runs as root, so files it wrote in data/ can be root-owned: fall back to sudo for those
if ! ( umask 077; tar czf "$BACKUP" data boiler.env ) 2>/dev/null; then
    echo "🟡 Some files in data/ are owned by root (written by the container): using sudo for the backup"
    rm -f "$BACKUP"
    sudo tar czf "$BACKUP" data boiler.env
    sudo chown "$(id -u):$(id -g)" "$BACKUP" && chmod 600 "$BACKUP"
fi
echo "🟢 Backup: $BACKUP ($(du -h "$BACKUP" | cut -f1))"

echo "🟡 2/3  Rebuilding the image"
docker build -t "$NAME" .

echo "🟡 3/3  Recreating the container (data/ is mounted, nothing is lost)"
docker rm -f "$NAME" >/dev/null 2>&1 || true
docker run -d --name "$NAME" --restart unless-stopped -p "$PORT:8080" --env-file boiler.env \
    -e TZ="$TZ_NAME" -v "$PWD/data:/data" "$NAME" >/dev/null
sleep 6

ls -1t "$BACKUP_DIR"/boiler-*.tgz 2>/dev/null | tail -n +$((KEEP + 1)) | xargs -r rm -f

if [ "$(docker inspect -f '{{.State.Running}}' "$NAME" 2>/dev/null)" = "true" ]; then
    echo "🟢 Running. Last log lines:"; docker logs --tail 15 "$NAME"
    echo "   Dashboard: http://$(hostname -I | awk '{print $1}'):$PORT  (Ctrl+F5 in the browser)"
else
    echo "🔴 The container is NOT running. Logs:"; docker logs --tail 40 "$NAME" 2>&1 || true
    echo "   Your backup is safe: $BACKUP"; exit 1
fi
