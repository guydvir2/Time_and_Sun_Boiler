#!/usr/bin/env bash
# Update BoilerMind on the VM: back up, pull, rebuild, recreate the container. Plain docker (no compose).
#
#   ./deploy.sh              # update the branch that is checked out now
#   ./deploy.sh <branch>     # switch to <branch> first
#
# Env overrides: NAME=boilermind  PORT=8080  TZ_NAME=Asia/Jerusalem  BACKUP_DIR=~/boiler-backups  KEEP=10
set -euo pipefail

cd "$(dirname "$(readlink -f "$0")")"
NAME="${NAME:-boilermind}"
PORT="${PORT:-8080}"
TZ_NAME="${TZ_NAME:-Asia/Jerusalem}"
BACKUP_DIR="${BACKUP_DIR:-$HOME/boiler-backups}"
KEEP="${KEEP:-10}"
STAMP="$(date +%F_%H%M%S)"

[ -f boiler.env ] || { echo "🔴 boiler.env not found here (secrets file). Run from the app folder." >&2; exit 1; }
[ -d data ] || { echo "🔴 data/ not found here. Run from the app folder." >&2; exit 1; }

echo "🟡 1/4  Backing up data/ and boiler.env"
mkdir -p "$BACKUP_DIR"
BACKUP="$BACKUP_DIR/boiler-$STAMP.tgz"
( umask 077; tar czf "$BACKUP" data boiler.env )            # holds secrets: owner-only
echo "🟢 Backup: $BACKUP ($(du -h "$BACKUP" | cut -f1))"

echo "🟡 2/4  Updating the code"
git fetch origin
[ -n "${1:-}" ] && git checkout "$1"
git pull --ff-only origin "$(git rev-parse --abbrev-ref HEAD)"
echo "🟢 Now on $(git rev-parse --abbrev-ref HEAD) @ $(git rev-parse --short HEAD)"

echo "🟡 3/4  Rebuilding the image"
docker build -t "$NAME" .

echo "🟡 4/4  Recreating the container (data/ is mounted, nothing is lost)"
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
