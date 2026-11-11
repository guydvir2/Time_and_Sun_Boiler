#!/usr/bin/env bash
# One-time install of the BoilerMind host updater (the service behind the app's "Update now" button).
# Run it on the VM, in the app folder:   updater/install.sh      (asks for your sudo password once)
set -euo pipefail
cd "$(dirname "$(readlink -f "$0")")/.."
APP_DIR="$PWD"; ME="$(id -un)"
[ -f boiler.env ] || { echo "🔴 boiler.env not found in $APP_DIR" >&2; exit 1; }

if ! grep -q '^UPDATER_TOKEN=.\+' boiler.env; then
    printf '\nUPDATER_TOKEN=%s\n' "$(python3 -c 'import secrets; print(secrets.token_hex(24))')" >> boiler.env
    echo "🟢 Created UPDATER_TOKEN in boiler.env"
fi
BIND="$(docker network inspect bridge -f '{{(index .IPAM.Config 0).Gateway}}')"
echo "🟢 Docker bridge address: $BIND (the updater listens only there, not on your LAN)"

sudo tee /etc/systemd/system/boilermind-updater.service >/dev/null <<UNIT
[Unit]
Description=BoilerMind host updater
After=docker.service
Requires=docker.service

[Service]
User=$ME
WorkingDirectory=$APP_DIR
Environment=UPDATER_BIND=$BIND
ExecStart=/usr/bin/python3 $APP_DIR/updater/boiler_updater.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
UNIT
sudo systemctl daemon-reload
sudo systemctl enable boilermind-updater >/dev/null 2>&1
sudo systemctl restart boilermind-updater
sleep 1
systemctl is-active --quiet boilermind-updater && echo "🟢 Updater service running" || { echo "🔴 Updater failed to start: journalctl -u boilermind-updater"; exit 1; }

echo "🟡 Recreating the app so it gets the token (deploy.sh)"
exec ./deploy.sh
