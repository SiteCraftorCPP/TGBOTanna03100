#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/opt/tgbotanna03100"
SERVICE_NAME="tgbotanna03100.service"

if [[ ! -f "$APP_DIR/.env" ]]; then
  echo "Missing $APP_DIR/.env. Create it before deploy."
  exit 1
fi

cd "$APP_DIR"
git pull --ff-only

python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

sudo cp deploy/systemd/tgbotanna03100.service /etc/systemd/system/$SERVICE_NAME
sudo systemctl daemon-reload
sudo systemctl enable $SERVICE_NAME
sudo systemctl restart $SERVICE_NAME
sudo systemctl status $SERVICE_NAME --no-pager
