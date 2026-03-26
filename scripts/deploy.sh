#!/usr/bin/env bash
set -euo pipefail

# Simple deploy helper for VPS. Run from the repo root or via SSH.
# Usage: ./scripts/deploy.sh [service_name] [branch]

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_DIR"

SERVICE_NAME="${1:-trading-bot}"
BRANCH="${2:-main}"

echo "Deploy: service=$SERVICE_NAME branch=$BRANCH repo=$REPO_DIR"

# fetch and reset to remote branch
git fetch origin "$BRANCH"
git reset --hard "origin/$BRANCH"

# create venv if missing
if [ ! -d .venv ]; then
  python3 -m venv .venv
fi
. .venv/bin/activate
python -m pip install --upgrade pip
if [ -f requirements.txt ]; then
  pip install -r requirements.txt
fi

# If a systemd unit exists in deploy/, install and restart it
if [ -f deploy/${SERVICE_NAME}.service ]; then
  echo "Installing systemd unit: deploy/${SERVICE_NAME}.service -> /etc/systemd/system/${SERVICE_NAME}.service"
  sudo cp deploy/${SERVICE_NAME}.service /etc/systemd/system/${SERVICE_NAME}.service
  sudo systemctl daemon-reload
  sudo systemctl enable ${SERVICE_NAME}.service
  sudo systemctl restart ${SERVICE_NAME}.service
  sudo journalctl -u ${SERVICE_NAME}.service -n 200 --no-pager
else
  echo "No systemd unit found for ${SERVICE_NAME} in deploy/. Skipping systemd install." >&2
fi

echo "Deploy complete."
