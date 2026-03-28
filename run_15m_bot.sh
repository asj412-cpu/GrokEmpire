#!/bin/bash
cd /home/trader/grok-empire

# Load .env properly
set -a
source .env 2>/dev/null || true
set +a

# Activate venv
source .venv/bin/activate

# Export path
export PYTHONPATH=/home/trader/grok-empire:$PYTHONPATH

echo "[$(date)] 15M BOT START - XAI_API_KEY loaded: ${XAI_API_KEY:0:20}..."

# Run the agent (single cycle)
python3 one_time_15m.py