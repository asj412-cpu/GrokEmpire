# Grok Empire - Kalshi Trading Agents

Grok-powered trading agents for Kalshi prediction markets.

## Agents (Paper Trading Active - DRY_RUN=true)
- `crypto_15m_agent.py` → 15m crypto binaries (BTC momentum/value signals)
  - Single cycle: `python one_time_15m.py`
  - Continuous: `python agents/trading/crypto_15m_agent.py`
  - Shell: `./run_15m_bot.sh`
- `kalshi_daily_grok_agent.py` → Daily batch trades on short-settle markets (Grok-powered)
  - Single cycle: `python one_time.py`
  - Continuous: `python agents/trading/kalshi_daily_grok_agent.py`
  - Shell: `./run_kalshi_bot.sh`

## Setup
1. Set API keys:
   - `export GROK_API_KEY=...`
   - `export KALSHI_KEY_ID=...`
   - `export KALSHI_PRIVATE_KEY=...`
   - `export DAILY_RISK_PCT=0.25` (optional, default 0.25)
2. Install deps: `pip install -r requirements.txt`
3. Run agents (paper trading - check CSVs):
   - Daily: `python one_time.py` or `./run_kalshi_bot.sh`
   - 15m: `python one_time_15m.py` or `./run_15m_bot.sh`

## Files
- `daily_trades.csv` → Trade log with Grok rationales
- `tools/data_sources.py` → Market enrichment funcs

# VPS Deployment Pipeline

## Prerequisites
- SSH key: `~/.ssh/trading-vps` for `trader@45.76.6.184`
- Repo cloned on VPS: `/home/trader/grok-empire`
- Python 3.12+ on VPS

## Local Setup (Mac)
```bash
cd /Users/andrewjohnson/GrokEmpire
git init  # if not already a repo
git add .
git commit -m "Initial GrokEmpire commit"
git remote add origin git@github.com:yourusername/GrokEmpire.git  # create remote repo first
git push -u origin main
```

## VPS Setup (one-time)
```bash
ssh -i ~/.ssh/trading-vps trader@45.76.6.184
cd /home/trader
git clone git@github.com:yourusername/GrokEmpire.git grok-empire  # or git pull if exists
cd grok-empire
./scripts/deploy.sh trading-bot main
```

## Deploy Workflow
1. Edit code locally, commit/push to GitHub.
2. On VPS: `cd /home/trader/grok-empire && ./scripts/deploy.sh trading-bot main`
   - Pulls latest, installs deps, restarts systemd service.
3. Check logs: `sudo journalctl -u trading-bot -f`

## Environment Vars (VPS)
Set in `~/.bashrc` or systemd `EnvironmentFile`:
```
export KALSHI_KEY_ID="f462dd1a-f78a-4366-a4bd-8f798b690542"
export KALSHI_PRIVATE_KEY="/home/trader/trading-assistant/server/key_pkcs8.pem"
```

## Service Management
```
sudo systemctl status trading-bot
sudo systemctl restart trading-bot
sudo journalctl -u trading-bot -f
```
