"""
Test tiered buy window strategy vs current:
- Tiered: min 0-7 buy 5-15c, min 7-10 buy 5-35c
- Current: min 0-5 buy 5-35c

Both: 6/10 fade, hold to settlement.
"""
import csv, requests, time
from collections import defaultdict
from datetime import datetime, timezone

KALSHI_CSV = "/tmp/kalshi_8cycles.csv"
COINS = ["BTC","ETH","SOL","XRP","BNB","HYPE","DOGE"]
API = "https://api.elections.kalshi.com/trade-api/v2"

print("Loading data...")
kalshi = []
with open(KALSHI_CSV) as f:
    for r in csv.DictReader(f):
        try:
            r['ts_ms'] = int(r['ts_ms'])
            r['yes_ask'] = int(r['yes_ask'])
            r['cycle_sec'] = int(r['cycle_sec'])
            kalshi.append(r)
        except: pass

by_ticker = defaultdict(list)
for k in kalshi:
    by_ticker[k['ticker']].append(k)

# Fetch settlements
print("Fetching settlements...")
settlements = {}
for ticker in by_ticker:
    try:
        r = requests.get(f"{API}/markets/{ticker}")
        m = r.json().get("market", {})
        settlements[ticker] = m.get("result", "")
        time.sleep(0.05)
    except:
        settlements[ticker] = ""

# Strategy A: Tiered (min 0-7 buy 5-15c, min 7-10 buy 5-35c)
def find_entry_tiered(ticks):
    """Returns first tick that meets tiered criteria, or None."""
    for t in ticks:
        cs = t['cycle_sec']
        ask = t['yes_ask']
        if cs <= 420 and 5 <= ask <= 15:  # min 0-7 (0-420 sec), 5-15c
            return t
        if 420 < cs <= 600 and 5 <= ask <= 35:  # min 7-10 (420-600 sec), 5-35c
            return t
    return None

# Strategy B: Current (min 0-5 buy 5-35c)
def find_entry_current(ticks):
    for t in ticks:
        if t['cycle_sec'] <= 300 and 5 <= t['yes_ask'] <= 35:
            return t
    return None

# Strategy C: Drift down — wait for actual bottom in 5-35c during min 0-10
def find_entry_chase_low(ticks):
    """Look for the first bottom in 5-35c during 0-600 sec."""
    candidates = [t for t in ticks if t['cycle_sec'] <= 600 and 5 <= t['yes_ask'] <= 35]
    if not candidates:
        return None
    # Return the cheapest in the first 600s
    return min(candidates, key=lambda x: x['yes_ask'])

# Strategy D: Late buy only (min 7-10, 5-35c)
def find_entry_late_only(ticks):
    for t in ticks:
        if 420 < t['cycle_sec'] <= 600 and 5 <= t['yes_ask'] <= 35:
            return t
    return None

# Run all strategies
strategies = {
    "Current (0-5min, 5-35c)": find_entry_current,
    "Tiered (0-7=5-15, 7-10=5-35)": find_entry_tiered,
    "Late only (7-10min, 5-35c)": find_entry_late_only,
    "Optimal bottom (cheapest in 0-10)": find_entry_chase_low,
}

print(f"\n{'='*100}")
print(f"{'Strategy':<35} | {'Trades':>6} | {'W/L':>9} | {'Win%':>5} | {'PnL':>9} | {'Avg/trade':>10}")
print(f"{'-'*100}")

for name, fn in strategies.items():
    trades = wins = losses = 0
    pnl = 0
    for ticker, ticks in by_ticker.items():
        ticks_sorted = sorted(ticks, key=lambda x: x['ts_ms'])
        entry = fn(ticks_sorted)
        if not entry: continue
        result = settlements.get(ticker, "")
        if not result: continue
        # Assume buy YES (fade strategy)
        if result == "yes":
            pnl += (100 - entry['yes_ask'])
            wins += 1
        else:
            pnl -= entry['yes_ask']
            losses += 1
        trades += 1
    if trades > 0:
        wr = wins/trades*100
        avg = pnl/trades
        print(f"{name:<35} | {trades:>6} | {wins:>3}W/{losses:<3}L | {wr:>4.0f}% | {pnl:>+7}c | {avg:>+8.1f}c")
    else:
        print(f"{name:<35} | {trades:>6} | {'-':>9} | {'-':>5} | {'-':>9} | {'-':>10}")

# Per-coin breakdown for Tiered
print(f"\n{'='*100}")
print("PER-COIN BREAKDOWN — Tiered strategy")
print(f"{'='*100}")
print(f"{'Coin':<5} | {'Trades':>6} | {'W/L':>9} | {'Win%':>5} | {'PnL':>9} | {'Avg':>9}")
for coin in COINS:
    trades = wins = losses = 0
    pnl = 0
    for ticker, ticks in by_ticker.items():
        if not ticks or ticks[0]['coin'] != coin: continue
        ticks_sorted = sorted(ticks, key=lambda x: x['ts_ms'])
        entry = find_entry_tiered(ticks_sorted)
        if not entry: continue
        result = settlements.get(ticker, "")
        if not result: continue
        if result == "yes":
            pnl += (100 - entry['yes_ask'])
            wins += 1
        else:
            pnl -= entry['yes_ask']
            losses += 1
        trades += 1
    if trades > 0:
        wr = wins/trades*100
        print(f"{coin:<5} | {trades:>6} | {wins:>3}W/{losses:<3}L | {wr:>4.0f}% | {pnl:>+7}c | {pnl/trades:>+7.1f}c")

# Show entry distribution for Tiered
print(f"\n{'='*100}")
print("TIERED ENTRY PRICE DISTRIBUTION (where did fills happen?)")
print(f"{'='*100}")
entries_by_tier = {"early_5_15": [], "late_5_35": []}
for ticker, ticks in by_ticker.items():
    ticks_sorted = sorted(ticks, key=lambda x: x['ts_ms'])
    entry = find_entry_tiered(ticks_sorted)
    if not entry: continue
    if entry['cycle_sec'] <= 420:
        entries_by_tier["early_5_15"].append(entry['yes_ask'])
    else:
        entries_by_tier["late_5_35"].append(entry['yes_ask'])

print(f"Early tier (0-7min, 5-15c): {len(entries_by_tier['early_5_15'])} fills")
if entries_by_tier['early_5_15']:
    print(f"  Avg: {sum(entries_by_tier['early_5_15'])/len(entries_by_tier['early_5_15']):.0f}c | "
          f"Min: {min(entries_by_tier['early_5_15'])}c | Max: {max(entries_by_tier['early_5_15'])}c")

print(f"Late tier (7-10min, 5-35c): {len(entries_by_tier['late_5_35'])} fills")
if entries_by_tier['late_5_35']:
    print(f"  Avg: {sum(entries_by_tier['late_5_35'])/len(entries_by_tier['late_5_35']):.0f}c | "
          f"Min: {min(entries_by_tier['late_5_35'])}c | Max: {max(entries_by_tier['late_5_35'])}c")
