"""
Test tiered window with cooldown:
- Min 0-7 (sec 0-420): buy if YES ask is 1-20c
- Min 7-10 (sec 420-600): buy if YES ask is 20-49c
- After each fill: 60s cooldown for that coin (within cycle, doesn't carry across)
- Max 3 contracts per market

Compare against:
- Current production (min 0-5, 5-35c, no cooldown)
- Previous tiered (0-7=5-15, 7-10=5-35, no cooldown)
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


def find_entries_tiered_cooldown(ticks, max_contracts=3, cooldown_sec=60):
    """Returns list of entry ticks for a single ticker with tiered window + cooldown"""
    ticks_sorted = sorted(ticks, key=lambda x: x['cycle_sec'])
    entries = []
    last_buy_sec = -999
    for t in ticks_sorted:
        if len(entries) >= max_contracts:
            break
        cs = t['cycle_sec']
        ask = t['yes_ask']
        # Cooldown check
        if cs - last_buy_sec < cooldown_sec:
            continue
        # Tier check
        if cs <= 420 and 1 <= ask <= 20:
            entries.append(t)
            last_buy_sec = cs
        elif 420 < cs <= 600 and 20 <= ask <= 49:
            entries.append(t)
            last_buy_sec = cs
    return entries

def find_entries_simple(ticks, sec_min, sec_max, ask_min, ask_max, max_contracts=3, cooldown_sec=0):
    """Generic finder with optional cooldown"""
    ticks_sorted = sorted(ticks, key=lambda x: x['cycle_sec'])
    entries = []
    last_buy_sec = -999
    for t in ticks_sorted:
        if len(entries) >= max_contracts:
            break
        cs = t['cycle_sec']
        ask = t['yes_ask']
        if cs - last_buy_sec < cooldown_sec:
            continue
        if sec_min <= cs <= sec_max and ask_min <= ask <= ask_max:
            entries.append(t)
            last_buy_sec = cs
    return entries

def find_entries_old_tiered(ticks, max_contracts=3):
    """Original tiered: 0-7=5-15, 7-10=5-35, NO cooldown"""
    ticks_sorted = sorted(ticks, key=lambda x: x['cycle_sec'])
    entries = []
    for t in ticks_sorted:
        if len(entries) >= max_contracts:
            break
        cs = t['cycle_sec']
        ask = t['yes_ask']
        if cs <= 420 and 5 <= ask <= 15:
            entries.append(t)
        elif 420 < cs <= 600 and 5 <= ask <= 35:
            entries.append(t)
    return entries


def run_strategy(name, finder_fn):
    trades = wins = losses = 0
    pnl = 0
    fills_per_ticker = []
    for ticker, ticks in by_ticker.items():
        entries = finder_fn(ticks)
        if not entries: continue
        result = settlements.get(ticker, "")
        if not result: continue
        fills_per_ticker.append(len(entries))
        for e in entries:
            entry_price = e['yes_ask']
            if result == "yes":
                pnl += (100 - entry_price)
                wins += 1
            else:
                pnl -= entry_price
                losses += 1
            trades += 1
    avg_fills = sum(fills_per_ticker)/max(1,len(fills_per_ticker))
    return trades, wins, losses, pnl, avg_fills

print(f"\n{'='*110}")
print(f"{'Strategy':<48} | {'Trades':>6} | {'W/L':>9} | {'Win%':>5} | {'PnL':>9} | {'Avg/trd':>8} | {'Fills/mkt':>9}")
print(f"{'-'*110}")

strategies = {
    "Current (0-5, 5-35c, 1 contract)": lambda t: find_entries_simple(t, 0, 300, 5, 35, max_contracts=1),
    "Current (0-5, 5-35c, 3 contracts)": lambda t: find_entries_simple(t, 0, 300, 5, 35, max_contracts=3),
    "Old Tiered (0-7=5-15, 7-10=5-35)": find_entries_old_tiered,
    "NEW Tiered+Cooldown (1-20/20-49 + 60s)": lambda t: find_entries_tiered_cooldown(t, max_contracts=3, cooldown_sec=60),
    "NEW No-Cooldown (1-20/20-49)": lambda t: find_entries_tiered_cooldown(t, max_contracts=3, cooldown_sec=0),
    "NEW Cooldown 30s": lambda t: find_entries_tiered_cooldown(t, max_contracts=3, cooldown_sec=30),
    "NEW Cooldown 90s": lambda t: find_entries_tiered_cooldown(t, max_contracts=3, cooldown_sec=90),
    "NEW Cooldown 120s": lambda t: find_entries_tiered_cooldown(t, max_contracts=3, cooldown_sec=120),
}

for name, fn in strategies.items():
    t, w, l, pnl, fills = run_strategy(name, fn)
    if t > 0:
        wr = w/t*100
        avg = pnl/t
        print(f"{name:<48} | {t:>6} | {w:>3}W/{l:<3}L | {wr:>4.0f}% | {pnl:>+7}c | {avg:>+6.1f}c | {fills:>7.2f}")

# Per-coin breakdown for the new tiered+cooldown
print(f"\n{'='*100}")
print("PER-COIN — NEW Tiered+Cooldown 60s")
print(f"{'='*100}")
print(f"{'Coin':<5} | {'Tickers':>7} | {'Trades':>6} | {'W/L':>9} | {'Win%':>5} | {'PnL':>8} | {'Avg':>8}")
for coin in COINS:
    t = w = l = 0
    pnl = 0
    coin_tickers = 0
    for ticker, ticks in by_ticker.items():
        if not ticks or ticks[0]['coin'] != coin: continue
        coin_tickers += 1
        entries = find_entries_tiered_cooldown(ticks, max_contracts=3, cooldown_sec=60)
        if not entries: continue
        result = settlements.get(ticker, "")
        if not result: continue
        for e in entries:
            ep = e['yes_ask']
            if result == "yes":
                pnl += (100 - ep); w += 1
            else:
                pnl -= ep; l += 1
            t += 1
    if t > 0:
        wr = w/t*100
        print(f"{coin:<5} | {coin_tickers:>7} | {t:>6} | {w:>3}W/{l:<3}L | {wr:>4.0f}% | {pnl:>+6}c | {pnl/t:>+6.1f}c")
    elif coin_tickers > 0:
        print(f"{coin:<5} | {coin_tickers:>7} | 0 (no fills)")

# Show entry distribution
print(f"\n{'='*100}")
print("ENTRY DISTRIBUTION (NEW Tiered+Cooldown 60s)")
print(f"{'='*100}")
all_entries = []
for ticker, ticks in by_ticker.items():
    entries = find_entries_tiered_cooldown(ticks, max_contracts=3, cooldown_sec=60)
    for e in entries:
        all_entries.append({"price": e['yes_ask'], "sec": e['cycle_sec']})

if all_entries:
    early = [e for e in all_entries if e['sec'] <= 420]
    late = [e for e in all_entries if e['sec'] > 420]
    print(f"Early (0-7min, 1-20c): {len(early)} fills")
    if early:
        prices = [e['price'] for e in early]
        print(f"  Avg: {sum(prices)/len(prices):.0f}c | Range: {min(prices)}-{max(prices)}c")
    print(f"Late (7-10min, 20-49c): {len(late)} fills")
    if late:
        prices = [e['price'] for e in late]
        print(f"  Avg: {sum(prices)/len(prices):.0f}c | Range: {min(prices)}-{max(prices)}c")
