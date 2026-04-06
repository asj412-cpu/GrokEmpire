"""
Pattern discovery on 4-day Kalshi data:
1. Multi-window outcome combinations (10-cycle + 100-cycle, etc.)
2. Streak patterns (consecutive same-side)
3. Time-of-day effects
4. Conditional fade win rates
"""
import requests, time as time_mod
import numpy as np
from datetime import datetime, timezone
from collections import defaultdict, Counter

API = "https://api.elections.kalshi.com/trade-api/v2"
COINS = ["BTC", "ETH", "SOL", "XRP", "BNB", "HYPE", "DOGE"]

start_ts = int(datetime(2026, 4, 2, 19, 30, tzinfo=timezone.utc).timestamp())
end_ts = int(datetime(2026, 4, 6, 23, 59, tzinfo=timezone.utc).timestamp())

# Fetch ALL settled markets including older ones for 100-cycle history
fetch_start = start_ts - (200 * 15 * 60)  # 200 cycles before for warmup

all_markets = {}
for coin in COINS:
    series = f"KX{coin}15M"
    markets, cursor = [], ""
    while True:
        url = f"{API}/markets?series_ticker={series}&status=settled&limit=100&min_close_ts={fetch_start}&max_close_ts={end_ts}"
        if cursor: url += f"&cursor={cursor}"
        d = requests.get(url).json()
        batch = d.get("markets", [])
        if not batch: break
        markets.extend(batch)
        cursor = d.get("cursor", "")
        if not cursor: break
        time_mod.sleep(0.2)
    all_markets[coin] = sorted(markets, key=lambda m: m["close_time"])
    print(f"{coin}: {len(markets)} markets")
print()


# ═══════════════════════════════════════════════════════════
# TEST 1: Multi-window combinations
# ═══════════════════════════════════════════════════════════
print("="*90)
print("TEST 1: 6/10 fade win rate CONDITIONED on broader window state")
print("="*90)
print(f"{'Coin':<5} | {'6/10 Sig':>9} | {'100yes>60':>11} | {'100yes 40-60':>13} | {'100yes<40':>11}")
print(f"{'-'*5}-+-{'-'*9}-+-{'-'*11}-+-{'-'*13}-+-{'-'*11}")

for coin in COINS:
    markets = all_markets[coin]
    results = [m.get("result","") for m in markets]
    if len(results) < 110: continue

    # Bucket by 100-cycle yes rate
    buckets = {"high":[], "mid":[], "low":[]}  # win/total tuples
    for i in range(100, len(results)):
        if not results[i]: continue
        # 100-cycle context
        prev100 = results[i-100:i]
        yes_100 = sum(1 for r in prev100 if r == "yes")
        # 10-cycle fade
        prev10 = results[i-10:i]
        yes_10 = sum(1 for r in prev10 if r == "yes")
        no_10 = 10 - yes_10

        if no_10 >= 6: sig = "yes"
        elif yes_10 >= 6: sig = "no"
        else: continue

        won = (sig == "yes" and results[i] == "yes") or (sig == "no" and results[i] == "no")

        if yes_100 > 60: bucket = "high"
        elif yes_100 < 40: bucket = "low"
        else: bucket = "mid"
        buckets[bucket].append(1 if won else 0)

    def fmt(b):
        if not b: return "—"
        wr = sum(b)/len(b)*100
        return f"{wr:.0f}% ({len(b)})"

    sig_count = sum(len(b) for b in buckets.values())
    print(f"{coin:<5} | {sig_count:>9} | {fmt(buckets['high']):>11} | {fmt(buckets['mid']):>13} | {fmt(buckets['low']):>11}")

print()


# ═══════════════════════════════════════════════════════════
# TEST 2: Consecutive streak patterns
# ═══════════════════════════════════════════════════════════
print("="*90)
print("TEST 2: After N consecutive same outcomes, what happens next?")
print("="*90)
print(f"{'Coin':<5} | {'2-streak NO→YES%':>16} | {'3-streak NO→YES%':>16} | {'4-streak NO→YES%':>16} | {'5-streak NO→YES%':>16}")
print(f"{'-'*5}-+-{'-'*16}-+-{'-'*16}-+-{'-'*16}-+-{'-'*16}")

for coin in COINS:
    results = [m.get("result","") for m in all_markets[coin]]
    if len(results) < 10: continue

    streaks = {2: [], 3: [], 4: [], 5: []}
    for n in [2, 3, 4, 5]:
        for i in range(n, len(results)):
            if not results[i]: continue
            prev = results[i-n:i]
            if all(r == "no" for r in prev):
                streaks[n].append(1 if results[i] == "yes" else 0)

    def fmt(b):
        if not b: return "—"
        wr = sum(b)/len(b)*100
        return f"{wr:.0f}% ({len(b)})"

    print(f"{coin:<5} | {fmt(streaks[2]):>16} | {fmt(streaks[3]):>16} | {fmt(streaks[4]):>16} | {fmt(streaks[5]):>16}")

print()
print(f"{'Coin':<5} | {'2-streak YES→NO%':>16} | {'3-streak YES→NO%':>16} | {'4-streak YES→NO%':>16} | {'5-streak YES→NO%':>16}")
print(f"{'-'*5}-+-{'-'*16}-+-{'-'*16}-+-{'-'*16}-+-{'-'*16}")

for coin in COINS:
    results = [m.get("result","") for m in all_markets[coin]]
    if len(results) < 10: continue

    streaks = {2: [], 3: [], 4: [], 5: []}
    for n in [2, 3, 4, 5]:
        for i in range(n, len(results)):
            if not results[i]: continue
            prev = results[i-n:i]
            if all(r == "yes" for r in prev):
                streaks[n].append(1 if results[i] == "no" else 0)

    def fmt(b):
        if not b: return "—"
        wr = sum(b)/len(b)*100
        return f"{wr:.0f}% ({len(b)})"

    print(f"{coin:<5} | {fmt(streaks[2]):>16} | {fmt(streaks[3]):>16} | {fmt(streaks[4]):>16} | {fmt(streaks[5]):>16}")

print()


# ═══════════════════════════════════════════════════════════
# TEST 3: Time-of-day effect (UTC hour)
# ═══════════════════════════════════════════════════════════
print("="*90)
print("TEST 3: 6/10 fade win rate by hour of day (UTC)")
print("="*90)

for coin in COINS:
    markets = all_markets[coin]
    results = [m.get("result","") for m in markets]
    times = [m.get("close_time","") for m in markets]
    if len(results) < 20: continue

    by_hour = defaultdict(list)
    for i in range(10, len(results)):
        if not results[i] or not times[i]: continue
        prev = results[i-10:i]
        yc = sum(1 for r in prev if r == "yes")
        nc = 10 - yc
        if nc >= 6: sig = "yes"
        elif yc >= 6: sig = "no"
        else: continue

        won = (sig == "yes" and results[i] == "yes") or (sig == "no" and results[i] == "no")
        try:
            hr = datetime.fromisoformat(times[i].replace("Z","+00:00")).hour
            by_hour[hr].append(1 if won else 0)
        except: pass

    print(f"\n{coin} hourly fade WR:")
    best_hours = []
    worst_hours = []
    for h in sorted(by_hour.keys()):
        b = by_hour[h]
        if len(b) < 5: continue
        wr = sum(b)/len(b)*100
        marker = ""
        if wr >= 60: marker = " ★"
        elif wr <= 40: marker = " ✗"
        if wr >= 60: best_hours.append(h)
        elif wr <= 40: worst_hours.append(h)
        print(f"  {h:02d}h: {sum(b)}/{len(b)} = {wr:.0f}%{marker}")

    if best_hours: print(f"  → Best hours (WR≥60%): {best_hours}")
    if worst_hours: print(f"  → Worst hours (WR≤40%): {worst_hours}")

print()


# ═══════════════════════════════════════════════════════════
# TEST 4: Threshold sensitivity per coin
# ═══════════════════════════════════════════════════════════
print("="*90)
print("TEST 4: Per-coin best fade threshold")
print("="*90)
print(f"{'Coin':<5} | {'5/10':>11} | {'6/10':>11} | {'7/10':>11} | {'8/10':>11} | {'4/6':>11} | {'5/7':>11}")
print(f"{'-'*5}-+-{'-'*11}-+-{'-'*11}-+-{'-'*11}-+-{'-'*11}-+-{'-'*11}-+-{'-'*11}")

configs = [(5,10),(6,10),(7,10),(8,10),(4,6),(5,7)]
for coin in COINS:
    results = [m.get("result","") for m in all_markets[coin]]
    if len(results) < 20: continue
    row = []
    for thresh, win in configs:
        wins = total = 0
        for i in range(win, len(results)):
            if not results[i]: continue
            prev = results[i-win:i]
            yc = sum(1 for r in prev if r == "yes")
            nc = win - yc
            if nc >= thresh: sig = "yes"
            elif yc >= thresh: sig = "no"
            else: continue
            total += 1
            if (sig == "yes" and results[i] == "yes") or (sig == "no" and results[i] == "no"):
                wins += 1
        if total == 0: row.append("—")
        else: row.append(f"{wins/total*100:.0f}% ({total})")
    print(f"{coin:<5} | {row[0]:>11} | {row[1]:>11} | {row[2]:>11} | {row[3]:>11} | {row[4]:>11} | {row[5]:>11}")
