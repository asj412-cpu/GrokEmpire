"""
Test: would HYPE benefit from "6/10 agree/trend" instead of "6/10 fade"?

For HYPE specifically, last 5 days of Kalshi data:
- Compute fade WR vs trend WR at thresholds 5/10, 6/10, 7/10, 8/10
- Apply the same tiered entry simulation (random in 5-30c range)
- Compare PnL outcomes
"""
import requests, time, numpy as np
from datetime import datetime, timedelta, timezone

API = "https://api.elections.kalshi.com/trade-api/v2"
COIN = "HYPE"
SERIES = "KXHYPE15M"

# Last 5 days
end = int(datetime.now(timezone.utc).timestamp())
start = end - (5 * 86400)

print(f"Period: {datetime.fromtimestamp(start, tz=timezone.utc).strftime('%Y-%m-%d %H:%M')} → {datetime.fromtimestamp(end, tz=timezone.utc).strftime('%Y-%m-%d %H:%M')}")

# Fetch all settled HYPE markets
print(f"\nFetching {COIN} settlements...")
markets, cursor = [], ""
while True:
    url = f"{API}/markets?series_ticker={SERIES}&status=settled&limit=100&min_close_ts={start}&max_close_ts={end}"
    if cursor: url += f"&cursor={cursor}"
    d = requests.get(url).json()
    batch = d.get("markets", [])
    if not batch: break
    markets.extend(batch)
    cursor = d.get("cursor", "")
    if not cursor: break
    time.sleep(0.2)

markets.sort(key=lambda m: m.get("close_time",""))
results = [m.get("result","") for m in markets if m.get("result")]
print(f"Got {len(results)} HYPE settled markets")

yes_pct = sum(1 for r in results if r == "yes") / len(results) * 100
print(f"Overall YES rate: {yes_pct:.0f}%")

# ═══════════════════════════════════════════════════════════════
# Test FADE vs TREND at multiple thresholds
# ═══════════════════════════════════════════════════════════════
print(f"\n{'='*100}")
print(f"FADE vs TREND signal — HYPE last 5 days, {len(results)} markets")
print(f"{'='*100}")

ENTRY_LO = 5
ENTRY_HI = 30

def run_strategy(thresh, window, mode):
    """mode: 'fade' or 'trend'"""
    trades = wins = losses = 0
    pnl = 0
    for i in range(window, len(results)):
        prev = results[i-window:i]
        yc = sum(1 for r in prev if r == "yes")
        nc = window - yc

        sig = None
        if mode == "fade":
            if nc >= thresh: sig = "yes"
            elif yc >= thresh: sig = "no"
        else:  # trend
            if yc >= thresh: sig = "yes"
            elif nc >= thresh: sig = "no"

        if not sig: continue

        # Random entry in 5-30c
        np.random.seed(i)
        entry = np.random.randint(ENTRY_LO, ENTRY_HI + 1)

        won = (sig == results[i])
        if won:
            pnl += (100 - entry); wins += 1
        else:
            pnl -= entry; losses += 1
        trades += 1
    return trades, wins, losses, pnl

print(f"{'Strategy':<25} | {'Trades':>6} | {'W/L':>9} | {'Win%':>6} | {'PnL':>9} | {'Avg/trd':>8}")
print("-" * 80)

for thresh, window in [(5,10),(6,10),(7,10),(8,10),(4,8),(5,8),(6,8)]:
    for mode in ["fade", "trend"]:
        t, w, l, pnl = run_strategy(thresh, window, mode)
        if t == 0: continue
        wr = w/t*100
        avg = pnl/t
        label = f"{thresh}/{window} {mode}"
        print(f"{label:<25} | {t:>6} | {w:>3}W/{l:<3}L | {wr:>5.1f}% | {pnl:>+7}c | {avg:>+6.1f}c")

# Day-by-day breakdown for 6/10 fade vs trend
print(f"\n{'='*100}")
print(f"DAILY BREAKDOWN — fade vs trend (6/10)")
print(f"{'='*100}")
print(f"{'Day':<11} | {'Trades':>6} | {'Yes%':>6} | {'Fade WR':>8} | {'Trend WR':>9} | {'Fade PnL':>9} | {'Trend PnL':>10}")

from collections import defaultdict
day_data = defaultdict(lambda: {"yes": 0, "no": 0, "fade_w": 0, "fade_t": 0, "fade_pnl": 0, "trend_w": 0, "trend_t": 0, "trend_pnl": 0})

# We need close_time for daily bucketing
markets_with_times = [(m.get("result",""), m.get("close_time","")) for m in markets if m.get("result")]
just_results = [r for r, _ in markets_with_times]

for i, (result, close_time) in enumerate(markets_with_times):
    try:
        day = datetime.fromisoformat(close_time.replace("Z","+00:00")).strftime("%Y-%m-%d")
    except:
        continue

    d = day_data[day]
    if result == "yes": d["yes"] += 1
    else: d["no"] += 1

    if i >= 10:
        prev = just_results[i-10:i]
        yc = sum(1 for r in prev if r == "yes")
        nc = 10 - yc

        # Fade
        if nc >= 6: fsig = "yes"
        elif yc >= 6: fsig = "no"
        else: fsig = None
        if fsig:
            np.random.seed(i)
            entry = np.random.randint(5, 31)
            won = (fsig == result)
            d["fade_t"] += 1
            d["fade_pnl"] += (100 - entry) if won else -entry
            if won: d["fade_w"] += 1

        # Trend
        if yc >= 6: tsig = "yes"
        elif nc >= 6: tsig = "no"
        else: tsig = None
        if tsig:
            np.random.seed(i)
            entry = np.random.randint(5, 31)
            won = (tsig == result)
            d["trend_t"] += 1
            d["trend_pnl"] += (100 - entry) if won else -entry
            if won: d["trend_w"] += 1

for day in sorted(day_data.keys()):
    d = day_data[day]
    total = d["yes"] + d["no"]
    yes_pct = d["yes"]/max(1,total)*100
    fwr = d["fade_w"]/max(1,d["fade_t"])*100
    twr = d["trend_w"]/max(1,d["trend_t"])*100
    print(f"{day:<11} | {total:>6} | {yes_pct:>5.0f}% | {fwr:>6.0f}% | {twr:>7.0f}% | {d['fade_pnl']:>+7}c | {d['trend_pnl']:>+8}c")

# Total
print()
ftw = sum(d["fade_w"] for d in day_data.values())
ftt = sum(d["fade_t"] for d in day_data.values())
ftp = sum(d["fade_pnl"] for d in day_data.values())
ttw = sum(d["trend_w"] for d in day_data.values())
ttt = sum(d["trend_t"] for d in day_data.values())
ttp = sum(d["trend_pnl"] for d in day_data.values())
print(f"TOTAL FADE:  {ftt} trades, {ftw}/{ftt} = {ftw/max(1,ftt)*100:.0f}%, PnL {ftp:+d}c")
print(f"TOTAL TREND: {ttt} trades, {ttw}/{ttt} = {ttw/max(1,ttt)*100:.0f}%, PnL {ttp:+d}c")
