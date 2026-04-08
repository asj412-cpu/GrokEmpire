"""
Per-coin fade vs trend optimization.
For each of the 7 coins:
- Test fade vs trend at thresholds 5/10, 6/10, 7/10, 8/10
- Show which combo wins per coin
- Show daily breakdown to spot regime shifts
"""
import requests, time, numpy as np
from datetime import datetime, timedelta, timezone
from collections import defaultdict

API = "https://api.elections.kalshi.com/trade-api/v2"
COINS = ["BTC","ETH","SOL","XRP","BNB","HYPE","DOGE"]

end = int(datetime.now(timezone.utc).timestamp())
start = end - (5 * 86400)

ENTRY_LO = 5
ENTRY_HI = 30

print(f"Period: {datetime.fromtimestamp(start, tz=timezone.utc).strftime('%Y-%m-%d')} → {datetime.fromtimestamp(end, tz=timezone.utc).strftime('%Y-%m-%d')}")

# Fetch all coins
all_results = {}
for coin in COINS:
    series = f"KX{coin}15M"
    markets, cursor = [], ""
    while True:
        url = f"{API}/markets?series_ticker={series}&status=settled&limit=100&min_close_ts={start}&max_close_ts={end}"
        if cursor: url += f"&cursor={cursor}"
        d = requests.get(url).json()
        batch = d.get("markets", [])
        if not batch: break
        markets.extend(batch)
        cursor = d.get("cursor", "")
        if not cursor: break
        time.sleep(0.2)
    markets.sort(key=lambda m: m.get("close_time",""))
    all_results[coin] = [(m.get("result",""), m.get("close_time","")) for m in markets if m.get("result")]
    print(f"  {coin}: {len(all_results[coin])}")

def run(coin, thresh, window, mode):
    results = [r for r, _ in all_results[coin]]
    trades = wins = losses = pnl = 0
    for i in range(window, len(results)):
        prev = results[i-window:i]
        yc = sum(1 for r in prev if r == "yes")
        nc = window - yc
        sig = None
        if mode == "fade":
            if nc >= thresh: sig = "yes"
            elif yc >= thresh: sig = "no"
        else:
            if yc >= thresh: sig = "yes"
            elif nc >= thresh: sig = "no"
        if not sig: continue
        np.random.seed(i)
        entry = np.random.randint(ENTRY_LO, ENTRY_HI + 1)
        won = (sig == results[i])
        if won: pnl += (100 - entry); wins += 1
        else: pnl -= entry; losses += 1
        trades += 1
    return trades, wins, losses, pnl

print(f"\n{'='*120}")
print(f"PER-COIN MATRIX — fade vs trend at multiple thresholds (last 5 days)")
print(f"{'='*120}")

best_per_coin = {}
for coin in COINS:
    print(f"\n  {coin}:")
    print(f"    {'Config':<18} | {'Trades':>6} | {'Win%':>6} | {'PnL':>9} | {'Avg/trd':>9}")
    coin_results = []
    for thresh, window in [(5,10),(6,10),(7,10),(8,10)]:
        for mode in ["fade", "trend"]:
            t, w, l, pnl = run(coin, thresh, window, mode)
            if t == 0: continue
            wr = w/t*100
            avg = pnl/t
            label = f"{thresh}/{window} {mode}"
            coin_results.append((label, t, w, l, pnl, avg, wr, mode, thresh))
            marker = ""
            print(f"    {label:<18} | {t:>6} | {wr:>5.0f}% | {pnl:>+7}c | {avg:>+7.1f}c")

    # Determine best for this coin (max total PnL with min 30 trades for stat sig)
    sig_results = [r for r in coin_results if r[1] >= 30]
    if sig_results:
        best = max(sig_results, key=lambda r: r[4])
        best_per_coin[coin] = best
        print(f"    ★ BEST: {best[0]} → {best[4]:+d}c on {best[1]} trades ({best[6]:.0f}% WR)")

# Summary
print(f"\n{'='*120}")
print(f"RECOMMENDED PER-COIN CONFIG")
print(f"{'='*120}")
print(f"{'Coin':<5} | {'Best config':<18} | {'Current 6/10 fade':<22} | {'Improvement':>12}")

for coin in COINS:
    if coin not in best_per_coin: continue
    best = best_per_coin[coin]
    # Get current 6/10 fade for comparison
    cur_t, cur_w, cur_l, cur_pnl = run(coin, 6, 10, "fade")
    cur_label = f"{cur_pnl:+d}c ({cur_t} trd, {cur_w/max(1,cur_t)*100:.0f}%)"
    delta = best[4] - cur_pnl
    star = " ★" if best[0] != "6/10 fade" else ""
    print(f"{coin:<5} | {best[0]:<18} | {cur_label:<22} | {delta:>+8}c{star}")

# Daily breakdown for any coin where best != 6/10 fade
print(f"\n{'='*120}")
print(f"DAILY BREAKDOWN — coins where current 6/10 fade is NOT optimal")
print(f"{'='*120}")
for coin in COINS:
    if coin not in best_per_coin: continue
    if best_per_coin[coin][0] == "6/10 fade": continue

    print(f"\n  {coin} (recommended: {best_per_coin[coin][0]}):")
    # Daily WR for current vs recommended
    by_day_fade = defaultdict(lambda: {"w": 0, "t": 0, "pnl": 0})
    by_day_best = defaultdict(lambda: {"w": 0, "t": 0, "pnl": 0})

    results = all_results[coin]
    just_r = [r for r, _ in results]
    best_mode = best_per_coin[coin][7]
    best_thresh = best_per_coin[coin][8]
    best_window = 10  # all are 10

    for i in range(10, len(results)):
        result, close_time = results[i]
        try:
            day = datetime.fromisoformat(close_time.replace("Z","+00:00")).strftime("%Y-%m-%d")
        except:
            continue

        prev = just_r[i-10:i]
        yc = sum(1 for r in prev if r == "yes")
        nc = 10 - yc

        # Current 6/10 fade
        sig = None
        if nc >= 6: sig = "yes"
        elif yc >= 6: sig = "no"
        if sig:
            np.random.seed(i)
            entry = np.random.randint(5, 31)
            d = by_day_fade[day]
            d["t"] += 1
            won = (sig == result)
            d["pnl"] += (100 - entry) if won else -entry
            if won: d["w"] += 1

        # Best
        sig = None
        if best_mode == "fade":
            if nc >= best_thresh: sig = "yes"
            elif yc >= best_thresh: sig = "no"
        else:
            if yc >= best_thresh: sig = "yes"
            elif nc >= best_thresh: sig = "no"
        if sig:
            np.random.seed(i)
            entry = np.random.randint(5, 31)
            d = by_day_best[day]
            d["t"] += 1
            won = (sig == result)
            d["pnl"] += (100 - entry) if won else -entry
            if won: d["w"] += 1

    print(f"    {'Day':<11} | {'6/10 Fade':<20} | {best_per_coin[coin][0]:<20}")
    for day in sorted(set(list(by_day_fade.keys()) + list(by_day_best.keys()))):
        f = by_day_fade.get(day, {"w":0,"t":0,"pnl":0})
        b = by_day_best.get(day, {"w":0,"t":0,"pnl":0})
        f_str = f"{f['pnl']:+d}c ({f['t']}, {f['w']/max(1,f['t'])*100:.0f}%)" if f['t'] > 0 else "—"
        b_str = f"{b['pnl']:+d}c ({b['t']}, {b['w']/max(1,b['t'])*100:.0f}%)" if b['t'] > 0 else "—"
        print(f"    {day:<11} | {f_str:<20} | {b_str:<20}")
