"""
Per-coin VALID threshold optimization (6/10, 7/10, 8/10 only).
Plus 4/8, 5/8, 6/8 (smaller window variants).
Excludes 5/10 because that fires on 5/5 splits which aren't a real majority.
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
    """Mode: fade or trend. Threshold must be > window/2 to require a real majority."""
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

# Only valid thresholds (must require true majority: >half the window)
configs = [
    (6, 10, "6/10"),
    (7, 10, "7/10"),
    (8, 10, "8/10"),
    (9, 10, "9/10"),
    (5, 8, "5/8"),
    (6, 8, "6/8"),
    (7, 8, "7/8"),
    (4, 7, "4/7"),
    (5, 7, "5/7"),
    (6, 7, "6/7"),
]

print(f"\n{'='*120}")
print(f"PER-COIN — fade vs trend at valid thresholds (last 5 days)")
print(f"{'='*120}")

best_per_coin = {}
for coin in COINS:
    print(f"\n  {coin}:")
    print(f"    {'Config':<18} | {'Trades':>6} | {'Win%':>6} | {'PnL':>9} | {'Avg/trd':>9}")
    coin_results = []
    for thresh, window, label in configs:
        for mode in ["fade", "trend"]:
            t, w, l, pnl = run(coin, thresh, window, mode)
            if t == 0: continue
            wr = w/t*100
            avg = pnl/t
            full_label = f"{label} {mode}"
            coin_results.append((full_label, t, w, l, pnl, avg, wr, mode, thresh, window))
            if t >= 30:  # only show statistically meaningful
                print(f"    {full_label:<18} | {t:>6} | {wr:>5.0f}% | {pnl:>+7}c | {avg:>+7.1f}c")

    sig_results = [r for r in coin_results if r[1] >= 50]  # min 50 trades
    if sig_results:
        # Best by avg PnL per trade (more selective trades preferred over volume)
        best_by_avg = max(sig_results, key=lambda r: r[5])
        best_by_total = max(sig_results, key=lambda r: r[4])
        print(f"    ★ BEST PnL: {best_by_total[0]} → {best_by_total[4]:+d}c on {best_by_total[1]} trades ({best_by_total[6]:.0f}% WR)")
        print(f"    ★ BEST EV/trade: {best_by_avg[0]} → {best_by_avg[5]:+.1f}c/trade ({best_by_avg[1]} trades, {best_by_avg[6]:.0f}% WR)")
        best_per_coin[coin] = best_by_total

# Recommended per-coin config
print(f"\n{'='*120}")
print(f"RECOMMENDED PER-COIN CONFIG (vs current 6/10 fade)")
print(f"{'='*120}")
print(f"{'Coin':<5} | {'Best (by total PnL)':<22} | {'Current 6/10 fade':<22} | {'Δ':>10}")
for coin in COINS:
    if coin not in best_per_coin: continue
    best = best_per_coin[coin]
    cur_t, cur_w, cur_l, cur_pnl = run(coin, 6, 10, "fade")
    cur_str = f"{cur_pnl:+d}c ({cur_w}/{cur_t}={cur_w/max(1,cur_t)*100:.0f}%)"
    delta = best[4] - cur_pnl
    star = " ★" if best[0] != "6/10 fade" else ""
    print(f"{coin:<5} | {best[0]:<22} | {cur_str:<22} | {delta:>+8}c{star}")
