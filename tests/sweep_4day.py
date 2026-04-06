"""Parameter sweep backtest: Apr 2-6 real Kalshi data"""
import requests, numpy as np, time as time_mod
from datetime import datetime, timezone
from itertools import product

API = "https://api.elections.kalshi.com/trade-api/v2"
COINS = {"BTC": "KXBTC15M", "ETH": "KXETH15M", "SOL": "KXSOL15M", "DOGE": "KXDOGE15M",
         "XRP": "KXXRP15M", "BNB": "KXBNB15M", "HYPE": "KXHYPE15M"}

start_ts = int(datetime(2026, 4, 2, 19, 30, tzinfo=timezone.utc).timestamp())
end_ts = int(datetime(2026, 4, 6, 23, 59, tzinfo=timezone.utc).timestamp())

# Fetch all markets
all_markets = {}
for coin, series in COINS.items():
    markets, cursor = [], ""
    while True:
        url = f"{API}/markets?series_ticker={series}&status=settled&limit=100&min_close_ts={start_ts - 3600}&max_close_ts={end_ts}"
        if cursor: url += f"&cursor={cursor}"
        d = requests.get(url).json()
        batch = d.get("markets", [])
        if not batch: break
        markets.extend(batch)
        cursor = d.get("cursor", "")
        if not cursor: break
        time_mod.sleep(0.25)
    all_markets[coin] = sorted(markets, key=lambda m: m["close_time"])
    print(f"{coin}: {len(markets)} markets")

def run(entry_lo, entry_hi, fade_thresh, fade_window, max_per_mkt):
    results_by_coin = {}
    for coin, markets in all_markets.items():
        trades = wins = losses = 0
        pnl = 0.0
        for i in range(fade_window, len(markets)):
            r = markets[i].get("result", "")
            if not r: continue
            hist = [markets[j].get("result","") for j in range(i-fade_window, i)]
            yc = sum(1 for h in hist if h == "yes")
            nc = fade_window - yc
            if nc >= fade_thresh: sig = "yes"
            elif yc >= fade_thresh: sig = "no"
            else: continue

            np.random.seed(i * 7 + hash(coin) % 1000)
            for _ in range(max_per_mkt):
                entry = np.random.randint(entry_lo, min(entry_hi, 50) + 1)
                if entry < entry_lo or entry > entry_hi: continue
                won = (sig == "yes" and r == "yes") or (sig == "no" and r == "no")
                p = (1.0 - entry/100) if won else -(entry/100)
                trades += 1; pnl += p
                if won: wins += 1
                else: losses += 1
        results_by_coin[coin] = (trades, wins, losses, pnl)

    tot_t = sum(v[0] for v in results_by_coin.values())
    tot_w = sum(v[1] for v in results_by_coin.values())
    tot_l = sum(v[2] for v in results_by_coin.values())
    tot_pnl = sum(v[3] for v in results_by_coin.values())
    return tot_t, tot_w, tot_l, tot_pnl, results_by_coin

# Grid
entry_ranges = [(5,25),(5,30),(5,35),(5,40),(5,45)]
fade_configs = [(6,10,"6/10"),(7,10,"7/10"),(8,10,"8/10"),(6,8,"6/8"),(7,8,"7/8")]
max_contracts = [1, 2, 3]

print(f"\n{'Config':<28} | {'Trades':>6} | {'W/L':>9} | {'Win%':>6} | {'PnL':>9} | {'Avg':>7}")
print("-"*75)

top_results = []
for (elo, ehi), (fth, fwin, flabel), mc in product(entry_ranges, fade_configs, max_contracts):
    t, w, l, pnl, by_coin = run(elo, ehi, fth, fwin, mc)
    wr = w/max(1,t)*100
    avg = pnl/max(1,t)
    label = f"{elo}-{ehi}c {flabel} max{mc}"
    print(f"{label:<28} | {t:>6} | {w:>4}/{l:<4} | {wr:>5.1f}% | ${pnl:>7.2f} | ${avg:>5.2f}")
    top_results.append((pnl, label, t, w, l, wr, avg, by_coin))

# Top 5
top_results.sort(key=lambda x: -x[0])
print(f"\n{'='*75}")
print("TOP 5 CONFIGS:")
for i, (pnl, label, t, w, l, wr, avg, by_coin) in enumerate(top_results[:5]):
    print(f"\n  #{i+1}: {label} → ${pnl:.2f} PnL ({t} trades, {wr:.1f}% WR, ${avg:.2f}/trade)")
    for coin in sorted(by_coin):
        ct, cw, cl, cp = by_coin[coin]
        if ct > 0:
            print(f"    {coin:>5}: {ct} trades ({cw}W/{cl}L) {cw/max(1,ct)*100:.0f}% ${cp:.2f}")
