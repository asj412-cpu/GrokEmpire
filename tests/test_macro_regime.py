"""
Test: does the 6/10 fade win rate depend on the macro BTC trend?

Hypothesis 1: Fade works in choppy markets, breaks in trending markets
Hypothesis 2: Strikes auto-reset each cycle so YES rate stays ~50% regardless of regime
Hypothesis 3: There's a regime-dependent edge

For each day in the last 30 days:
1. Get daily BTC % change (Coinbase)
2. Get all 7 coin Kalshi settlements
3. Compute YES rate per coin
4. Compute 6/10 fade WR
5. Correlate
"""
import requests, time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

API = "https://api.elections.kalshi.com/trade-api/v2"
COINS = ["BTC","ETH","SOL","XRP","BNB","HYPE","DOGE"]

# Get last 30 days
end = datetime.now(timezone.utc)
start = end - timedelta(days=30)

# Pull Coinbase BTC daily candles for macro context
print("Fetching BTC daily candles from Coinbase...")
cb_url = f"https://api.exchange.coinbase.com/products/BTC-USD/candles?granularity=86400&start={start.strftime('%Y-%m-%d')}&end={end.strftime('%Y-%m-%d')}"
r = requests.get(cb_url)
candles = r.json()
# [time, low, high, open, close, volume]
btc_daily = {}
for c in candles:
    day = datetime.fromtimestamp(c[0], tz=timezone.utc).strftime("%Y-%m-%d")
    delta_pct = (c[4] - c[3]) / c[3] * 100  # close - open
    btc_daily[day] = {"open": c[3], "close": c[4], "delta_pct": delta_pct, "volume": c[5]}
print(f"Got {len(btc_daily)} BTC daily candles")

# Pull Kalshi settlements per coin for the same period
print("\nFetching Kalshi settlements...")
all_results = {}
for coin in COINS:
    series = f"KX{coin}15M"
    markets = []
    cursor = ""
    start_ts = int(start.timestamp())
    end_ts = int(end.timestamp())
    while True:
        url = f"{API}/markets?series_ticker={series}&status=settled&limit=100&min_close_ts={start_ts}&max_close_ts={end_ts}"
        if cursor: url += f"&cursor={cursor}"
        d = requests.get(url).json()
        batch = d.get("markets", [])
        if not batch: break
        markets.extend(batch)
        cursor = d.get("cursor", "")
        if not cursor: break
        time.sleep(0.2)
    markets.sort(key=lambda m: m.get("close_time",""))
    all_results[coin] = markets
    print(f"  {coin}: {len(markets)} markets")

# Compute per-day per-coin: YES rate, fade WR, # of trades
print(f"\n{'='*120}")
print(f"DAILY BREAKDOWN — fade WR vs BTC daily move")
print(f"{'='*120}")
print(f"{'Day':<11} | {'BTC Δ':>8} | {'BTC YES%':>9} | {'BTC Fade':>10} | {'AGG YES%':>9} | {'AGG Fade WR':>12} | {'Fade Sigs':>10}")
print(f"{'-'*120}")

# Group settlements by day
day_data = defaultdict(lambda: {"yes": 0, "no": 0, "fade_signals": 0, "fade_wins": 0, "btc_yes": 0, "btc_no": 0, "btc_fade_sig": 0, "btc_fade_win": 0})

for coin in COINS:
    results_with_time = [(m.get("result",""), m.get("close_time","")) for m in all_results[coin] if m.get("result")]
    # For fade signal we need running history per coin, but bucketed by day
    just_results = [r[0] for r in results_with_time]

    # For each market i (i >= 10), compute fade signal from prior 10
    for i, (result, close_time) in enumerate(results_with_time):
        try:
            day = datetime.fromisoformat(close_time.replace("Z","+00:00")).strftime("%Y-%m-%d")
        except:
            continue

        if result == "yes":
            day_data[day]["yes"] += 1
            if coin == "BTC": day_data[day]["btc_yes"] += 1
        else:
            day_data[day]["no"] += 1
            if coin == "BTC": day_data[day]["btc_no"] += 1

        if i >= 10:
            prev = just_results[i-10:i]
            yc = sum(1 for r in prev if r == "yes")
            nc = 10 - yc
            sig = None
            if nc >= 6: sig = "yes"
            elif yc >= 6: sig = "no"
            if sig:
                day_data[day]["fade_signals"] += 1
                if coin == "BTC": day_data[day]["btc_fade_sig"] += 1
                if sig == result:
                    day_data[day]["fade_wins"] += 1
                    if coin == "BTC": day_data[day]["btc_fade_win"] += 1

# Sort days
days = sorted(day_data.keys())
for day in days:
    d = day_data[day]
    btc = btc_daily.get(day, {})
    btc_delta = btc.get("delta_pct", None)
    btc_delta_str = f"{btc_delta:+.2f}%" if btc_delta is not None else "—"

    btc_total = d["btc_yes"] + d["btc_no"]
    btc_yes_pct = d["btc_yes"]/max(1,btc_total)*100
    btc_fade_wr = d["btc_fade_win"]/max(1,d["btc_fade_sig"])*100 if d["btc_fade_sig"] else 0

    agg_total = d["yes"] + d["no"]
    agg_yes_pct = d["yes"]/max(1,agg_total)*100
    agg_fade_wr = d["fade_wins"]/max(1,d["fade_signals"])*100 if d["fade_signals"] else 0

    btc_fade_str = f"{btc_fade_wr:.0f}% ({d['btc_fade_sig']})"
    print(f"{day:<11} | {btc_delta_str:>8} | {btc_yes_pct:>7.0f}% | {btc_fade_str:>10} | {agg_yes_pct:>7.0f}% | {agg_fade_wr:>10.0f}% | {d['fade_signals']:>10}")

# Bucket by BTC regime
print(f"\n{'='*120}")
print(f"REGIME BUCKETS")
print(f"{'='*120}")
buckets = {
    "STRONG DOWN (≤-1%)": [],
    "MILD DOWN (-1 to -0.2%)": [],
    "FLAT (-0.2 to +0.2%)": [],
    "MILD UP (+0.2 to +1%)": [],
    "STRONG UP (≥+1%)": [],
}
for day in days:
    btc = btc_daily.get(day)
    if not btc: continue
    delta = btc["delta_pct"]
    d = day_data[day]
    if d["fade_signals"] == 0: continue
    if delta <= -1: bucket = "STRONG DOWN (≤-1%)"
    elif delta <= -0.2: bucket = "MILD DOWN (-1 to -0.2%)"
    elif delta < 0.2: bucket = "FLAT (-0.2 to +0.2%)"
    elif delta < 1: bucket = "MILD UP (+0.2 to +1%)"
    else: bucket = "STRONG UP (≥+1%)"
    buckets[bucket].append(d)

print(f"{'Regime':<25} | {'Days':>5} | {'Sigs':>6} | {'WR':>6} | {'YES%':>6}")
for label, days_list in buckets.items():
    if not days_list: continue
    total_sigs = sum(d["fade_signals"] for d in days_list)
    total_wins = sum(d["fade_wins"] for d in days_list)
    total_yes = sum(d["yes"] for d in days_list)
    total_n = sum(d["yes"] + d["no"] for d in days_list)
    wr = total_wins/max(1,total_sigs)*100
    yes_pct = total_yes/max(1,total_n)*100
    print(f"{label:<25} | {len(days_list):>5} | {total_sigs:>6} | {wr:>5.0f}% | {yes_pct:>5.0f}%")

# BTC-only regime test
print(f"\n{'='*120}")
print(f"BTC-ONLY: BTC fade WR by BTC daily move")
print(f"{'='*120}")
btc_buckets = {
    "STRONG DOWN (≤-1%)": [],
    "MILD DOWN (-1 to -0.2%)": [],
    "FLAT (-0.2 to +0.2%)": [],
    "MILD UP (+0.2 to +1%)": [],
    "STRONG UP (≥+1%)": [],
}
for day in days:
    btc = btc_daily.get(day)
    if not btc: continue
    delta = btc["delta_pct"]
    d = day_data[day]
    if d["btc_fade_sig"] == 0: continue
    if delta <= -1: bucket = "STRONG DOWN (≤-1%)"
    elif delta <= -0.2: bucket = "MILD DOWN (-1 to -0.2%)"
    elif delta < 0.2: bucket = "FLAT (-0.2 to +0.2%)"
    elif delta < 1: bucket = "MILD UP (+0.2 to +1%)"
    else: bucket = "STRONG UP (≥+1%)"
    btc_buckets[bucket].append(d)

print(f"{'Regime':<25} | {'Days':>5} | {'Sigs':>6} | {'WR':>6}")
for label, days_list in btc_buckets.items():
    if not days_list: continue
    total_sigs = sum(d["btc_fade_sig"] for d in days_list)
    total_wins = sum(d["btc_fade_win"] for d in days_list)
    wr = total_wins/max(1,total_sigs)*100
    print(f"{label:<25} | {len(days_list):>5} | {total_sigs:>6} | {wr:>5.0f}%")
