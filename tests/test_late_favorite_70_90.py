"""
Backtest: <5 min remaining, buy the favored side if its cost ∈ [70, 90c].
Per-coin breakdown.

For each settled market in the last 5 days:
1. Fetch 1-min candlesticks around "5 min before close"
2. Read yes_bid + yes_ask at that candle
3. Determine favorite (yes_mid > 50 → YES, else NO)
4. Cost to buy favorite = yes_ask if YES, else (100 - yes_bid)
5. Filter: only trades where cost ∈ [70, 90]
6. Win = (favorite == result)
7. PnL = (100 - cost) if win, else -cost
"""
import requests, time
from datetime import datetime, timezone
from collections import defaultdict

API = "https://api.elections.kalshi.com/trade-api/v2"
COINS = ["BTC", "ETH", "SOL", "XRP", "BNB", "HYPE", "DOGE"]
DAYS = 5
COST_LO, COST_HI = 70, 90
MIN_REMAINING_SEC = 300  # "5 min remaining"

end = int(datetime.now(timezone.utc).timestamp())
start = end - (DAYS * 86400)

print(f"Period: {datetime.fromtimestamp(start, tz=timezone.utc).strftime('%Y-%m-%d')} → {datetime.fromtimestamp(end, tz=timezone.utc).strftime('%Y-%m-%d')}")
print(f"Filter: buy favored side at <5 min remaining if cost ∈ [{COST_LO}, {COST_HI}]c\n")

session = requests.Session()

def get_with_retry(url, attempts=4):
    for i in range(attempts):
        try:
            r = session.get(url, timeout=15)
            if r.status_code == 429:
                time.sleep(2 + i * 2)
                continue
            return r.json()
        except Exception:
            time.sleep(1 + i)
    return {}

def fetch_settled(series):
    markets, cursor = [], ""
    while True:
        url = f"{API}/markets?series_ticker={series}&status=settled&limit=100&min_close_ts={start}&max_close_ts={end}"
        if cursor:
            url += f"&cursor={cursor}"
        d = get_with_retry(url)
        batch = d.get("markets", [])
        if not batch:
            break
        markets.extend(batch)
        cursor = d.get("cursor", "")
        if not cursor:
            break
        time.sleep(0.15)
    return markets

def fetch_candle_at(series, ticker, target_ts):
    """Fetch 1-min candles in a wider window; pick the latest candle at or before target_ts."""
    s_ts = target_ts - 240   # widen window to catch low-volume markets
    e_ts = target_ts + 30
    url = f"{API}/series/{series}/markets/{ticker}/candlesticks?start_ts={s_ts}&end_ts={e_ts}&period_interval=1"
    d = get_with_retry(url)
    candles = d.get("candlesticks", [])
    if not candles:
        return None
    # Pick the LATEST candle whose end_period_ts <= target_ts + 30
    eligible = [c for c in candles if int(c.get("end_period_ts", 0)) <= target_ts + 30]
    best = eligible[-1] if eligible else candles[-1]
    yes_bid = float(best.get("yes_bid", {}).get("close_dollars", 0) or 0) * 100
    yes_ask = float(best.get("yes_ask", {}).get("close_dollars", 0) or 0) * 100
    if yes_bid <= 0 or yes_ask <= 0 or yes_ask > 100:
        return None
    return yes_bid, yes_ask

results_per_coin = defaultdict(lambda: {"trades": 0, "wins": 0, "pnl": 0, "checked": 0, "no_data": 0})

for coin in COINS:
    series = f"KX{coin}15M"
    markets = fetch_settled(series)
    print(f"  {coin}: {len(markets)} settled markets... ", end="", flush=True)

    for m in markets:
        result = m.get("result", "")
        if result not in ("yes", "no"):
            continue
        ticker = m.get("ticker", "")
        close_time_str = m.get("close_time", "")
        try:
            close_ts = int(datetime.fromisoformat(close_time_str.replace("Z", "+00:00")).timestamp())
        except Exception:
            continue
        target_ts = close_ts - MIN_REMAINING_SEC

        results_per_coin[coin]["checked"] += 1
        candle = fetch_candle_at(series, ticker, target_ts)
        if candle is None:
            results_per_coin[coin]["no_data"] += 1
            continue
        yes_bid, yes_ask = candle
        yes_mid = (yes_bid + yes_ask) / 2

        # Determine favorite + cost to buy it
        if yes_mid > 50:
            fav_side = "yes"
            cost = yes_ask  # cross the spread to buy YES
        else:
            fav_side = "no"
            cost = 100 - yes_bid  # cross the spread to buy NO

        if not (COST_LO <= cost <= COST_HI):
            continue

        won = (fav_side == result)
        pnl = (100 - cost) if won else -cost
        results_per_coin[coin]["trades"] += 1
        if won:
            results_per_coin[coin]["wins"] += 1
        results_per_coin[coin]["pnl"] += pnl

        time.sleep(0.08)  # rate limit

    r = results_per_coin[coin]
    print(f"checked {r['checked']}, no_data {r['no_data']}, qualified {r['trades']}")
    time.sleep(1.0)  # cool-down between coins

print(f"\n{'='*100}")
print(f"PER-COIN — buy favorite at <5 min remaining, cost ∈ [{COST_LO}, {COST_HI}]c")
print(f"{'='*100}")
print(f"{'Coin':<6} | {'Trades':>7} | {'Wins':>5} | {'WR%':>6} | {'Total PnL':>11} | {'Avg/trade':>11} | {'Markets':>8}")
print("-" * 100)

total_t = total_w = total_pnl = 0
for coin in COINS:
    r = results_per_coin[coin]
    if r["trades"] == 0:
        print(f"{coin:<6} | {0:>7} | {0:>5} | {'—':>6} | {'—':>11} | {'—':>11} | {r['checked']:>8}")
        continue
    wr = r["wins"] / r["trades"] * 100
    avg = r["pnl"] / r["trades"]
    print(f"{coin:<6} | {r['trades']:>7} | {r['wins']:>5} | {wr:>5.1f}% | {r['pnl']:>+9.0f}c | {avg:>+9.1f}c | {r['checked']:>8}")
    total_t += r["trades"]
    total_w += r["wins"]
    total_pnl += r["pnl"]

print("-" * 100)
if total_t:
    print(f"{'TOTAL':<6} | {total_t:>7} | {total_w:>5} | {total_w/total_t*100:>5.1f}% | {total_pnl:>+9.0f}c | {total_pnl/total_t:>+9.1f}c |")
