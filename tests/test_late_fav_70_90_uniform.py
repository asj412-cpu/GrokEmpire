"""
One-size-fits-all: 70-90c band, all 7 coins, at 3/4/5 min remaining.
Net of Kalshi fees + 1c slippage.
"""
import requests, time, math
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

API = "https://api.elections.kalshi.com/trade-api/v2"
COINS = ["BTC", "ETH", "SOL", "XRP", "BNB", "HYPE", "DOGE"]
DAYS = 5
SLIPPAGE_C = 1
BAND_LO, BAND_HI = 70, 90
MIN_REMAINING = [3, 4, 5]

end = int(datetime.now(timezone.utc).timestamp())
start = end - (DAYS * 86400)

print(f"Period: {datetime.fromtimestamp(start, tz=timezone.utc).strftime('%Y-%m-%d')} → {datetime.fromtimestamp(end, tz=timezone.utc).strftime('%Y-%m-%d')}")
print(f"Uniform config: {BAND_LO}-{BAND_HI}c at 3/4/5 min remaining, net of fees + {SLIPPAGE_C}c slippage\n")

session = requests.Session()

def get_with_retry(url, attempts=4):
    for i in range(attempts):
        try:
            r = session.get(url, timeout=15)
            if r.status_code == 429:
                time.sleep(1 + i * 2)
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
    return markets

def fetch_candles(series, ticker, close_ts):
    s_ts = close_ts - 420
    e_ts = close_ts - 120
    url = f"{API}/series/{series}/markets/{ticker}/candlesticks?start_ts={s_ts}&end_ts={e_ts}&period_interval=1"
    d = get_with_retry(url)
    candles = d.get("candlesticks", [])
    parsed = []
    for c in candles:
        try:
            ts = int(c.get("end_period_ts", 0))
            yb = float(c.get("yes_bid", {}).get("close_dollars", 0) or 0) * 100
            ya = float(c.get("yes_ask", {}).get("close_dollars", 0) or 0) * 100
            if 0 < yb <= 100 and 0 < ya <= 100:
                parsed.append((ts, yb, ya))
        except Exception:
            pass
    return parsed

def kalshi_fee_cents(price_c):
    p = price_c / 100.0
    return math.ceil(7 * p * (1 - p))

def find_candle_at(candles, target_ts):
    eligible = [c for c in candles if c[0] <= target_ts + 30]
    if not eligible:
        return None
    return eligible[-1]

def simulate(close_ts, result, candles, min_rem_sec):
    target_ts = close_ts - min_rem_sec
    candle = find_candle_at(candles, target_ts)
    if candle is None:
        return False, False, 0
    _, yb, ya = candle
    yes_mid = (yb + ya) / 2
    if yes_mid > 50:
        fav, cost = "yes", ya
    else:
        fav, cost = "no", 100 - yb
    cost += SLIPPAGE_C
    if not (BAND_LO <= cost <= BAND_HI):
        return False, False, 0
    fee = kalshi_fee_cents(cost)
    won = (fav == result)
    pnl = (100 - cost - fee) if won else (-cost - fee)
    return True, won, pnl

# Fetch all data
all_data = {}
for coin in COINS:
    series = f"KX{coin}15M"
    print(f"  {coin}: ", end="", flush=True)
    markets = fetch_settled(series)
    print(f"{len(markets)} markets, candles...", end=" ", flush=True)

    def task(m):
        result = m.get("result", "")
        if result not in ("yes", "no"):
            return None
        ticker = m.get("ticker", "")
        close_time_str = m.get("close_time", "")
        try:
            close_ts = int(datetime.fromisoformat(close_time_str.replace("Z", "+00:00")).timestamp())
        except Exception:
            return None
        candles = fetch_candles(series, ticker, close_ts)
        if not candles:
            return None
        return (close_ts, result, candles)

    coin_data = []
    with ThreadPoolExecutor(max_workers=12) as ex:
        futures = [ex.submit(task, m) for m in markets]
        for f in as_completed(futures):
            r = f.result()
            if r:
                coin_data.append(r)
    all_data[coin] = coin_data
    print(f"{len(coin_data)} usable")
    time.sleep(0.3)

# Run for each min-remaining
for mr in MIN_REMAINING:
    print(f"\n{'='*100}")
    print(f"  UNIFORM 70-90c @ {mr} min remaining (net of fees + {SLIPPAGE_C}c slippage)")
    print(f"{'='*100}")
    print(f"  {'Coin':<6} | {'Trades':>7} | {'Wins':>5} | {'WR%':>6} | {'Net PnL':>10} | {'Avg/trd':>10}")
    print("  " + "-" * 70)
    total_t = total_w = total_pnl = 0
    for coin in COINS:
        t = w = pnl = 0
        for close_ts, result, candles in all_data.get(coin, []):
            q, won, p = simulate(close_ts, result, candles, mr * 60)
            if q:
                t += 1
                if won:
                    w += 1
                pnl += p
        if t == 0:
            print(f"  {coin:<6} | {0:>7} | {0:>5} | {'—':>6} | {'—':>10} | {'—':>10}")
            continue
        wr = w / t * 100
        avg = pnl / t
        marker = " ✓" if pnl > 0 else " ✗"
        print(f"  {coin:<6} | {t:>7} | {w:>5} | {wr:>5.1f}% | {pnl:>+8.0f}c | {avg:>+8.2f}c{marker}")
        total_t += t
        total_w += w
        total_pnl += pnl
    print("  " + "-" * 70)
    if total_t:
        print(f"  {'TOTAL':<6} | {total_t:>7} | {total_w:>5} | {total_w/total_t*100:>5.1f}% | {total_pnl:>+8.0f}c | {total_pnl/total_t:>+8.2f}c")
        print(f"  → ${total_pnl/100:.2f} over {DAYS} days = ${total_pnl/DAYS/100:.2f}/day")

# Excluding HYPE
print(f"\n{'='*100}")
print(f"  EXCLUDING HYPE (since it's structurally bad)")
print(f"{'='*100}")
for mr in MIN_REMAINING:
    total_t = total_w = total_pnl = 0
    for coin in COINS:
        if coin == "HYPE":
            continue
        for close_ts, result, candles in all_data.get(coin, []):
            q, won, p = simulate(close_ts, result, candles, mr * 60)
            if q:
                total_t += 1
                if won:
                    total_w += 1
                total_pnl += p
    if total_t:
        print(f"  @ {mr} min remaining: {total_t} trades, {total_w/total_t*100:.1f}% WR, {total_pnl:+.0f}c (${total_pnl/100:.2f}) → ${total_pnl/DAYS/100:.2f}/day")
