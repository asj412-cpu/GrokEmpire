"""
Sweep: buy favored side at late cycle, varying (price band, min remaining).
Per-coin best-config selection. Net of Kalshi fees + 1c slippage on entry.

Kalshi fee formula (binary): fee = ceil(7 * P * (1-P)) cents, where P is dollar price.
"""
import requests, time, math
from datetime import datetime, timezone
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

API = "https://api.elections.kalshi.com/trade-api/v2"
COINS = ["BTC", "ETH", "SOL", "XRP", "BNB", "HYPE", "DOGE"]
DAYS = 5
SLIPPAGE_C = 1  # add 1c to entry price (cross the spread realistically)

# Bands and min-remaining combos to sweep
BANDS = [
    (60, 80), (65, 85), (65, 95), (70, 90), (72, 88),
    (75, 85), (78, 88), (78, 92), (80, 92), (80, 95),
]
MIN_REMAINING = [3, 4, 5, 6, 7]  # minutes

end = int(datetime.now(timezone.utc).timestamp())
start = end - (DAYS * 86400)

print(f"Period: {datetime.fromtimestamp(start, tz=timezone.utc).strftime('%Y-%m-%d')} → {datetime.fromtimestamp(end, tz=timezone.utc).strftime('%Y-%m-%d')}")
print(f"Sweep: {len(BANDS)} bands × {len(MIN_REMAINING)} mins-remaining = {len(BANDS)*len(MIN_REMAINING)} configs/coin")
print(f"Net of Kalshi fees + {SLIPPAGE_C}c entry slippage\n")

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
    """Pull 1-min candles from min 5 to min 14 of the cycle (covers 1-10 min remaining)."""
    s_ts = close_ts - 660  # 11 min before close
    e_ts = close_ts - 30   # 30s before close
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
    """Binary market fee: ceil(7 * P * (1-P)) cents, P in dollars."""
    p = price_c / 100.0
    return math.ceil(7 * p * (1 - p))

def find_candle_at(candles, target_ts):
    """Return latest candle with end_period_ts <= target_ts + 30."""
    eligible = [c for c in candles if c[0] <= target_ts + 30]
    if not eligible:
        return None
    return eligible[-1]  # latest one

def simulate(market_data, band_lo, band_hi, min_rem_sec):
    """Returns (qualified, won, pnl_cents_net)."""
    close_ts, result, candles = market_data
    if result not in ("yes", "no"):
        return False, False, 0
    target_ts = close_ts - min_rem_sec
    candle = find_candle_at(candles, target_ts)
    if candle is None:
        return False, False, 0
    _, yb, ya = candle
    yes_mid = (yb + ya) / 2
    if yes_mid > 50:
        fav = "yes"
        cost = ya  # cross the spread
    else:
        fav = "no"
        cost = 100 - yb
    cost += SLIPPAGE_C
    if not (band_lo <= cost <= band_hi):
        return False, False, 0
    fee = kalshi_fee_cents(cost)
    won = (fav == result)
    if won:
        pnl = (100 - cost) - fee
    else:
        pnl = -cost - fee
    return True, won, pnl

# ─── Fetch everything (parallel) ─────────────────────────────────
all_data = {}  # coin -> list of (close_ts, result, candles)

for coin in COINS:
    series = f"KX{coin}15M"
    print(f"  {coin}: fetching markets...", end=" ", flush=True)
    markets = fetch_settled(series)
    print(f"{len(markets)} markets, fetching candles in parallel...", end=" ", flush=True)
    t0 = time.time()
    coin_data = []

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

    with ThreadPoolExecutor(max_workers=12) as ex:
        futures = [ex.submit(task, m) for m in markets]
        for f in as_completed(futures):
            r = f.result()
            if r:
                coin_data.append(r)

    all_data[coin] = coin_data
    print(f"{len(coin_data)} usable in {time.time()-t0:.0f}s")
    time.sleep(0.5)

# ─── Sweep ────────────────────────────────────────────────────────
print(f"\n{'='*120}")
print(f"PER-COIN SWEEP — best (band × min-remaining) by NET PnL")
print(f"{'='*120}")

best_per_coin = {}
all_results_per_coin = defaultdict(list)

for coin in COINS:
    data = all_data.get(coin, [])
    if not data:
        continue
    for band_lo, band_hi in BANDS:
        for mr in MIN_REMAINING:
            mr_sec = mr * 60
            t = w = pnl = 0
            for md in data:
                q, won, p = simulate(md, band_lo, band_hi, mr_sec)
                if q:
                    t += 1
                    if won:
                        w += 1
                    pnl += p
            if t < 30:
                continue
            wr = w / t * 100
            avg = pnl / t
            all_results_per_coin[coin].append((band_lo, band_hi, mr, t, w, wr, pnl, avg))

# Print per-coin top 5 + overall best
for coin in COINS:
    rows = all_results_per_coin.get(coin, [])
    if not rows:
        print(f"\n  {coin}: no qualifying configs")
        continue
    rows.sort(key=lambda r: r[6], reverse=True)  # by total pnl
    print(f"\n  {coin} — top 5 by net PnL:")
    print(f"    {'Band':<10} | {'Min':>4} | {'Trades':>7} | {'Win%':>6} | {'Net PnL':>9} | {'Avg/trd':>9}")
    for band_lo, band_hi, mr, t, w, wr, pnl, avg in rows[:5]:
        band_str = f"{band_lo}-{band_hi}c"
        print(f"    {band_str:<10} | {mr:>3}m | {t:>7} | {wr:>5.1f}% | {pnl:>+7.0f}c | {avg:>+7.2f}c")
    best_per_coin[coin] = rows[0]

# ─── Recommended config ──────────────────────────────────────────
print(f"\n{'='*120}")
print(f"RECOMMENDED PER-COIN — net of fees + slippage (last {DAYS} days)")
print(f"{'='*120}")
print(f"{'Coin':<6} | {'Band':<10} | {'Min Rem':>8} | {'Trades':>7} | {'Win%':>6} | {'Net PnL':>9} | {'Avg/trd':>9}")
print("-" * 90)
total_pnl = total_t = total_w = 0
profitable_coins = []
for coin in COINS:
    if coin not in best_per_coin:
        print(f"{coin:<6} | {'—':<10} | {'—':>8} | {'—':>7} | {'—':>6} | {'—':>9} | {'—':>9}")
        continue
    band_lo, band_hi, mr, t, w, wr, pnl, avg = best_per_coin[coin]
    band_str = f"{band_lo}-{band_hi}c"
    marker = " ✓" if pnl > 0 else " ✗"
    print(f"{coin:<6} | {band_str:<10} | {mr:>6}m  | {t:>7} | {wr:>5.1f}% | {pnl:>+7.0f}c | {avg:>+7.2f}c{marker}")
    if pnl > 0:
        profitable_coins.append((coin, band_lo, band_hi, mr, pnl))
        total_pnl += pnl
        total_t += t
        total_w += w

print("-" * 90)
if total_t:
    print(f"{'PROFITABLE':<6}   {len(profitable_coins)} coins   |   {total_t:>7} | {total_w/total_t*100:>5.1f}% | {total_pnl:>+7.0f}c | {total_pnl/total_t:>+7.2f}c")
print(f"\n→ Estimated daily PnL (profitable coins only): {total_pnl/DAYS:+.0f}c/day = ${total_pnl/DAYS/100:+.2f}/day")
