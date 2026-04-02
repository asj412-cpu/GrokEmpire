"""
Monitor 4 complete 15-min cycles — all 7 coins, every WS tick.
Saves raw CSV + prints per-cycle summaries.
"""

import asyncio
import json
import time
import base64
import os
import csv
from datetime import datetime, timezone
from collections import defaultdict

import websockets
import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend
from dotenv import load_dotenv

load_dotenv(override=True)

KALSHI_WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"
KALSHI_API = "https://api.elections.kalshi.com/trade-api/v2"
KEY_ID = os.getenv("KALSHI_KEY_ID")
KEY_PATH = os.getenv("KALSHI_PRIVATE_KEY")

ALL_COINS = {
    "BTC": "KXBTC15M", "ETH": "KXETH15M", "SOL": "KXSOL15M",
    "XRP": "KXXRP15M", "BNB": "KXBNB15M", "HYPE": "KXHYPE15M", "DOGE": "KXDOGE15M",
}

OUT_CSV = "tests/4cycle_tick_data.csv"
all_rows = []
cycle_num = 0
last_minute_mark = None


def generate_signature(timestamp_ms):
    with open(KEY_PATH, 'rb') as f:
        pk = serialization.load_pem_private_key(f.read(), password=None, backend=default_backend())
    msg = f"{timestamp_ms}GET/trade-api/ws/v2"
    sig = pk.sign(msg.encode(), padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH), hashes.SHA256())
    return base64.b64encode(sig).decode()


def get_open_tickers():
    tickers = {}
    for coin, series in ALL_COINS.items():
        try:
            r = requests.get(f"{KALSHI_API}/markets?series_ticker={series}&status=open&limit=1")
            mkts = r.json().get("markets", [])
            if mkts:
                tickers[coin] = mkts[0]["ticker"]
        except:
            pass
    return tickers


def print_cycle_summary(cycle, data_by_coin):
    print(f"\n{'='*90}")
    print(f"  CYCLE {cycle} SUMMARY")
    print(f"{'='*90}")
    print(f"  {'Coin':<5} | {'Ticks':>5} | {'YES low':>7} | {'YES high':>8} | {'YES swing':>9} | {'NO low':>6} | {'NO high':>7} | {'NO swing':>8} | {'5-40c YES':>9} | {'5-40c NO':>8}")
    print(f"  {'-'*5}-+-{'-'*5}-+-{'-'*7}-+-{'-'*8}-+-{'-'*9}-+-{'-'*6}-+-{'-'*7}-+-{'-'*8}-+-{'-'*9}-+-{'-'*8}")

    for coin in ALL_COINS:
        pts = data_by_coin.get(coin, [])
        if not pts:
            continue
        ya = [p[2] for p in pts]  # yes_ask
        nc = [p[4] for p in pts]  # no_cost
        yb = [p[1] for p in pts]  # yes_bid

        yes_lo, yes_hi = min(ya), max(ya)
        no_lo, no_hi = min(nc), max(nc)
        yes_zone = sum(1 for v in ya if 5 <= v <= 40)
        no_zone = sum(1 for v in nc if 5 <= v <= 40)
        yes_pct = f"{yes_zone/len(pts)*100:.0f}%" if pts else "0%"
        no_pct = f"{no_zone/len(pts)*100:.0f}%" if pts else "0%"

        print(f"  {coin:<5} | {len(pts):>5} | {yes_lo:>5}c | {yes_hi:>6}c | {yes_hi-yes_lo:>7}c | {no_lo:>4}c | {no_hi:>5}c | {no_hi-no_lo:>6}c | {yes_zone:>4} ({yes_pct:>3}) | {no_zone:>3} ({no_pct:>3})")

    # Yoyo check
    print(f"\n  Yoyo detection (YES crossed both ≤40c AND ≥60c in same cycle):")
    for coin in ALL_COINS:
        pts = data_by_coin.get(coin, [])
        if not pts:
            continue
        ya = [p[2] for p in pts]
        low = any(v <= 40 for v in ya)
        high = any(v >= 60 for v in ya)
        yoyo = "✓ YOYO" if low and high else "—"
        print(f"    {coin}: {yoyo}  (YES range {min(ya)}c-{max(ya)}c)")


async def monitor():
    global cycle_num, last_minute_mark

    ts_ms = int(time.time() * 1000)
    sig = generate_signature(ts_ms)
    headers = {
        "KALSHI-ACCESS-KEY": KEY_ID,
        "KALSHI-ACCESS-SIGNATURE": sig,
        "KALSHI-ACCESS-TIMESTAMP": str(ts_ms),
    }

    current_tickers = get_open_tickers()
    ticker_to_coin = {v: k for k, v in current_tickers.items()}

    # Wait for next cycle boundary
    now = datetime.now(timezone.utc)
    min_in_cycle = now.minute % 15
    if min_in_cycle > 0:
        wait_secs = (15 - min_in_cycle) * 60 - now.second
        print(f"Waiting {wait_secs}s for next cycle boundary...")
        await asyncio.sleep(wait_secs + 2)

    current_tickers = get_open_tickers()
    ticker_to_coin = {v: k for k, v in current_tickers.items()}
    print(f"Starting: {list(current_tickers.keys())}")

    cycle_data = defaultdict(list)
    cycle_num = 1
    prev_min_left = 15

    async with websockets.connect(KALSHI_WS_URL, additional_headers=headers, ping_interval=30, ping_timeout=10) as ws:
        await ws.send(json.dumps({
            "id": 1, "cmd": "subscribe",
            "params": {"channels": ["ticker"], "market_tickers": list(current_tickers.values())}
        }))
        print(f"✅ Cycle {cycle_num} — subscribed to {len(current_tickers)} tickers")

        async for msg in ws:
            data = json.loads(msg)
            if data.get("type") != "ticker":
                continue

            m = data.get("msg", {})
            ticker = m.get("market_ticker", "")
            coin = ticker_to_coin.get(ticker)
            if not coin:
                continue

            try:
                yes_bid = int(float(m.get("yes_bid_dollars", "0")) * 100)
                yes_ask = int(float(m.get("yes_ask_dollars", "0")) * 100)
            except:
                continue

            no_cost = 100 - yes_bid
            mid = (yes_bid + yes_ask) // 2
            now = datetime.now(timezone.utc)
            min_left = 15 - (now.minute % 15)
            ts_str = now.strftime("%H:%M:%S.%f")[:-3]

            row = (ts_str, yes_bid, yes_ask, mid, no_cost, min_left)
            cycle_data[coin].append(row)
            all_rows.append((cycle_num, ts_str, coin, ticker, yes_bid, yes_ask, mid, no_cost, min_left))

            # Detect cycle boundary: min_left jumped back to 15
            if min_left >= 14 and prev_min_left <= 1:
                # Print summary of completed cycle
                print_cycle_summary(cycle_num, cycle_data)

                cycle_num += 1
                if cycle_num > 4:
                    break

                # Rotate tickers
                cycle_data = defaultdict(list)
                new_tickers = get_open_tickers()
                if new_tickers != current_tickers:
                    current_tickers = new_tickers
                    ticker_to_coin = {v: k for k, v in current_tickers.items()}
                    await ws.send(json.dumps({
                        "id": cycle_num + 10, "cmd": "subscribe",
                        "params": {"channels": ["ticker"], "market_tickers": list(current_tickers.values())}
                    }))
                print(f"\n✅ Cycle {cycle_num} — subscribed")

            prev_min_left = min_left

    # Save CSV
    with open(OUT_CSV, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["cycle", "time", "coin", "ticker", "yes_bid", "yes_ask", "mid", "no_cost", "min_left"])
        for r in all_rows:
            w.writerow(r)
    print(f"\n📁 Saved {len(all_rows)} rows to {OUT_CSV}")

    # Final aggregate
    print(f"\n{'='*90}")
    print(f"  FINAL AGGREGATE — {cycle_num - 1} cycles, {len(all_rows)} ticks")
    print(f"{'='*90}")
    for coin in ALL_COINS:
        pts = [r for r in all_rows if r[2] == coin]
        if not pts:
            continue
        ya = [p[5] for p in pts]
        nc = [p[7] for p in pts]
        yoyos = 0
        # Count yoyos per cycle
        for c in range(1, cycle_num):
            cpts = [p[5] for p in pts if p[0] == c]
            if cpts and any(v <= 40 for v in cpts) and any(v >= 60 for v in cpts):
                yoyos += 1
        print(f"  {coin}: {len(pts)} ticks | YES {min(ya)}-{max(ya)}c | NO {min(nc)}-{max(nc)}c | Yoyos: {yoyos}/{cycle_num-1} cycles")


if __name__ == "__main__":
    asyncio.run(monitor())
