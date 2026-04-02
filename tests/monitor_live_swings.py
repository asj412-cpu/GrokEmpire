"""
Live monitor: Record second-by-second Kalshi 15m contract prices.
Watches BTC, ETH, SOL for 4 cycles (1 hour) and records every price change.
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

WATCH_COINS = {
    "BTC": "KXBTC15M",
    "ETH": "KXETH15M",
    "SOL": "KXSOL15M",
}

# Track all price updates
price_log = []  # list of (timestamp, coin, ticker, yes_bid, yes_ask, yes_cost, no_cost, min_left)
cycle_summaries = []


def generate_signature(timestamp_ms):
    with open(KEY_PATH, 'rb') as f:
        pk = serialization.load_pem_private_key(f.read(), password=None, backend=default_backend())
    message = f"{timestamp_ms}GET/trade-api/ws/v2"
    sig = pk.sign(message.encode(), padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH), hashes.SHA256())
    return base64.b64encode(sig).decode()


def get_open_tickers():
    """Get current open market tickers."""
    tickers = {}
    for coin, series in WATCH_COINS.items():
        r = requests.get(f"{KALSHI_API}/markets?series_ticker={series}&status=open&limit=1")
        mkts = r.json().get("markets", [])
        if mkts:
            tickers[coin] = mkts[0]["ticker"]
    return tickers


async def monitor():
    ts_ms = int(time.time() * 1000)
    sig = generate_signature(ts_ms)
    headers = {
        "KALSHI-ACCESS-KEY": KEY_ID,
        "KALSHI-ACCESS-SIGNATURE": sig,
        "KALSHI-ACCESS-TIMESTAMP": str(ts_ms),
    }

    current_tickers = get_open_tickers()
    ticker_to_coin = {v: k for k, v in current_tickers.items()}
    print(f"Monitoring: {current_tickers}")

    # Per-cycle tracking
    cycle_data = defaultdict(list)  # coin -> list of (time, yes_cost, no_cost)
    cycle_start = datetime.now(timezone.utc)
    cycles_completed = 0

    async with websockets.connect(KALSHI_WS_URL, additional_headers=headers, ping_interval=30) as ws:
        # Subscribe
        await ws.send(json.dumps({
            "id": 1, "cmd": "subscribe",
            "params": {"channels": ["ticker"], "market_tickers": list(current_tickers.values())}
        }))
        print("✅ Connected & subscribed")
        print(f"{'Time':>12} | {'Coin':>4} | {'YES bid':>7} | {'YES ask':>7} | {'NO cost':>7} | {'Mid':>5} | {'Min':>3}")
        print("-" * 60)

        async for msg in ws:
            data = json.loads(msg)
            if data.get("type") != "ticker":
                continue

            m = data.get("msg", {})
            ticker = m.get("market_ticker", "")
            coin = ticker_to_coin.get(ticker)
            if not coin:
                # Check if markets rotated
                new_tickers = get_open_tickers()
                if new_tickers != current_tickers:
                    # Print cycle summary before rotating
                    if cycle_data:
                        print(f"\n{'='*60}")
                        print(f"  CYCLE SUMMARY #{cycles_completed + 1}")
                        print(f"{'='*60}")
                        for c, points in cycle_data.items():
                            if not points:
                                continue
                            yes_costs = [p[1] for p in points]
                            no_costs = [p[2] for p in points]
                            print(f"  {c}: {len(points)} updates")
                            print(f"    YES: low={min(yes_costs)}c high={max(yes_costs)}c range={max(yes_costs)-min(yes_costs)}c")
                            print(f"    NO:  low={min(no_costs)}c high={max(no_costs)}c range={max(no_costs)-min(no_costs)}c")

                            # Find best entry/exit points
                            yes_low = min(yes_costs)
                            yes_high = max(yes_costs)
                            no_low = min(no_costs)
                            no_high = max(no_costs)
                            print(f"    Best YES trade: buy {yes_low}c → could sell {yes_high}c = +{yes_high-yes_low}c")
                            print(f"    Best NO trade:  buy {no_low}c → could sell {no_high}c = +{no_high-no_low}c")

                            cycle_summaries.append({
                                "cycle": cycles_completed + 1,
                                "coin": c,
                                "yes_low": yes_low, "yes_high": yes_high,
                                "no_low": no_low, "no_high": no_high,
                                "updates": len(points),
                            })
                        print(f"{'='*60}\n")

                    cycles_completed += 1
                    if cycles_completed >= 4:
                        break

                    # Rotate subscriptions
                    old_tickers = list(current_tickers.values())
                    current_tickers = new_tickers
                    ticker_to_coin = {v: k for k, v in current_tickers.items()}
                    cycle_data = defaultdict(list)

                    # Unsubscribe old, subscribe new
                    await ws.send(json.dumps({
                        "id": 10 + cycles_completed, "cmd": "subscribe",
                        "params": {"channels": ["ticker"], "market_tickers": list(current_tickers.values())}
                    }))
                    print(f"\n🔄 New cycle #{cycles_completed + 1}: {current_tickers}")
                continue

            try:
                yes_bid = int(float(m.get("yes_bid_dollars", "0")) * 100)
                yes_ask = int(float(m.get("yes_ask_dollars", "0")) * 100)
            except (ValueError, TypeError):
                continue

            no_cost = 100 - yes_bid
            yes_cost = yes_ask
            mid = (yes_bid + yes_ask) // 2
            now = datetime.now(timezone.utc)
            min_left = 15 - (now.minute % 15)
            ts_str = now.strftime("%H:%M:%S")

            cycle_data[coin].append((ts_str, yes_cost, no_cost))
            price_log.append((ts_str, coin, ticker, yes_bid, yes_ask, yes_cost, no_cost, min_left))

            # Only print when price changes significantly or is in our entry zone
            if yes_cost <= 40 or no_cost <= 40:
                marker = " ◀ ENTRY ZONE" if 5 <= yes_cost <= 40 or 5 <= no_cost <= 40 else ""
                print(f"{ts_str:>12} | {coin:>4} | {yes_bid:>5}c | {yes_ask:>5}c | {no_cost:>5}c | {mid:>3}c | {min_left:>3}{marker}")

    # Final summary
    print(f"\n{'='*60}")
    print(f"  FINAL REPORT — {cycles_completed} cycles monitored")
    print(f"{'='*60}")
    print(f"  {'Coin':>4} | {'Cycle':>5} | {'YES range':>10} | {'NO range':>10} | {'Best YES':>9} | {'Best NO':>8}")
    print(f"  {'-'*4}-+-{'-'*5}-+-{'-'*10}-+-{'-'*10}-+-{'-'*9}-+-{'-'*8}")
    for s in cycle_summaries:
        yr = f"{s['yes_low']}-{s['yes_high']}c"
        nr = f"{s['no_low']}-{s['no_high']}c"
        print(f"  {s['coin']:>4} | {s['cycle']:>5} | {yr:>10} | {nr:>10} | +{s['yes_high']-s['yes_low']:>3}c    | +{s['no_high']-s['no_low']:>3}c")

    # Save raw data
    with open("tests/live_swing_data.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["time", "coin", "ticker", "yes_bid", "yes_ask", "yes_cost", "no_cost", "min_left"])
        for row in price_log:
            w.writerow(row)
    print(f"\nRaw data saved to tests/live_swing_data.csv ({len(price_log)} rows)")


if __name__ == "__main__":
    asyncio.run(monitor())
