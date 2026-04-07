"""
Dual WebSocket Monitor:
- Kalshi WS: contract prices (yes_bid, yes_ask, last) for all 7 coin 15m markets
- Coinbase WS: spot prices for all 7 coins
- Both recorded with millisecond timestamps for lag/correlation analysis
- 4 cycles = 1 hour of data
- Output: CSV with both feeds interleaved by timestamp
"""

import asyncio
import json
import time
import base64
import os
import csv
from datetime import datetime, timezone

import websockets
import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend
from dotenv import load_dotenv

load_dotenv(override=True)

KALSHI_WS = "wss://api.elections.kalshi.com/trade-api/ws/v2"
COINBASE_WS = "wss://ws-feed.exchange.coinbase.com"
KALSHI_API = "https://api.elections.kalshi.com/trade-api/v2"
KEY_ID = os.getenv("KALSHI_KEY_ID")
KEY_PATH = os.getenv("KALSHI_PRIVATE_KEY")

COINS = {
    "BTC": ("KXBTC15M", "BTC-USD"),
    "ETH": ("KXETH15M", "ETH-USD"),
    "SOL": ("KXSOL15M", "SOL-USD"),
    "XRP": ("KXXRP15M", "XRP-USD"),
    "BNB": ("KXBNB15M", "BNB-USD"),
    "HYPE": ("KXHYPE15M", "HYPE-USD"),
    "DOGE": ("KXDOGE15M", "DOGE-USD"),
}

OUT_CSV = "/tmp/dual_ws_data.csv"
all_rows = []
cycles_done = 0
TARGET_CYCLES = 4
start_time = None


def generate_signature(timestamp_ms):
    with open(KEY_PATH, 'rb') as f:
        pk = serialization.load_pem_private_key(f.read(), password=None, backend=default_backend())
    msg = f"{timestamp_ms}GET/trade-api/ws/v2"
    sig = pk.sign(msg.encode(), padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH), hashes.SHA256())
    return base64.b64encode(sig).decode()


def get_open_kalshi_tickers():
    tickers = {}
    for coin, (series, _) in COINS.items():
        try:
            r = requests.get(f"{KALSHI_API}/markets?series_ticker={series}&status=open&limit=1")
            mkts = r.json().get("markets", [])
            if mkts:
                tickers[coin] = mkts[0]["ticker"]
        except Exception as e:
            print(f"  {coin} ticker fetch err: {e}")
    return tickers


# Shared state
kalshi_ticker_to_coin = {}


async def kalshi_loop():
    """Subscribe to Kalshi WS and record every ticker update."""
    global cycles_done
    while cycles_done < TARGET_CYCLES:
        try:
            ts_ms = int(time.time() * 1000)
            sig = generate_signature(ts_ms)
            headers = {
                "KALSHI-ACCESS-KEY": KEY_ID,
                "KALSHI-ACCESS-SIGNATURE": sig,
                "KALSHI-ACCESS-TIMESTAMP": str(ts_ms),
            }

            tickers = list(kalshi_ticker_to_coin.keys())
            async with websockets.connect(KALSHI_WS, additional_headers=headers, ping_interval=30) as ws:
                await ws.send(json.dumps({
                    "id": 1, "cmd": "subscribe",
                    "params": {"channels": ["ticker"], "market_tickers": tickers}
                }))
                print(f"[Kalshi] Connected, subscribed to {len(tickers)} tickers")

                async for msg in ws:
                    if cycles_done >= TARGET_CYCLES:
                        return
                    data = json.loads(msg)
                    if data.get("type") != "ticker":
                        continue
                    m = data.get("msg", {})
                    ticker = m.get("market_ticker", "")
                    coin = kalshi_ticker_to_coin.get(ticker)
                    if not coin:
                        continue
                    try:
                        yes_bid = int(float(m.get("yes_bid_dollars", "0")) * 100)
                        yes_ask = int(float(m.get("yes_ask_dollars", "0")) * 100)
                    except (ValueError, TypeError):
                        continue
                    now = datetime.now(timezone.utc)
                    ts = now.strftime("%H:%M:%S.%f")[:-3]
                    minute_in_cycle = now.minute % 15
                    second_in_cycle = minute_in_cycle * 60 + now.second
                    all_rows.append({
                        "ts": ts,
                        "ts_ms": int(now.timestamp() * 1000),
                        "source": "kalshi",
                        "coin": coin,
                        "ticker": ticker,
                        "yes_bid": yes_bid,
                        "yes_ask": yes_ask,
                        "spot": "",
                        "cycle_sec": second_in_cycle,
                    })
        except Exception as e:
            print(f"[Kalshi] WS error: {e}")
            await asyncio.sleep(3)


async def coinbase_loop():
    """Subscribe to Coinbase WS for all 7 coin spot prices."""
    global cycles_done
    while cycles_done < TARGET_CYCLES:
        try:
            async with websockets.connect(COINBASE_WS, ping_interval=30) as ws:
                products = [p for _, p in COINS.values()]
                await ws.send(json.dumps({
                    "type": "subscribe",
                    "product_ids": products,
                    "channels": ["ticker"],
                }))
                print(f"[Coinbase] Connected, subscribed to {len(products)} products")

                async for msg in ws:
                    if cycles_done >= TARGET_CYCLES:
                        return
                    data = json.loads(msg)
                    if data.get("type") != "ticker":
                        continue
                    product = data.get("product_id", "")
                    price = data.get("price", "")
                    if not price:
                        continue

                    # Map product back to coin
                    coin = None
                    for c, (_, p) in COINS.items():
                        if p == product:
                            coin = c
                            break
                    if not coin:
                        continue

                    now = datetime.now(timezone.utc)
                    ts = now.strftime("%H:%M:%S.%f")[:-3]
                    minute_in_cycle = now.minute % 15
                    second_in_cycle = minute_in_cycle * 60 + now.second
                    all_rows.append({
                        "ts": ts,
                        "ts_ms": int(now.timestamp() * 1000),
                        "source": "coinbase",
                        "coin": coin,
                        "ticker": "",
                        "yes_bid": "",
                        "yes_ask": "",
                        "spot": float(price),
                        "cycle_sec": second_in_cycle,
                    })
        except Exception as e:
            print(f"[Coinbase] WS error: {e}")
            await asyncio.sleep(3)


async def cycle_watcher():
    """Detect cycle boundaries and rotate Kalshi tickers."""
    global cycles_done, kalshi_ticker_to_coin
    last_min = None
    while cycles_done < TARGET_CYCLES:
        now = datetime.now(timezone.utc)
        min_in_cycle = now.minute % 15
        if last_min is not None and last_min == 14 and min_in_cycle == 0:
            cycles_done += 1
            print(f"\n=== CYCLE {cycles_done}/{TARGET_CYCLES} BOUNDARY ===")
            # Rotate kalshi tickers
            new_tickers = get_open_kalshi_tickers()
            kalshi_ticker_to_coin = {v: k for k, v in new_tickers.items()}
            print(f"  New tickers: {list(new_tickers.values())[:3]}...")

            # Save partial CSV
            save_csv()

        last_min = min_in_cycle
        await asyncio.sleep(2)


def save_csv():
    with open(OUT_CSV, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["ts", "ts_ms", "source", "coin", "ticker", "yes_bid", "yes_ask", "spot", "cycle_sec"])
        w.writeheader()
        for row in all_rows:
            w.writerow(row)


async def main():
    global start_time, kalshi_ticker_to_coin

    # Wait for next cycle boundary so we capture full cycles
    now = datetime.now(timezone.utc)
    secs_to_boundary = (15 - (now.minute % 15)) * 60 - now.second
    if secs_to_boundary > 30:
        print(f"Waiting {secs_to_boundary}s for next cycle boundary...")
        await asyncio.sleep(secs_to_boundary + 2)

    start_time = datetime.now(timezone.utc)
    initial_tickers = get_open_kalshi_tickers()
    kalshi_ticker_to_coin = {v: k for k, v in initial_tickers.items()}
    print(f"Starting at {start_time.strftime('%H:%M:%S')}")
    print(f"Initial Kalshi tickers: {list(initial_tickers.values())}")

    await asyncio.gather(
        kalshi_loop(),
        coinbase_loop(),
        cycle_watcher(),
    )

    save_csv()
    print(f"\n✅ Done. {len(all_rows)} ticks saved to {OUT_CSV}")
    kalshi_ct = sum(1 for r in all_rows if r["source"] == "kalshi")
    cb_ct = sum(1 for r in all_rows if r["source"] == "coinbase")
    print(f"  Kalshi ticks: {kalshi_ct}")
    print(f"  Coinbase ticks: {cb_ct}")


if __name__ == "__main__":
    asyncio.run(main())
