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
TARGET_CYCLES = 2
start_time = None


def generate_signature(timestamp_ms):
    with open(KEY_PATH, 'rb') as f:
        pk = serialization.load_pem_private_key(f.read(), password=None, backend=default_backend())
    msg = f"{timestamp_ms}GET/trade-api/ws/v2"
    sig = pk.sign(msg.encode(), padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH), hashes.SHA256())
    return base64.b64encode(sig).decode()


resubscribe_needed = False

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
    """Subscribe to Kalshi orderbook_delta and maintain local order book per ticker."""
    global cycles_done, resubscribe_needed
    # Local order books: ticker -> {"yes": {price: qty}, "no": {price: qty}}
    books = {}

    def best_bid_ask(ticker):
        """Returns (yes_bid, yes_ask) in cents from local book."""
        b = books.get(ticker, {})
        yes_levels = b.get("yes", {})
        no_levels = b.get("no", {})
        # YES bid = highest yes price with qty>0
        yes_bid = max((p for p, q in yes_levels.items() if q > 0), default=0)
        # YES ask = lowest no price flipped (100 - highest no bid)
        no_bid = max((p for p, q in no_levels.items() if q > 0), default=0)
        yes_ask = (100 - no_bid) if no_bid > 0 else 100
        return yes_bid, yes_ask

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
                    "params": {"channels": ["orderbook_delta"], "market_tickers": tickers}
                }))
                print(f"[Kalshi] Connected, subscribed to orderbook_delta for {len(tickers)} tickers")

                async for msg in ws:
                    if cycles_done >= TARGET_CYCLES:
                        return
                    if resubscribe_needed:
                        resubscribe_needed = False
                        new_tickers = list(kalshi_ticker_to_coin.keys())
                        await ws.send(json.dumps({
                            "id": 99, "cmd": "subscribe",
                            "params": {"channels": ["orderbook_delta"], "market_tickers": new_tickers}
                        }))
                        books.clear()
                        print(f"[Kalshi] Re-subscribed to {len(new_tickers)} new tickers")
                    data = json.loads(msg)
                    msg_type = data.get("type", "")

                    if msg_type == "orderbook_snapshot":
                        m = data.get("msg", {})
                        ticker = m.get("market_ticker", "")
                        if not ticker:
                            continue
                        books[ticker] = {"yes": {}, "no": {}}
                        for price_str, qty_str in m.get("yes", []) or m.get("yes_dollars_fp", []):
                            try:
                                p = int(float(price_str) * 100) if "." in str(price_str) else int(price_str)
                                q = float(qty_str)
                                books[ticker]["yes"][p] = q
                            except (ValueError, TypeError):
                                pass
                        for price_str, qty_str in m.get("no", []) or m.get("no_dollars_fp", []):
                            try:
                                p = int(float(price_str) * 100) if "." in str(price_str) else int(price_str)
                                q = float(qty_str)
                                books[ticker]["no"][p] = q
                            except (ValueError, TypeError):
                                pass

                    elif msg_type == "orderbook_delta":
                        m = data.get("msg", {})
                        ticker = m.get("market_ticker", "")
                        if not ticker or ticker not in books:
                            continue
                        side = m.get("side", "")
                        # Field is price_dollars (string like "0.9520")
                        price_raw = m.get("price_dollars")
                        if price_raw is None:
                            continue
                        try:
                            price = int(round(float(price_raw) * 100))
                        except (ValueError, TypeError):
                            continue
                        delta = float(m.get("delta_fp", 0))
                        current = books[ticker].get(side, {}).get(price, 0)
                        books[ticker][side][price] = max(0, current + delta)
                    else:
                        continue  # skip subscribe confirmations etc

                    coin = kalshi_ticker_to_coin.get(ticker)
                    if not coin:
                        continue

                    yes_bid, yes_ask = best_bid_ask(ticker)
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
    """Detect cycle boundaries and rotate Kalshi tickers. Save CSV every 15s."""
    global cycles_done, kalshi_ticker_to_coin, resubscribe_needed
    last_min = None
    save_counter = 0
    while cycles_done < TARGET_CYCLES:
        now = datetime.now(timezone.utc)
        min_in_cycle = now.minute % 15
        if last_min is not None and last_min == 14 and min_in_cycle == 0:
            cycles_done += 1
            print(f"\n=== CYCLE {cycles_done}/{TARGET_CYCLES} BOUNDARY ===")
            new_tickers = get_open_kalshi_tickers()
            kalshi_ticker_to_coin = {v: k for k, v in new_tickers.items()}
            resubscribe_needed = True
            print(f"  New tickers: {list(new_tickers.values())[:3]}... (resub flag set)")
            save_csv()

        # Flush every 15s for live analysis
        save_counter += 1
        if save_counter >= 5:
            save_csv()
            save_counter = 0

        last_min = min_in_cycle
        await asyncio.sleep(3)


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
