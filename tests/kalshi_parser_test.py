"""Replicate the exact dual_ws_monitor kalshi_loop parser logic but run for 30 sec only"""
import asyncio, json, time, base64, os
from collections import defaultdict
import websockets, requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend
from dotenv import load_dotenv
load_dotenv(override=True)

KEY_ID = os.getenv("KALSHI_KEY_ID")
KEY_PATH = os.getenv("KALSHI_PRIVATE_KEY")
COINS = ["BTC","ETH","SOL","XRP","BNB","HYPE","DOGE"]

def sig(ts):
    with open(KEY_PATH,'rb') as f:
        pk = serialization.load_pem_private_key(f.read(), password=None, backend=default_backend())
    s = pk.sign(f"{ts}GET/trade-api/ws/v2".encode(), padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH), hashes.SHA256())
    return base64.b64encode(s).decode()

async def main():
    tickers = []
    ticker_to_coin = {}
    for coin in COINS:
        r = requests.get(f"https://api.elections.kalshi.com/trade-api/v2/markets?series_ticker=KX{coin}15M&status=open&limit=1")
        m = r.json().get("markets", [])
        if m:
            tickers.append(m[0]["ticker"])
            ticker_to_coin[m[0]["ticker"]] = coin
    print(f"Subscribing to {len(tickers)} tickers")

    books = {}
    rows_logged = 0
    msg_count = {"snapshot": 0, "delta": 0, "delta_processed": 0, "delta_skipped_no_book": 0, "delta_skipped_no_coin": 0, "rows": 0}

    ts = int(time.time()*1000)
    headers = {"KALSHI-ACCESS-KEY": KEY_ID, "KALSHI-ACCESS-SIGNATURE": sig(ts), "KALSHI-ACCESS-TIMESTAMP": str(ts)}
    async with websockets.connect("wss://api.elections.kalshi.com/trade-api/ws/v2", additional_headers=headers, ping_interval=30) as ws:
        await ws.send(json.dumps({"id":1,"cmd":"subscribe","params":{"channels":["orderbook_delta"],"market_tickers":tickers}}))
        print("Subscribed")

        start = time.time()
        first_msgs = 0
        while time.time() - start < 25:
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=2)
            except asyncio.TimeoutError:
                continue
            if first_msgs < 3:
                print(f"DEBUG msg {first_msgs}: {msg[:300]}")
                first_msgs += 1
            data = json.loads(msg)
            msg_type = data.get("type", "")

            if msg_type == "orderbook_snapshot":
                msg_count["snapshot"] += 1
                m = data.get("msg", {})
                ticker = m.get("market_ticker", "")
                if not ticker: continue
                books[ticker] = {"yes": {}, "no": {}}
                for price_str, qty_str in m.get("yes", []) or m.get("yes_dollars_fp", []):
                    try:
                        p = int(round(float(price_str) * 100))
                        books[ticker]["yes"][p] = float(qty_str)
                    except: pass
                for price_str, qty_str in m.get("no", []) or m.get("no_dollars_fp", []):
                    try:
                        p = int(round(float(price_str) * 100))
                        books[ticker]["no"][p] = float(qty_str)
                    except: pass

            elif msg_type == "orderbook_delta":
                msg_count["delta"] += 1
                m = data.get("msg", {})
                ticker = m.get("market_ticker", "")
                if not ticker or ticker not in books:
                    msg_count["delta_skipped_no_book"] += 1
                    continue
                side = m.get("side", "")
                price_raw = m.get("price_dollars")
                if price_raw is None:
                    continue
                try:
                    price = int(round(float(price_raw) * 100))
                except: continue
                delta = float(m.get("delta_fp", 0))
                current = books[ticker].get(side, {}).get(price, 0)
                books[ticker][side][price] = max(0, current + delta)
                msg_count["delta_processed"] += 1
            else:
                continue

            coin = ticker_to_coin.get(ticker)
            if not coin:
                if msg_type == "orderbook_delta":
                    msg_count["delta_skipped_no_coin"] += 1
                continue

            msg_count["rows"] += 1
            rows_logged += 1

        print(f"\nFinal counts: {msg_count}")
        print(f"Rows that would be logged: {rows_logged}")
        print(f"Books populated: {list(books.keys())}")

asyncio.run(main())
