"""Pure Kalshi orderbook_delta monitor - 2 cycles, writes CSV continuously"""
import asyncio, json, time, base64, os, csv
from datetime import datetime, timezone
import websockets, requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend
from dotenv import load_dotenv
load_dotenv(override=True)

KEY_ID = os.getenv("KALSHI_KEY_ID")
KEY_PATH = os.getenv("KALSHI_PRIVATE_KEY")
COINS = ["BTC","ETH","SOL","XRP","BNB","HYPE","DOGE"]
OUT_CSV = "/tmp/kalshi_ticks.csv"
TARGET_CYCLES = 8

def sig(ts):
    with open(KEY_PATH,'rb') as f:
        pk = serialization.load_pem_private_key(f.read(), password=None, backend=default_backend())
    s = pk.sign(f"{ts}GET/trade-api/ws/v2".encode(), padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH), hashes.SHA256())
    return base64.b64encode(s).decode()

def get_tickers():
    out = {}
    for c in COINS:
        r = requests.get(f"https://api.elections.kalshi.com/trade-api/v2/markets?series_ticker=KX{c}15M&status=open&limit=1")
        m = r.json().get("markets", [])
        if m: out[m[0]["ticker"]] = c
    return out

async def main():
    # Wait for cycle boundary
    now = datetime.now(timezone.utc)
    wait = (15 - (now.minute % 15)) * 60 - now.second
    if wait > 30:
        print(f"Waiting {wait}s")
        await asyncio.sleep(wait + 2)

    cycles = 0
    csv_file = open(OUT_CSV, "w", newline="")
    writer = csv.writer(csv_file)
    writer.writerow(["ts_ms","ts","coin","ticker","msg_type","yes_bid","yes_ask","spot","cycle_sec"])

    books = {}
    ticker_to_coin = {}
    last_min = None
    rows_written = 0

    def best_bid_ask(t):
        b = books.get(t, {"yes":{}, "no":{}})
        ybid = max((p for p,q in b["yes"].items() if q>0), default=0)
        nbid = max((p for p,q in b["no"].items() if q>0), default=0)
        yask = (100 - nbid) if nbid > 0 else 100
        return ybid, yask

    while cycles < TARGET_CYCLES:
        try:
            ticker_to_coin = get_tickers()
            tickers = list(ticker_to_coin.keys())
            print(f"Cycle {cycles+1}/{TARGET_CYCLES}: {len(tickers)} tickers")
            books.clear()

            ts = int(time.time()*1000)
            headers = {"KALSHI-ACCESS-KEY": KEY_ID, "KALSHI-ACCESS-SIGNATURE": sig(ts), "KALSHI-ACCESS-TIMESTAMP": str(ts)}
            async with websockets.connect("wss://api.elections.kalshi.com/trade-api/ws/v2", additional_headers=headers, ping_interval=30) as ws:
                await ws.send(json.dumps({"id":1,"cmd":"subscribe","params":{"channels":["orderbook_delta"],"market_tickers":tickers}}))

                while True:
                    msg = await asyncio.wait_for(ws.recv(), timeout=60)
                    data = json.loads(msg)
                    mt = data.get("type", "")
                    if mt not in ("orderbook_snapshot", "orderbook_delta"):
                        continue
                    m = data.get("msg", {})
                    ticker = m.get("market_ticker", "")
                    if not ticker: continue
                    coin = ticker_to_coin.get(ticker)
                    if not coin: continue

                    if mt == "orderbook_snapshot":
                        books[ticker] = {"yes": {}, "no": {}}
                        for ps, qs in (m.get("yes_dollars_fp", []) or []):
                            try:
                                p = int(round(float(ps)*100))
                                books[ticker]["yes"][p] = float(qs)
                            except: pass
                        for ps, qs in (m.get("no_dollars_fp", []) or []):
                            try:
                                p = int(round(float(ps)*100))
                                books[ticker]["no"][p] = float(qs)
                            except: pass
                    else:  # delta
                        if ticker not in books: continue
                        side = m.get("side", "")
                        try:
                            price = int(round(float(m.get("price_dollars", "0")) * 100))
                        except: continue
                        delta = float(m.get("delta_fp", 0))
                        cur = books[ticker].get(side, {}).get(price, 0)
                        books[ticker][side][price] = max(0, cur + delta)

                    yb, ya = best_bid_ask(ticker)
                    now = datetime.now(timezone.utc)
                    cycle_sec = (now.minute % 15) * 60 + now.second
                    writer.writerow([int(now.timestamp()*1000), now.strftime("%H:%M:%S.%f")[:-3], coin, ticker, mt, yb, ya, "", cycle_sec])
                    rows_written += 1
                    if rows_written % 1000 == 0:
                        csv_file.flush()
                        print(f"  rows={rows_written}")

                    # Check cycle boundary
                    cur_min = now.minute % 15
                    if last_min is not None and last_min == 14 and cur_min == 0:
                        cycles += 1
                        print(f"=== CYCLE {cycles} BOUNDARY === total rows: {rows_written}")
                        csv_file.flush()
                        if cycles >= TARGET_CYCLES:
                            csv_file.close()
                            return
                        last_min = None  # reset to avoid false boundary
                        break
                    last_min = cur_min
        except Exception as e:
            print(f"WS error: {e}")
            await asyncio.sleep(3)

    csv_file.close()
    print(f"DONE. Total rows: {rows_written}")

asyncio.run(main())
