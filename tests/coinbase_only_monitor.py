"""Pure Coinbase ticker monitor - 30 min, writes CSV continuously"""
import asyncio, json, csv
from datetime import datetime, timezone
import websockets

OUT_CSV = "/tmp/coinbase_ticks.csv"
COINS = {"BTC":"BTC-USD","ETH":"ETH-USD","SOL":"SOL-USD","XRP":"XRP-USD","BNB":"BNB-USD","HYPE":"HYPE-USD","DOGE":"DOGE-USD"}
DURATION_SEC = 35 * 60  # 35 min covers waiting + 2 cycles

async def main():
    # Wait for cycle boundary
    now = datetime.now(timezone.utc)
    wait = (15 - (now.minute % 15)) * 60 - now.second
    if wait > 30:
        print(f"Waiting {wait}s")
        await asyncio.sleep(wait + 2)

    csv_file = open(OUT_CSV, "w", newline="")
    writer = csv.writer(csv_file)
    writer.writerow(["ts_ms","ts","coin","spot","cycle_sec"])
    rows = 0
    coin_to_product = COINS
    product_to_coin = {v:k for k,v in COINS.items()}

    start = datetime.now(timezone.utc)
    print(f"Started at {start.strftime('%H:%M:%S')}")

    while (datetime.now(timezone.utc) - start).total_seconds() < DURATION_SEC:
        try:
            async with websockets.connect("wss://ws-feed.exchange.coinbase.com", ping_interval=30) as ws:
                await ws.send(json.dumps({"type":"subscribe","product_ids":list(COINS.values()),"channels":["ticker"]}))
                while (datetime.now(timezone.utc) - start).total_seconds() < DURATION_SEC:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=10)
                    except asyncio.TimeoutError:
                        continue
                    d = json.loads(msg)
                    if d.get("type") != "ticker": continue
                    product = d.get("product_id")
                    price = d.get("price")
                    if not price: continue
                    coin = product_to_coin.get(product)
                    if not coin: continue
                    now = datetime.now(timezone.utc)
                    cycle_sec = (now.minute % 15) * 60 + now.second
                    writer.writerow([int(now.timestamp()*1000), now.strftime("%H:%M:%S.%f")[:-3], coin, float(price), cycle_sec])
                    rows += 1
                    if rows % 2000 == 0:
                        csv_file.flush()
                        print(f"  rows={rows}")
        except Exception as e:
            print(f"WS error: {e}")
            await asyncio.sleep(3)

    csv_file.close()
    print(f"DONE. rows={rows}")

asyncio.run(main())
