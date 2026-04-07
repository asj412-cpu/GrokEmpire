"""Quick raw Kalshi WS test - dump first 30 messages from orderbook_delta"""
import asyncio, json, time, base64, os
import websockets, requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend
from dotenv import load_dotenv
load_dotenv(override=True)

KEY_ID = os.getenv("KALSHI_KEY_ID")
KEY_PATH = os.getenv("KALSHI_PRIVATE_KEY")

def sig(ts):
    with open(KEY_PATH,'rb') as f:
        pk = serialization.load_pem_private_key(f.read(), password=None, backend=default_backend())
    s = pk.sign(f"{ts}GET/trade-api/ws/v2".encode(), padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH), hashes.SHA256())
    return base64.b64encode(s).decode()

async def main():
    # Get current open BTC market
    r = requests.get("https://api.elections.kalshi.com/trade-api/v2/markets?series_ticker=KXBTC15M&status=open&limit=1")
    ticker = r.json()["markets"][0]["ticker"]
    print(f"Subscribing to: {ticker}")

    ts = int(time.time()*1000)
    headers = {"KALSHI-ACCESS-KEY": KEY_ID, "KALSHI-ACCESS-SIGNATURE": sig(ts), "KALSHI-ACCESS-TIMESTAMP": str(ts)}

    async with websockets.connect("wss://api.elections.kalshi.com/trade-api/ws/v2", additional_headers=headers, ping_interval=30) as ws:
        await ws.send(json.dumps({"id":1,"cmd":"subscribe","params":{"channels":["orderbook_delta"],"market_tickers":[ticker]}}))
        print("Subscribed, waiting for messages...")
        count = 0
        async for msg in ws:
            count += 1
            data = json.loads(msg)
            print(f"\n--- MSG {count} ---")
            print(json.dumps(data, indent=2)[:500])
            if count >= 10:
                break

asyncio.run(main())
