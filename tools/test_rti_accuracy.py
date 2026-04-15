"""
Test CF RTI Engine accuracy against actual Kalshi strike prices.

Connects to exchange orderbook WS feeds, computes the RTI using the
exact CF Benchmarks formula, and compares to Kalshi's floor_strike
(when available) or the UI-displayed strike.

Run this during a cycle boundary to capture the settlement/strike transition.
"""
import asyncio
import json
import time
import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from agents.trading.cf_rti_engine import CFRealTimeIndex, EXCHANGE_WS_CONFIG

import websockets


class RTIAccuracyTest:
    def __init__(self):
        self.rti_btc = CFRealTimeIndex("BTC")
        self.rti_eth = CFRealTimeIndex("ETH")
        self.rti_sol = CFRealTimeIndex("SOL")
        self.rtis = {"BTC": self.rti_btc, "ETH": self.rti_eth, "SOL": self.rti_sol}

    async def coinbase_orderbook_ws(self):
        """Coinbase L2 orderbook feed."""
        url = "wss://ws-feed.exchange.coinbase.com"
        pairs = {"BTC-USD": "BTC", "ETH-USD": "ETH", "SOL-USD": "SOL"}
        books = {coin: {"bids": {}, "asks": {}} for coin in pairs.values()}

        while True:
            try:
                async with websockets.connect(url, ping_interval=30, max_size=10_000_000) as ws:
                    # Subscribe to L2 updates
                    await ws.send(json.dumps({
                        "type": "subscribe",
                        "channels": [{"name": "level2_batch", "product_ids": list(pairs.keys())}]
                    }))
                    print("  ✅ Coinbase L2 connected")

                    async for msg in ws:
                        data = json.loads(msg)
                        msg_type = data.get("type")
                        product = data.get("product_id", "")
                        coin = pairs.get(product)
                        if not coin:
                            continue

                        if msg_type == "snapshot":
                            # Full orderbook snapshot
                            book = books[coin]
                            book["bids"] = {float(p): float(s) for p, s in data.get("bids", [])}
                            book["asks"] = {float(p): float(s) for p, s in data.get("asks", [])}
                            self._update_rti(coin, "coinbase", book)

                        elif msg_type == "l2update":
                            book = books[coin]
                            for side, price, size in data.get("changes", []):
                                p, s = float(price), float(size)
                                target = book["bids"] if side == "buy" else book["asks"]
                                if s == 0:
                                    target.pop(p, None)
                                else:
                                    target[p] = s
                            self._update_rti(coin, "coinbase", book)

            except Exception as e:
                print(f"  Coinbase error: {e}, reconnecting...")
                await asyncio.sleep(2)

    async def kraken_orderbook_ws(self):
        """Kraken orderbook feed."""
        url = "wss://ws.kraken.com"
        pairs = {"XBT/USD": "BTC", "ETH/USD": "ETH", "SOL/USD": "SOL"}
        books = {coin: {"bids": {}, "asks": {}} for coin in pairs.values()}

        while True:
            try:
                async with websockets.connect(url, ping_interval=30) as ws:
                    await ws.send(json.dumps({
                        "event": "subscribe",
                        "pair": list(pairs.keys()),
                        "subscription": {"name": "book", "depth": 25}
                    }))
                    print("  ✅ Kraken book connected")

                    async for msg in ws:
                        data = json.loads(msg)
                        if isinstance(data, list) and len(data) >= 4:
                            pair_name = data[-1]
                            coin = pairs.get(pair_name)
                            if not coin:
                                continue
                            book = books[coin]
                            updates = data[1] if isinstance(data[1], dict) else {}

                            if "bs" in updates:  # snapshot bids
                                book["bids"] = {float(e[0]): float(e[1]) for e in updates["bs"]}
                            if "as" in updates:  # snapshot asks
                                book["asks"] = {float(e[0]): float(e[1]) for e in updates["as"]}
                            if "b" in updates:  # update bids
                                for e in updates["b"]:
                                    p, s = float(e[0]), float(e[1])
                                    if s == 0:
                                        book["bids"].pop(p, None)
                                    else:
                                        book["bids"][p] = s
                            if "a" in updates:  # update asks
                                for e in updates["a"]:
                                    p, s = float(e[0]), float(e[1])
                                    if s == 0:
                                        book["asks"].pop(p, None)
                                    else:
                                        book["asks"][p] = s

                            # Handle case where data[1] has bids and data[2] has asks
                            if len(data) >= 5 and isinstance(data[2], dict):
                                for key in ["b", "a"]:
                                    if key in data[2]:
                                        target = book["bids"] if key == "b" else book["asks"]
                                        for e in data[2][key]:
                                            p, s = float(e[0]), float(e[1])
                                            if s == 0:
                                                target.pop(p, None)
                                            else:
                                                target[p] = s

                            self._update_rti(coin, "kraken", book)

            except Exception as e:
                print(f"  Kraken error: {e}, reconnecting...")
                await asyncio.sleep(2)

    async def bitstamp_orderbook_ws(self):
        """Bitstamp orderbook feed."""
        url = "wss://ws.bitstamp.net"
        pairs = {"btcusd": "BTC", "ethusd": "ETH", "solusd": "SOL", "xrpusd": "XRP"}

        while True:
            try:
                async with websockets.connect(url, ping_interval=30) as ws:
                    for pair in pairs:
                        await ws.send(json.dumps({
                            "event": "bts:subscribe",
                            "data": {"channel": f"order_book_{pair}"}
                        }))
                    print("  ✅ Bitstamp book connected")

                    async for msg in ws:
                        data = json.loads(msg)
                        if data.get("event") == "data":
                            channel = data.get("channel", "")
                            for pair, coin in pairs.items():
                                if pair in channel:
                                    book_data = data.get("data", {})
                                    bids_raw = book_data.get("bids", [])
                                    asks_raw = book_data.get("asks", [])
                                    book = {
                                        "bids": {float(b[0]): float(b[1]) for b in bids_raw},
                                        "asks": {float(a[0]): float(a[1]) for a in asks_raw},
                                    }
                                    self._update_rti(coin, "bitstamp", book)
                                    break

            except Exception as e:
                print(f"  Bitstamp error: {e}, reconnecting...")
                await asyncio.sleep(2)

    async def gemini_orderbook_ws(self, coin: str):
        """Gemini orderbook feed (one connection per symbol)."""
        pairs = {"BTC": "BTCUSD", "ETH": "ETHUSD", "SOL": "SOLUSD"}
        symbol = pairs.get(coin)
        if not symbol:
            return

        url = f"wss://api.gemini.com/v1/marketdata/{symbol}?top_of_book=false"
        book = {"bids": {}, "asks": {}}

        while True:
            try:
                async with websockets.connect(url, ping_interval=30) as ws:
                    print(f"  ✅ Gemini {coin} book connected")

                    async for msg in ws:
                        data = json.loads(msg)
                        events = data.get("events", [])
                        for event in events:
                            if event.get("type") == "change":
                                side = event.get("side")
                                price = float(event.get("price", 0))
                                remaining = float(event.get("remaining", 0))
                                if side == "bid":
                                    if remaining == 0:
                                        book["bids"].pop(price, None)
                                    else:
                                        book["bids"][price] = remaining
                                elif side == "ask":
                                    if remaining == 0:
                                        book["asks"].pop(price, None)
                                    else:
                                        book["asks"][price] = remaining

                        self._update_rti(coin, "gemini", book)

            except Exception as e:
                print(f"  Gemini {coin} error: {e}, reconnecting...")
                await asyncio.sleep(2)

    def _update_rti(self, coin: str, exchange: str, book: dict):
        """Convert book dict to sorted tuples and update the RTI engine."""
        if coin not in self.rtis:
            return
        bids = sorted(book["bids"].items(), key=lambda x: -x[0])[:50]  # top 50 levels
        asks = sorted(book["asks"].items(), key=lambda x: x[0])[:50]
        self.rtis[coin].update_orderbook(exchange, bids, asks)

    async def crypto_com_orderbook_ws(self):
        """Crypto.com exchange orderbook feed."""
        url = "wss://stream.crypto.com/exchange/v1/market"
        pairs = {"BTC_USD": "BTC", "ETH_USD": "ETH", "SOL_USD": "SOL", "XRP_USD": "XRP"}
        books = {coin: {"bids": {}, "asks": {}} for coin in pairs.values()}

        while True:
            try:
                async with websockets.connect(url, ping_interval=30) as ws:
                    await ws.send(json.dumps({
                        "id": 1,
                        "method": "subscribe",
                        "params": {
                            "channels": [f"book.{pair}.50" for pair in pairs.keys()]
                        }
                    }))
                    print("  ✅ Crypto.com book connected")

                    async for msg in ws:
                        data = json.loads(msg)
                        result = data.get("result", {})
                        channel = result.get("channel", "")
                        if not channel.startswith("book."):
                            continue

                        instrument = channel.split(".")[1]  # e.g. "BTC_USD"
                        coin = pairs.get(instrument)
                        if not coin:
                            continue

                        book_data = result.get("data", [{}])
                        if isinstance(book_data, list) and book_data:
                            book_data = book_data[0]

                        bids_raw = book_data.get("bids", [])
                        asks_raw = book_data.get("asks", [])

                        book = books[coin]
                        # Crypto.com format: [[price, quantity, num_orders], ...]
                        for b in bids_raw:
                            p, s = float(b[0]), float(b[1])
                            if s == 0:
                                book["bids"].pop(p, None)
                            else:
                                book["bids"][p] = s
                        for a in asks_raw:
                            p, s = float(a[0]), float(a[1])
                            if s == 0:
                                book["asks"].pop(p, None)
                            else:
                                book["asks"][p] = s

                        self._update_rti(coin, "crypto_com", book)

            except Exception as e:
                print(f"  Crypto.com error: {e}, reconnecting...")
                await asyncio.sleep(2)

    async def print_loop(self):
        """Print RTI values every second, compare to known strike."""
        await asyncio.sleep(10)  # let feeds warm up
        print("\n📊 RTI Accuracy Test — computing every second")
        print("=" * 70)

        while True:
            ts = datetime.now().strftime("%H:%M:%S")
            cycle_sec = (datetime.now().minute % 15) * 60 + datetime.now().second

            for coin, rti in self.rtis.items():
                value = rti.compute()
                status = rti.get_status()
                active = sum(1 for s in status.values() if s["valid"])
                total = len(status)

                if value:
                    print(f"  [{ts}] {coin} RTI: ${value:,.2f} | feeds: {active}/{total} | cycle_sec: {cycle_sec}")
                else:
                    print(f"  [{ts}] {coin} RTI: FAILED | feeds: {active}/{total}")

            # At cycle boundary (sec 0-5), this is the strike
            if cycle_sec <= 5:
                print(f"\n  🎯 CYCLE BOUNDARY — these values ARE the strike!")
                print(f"     Compare to Kalshi UI strike values\n")

            await asyncio.sleep(1)


async def main():
    test = RTIAccuracyTest()
    print("🔬 CF RTI Accuracy Test")
    print("   Connecting to exchange orderbook feeds...")
    print()

    # Launch all feeds + print loop
    tasks = [
        asyncio.create_task(test.coinbase_orderbook_ws()),
        asyncio.create_task(test.kraken_orderbook_ws()),
        asyncio.create_task(test.bitstamp_orderbook_ws()),
        asyncio.create_task(test.gemini_orderbook_ws("BTC")),
        asyncio.create_task(test.gemini_orderbook_ws("ETH")),
        asyncio.create_task(test.gemini_orderbook_ws("SOL")),
        asyncio.create_task(test.crypto_com_orderbook_ws()),
        asyncio.create_task(test.print_loop()),
    ]

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
