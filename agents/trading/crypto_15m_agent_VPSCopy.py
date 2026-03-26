import asyncio
import os
import csv
from datetime import datetime
import websockets
import json
from kalshi_client import KalshiClient

BASE_CASH_FLOOR = 40.0
RATCHET_PERCENT = 0.80
RISK_MULTIPLIER = 0.08
DRY_RUN = False

COINS = {
    "BTC":  "KXBTC15M",
    "ETH":  "KXETH15M",
    "SOL":  "KXSOL15M",
    "XRP":  "KXXRP15M",
    "DOGE": "KXDOGE15M",
    "HYPE": "KXHYPE15M",
    "BNB":  "KXBNB15M",
}

COINBASE_PRODUCTS = {
    "BTC":  "BTC-USD",
    "ETH":  "ETH-USD",
    "SOL":  "SOL-USD",
    "XRP":  "XRP-USD",
    "DOGE": "DOGE-USD",
    "HYPE": "HYPE-USD",
    "BNB":  "BNB-USD",
}

THRESHOLDS = {
    "BTC":  {"yes": 0.60, "no": 0.25, "min_vol": 40000, "distance": 200},   # BUY NO threshold loosened
    "ETH":  {"yes": 0.55, "no": 0.20, "min_vol": 30000, "distance": 150},
    "SOL":  {"yes": 0.52, "no": 0.15, "min_vol": 35000, "distance": 100},
    "XRP":  {"yes": 0.58, "no": 0.22, "min_vol": 25000, "distance": 80},
    "DOGE": {"yes": 0.50, "no": 0.12, "min_vol": 40000, "distance": 60},
    "HYPE": {"yes": 0.48, "no": 0.10, "min_vol": 20000, "distance": 50},
    "BNB":  {"yes": 0.56, "no": 0.19, "min_vol": 25000, "distance": 120},
}


class Crypto15mAgent:
    def __init__(self):
        self.client = KalshiClient()
        self.running = True
        self.prices = {coin: None for coin in COINS}
        self.log_file = "15m_signals.csv"
        self.last_traded_ticker = None
        self.current_cash_floor = BASE_CASH_FLOOR
        self.last_balance = 0.0

        if not os.path.exists(self.log_file):
            with open(self.log_file, "w", newline="") as f:
                csv.writer(f).writerow([
                    "timestamp", "coin", "ticker", "strike", "mid", "coin_price",
                    "decision", "contracts", "price", "client_order_id", "status",
                    "current_floor", "dry_run", "signal_id"
                ])

    async def coinbase_websocket(self):
        while self.running:
            try:
                async with websockets.connect("wss://ws-feed.exchange.coinbase.com") as ws:
                    await ws.send(json.dumps({
                        "type": "subscribe",
                        "product_ids": list(COINBASE_PRODUCTS.values()),
                        "channels": ["ticker"],
                    }))
                    print("✅ Coinbase WS connected")

                    async for msg in ws:
                        data = json.loads(msg)
                        if data.get("type") == "ticker":
                            product = data.get("product_id")
                            price = data.get("price")
                            if price:
                                for coin, prod in COINBASE_PRODUCTS.items():
                                    if prod == product:
                                        self.prices[coin] = float(price)
                                        break
            except Exception as e:
                print(f"Coinbase WS error: {e} — reconnecting...")
                await asyncio.sleep(5)

    async def get_balance(self):
        try:
            data = await self.client._signed_request("GET", f"{self.client.base_path}/portfolio/balance")
            balance_cents = data.get("balance", 0)
            return balance_cents / 100.0
        except Exception as e:
            print(f"Balance check error: {e}")
            return None

    async def kalshi_discovery(self):
        balance = await self.get_balance() or self.last_balance
        riskable = max(0.0, balance - self.current_cash_floor)

        if balance > self.current_cash_floor:
            new_floor = balance * RATCHET_PERCENT
            if new_floor > self.current_cash_floor:
                print(f"   🟢 New high! Raising cash floor from ${self.current_cash_floor:.2f} to ${new_floor:.2f}")
            self.current_cash_floor = new_floor

        self.last_balance = balance

        for coin_name, series in COINS.items():
            try:
                data = await self.client.get_markets(series_ticker=series, status="open", limit=2)
                if not data.get("markets"):
                    continue

                m = data["markets"][0]
                ticker = m.get("ticker")
                yes_bid = float(m.get("yes_bid_dollars", 0))
                yes_ask = float(m.get("yes_ask_dollars", 0))
                strike_raw = m.get("floor_strike")
                volume = float(m.get("volume_fp", 0))

                if not yes_bid or not yes_ask:
                    continue

                strike = float(strike_raw) if strike_raw is not None else 0.0
                mid = (yes_bid + yes_ask) / 2
                current_price = self.prices.get(coin_name)

                th = THRESHOLDS.get(coin_name, THRESHOLDS["BTC"])

                decision = "HOLD"
                entry_price = None
                minutes_remaining = 15 - (datetime.now().minute % 15)

                if current_price is not None:
                    distance = abs(current_price - strike)

                    min_distance_table = {
                        "BTC":  {2: 35,  5: 80,  10: 130, 15: 210},
                        "ETH":  {2: 4,   5: 9,   10: 14,  15: 22},
                        "SOL":  {2: 0.35,5: 0.80,10: 1.3, 15: 2.1},
                        "BNB":  {2: 2.5, 5: 6,   10: 9,   15: 14},
                        "HYPE": {2: 0.45,5: 1.0, 10: 1.6, 15: 2.5},
                        "XRP":  {2: 0.008,5: 0.018,10: 0.028,15: 0.045},
                        "DOGE": {2: 0.0012,5: 0.0028,10: 0.0045,15: 0.007},
                    }

                    table = min_distance_table.get(coin_name, min_distance_table["BTC"])

                    if minutes_remaining <= 2:
                        min_distance = table[2]
                    elif minutes_remaining <= 5:
                        min_distance = table[5]
                    elif minutes_remaining <= 10:
                        min_distance = table[10]
                    else:
                        min_distance = table[15]

                    if mid >= 0.88:
                        print(f"   PRICE GUARD: rejected BUY YES on {coin_name} at mid {mid:.4f}")
                        decision = "HOLD"
                    elif mid <= 0.12:
                        print(f"   PRICE GUARD: rejected BUY NO on {coin_name} at mid {mid:.4f}")
                        decision = "HOLD"

                    if mid <= th["no"] and volume >= th["min_vol"]:
                        decision = "BUY NO"
                        entry_price = int(round((1.0 - yes_bid) * 100))
                        entry_price = max(1, min(90, entry_price))
                    elif mid >= th["yes"] and distance > min_distance:
                        decision = "BUY YES"
                        entry_price = int(round(yes_ask * 100))
                        entry_price = max(1, min(90, entry_price))

                timestamp = datetime.now().strftime("%H:%M:%S")
                signal_id = f"{coin_name}-{ticker}-{int(datetime.now().timestamp())}"

                print(f"[{timestamp}] {coin_name} | Mid: {mid:.4f} | Price: {current_price} | Min left: {minutes_remaining} | Decision: {decision}")

                contracts = max(1, int(riskable * RISK_MULTIPLIER)) if riskable > 0 else 0

                print(f"   Balance: ${balance:.2f} | Floor: ${self.current_cash_floor:.2f} | Riskable: ${riskable:.2f} | Contracts: {contracts}")

                with open(self.log_file, "a", newline="") as f:
                    csv.writer(f).writerow([
                        timestamp, coin_name, ticker, strike, mid, current_price,
                        decision, contracts, entry_price, None, "CYCLE_LOG", self.current_cash_floor, str(DRY_RUN), signal_id
                    ])

                if decision == "HOLD" or coin_name != "BTC" or DRY_RUN:
                    continue

                if ticker == self.last_traded_ticker or riskable <= 0:
                    continue

                side = "no" if decision == "BUY NO" else "yes"
                client_order_id = f"cr15m-{side}-{ticker}-{int(datetime.now().timestamp())}"

                print(f"   → ORDER: BUY {side.upper()} {contracts}x @ {entry_price}c | ID: {client_order_id}")

                result = await self.client.place_order(ticker, side, contracts, price=entry_price)
                status = "SUCCESS" if result else "FAILED"
                self.last_traded_ticker = ticker

                with open(self.log_file, "a", newline="") as f:
                    csv.writer(f).writerow([
                        timestamp, coin_name, ticker, strike, mid, current_price,
                        decision, contracts, entry_price, client_order_id, status, self.current_cash_floor, "false", signal_id
                    ])

            except Exception as e:
                print(f"Error processing {coin_name}: {e}")

    async def run(self):
        print(f"🚀 GrokEmpire 15m Agent — LIVE (BTC Only)")
        print(f"   Base floor: ${BASE_CASH_FLOOR:.0f} | DRY_RUN: {DRY_RUN}")
        asyncio.create_task(self.coinbase_websocket())

        while self.running:
            await self.kalshi_discovery()
            await asyncio.sleep(30)

async def main():
    agent = Crypto15mAgent()
    try:
        await agent.run()
    except KeyboardInterrupt:
        print("\nShutting down...")

if __name__ == "__main__":
    asyncio.run(main())