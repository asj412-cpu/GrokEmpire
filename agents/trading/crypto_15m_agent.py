import asyncio
import os
import csv
from datetime import datetime
import websockets
import json
import ccxt
import pandas as pd
import numpy as np
from kalshi_client.client import KalshiClient
from collections import deque
import time
import random

BASE_CASH_FLOOR = 40.0
RATCHET_PERCENT = 0.80
RISK_MULTIPLIER = 0.08
DRY_RUN = os.getenv('DRY_RUN', 'false').lower() == 'true'

COINS = {
    "BTC":  "KXBTC15M",
}

COINBASE_PRODUCTS = {
    "BTC":  "BTC-USD",
}

THRESHOLDS = {
    "BTC":  {"yes": 0.60, "no": 0.25, "min_vol": 40000, "distance": 200},
}

# Exchange instance
EXCHANGE = ccxt.coinbase()  # Coinbase exchange

class Crypto15mAgent:
    def __init__(self):
        key_id = os.getenv("KALSHI_KEY_ID")
        private_key = os.getenv("KALSHI_PRIVATE_KEY")
        self.client = None
        if key_id and private_key:
            try:
                self.client = KalshiClient(key_id=key_id, private_key=private_key)
            except Exception as e:
                print(f"Kalshi client init failed ({e}) — dry run mode")
                self.client = None
        else:
            print("No Kalshi keys — dry run mode only")
        if os.getenv("DRY_RUN", "false").lower() == "true":
            self.client = None
            print("DRY_RUN=True: forcing full mocks (client=None, balance=$1000)")
        else:
            print("Live Kalshi client ready")
        self.running = True
        self.exchange = ccxt.coinbase()
        self.prices = {coin: None for coin in COINS}
        self.log_file = "15m_signals.csv"
        self.last_traded_ticker = None
        self.current_cash_floor = BASE_CASH_FLOOR
        self.last_balance = 0.0
        self.positions = {}

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
            except Exception as e:
                print(f"WS error: {e}, reconnecting...")
                await asyncio.sleep(5)
    async def get_balance(self):
        if not self.client:
            return 1000.0  # mock for dry run
        try:
            data = await self.client.get_balance()
            balance_cents = data.get("balance", 0) if isinstance(data, dict) else 0
            return balance_cents / 100.0
        except Exception as e:
            print(f"Balance check error: {e}")
            return self.last_balance

    async def kalshi_discovery(self):
        balance = await self.get_balance() or self.last_balance
        riskable = max(0.0, balance - self.current_cash_floor)

        if balance > self.current_cash_floor:
            new_floor = balance * RATCHET_PERCENT
            if new_floor > self.current_cash_floor:
                self.current_cash_floor = new_floor
                print(f"🔒 Ratchet cash floor to ${new_floor:.2f}")

        self.last_balance = balance

        for coin_name, series in COINS.items():
            current_price = self.prices.get(coin_name)
            if not current_price:
                continue

            if self.client:
                try:
                    data = await self.client.get_markets(series_ticker=series, status="open", limit=2)
                except Exception as e:
                    print(f"Error processing {coin_name}: {e}")
                    continue
            else:
                # Dry run: mock realistic market
                mock_ticker = f"{series}-MOCK"
                mock_strike = current_price * random.uniform(0.97, 1.03)
                mock_yes_bid = random.uniform(0.15, 0.85)
                mock_yes_ask = mock_yes_bid + random.uniform(0.005, 0.03)
                mock_volume = random.uniform(45000, 150000)
                data = {
                    "markets": [{
                        "ticker": mock_ticker,
                        "floor_strike": mock_strike,
                        "yes_bid_dollars": mock_yes_bid,
                        "yes_ask_dollars": mock_yes_ask,
                        "volume_fp": mock_volume,
                    }]
                }

            if not data.get("markets"):
                continue

            m = data["markets"][0]
            ticker = m.get("ticker")
            yes_bid = float(m.get("yes_bid_dollars", 0))
            yes_ask = float(m.get("yes_ask_dollars", 0))
            strike_raw = m.get("floor_strike")
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

                rsi = await self.get_rsi(coin_name)
                if decision == "BUY YES" and rsi < 60:
                    decision = "HOLD"
                elif decision == "BUY NO" and rsi > 40:
                    decision = "HOLD"

                # Kelly
                WIN_RATE = 0.55
                AVG_WIN = 40
                AVG_LOSS = 25
                kelly = (WIN_RATE * AVG_WIN - (1 - WIN_RATE) * AVG_LOSS) / AVG_WIN * 0.25  # conservative
                contracts = max(1, int(riskable * RISK_MULTIPLIER * kelly)) if riskable > 0 else 0

                timestamp = datetime.now().strftime("%H:%M:%S")
                signal_id = f"{coin_name}-{ticker}-{int(datetime.now().timestamp())}"

                print(f"[{timestamp}] {coin_name} | Mid: {mid:.4f} | Price: {current_price} | Min left: {minutes_remaining} | Decision: {decision}")

                print(f"   Balance: ${balance:.2f} | Floor: ${self.current_cash_floor:.2f} | Riskable: ${riskable:.2f} | Contracts: {contracts}")

                with open(self.log_file, "a", newline="") as f:
                    csv.writer(f).writerow([
                        timestamp, coin_name, ticker, strike, mid, current_price,
                        decision, contracts, entry_price, None, "CYCLE_LOG", self.current_cash_floor, str(DRY_RUN), signal_id
                    ])

                # Skip order if HOLD, wrong coin, dry run, low risk, recent trade
                if decision in ("BUY YES", "BUY NO") and coin_name == "BTC" and not DRY_RUN and riskable > 0 and ticker != self.last_traded_ticker:
                    side = "yes" if decision == "BUY YES" else "no"
                    client_order_id = f"cr15m-{side}-{ticker}-{int(time.time())}"
                    print(f"   → {decision} {contracts} @ {entry_price}c ID: {client_order_id}")

                    if self.client:
                        result = await self.client.place_order(ticker, side, contracts, price=entry_price)
                        status = "SUCCESS" if result else "FAILED"
                        self.last_traded_ticker = ticker
                        self.positions[ticker] = {
                            'side': side,
                            'contracts': contracts,
                            'entry_price': entry_price / 100.0
                        }
                    else:
                        # Paper trade mock
                        print(f"   📄 PAPER: {decision} {contracts} @ {entry_price}c")
                        status = "MOCK_SUCCESS"
                        self.last_traded_ticker = ticker
                        self.positions[ticker] = {
                            'side': side,
                            'contracts': contracts,
                            'entry_price': entry_price / 100.0
                        }

                    # Log order outcome
                    with open(self.log_file, "a", newline="") as f:
                        csv.writer(f).writerow([
                            timestamp, coin_name, ticker, strike, mid, current_price,
                            decision, contracts, entry_price, client_order_id, status,
                            self.current_cash_floor, str(DRY_RUN), signal_id
                        ])
                self.last_traded_ticker = ticker

                with open(self.log_file, "a", newline="") as f:
                    csv.writer(f).writerow([
                        timestamp, coin_name, ticker, strike, mid, current_price,
                        decision, contracts, entry_price, client_order_id, status, self.current_cash_floor, "false", signal_id
                    ])

                self.positions[ticker] = {'side': side, 'contracts': contracts, 'entry_price': entry_price}

            except Exception as e:
                print(f"Error processing {coin_name}: {e}")

    async def get_rsi(self, coin, period=14):
        ohlcv = await self.exchange.fetch_ohlcv(COINBASE_PRODUCTS[coin], '1m', limit=period+1)
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi.iloc[-1]

    async def run(self):
        print(f"🚀 GrokEmpire 15m Agent — LIVE (BTC Only)")
        print(f"   Base floor: ${BASE_CASH_FLOOR:.0f} | DRY_RUN: {DRY_RUN}")
        asyncio.create_task(self.coinbase_websocket())

        while self.running:
            await self.kalshi_discovery()
            await asyncio.sleep(30)

async def monitor_positions(self):
    while self.running:
        try:
            data = await self.client.get_positions()
            for p in data.get('positions', []):
                ticker = p['ticker']
                if ticker in self.positions:
                    pos = self.positions[ticker]
                    # Fetch current mid
                    m = await self.client.get_markets(ticker=ticker)
                    mid = (float(m['yes_bid_dollars']) + float(m['yes_ask_dollars'])) / 2
                    pnl_pct = (mid - pos['entry_price']) / pos['entry_price'] if pos['side'] == 'yes' else (pos['entry_price'] - mid) / pos['entry_price']
                    if pnl_pct <= -0.25 or pnl_pct >= 0.50:
                        await self.client.close_position(ticker)
                        del self.positions[ticker]
                        print(f"Early exit {ticker} PnL {pnl_pct:.1%}")
        except:
            pass
        await asyncio.sleep(30)

async def main():
    agent = Crypto15mAgent()
    try:
        await agent.run()
    except KeyboardInterrupt:
        print("\nShutting down...")

if __name__ == "__main__":
    asyncio.run(main())