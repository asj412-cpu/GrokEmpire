"""
Crypto 15m Agent — Mean-Reversion Fade Strategy
================================================
8/10 rolling settlement fade + cheap contract entry (12-54c).
Hold to settlement. Max 2 contracts per market (average down once).
"""

import asyncio
import os
import csv
import time
import random
from datetime import datetime
import websockets
import json
import ccxt
from kalshi_client.client import KalshiClient
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv

load_dotenv(override=True)

# ─── Config ───
BASE_CASH_FLOOR = 40.0
RATCHET_PERCENT = 0.80
DRY_RUN = os.getenv('DRY_RUN', 'false').lower() == 'true'

ENTRY_LOW = 12   # min entry price in cents
ENTRY_HIGH = 54  # max entry price in cents
FADE_THRESHOLD = 8  # out of 10 rolling cycles
FADE_WINDOW = 10
MAX_CONTRACTS_PER_MARKET = 2
VALUE_HUNTER_WINDOW_MINUTES = 8  # first 8 min of cycle

COINS = {
    "BTC": "KXBTC15M",
    "ETH": "KXETH15M",
    "SOL": "KXSOL15M",
    "XRP": "KXXRP15M",
    "BNB": "KXBNB15M",
    "HYPE": "KXHYPE15M",
    "DOGE": "KXDOGE15M",
}

COINBASE_PRODUCTS = {
    "BTC": "BTC-USD",
    "ETH": "ETH-USD",
    "SOL": "SOL-USD",
    "XRP": "XRP-USD",
    "BNB": "BNB-USD",
    "HYPE": "HYPE-USD",
    "DOGE": "DOGE-USD",
}

EXCHANGE = ccxt.coinbase()


class Crypto15mAgent:
    def __init__(self):
        key_id = os.getenv("KALSHI_KEY_ID")
        private_key_env = os.getenv("KALSHI_PRIVATE_KEY")
        self.client = None
        if key_id and private_key_env and not DRY_RUN:
            try:
                from kalshi_client.utils import load_private_key_from_file
                if os.path.exists(private_key_env):
                    priv_obj = load_private_key_from_file(private_key_env)
                else:
                    priv_obj = private_key_env
                self.client = KalshiClient(
                    key_id=key_id,
                    private_key=priv_obj,
                    exchange_api_base='https://api.elections.kalshi.com/trade-api/v2'
                )
                print("🔥 LIVE Kalshi client ready")
            except Exception as e:
                print(f"Kalshi client init failed ({e}) – dry-run mode")
                self.client = None
        else:
            print("DRY_RUN or missing keys – paper mode")

        self.running = True
        self.exchange = ccxt.coinbase()
        self.prices = {coin: None for coin in COINS}
        self.log_file = "15m_signals.csv"
        self.current_cash_floor = BASE_CASH_FLOOR
        self.last_balance = 0.0
        self.mock_balance = 1000.0

        # Per-coin rolling settlement history (deque of "yes"/"no")
        self.settlement_history = {coin: [] for coin in COINS}
        # Per-ticker: how many contracts already placed this market
        self.ticker_contracts = {}

        if not os.path.exists(self.log_file):
            with open(self.log_file, "w", newline="") as f:
                csv.writer(f).writerow([
                    "timestamp", "coin", "ticker", "mid", "decision",
                    "entry_price", "contracts", "fade_signal", "status"
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

    def get_balance(self):
        if not self.client:
            return self.mock_balance if DRY_RUN else 0.0
        try:
            data = self.client.get_balance()
            return data.get("balance", 0) / 100.0
        except Exception as e:
            print(f"Balance error: {e}")
            return self.last_balance

    def fetch_settlement_history(self, series):
        """Fetch last FADE_WINDOW settled markets for rolling history."""
        try:
            data = self.client.get_markets(series_ticker=series, status="settled", limit=FADE_WINDOW)
            markets = data.get("markets", [])
            markets.sort(key=lambda m: m.get("close_time", ""))
            return [m.get("result", "") for m in markets]
        except Exception as e:
            print(f"Settlement history error: {e}")
            return []

    def get_fade_signal(self, history):
        """
        Returns 'buy_yes', 'buy_no', or None.
        8/10 NO → fade → buy YES
        8/10 YES → fade → buy NO
        """
        if len(history) < FADE_WINDOW:
            return None
        recent = history[-FADE_WINDOW:]
        yes_count = sum(1 for r in recent if r == "yes")
        no_count = FADE_WINDOW - yes_count

        if no_count >= FADE_THRESHOLD:
            return "buy_yes"
        elif yes_count >= FADE_THRESHOLD:
            return "buy_no"
        return None

    async def kalshi_discovery(self):
        balance = self.get_balance() or self.last_balance
        if balance > self.current_cash_floor:
            new_floor = balance * RATCHET_PERCENT
            if new_floor > self.current_cash_floor:
                self.current_cash_floor = new_floor
        self.last_balance = balance

        # Ensure live prices if WS not yet connected
        if not any(self.prices.values()):
            for coin in COINS:
                try:
                    self.prices[coin] = self.exchange.fetch_ticker(COINBASE_PRODUCTS[coin])['last']
                except Exception:
                    pass

        for coin_name, series in COINS.items():
            if not self.prices.get(coin_name):
                continue

            # Get open market
            if self.client:
                try:
                    data = self.client.get_markets(series_ticker=series, status="open", limit=1)
                except Exception as e:
                    print(f"Error {coin_name}: {e}")
                    continue
            else:
                data = {"markets": [{"ticker": f"{series}-MOCK",
                    "yes_bid_dollars": random.uniform(0.15, 0.85),
                    "yes_ask_dollars": random.uniform(0.15, 0.85) + 0.01,
                    "volume_fp": 50000}]}

            if not data.get("markets"):
                continue

            m = data["markets"][0]
            ticker = m.get("ticker")
            yes_bid = float(m.get("yes_bid_dollars", 0))
            yes_ask = float(m.get("yes_ask_dollars", 0))
            if not yes_bid or not yes_ask:
                continue

            mid = (yes_bid + yes_ask) / 2
            yes_cost = int(round(yes_ask * 100))
            no_cost = int(round((1.0 - yes_bid) * 100))
            minutes_remaining = 15 - (datetime.now().minute % 15)

            # Fetch settlement history for this coin
            if self.client:
                history = self.fetch_settlement_history(series)
                self.settlement_history[coin_name] = history

            fade_signal = self.get_fade_signal(self.settlement_history[coin_name])

            # Only trade in first 8 minutes of cycle
            if minutes_remaining < (15 - VALUE_HUNTER_WINDOW_MINUTES):
                fade_signal = None

            # Check how many contracts already on this ticker
            existing = self.ticker_contracts.get(ticker, 0)
            if existing >= MAX_CONTRACTS_PER_MARKET:
                fade_signal = None

            decision = "HOLD"
            entry_price = None

            if fade_signal == "buy_yes" and ENTRY_LOW <= yes_cost <= ENTRY_HIGH:
                decision = "BUY YES"
                entry_price = yes_cost
            elif fade_signal == "buy_no" and ENTRY_LOW <= no_cost <= ENTRY_HIGH:
                decision = "BUY NO"
                entry_price = no_cost

            timestamp = datetime.now().strftime("%H:%M:%S")
            hist_summary = ""
            if len(self.settlement_history[coin_name]) >= FADE_WINDOW:
                recent = self.settlement_history[coin_name][-FADE_WINDOW:]
                yes_ct = sum(1 for r in recent if r == "yes")
                hist_summary = f"{yes_ct}Y/{FADE_WINDOW - yes_ct}N"

            print(f"[{timestamp}] {coin_name} | Mid: {mid:.4f} | YES:{yes_cost}c NO:{no_cost}c | Min left: {minutes_remaining} | Fade: {hist_summary} | Decision: {decision}")

            if decision in ("BUY YES", "BUY NO"):
                side = "yes" if decision == "BUY YES" else "no"
                client_order_id = f"fade-{side}-{ticker}-{int(time.time())}"

                print(f"  → {decision} 1 @ {entry_price}c | ID: {client_order_id}")

                if self.client and not DRY_RUN:
                    try:
                        result = self.client.create_order(
                            ticker=ticker,
                            client_order_id=client_order_id,
                            side=side,
                            action="buy",
                            count=1,
                            type="limit",
                            yes_price=entry_price if side == "yes" else None,
                            no_price=entry_price if side == "no" else None,
                        )
                        status = "SUCCESS" if result else "FAILED"
                    except Exception as e:
                        print(f"  Order error: {e}")
                        status = "ERROR"
                elif DRY_RUN:
                    print(f"  📄 PAPER: {decision} 1 @ {entry_price}c")
                    self.mock_balance -= entry_price / 100.0
                    status = "MOCK"
                else:
                    status = "NO_CLIENT"

                self.ticker_contracts[ticker] = existing + 1

                with open(self.log_file, "a", newline="") as f:
                    csv.writer(f).writerow([
                        timestamp, coin_name, ticker, f"{mid:.4f}", decision,
                        entry_price, 1, hist_summary, status
                    ])

    async def run(self):
        coins_str = ", ".join(COINS.keys())
        print(f"🚀 Crypto 15m Fade Agent — {coins_str}")
        print(f"   Fade: {FADE_THRESHOLD}/{FADE_WINDOW} | Entry: {ENTRY_LOW}-{ENTRY_HIGH}c | Max: {MAX_CONTRACTS_PER_MARKET}/mkt | Window: {VALUE_HUNTER_WINDOW_MINUTES}min")
        print(f"   DRY_RUN: {DRY_RUN}")
        asyncio.create_task(self.coinbase_websocket())

        while self.running:
            try:
                await self.kalshi_discovery()
            except Exception as e:
                print(f"Cycle error: {e}")
            await asyncio.sleep(30)


async def main():
    agent = Crypto15mAgent()
    try:
        await agent.run()
    except KeyboardInterrupt:
        print("\nShutting down...")

if __name__ == "__main__":
    asyncio.run(main())
