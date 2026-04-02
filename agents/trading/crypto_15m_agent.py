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
from dotenv import load_dotenv

load_dotenv(override=True)

BASE_CASH_FLOOR = 40.0
RATCHET_PERCENT = 0.80
RISK_MULTIPLIER = 0.08          # base – dynamic engine boosts on value plays
DRY_RUN = os.getenv('DRY_RUN', 'false').lower() == 'true'  # PRODUCTION DEFAULT = false

# Value Hunter (early-cycle only – first 5 minutes)
VALUE_HUNTER_WINDOW_MINUTES = 5
VALUE_LOW = 0.15
VALUE_HIGH = 0.54
MAX_RISK_MULTIPLIER = 0.18

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

THRESHOLDS = {
    "BTC": {"yes": 0.54, "no": 0.15, "min_vol": 40000, "distance": 200},
    "ETH": {"yes": 0.54, "no": 0.15, "min_vol": 30000, "distance": 50},
    "SOL": {"yes": 0.54, "no": 0.15, "min_vol": 25000, "distance": 0.5},
    "XRP": {"yes": 0.54, "no": 0.15, "min_vol": 20000, "distance": 0.01},
    "BNB": {"yes": 0.54, "no": 0.15, "min_vol": 15000, "distance": 5},
    "HYPE": {"yes": 0.54, "no": 0.15, "min_vol": 10000, "distance": 0.05},
    "DOGE": {"yes": 0.54, "no": 0.15, "min_vol": 20000, "distance": 0.001},
}

# Distance as % of coin price per time bucket (from backtest optimization)
# BTC/ETH optimal at 0.25x, SOL/DOGE at 0.50x of base
BASE_DIST_PCT = {2: 0.0004, 5: 0.001, 10: 0.0015, 15: 0.0025}
DIST_SCALES = {"BTC": 0.25, "ETH": 0.25, "SOL": 0.50, "XRP": 0.50, "BNB": 0.50, "HYPE": 0.50, "DOGE": 0.50}

# Exchange instance
EXCHANGE = ccxt.coinbase()  # Coinbase exchange

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
                    exchange_api_base='https://trading-api.kalshi.com/trade-api/v2'
                )
                print("🔥 LIVE Kalshi client ready – REAL orders enabled")
            except Exception as e:
                print(f"Kalshi client init failed ({e}) – falling back to dry-run")
                self.client = None
        else:
            print("DRY_RUN or missing keys – paper mode only")

        # PNL ledger for real-money dashboard
        self.pnl_ledger = "15m_pnl_ledger.csv"
        if not os.path.exists(self.pnl_ledger):
            with open(self.pnl_ledger, "w", newline="") as f:
                csv.writer(f).writerow(["timestamp", "ticker", "side", "contracts", "entry_price", "outcome", "realized_pnl", "cash_floor"])

        self.running = True
        self.exchange = ccxt.coinbase()
        self.prices = {coin: None for coin in COINS}
        self.log_file = "15m_signals.csv"
        self.last_traded_ticker = None
        self.current_cash_floor = BASE_CASH_FLOOR
        self.last_balance = 0.0
        self.positions = {}
        self.mock_balance = 1000.0
        self.cycle_momentum_trades = 0
        self.cycle_value_trades = 0
        self.cycle_paper_pnl = 0.0
        self.entries = {}

        if not os.path.exists(self.log_file):
            with open(self.log_file, "w", newline="") as f:
                csv.writer(f).writerow([
                    "timestamp", "coin", "ticker", "strike", "mid", "coin_price",
                    "decision", "contracts", "price", "client_order_id", "status",
                    "current_floor", "dry_run", "signal_id", "benchmark", "strike_vs_benchmark", "cycle_profitable"
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
            if DRY_RUN:
                return self.mock_balance
            else:
                print("ERROR: No Kalshi client in production mode (DRY_RUN=false)")
                return 0.0
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

        # Ensure live prices for tracking if WS not yet fed data
        if not any(self.prices.values()):
            for coin in COINS:
                try:
                    ticker = COINBASE_PRODUCTS[coin]
                    price = self.exchange.fetch_ticker(ticker)['last']
                    self.prices[coin] = price
                    print(f"Initial price for {coin}: {price}")
                except Exception as e:
                    print(f"Price fetch error for {coin}: {e}")

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
                mock_ticker = series
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
            volume = float(m.get("volume_fp", 0))
            strike_raw = m.get("floor_strike")
            if not yes_bid or not yes_ask:
                continue

            strike = float(strike_raw) if strike_raw is not None else 0.0
            mid = (yes_bid + yes_ask) / 2
            current_price = self.prices.get(coin_name)

            th = THRESHOLDS.get(coin_name, THRESHOLDS["BTC"])
            decision = "HOLD"
            decision_type = "MOMENTUM"  # default
            entry_price = None
            minutes_remaining = 15 - (datetime.now().minute % 15)

            if current_price is not None:
                distance = abs(current_price - strike)

                # Price-proportional distance (backtest-optimized per-coin scales)
                scale = DIST_SCALES.get(coin_name, 0.50)
                if minutes_remaining <= 2:
                    min_distance = current_price * BASE_DIST_PCT[2] * scale
                elif minutes_remaining <= 5:
                    min_distance = current_price * BASE_DIST_PCT[5] * scale
                elif minutes_remaining <= 10:
                    min_distance = current_price * BASE_DIST_PCT[10] * scale
                else:
                    min_distance = current_price * BASE_DIST_PCT[15] * scale

                # === MOMENTUM LAYER (any time in cycle) ===
                if mid <= th["no"] and volume >= th["min_vol"]:
                    decision = "BUY NO"
                    entry_price = int(round((1.0 - yes_bid) * 100))
                    entry_price = max(1, min(90, entry_price))
                elif mid >= th["yes"] and distance > min_distance:
                    decision = "BUY YES"
                    entry_price = int(round(yes_ask * 100))
                    entry_price = max(1, min(90, entry_price))

                # === VALUE HUNTER LAYER (first 0-5 minutes of cycle ONLY) ===
                if decision == "HOLD" and minutes_remaining >= (15 - VALUE_HUNTER_WINDOW_MINUTES):
                    if VALUE_LOW <= mid <= VALUE_HIGH:
                        # Buy the cheap side that aligns with current price action vs strike
                        if current_price > strike and mid <= VALUE_HIGH:   # YES is undervalued while price is already above strike
                            decision = "BUY YES"
                            decision_type = "VALUE"
                        elif current_price < strike and mid <= VALUE_HIGH:  # NO is undervalued while price is below strike
                            decision = "BUY NO"
                            decision_type = "VALUE"
                        elif mid < 0.33:  # fallback – buy the cheaper contract in value zone
                            decision = "BUY NO"
                            decision_type = "VALUE"
                        else:
                            decision = "BUY YES"
                            decision_type = "VALUE"

                        # Set entry price
                        entry_price = int(round(yes_ask * 100)) if decision == "BUY YES" else int(round((1.0 - yes_bid) * 100))
                        entry_price = max(1, min(90, entry_price))

                # Price guards (real-money safety – never removed)
                if mid >= 0.88:
                    print(f"   PRICE GUARD: rejected BUY YES on {coin_name} at mid {mid:.4f}")
                    decision = "HOLD"
                elif mid <= 0.12:
                    print(f"   PRICE GUARD: rejected BUY NO on {coin_name} at mid {mid:.4f}")
                    decision = "HOLD"

            # RSI filter (kept)
            rsi = await self.get_rsi(coin_name)
            if decision == "BUY YES" and rsi < 60:
                decision = "HOLD"
            elif decision == "BUY NO" and rsi > 40:
                decision = "HOLD"

            # Benchmark + strike vs price (computed every cycle; profitable check end-of-cycle only)
            benchmark = await self.get_1min_benchmark(coin_name)
            strike_vs_benchmark = "N/A"
            cycle_profitable = False
            if strike is not None and benchmark > 0.0:
                strike_vs_benchmark = "ABOVE" if strike > benchmark else "BELOW"
                if minutes_remaining <= 1 and decision in ("BUY YES", "BUY NO"):
                    cycle_profitable = (decision == "BUY YES" and strike < benchmark) or (decision == "BUY NO" and strike > benchmark)

            # DYNAMIC RISK MULTIPLIER (boosts Value Hunter)
            risk_mult = self.calculate_dynamic_risk_multiplier(mid, decision_type, minutes_remaining)
            WIN_RATE = 0.55
            AVG_WIN = 40
            AVG_LOSS = 25
            kelly = (WIN_RATE * AVG_WIN - (1 - WIN_RATE) * AVG_LOSS) / AVG_WIN * 0.25
            contracts = max(1, int(riskable * risk_mult * kelly)) if riskable > 0 else 0

            full_decision = f"{decision_type} {decision}" if decision != "HOLD" else "HOLD"

            timestamp = datetime.now().strftime("%H:%M:%S")
            signal_id = f"{coin_name}-{ticker}-{int(datetime.now().timestamp())}"

            print(f"[{timestamp}] {coin_name} | Mid: {mid:.4f} | Price: {current_price} | Benchmark: {benchmark:.4f} | Min left: {minutes_remaining} | Decision: {full_decision} | Risk×: {risk_mult:.3f}")

            # Log every cycle
            with open(self.log_file, "a", newline="") as f:
                csv.writer(f).writerow([
                    timestamp, coin_name, ticker, strike, mid, current_price,
                    full_decision, contracts, entry_price, None, "CYCLE_LOG", self.current_cash_floor, str(DRY_RUN), signal_id,
                    benchmark, strike_vs_benchmark, cycle_profitable
                ])

            # REAL TRADE TRIGGER – all coins, no gates
            if decision in ("BUY YES", "BUY NO") and riskable > 0:
                side = "yes" if decision == "BUY YES" else "no"
                client_order_id = f"cr15m-{side}-{ticker}-{int(time.time())}"

                print(f"[LIVE] {full_decision} {contracts} @ {entry_price}¢ on {coin_name} | ID: {client_order_id}")

                if self.client and not DRY_RUN:
                    result = await self.client.place_order(ticker, side, contracts, price=entry_price)
                    status = "SUCCESS" if result else "FAILED"
                elif DRY_RUN:
                    print(f"   📄 PAPER: {full_decision} {contracts} @ {entry_price}c")
                    cost = contracts * (entry_price / 100.0)
                    self.mock_balance -= cost
                    status = "MOCK_SUCCESS"
                else:
                    print(f"   ERROR: Production mode but no client - skipping order")
                    status = "NO_CLIENT"
                    client_order_id = None

                self.last_traded_ticker = ticker
                self.entries[ticker] = {'side': side, 'contracts': contracts, 'entry_price': entry_price / 100.0, 'entry_time': time.time()}

                if decision_type == "VALUE":
                    self.cycle_value_trades += 1
                else:
                    self.cycle_momentum_trades += 1

                # order log row
                with open(self.log_file, "a", newline="") as f:
                    csv.writer(f).writerow([
                        timestamp, coin_name, ticker, strike, mid, current_price,
                        full_decision, contracts, entry_price, client_order_id, status,
                        self.current_cash_floor, str(DRY_RUN), signal_id,
                        benchmark, strike_vs_benchmark, cycle_profitable
                    ])


    async def get_rsi(self, coin, period=14):
        ohlcv = self.exchange.fetch_ohlcv(COINBASE_PRODUCTS[coin], '1m', limit=period+1)
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi.iloc[-1]

    async def get_1min_benchmark(self, coin):
        """Calculate 1min rolling benchmark (rolling mean of close, not spot price)."""
        try:
            ohlcv = self.exchange.fetch_ohlcv(COINBASE_PRODUCTS.get(coin, "BTC-USD"), '1m', limit=5)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            return float(df['close'].rolling(window=1).mean().iloc[-1])
        except Exception as e:
            print(f"Benchmark error for {coin}: {e}")
            return self.prices.get(coin, 0.0)

    def calculate_dynamic_risk_multiplier(self, mid: float, decision_type: str, minutes_remaining: int) -> float:
        """Dynamic compounding engine – extra boost on early-cycle Value Hunter plays."""
        base = RISK_MULTIPLIER
        boost = 0.0

        if decision_type == "VALUE":
            boost += 0.08  # premium for catching early mispricings
        if VALUE_LOW <= mid <= VALUE_HIGH:
            boost += 0.04  # true value zone
        if minutes_remaining >= 12:  # very early cycle
            boost += 0.02

        return min(base + boost, MAX_RISK_MULTIPLIER)

    async def run(self):
        print(f"🚀 GrokEmpire 15m Agent — LIVE (Multi-Coin Paper Mode)")
        print(f"   Base floor: ${BASE_CASH_FLOOR:.0f} | DRY_RUN: {DRY_RUN}")
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