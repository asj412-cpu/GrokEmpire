"""
Crypto 15m Agent — Mean-Reversion Fade + Kalshi WebSocket
=========================================================
6/10 rolling settlement fade + cheap contract entry (5-40c).
Uses Kalshi WebSocket for real-time prices — reacts instantly to dips.
Max 2 contracts per market. Hold to settlement.
"""

import asyncio
import os
import csv
import time
import json
import base64
from datetime import datetime
from typing import Dict, Optional, Set
from collections import defaultdict

import websockets
from kalshi_client.client import KalshiClient
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend
from dotenv import load_dotenv

load_dotenv(override=True)

# ─── Config ───
BASE_CASH_FLOOR = 40.0
RATCHET_PERCENT = 0.80
DRY_RUN = os.getenv('DRY_RUN', 'false').lower() == 'true'

ENTRY_LOW = 5    # min entry price in cents
ENTRY_HIGH = 30  # max entry price in cents
EXIT_MULTIPLIER = 2  # resting sell at 2x entry price
FADE_THRESHOLD = 6  # out of 10 rolling cycles
FADE_WINDOW = 10
MAX_CONTRACTS_PER_MARKET = 3
VALUE_HUNTER_WINDOW_MINUTES = 8

COINS = {
    "BTC": "KXBTC15M",
    "ETH": "KXETH15M",
    "SOL": "KXSOL15M",
    "XRP": "KXXRP15M",
    "BNB": "KXBNB15M",
    "HYPE": "KXHYPE15M",
    "DOGE": "KXDOGE15M",
}

KALSHI_WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"
KALSHI_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"


class Crypto15mAgent:
    def __init__(self):
        self.key_id = os.getenv("KALSHI_KEY_ID")
        self.private_key_path = os.getenv("KALSHI_PRIVATE_KEY")
        self.client = None

        if self.key_id and self.private_key_path and not DRY_RUN:
            try:
                from kalshi_client.utils import load_private_key_from_file
                if os.path.exists(self.private_key_path):
                    priv_obj = load_private_key_from_file(self.private_key_path)
                else:
                    priv_obj = self.private_key_path
                self.client = KalshiClient(
                    key_id=self.key_id,
                    private_key=priv_obj,
                    exchange_api_base=KALSHI_API_BASE
                )
                print("🔥 LIVE Kalshi client ready")
            except Exception as e:
                print(f"Kalshi client init failed ({e}) – dry-run mode")
        else:
            print("DRY_RUN or missing keys – paper mode")

        self.running = True
        self.log_file = "15m_signals.csv"
        self.current_cash_floor = BASE_CASH_FLOOR
        self.last_balance = 0.0
        self.mock_balance = 1000.0

        # Settlement history per coin
        self.settlement_history: Dict[str, list] = {coin: [] for coin in COINS}
        self.last_settled_ticker: Dict[str, str] = {}
        self.history_seeded = False

        # Per-ticker contract count (max 3)
        self.ticker_contracts: Dict[str, int] = {}

        # Per-ticker: held contracts and resting sell orders
        self.positions: Dict[str, list] = defaultdict(list)
        self.resting_sells: Dict[str, int] = {}  # ticker -> count of resting sell orders

        # Resting buy orders — drift with market
        # {ticker: {order_id, side, price, coin}}
        self.resting_buys: Dict[str, dict] = {}

        # Current open market tickers per coin
        self.current_tickers: Dict[str, str] = {}

        # Kalshi WS ticker cache: ticker -> {yes_bid, yes_ask, ...}
        self.ws_prices: Dict[str, dict] = {}
        self.ws_connected = False

        if not os.path.exists(self.log_file):
            with open(self.log_file, "w", newline="") as f:
                csv.writer(f).writerow([
                    "timestamp", "coin", "ticker", "yes_cost", "no_cost",
                    "decision", "entry_price", "fade_signal", "status"
                ])

    # ─── Kalshi WebSocket ─────────────────────────────────────

    def _generate_ws_signature(self, timestamp_ms: int) -> str:
        with open(self.private_key_path, 'rb') as f:
            private_key = serialization.load_pem_private_key(f.read(), password=None, backend=default_backend())
        message = f"{timestamp_ms}GET/trade-api/ws/v2"
        signature = private_key.sign(
            message.encode('utf-8'),
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH),
            hashes.SHA256()
        )
        return base64.b64encode(signature).decode('utf-8')

    async def kalshi_websocket(self):
        """Connect to Kalshi WS, subscribe to open 15m markets, react to price updates."""
        while self.running:
            try:
                timestamp_ms = int(time.time() * 1000)
                sig = self._generate_ws_signature(timestamp_ms)
                headers = {
                    "KALSHI-ACCESS-KEY": self.key_id,
                    "KALSHI-ACCESS-SIGNATURE": sig,
                    "KALSHI-ACCESS-TIMESTAMP": str(timestamp_ms),
                }

                async with websockets.connect(KALSHI_WS_URL, additional_headers=headers, ping_interval=30, ping_timeout=10) as ws:
                    self.ws_connected = True
                    self.ws = ws
                    print("✅ Kalshi WS connected")

                    # Subscribe to fills
                    await ws.send(json.dumps({"id": 1, "cmd": "subscribe", "params": {"channels": ["fill"]}}))

                    # Subscribe to current open market tickers
                    await self._subscribe_open_markets()

                    async for msg in ws:
                        try:
                            data = json.loads(msg)
                            await self._handle_ws_message(data)
                        except Exception as e:
                            print(f"WS msg error: {e}")

            except Exception as e:
                print(f"Kalshi WS error: {e}, reconnecting...")
                self.ws_connected = False
                await asyncio.sleep(5)

    async def _subscribe_open_markets(self):
        """Find open markets for all coins and subscribe to their tickers."""
        if not self.client:
            return
        tickers = []
        for coin, series in COINS.items():
            try:
                data = self.client.get_markets(series_ticker=series, status="open", limit=1)
                for m in data.get("markets", []):
                    ticker = m.get("ticker")
                    if ticker:
                        self.current_tickers[coin] = ticker
                        tickers.append(ticker)
            except Exception as e:
                print(f"  Open market lookup error {coin}: {e}")

        if tickers and self.ws_connected:
            await self.ws.send(json.dumps({
                "id": 2,
                "cmd": "subscribe",
                "params": {"channels": ["ticker"], "market_tickers": tickers}
            }))
            print(f"📡 Subscribed to {len(tickers)} market tickers: {', '.join(tickers[-3:])}")

    async def _handle_ws_message(self, data):
        msg_type = data.get("type")

        if msg_type == "ticker":
            msg = data.get("msg", {})
            ticker = msg.get("market_ticker")
            if ticker:
                # WS sends dollar strings like "0.5500" — convert to cents
                try:
                    yes_bid = int(float(msg.get("yes_bid_dollars", "0")) * 100)
                    yes_ask = int(float(msg.get("yes_ask_dollars", "0")) * 100)
                except (ValueError, TypeError):
                    yes_bid = yes_ask = 0
                self.ws_prices[ticker] = {
                    "yes_bid": yes_bid,
                    "yes_ask": yes_ask,
                }
                # Evaluate trade on every price update
                await self._evaluate_trade(ticker)

        elif msg_type == "fill":
            msg = data.get("msg", {})
            ticker = msg.get("market_ticker", "")
            side = msg.get("side", "")
            action = msg.get("action", "")
            price = msg.get("yes_price", 0)
            count = msg.get("count", 0)
            coin = None
            for c, t in self.current_tickers.items():
                if t == ticker:
                    coin = c
                    break

            print(f"  💰 FILL: {action.upper()} {side.upper()} {coin or ticker} {count}x @ {price}c")

            if action == "buy":
                # Buy filled — increment position, clear resting buy, post resting sell
                self.ticker_contracts[ticker] = self.ticker_contracts.get(ticker, 0) + 1
                entry_price = price
                self.positions[ticker].append({"side": side, "entry_price": entry_price})
                if ticker in self.resting_buys:
                    del self.resting_buys[ticker]
                print(f"  ✅ Bought! Now {self.ticker_contracts[ticker]}/{MAX_CONTRACTS_PER_MARKET} contracts")
                # Post resting sell
                if self.client and not DRY_RUN and coin:
                    self._post_resting_sell(ticker, coin, side, entry_price)

            elif action == "sell":
                # Sell filled — decrement resting sells and positions
                self.resting_sells[ticker] = max(0, self.resting_sells.get(ticker, 0) - 1)
                if self.positions.get(ticker):
                    self.positions[ticker].pop(0)
                print(f"  ✅ Exit filled! Resting sells remaining: {self.resting_sells.get(ticker, 0)}")

        elif msg_type == "subscribed":
            channel = data.get("msg", {}).get("channel", "")
            print(f"  WS subscribed: {channel}")

    async def _evaluate_trade(self, ticker):
        """Called on every WS price update — manage resting buys that drift with market."""
        # Find which coin this ticker belongs to
        coin = None
        for c, t in self.current_tickers.items():
            if t == ticker:
                coin = c
                break
        if not coin:
            return

        # Time window check
        minutes_remaining = 15 - (datetime.now().minute % 15)
        in_window = minutes_remaining >= (15 - VALUE_HUNTER_WINDOW_MINUTES)

        # Contract count check
        existing = self.ticker_contracts.get(ticker, 0)
        at_max = existing >= MAX_CONTRACTS_PER_MARKET

        # Fade signal check
        fade_signal = self.get_fade_signal(self.settlement_history[coin])

        # Price check from WS cache
        prices = self.ws_prices.get(ticker)
        if not prices:
            return
        yes_ask = prices.get("yes_ask")
        yes_bid = prices.get("yes_bid")
        if yes_ask is None or yes_bid is None:
            return
        yes_cost = yes_ask
        no_cost = 100 - yes_bid if yes_bid else None
        if no_cost is None:
            return

        # Determine target side and current market price
        target_side = None
        market_price = None
        if fade_signal == "buy_yes":
            target_side = "yes"
            market_price = yes_cost
        elif fade_signal == "buy_no":
            target_side = "no"
            market_price = no_cost

        # Check if we have a resting buy for this ticker
        resting = self.resting_buys.get(ticker)

        # Cancel resting buy if outside window, at max, or no fade signal
        if resting and (not in_window or at_max or not fade_signal):
            self._cancel_order(resting["order_id"], ticker, "buy window closed")
            del self.resting_buys[ticker]
            return

        # No signal or outside window — nothing to do
        if not fade_signal or not in_window or at_max or not target_side:
            return

        in_range = ENTRY_LOW <= market_price <= ENTRY_HIGH

        if resting:
            # Already have a resting buy — drift it down if market dropped
            if in_range and market_price < resting["price"]:
                # Market dropped — cancel old, post new at lower price
                self._cancel_order(resting["order_id"], ticker, f"drift {resting['price']}c→{market_price}c")
                self._post_buy(ticker, coin, target_side, market_price)
            elif not in_range:
                # Price left our range — cancel
                self._cancel_order(resting["order_id"], ticker, "left range")
                del self.resting_buys[ticker]
        else:
            # No resting buy — post one if in range
            if in_range:
                self._post_buy(ticker, coin, target_side, market_price)

    def _post_buy(self, ticker, coin, side, price):
        """Post a resting limit buy order."""
        client_order_id = f"fade-{side}-{ticker}-{int(time.time())}"
        hist = self.settlement_history.get(coin, [])[-FADE_WINDOW:]
        yes_ct = sum(1 for r in hist if r == "yes")
        fade_str = f"{yes_ct}Y/{FADE_WINDOW - yes_ct}N"
        decision = f"BUY {side.upper()}"

        print(f"[{datetime.now().strftime('%H:%M:%S')}] {coin} | {decision} 1 @ {price}c | Fade: {fade_str} | {ticker}")

        if self.client and not DRY_RUN:
            try:
                result = self.client.create_order(
                    ticker=ticker,
                    client_order_id=client_order_id,
                    side=side,
                    action="buy",
                    count=1,
                    type="limit",
                    yes_price=price if side == "yes" else None,
                    no_price=price if side == "no" else None,
                )
                self.resting_buys[ticker] = {"order_id": client_order_id, "side": side, "price": price, "coin": coin}
                print(f"  → Resting buy posted: {price}c")
            except Exception as e:
                print(f"  → Buy error: {e}")
        elif DRY_RUN:
            self.resting_buys[ticker] = {"order_id": client_order_id, "side": side, "price": price, "coin": coin}
            mult = 3 if price < 20 else EXIT_MULTIPLIER
            print(f"  📄 PAPER: {decision} 1 @ {price}c → sell @ {min(price * mult, 95)}c ({mult}x)")

        with open(self.log_file, "a", newline="") as f:
            csv.writer(f).writerow([
                datetime.now().strftime("%H:%M:%S"), coin, ticker,
                "—", "—", decision, price, fade_str, "RESTING"
            ])

    def _cancel_order(self, order_id, ticker, reason=""):
        """Cancel a resting order."""
        if self.client and not DRY_RUN:
            try:
                self.client.cancel_order(order_id)
                print(f"  ✗ Cancelled {order_id[:20]}... ({reason})")
            except Exception as e:
                print(f"  ✗ Cancel error: {e}")
        else:
            print(f"  ✗ PAPER cancel {order_id[:20]}... ({reason})")
        if ticker in self.resting_buys:
            del self.resting_buys[ticker]

    def _post_resting_sell(self, ticker, coin, side, entry_price):
        """Post a resting sell order at 2x (or 3x if <20c). Never sell more than held."""
        multiplier = 3 if entry_price < 20 else EXIT_MULTIPLIER
        sell_price = min(entry_price * multiplier, 95)  # cap at 95c

        # Safety: never post more sells than contracts held
        held = len(self.positions.get(ticker, []))
        existing_sells = self.resting_sells.get(ticker, 0)
        if existing_sells >= held:
            print(f"  ⚠ Skip sell — already {existing_sells} resting sells for {held} held")
            return

        sell_order_id = f"exit-{side}-{ticker}-{int(time.time())}"
        try:
            result = self.client.create_order(
                ticker=ticker,
                client_order_id=sell_order_id,
                side=side,
                action="sell",
                count=1,
                type="limit",
                yes_price=sell_price if side == "yes" else None,
                no_price=sell_price if side == "no" else None,
            )
            self.resting_sells[ticker] = existing_sells + 1
            print(f"  → Resting SELL {side.upper()} 1 @ {sell_price}c (2x of {entry_price}c) | {sell_order_id}")
        except Exception as e:
            print(f"  → Sell order error: {e}")

    # ─── Settlement History ───────────────────────────────────

    def seed_open_positions(self):
        if not self.client:
            return
        try:
            positions = self.client.get_positions()
            for p in positions.get("event_positions", []):
                event_ticker = p.get("event_ticker", "")
                count = int(float(p.get("total_cost_shares_fp", 0)))
                if count <= 0:
                    continue
                for series in COINS.values():
                    if event_ticker.startswith(series):
                        try:
                            mkts = self.client.get_markets(event_ticker=event_ticker, limit=5)
                            for m in mkts.get("markets", []):
                                t = m.get("ticker", "")
                                if t:
                                    self.ticker_contracts[t] = count
                            print(f"  📌 {event_ticker}: {count} contracts (blocked)")
                        except Exception:
                            pass
                        break
        except Exception as e:
            print(f"  Position seed error: {e}")

    def seed_settlement_history(self):
        self.seed_open_positions()
        if not self.client:
            self.history_seeded = True
            return
        for coin_name, series in COINS.items():
            try:
                data = self.client.get_markets(series_ticker=series, status="settled", limit=FADE_WINDOW)
                markets = data.get("markets", [])
                markets.sort(key=lambda m: m.get("close_time", ""))
                self.settlement_history[coin_name] = [m.get("result", "") for m in markets]
                if markets:
                    self.last_settled_ticker[coin_name] = markets[-1].get("ticker")
                print(f"  {coin_name}: seeded {len(self.settlement_history[coin_name])} → {self.settlement_history[coin_name]}")
            except Exception as e:
                print(f"  {coin_name}: seed error ({e})")
        self.history_seeded = True

    def check_new_settlement(self, coin_name, series):
        if not self.client:
            return
        try:
            data = self.client.get_markets(series_ticker=series, status="settled", limit=1)
            markets = data.get("markets", [])
            if not markets:
                return
            latest = markets[0]
            latest_ticker = latest.get("ticker")
            if latest_ticker and latest_ticker != self.last_settled_ticker.get(coin_name):
                result = latest.get("result", "")
                if result:
                    self.settlement_history[coin_name].append(result)
                    if len(self.settlement_history[coin_name]) > FADE_WINDOW * 2:
                        self.settlement_history[coin_name] = self.settlement_history[coin_name][-FADE_WINDOW * 2:]
                    self.last_settled_ticker[coin_name] = latest_ticker
                    hist = self.settlement_history[coin_name][-FADE_WINDOW:]
                    yes_ct = sum(1 for r in hist if r == "yes")
                    print(f"  📊 {coin_name} settled {result.upper()} → {yes_ct}Y/{FADE_WINDOW - yes_ct}N")
        except Exception:
            pass

    def get_fade_signal(self, history):
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

    # ─── Periodic Tasks ───────────────────────────────────────

    async def settlement_check_loop(self):
        """Every 30s: check for new settlements and refresh open market subscriptions."""
        while self.running:
            try:
                for coin, series in COINS.items():
                    self.check_new_settlement(coin, series)

                # Re-subscribe if markets rotated (new 15-min window)
                if self.ws_connected:
                    await self._subscribe_open_markets()

                # Balance ratchet
                balance = self.get_balance() or self.last_balance
                if balance > self.current_cash_floor:
                    new_floor = balance * RATCHET_PERCENT
                    if new_floor > self.current_cash_floor:
                        self.current_cash_floor = new_floor
                self.last_balance = balance

                # Log status every cycle
                minutes_remaining = 15 - (datetime.now().minute % 15)
                active_fades = []
                for coin in COINS:
                    sig = self.get_fade_signal(self.settlement_history[coin])
                    if sig:
                        hist = self.settlement_history[coin][-FADE_WINDOW:]
                        yes_ct = sum(1 for r in hist if r == "yes")
                        active_fades.append(f"{coin}:{sig.replace('buy_','').upper()}({yes_ct}Y/{FADE_WINDOW-yes_ct}N)")

                fades_str = " | ".join(active_fades) if active_fades else "none"
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Min left: {minutes_remaining} | Fades: {fades_str} | WS: {'✓' if self.ws_connected else '✗'}")

            except Exception as e:
                print(f"Settlement check error: {e}")

            await asyncio.sleep(30)

    def get_balance(self):
        if not self.client:
            return self.mock_balance if DRY_RUN else 0.0
        try:
            data = self.client.get_balance()
            return data.get("balance", 0) / 100.0
        except Exception as e:
            print(f"Balance error: {e}")
            return self.last_balance

    # ─── Main ─────────────────────────────────────────────────

    async def run(self):
        coins_str = ", ".join(COINS.keys())
        print(f"🚀 Crypto 15m Fade Agent — {coins_str}")
        print(f"   Fade: {FADE_THRESHOLD}/{FADE_WINDOW} | Entry: {ENTRY_LOW}-{ENTRY_HIGH}c | Max: {MAX_CONTRACTS_PER_MARKET}/mkt | Window: {VALUE_HUNTER_WINDOW_MINUTES}min")
        print(f"   DRY_RUN: {DRY_RUN} | WebSocket mode")

        print("Seeding settlement history...")
        self.seed_settlement_history()

        # Launch WS and settlement check as parallel tasks
        if self.client and self.key_id and self.private_key_path:
            asyncio.create_task(self.kalshi_websocket())
        asyncio.create_task(self.settlement_check_loop())

        # Keep main alive
        while self.running:
            await asyncio.sleep(60)


async def main():
    agent = Crypto15mAgent()
    try:
        await agent.run()
    except KeyboardInterrupt:
        print("\nShutting down...")

if __name__ == "__main__":
    asyncio.run(main())
