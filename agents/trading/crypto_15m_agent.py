"""
Crypto 15m Agent — BRTI High-Conviction Strategy + Kalshi WebSocket
====================================================================
Trades BTC, ETH, SOL, XRP 15-minute Kalshi markets using a synthetic BRTI
index built from real-time WebSocket feeds (Coinbase, Kraken, Bitstamp, Gemini).

Strategy: enter on sBRTI direction, detect mathematical lock on 60s-smoothed
settlement, conviction-add when locked, take profit at 95c, one flip max per
cycle, hard stop at 35c loss. No trailing stop. No protect profit.
"""

import asyncio
import os
import csv
import time
import json
import math
import base64
from datetime import datetime
from typing import Dict
from collections import defaultdict

import statistics
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

# ─── BRTI Config ───
BRTI_SMOOTHING_WINDOW = 60    # 60-second rolling average to simulate settlement smoothing

# Per-coin trading parameters — scaled proportionally to price
# BTC ~$73,000 (base), ETH ~$2,250 (0.031x), SOL ~$170 (0.0023x), XRP ~$1.35 (0.0000185x)
BRTI_COIN_CONFIG = {
    "BTC": {
        "series": "KXBTC15M",
        "direction_min_dist": 20.0,          # position-based direction threshold
        "direction_min_momentum": 5.0,       # 25% of direction_min_dist
        "conviction_min_distance": 50.0,     # smoothed $ past strike to add
        "momentum_flip_distance": 100.0,     # projected $ past strike on wrong side to flip
        "max_spot_move_per_sec": 50.0,       # extreme max $/sec for lock calc
        "flip_cooldown_sec": 90,
        "stop_loss_hard_c": 35,
        "take_profit_c": 95,
        "reentry_max_price": 59,
        "entry_max": 79,
        "entry_contracts": 2,
        "conviction_max_adds": 2,
        "conviction_cooldown_sec": 60,
        "conviction_max_price": 85,
        "momentum_window": 15,
        "ws_pairs": {"coinbase": "BTC-USD", "kraken": "XBT/USD", "bitstamp": "btcusd", "gemini": "BTCUSD"},
    },
    "ETH": {
        "series": "KXETH15M",
        "direction_min_dist": 0.62,
        "direction_min_momentum": 0.155,
        "conviction_min_distance": 1.55,
        "momentum_flip_distance": 3.10,
        "max_spot_move_per_sec": 1.50,
        "flip_cooldown_sec": 90,
        "stop_loss_hard_c": 35,
        "take_profit_c": 95,
        "reentry_max_price": 59,
        "entry_max": 79,
        "entry_contracts": 2,
        "conviction_max_adds": 2,
        "conviction_cooldown_sec": 60,
        "conviction_max_price": 85,
        "momentum_window": 15,
        "ws_pairs": {"coinbase": "ETH-USD", "kraken": "ETH/USD", "bitstamp": "ethusd", "gemini": "ETHUSD"},
    },
    "SOL_DISABLED": {
        "series": "KXSOL15M",
        "direction_min_dist": 0.047,
        "direction_min_momentum": 0.012,
        "conviction_min_distance": 0.117,
        "momentum_flip_distance": 0.23,
        "max_spot_move_per_sec": 0.12,
        "flip_cooldown_sec": 90,
        "stop_loss_hard_c": 35,
        "take_profit_c": 95,
        "reentry_max_price": 59,
        "entry_max": 79,
        "entry_contracts": 2,
        "conviction_max_adds": 2,
        "conviction_cooldown_sec": 60,
        "conviction_max_price": 85,
        "momentum_window": 15,
        "ws_pairs": {"coinbase": "SOL-USD", "kraken": "SOL/USD", "bitstamp": "solusd", "gemini": "SOLUSD"},
    },
    # XRP DISABLED — direction_min_dist too small, enters on noise, instant hard stops
    # Need to calibrate thresholds from live cycle data before enabling
    "XRP_DISABLED": {
        "series": "KXXRP15M",
        "direction_min_dist": 0.00037,
        "direction_min_momentum": 0.000093,
        "conviction_min_distance": 0.00093,
        "momentum_flip_distance": 0.00185,
        "max_spot_move_per_sec": 0.001,
        "flip_cooldown_sec": 90,
        "stop_loss_hard_c": 35,
        "take_profit_c": 95,
        "reentry_max_price": 59,
        "entry_max": 79,
        "entry_contracts": 2,
        "conviction_max_adds": 2,
        "conviction_cooldown_sec": 60,
        "conviction_max_price": 85,
        "momentum_window": 15,
        "ws_pairs": {"coinbase": "XRP-USD", "kraken": "XRP/USD", "bitstamp": "xrpusd", "gemini": "XRPUSD"},
    },
}

# Synthetic BRTI — real-time feed from constituent exchange WebSockets
# Volume-weighted median of Coinbase, Kraken, Bitstamp, Gemini (~80%+ of BRTI weight)
def _build_brti_exchanges():
    """Build per-exchange WS config that subscribes to ALL BRTI coins' pairs."""
    all_coinbase_pairs = [cfg["ws_pairs"]["coinbase"] for cfg in BRTI_COIN_CONFIG.values()]
    all_kraken_pairs = [cfg["ws_pairs"]["kraken"] for cfg in BRTI_COIN_CONFIG.values()]
    return {
        "coinbase": {
            "url": "wss://ws-feed.exchange.coinbase.com",
            "subscribe": {"type": "subscribe", "channels": [{"name": "ticker", "product_ids": all_coinbase_pairs}]},
        },
        "kraken": {
            "url": "wss://ws.kraken.com",
            "subscribe": {"event": "subscribe", "pair": all_kraken_pairs, "subscription": {"name": "trade"}},
        },
        "bitstamp": {
            "url": "wss://ws.bitstamp.net",
            "channels": [cfg["ws_pairs"]["bitstamp"] for cfg in BRTI_COIN_CONFIG.values()],
        },
        "gemini": {
            "symbols": [cfg["ws_pairs"]["gemini"] for cfg in BRTI_COIN_CONFIG.values()],
        },
    }

BRTI_EXCHANGES = _build_brti_exchanges()

# Reverse lookup: exchange pair name -> coin (e.g., "BTC-USD" -> "BTC", "ethusd" -> "ETH")
PAIR_TO_COIN = {}
for _coin, _cfg in BRTI_COIN_CONFIG.items():
    for _exchange, _pair in _cfg["ws_pairs"].items():
        PAIR_TO_COIN[(_exchange, _pair)] = _coin
        PAIR_TO_COIN[(_exchange, _pair.lower())] = _coin
        PAIR_TO_COIN[(_exchange, _pair.upper())] = _coin

ROLLOVER_GUARD_SEC = 5     # first 5s of each new cycle: skip evals until tickers refresh
MIN_TICKER_LIFE_SEC = 60   # only subscribe to markets with >60s until close

# COINS maps coin name -> Kalshi series ticker
COINS = {coin: cfg["series"] for coin, cfg in BRTI_COIN_CONFIG.items()}

KALSHI_WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"
KALSHI_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"

# Lock detection safety factor
LOCK_SAFETY_FACTOR = 1.5


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

        # Per-ticker contract count
        self.ticker_contracts: Dict[str, int] = {}
        # Kalshi-authoritative position count (from post_position_fp in fill messages)
        self.kalshi_positions: Dict[str, int] = {}

        # Per-ticker: held contracts and resting sell orders
        self.positions: Dict[str, list] = defaultdict(list)
        self.resting_sells: Dict[str, int] = {}

        # Resting buy orders
        self.resting_buys: Dict[str, dict] = {}

        # Cooldown tracking: ticker -> last buy fill timestamp (ms)
        self.last_buy_ts: Dict[str, int] = {}
        self.last_buy_price: Dict[str, int] = {}

        # Current open market tickers per coin
        self.current_tickers: Dict[str, str] = {}
        self.ticker_refreshed_ts: Dict[str, int] = {}

        # Kalshi WS ticker cache: ticker -> {yes_bid, yes_ask, ...}
        self.ws_prices: Dict[str, dict] = {}
        self.ws_connected = False

        # BRTI state per coin
        self.brti_state: Dict[str, dict] = {}
        for _coin in BRTI_COIN_CONFIG:
            self.brti_state[_coin] = {
                "ticks": [],              # [(unix_ts, value), ...] rolling synthetic index
                "strike": 0.0,            # current cycle strike price
                "direction": "",          # "up" or "down" from initial momentum
                "entry_made": False,      # whether we entered this cycle
                "held_side": "",          # "yes" or "no" — what we currently hold
                "last_flip_ts": 0.0,      # timestamp of last flip
                "entry_price": 0,         # what we paid for current position (cents)
                "peak_value": 0,          # highest value our position has reached (cents)
                "conviction_adds": 0,     # conviction buys this cycle
                "last_conviction_ts": 0.0,
                "last_tp_ts": 0.0,        # timestamp of last take profit (re-entry cooldown)
                "entered_this_cycle": False,
                "flipped_this_cycle": False,  # ONE flip max per cycle
                "original_entry_count": 0,    # contracts from initial entry (for flip sizing)
            }

        # Exchange price feeds for synthetic index
        self.exchange_prices: Dict[str, Dict[str, float]] = defaultdict(dict)
        self.exchange_trades: Dict[str, Dict[str, list]] = defaultdict(lambda: defaultdict(list))
        self.exchange_status: Dict[str, str] = {}

        if not os.path.exists(self.log_file):
            with open(self.log_file, "w", newline="") as f:
                csv.writer(f).writerow([
                    "timestamp", "coin", "ticker", "yes_cost", "no_cost",
                    "decision", "entry_price", "fade_signal", "status"
                ])

    # ─── Mathematical Lock Detection ─────────────────────────

    def detect_spike(self, coin: str) -> float:
        """
        Detect if a massive price spike happened in the last 5 seconds.
        Returns the magnitude of the move (0 if no spike).
        Used to invalidate locks and trigger fast-path flips.
        """
        st = self.brti_state.get(coin, {})
        ticks = st.get("ticks", [])
        if len(ticks) < 5:
            return 0
        now_ts = time.time()
        recent = [v for t, v in ticks if t > now_ts - 5]
        if len(recent) < 2:
            return 0
        return abs(max(recent) - min(recent))

    def is_locked(self, coin: str, smoothed_brti: float, strike: float, secs_remaining: float) -> bool:
        """
        Check if the 60s-smoothed sBRTI settlement is mathematically locked.

        The smoothed average can only move by (T/60) * max_spot_move_per_second in T seconds.
        We apply a 1.5x safety factor. If the current distance from strike exceeds the
        max possible move with safety factor, settlement direction is guaranteed.

        SPIKE INVALIDATION: if a massive move happened in the last 5s, the normal
        max_spot_move_per_sec assumption is wrong — invalidate the lock.
        """
        cfg = BRTI_COIN_CONFIG.get(coin, {})
        max_spot_move = cfg.get("max_spot_move_per_sec", 50.0)
        T = max(0.0, float(secs_remaining))
        if T <= 0:
            return True  # no time left = locked by definition

        # Check for spike — if spot just moved more than 2x the expected max in 5s,
        # the market is in an extreme regime and lock assumptions don't hold
        spike = self.detect_spike(coin)
        if spike > max_spot_move * 10:  # 10 seconds worth of max move in 5 seconds = extreme
            return False  # invalidate lock

        max_smooth_move = (T / 60.0) * max_spot_move
        max_smooth_move_safe = max_smooth_move * LOCK_SAFETY_FACTOR

        distance = abs(smoothed_brti - strike)
        return distance > max_smooth_move_safe

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
        rotated = False
        now_ms = int(datetime.now().timestamp() * 1000)
        min_close_ts = int(datetime.now().timestamp()) + MIN_TICKER_LIFE_SEC
        for coin, series in COINS.items():
            try:
                data = self.client.get_markets(series_ticker=series, status="open", limit=1)
                for m in data.get("markets", []):
                    ticker = m.get("ticker")
                    if not ticker:
                        continue
                    close_time_str = m.get("close_time", "")
                    try:
                        close_dt = datetime.fromisoformat(close_time_str.replace("Z", "+00:00"))
                        close_ts = int(close_dt.timestamp())
                    except (ValueError, AttributeError):
                        close_ts = 0
                    if close_ts and close_ts <= min_close_ts:
                        continue
                    if self.current_tickers.get(coin) != ticker:
                        rotated = True
                        old = self.current_tickers.get(coin)
                        if old:
                            self.ws_prices.pop(old, None)
                    self.current_tickers[coin] = ticker
                    self.ticker_refreshed_ts[ticker] = now_ms
                    tickers.append(ticker)
                    if coin in BRTI_COIN_CONFIG:
                        strike = m.get("floor_strike")
                        if strike:
                            self.brti_state[coin]["strike"] = float(strike)
                            print(f"  📍 {coin} strike: ${self.brti_state[coin]['strike']:,.2f}")
            except Exception as e:
                print(f"  Open market lookup error {coin}: {e}")

        # Reset cooldowns and state on cycle rotation
        if rotated:
            self.last_buy_ts.clear()
            self.last_buy_price.clear()
            for _coin in BRTI_COIN_CONFIG:
                st = self.brti_state[_coin]
                old_ticker = self.current_tickers.get(_coin, "")
                if old_ticker:
                    self.ticker_contracts[old_ticker] = 0
                st["direction"] = ""
                st["entry_made"] = False
                st["held_side"] = ""
                st["last_flip_ts"] = 0
                st["entry_price"] = 0
                st["peak_value"] = 0
                st["conviction_adds"] = 0
                st["last_conviction_ts"] = 0
                st["last_tp_ts"] = 0.0
                st["entered_this_cycle"] = False
                st["flipped_this_cycle"] = False
                st["original_entry_count"] = 0
                print(f"  🔄 {_coin} BRTI cycle reset (strike: ${st['strike']:,.2f})")
            self._mid_cycle_startup = False  # clear entry block on new cycle

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
                try:
                    yes_bid = int(float(msg.get("yes_bid_dollars", "0")) * 100)
                    yes_ask = int(float(msg.get("yes_ask_dollars", "0")) * 100)
                except (ValueError, TypeError):
                    yes_bid = yes_ask = 0
                self.ws_prices[ticker] = {
                    "yes_bid": yes_bid,
                    "yes_ask": yes_ask,
                }
                # Minimal: just update ws_prices. All trading logic is in the fast loop.

        elif msg_type == "fill":
            msg = data.get("msg", {})
            ticker = msg.get("market_ticker", "")
            side = msg.get("side", "")
            action = msg.get("action", "")
            price_dollars = msg.get("yes_price_dollars") or msg.get("price_dollars") or msg.get("no_price_dollars")
            if price_dollars:
                try:
                    price = int(float(price_dollars) * 100)
                except (ValueError, TypeError):
                    price = msg.get("yes_price", 0) or msg.get("price", 0) or 0
            else:
                price = msg.get("yes_price", 0) or msg.get("price", 0) or 0
            count = round(float(msg.get("count_fp") or msg.get("count") or 0))
            coin = None
            for c, t in self.current_tickers.items():
                if t == ticker:
                    coin = c
                    break

            purchased_side = (msg.get("purchased_side") or side or "").lower()
            if action == "sell":
                cost_price = price if side.lower() == "yes" else (100 - price)
            else:
                cost_price = price if purchased_side == "yes" else (100 - price)

            print(f"  💰 FILL: {action.upper()} {side.upper()} {coin or ticker} {count}x @ {cost_price}c (yes_px={price}c) | raw={msg}")

            if action == "buy":
                self.ticker_contracts[ticker] = self.ticker_contracts.get(ticker, 0) + count
                self.positions[ticker].append({"side": side, "entry_price": cost_price, "count": count})
                if ticker in self.resting_buys:
                    del self.resting_buys[ticker]
                self.last_buy_ts[ticker] = int(datetime.now().timestamp() * 1000)
                self.last_buy_price[ticker] = cost_price
                print(f"  ✅ Bought! Now {self.ticker_contracts[ticker]} contracts @ {cost_price}c (HOLD to settlement)")
                post_pos = msg.get("post_position_fp")
                if post_pos is not None:
                    actual_count = abs(int(round(float(post_pos))))
                    tracked_count = self.ticker_contracts.get(ticker, 0)
                    if actual_count != tracked_count:
                        print(f"  ⚠️ POSITION SYNC: {ticker} tracked={tracked_count} kalshi={actual_count} — correcting")
                        self.ticker_contracts[ticker] = actual_count
                    self.kalshi_positions[ticker] = actual_count
                    # Note: removed "absorbed by residuals" reset — positions don't
                    # carry across 15-min cycles. Each cycle is a fresh market.

            elif action == "sell":
                self.resting_sells[ticker] = max(0, self.resting_sells.get(ticker, 0) - count)
                if self.positions.get(ticker):
                    for _ in range(min(count, len(self.positions[ticker]))):
                        self.positions[ticker].pop(0)
                self.ticker_contracts[ticker] = max(0, self.ticker_contracts.get(ticker, 0) - count)
                print(f"  ✅ Exit filled! Held: {self.ticker_contracts[ticker]} | Resting sells: {self.resting_sells.get(ticker, 0)}")
                post_pos = msg.get("post_position_fp")
                if post_pos is not None:
                    actual_count = abs(int(round(float(post_pos))))
                    tracked_count = self.ticker_contracts.get(ticker, 0)
                    if actual_count != tracked_count:
                        print(f"  ⚠️ POSITION SYNC: {ticker} tracked={tracked_count} kalshi={actual_count} — correcting")
                        self.ticker_contracts[ticker] = actual_count
                    self.kalshi_positions[ticker] = actual_count
                    if actual_count == 0:
                        self.positions[ticker] = []

        elif msg_type == "subscribed":
            channel = data.get("msg", {}).get("channel", "")
            print(f"  WS subscribed: {channel}")

    def _post_buy(self, ticker, coin, side, price, target_contracts, count=1):
        """Post a limit buy order via asyncio.to_thread to avoid blocking."""
        client_order_id = f"brti-{side}-{ticker}-{int(time.time())}"
        decision = f"BUY {side.upper()}"
        existing = self.ticker_contracts.get(ticker, 0)

        print(f"[{datetime.now().strftime('%H:%M:%S')}] {coin} | {decision} {count} @ {price}c | held:{existing}/{target_contracts}")

        if self.client and not DRY_RUN:
            try:
                self.client.create_order(
                    ticker=ticker,
                    client_order_id=client_order_id,
                    side=side,
                    action="buy",
                    count=count,
                    type="limit",
                    yes_price=price if side == "yes" else None,
                    no_price=price if side == "no" else None,
                )
                self.resting_buys[ticker] = {"order_id": client_order_id, "side": side, "price": price, "coin": coin}
            except Exception as e:
                print(f"  → Buy error: {e}")
                self.ticker_contracts[ticker] = max(0, self.ticker_contracts.get(ticker, 0) - count)
        elif DRY_RUN:
            self.resting_buys[ticker] = {"order_id": client_order_id, "side": side, "price": price, "coin": coin}
            print(f"  📄 PAPER: → buy {side} @ {price}c (HOLD to settlement)")

        with open(self.log_file, "a", newline="") as f:
            csv.writer(f).writerow([
                datetime.now().strftime("%H:%M:%S"), coin, ticker,
                "—", "—", decision, price, "brti", "RESTING"
            ])

    async def _post_buy_async(self, ticker, coin, side, price, count=1):
        """Non-blocking buy order — wraps create_order in asyncio.to_thread."""
        client_order_id = f"brti-{side}-{ticker}-{int(time.time())}"
        decision = f"BUY {side.upper()}"
        existing = self.ticker_contracts.get(ticker, 0)

        print(f"[{datetime.now().strftime('%H:%M:%S')}] {coin} | {decision} {count} @ {price}c | held:{existing}")

        if self.client and not DRY_RUN:
            try:
                await asyncio.to_thread(
                    self.client.create_order,
                    ticker=ticker,
                    client_order_id=client_order_id,
                    side=side,
                    action="buy",
                    count=count,
                    type="limit",
                    yes_price=price if side == "yes" else None,
                    no_price=price if side == "no" else None,
                )
                self.resting_buys[ticker] = {"order_id": client_order_id, "side": side, "price": price, "coin": coin}
            except Exception as e:
                print(f"  → Buy error: {e}")
                self.ticker_contracts[ticker] = max(0, self.ticker_contracts.get(ticker, 0) - count)
        elif DRY_RUN:
            self.resting_buys[ticker] = {"order_id": client_order_id, "side": side, "price": price, "coin": coin}
            print(f"  📄 PAPER: → buy {side} @ {price}c (HOLD to settlement)")

        with open(self.log_file, "a", newline="") as f:
            csv.writer(f).writerow([
                datetime.now().strftime("%H:%M:%S"), coin, ticker,
                "—", "—", decision, price, "brti", "RESTING"
            ])

    async def _sell_async(self, ticker, coin, side, count, yes_bid, yes_ask, reason=""):
        """Non-blocking sell order — wraps create_order in asyncio.to_thread."""
        order_id = f"sell-{coin.lower()}-{ticker}-{int(time.time()*1000)}"
        if self.client and not DRY_RUN:
            try:
                await asyncio.to_thread(
                    self.client.create_order,
                    ticker=ticker,
                    client_order_id=order_id,
                    side=side,
                    action="sell",
                    count=count,
                    type="limit",
                    yes_price=yes_bid if side == "yes" else None,
                    no_price=(100 - yes_ask) if side == "no" else None,
                )
                print(f"  → {coin} Sell {count}x {side.upper()} ({reason})")
            except Exception as e:
                print(f"  → {coin} Sell error ({reason}): {e}")
        else:
            print(f"  📄 PAPER: {coin} sell {count}x {side.upper()} ({reason})")

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

    # ─── Settlement History ───────────────────────────────────

    def seed_open_positions(self):
        """Log existing Kalshi positions but DON'T set ticker_contracts.
        Residual positions from previous processes cause state confusion.
        Let the fill handler reconcile via post_position_fp instead."""
        if not self.client:
            return
        try:
            positions = self.client.get_positions()
            for p in positions.get("event_positions", []):
                event_ticker = p.get("event_ticker", "")
                exposure = float(p.get("event_exposure_dollars", 0))
                if exposure > 0:
                    print(f"  📌 {event_ticker}: live exposure ${exposure:.2f} (will reconcile on first fill)")
        except Exception as e:
            print(f"  Position seed error: {e}")

    def seed_settlement_history(self):
        self.seed_open_positions()
        if not self.client:
            self.history_seeded = True
            return
        for coin_name, series in COINS.items():
            try:
                data = self.client.get_markets(series_ticker=series, status="settled", limit=10)
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
                    if len(self.settlement_history[coin_name]) > 20:
                        self.settlement_history[coin_name] = self.settlement_history[coin_name][-20:]
                    self.last_settled_ticker[coin_name] = latest_ticker
                    hist = self.settlement_history[coin_name][-10:]
                    yes_ct = sum(1 for r in hist if r == "yes")
                    print(f"  📊 {coin_name} settled {result.upper()} → {yes_ct}Y/{10 - yes_ct}N (last 10)")
        except Exception:
            pass

    # ─── Fast Loop — Core Trading Logic ───────────────────────

    async def brti_fast_flip_loop(self):
        """SIMPLIFIED: 1 contract, enter at min 10, hold to settlement. No stops, no flips, no conviction.
        Take profit at 95c is the only exit. Uses ONLY cached ws_prices."""
        while self.running:
            for coin, cfg in BRTI_COIN_CONFIG.items():
                try:
                    st = self.brti_state[coin]
                    if st["strike"] <= 0 or not st["ticks"]:
                        continue
                    coin_ticker = self.current_tickers.get(coin, "")
                    if not coin_ticker:
                        continue

                    now = datetime.now()
                    cycle_sec = (now.minute % 15) * 60 + now.second
                    secs_remaining = max(1, 900 - cycle_sec)
                    now_ts = time.time()

                    # Rollover guard
                    if cycle_sec < ROLLOVER_GUARD_SEC:
                        continue
                    now_ms = int(now.timestamp() * 1000)
                    refreshed_ms = self.ticker_refreshed_ts.get(coin_ticker, 0)
                    if now_ms - refreshed_ms > 60_000:
                        continue

                    latest_brti = st["ticks"][-1][1] if st["ticks"] else 0
                    if latest_brti <= 0:
                        continue

                    # ── Determine direction (once per cycle) ──
                    direction_min_dist = cfg.get("direction_min_dist", 20.0)
                    direction_min_momentum = cfg.get("direction_min_momentum", 5.0)

                    if not st["direction"] and len(st["ticks"]) >= 10:
                        recent_ticks = [v for _, v in st["ticks"][-10:]]
                        if recent_ticks:
                            recent_avg = sum(recent_ticks) / len(recent_ticks)
                            distance_from_strike = recent_avg - st["strike"]

                            if abs(distance_from_strike) >= direction_min_dist:
                                st["direction"] = "up" if distance_from_strike > 0 else "down"
                                print(f"[{now.strftime('%H:%M:%S')}] {coin} direction: {st['direction']} (${distance_from_strike:+,.2f} from strike)")
                            else:
                                st["direction"] = "flat"

                    # ── Get WS prices ──
                    prices = self.ws_prices.get(coin_ticker, {})
                    yes_ask = prices.get("yes_ask", 0)
                    yes_bid = prices.get("yes_bid", 0)
                    if yes_ask <= 0 or yes_bid <= 0:
                        continue

                    # ── ENTRY: 1 contract at minute 10+, hold to settlement ──
                    if (st["direction"] and st["direction"] != "flat"
                            and not st["entry_made"] and not st["held_side"]
                            and not self._mid_cycle_startup
                            and cycle_sec >= 600):  # minute 10+
                        target_side = "yes" if st["direction"] == "up" else "no"
                        cost = yes_ask if target_side == "yes" else (100 - yes_bid)
                        entry_max = cfg.get("entry_max", 79)

                        if 1 <= cost <= entry_max:
                            st["entry_made"] = True
                            st["held_side"] = target_side
                            st["entry_price"] = cost
                            self.ticker_contracts[coin_ticker] = 1
                            print(f"[{now.strftime('%H:%M:%S')}] {coin} ENTRY {target_side.upper()} 1x @ {cost}c (dir {st['direction']}, strike ${st['strike']:,.2f})")
                            await self._post_buy_async(coin_ticker, coin, target_side, cost, count=1)
                        continue

                    # ── TAKE PROFIT at 95c (only exit) ──
                    if not st["held_side"]:
                        continue
                    held = self.ticker_contracts.get(coin_ticker, 0)
                    if held <= 0:
                        continue

                    if st["held_side"] == "yes":
                        current_value = yes_bid
                    else:
                        current_value = 100 - yes_ask

                    take_profit_c = cfg.get("take_profit_c", 95)
                    if current_value >= take_profit_c:
                        old_side = st["held_side"]
                        profit = current_value - st["entry_price"]
                        print(f"[{now.strftime('%H:%M:%S')}] 💰 {coin} TAKE PROFIT: {old_side.upper()} 1x @ {current_value}c (pnl:+{profit}c)")
                        self.ticker_contracts[coin_ticker] = 0
                        st["held_side"] = ""
                        st["entry_made"] = True  # don't re-enter this cycle
                        await self._sell_async(coin_ticker, coin, old_side, 1, yes_bid, yes_ask, reason=f"TP +{profit}c")
                        continue

                    # ── Otherwise: HOLD TO SETTLEMENT. No stops. No flips. ──
                        # Projected settlement on the wrong side?
                    # No flips. No stops. Hold to settlement.
                        continue

                except Exception as e:
                    print(f"  Fast loop error ({coin}): {e}")
            await asyncio.sleep(0.5)

    # ─── Periodic Tasks ───────────────────────────────────────

    def compute_synthetic_brti(self, coin):
        """Volume-weighted median of exchange trade prices for a specific coin → synthetic index."""
        prices_with_volume = []
        for ex in self.exchange_trades:
            trades = self.exchange_trades[ex].get(coin, [])
            cutoff = time.time() - 5
            recent = [(p, v) for t, p, v in trades if t > cutoff]
            if not recent:
                if self.exchange_prices.get(ex, {}).get(coin):
                    prices_with_volume.append((self.exchange_prices[ex][coin], 0.001))
                continue
            total_vol = sum(v for _, v in recent)
            vwap = sum(p * v for p, v in recent) / total_vol if total_vol > 0 else recent[-1][0]
            prices_with_volume.append((vwap, total_vol))
        if not prices_with_volume:
            return None
        prices_with_volume.sort(key=lambda x: x[0])
        total_volume = sum(v for _, v in prices_with_volume)
        if total_volume <= 0:
            return statistics.median([p for p, _ in prices_with_volume])
        cumulative = 0
        for price, vol in prices_with_volume:
            cumulative += vol
            if cumulative >= total_volume / 2:
                return price
        return prices_with_volume[-1][0]

    def _record_exchange_trade(self, exchange, coin, price, volume):
        """Record a trade from an exchange for a specific coin and update synthetic index."""
        if price <= 0:
            return
        self.exchange_prices[exchange][coin] = price
        self.exchange_trades[exchange][coin].append((time.time(), price, volume))
        cutoff = time.time() - 60
        self.exchange_trades[exchange][coin] = [(t, p, v) for t, p, v in self.exchange_trades[exchange][coin] if t > cutoff]
        synthetic = self.compute_synthetic_brti(coin)
        if synthetic:
            now = time.time()
            st = self.brti_state[coin]
            if not st["ticks"] or now - st["ticks"][-1][0] >= 0.5:
                st["ticks"].append((now, synthetic))
                cutoff = now - 960
                st["ticks"] = [(t, v) for t, v in st["ticks"] if t > cutoff]

    async def _coinbase_ws(self):
        """Coinbase Exchange WebSocket — ticker channel for all BRTI coins."""
        while self.running:
            try:
                async with websockets.connect(BRTI_EXCHANGES["coinbase"]["url"], ping_interval=30, ping_timeout=10) as ws:
                    await ws.send(json.dumps(BRTI_EXCHANGES["coinbase"]["subscribe"]))
                    self.exchange_status["coinbase"] = "connected"
                    print("  ✅ Coinbase connected")
                    async for msg in ws:
                        try:
                            data = json.loads(msg)
                            if data.get("type") == "ticker":
                                product_id = data.get("product_id", "")
                                coin = PAIR_TO_COIN.get(("coinbase", product_id))
                                if coin:
                                    price = float(data.get("price", 0))
                                    vol = float(data.get("last_size", 0))
                                    self._record_exchange_trade("coinbase", coin, price, vol)
                        except Exception:
                            pass
            except Exception as e:
                self.exchange_status["coinbase"] = "error"
                print(f"  Coinbase WS error: {e}, reconnecting...")
                await asyncio.sleep(5)

    async def _kraken_ws(self):
        """Kraken WebSocket — trade channel for all BRTI coins."""
        while self.running:
            try:
                async with websockets.connect(BRTI_EXCHANGES["kraken"]["url"], ping_interval=30, ping_timeout=10) as ws:
                    await ws.send(json.dumps(BRTI_EXCHANGES["kraken"]["subscribe"]))
                    self.exchange_status["kraken"] = "connected"
                    print("  ✅ Kraken connected")
                    async for msg in ws:
                        try:
                            data = json.loads(msg)
                            if isinstance(data, list) and len(data) >= 4:
                                pair_name = data[-1] if isinstance(data[-1], str) else ""
                                coin = PAIR_TO_COIN.get(("kraken", pair_name))
                                if coin:
                                    trades = data[1] if isinstance(data[1], list) else []
                                    for trade in trades:
                                        if isinstance(trade, list) and len(trade) >= 2:
                                            self._record_exchange_trade("kraken", coin, float(trade[0]), float(trade[1]))
                        except Exception:
                            pass
            except Exception as e:
                self.exchange_status["kraken"] = "error"
                print(f"  Kraken WS error: {e}, reconnecting...")
                await asyncio.sleep(5)

    async def _bitstamp_ws(self):
        """Bitstamp WebSocket — live trades for all BRTI coins."""
        while self.running:
            try:
                async with websockets.connect(BRTI_EXCHANGES["bitstamp"]["url"], ping_interval=30, ping_timeout=10) as ws:
                    for channel in BRTI_EXCHANGES["bitstamp"]["channels"]:
                        await ws.send(json.dumps({
                            "event": "bts:subscribe",
                            "data": {"channel": f"live_trades_{channel}"}
                        }))
                    self.exchange_status["bitstamp"] = "connected"
                    print("  ✅ Bitstamp connected")
                    async for msg in ws:
                        try:
                            data = json.loads(msg)
                            if data.get("event") == "trade":
                                channel = data.get("channel", "")
                                pair = channel.replace("live_trades_", "")
                                coin = PAIR_TO_COIN.get(("bitstamp", pair))
                                if coin:
                                    td = data.get("data", {})
                                    self._record_exchange_trade("bitstamp", coin, float(td.get("price", 0)), float(td.get("amount", 0)))
                        except Exception:
                            pass
            except Exception as e:
                self.exchange_status["bitstamp"] = "error"
                print(f"  Bitstamp WS error: {e}, reconnecting...")
                await asyncio.sleep(5)

    async def _gemini_ws_single(self, symbol, coin):
        """Gemini WebSocket for a single symbol — auto-streams trades."""
        url = f"wss://api.gemini.com/v1/marketdata/{symbol}?trades=true&bids=false&offers=false"
        while self.running:
            try:
                async with websockets.connect(url, ping_interval=30, ping_timeout=10) as ws:
                    self.exchange_status[f"gemini_{coin}"] = "connected"
                    print(f"  ✅ Gemini {coin} connected")
                    async for msg in ws:
                        try:
                            data = json.loads(msg)
                            for ev in data.get("events", []):
                                if ev.get("type") == "trade":
                                    self._record_exchange_trade("gemini", coin, float(ev.get("price", 0)), float(ev.get("amount", 0)))
                        except Exception:
                            pass
            except Exception as e:
                self.exchange_status[f"gemini_{coin}"] = "error"
                print(f"  Gemini {coin} WS error: {e}, reconnecting...")
                await asyncio.sleep(5)

    async def _gemini_ws(self):
        """Launch Gemini WebSocket connections for all BRTI coins."""
        tasks = []
        for coin, cfg in BRTI_COIN_CONFIG.items():
            symbol = cfg["ws_pairs"]["gemini"]
            tasks.append(asyncio.create_task(self._gemini_ws_single(symbol, coin)))
        await asyncio.gather(*tasks)

    async def settlement_check_loop(self):
        """Every 30s: check for new settlements, refresh subscriptions, log status."""
        while self.running:
            try:
                for coin, series in COINS.items():
                    self.check_new_settlement(coin, series)

                # Re-subscribe if markets rotated
                if self.ws_connected:
                    await self._subscribe_open_markets()

                # Balance ratchet
                balance = self.get_balance() or self.last_balance
                if balance > self.current_cash_floor:
                    new_floor = balance * RATCHET_PERCENT
                    if new_floor > self.current_cash_floor:
                        self.current_cash_floor = new_floor
                self.last_balance = balance

                # Status line
                now = datetime.now()
                minutes_remaining = 15 - (now.minute % 15)
                cycle_sec = (now.minute % 15) * 60 + now.second
                secs_remaining = max(1, 900 - cycle_sec)
                now_ts = time.time()

                coin_parts = []
                for _coin in BRTI_COIN_CONFIG:
                    _st = self.brti_state[_coin]
                    _val = f"${_st['ticks'][-1][1]:,.2f}" if _st["ticks"] else "?"
                    _age = f"{now_ts - _st['ticks'][-1][0]:.1f}s" if _st["ticks"] else "?"
                    _ticker = self.current_tickers.get(_coin, "")
                    _held = self.ticker_contracts.get(_ticker, 0)
                    _side = _st["held_side"].upper() if _st["held_side"] else "flat"

                    # Lock status
                    if _st["ticks"] and _st["strike"] > 0:
                        _smooth = [v for t, v in _st["ticks"] if t > now_ts - BRTI_SMOOTHING_WINDOW]
                        _smoothed = sum(_smooth) / len(_smooth) if _smooth else 0
                        _locked = self.is_locked(_coin, _smoothed, _st["strike"], secs_remaining)
                        _lock_str = "🔒" if _locked else "open"
                    else:
                        _lock_str = "?"

                    coin_parts.append(f"{_coin}:{_val}({_age}) stk${_st['strike']:,.0f} {_st['direction'] or 'wait'} {_side} {_held}x {_lock_str}")

                feeds = sum(1 for s in self.exchange_status.values() if s == "connected")
                num_feeds_expected = len(BRTI_COIN_CONFIG) + 3
                status = " | ".join(coin_parts) + f" | feeds:{feeds}/{num_feeds_expected}"
                print(f"[{now.strftime('%H:%M:%S')}] Min left: {minutes_remaining} | {status}")

            except Exception as e:
                print(f"Settlement check error: {e}")

            now2 = datetime.now()
            cycle_sec_now = (now2.minute % 15) * 60 + now2.second
            sec_to_boundary = max(1, 900 - cycle_sec_now + 1)
            await asyncio.sleep(min(30, sec_to_boundary))

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
        coins_str = ", ".join(BRTI_COIN_CONFIG.keys())
        print(f"🚀 Crypto 15m Agent — High-Conviction Strategy — {coins_str}")
        print(f"   Strategy: enter on sBRTI direction, lock detection, conviction adds, 1 flip max, TP@95c, hard stop@35c")
        for _coin, _cfg in BRTI_COIN_CONFIG.items():
            print(f"   {_coin}: entry≤{_cfg['entry_max']}c {_cfg['entry_contracts']}x | TP:{_cfg['take_profit_c']}c | hard_stop:{_cfg['stop_loss_hard_c']}c | conv_dist:{_cfg['conviction_min_distance']} | flip_dist:{_cfg['momentum_flip_distance']} | max_move/s:{_cfg['max_spot_move_per_sec']}")
        print(f"   Source: synthetic (Coinbase+Kraken+Bitstamp+Gemini WebSockets)")
        print(f"   DRY_RUN: {DRY_RUN} | No trailing stop | No protect profit | Lock safety: {LOCK_SAFETY_FACTOR}x")

        # Startup guard: if mid-cycle, block NEW ENTRIES but still run protection loops
        now = datetime.now()
        cycle_sec = (now.minute % 15) * 60 + now.second
        if 60 < cycle_sec < 840:
            self._mid_cycle_startup = True
            print(f"⏳ Mid-cycle startup (min {cycle_sec//60}) — entries blocked until next cycle, protection active")
        else:
            self._mid_cycle_startup = False

        print("Seeding settlement history...")
        self.seed_settlement_history()

        # Launch WS and loops as parallel tasks
        if self.client and self.key_id and self.private_key_path:
            asyncio.create_task(self.kalshi_websocket())
        asyncio.create_task(self.settlement_check_loop())
        asyncio.create_task(self._coinbase_ws())
        asyncio.create_task(self._kraken_ws())
        asyncio.create_task(self._bitstamp_ws())
        asyncio.create_task(self._gemini_ws())
        asyncio.create_task(self.brti_fast_flip_loop())
        brti_coins_str = "+".join(BRTI_COIN_CONFIG.keys())
        print(f"📡 Synthetic index feeds launching ({brti_coins_str}, 4 exchanges) + fast loop (500ms)")

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
