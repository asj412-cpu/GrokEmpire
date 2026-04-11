"""
Crypto 15m Agent — BRTI Momentum Strategy + Kalshi WebSocket
=============================================================
Trades BTC and ETH 15-minute Kalshi markets using a synthetic BRTI index
built from real-time WebSocket feeds (Coinbase, Kraken, Bitstamp, Gemini).
Entry from first 15s momentum direction, trailing stop + stop loss via
500ms fast flip loop, conviction adds on strong projected settlement.
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

# ─── Probability model config ───
# Flip when P(held side settles profitably) drops below this.
# Calibrated from 78 BTC / 52 ETH observed cycles (Apr 10-11 2026).
# 0.38 = conservative: only flip when we're clearly losing (~62% chance against us).
FLIP_PROBABILITY_THRESHOLD = float(os.getenv('FLIP_PROB_THRESHOLD', '0.38'))
SETTLEMENT_SMOOTHING_FACTOR = 0.55  # CF Benchmark 1-min VWAP reduces effective vol by ~sqrt(1/3)

# ─── BRTI Config ───
BRTI_SMOOTHING_WINDOW = 60    # 60-second rolling average to simulate settlement smoothing

# Per-coin trading parameters — tuned for each asset's volatility and price level
BRTI_COIN_CONFIG = {
    "BTC": {
        "series": "KXBTC15M",
        "flip_cooldown_sec": 90,
        "trailing_stop_c": 5,          # base trailing stop (dynamic: 5-15c based on distance from strike)
        "trailing_stop_far_c": 15,     # trailing stop when $50+ from strike
        "trailing_stop_mid_c": 10,     # trailing stop when $20-50 from strike
        "trailing_stop_near_c": 5,     # trailing stop when <$20 from strike (danger zone)
        "trailing_stop_far_dist": 50,  # "far" = $50+ from strike
        "trailing_stop_mid_dist": 20,  # "mid" = $20-50 from strike
        "stop_loss_hard_c": 20,        # hard stop: max loss regardless (emergency exit, go flat)
        "momentum_flip_distance": 30,  # momentum flip: projected settlement $30+ past strike on wrong side
        "conviction_min_distance": 50,
        "conviction_min_cycle_sec": 180,
        "conviction_max_adds": 2,
        "conviction_cooldown_sec": 60,
        "conviction_max_price": 72,
        "take_profit_c": 95,
        "reentry_max_price": 59,
        "entry_max": 49,
        "entry_contracts": 3,
        "momentum_window": 15,
        "ws_pairs": {"coinbase": "BTC-USD", "kraken": "XBT/USD", "bitstamp": "btcusd", "gemini": "BTCUSD"},
        # Probability model — calibrated from 78 observed 15m cycles (Apr 2026)
        # σ_15m=$66 => σ_per_sec=$66/sqrt(900)=$2.20/sec
        "volatility_per_sec": 2.20,
        "flip_probability_threshold": FLIP_PROBABILITY_THRESHOLD,
    },
    "ETH": {
        "series": "KXETH15M",
        "flip_cooldown_sec": 90,
        "trailing_stop_c": 5,
        "trailing_stop_far_c": 15,
        "trailing_stop_mid_c": 10,
        "trailing_stop_near_c": 5,
        "trailing_stop_far_dist": 3.00,  # ETH: $3.00 — wider, ETH oscillates $1-2 routinely
        "trailing_stop_mid_dist": 1.50,  # ETH: $1.50 — mid zone starts further out
        "stop_loss_hard_c": 35,          # ETH: wider hard stop — 50/50 contracts swing 30c on noise
        "momentum_flip_distance": 1.00,  # ETH: $1+ wrong side to flip (proportional to BTC $30)
        "conviction_min_distance": 2.00, # ETH: $2+ past strike to add
        "conviction_min_cycle_sec": 180,
        "conviction_max_adds": 2,
        "conviction_cooldown_sec": 60,
        "conviction_max_price": 72,
        "take_profit_c": 95,
        "reentry_max_price": 59,
        "entry_max": 55,
        "entry_contracts": 2,
        "momentum_window": 15,
        "ws_pairs": {"coinbase": "ETH-USD", "kraken": "ETH/USD", "bitstamp": "ethusd", "gemini": "ETHUSD"},
        # Probability model — calibrated from 52 observed 15m cycles (Apr 2026)
        # σ_15m=$2.14 => σ_per_sec=$2.14/sqrt(900)=$0.071/sec
        "volatility_per_sec": 0.071,
        "flip_probability_threshold": FLIP_PROBABILITY_THRESHOLD,
    },
    "SOL": {
        "series": "KXSOL15M",
        "flip_cooldown_sec": 90,
        "trailing_stop_c": 5,
        "trailing_stop_far_c": 15,
        "trailing_stop_mid_c": 10,
        "trailing_stop_near_c": 5,
        "trailing_stop_far_dist": 1.50,  # SOL: $1.50+ from strike = far zone
        "trailing_stop_mid_dist": 0.50,  # SOL: $0.50-1.50 from strike = mid zone
        "stop_loss_hard_c": 25,
        "momentum_flip_distance": 0.30,  # SOL: $0.30+ wrong side (proportional to BTC $30 at ~$150 price)
        "conviction_min_distance": 0.75, # SOL: $0.75 past strike to add (~0.5σ_15m)
        "conviction_min_cycle_sec": 180,
        "conviction_max_adds": 2,
        "conviction_cooldown_sec": 60,
        "conviction_max_price": 72,
        "take_profit_c": 95,
        "reentry_max_price": 59,
        "entry_max": 49,
        "entry_contracts": 1,
        "momentum_window": 15,
        "ws_pairs": {"coinbase": "SOL-USD", "kraken": "SOL/USD", "bitstamp": "solusd", "gemini": "SOLUSD"},
        # Probability model — SOL ~$150, ~1.5-2x BTC/ETH percentage volatility
        # σ_15m≈$1.50 => σ_per_sec=$1.50/sqrt(900)=$0.050/sec (conservative — errs toward less-flip)
        "volatility_per_sec": 0.050,
        "flip_probability_threshold": FLIP_PROBABILITY_THRESHOLD,
    },
    "XRP": {
        "series": "KXXRP15M",
        "flip_cooldown_sec": 90,
        "trailing_stop_c": 5,
        "trailing_stop_far_c": 15,
        "trailing_stop_mid_c": 10,
        "trailing_stop_near_c": 5,
        "trailing_stop_far_dist": 0.05,  # XRP: $0.05+ from strike = far zone (2% at $2.50)
        "trailing_stop_mid_dist": 0.02,  # XRP: $0.02-0.05 from strike = mid zone (0.8% at $2.50)
        "stop_loss_hard_c": 25,
        "momentum_flip_distance": 0.02,  # XRP: $0.02+ wrong side (0.8% — proportional to BTC)
        "conviction_min_distance": 0.05, # XRP: $0.05 past strike to add (2% at $2.50)
        "conviction_min_cycle_sec": 180,
        "conviction_max_adds": 2,
        "conviction_cooldown_sec": 60,
        "conviction_max_price": 72,
        "take_profit_c": 95,
        "reentry_max_price": 59,
        "entry_max": 49,
        "entry_contracts": 1,
        "momentum_window": 15,
        "ws_pairs": {"coinbase": "XRP-USD", "kraken": "XRP/USD", "bitstamp": "xrpusd", "gemini": "XRPUSD"},
        # Probability model — XRP ~$2.50, higher percentage volatility than BTC/ETH
        # σ_15m≈$0.09 => σ_per_sec=$0.09/sqrt(900)=$0.003/sec (conservative — errs toward less-flip)
        "volatility_per_sec": 0.003,
        "flip_probability_threshold": FLIP_PROBABILITY_THRESHOLD,
    },
}

# Default constants — used as fallbacks in cfg.get() when a key is missing from BRTI_COIN_CONFIG
BRTI_FLIP_COOLDOWN_SEC = 90
BRTI_TRAILING_STOP_C = 5
BRTI_STOP_LOSS_HARD_C = 20
BRTI_CONVICTION_MIN_DISTANCE = 50
BRTI_CONVICTION_MIN_CYCLE_SEC = 180
BRTI_CONVICTION_MAX_ADDS = 2
BRTI_CONVICTION_COOLDOWN_SEC = 60
BRTI_CONVICTION_MAX_PRICE = 85
BRTI_TAKE_PROFIT_C = 95
BRTI_REENTRY_MAX_PRICE = 79
BRTI_ENTRY_MAX = 79
BRTI_MOMENTUM_WINDOW = 15

# Synthetic BRTI — real-time feed from constituent exchange WebSockets
# Volume-weighted median of Coinbase, Kraken, Bitstamp, Gemini (~80%+ of BRTI weight)
# Built dynamically for all coins in BRTI_COIN_CONFIG
def _build_brti_exchanges():
    """Build per-exchange WS config that subscribes to ALL BRTI coins' pairs."""
    all_coinbase_pairs = [cfg["ws_pairs"]["coinbase"] for cfg in BRTI_COIN_CONFIG.values()]
    all_kraken_pairs = [cfg["ws_pairs"]["kraken"] for cfg in BRTI_COIN_CONFIG.values()]
    # Bitstamp needs one subscription per channel, handled in the WS handler
    # Gemini needs one WS connection per symbol, handled in the WS handler
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
            # Subscribe to all coins — sent as separate messages in _bitstamp_ws
            "channels": [cfg["ws_pairs"]["bitstamp"] for cfg in BRTI_COIN_CONFIG.values()],
        },
        "gemini": {
            # Gemini needs separate WS connections per symbol — handled in _gemini_ws
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
MIN_TICKER_LIFE_SEC = 60   # only subscribe to markets with >60s until close (avoid about-to-settle)

# COINS maps coin name -> Kalshi series ticker (used for settlement history + market lookups)
COINS = {coin: cfg["series"] for coin, cfg in BRTI_COIN_CONFIG.items()}

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
        # Kalshi-authoritative position count (from post_position_fp in fill messages)
        self.kalshi_positions: Dict[str, int] = {}

        # Per-ticker: held contracts and resting sell orders
        self.positions: Dict[str, list] = defaultdict(list)
        self.resting_sells: Dict[str, int] = {}  # ticker -> count of resting sell orders

        # Resting buy orders — drift with market
        # {ticker: {order_id, side, price, coin}}
        self.resting_buys: Dict[str, dict] = {}

        # Cooldown tracking: ticker -> last buy fill timestamp (ms)
        self.last_buy_ts: Dict[str, int] = {}
        # Last fill price per ticker (for averaging-down enforcement)
        self.last_buy_price: Dict[str, int] = {}

        # Current open market tickers per coin
        self.current_tickers: Dict[str, str] = {}
        # Per-ticker timestamp of last subscribe refresh (ms) — for rollover guard
        self.ticker_refreshed_ts: Dict[str, int] = {}

        # Kalshi WS ticker cache: ticker -> {yes_bid, yes_ask, ...}
        self.ws_prices: Dict[str, dict] = {}
        self.ws_connected = False

        # BRTI state per coin
        # Each coin gets its own state dict with: ticks, strike, direction, entry_made,
        # held_side, last_flip_ts, entry_price, peak_value, conviction_adds, last_conviction_ts
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
                "last_conviction_ts": 0.0, # last conviction buy timestamp
                "flip_confirm_ticks": 0,  # sustained flip signal counter (need 4 = ~2s)
                "last_tp_ts": 0.0,        # timestamp of last take profit (re-entry cooldown)
            }

        # Exchange price feeds for synthetic index — keyed by (exchange, coin)
        self.exchange_prices: Dict[str, Dict[str, float]] = defaultdict(dict)   # exchange -> {coin -> price}
        self.exchange_trades: Dict[str, Dict[str, list]] = defaultdict(lambda: defaultdict(list))  # exchange -> {coin -> [(ts, price, vol)]}
        self.exchange_status: Dict[str, str] = {}                # exchange -> status

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
        rotated = False
        now_ms = int(datetime.now().timestamp() * 1000)
        # Only accept markets whose close_ts is far enough in the future to actually trade
        min_close_ts = int(datetime.now().timestamp()) + MIN_TICKER_LIFE_SEC
        for coin, series in COINS.items():
            try:
                data = self.client.get_markets(series_ticker=series, status="open", limit=1)
                for m in data.get("markets", []):
                    ticker = m.get("ticker")
                    if not ticker:
                        continue
                    # Kalshi returns close_time as ISO string ("2026-04-08T20:15:00Z")
                    close_time_str = m.get("close_time", "")
                    try:
                        close_dt = datetime.fromisoformat(close_time_str.replace("Z", "+00:00"))
                        close_ts = int(close_dt.timestamp())
                    except (ValueError, AttributeError):
                        close_ts = 0
                    if close_ts and close_ts <= min_close_ts:
                        continue  # market about to settle — skip
                    if self.current_tickers.get(coin) != ticker:
                        rotated = True
                        # Drop stale price cache for the old ticker
                        old = self.current_tickers.get(coin)
                        if old:
                            self.ws_prices.pop(old, None)
                    self.current_tickers[coin] = ticker
                    self.ticker_refreshed_ts[ticker] = now_ms
                    tickers.append(ticker)
                    # Capture strike for BRTI strategy
                    if coin in BRTI_COIN_CONFIG:
                        strike = m.get("floor_strike")
                        if strike:
                            self.brti_state[coin]["strike"] = float(strike)
                            print(f"  📍 {coin} strike: ${self.brti_state[coin]['strike']:,.2f}")
            except Exception as e:
                print(f"  Open market lookup error {coin}: {e}")

        # Reset cooldowns and avg-down state on cycle rotation
        if rotated:
            self.last_buy_ts.clear()
            self.last_buy_price.clear()
            # Reset BRTI cycle state for all coins
            for _coin in BRTI_COIN_CONFIG:
                st = self.brti_state[_coin]
                # Zero out the outgoing ticker's contract count — prevents phantom
                # accumulation when a buy 404s at cycle boundary (pre-increment is
                # never rolled back, so stale counts survive into the new cycle and
                # push total_exposure >= 10, blocking all new entries)
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
                st["flip_confirm_ticks"] = 0
                print(f"  🔄 {_coin} BRTI cycle reset (strike: ${st['strike']:,.2f})")

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
            # Try multiple field names — Kalshi WS field names vary
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

            # Convert raw yes_price into side-relative cost (what we actually paid/received)
            purchased_side = (msg.get("purchased_side") or side or "").lower()
            if action == "sell":
                # For sell fills use the order side — purchased_side can refer to the counterparty
                cost_price = price if side.lower() == "yes" else (100 - price)
            else:
                cost_price = price if purchased_side == "yes" else (100 - price)

            print(f"  💰 FILL: {action.upper()} {side.upper()} {coin or ticker} {count}x @ {cost_price}c (yes_px={price}c) | raw={msg}")

            if action == "buy":
                self.ticker_contracts[ticker] = self.ticker_contracts.get(ticker, 0) + count
                self.positions[ticker].append({"side": side, "entry_price": cost_price, "count": count})
                if ticker in self.resting_buys:
                    del self.resting_buys[ticker]
                # Start cooldown + track fill price (side-relative) for averaging-down enforcement
                self.last_buy_ts[ticker] = int(datetime.now().timestamp() * 1000)
                self.last_buy_price[ticker] = cost_price
                print(f"  ✅ Bought! Now {self.ticker_contracts[ticker]} contracts @ {cost_price}c (HOLD to settlement)")
                # Reconcile with Kalshi's authoritative position count
                post_pos = msg.get("post_position_fp")
                if post_pos is not None:
                    actual_count = abs(int(round(float(post_pos))))
                    tracked_count = self.ticker_contracts.get(ticker, 0)
                    if actual_count != tracked_count:
                        print(f"  ⚠️ POSITION SYNC: {ticker} tracked={tracked_count} kalshi={actual_count} — correcting")
                        self.ticker_contracts[ticker] = actual_count
                    self.kalshi_positions[ticker] = actual_count

            elif action == "sell":
                self.resting_sells[ticker] = max(0, self.resting_sells.get(ticker, 0) - count)
                if self.positions.get(ticker):
                    for _ in range(min(count, len(self.positions[ticker]))):
                        self.positions[ticker].pop(0)
                self.ticker_contracts[ticker] = max(0, self.ticker_contracts.get(ticker, 0) - count)
                print(f"  ✅ Exit filled! Held: {self.ticker_contracts[ticker]} | Resting sells: {self.resting_sells.get(ticker, 0)}")
                # Reconcile with Kalshi's authoritative position count
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

    async def _evaluate_trade(self, ticker):
        """Evaluate BRTI momentum trade on each price update."""
        await self._evaluate_brti(ticker)

    async def _evaluate_brti(self, ticker):
        """BRTI momentum strategy: all coins in BRTI_COIN_CONFIG.
        1. At cycle start (after 15s), buy the side momentum suggests, at ≤entry_max c
        2. Throughout cycle, monitor index vs strike — flip sell if position going unprofitable
        """
        coin = None
        for c, t in self.current_tickers.items():
            if t == ticker:
                coin = c
                break
        if not coin or coin not in BRTI_COIN_CONFIG:
            return
        st = self.brti_state[coin]
        cfg = BRTI_COIN_CONFIG[coin]
        if st["strike"] <= 0 or not st["ticks"]:
            return

        now = datetime.now()
        cycle_sec = (now.minute % 15) * 60 + now.second

        # Rollover guard
        if cycle_sec < ROLLOVER_GUARD_SEC:
            return
        now_ms = int(now.timestamp() * 1000)
        refreshed_ms = self.ticker_refreshed_ts.get(ticker, 0)
        if now_ms - refreshed_ms > 60_000:
            return

        prices = self.ws_prices.get(ticker)
        if not prices:
            return
        yes_ask = prices.get("yes_ask", 0)
        yes_bid = prices.get("yes_bid", 0)
        if yes_ask <= 0 or yes_bid <= 0:
            return

        latest_brti = st["ticks"][-1][1] if st["ticks"] else 0
        if latest_brti <= 0:
            return

        momentum_window = cfg.get("momentum_window", BRTI_MOMENTUM_WINDOW)

        # ── Phase 1: Determine direction ──
        # Priority: position relative to strike > momentum
        # If sBRTI is clearly on one side of strike, that's the direction
        # regardless of tiny momentum noise. Only use momentum when near strike.
        if not st["direction"] and cycle_sec >= momentum_window and len(st["ticks"]) >= 10:
            recent_ticks = [v for _, v in st["ticks"][-10:]]
            if recent_ticks:
                recent_avg = sum(recent_ticks) / len(recent_ticks)
                distance_from_strike = recent_avg - st["strike"]
                min_clear_dist = cfg.get("trailing_stop_mid_dist", 20)  # $20 BTC / $0.60 ETH

                if abs(distance_from_strike) >= min_clear_dist:
                    # sBRTI is clearly on one side — use position, not momentum
                    st["direction"] = "up" if distance_from_strike > 0 else "down"
                    print(f"[{now.strftime('%H:%M:%S')}] {coin} direction (position): {st['direction']} (index ${recent_avg:,.2f} is ${distance_from_strike:+,.2f} from strike ${st['strike']:,.2f})")
                else:
                    # sBRTI near strike — use momentum to break the tie
                    cycle_start_ts = now.timestamp() - cycle_sec
                    start_ticks = [v for t, v in st["ticks"] if cycle_start_ts - 5 <= t <= cycle_start_ts + 10]
                    if start_ticks:
                        start_avg = sum(start_ticks) / len(start_ticks)
                        delta = recent_avg - start_avg
                        # Require meaningful momentum (at least $5 BTC / $0.15 ETH)
                        min_momentum = min_clear_dist * 0.25
                        if delta > min_momentum:
                            st["direction"] = "up"
                        elif delta < -min_momentum:
                            st["direction"] = "down"
                        else:
                            st["direction"] = "flat"  # too close to call
                        print(f"[{now.strftime('%H:%M:%S')}] {coin} direction (momentum): {st['direction']} (${start_avg:,.2f} → ${recent_avg:,.2f}, Δ${delta:+,.2f}, need ±${min_momentum:,.2f}) | strike: ${st['strike']:,.2f}")
                    else:
                        # No start data — use position vs strike
                        st["direction"] = "up" if distance_from_strike > 0 else "down"
                        print(f"[{now.strftime('%H:%M:%S')}] {coin} direction (vs strike): {st['direction']} (index ${recent_avg:,.2f} vs strike ${st['strike']:,.2f})")

        # ── Phase 2: Initial entry ──
        if st["direction"] and not st["entry_made"] and st["direction"] != "flat" and cycle_sec >= 120:
            target_side = "yes" if st["direction"] == "up" else "no"

            # Late-cycle guard: in final 2 min, only enter on regime change
            secs_remaining = max(1, 900 - cycle_sec)
            late_cycle_min_distance = cfg["conviction_min_distance"]
            if secs_remaining <= 120:
                smooth_ticks = [v for t, v in st["ticks"] if t > now.timestamp() - BRTI_SMOOTHING_WINDOW]
                if smooth_ticks:
                    smoothed = sum(smooth_ticks) / len(smooth_ticks)
                    distance_past_strike = smoothed - st["strike"] if target_side == "yes" else st["strike"] - smoothed
                    if distance_past_strike < late_cycle_min_distance:
                        return
                else:
                    return

            # Cost to buy
            if target_side == "yes":
                cost = yes_ask
            else:
                cost = 100 - yes_bid
            if cost <= 0 or cost > 100:
                try:
                    md = self.client.get_markets(series_ticker=cfg["series"], status="open", limit=1) if self.client else {}
                    mkt = md.get("markets", [{}])[0]
                    if target_side == "yes":
                        cost = int(float(mkt.get("yes_ask_dollars", "0")) * 100)
                    else:
                        yb = int(float(mkt.get("yes_bid_dollars", "0")) * 100)
                        cost = 100 - yb if yb > 0 else 0
                except Exception:
                    pass
            entry_max = cfg.get("entry_max", 49)
            reentry_max = cfg.get("reentry_max_price", entry_max)
            is_reentry = st["conviction_adds"] > 0 or st["peak_value"] > 0
            max_price = reentry_max if is_reentry else entry_max
            # Global exposure guard: max 10 contracts across current-cycle tickers only
            total_exposure = sum(self.ticker_contracts.get(t, 0) for t in self.current_tickers.values())
            if total_exposure >= 15:
                return

            if 10 <= cost <= max_price and time.time() - st.get("last_tp_ts", 0) > 90:
                st["entry_made"] = True
                st["held_side"] = target_side
                st["entry_price"] = cost
                st["peak_value"] = cost
                print(f"[{now.strftime('%H:%M:%S')}] {coin} BRTI-ENTRY {target_side.upper()} 1 @ {cost}c (dir {st['direction']}, strike ${st['strike']:,.2f})")
                entry_count = cfg.get("entry_contracts", 1)
                self._post_buy(ticker, coin, target_side, cost, target_contracts=entry_count, count=entry_count)
                return

        # Phase 3 handled by brti_fast_flip_loop (500ms, trailing stop + stop loss)

    def estimate_settlement_probability(
        self,
        coin: str,
        distance_from_strike: float,  # sBRTI - strike (signed: + = above, - = below)
        secs_remaining: float,
        momentum: float = 0.0,         # (10s_avg - 30s_avg) in raw price units
    ) -> float:
        """
        Returns P(settlement ABOVE strike) using a normal distribution model.

        The CF Benchmark BRTI settles as the 1-minute VWAP of trades in seconds
        840-900 of each cycle. This smoothing reduces effective settlement variance
        vs spot by ~sqrt(1/3). We use SETTLEMENT_SMOOTHING_FACTOR = 0.55.

        Model:
            projected_mean  = distance_from_strike + momentum_adjustment
            momentum_adj    = momentum * min(60, T) * 0.015
            effective_sigma = σ_per_sec * sqrt(T) * smoothing_factor

        For T < 60s (inside the settlement window), variance further reduced:
            effective_sigma = σ_per_sec * sqrt(T/3) * smoothing_factor * 0.8

        P(above strike) = Φ(projected_mean / effective_sigma)
        where Φ = standard normal CDF.

        To get P(held side wins):
            P(YES wins) = P(above strike)
            P(NO wins)  = 1 - P(above strike)
        """
        cfg = BRTI_COIN_CONFIG.get(coin, {})
        sigma_per_sec = cfg.get("volatility_per_sec", 2.20 if coin == "BTC" else 0.071)
        flip_smoothing = SETTLEMENT_SMOOTHING_FACTOR

        T = max(1.0, float(secs_remaining))

        # Momentum adjustment: current short-term trend, heavily damped
        # min(60, T) prevents over-extrapolating momentum far into the future
        momentum_adj = momentum * min(60.0, T) * 0.015

        projected_mean = distance_from_strike + momentum_adj

        if T >= 60:
            # Standard: settlement is still > 1 min away, full random walk model
            effective_sigma = sigma_per_sec * math.sqrt(T) * flip_smoothing
        else:
            # Inside the settlement window: price averaging has started.
            # Remaining uncertainty is only for the not-yet-averaged fraction.
            effective_sigma = sigma_per_sec * math.sqrt(T / 3.0) * flip_smoothing * 0.8

        if effective_sigma <= 0:
            return 1.0 if projected_mean > 0 else (0.0 if projected_mean < 0 else 0.5)

        # Φ(z) using math.erf: Φ(z) = 0.5 * (1 + erf(z / sqrt(2)))
        z = projected_mean / (effective_sigma * math.sqrt(2))
        return 0.5 * (1.0 + math.erf(z))

    def _post_buy(self, ticker, coin, side, price, target_contracts, count=1):
        """Post a limit buy order. count=number of contracts in this single order."""
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
                # Roll back the pre-increment so a failed order doesn't phantom-accumulate
                # ticker_contracts and push total_exposure >= 10 (which blocks all entries)
                self.ticker_contracts[ticker] = max(0, self.ticker_contracts.get(ticker, 0) - count)
        elif DRY_RUN:
            self.resting_buys[ticker] = {"order_id": client_order_id, "side": side, "price": price, "coin": coin}
            print(f"  📄 PAPER: → buy {side} @ {price}c (HOLD to settlement)")

        with open(self.log_file, "a", newline="") as f:
            csv.writer(f).writerow([
                datetime.now().strftime("%H:%M:%S"), coin, ticker,
                "—", "—", decision, price, "brti", "RESTING"
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
                    # Keep last 20 for reference
                    if len(self.settlement_history[coin_name]) > 20:
                        self.settlement_history[coin_name] = self.settlement_history[coin_name][-20:]
                    self.last_settled_ticker[coin_name] = latest_ticker
                    hist = self.settlement_history[coin_name][-10:]
                    yes_ct = sum(1 for r in hist if r == "yes")
                    print(f"  📊 {coin_name} settled {result.upper()} → {yes_ct}Y/{10 - yes_ct}N (last 10)")
        except Exception:
            pass

    # ─── Fast BRTI Flip Check Loop ─────────────────────────────

    async def brti_fast_flip_loop(self):
        """Every 500ms: check entry (if no position) + trailing stop + flip conditions."""
        while self.running:
            for coin, cfg in BRTI_COIN_CONFIG.items():
                try:
                    st = self.brti_state[coin]
                    if st["strike"] <= 0 or not st["ticks"]:
                        continue
                    coin_ticker = self.current_tickers.get(coin, "")
                    if not coin_ticker:
                        continue

                    # ── Entry check (when Kalshi WS is quiet) ──
                    if st["direction"] and not st["entry_made"] and st["direction"] != "flat" and not st["held_side"]:
                        # Fetch prices via API since WS may not be sending ticks
                        prices = self.ws_prices.get(coin_ticker, {})
                        yes_ask = prices.get("yes_ask", 0)
                        yes_bid = prices.get("yes_bid", 0)
                        if yes_ask <= 0 or yes_bid <= 0:
                            try:
                                series = cfg.get("series", "")
                                md = self.client.get_markets(series_ticker=series, status="open", limit=1) if self.client else {}
                                mkt = md.get("markets", [{}])[0]
                                yes_ask = int(float(mkt.get("yes_ask_dollars", "0")) * 100)
                                yes_bid = int(float(mkt.get("yes_bid_dollars", "0")) * 100)
                            except Exception:
                                pass
                        if yes_ask > 0 and yes_bid > 0:
                            target_side = "yes" if st["direction"] == "up" else "no"
                            cost = yes_ask if target_side == "yes" else (100 - yes_bid)
                            entry_max = cfg.get("entry_max", 79)
                            now = datetime.now()
                            cycle_sec = (now.minute % 15) * 60 + now.second
                            secs_remaining = max(1, 900 - cycle_sec)
                            # Late-cycle guard
                            if secs_remaining <= 120:
                                smooth_ticks = [v for t, v in st["ticks"] if t > now.timestamp() - BRTI_SMOOTHING_WINDOW]
                                if smooth_ticks:
                                    smoothed = sum(smooth_ticks) / len(smooth_ticks)
                                    dist_past = smoothed - st["strike"] if target_side == "yes" else st["strike"] - smoothed
                                    if dist_past < cfg.get("conviction_min_distance", 50):
                                        continue
                                else:
                                    continue
                            # Global exposure guard: current-cycle tickers only
                            total_exposure = sum(self.ticker_contracts.get(t, 0) for t in self.current_tickers.values())
                            if total_exposure >= 15:
                                continue
                            entry_count = cfg.get("entry_contracts", 1)
                            if cycle_sec >= 120 and 10 <= cost <= entry_max and time.time() - st.get("last_tp_ts", 0) > 90:
                                st["entry_made"] = True
                                st["held_side"] = target_side
                                st["entry_price"] = cost
                                st["peak_value"] = cost
                                print(f"[{now.strftime('%H:%M:%S')}] {coin} BRTI-ENTRY(fast) {target_side.upper()} {entry_count}x @ {cost}c (dir {st['direction']}, strike ${st['strike']:,.2f})")
                                self._post_buy(coin_ticker, coin, target_side, cost, target_contracts=entry_count, count=entry_count)
                        continue  # done with entry check for this coin

                    # ── Flip/stop checks (only when holding) ──
                    if not st["held_side"]:
                        continue
                    held = self.ticker_contracts.get(coin_ticker, 0)
                    flip_cooldown = cfg.get("flip_cooldown_sec", BRTI_FLIP_COOLDOWN_SEC)
                    if held <= 0 or (time.time() - st["last_flip_ts"]) <= flip_cooldown:
                        continue
                    # Get current position value from Kalshi WS prices
                    prices = self.ws_prices.get(coin_ticker, {})
                    yes_bid = prices.get("yes_bid", 0)
                    yes_ask = prices.get("yes_ask", 0)
                    if yes_bid <= 0 or yes_ask <= 0:
                        continue

                    # Current value of our position (what we'd get if we sold now)
                    if st["held_side"] == "yes":
                        current_value = yes_bid  # sell YES at bid
                    else:
                        current_value = 100 - yes_ask  # sell NO at 100-ask

                    # Update peak (high water mark)
                    if current_value > st["peak_value"]:
                        st["peak_value"] = current_value

                    take_profit_c = cfg.get("take_profit_c", BRTI_TAKE_PROFIT_C)
                    # stop_loss replaced by three-tier: momentum flip + hard stop + do nothing

                    # TRAILING STOP DISABLED (2026-04-11) — relying on prob model flip + TP + hard stop only
                    if False:  # zone-based trailing_stop_c calculation — disabled with trailing stop
                        now_ts = time.time()
                        smooth_for_zone = [v for t, v in st["ticks"] if t > now_ts - 30]
                        if smooth_for_zone:
                            smoothed_for_zone = sum(smooth_for_zone) / len(smooth_for_zone)
                        else:
                            smoothed_for_zone = st["ticks"][-1][1] if st["ticks"] else 0
                        abs_distance = abs(smoothed_for_zone - st["strike"]) if smoothed_for_zone and st["strike"] else 0
                        far_dist = cfg.get("trailing_stop_far_dist", 50)
                        mid_dist = cfg.get("trailing_stop_mid_dist", 20)
                        if abs_distance >= far_dist:
                            trailing_stop_c = cfg.get("trailing_stop_far_c", 15)
                        elif abs_distance >= mid_dist:
                            trailing_stop_c = cfg.get("trailing_stop_mid_c", 10)
                        else:
                            trailing_stop_c = cfg.get("trailing_stop_near_c", 5)
                    # Safe sell count: only sell what we actually bought this cycle (entry + conviction adds)
                    safe_sell_count = held  # sell exactly what we hold — no more, no less
                    conviction_max_adds = cfg.get("conviction_max_adds", BRTI_CONVICTION_MAX_ADDS)
                    conviction_min_cycle_sec = cfg.get("conviction_min_cycle_sec", BRTI_CONVICTION_MIN_CYCLE_SEC)
                    conviction_cooldown_sec = cfg.get("conviction_cooldown_sec", BRTI_CONVICTION_COOLDOWN_SEC)
                    conviction_min_distance = cfg.get("conviction_min_distance", BRTI_CONVICTION_MIN_DISTANCE)
                    conviction_max_price = cfg.get("conviction_max_price", BRTI_CONVICTION_MAX_PRICE)
                    entry_max = cfg.get("entry_max", BRTI_ENTRY_MAX)

                    # ── Take profit: exit at take_profit_c+ — lock in the win ──
                    if current_value >= take_profit_c:
                        old_side = st["held_side"]
                        profit = current_value - st["entry_price"]
                        # Cap sell count: entry_contracts + conviction_adds — don't oversell
                        entry_contracts = cfg.get("entry_contracts", 1)
                        safe_sell_count = max(1, min(held, entry_contracts + st.get("conviction_adds", 0)))
                        # SELL GUARD: cap against Kalshi's authoritative position to prevent oversell
                        _kal = self.kalshi_positions.get(coin_ticker)
                        if _kal is not None:
                            if safe_sell_count > _kal:
                                print(f"  ⚠️ SELL GUARD: {coin} TP capping {safe_sell_count}x → {_kal}x (kalshi holds {_kal})")
                                safe_sell_count = _kal
                            if safe_sell_count <= 0:
                                print(f"  ⚠️ SELL GUARD: {coin} kalshi shows 0 held — skipping TP")
                                continue
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] 💰 {coin} TAKE PROFIT: {old_side.upper()} {safe_sell_count}x @ {current_value}c (entry:{st['entry_price']}c pnl:+{profit}c)")
                        if self.client and not DRY_RUN:
                            # Reset state BEFORE order — prevents infinite storm if create_order throws
                            self.ticker_contracts[coin_ticker] = 0
                            self.positions[coin_ticker] = []
                            st["held_side"] = ""
                            st["entry_made"] = False
                            st["peak_value"] = 0
                            st["entry_price"] = 0
                            st["conviction_adds"] = 0
                            st["direction"] = ""      # stale direction causes wrong re-entry at extreme prices
                            st["last_tp_ts"] = time.time()  # cooldown: block re-entry for 90s post-TP
                            try:
                                tp_id = f"tp-{coin_ticker}-{int(time.time()*1000)}"
                                self.client.create_order(
                                    ticker=coin_ticker, client_order_id=tp_id,
                                    side=old_side, action="sell", count=safe_sell_count, type="limit",
                                    yes_price=yes_bid if old_side == "yes" else None,
                                    no_price=(100 - yes_ask) if old_side == "no" else None,
                                )
                                print(f"  → {coin} Sold {held}x @ {current_value}c — direction reset, 90s re-entry cooldown")
                            except Exception as e:
                                print(f"  ⚠️ {coin} TP order failed — state cleared, LEAKED {safe_sell_count}x {old_side}. Error: {e}")
                        continue

                    latest_brti = st["ticks"][-1][1]
                    distance = latest_brti - st["strike"]

                    # ── Conviction buy: add when projected settlement is locked ──
                    cycle_now = datetime.now()
                    cycle_sec = (cycle_now.minute % 15) * 60 + cycle_now.second
                    total_exposure = sum(self.ticker_contracts.get(t, 0) for t in self.current_tickers.values())
                    if (st["conviction_adds"] < conviction_max_adds
                            and cycle_sec >= conviction_min_cycle_sec
                            and (time.time() - st["last_conviction_ts"]) > conviction_cooldown_sec
                            and total_exposure < 15):
                        now_ts = time.time()
                        smooth_ticks = [v for t, v in st["ticks"] if t > now_ts - BRTI_SMOOTHING_WINDOW]
                        if smooth_ticks:
                            smoothed = sum(smooth_ticks) / len(smooth_ticks)
                            if st["held_side"] == "yes":
                                proj_distance = smoothed - st["strike"]
                            else:
                                proj_distance = st["strike"] - smoothed
                            buy_price = yes_ask if st["held_side"] == "yes" else (100 - yes_bid)
                            if proj_distance >= conviction_min_distance and buy_price <= conviction_max_price:
                                st["conviction_adds"] += 1
                                st["last_conviction_ts"] = time.time()
                                print(f"[{cycle_now.strftime('%H:%M:%S')}] 💪 {coin} CONVICTION BUY: {st['held_side'].upper()} 1x @ {buy_price}c (smoothed ${proj_distance:+,.0f} from strike, add {st['conviction_adds']}/{conviction_max_adds})")
                                if self.client and not DRY_RUN:
                                    try:
                                        conv_id = f"conv-{coin_ticker}-{int(time.time()*1000)}"
                                        self.client.create_order(
                                            ticker=coin_ticker, client_order_id=conv_id,
                                            side=st["held_side"], action="buy", count=1, type="limit",
                                            yes_price=yes_ask if st["held_side"] == "yes" else None,
                                            no_price=(100 - yes_bid) if st["held_side"] == "no" else None,
                                        )
                                    except Exception as e:
                                        st["conviction_adds"] -= 1  # rollback — order didn't go through
                                        print(f"  → {coin} Conviction buy error (rolled back): {e}")

                    # ── Check trailing stop / stop loss ──
                    should_flip = False
                    new_side = ""
                    reason = ""
                    loss_from_entry = st["entry_price"] - current_value

                    # ── Compute projected settlement value ──
                    now_ts = time.time()
                    smooth_ticks = [v for t, v in st["ticks"] if t > now_ts - BRTI_SMOOTHING_WINDOW]
                    smoothed_brti = sum(smooth_ticks) / len(smooth_ticks) if smooth_ticks else latest_brti

                    ticks_30s = [v for t, v in st["ticks"] if t > now_ts - 30]
                    ticks_10s = [v for t, v in st["ticks"] if t > now_ts - 10]
                    if len(ticks_30s) >= 2 and len(ticks_10s) >= 1:
                        momentum = (sum(ticks_10s) / len(ticks_10s)) - (sum(ticks_30s) / len(ticks_30s))
                    else:
                        momentum = 0

                    cycle_now = datetime.now()
                    cycle_sec = (cycle_now.minute % 15) * 60 + cycle_now.second
                    secs_remaining = max(1, 900 - cycle_sec)

                    momentum_per_sec = momentum / 20
                    dampen = min(1.0, 60 / secs_remaining)
                    projected_settlement = smoothed_brti + (momentum_per_sec * secs_remaining * dampen * 0.3)

                    if st["held_side"] == "yes":
                        projected_winning = projected_settlement >= st["strike"]
                    else:
                        projected_winning = projected_settlement < st["strike"]

                    if False:  # TRAILING STOP DISABLED (2026-04-11) — was: if was_profitable; prob model flip + TP + hard stop handle all exits
                        # Only activate trailing stop once we have +8c locked in
                        # Prevents selling on tiny peaks that barely exceeded entry
                        if drop_from_peak >= trailing_stop_c and current_value >= st["entry_price"] + 8:
                            should_flip = True
                            new_side = "no" if st["held_side"] == "yes" else "yes"
                            profit = current_value - st["entry_price"]
                            reason = f"TRAILING STOP (entry:{st['entry_price']}c peak:{st['peak_value']}c now:{current_value}c pnl:{profit:+d}c)"
                            if projected_winning:
                                should_flip = False
                                if drop_from_peak >= trailing_stop_c:
                                    # Prob model gate: if P(held_side_wins) is still high,
                                    # Kalshi price pullback is noise — hold for TP instead.
                                    _prot_dist = smoothed_brti - st["strike"]
                                    _prot_p_above = self.estimate_settlement_probability(
                                        coin, _prot_dist, secs_remaining, momentum
                                    )
                                    _prot_p_held = _prot_p_above if st["held_side"] == "yes" else (1.0 - _prot_p_above)
                                    _prot_gate = cfg.get("protect_profit_prob_gate", 0.70)
                                    if _prot_p_held >= _prot_gate:
                                        # P(win) still high — suppress protect profit, let TP fire
                                        print(
                                            f"[{datetime.now().strftime('%H:%M:%S')}] {coin} protect profit suppressed: "
                                            f"P({st['held_side']}_wins)={_prot_p_held:.1%} >= {_prot_gate:.0%} gate "
                                            f"(dist={_prot_dist:+.0f}, T={secs_remaining:.0f}s)"
                                        )
                                    else:
                                        old_side = st["held_side"]
                                        sell_price = current_value
                                        pnl_val = sell_price - st["entry_price"]
                                        print(f"[{datetime.now().strftime('%H:%M:%S')}] 📉 {coin} PROTECT PROFIT: SELL {old_side.upper()} @ {sell_price}c (peak:{st['peak_value']}c pnl:{pnl_val:+d}c P(win)={_prot_p_held:.0%}) — flat, watching")
                                        # ALWAYS reset state — prevents storm in paper mode too
                                        self.ticker_contracts[coin_ticker] = 0
                                        self.positions[coin_ticker] = []
                                        st["held_side"] = ""
                                        st["entry_made"] = False
                                        st["peak_value"] = 0
                                        st["entry_price"] = 0
                                        st["last_flip_ts"] = time.time()
                                        st["direction"] = ""  # re-evaluate before re-entry
                                        if self.client and not DRY_RUN:
                                            try:
                                                tp_id = f"prot-{coin_ticker}-{int(time.time()*1000)}"
                                                _pp_count = safe_sell_count
                                                _kal = self.kalshi_positions.get(coin_ticker)
                                                if _kal is not None and _pp_count > _kal:
                                                    print(f"  ⚠️ SELL GUARD: {coin} prot capping {_pp_count}x → {_kal}x")
                                                    _pp_count = _kal
                                                if _pp_count > 0:
                                                    self.client.create_order(
                                                        ticker=coin_ticker, client_order_id=tp_id,
                                                        side=old_side, action="sell", count=_pp_count, type="limit",
                                                        yes_price=yes_bid if old_side == "yes" else None,
                                                        no_price=(100 - yes_ask) if old_side == "no" else None,
                                                    )
                                                else:
                                                    print(f"  ⚠️ SELL GUARD: {coin} skipping prot sell — kalshi shows 0 held")
                                            except Exception as e:
                                                print(f"  → {coin} Protect profit error (state already cleared): {e}")
                                        else:
                                            print(f"  📄 PAPER: {coin} protect profit — flat, direction reset")
                    else:
                        # ── SCENARIO 2: Never profitable — two tiers ──
                        hard_stop = cfg.get("stop_loss_hard_c", 20)

                        # Tier A: PROBABILITY MODEL FLIP
                        # Replaces the old fixed $30 BTC / $1.00 ETH threshold.
                        # Computes P(held side settles profitably) from:
                        #   - signed distance of sBRTI from strike
                        #   - seconds remaining in cycle
                        #   - short-term momentum (10s avg vs 30s avg)
                        #   - 1-min BRTI smoothing factor (CF Benchmark averaging)
                        # Flips when P < flip_probability_threshold (default 38%).
                        # Use 10s avg for fast flip detection — 60s smoothed is too slow
                        # (a $78 drop in 60s only moves smoothed_brti ~$38, masking the real signal)
                        recent_10s = [v for t, v in st["ticks"] if t > now_ts - 10]
                        flip_brti = sum(recent_10s) / len(recent_10s) if recent_10s else smoothed_brti
                        distance_from_strike = flip_brti - st["strike"]
                        flip_prob_threshold = cfg.get("flip_probability_threshold", FLIP_PROBABILITY_THRESHOLD)
                        p_above = self.estimate_settlement_probability(
                            coin, distance_from_strike, secs_remaining, momentum
                        )
                        p_held_wins = p_above if st["held_side"] == "yes" else (1.0 - p_above)

                        if p_held_wins < flip_prob_threshold:
                            should_flip = True
                            new_side = "no" if st["held_side"] == "yes" else "yes"
                            reason = (
                                f"PROB MODEL FLIP (P(win)={p_held_wins:.1%}<{flip_prob_threshold:.0%}, "
                                f"dist={distance_from_strike:+.0f}, T={secs_remaining:.0f}s, mom={momentum:+.1f})"
                            )
                            if st["flip_confirm_ticks"] == 0:  # log only on first confirmation tick
                                print(
                                    f"[{datetime.now().strftime('%H:%M:%S')}] {coin} prob flip signal: "
                                    f"P({st['held_side']}_wins)={p_held_wins:.1%} < {flip_prob_threshold:.0%} "
                                    f"(d={distance_from_strike:+.0f}, T={secs_remaining:.0f}s, mom={momentum:+.1f})"
                                )

                        # Tier B: HARD STOP — emergency cap, only when projection also against us
                        # "We've lost too much AND settlement projection has turned against us"
                        # projected_winning guard: if sBRTI still favors our side, Tier C (hold) is correct
                        elif loss_from_entry >= hard_stop and not projected_winning:
                            # Go flat, don't flip — we don't have conviction about the other side
                            old_side = st["held_side"]
                            print(f"[{datetime.now().strftime('%H:%M:%S')}] 🛑 {coin} HARD STOP: SELL {old_side.upper()} @ {current_value}c (entry:{st['entry_price']}c loss:{loss_from_entry}c) — flat")
                            # ALWAYS reset state first — prevents infinite hard-stop storm in paper mode
                            self.ticker_contracts[coin_ticker] = 0
                            self.positions[coin_ticker] = []
                            st["held_side"] = ""
                            st["entry_made"] = False
                            st["peak_value"] = 0
                            st["entry_price"] = 0
                            st["conviction_adds"] = 0
                            st["last_flip_ts"] = time.time()
                            st["direction"] = ""  # re-evaluate direction before re-entry
                            if self.client and not DRY_RUN:
                                try:
                                    hs_id = f"hstop-{coin_ticker}-{int(time.time()*1000)}"
                                    _hs_count = safe_sell_count
                                    _kal = self.kalshi_positions.get(coin_ticker)
                                    if _kal is not None and _hs_count > _kal:
                                        print(f"  ⚠️ SELL GUARD: {coin} hard stop capping {_hs_count}x → {_kal}x")
                                        _hs_count = _kal
                                    if _hs_count > 0:
                                        self.client.create_order(
                                            ticker=coin_ticker, client_order_id=hs_id,
                                            side=old_side, action="sell", count=_hs_count, type="limit",
                                            yes_price=yes_bid if old_side == "yes" else None,
                                            no_price=(100 - yes_ask) if old_side == "no" else None,
                                        )
                                    else:
                                        print(f"  ⚠️ SELL GUARD: {coin} skipping hard stop sell — kalshi shows 0 held")
                                    print(f"  → {coin} Hard stop executed — direction reset, re-evaluating")
                                except Exception as e:
                                    print(f"  → {coin} Hard stop error (state already cleared): {e}")
                            else:
                                print(f"  📄 PAPER: {coin} hard stop — flat, direction reset")
                            await asyncio.sleep(0.5)
                            continue

                        # Tier C: small loss, projection unclear → DO NOTHING, let it play out

                    # Flip confirmation: require 4 sustained ticks (~2s) before executing
                    if should_flip:
                        st["flip_confirm_ticks"] += 1
                        if st["flip_confirm_ticks"] < 4:
                            continue  # wait for confirmation
                        st["flip_confirm_ticks"] = 0  # reset after executing
                    else:
                        st["flip_confirm_ticks"] = 0  # reset if signal disappears

                    if should_flip:
                        old_side = st["held_side"]
                        sell_price = current_value
                        new_cost = yes_ask if new_side == "yes" else (100 - yes_bid)
                        pnl = sell_price - st["entry_price"]
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] 🔄 {coin} FLIP: {reason} | SELL {old_side.upper()} @ {sell_price}c (pnl:{pnl:+d}c) → BUY {new_side.upper()} @ {new_cost}c")

                        if self.client and not DRY_RUN:
                            # Sell guard: verify actual position against Kalshi before placing atomic flip
                            _kal = self.kalshi_positions.get(coin_ticker)
                            actual_held = held
                            if _kal is not None:
                                if _kal == 0:
                                    print(f"  ⚠️ SELL GUARD: {coin} kalshi shows 0 held — skipping flip")
                                    self.ticker_contracts[coin_ticker] = 0
                                    self.positions[coin_ticker] = []
                                    st["held_side"] = ""
                                    st["entry_made"] = False
                                    st["peak_value"] = 0
                                    st["entry_price"] = 0
                                    st["conviction_adds"] = 0
                                    st["last_flip_ts"] = time.time()
                                    await asyncio.sleep(0.5)
                                    continue
                                if _kal != actual_held:
                                    print(f"  ⚠️ SELL GUARD: {coin} state={actual_held}x kalshi={_kal}x — using kalshi count")
                                    actual_held = _kal

                            # Single atomic flip: BUY new_side for (actual_held + flip_buy_count)
                            # Kalshi auto-closes the old position and opens the new one in one order.
                            flip_buy_count = actual_held + 1  # desired new position size
                            flip_total = actual_held + flip_buy_count  # closes old + opens new
                            taker_price = min(new_cost + 5, 99)  # aggressive limit for immediate taker fill

                            # Reset state BEFORE order — prevents infinite flip storm if order throws
                            self.ticker_contracts[coin_ticker] = 0
                            self.positions[coin_ticker] = []
                            st["held_side"] = ""
                            st["entry_made"] = False
                            st["peak_value"] = 0
                            st["entry_price"] = 0
                            st["conviction_adds"] = 0
                            st["last_flip_ts"] = time.time()

                            try:
                                flip_id = f"fflip-{coin_ticker}-{int(time.time()*1000)}"
                                self.client.create_order(
                                    ticker=coin_ticker, client_order_id=flip_id,
                                    side=new_side, action="buy", count=flip_total, type="limit",
                                    yes_price=taker_price if new_side == "yes" else None,
                                    no_price=taker_price if new_side == "no" else None,
                                )
                                st["held_side"] = new_side
                                st["entry_price"] = new_cost
                                st["peak_value"] = new_cost
                                st["entry_made"] = True
                                self.ticker_contracts[coin_ticker] = flip_buy_count
                                print(f"  → {coin} Atomic flip → {new_side.upper()} {flip_buy_count}x @ {new_cost}c (order {flip_total}x, closed {actual_held}x {old_side})")
                            except Exception as e:
                                st["direction"] = ""
                                print(f"  → {coin} Atomic flip error (flat, direction reset): {e}")
                except Exception as e:
                    print(f"  Fast flip loop error ({coin}): {e}")
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
        # Volume-weighted median
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
        # Trim to last 60s
        cutoff = time.time() - 60
        self.exchange_trades[exchange][coin] = [(t, p, v) for t, p, v in self.exchange_trades[exchange][coin] if t > cutoff]
        # Update synthetic index tick for this coin
        synthetic = self.compute_synthetic_brti(coin)
        if synthetic:
            now = time.time()
            st = self.brti_state[coin]
            # Only append if at least 0.5s since last tick (avoid flooding)
            if not st["ticks"] or now - st["ticks"][-1][0] >= 0.5:
                st["ticks"].append((now, synthetic))
                # Trim to last 16 min
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
                            # Kraken trade messages: [channelID, [[price, vol, ...], ...], "trade", "XBT/USD"]
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
                    # Subscribe to each coin's channel separately
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
                                # Channel name is like "live_trades_btcusd"
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
                coin_parts = []
                for _coin in BRTI_COIN_CONFIG:
                    _st = self.brti_state[_coin]
                    _val = f"${_st['ticks'][-1][1]:,.2f}" if _st["ticks"] else "?"
                    _age = f"{time.time() - _st['ticks'][-1][0]:.1f}s" if _st["ticks"] else "?"
                    _ticker = self.current_tickers.get(_coin, "")
                    _held = self.ticker_contracts.get(_ticker, 0)
                    _side = _st["held_side"].upper() if _st["held_side"] else "flat"
                    coin_parts.append(f"{_coin}:{_val}({_age}) stk${_st['strike']:,.0f} {_st['direction'] or 'wait'} {_side}{_held}x")
                feeds = sum(1 for s in self.exchange_status.values() if s == "connected")
                num_feeds_expected = len(BRTI_COIN_CONFIG) + 3  # gemini has per-coin connections, others are shared
                status = " | ".join(coin_parts) + f" | feeds:{feeds}/{num_feeds_expected}"
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Min left: {minutes_remaining} | {status} | WS: {'✓' if self.ws_connected else '✗'}")

            except Exception as e:
                print(f"Settlement check error: {e}")

            # Sleep 30s, but never past the next 15m cycle boundary (refresh promptly on rollover)
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
        print(f"🚀 Crypto 15m Agent — BRTI Momentum — {coins_str}")
        print(f"   Signal: BRTI momentum | {coins_str} | synthetic index from 4 exchanges")
        for _coin, _cfg in BRTI_COIN_CONFIG.items():
            print(f"   {_coin}: entry≤{_cfg['entry_max']}c | trail:DISABLED | hard_stop:{_cfg['stop_loss_hard_c']}c | TP:{_cfg['take_profit_c']}c | conv:{_cfg['conviction_min_distance']} | cd:{_cfg['flip_cooldown_sec']}s")
            print(f"   {_coin}: prob_flip_threshold={_cfg.get('flip_probability_threshold', FLIP_PROBABILITY_THRESHOLD):.0%} | σ_per_sec=${_cfg.get('volatility_per_sec', 2.20):.3f} | smoothing={SETTLEMENT_SMOOTHING_FACTOR}")
        print(f"   Source: synthetic (Coinbase+Kraken+Bitstamp+Gemini WebSockets)")
        print(f"   DRY_RUN: {DRY_RUN} | WebSocket mode")

        print("Seeding settlement history...")
        self.seed_settlement_history()

        # Launch WS and settlement check as parallel tasks
        if self.client and self.key_id and self.private_key_path:
            asyncio.create_task(self.kalshi_websocket())
        asyncio.create_task(self.settlement_check_loop())
        asyncio.create_task(self._coinbase_ws())
        asyncio.create_task(self._kraken_ws())
        asyncio.create_task(self._bitstamp_ws())
        asyncio.create_task(self._gemini_ws())
        asyncio.create_task(self.brti_fast_flip_loop())
        brti_coins_str = "+".join(BRTI_COIN_CONFIG.keys())
        print(f"📡 Synthetic index feeds launching ({brti_coins_str}, 4 exchanges) + fast flip loop (500ms)")

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
