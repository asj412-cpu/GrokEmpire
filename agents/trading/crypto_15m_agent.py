"""
Crypto 15m Agent — BRTI Market Maker + Kalshi WebSocket
========================================================
Trades BTC and ETH 15-minute Kalshi markets using a synthetic BRTI index
built from real-time WebSocket feeds (Coinbase, Kraken, Bitstamp, Gemini).

Mode: MM_MODE=true  → Probability-based 2-sided quoting (market maker)
      MM_MODE=false → Directional tiered entry (legacy, tag: v1-directional-tiered)

Position management (TP, hard stop, regime exit) is shared across both modes.
"""

import asyncio
import math
import os
import csv
import time
import json
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
MM_MODE = os.getenv('MM_MODE', 'false').lower() == 'true'

# ─── Market-Making Config ───
MM_EDGE_C = 7                  # edge below fair value per side (cents) — widened from 3, adverse selection was eating us
MM_REQUOTE_THRESHOLD_C = 5     # re-quote if model moved ≥5c (was 2c, caused churn)
MM_SETTLE_GUARD_SEC = 60       # cancel all quotes 60s before settlement
MM_MAX_CONTRACTS = 1           # max contracts per quote side
MM_QUOTE_MIN_C = 15            # don't quote below 15c — extreme prices = pure adverse selection
MM_QUOTE_MAX_C = 93            # allow quoting up to 93c — winning side needs to participate late cycle
MM_RECONCILE_INTERVAL_SEC = 10 # safety reconciliation frequency
MM_SIGMA = {"BTC": 2.20, "ETH": 0.071}   # calibrated σ/sec from backtest
MM_SMOOTHING = 0.55            # CF BRTI 1-min average smoothing factor
MM_MOMENTUM_WEIGHT = 0.015     # momentum adjustment weight


def _norm_cdf(x):
    """Standard normal CDF without scipy dependency."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2)))

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
        "conviction_max_price": 75,    # lowered from 85 — 82-85c adds are net losers (45% WR, need 85%)
        "take_profit_c": 95,
        "reentry_max_price": 59,
        # Tiered entry pricing: wait for value, don't chase at cycle open
        "tier1_max": 45,               # Min 0-7: value entries, sBRTI momentum confirms
        "tier1_end_sec": 420,          # 7 minutes
        "tier2_max": 65,               # Min 7-10: cap increases with more data
        "tier2_end_sec": 600,          # 10 minutes
        "tier3_max": 85,               # Min 10-14: high conviction only (sBRTI past conviction_min_distance)
        "entry_contracts": 3,
        "momentum_window": 8,          # 8s detection — catch sBRTI lead before Kalshi reprices
        "sigma_per_sec": 2.20,         # calibrated BTC volatility for probability model
        "ws_pairs": {"coinbase": "BTC-USD", "kraken": "XBT/USD", "bitstamp": "btcusd", "gemini": "BTCUSD"},
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
        "conviction_max_price": 75,      # lowered from 85 — 82-85c adds are net losers
        "take_profit_c": 95,
        "reentry_max_price": 59,
        # Tiered entry pricing: wait for value, don't chase at cycle open
        "tier1_max": 45,               # Min 0-7: value entries, sBRTI momentum confirms
        "tier1_end_sec": 420,          # 7 minutes
        "tier2_max": 65,               # Min 7-10: cap increases with more data
        "tier2_end_sec": 600,          # 10 minutes
        "tier3_max": 85,               # Min 10-14: high conviction only
        "entry_contracts": 3,
        "momentum_window": 8,          # 8s detection — catch sBRTI lead before Kalshi reprices
        "sigma_per_sec": 0.071,        # calibrated ETH volatility for probability model
        "ws_pairs": {"coinbase": "ETH-USD", "kraken": "ETH/USD", "bitstamp": "ethusd", "gemini": "ETHUSD"},
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
BRTI_CONVICTION_MAX_PRICE = 75
BRTI_TAKE_PROFIT_C = 95
BRTI_REENTRY_MAX_PRICE = 59
BRTI_MOMENTUM_WINDOW = 8
# Tiered entry defaults
BRTI_TIER1_MAX = 45
BRTI_TIER1_END_SEC = 420
BRTI_TIER2_MAX = 65
BRTI_TIER2_END_SEC = 600
BRTI_TIER3_MAX = 85


def _get_tiered_entry_max(cycle_sec, cfg):
    """Return max entry price based on cycle phase.
    Tier 1 (min 0-7):  35c — value entries, sBRTI momentum must confirm
    Tier 2 (min 7-10): 49c — more data, wider cap
    Tier 3 (min 10-14): 85c — high conviction only (requires sBRTI past conviction_min_distance)
    """
    tier1_end = cfg.get("tier1_end_sec", BRTI_TIER1_END_SEC)
    tier2_end = cfg.get("tier2_end_sec", BRTI_TIER2_END_SEC)
    if cycle_sec < tier1_end:
        return cfg.get("tier1_max", BRTI_TIER1_MAX)
    elif cycle_sec < tier2_end:
        return cfg.get("tier2_max", BRTI_TIER2_MAX)
    else:
        return cfg.get("tier3_max", BRTI_TIER3_MAX)

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
            }

        # ─── Market-Making State ───
        self.mm_mode = MM_MODE
        self.mm_state: Dict[str, dict] = {}
        for _coin in BRTI_COIN_CONFIG:
            self.mm_state[_coin] = {
                "yes_order_id": None,     # resting YES bid order_id from Kalshi
                "no_order_id": None,      # resting NO bid order_id from Kalshi
                "yes_price": 0,           # price of resting YES bid (cents)
                "no_price": 0,            # price of resting NO bid (cents)
                "quotes_active": False,   # True when 2-sided quote is live
                "requote_pending": False,  # signal from sBRTI tick to requote
                "quoting_in_flight": False,  # True while API call in progress — prevents accumulation
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

                    # Subscribe to fills + order updates (instant state change notifications)
                    await ws.send(json.dumps({"id": 1, "cmd": "subscribe", "params": {"channels": ["fill", "user_orders"]}}))

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
            # Cancel all MM quotes on rotation — never carry quotes into new market
            if self.mm_mode:
                asyncio.create_task(self.mm_cancel_all_coins("cycle rotation"))
            # Reset BRTI cycle state for all coins
            for _coin in BRTI_COIN_CONFIG:
                st = self.brti_state[_coin]
                st["direction"] = ""
                st["entry_made"] = False
                st["held_side"] = ""
                st["last_flip_ts"] = 0
                st["entry_price"] = 0
                st["peak_value"] = 0
                st["conviction_adds"] = 0
                st["last_conviction_ts"] = 0
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

            # Convert raw yes_price into side-relative cost (what we actually paid)
            purchased_side = (msg.get("purchased_side") or side or "").lower()
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

                # MM fill detection: cancel opposite side immediately
                order_cid = msg.get("client_order_id", "")
                if self.mm_mode and order_cid.startswith("mm-") and coin:
                    asyncio.create_task(self.mm_on_fill(coin, purchased_side, cost_price))

            elif action == "sell":
                self.resting_sells[ticker] = max(0, self.resting_sells.get(ticker, 0) - count)
                if self.positions.get(ticker):
                    for _ in range(min(count, len(self.positions[ticker]))):
                        self.positions[ticker].pop(0)
                self.ticker_contracts[ticker] = max(0, self.ticker_contracts.get(ticker, 0) - count)
                print(f"  ✅ Exit filled! Held: {self.ticker_contracts[ticker]} | Resting sells: {self.resting_sells.get(ticker, 0)}")

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
        # Re-evaluate if "flat" but sBRTI has since moved clearly past strike
        needs_eval = not st["direction"]
        if st["direction"] == "flat" and not st["entry_made"] and st["ticks"]:
            latest = st["ticks"][-1][1]
            min_clear = cfg.get("trailing_stop_mid_dist", 20)
            if abs(latest - st["strike"]) >= min_clear:
                needs_eval = True  # override flat — sBRTI moved clearly past strike
        if needs_eval and cycle_sec >= momentum_window and len(st["ticks"]) >= 10:
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

        # ── Phase 2: Initial entry (tiered pricing) — directional mode only ──
        if not self.mm_mode and st["direction"] and not st["entry_made"] and st["direction"] != "flat":
            # Re-entry cooldown: after any exit (regime/stop), wait before re-entering
            flip_cooldown = cfg.get("flip_cooldown_sec", BRTI_FLIP_COOLDOWN_SEC)
            if st["last_flip_ts"] and (time.time() - st["last_flip_ts"]) < flip_cooldown:
                return

            target_side = "yes" if st["direction"] == "up" else "no"

            # Sanity check: don't enter a side that disagrees with current sBRTI
            # Direction may be stale (set seconds ago) but sBRTI has since crossed strike
            if st["ticks"]:
                current_brti = st["ticks"][-1][1]
                if target_side == "yes" and current_brti < st["strike"]:
                    return  # direction says up but sBRTI is below strike — stale signal
                if target_side == "no" and current_brti > st["strike"]:
                    return  # direction says down but sBRTI is above strike — stale signal

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

            # Tier 3 conviction guard: min 10-14 requires sBRTI past conviction_min_distance
            tier2_end = cfg.get("tier2_end_sec", BRTI_TIER2_END_SEC)
            if cycle_sec >= tier2_end:
                smooth_ticks = [v for t, v in st["ticks"] if t > now.timestamp() - BRTI_SMOOTHING_WINDOW]
                if smooth_ticks:
                    smoothed = sum(smooth_ticks) / len(smooth_ticks)
                    dist_past = smoothed - st["strike"] if target_side == "yes" else st["strike"] - smoothed
                    if dist_past < cfg.get("conviction_min_distance", BRTI_CONVICTION_MIN_DISTANCE):
                        return  # not enough conviction for Tier 3 entry
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
            # Tiered entry max: 35c (min 0-7) → 49c (min 7-10) → 85c (min 10-14)
            entry_max = _get_tiered_entry_max(cycle_sec, cfg)
            reentry_max = cfg.get("reentry_max_price", entry_max)
            is_reentry = st["conviction_adds"] > 0 or st["peak_value"] > 0
            max_price = min(reentry_max, entry_max) if is_reentry else entry_max
            # Global exposure guard: max 10 contracts across all coins
            total_exposure = sum(self.ticker_contracts.values())
            if total_exposure >= 10:
                return

            tier_label = "T1" if cycle_sec < cfg.get("tier1_end_sec", BRTI_TIER1_END_SEC) else ("T2" if cycle_sec < tier2_end else "T3")
            if 1 <= cost <= max_price:
                st["entry_made"] = True
                st["held_side"] = target_side
                st["entry_price"] = cost
                st["peak_value"] = cost
                print(f"[{now.strftime('%H:%M:%S')}] {coin} BRTI-ENTRY {target_side.upper()} 1 @ {cost}c [{tier_label}≤{max_price}c] (dir {st['direction']}, strike ${st['strike']:,.2f})")
                entry_count = cfg.get("entry_contracts", 1)
                self._post_buy(ticker, coin, target_side, cost, target_contracts=entry_count, count=entry_count)
                return

        # Phase 3 handled by brti_fast_flip_loop (500ms, trailing stop + stop loss)

    def _post_buy(self, ticker, coin, side, price, target_contracts, count=1):
        """Post a limit buy order (sync). count=number of contracts."""
        client_order_id = f"brti-{side}-{ticker}-{int(time.time())}"
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {coin} | BUY {side.upper()} {count} @ {price}c | held:{self.ticker_contracts.get(ticker, 0)}")
        if self.client and not DRY_RUN:
            try:
                self.client.create_order(
                    ticker=ticker, client_order_id=client_order_id,
                    side=side, action="buy", count=count, type="limit",
                    yes_price=price if side == "yes" else None,
                    no_price=price if side == "no" else None,
                )
            except Exception as e:
                print(f"  → Buy error: {e}")

    async def _post_buy_async(self, ticker, coin, side, price, count=1):
        """Non-blocking buy — runs create_order in thread so event loop stays responsive."""
        client_order_id = f"brti-{side}-{ticker}-{int(time.time())}"
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {coin} | BUY {side.upper()} {count} @ {price}c")
        if self.client and not DRY_RUN:
            try:
                await asyncio.to_thread(
                    self.client.create_order,
                    ticker=ticker, client_order_id=client_order_id,
                    side=side, action="buy", count=count, type="limit",
                    yes_price=price if side == "yes" else None,
                    no_price=price if side == "no" else None,
                )
            except Exception as e:
                print(f"  → Buy error: {e}")

    async def _sell_async(self, ticker, coin, side, count, yes_bid, yes_ask, reason=""):
        """Non-blocking sell."""
        sell_id = f"sell-{coin.lower()}-{ticker}-{int(time.time()*1000)}"
        if self.client and not DRY_RUN:
            try:
                await asyncio.to_thread(
                    self.client.create_order,
                    ticker=ticker, client_order_id=sell_id,
                    side=side, action="sell", count=count, type="limit",
                    yes_price=yes_bid if side == "yes" else None,
                    no_price=(100 - yes_ask) if side == "no" else None,
                )
                print(f"  → {coin} Sell {count}x {side.upper()} ({reason})")
            except Exception as e:
                print(f"  → {coin} Sell error: {e}")
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

    # ─── Market-Making Engine ─────────────────────────────────

    def mm_compute_fair_value(self, coin):
        """Compute P(YES) from sBRTI probability model. Returns (yes_fair, no_fair) in cents."""
        st = self.brti_state[coin]
        cfg = BRTI_COIN_CONFIG[coin]
        if st["strike"] <= 0 or not st["ticks"]:
            return 50, 50  # no data → 50/50

        now_ts = time.time()
        cycle_sec = (datetime.now().minute % 15) * 60 + datetime.now().second
        secs_remaining = max(1, 900 - cycle_sec)

        # Use 10-second smoothed sBRTI for stability
        recent = [v for t, v in st["ticks"] if t > now_ts - 10]
        if not recent:
            recent = [st["ticks"][-1][1]]
        smoothed_brti = sum(recent) / len(recent)
        distance = smoothed_brti - st["strike"]

        # Momentum adjustment (same as backtest_prob_model.py)
        ticks_10s = [v for t, v in st["ticks"] if t > now_ts - 10]
        ticks_30s = [v for t, v in st["ticks"] if t > now_ts - 30]
        if len(ticks_10s) >= 2 and len(ticks_30s) >= 2:
            momentum = (sum(ticks_10s) / len(ticks_10s)) - (sum(ticks_30s) / len(ticks_30s))
        else:
            momentum = 0
        momentum_adj = momentum * min(60, secs_remaining) * MM_MOMENTUM_WEIGHT

        # Probability: Φ((distance + momentum_adj) / (σ × √T × smoothing))
        sigma = cfg.get("sigma_per_sec", MM_SIGMA.get(coin, 2.20))
        effective_sigma = sigma * (secs_remaining ** 0.5) * MM_SMOOTHING
        if effective_sigma <= 0:
            return 50, 50

        p_yes = _norm_cdf((distance + momentum_adj) / effective_sigma)
        yes_fair = max(1, min(99, round(p_yes * 100)))
        no_fair = 100 - yes_fair
        return yes_fair, no_fair

    def mm_compute_quotes(self, coin):
        """Compute bid prices for both sides. Returns (yes_bid, no_bid) or None."""
        yes_fair, no_fair = self.mm_compute_fair_value(coin)
        yes_bid = yes_fair - MM_EDGE_C
        no_bid = no_fair - MM_EDGE_C

        # Safety: combined cost must be < 100c (otherwise guaranteed loss)
        if yes_bid + no_bid >= 100:
            return None

        # Clamp to reasonable range
        if yes_bid < MM_QUOTE_MIN_C:
            yes_bid = 0  # don't quote
        if yes_bid > MM_QUOTE_MAX_C:
            yes_bid = 0
        if no_bid < MM_QUOTE_MIN_C:
            no_bid = 0
        if no_bid > MM_QUOTE_MAX_C:
            no_bid = 0

        if yes_bid == 0 and no_bid == 0:
            return None
        return yes_bid, no_bid

    async def mm_cancel_all_quotes(self, coin, reason=""):
        """Cancel all resting MM orders for a coin. Uses batch cancel for speed. Clears state regardless."""
        ms = self.mm_state[coin]
        ids_to_cancel = []
        if ms["yes_order_id"]:
            ids_to_cancel.append(ms["yes_order_id"])
        if ms["no_order_id"]:
            ids_to_cancel.append(ms["no_order_id"])

        if ids_to_cancel and self.client and not DRY_RUN:
            try:
                # Batch cancel — single API call for both sides
                await asyncio.to_thread(self.client.batch_cancel_orders, ids_to_cancel)
            except Exception:
                # Fallback: cancel individually
                for oid in ids_to_cancel:
                    try:
                        await asyncio.to_thread(self.client.cancel_order, oid)
                    except Exception:
                        pass

        # Always clear local state — defensive
        if ids_to_cancel:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 📊 {coin} MM cancel {len(ids_to_cancel)} quotes ({reason})")
        ms["yes_order_id"] = None
        ms["no_order_id"] = None
        ms["yes_price"] = 0
        ms["no_price"] = 0
        ms["quotes_active"] = False

    async def mm_cancel_all_coins(self, reason=""):
        """Cancel all MM quotes across all coins."""
        for coin in BRTI_COIN_CONFIG:
            await self.mm_cancel_all_quotes(coin, reason)

    async def mm_place_quotes(self, coin):
        """Place 2-sided quotes. ALWAYS cancel-before-replace to prevent accumulation."""
        ms = self.mm_state[coin]
        if ms["quoting_in_flight"]:
            return  # previous API call still pending — don't stack
        ticker = self.current_tickers.get(coin, "")
        if not ticker:
            return
        ms["quoting_in_flight"] = True
        try:
            await self._mm_place_quotes_inner(coin, ticker, ms)
        finally:
            ms["quoting_in_flight"] = False

    async def _mm_place_quotes_inner(self, coin, ticker, ms):
        """Inner implementation — separated so quoting_in_flight guard wraps everything."""
        # Step 1: Cancel existing quotes FIRST (cancel-before-replace)
        if ms["quotes_active"]:
            await self.mm_cancel_all_quotes(coin, "requote")

        # Step 2: Don't quote if we already hold a position
        st = self.brti_state[coin]
        if st["held_side"]:
            return

        # Step 3: Compute new quotes
        quotes = self.mm_compute_quotes(coin)
        if not quotes:
            return
        yes_bid, no_bid = quotes

        # Step 4: Place BOTH orders in single batch API call (1 round-trip instead of 2)
        yes_oid = None
        no_oid = None

        if self.client and not DRY_RUN:
            ts_ms = int(time.time() * 1000)
            batch = []
            yes_cid = None
            no_cid = None
            if yes_bid > 0:
                yes_cid = f"mm-yes-{ticker}-{ts_ms}"
                batch.append({
                    "ticker": ticker, "client_order_id": yes_cid,
                    "side": "yes", "action": "buy", "count": MM_MAX_CONTRACTS,
                    "type": "limit", "yes_price": yes_bid,
                })
            if no_bid > 0:
                no_cid = f"mm-no-{ticker}-{ts_ms}"
                batch.append({
                    "ticker": ticker, "client_order_id": no_cid,
                    "side": "no", "action": "buy", "count": MM_MAX_CONTRACTS,
                    "type": "limit", "no_price": no_bid,
                })

            if batch:
                try:
                    result = await asyncio.to_thread(self.client.batch_create_orders, batch)
                    # Parse response: {"orders": [{"order": {"order_id": ..., "client_order_id": ...}}, ...]}
                    for ro in result.get("orders", []):
                        inner = ro.get("order", ro)  # handle both nested and flat formats
                        cid = inner.get("client_order_id", "")
                        oid = inner.get("order_id", "")
                        if cid == yes_cid:
                            yes_oid = oid
                        elif cid == no_cid:
                            no_oid = oid
                except Exception as e:
                    print(f"  → {coin} MM batch order error: {e}")
                    # Fallback: if batch fails, try individual orders
                    if yes_bid > 0:
                        try:
                            r = await asyncio.to_thread(
                                self.client.create_order,
                                ticker=ticker, client_order_id=f"mm-yes-{ticker}-{ts_ms}f",
                                side="yes", action="buy", count=MM_MAX_CONTRACTS,
                                type="limit", yes_price=yes_bid,
                            )
                            yes_oid = r.get("order", {}).get("order_id")
                        except Exception:
                            pass
                    if no_bid > 0:
                        try:
                            r = await asyncio.to_thread(
                                self.client.create_order,
                                ticker=ticker, client_order_id=f"mm-no-{ticker}-{ts_ms}f",
                                side="no", action="buy", count=MM_MAX_CONTRACTS,
                                type="limit", no_price=no_bid,
                            )
                            no_oid = r.get("order", {}).get("order_id")
                        except Exception:
                            pass

        # Step 5: Update state
        ms["yes_order_id"] = yes_oid
        ms["no_order_id"] = no_oid
        ms["yes_price"] = yes_bid if yes_oid else 0
        ms["no_price"] = no_bid if no_oid else 0
        ms["quotes_active"] = bool(yes_oid or no_oid)

        yes_fair, no_fair = self.mm_compute_fair_value(coin)
        if ms["quotes_active"]:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 📊 {coin} MM QUOTE: YES bid {yes_bid}c / NO bid {no_bid}c (fair:{yes_fair}/{no_fair})")

    async def mm_on_fill(self, coin, filled_side, fill_price):
        """Handle MM fill: cancel opposite side, hand off to position management."""
        ms = self.mm_state[coin]

        # IMMEDIATELY cancel opposite side — prevent holding both directions
        if filled_side == "yes" and ms["no_order_id"]:
            if self.client and not DRY_RUN:
                try:
                    await asyncio.to_thread(self.client.cancel_order, ms["no_order_id"])
                except Exception:
                    pass
            ms["no_order_id"] = None
        elif filled_side == "no" and ms["yes_order_id"]:
            if self.client and not DRY_RUN:
                try:
                    await asyncio.to_thread(self.client.cancel_order, ms["yes_order_id"])
                except Exception:
                    pass
            ms["yes_order_id"] = None

        ms["quotes_active"] = False
        ms["yes_price"] = 0
        ms["no_price"] = 0

        # Hand off to position management via brti_state
        st = self.brti_state[coin]
        st["held_side"] = filled_side
        st["entry_made"] = True
        st["entry_price"] = fill_price
        st["peak_value"] = fill_price
        st["direction"] = "up" if filled_side == "yes" else "down"

        print(f"[{datetime.now().strftime('%H:%M:%S')}] 📊 {coin} MM FILL: {filled_side.upper()} @ {fill_price}c — managing position")

    async def mm_requote_loop(self):
        """Main MM event loop: update quotes on every sBRTI tick. Runs alongside fast flip loop."""
        while self.running:
            for coin, cfg in BRTI_COIN_CONFIG.items():
                try:
                    ms = self.mm_state[coin]
                    st = self.brti_state[coin]

                    # Skip if position held (fast flip loop handles exits)
                    if st["held_side"]:
                        if ms["quotes_active"]:
                            await self.mm_cancel_all_quotes(coin, "position held")
                        continue

                    # Cooldown after exit: don't re-quote for 30s after hard stop/regime exit
                    # Prevents immediately getting picked off on the same losing side
                    if st["last_flip_ts"] and (time.time() - st["last_flip_ts"]) < 30:
                        continue

                    # Skip if no pending tick (event-driven)
                    if not ms["requote_pending"]:
                        continue
                    ms["requote_pending"] = False

                    if st["strike"] <= 0 or not st["ticks"]:
                        continue
                    ticker = self.current_tickers.get(coin, "")
                    if not ticker:
                        continue

                    cycle_sec = (datetime.now().minute % 15) * 60 + datetime.now().second

                    # Settlement guard: cancel all quotes in last 60s
                    if cycle_sec >= (900 - MM_SETTLE_GUARD_SEC):
                        if ms["quotes_active"]:
                            await self.mm_cancel_all_quotes(coin, "settlement guard")
                        continue

                    # Rollover guard: no quotes in first 5s
                    if cycle_sec < ROLLOVER_GUARD_SEC:
                        continue

                    # Compute new quotes
                    quotes = self.mm_compute_quotes(coin)
                    if not quotes:
                        if ms["quotes_active"]:
                            await self.mm_cancel_all_quotes(coin, "no valid quote")
                        continue

                    yes_bid, no_bid = quotes

                    # Only re-quote if price moved enough (avoid churning API)
                    if ms["quotes_active"]:
                        yes_moved = abs(yes_bid - ms["yes_price"]) >= MM_REQUOTE_THRESHOLD_C if yes_bid > 0 and ms["yes_price"] > 0 else yes_bid != ms["yes_price"]
                        no_moved = abs(no_bid - ms["no_price"]) >= MM_REQUOTE_THRESHOLD_C if no_bid > 0 and ms["no_price"] > 0 else no_bid != ms["no_price"]
                        if not yes_moved and not no_moved:
                            continue  # prices haven't moved enough

                    await self.mm_place_quotes(coin)
                except Exception as e:
                    print(f"  MM requote error ({coin}): {e}")

            await asyncio.sleep(0.01)  # 10ms yield — faster requote cycle

    async def mm_safety_reconcile_loop(self):
        """Every 10s: verify resting orders match local state. THE KEY SAFETY BACKSTOP.
        Prevents the resting order accumulation bug that wiped out cash previously."""
        while self.running:
            if self.mm_mode and self.client and not DRY_RUN:
                for coin in BRTI_COIN_CONFIG:
                    ticker = self.current_tickers.get(coin, "")
                    if not ticker:
                        continue
                    try:
                        orders = await asyncio.to_thread(self.client.get_orders, ticker=ticker)
                        resting_mm = [
                            o for o in orders.get("orders", [])
                            if o.get("status") == "resting"
                            and (o.get("client_order_id") or "").startswith("mm-")
                        ]
                        ms = self.mm_state[coin]

                        # SAFETY 1: More than 2 resting MM orders = ACCUMULATION — cancel all
                        if len(resting_mm) > 2:
                            print(f"  🚨 SAFETY: {coin} has {len(resting_mm)} resting MM orders (max 2) — canceling ALL")
                            for o in resting_mm:
                                try:
                                    await asyncio.to_thread(self.client.cancel_order, o["order_id"])
                                except Exception:
                                    pass
                            ms["yes_order_id"] = None
                            ms["no_order_id"] = None
                            ms["quotes_active"] = False

                        # SAFETY 2: Local says no quotes, but exchange has resting — orphans
                        elif not ms["quotes_active"] and resting_mm:
                            print(f"  🚨 SAFETY: {coin} local=inactive but {len(resting_mm)} orphan orders — canceling")
                            for o in resting_mm:
                                try:
                                    await asyncio.to_thread(self.client.cancel_order, o["order_id"])
                                except Exception:
                                    pass

                        # SAFETY 3: Local says active but orders gone from exchange
                        elif ms["quotes_active"]:
                            resting_ids = {o["order_id"] for o in resting_mm}
                            if ms["yes_order_id"] and ms["yes_order_id"] not in resting_ids:
                                ms["yes_order_id"] = None
                            if ms["no_order_id"] and ms["no_order_id"] not in resting_ids:
                                ms["no_order_id"] = None
                            if not ms["yes_order_id"] and not ms["no_order_id"]:
                                ms["quotes_active"] = False

                    except Exception as e:
                        print(f"  Safety reconcile error ({coin}): {e}")

            await asyncio.sleep(MM_RECONCILE_INTERVAL_SEC)

    # ─── Settlement History ───────────────────────────────────

    def seed_open_positions(self):
        """On restart, detect and MANAGE inherited positions instead of blocking them."""
        if not self.client:
            return
        try:
            positions = self.client.get_positions(count_filter="position", settlement_status="unsettled")
            for mp in positions.get("market_positions", []):
                ticker = mp.get("ticker", "")
                position_fp = float(mp.get("position_fp", 0))
                if position_fp == 0 or not ticker:
                    continue
                # Match to a coin
                coin = None
                for c, cfg in BRTI_COIN_CONFIG.items():
                    if ticker.startswith(cfg["series"]):
                        coin = c
                        break
                if not coin:
                    continue
                # Determine side and count
                held_side = "yes" if position_fp > 0 else "no"
                count = int(abs(position_fp))
                # Estimate entry price from cost
                exposure = float(mp.get("market_exposure_dollars", "0"))
                entry_price = int(exposure / count * 100) if count > 0 else 50  # cents
                # Set up brti_state so fast flip loop manages this position
                st = self.brti_state[coin]
                st["held_side"] = held_side
                st["entry_made"] = True
                st["entry_price"] = entry_price
                st["peak_value"] = entry_price
                self.ticker_contracts[ticker] = count
                self.current_tickers[coin] = ticker
                print(f"  📌 {coin} INHERITED: {held_side.upper()} {count}x @ ~{entry_price}c (ticker: {ticker}) — actively managing")
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
                    # Only process when new exchange data arrived (event-driven)
                    if not st.get("_tick_pending", False):
                        continue
                    st["_tick_pending"] = False
                    coin_ticker = self.current_tickers.get(coin, "")
                    if not coin_ticker:
                        continue

                    # ── Entry check (WS prices only — no API blocking, tiered pricing) ──
                    # In MM mode, mm_requote_loop handles entries — skip directional logic
                    # Re-entry cooldown: after any exit (regime/stop), wait before re-entering
                    flip_cooldown = cfg.get("flip_cooldown_sec", BRTI_FLIP_COOLDOWN_SEC)
                    if not self.mm_mode and st["direction"] and not st["entry_made"] and st["direction"] != "flat" and not st["held_side"] \
                            and (not st["last_flip_ts"] or (time.time() - st["last_flip_ts"]) >= flip_cooldown):
                        prices = self.ws_prices.get(coin_ticker, {})
                        yes_ask = prices.get("yes_ask", 0)
                        yes_bid = prices.get("yes_bid", 0)
                        if yes_ask <= 0 or yes_bid <= 0:
                            continue  # WS prices stale — skip, don't block on API
                        if yes_ask > 0 and yes_bid > 0:
                            target_side = "yes" if st["direction"] == "up" else "no"
                            # Sanity check: don't enter a side that disagrees with current sBRTI
                            if st["ticks"]:
                                current_brti = st["ticks"][-1][1]
                                if target_side == "yes" and current_brti < st["strike"]:
                                    continue
                                if target_side == "no" and current_brti > st["strike"]:
                                    continue
                            cost = yes_ask if target_side == "yes" else (100 - yes_bid)
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
                            # Tier 3 conviction guard
                            tier2_end = cfg.get("tier2_end_sec", BRTI_TIER2_END_SEC)
                            if cycle_sec >= tier2_end and secs_remaining > 120:
                                smooth_ticks = [v for t, v in st["ticks"] if t > now.timestamp() - BRTI_SMOOTHING_WINDOW]
                                if smooth_ticks:
                                    smoothed = sum(smooth_ticks) / len(smooth_ticks)
                                    dist_past = smoothed - st["strike"] if target_side == "yes" else st["strike"] - smoothed
                                    if dist_past < cfg.get("conviction_min_distance", BRTI_CONVICTION_MIN_DISTANCE):
                                        continue
                                else:
                                    continue
                            # Global exposure guard
                            total_exposure = sum(self.ticker_contracts.values())
                            if total_exposure >= 10:
                                continue
                            # Tiered entry max: 35c (min 0-7) → 49c (min 7-10) → 85c (min 10-14)
                            entry_max = _get_tiered_entry_max(cycle_sec, cfg)
                            entry_count = cfg.get("entry_contracts", 1)
                            tier_label = "T1" if cycle_sec < cfg.get("tier1_end_sec", BRTI_TIER1_END_SEC) else ("T2" if cycle_sec < tier2_end else "T3")
                            if 1 <= cost <= entry_max:
                                st["entry_made"] = True
                                st["held_side"] = target_side
                                st["entry_price"] = cost
                                st["peak_value"] = cost
                                # Set ticker_contracts immediately — don't wait for WS fill
                                # (WS may be quiet during low-liquidity hours)
                                self.ticker_contracts[coin_ticker] = self.ticker_contracts.get(coin_ticker, 0) + entry_count
                                print(f"[{now.strftime('%H:%M:%S')}] {coin} BRTI-ENTRY(fast) {target_side.upper()} {entry_count}x @ {cost}c [{tier_label}≤{entry_max}c] (dir {st['direction']}, strike ${st['strike']:,.2f})")
                                await self._post_buy_async(coin_ticker, coin, target_side, cost, count=entry_count)
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

                    # Dynamic trailing stop based on distance from strike
                    # Far from strike = wider stop (noise), near strike = tight stop (danger)
                    # Use SMOOTHED sBRTI for zone determination — prevents zone-hopping on tick noise
                    # A momentary bounce toward strike shouldn't shrink our trailing stop
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
                    # entry_max for flip re-buy: use current tier
                    _now_flip = datetime.now()
                    _cycle_sec_flip = (_now_flip.minute % 15) * 60 + _now_flip.second
                    entry_max = _get_tiered_entry_max(_cycle_sec_flip, cfg)

                    # ── Take profit: exit at take_profit_c+ — lock in the win ──
                    if current_value >= take_profit_c:
                        old_side = st["held_side"]
                        profit = current_value - st["entry_price"]
                        # Cap sell count: entry_contracts + conviction_adds — don't oversell
                        entry_contracts = cfg.get("entry_contracts", 1)
                        safe_sell_count = max(1, min(held, entry_contracts + st.get("conviction_adds", 0)))
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] 💰 {coin} TAKE PROFIT: {old_side.upper()} {safe_sell_count}x @ {current_value}c (entry:{st['entry_price']}c pnl:+{profit}c)")
                        if self.client and not DRY_RUN:
                            try:
                                tp_id = f"tp-{coin_ticker}-{int(time.time()*1000)}"
                                await asyncio.to_thread(self.client.create_order,
                                    ticker=coin_ticker, client_order_id=tp_id,
                                    side=old_side, action="sell", count=safe_sell_count, type="limit",
                                    yes_price=yes_bid if old_side == "yes" else None,
                                    no_price=(100 - yes_ask) if old_side == "no" else None,
                                )
                                self.ticker_contracts[coin_ticker] = 0
                                self.positions[coin_ticker] = []
                                st["held_side"] = ""
                                st["entry_made"] = True  # cycle done — don't re-buy what we just sold at 95c
                                st["peak_value"] = 0
                                st["entry_price"] = 0
                                print(f"  → {coin} Sold {held}x @ {current_value}c — cycle done, no re-entry")
                            except Exception as e:
                                print(f"  → {coin} Take profit error: {e}")
                        continue

                    latest_brti = st["ticks"][-1][1]
                    distance = latest_brti - st["strike"]
                    drop_from_peak = st["peak_value"] - current_value

                    # ── Conviction buy: add when projected settlement is locked ──
                    cycle_now = datetime.now()
                    cycle_sec = (cycle_now.minute % 15) * 60 + cycle_now.second
                    total_exposure = sum(self.ticker_contracts.values())
                    if (st["conviction_adds"] < conviction_max_adds
                            and cycle_sec >= conviction_min_cycle_sec
                            and (time.time() - st["last_conviction_ts"]) > conviction_cooldown_sec
                            and total_exposure < 10):
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
                                        await asyncio.to_thread(self.client.create_order,
                                            ticker=coin_ticker, client_order_id=conv_id,
                                            side=st["held_side"], action="buy", count=1, type="limit",
                                            yes_price=yes_ask if st["held_side"] == "yes" else None,
                                            no_price=(100 - yes_bid) if st["held_side"] == "no" else None,
                                        )
                                    except Exception as e:
                                        print(f"  → {coin} Conviction buy error: {e}")

                    # ── Check trailing stop / stop loss ──
                    should_flip = False
                    new_side = ""
                    reason = ""
                    was_profitable = st["peak_value"] > st["entry_price"]
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

                    # ── REGIME EXIT: fires regardless of was_profitable ──
                    # If projected settlement is $30+ wrong AND momentum confirms, sell and go flat
                    hard_stop = cfg.get("stop_loss_hard_c", 20)
                    momentum_flip_dist = cfg.get("momentum_flip_distance", conviction_min_distance)

                    if st["held_side"] == "yes":
                        wrong_side_distance = st["strike"] - projected_settlement
                    else:
                        wrong_side_distance = projected_settlement - st["strike"]

                    ticks_10s_m = [v for t, v in st["ticks"] if t > now_ts - 10]
                    ticks_30s_m = [v for t, v in st["ticks"] if t > now_ts - 30]
                    if len(ticks_10s_m) >= 2 and len(ticks_30s_m) >= 2:
                        short_momentum = sum(ticks_10s_m) / len(ticks_10s_m) - sum(ticks_30s_m) / len(ticks_30s_m)
                    else:
                        short_momentum = 0
                    momentum_confirms = (st["held_side"] == "yes" and short_momentum < 0) or \
                                       (st["held_side"] == "no" and short_momentum > 0)

                    if wrong_side_distance >= momentum_flip_dist and momentum_confirms:
                            # Regime change: sell and go FLAT — re-evaluate before re-entering
                            old_side = st["held_side"]
                            pnl_val = current_value - st["entry_price"]
                            print(f"[{datetime.now().strftime('%H:%M:%S')}] 🔄 {coin} REGIME EXIT: SELL {old_side.upper()} @ {current_value}c (pnl:{pnl_val:+d}c, proj ${wrong_side_distance:,.0f} wrong side) — flat, re-evaluating")
                            if self.client and not DRY_RUN:
                                try:
                                    re_id = f"regime-{coin_ticker}-{int(time.time()*1000)}"
                                    await asyncio.to_thread(self.client.create_order,
                                        ticker=coin_ticker, client_order_id=re_id,
                                        side=old_side, action="sell", count=safe_sell_count, type="limit",
                                        yes_price=yes_bid if old_side == "yes" else None,
                                        no_price=(100 - yes_ask) if old_side == "no" else None,
                                    )
                                    self.ticker_contracts[coin_ticker] = 0
                                    self.positions[coin_ticker] = []
                                    st["held_side"] = ""
                                    st["entry_made"] = False
                                    st["peak_value"] = 0
                                    st["entry_price"] = 0
                                    st["last_flip_ts"] = time.time()
                                    st["direction"] = ""  # re-evaluate market before re-entry
                                except Exception as e:
                                    print(f"  → {coin} Regime exit error: {e}")
                            await asyncio.sleep(0.5)
                            continue

                    # ── HARD STOP: emergency cap, max loss regardless ──
                    # Always fire at max loss — was_profitable bypass caused 3 catastrophic
                    # losses (positions briefly profitable then collapsed with no stop)
                    if loss_from_entry >= hard_stop:
                            # Go flat, don't flip — we don't have conviction about the other side
                            old_side = st["held_side"]
                            pnl_val = current_value - st["entry_price"]
                            print(f"[{datetime.now().strftime('%H:%M:%S')}] 🛑 {coin} HARD STOP: SELL {old_side.upper()} @ {current_value}c (entry:{st['entry_price']}c loss:{loss_from_entry}c) — flat")
                            if self.client and not DRY_RUN:
                                try:
                                    hs_id = f"hstop-{coin_ticker}-{int(time.time()*1000)}"
                                    await asyncio.to_thread(self.client.create_order,
                                        ticker=coin_ticker, client_order_id=hs_id,
                                        side=old_side, action="sell", count=safe_sell_count, type="limit",
                                        yes_price=yes_bid if old_side == "yes" else None,
                                        no_price=(100 - yes_ask) if old_side == "no" else None,
                                    )
                                    self.ticker_contracts[coin_ticker] = 0
                                    self.positions[coin_ticker] = []
                                    st["held_side"] = ""
                                    st["entry_made"] = False
                                    st["peak_value"] = 0
                                    st["entry_price"] = 0
                                    st["last_flip_ts"] = time.time()
                                    # Reset direction — stale direction caused re-entry on losing side
                                    st["direction"] = ""
                                    print(f"  → {coin} Direction reset — will re-evaluate before re-entry")
                                except Exception as e:
                                    print(f"  → {coin} Hard stop error: {e}")
                            await asyncio.sleep(0.5)
                            continue

                        # Tier C: small loss, projection unclear → DO NOTHING, let it play out

                    # Old flip logic disabled — regime exit goes flat instead
                    should_flip = False
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
                            try:
                                sell_id = f"fflip-sell-{coin_ticker}-{int(time.time()*1000)}"
                                await asyncio.to_thread(self.client.create_order,
                                    ticker=coin_ticker, client_order_id=sell_id,
                                    side=old_side, action="sell", count=safe_sell_count, type="limit",
                                    yes_price=yes_bid if old_side == "yes" else None,
                                    no_price=(100 - yes_ask) if old_side == "no" else None,
                                )
                                self.ticker_contracts[coin_ticker] = 0
                                self.positions[coin_ticker] = []

                                flip_buy_count = held + 1  # sell N, buy N+1 — double down on new direction
                                if new_cost <= entry_max:
                                    buy_id = f"fflip-buy-{coin_ticker}-{int(time.time()*1000)}"
                                    await asyncio.to_thread(self.client.create_order,
                                        ticker=coin_ticker, client_order_id=buy_id,
                                        side=new_side, action="buy", count=flip_buy_count, type="limit",
                                        yes_price=yes_ask if new_side == "yes" else None,
                                        no_price=(100 - yes_bid) if new_side == "no" else None,
                                    )
                                    st["held_side"] = new_side
                                    st["entry_price"] = new_cost
                                    st["peak_value"] = new_cost
                                    print(f"  → {coin} Flipped to {new_side.upper()} {flip_buy_count}x @ {new_cost}c (was {held}x)")
                                else:
                                    st["held_side"] = ""
                                    print(f"  → {coin} Sold, new side too expensive ({new_cost}c)")
                                st["last_flip_ts"] = time.time()
                            except Exception as e:
                                print(f"  → {coin} Flip error: {e}")
                except Exception as e:
                    print(f"  Fast flip loop error ({coin}): {e}")
            # Event-driven: sleep only 50ms (yield to event loop for WS ticks)
            # Trading logic only fires when st["_tick_pending"] is True
            await asyncio.sleep(0.05)

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
            st["ticks"].append((now, synthetic))
            # Trim to last 16 min
            cutoff = now - 960
            st["ticks"] = [(t, v) for t, v in st["ticks"] if t > cutoff]
            # Signal that new data is available for this coin
            st["_tick_pending"] = True
            # Signal MM requote engine
            if self.mm_mode and coin in self.mm_state:
                self.mm_state[coin]["requote_pending"] = True

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
        mode_str = "MARKET MAKER" if self.mm_mode else "DIRECTIONAL"
        print(f"🚀 Crypto 15m Agent — {mode_str} — {coins_str}")
        if self.mm_mode:
            print(f"   Mode: MM | edge:{MM_EDGE_C}c | requote:{MM_REQUOTE_THRESHOLD_C}c | settle_guard:{MM_SETTLE_GUARD_SEC}s | contracts:{MM_MAX_CONTRACTS}")
            print(f"   Safety: reconcile every {MM_RECONCILE_INTERVAL_SEC}s | max 2 resting/market | cancel-on-fill | cancel-on-rotate")
        else:
            print(f"   Mode: Directional tiered entry (rollback: git checkout v1-directional-tiered)")
        for _coin, _cfg in BRTI_COIN_CONFIG.items():
            print(f"   {_coin}: σ/sec={_cfg.get('sigma_per_sec', '?')} | hard_stop:{_cfg['stop_loss_hard_c']}c | TP:{_cfg['take_profit_c']}c")
        print(f"   Source: synthetic (Coinbase+Kraken+Bitstamp+Gemini WebSockets)")
        print(f"   DRY_RUN: {DRY_RUN} | MM_MODE: {self.mm_mode}")

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
        if self.mm_mode:
            asyncio.create_task(self.mm_requote_loop())
            asyncio.create_task(self.mm_safety_reconcile_loop())
        brti_coins_str = "+".join(BRTI_COIN_CONFIG.keys())
        print(f"📡 Synthetic index feeds launching ({brti_coins_str}, 4 exchanges) + fast flip loop + {'MM quoting' if self.mm_mode else 'directional entry'}")

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
