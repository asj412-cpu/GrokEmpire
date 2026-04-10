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

# Strategy selector — mutually exclusive
#   "fade"          → existing tiered fade strategy (min 0-10, entry 1-49c)
#   "late_favorite" → buy the favored side late in cycle (per-coin band + min-rem)
STRATEGY = os.getenv('STRATEGY', 'fade').lower()
if STRATEGY not in ("fade", "late_favorite"):
    raise SystemExit(f"Invalid STRATEGY={STRATEGY!r}; must be 'fade' or 'late_favorite'")

# Per-coin late-favorite config (5-day backtest 2026-04-08, net of fees+slippage)
# {lo, hi} = entry cost band in cents; min_rem = trigger when this many minutes remain
# HYPE excluded — every config loses on 5-day data (regime broken)
COIN_LATE_FAV_CONFIG = {
    "BTC":  {"lo": 65, "hi": 85, "min_rem": 3},   # +587c, 84% WR, 76 trades
    "ETH":  {"lo": 60, "hi": 80, "min_rem": 5},   # +725c, 78% WR, 127 trades
    "SOL":  {"lo": 60, "hi": 80, "min_rem": 5},   # +601c, 76% WR, 144 trades
    "XRP":  {"lo": 80, "hi": 92, "min_rem": 5},   # +436c, 92% WR, 92 trades
    "BNB":  {"lo": 65, "hi": 95, "min_rem": 6},   # +857c, 87% WR, 292 trades
    "DOGE": {"lo": 75, "hi": 85, "min_rem": 6},   # +624c, 89% WR, 90 trades
    # HYPE: skip
}

# Tiered entry strategy
# Tier 1: min 0-7 of cycle (cycle_sec 0-420 = 8-15 min remaining) → entry 1-20c
# Tier 2: min 7-10 of cycle (cycle_sec 420-600 = 5-8 min remaining) → entry 20-35c
# (T2 max lowered from 49→35: live data showed 36-49c range was 36% WR, −$22/5d)
TIER1_MAX_CYCLE_SEC = 420   # min 7
TIER1_ENTRY_LOW = 1
TIER1_ENTRY_HIGH = 20
TIER2_MAX_CYCLE_SEC = 600   # min 10
TIER2_ENTRY_LOW = 20
TIER2_ENTRY_HIGH = 35

# Legacy single-tier constants kept for backwards compat in some helpers
ENTRY_LOW = TIER1_ENTRY_LOW
ENTRY_HIGH = TIER2_ENTRY_HIGH

COOLDOWN_SEC = 60  # cooldown between fills per ticker (resets at cycle boundary)

# Per-coin signal direction. "fade" = mean-reversion (default), "trend" = follow majority.
# HYPE flipped to trend on 2026-04-06 — backtest shows trend WR 51.9% vs fade 48.1% over last 5 days
COIN_SIGNAL_MODE = {
    "BTC": "fade",
    "ETH": "trend",
    "SOL": "fade",
    "XRP": "fade",
    "BNB": "fade",
    "HYPE": "trend",
    "DOGE": "fade",
}

# Per-coin fade threshold/window overrides (data-driven from 5-day backtest 2026-04-08)
# BTC: 7/10 fade — 59% WR at 162 trades, +42.4c avg (vs 6/10's 53% WR, +35.6c)
# ETH: 5/7 fade — 58% WR at 190 trades, +41.6c avg (vs 6/10's 53% WR, +36.8c)
# DOGE: 5/7 fade — 55% WR at 173 trades, +38.6c avg (vs 6/10's 52% WR, +35.9c)
# Others stay at 6/10 baseline. Re-evaluate 2026-04-10.
COIN_FADE_CONFIG = {
    "BTC":  {"thresh": 7, "window": 10},
    "ETH":  {"thresh": 6, "window": 10},
    "SOL":  {"thresh": 6, "window": 10},
    "XRP":  {"thresh": 6, "window": 10},
    "BNB":  {"thresh": 6, "window": 10},
    "HYPE": {"thresh": 6, "window": 10},
    "DOGE": {"thresh": 5, "window": 7},
}

ROLLOVER_GUARD_SEC = 5     # first 5s of each new cycle: skip evals until tickers refresh
MIN_TICKER_LIFE_SEC = 60   # only subscribe to markets with >60s until close (avoid about-to-settle)
FADE_THRESHOLD = 6  # out of 10 rolling cycles
FADE_WINDOW = 10
STREAK_BONUS_LEN = 5  # 5-in-a-row streak adds +1 contract
BASE_CONTRACTS = 2   # base contracts per market
MAX_CONTRACTS_PER_MARKET = 3  # max with streak bonus
DRIFT_MIN_DELTA = 2  # minimum price drop (cents) before cancel/repost

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
            except Exception as e:
                print(f"  Open market lookup error {coin}: {e}")

        # Reset cooldowns and avg-down state on cycle rotation
        if rotated:
            self.last_buy_ts.clear()
            self.last_buy_price.clear()

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
            count = int(float(msg.get("count_fp") or msg.get("count") or 0))
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
                print(f"  ✅ Bought! Now {self.ticker_contracts[ticker]} contracts @ {cost_price}c (HOLD to settlement) | cooldown {COOLDOWN_SEC}s")

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
        """Router: dispatch to the active strategy's evaluator."""
        if STRATEGY == "fade":
            await self._evaluate_fade(ticker)
        elif STRATEGY == "late_favorite":
            await self._evaluate_late_favorite(ticker)

    async def _evaluate_late_favorite(self, ticker):
        """Late-favorite strategy: at min_rem before close, buy the favored side if cost in band.
        One trade per ticker per cycle. No averaging-down, no cooldown (single fire)."""
        coin = None
        for c, t in self.current_tickers.items():
            if t == ticker:
                coin = c
                break
        if not coin or coin not in COIN_LATE_FAV_CONFIG:
            return

        cfg = COIN_LATE_FAV_CONFIG[coin]
        now = datetime.now()
        cycle_sec = (now.minute % 15) * 60 + now.second  # 0-899

        # Trigger window: from (min_rem * 60) seconds before close until 30s before close
        # min_rem=5 → fire any time between cycle_sec 600 and 870
        entry_threshold_sec = 900 - (cfg["min_rem"] * 60)
        if cycle_sec < entry_threshold_sec or cycle_sec > 870:
            return

        # Rollover guard: ensure ticker is fresh (refreshed in last 60s)
        now_ms = int(now.timestamp() * 1000)
        refreshed_ms = self.ticker_refreshed_ts.get(ticker, 0)
        if now_ms - refreshed_ms > 60_000:
            return

        # One trade per ticker per cycle (last_buy_ts cleared on rotation in _subscribe_open_markets)
        if self.last_buy_ts.get(ticker) is not None:
            return
        if self.ticker_contracts.get(ticker, 0) > 0:
            return
        if ticker in self.resting_buys:
            return  # in-flight order pending

        # Get prices
        prices = self.ws_prices.get(ticker)
        if not prices:
            return
        yes_ask = prices.get("yes_ask")
        yes_bid = prices.get("yes_bid")
        if yes_ask is None or yes_bid is None or yes_ask <= 0 or yes_bid <= 0:
            return

        yes_mid = (yes_bid + yes_ask) / 2
        if yes_mid > 50:
            target_side = "yes"
            cost = yes_ask  # cross the spread to take YES
        else:
            target_side = "no"
            cost = 100 - yes_bid  # cross the spread to take NO

        if not (cfg["lo"] <= cost <= cfg["hi"]):
            return

        # Set in-flight guard BEFORE posting so a second WS tick can't double-fire
        self.last_buy_ts[ticker] = now_ms
        print(f"[{now.strftime('%H:%M:%S')}] {coin} | LATE-FAV {target_side.upper()} 2 @ {cost}c (band {cfg['lo']}-{cfg['hi']}, {cfg['min_rem']}m left)")
        self._post_buy(ticker, coin, target_side, cost, target_contracts=2, count=2)

    async def _evaluate_fade(self, ticker):
        """Called on every WS price update — manage resting buys with tiered window + cooldown."""
        coin = None
        for c, t in self.current_tickers.items():
            if t == ticker:
                coin = c
                break
        if not coin:
            return

        # Compute cycle position
        now = datetime.now()
        minutes_remaining = 15 - (now.minute % 15)
        cycle_sec = (now.minute % 15) * 60 + now.second  # 0-899

        # Tiered entry window
        # Tier 1: cycle_sec 0-420 (min 0-7), entry 1-20c
        # Tier 2: cycle_sec 420-600 (min 7-10), entry 20-49c
        # After cycle_sec 600: no buys
        if cycle_sec <= TIER1_MAX_CYCLE_SEC:
            tier_low, tier_high, tier_label = TIER1_ENTRY_LOW, TIER1_ENTRY_HIGH, "T1"
            in_window = True
        elif cycle_sec <= TIER2_MAX_CYCLE_SEC:
            tier_low, tier_high, tier_label = TIER2_ENTRY_LOW, TIER2_ENTRY_HIGH, "T2"
            in_window = True
        else:
            tier_low, tier_high, tier_label = 0, 0, "OUT"
            in_window = False

        # Rollover guard: skip first ROLLOVER_GUARD_SEC of new cycle until ticker is refreshed
        if cycle_sec < ROLLOVER_GUARD_SEC:
            in_window = False
        else:
            # Also skip if this ticker hasn't been refreshed in the last 60s
            # (settlement_check_loop refreshes ~every 30s, so 60s is generous)
            now_ms = int(now.timestamp() * 1000)
            refreshed_ms = self.ticker_refreshed_ts.get(ticker, 0)
            if now_ms - refreshed_ms > 60_000:
                in_window = False

        # Get target contract count from fade signal
        fade_signal, target_contracts = self.get_fade_signal(self.settlement_history[coin], coin=coin)

        existing = self.ticker_contracts.get(ticker, 0)
        at_target = existing >= target_contracts

        # Cooldown check
        cooldown_active = False
        last_ts = self.last_buy_ts.get(ticker)
        if last_ts is not None:
            elapsed_sec = (int(now.timestamp() * 1000) - last_ts) / 1000
            if elapsed_sec < COOLDOWN_SEC:
                cooldown_active = True

        # Price check
        prices = self.ws_prices.get(ticker)
        if not prices:
            return
        yes_ask = prices.get("yes_ask")
        yes_bid = prices.get("yes_bid")
        if yes_ask is None or yes_bid is None:
            return
        yes_cost = yes_ask
        no_cost = 100 - yes_bid if yes_bid is not None else None
        if no_cost is None:
            return

        target_side = None
        market_price = None
        if fade_signal == "buy_yes":
            target_side = "yes"
            market_price = yes_cost
        elif fade_signal == "buy_no":
            target_side = "no"
            market_price = no_cost

        resting = self.resting_buys.get(ticker)

        # Cancel resting buy if conditions changed
        if resting and (not in_window or at_target or not fade_signal or cooldown_active):
            reason = "window closed" if not in_window else \
                     ("target reached" if at_target else
                      ("cooldown active" if cooldown_active else "no signal"))
            self._cancel_order(resting["order_id"], ticker, reason)
            return

        if not fade_signal or not in_window or at_target or not target_side or cooldown_active:
            return

        in_range = tier_low <= market_price <= tier_high

        # Average-down enforcement: subsequent buys must be at or below the last fill price
        last_fill_price = self.last_buy_price.get(ticker)
        if last_fill_price is not None and market_price > last_fill_price:
            # Cancel any resting buy that would chase price up
            if resting:
                self._cancel_order(resting["order_id"], ticker, f"price climbed above last fill ({market_price}c > {last_fill_price}c)")
            return

        if resting:
            if in_range and (resting["price"] - market_price) >= DRIFT_MIN_DELTA:
                self._cancel_order(resting["order_id"], ticker, f"drift {resting['price']}c→{market_price}c")
                self._post_buy(ticker, coin, target_side, market_price, target_contracts)
            elif not in_range:
                self._cancel_order(resting["order_id"], ticker, f"left {tier_label} range")
        else:
            if in_range:
                self._post_buy(ticker, coin, target_side, market_price, target_contracts)

    def _post_buy(self, ticker, coin, side, price, target_contracts, count=1):
        """Post a limit buy order. count=number of contracts in this single order."""
        client_order_id = f"fade-{side}-{ticker}-{int(time.time())}"
        coin_window = COIN_FADE_CONFIG.get(coin, {}).get("window", FADE_WINDOW)
        hist = self.settlement_history.get(coin, [])[-coin_window:]
        yes_ct = sum(1 for r in hist if r == "yes")
        fade_str = f"{yes_ct}Y/{coin_window - yes_ct}N"
        decision = f"BUY {side.upper()}"
        existing = self.ticker_contracts.get(ticker, 0)

        print(f"[{datetime.now().strftime('%H:%M:%S')}] {coin} | {decision} {count} @ {price}c | held:{existing}/{target_contracts} | Fade: {fade_str}")

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
        elif DRY_RUN:
            self.resting_buys[ticker] = {"order_id": client_order_id, "side": side, "price": price, "coin": coin}
            print(f"  📄 PAPER: → buy {side} @ {price}c (HOLD to settlement)")

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
                    cw = COIN_FADE_CONFIG.get(coin_name, {}).get("window", FADE_WINDOW)
                    hist = self.settlement_history[coin_name][-cw:]
                    yes_ct = sum(1 for r in hist if r == "yes")
                    print(f"  📊 {coin_name} settled {result.upper()} → {yes_ct}Y/{cw - yes_ct}N")
        except Exception:
            pass

    def get_fade_signal(self, history, coin=None):
        """Returns (signal, num_contracts).
        signal: 'buy_yes' / 'buy_no' / None
        num_contracts: BASE_CONTRACTS, +1 if 5-streak aligns with signal

        Per-coin mode (COIN_SIGNAL_MODE):
        - 'fade' (default): mean-reversion — buy opposite of majority
        - 'trend': follow majority — buy with the trend
        """
        cfg = COIN_FADE_CONFIG.get(coin, {"thresh": FADE_THRESHOLD, "window": FADE_WINDOW}) if coin else {"thresh": FADE_THRESHOLD, "window": FADE_WINDOW}
        window = cfg["window"]
        thresh = cfg["thresh"]
        if len(history) < window:
            return None, 0
        recent = history[-window:]
        yes_count = sum(1 for r in recent if r == "yes")
        no_count = window - yes_count

        mode = COIN_SIGNAL_MODE.get(coin, "fade") if coin else "fade"

        signal = None
        if mode == "fade":
            if no_count >= thresh:
                signal = "buy_yes"
            elif yes_count >= thresh:
                signal = "buy_no"
        else:  # trend
            if yes_count >= thresh:
                signal = "buy_yes"
            elif no_count >= thresh:
                signal = "buy_no"

        if not signal:
            return None, 0

        # Streak bonus check (aligned with signal mode)
        if len(history) >= STREAK_BONUS_LEN:
            last_n = history[-STREAK_BONUS_LEN:]
            streak_side = None
            if mode == "fade":
                if all(r == "no" for r in last_n):
                    streak_side = "buy_yes"
                elif all(r == "yes" for r in last_n):
                    streak_side = "buy_no"
            else:  # trend
                if all(r == "yes" for r in last_n):
                    streak_side = "buy_yes"
                elif all(r == "no" for r in last_n):
                    streak_side = "buy_no"
            if streak_side == signal:
                return signal, min(BASE_CONTRACTS + 1, MAX_CONTRACTS_PER_MARKET)

        return signal, BASE_CONTRACTS

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

                # FLIP SELL DISABLED — live test revealed math flaw:
                # When held side bid < 50c, opp ask > 50c (always sums to ~100)
                # so flip-buying opp has minimal upside. Was burning capital.
                # self._check_flip_sell_window()

                # Balance ratchet
                balance = self.get_balance() or self.last_balance
                if balance > self.current_cash_floor:
                    new_floor = balance * RATCHET_PERCENT
                    if new_floor > self.current_cash_floor:
                        self.current_cash_floor = new_floor
                self.last_balance = balance

                # Log status every cycle
                minutes_remaining = 15 - (datetime.now().minute % 15)
                if STRATEGY == "fade":
                    active = []
                    for coin in COINS:
                        sig, n = self.get_fade_signal(self.settlement_history[coin], coin=coin)
                        if sig:
                            cw = COIN_FADE_CONFIG.get(coin, {}).get("window", FADE_WINDOW)
                            hist = self.settlement_history[coin][-cw:]
                            yes_ct = sum(1 for r in hist if r == "yes")
                            bonus = "+1" if n > BASE_CONTRACTS else ""
                            mode_tag = "T" if COIN_SIGNAL_MODE.get(coin) == "trend" else ""
                            active.append(f"{coin}{mode_tag}:{sig.replace('buy_','').upper()}{bonus}({yes_ct}Y/{cw-yes_ct}N)")
                    status = "Fades: " + (" | ".join(active) if active else "none")
                else:  # late_favorite
                    armed = []
                    for coin, cfg in COIN_LATE_FAV_CONFIG.items():
                        ticker = self.current_tickers.get(coin)
                        held = self.ticker_contracts.get(ticker, 0) if ticker else 0
                        fired = self.last_buy_ts.get(ticker) is not None if ticker else False
                        if held or fired:
                            armed.append(f"{coin}:done")
                        elif minutes_remaining <= cfg["min_rem"]:
                            armed.append(f"{coin}:armed({cfg['lo']}-{cfg['hi']}c)")
                        else:
                            armed.append(f"{coin}:wait{cfg['min_rem']}m")
                    status = "LateFav: " + " | ".join(armed)
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Min left: {minutes_remaining} | {status} | WS: {'✓' if self.ws_connected else '✗'}")

            except Exception as e:
                print(f"Settlement check error: {e}")

            # Sleep 30s, but never past the next 15m cycle boundary (refresh promptly on rollover)
            now2 = datetime.now()
            cycle_sec_now = (now2.minute % 15) * 60 + now2.second
            sec_to_boundary = max(1, 900 - cycle_sec_now + 1)
            await asyncio.sleep(min(30, sec_to_boundary))

    def _check_flip_sell_window(self):
        """At minute 11 (4 min remaining), flip held positions if losing.

        Backtest showed: if held YES is still <50c at min 11, contract has 59-100%
        chance of settling NO. Flipping to NO converts likely loss into likely win.

        Logic:
        - Only fires once per ticker (tracked via self.flipped_tickers)
        - Only fires when minutes_remaining == 4 (min 11 of cycle)
        - For each held position, check current ask. If ask < 50c → flip via two orders:
          1. Sell our held position (close)
          2. Buy opposite side at current ask
        """
        minutes_remaining = 15 - (datetime.now().minute % 15)
        if minutes_remaining != 4:
            return
        if not self.client or DRY_RUN:
            return

        if not hasattr(self, 'flipped_tickers'):
            self.flipped_tickers = set()

        for ticker, count in list(self.ticker_contracts.items()):
            if count <= 0 or ticker in self.flipped_tickers:
                continue
            # Get current prices from WS cache
            prices = self.ws_prices.get(ticker)
            if not prices:
                continue
            yes_ask = prices.get("yes_ask")
            yes_bid = prices.get("yes_bid")
            if yes_ask is None or yes_bid is None:
                continue

            # Determine our position side from positions dict
            position = self.positions.get(ticker, [])
            if not position:
                continue
            held_side = position[0].get("side", "yes")

            # Compute the cost of our held side at current bid (what we'd recover by selling)
            if held_side == "yes":
                our_ask = yes_ask  # cost of buying more YES
                our_bid = yes_bid  # what we'd get for selling YES
                opp_side = "no"
                opp_ask = 100 - yes_bid  # cost of buying NO
            else:
                our_ask = 100 - yes_bid  # cost of buying more NO
                our_bid = 100 - yes_ask  # what we'd get for selling NO
                opp_side = "yes"
                opp_ask = yes_ask

            # Flip only when:
            # 1. Our held side's value (bid) is LOW (<50c) → we're losing on this position
            # 2. AND the opposite side has room to grow (opp_ask <50c) → flip can profit
            # If opp_ask is >=50c, the flip doesn't have enough upside to be worth it
            if our_bid >= 50:
                continue  # we're not losing — keep the position
            if opp_ask >= 50:
                continue  # opposite side too expensive — no upside in flipping

            print(f"[FLIP @ min 11] {ticker} | held {count}x {held_side.upper()} | our ask {our_ask}c | flipping to {opp_side.upper()}")

            try:
                # Step 1: Sell our held position
                sell_id = f"flip-sell-{ticker}-{int(time.time())}"
                self.client.create_order(
                    ticker=ticker,
                    client_order_id=sell_id,
                    side=held_side,
                    action="sell",
                    count=count,
                    type="limit",
                    yes_price=our_bid if held_side == "yes" else None,
                    no_price=our_bid if held_side == "no" else None,
                )
                print(f"  → Sold {count}x {held_side.upper()} @ {our_bid}c")

                # Step 2: Buy opposite side
                buy_id = f"flip-buy-{ticker}-{int(time.time())}"
                self.client.create_order(
                    ticker=ticker,
                    client_order_id=buy_id,
                    side=opp_side,
                    action="buy",
                    count=count,
                    type="limit",
                    yes_price=opp_ask if opp_side == "yes" else None,
                    no_price=opp_ask if opp_side == "no" else None,
                )
                print(f"  → Bought {count}x {opp_side.upper()} @ {opp_ask}c")

                self.flipped_tickers.add(ticker)
            except Exception as e:
                print(f"  → Flip error: {e}")

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
        print(f"🚀 Crypto 15m Agent — STRATEGY={STRATEGY} — {coins_str}")
        if STRATEGY == "fade":
            print(f"   Signal: per-coin fade | T1: min 0-7 @ {TIER1_ENTRY_LOW}-{TIER1_ENTRY_HIGH}c | T2: min 7-10 @ {TIER2_ENTRY_LOW}-{TIER2_ENTRY_HIGH}c")
            per_coin = " ".join(
                f"{c}={COIN_FADE_CONFIG[c]['thresh']}/{COIN_FADE_CONFIG[c]['window']}{COIN_SIGNAL_MODE.get(c,'fade')[0].upper()}"
                for c in COINS
            )
            print(f"   Per-coin: {per_coin}")
            print(f"   Cooldown: {COOLDOWN_SEC}s/coin | Base: {BASE_CONTRACTS}/mkt (+1 on {STREAK_BONUS_LEN}-streak, max {MAX_CONTRACTS_PER_MARKET}) | Hold to settlement")
        else:  # late_favorite
            print(f"   Signal: late-favorite | one buy/cycle/coin | hold to settlement")
            per_coin = " ".join(
                f"{c}={cfg['lo']}-{cfg['hi']}c@{cfg['min_rem']}m"
                for c, cfg in COIN_LATE_FAV_CONFIG.items()
            )
            print(f"   Per-coin: {per_coin}")
            print(f"   Excluded: HYPE (no profitable config in backtest)")
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
