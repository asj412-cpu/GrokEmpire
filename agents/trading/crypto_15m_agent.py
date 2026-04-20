"""
Crypto 15m Agent — BRTI Market Maker + Kalshi WebSocket
========================================================
Trades BTC, ETH, and SOL 15-minute Kalshi markets using a synthetic BRTI index
built from real-time WebSocket feeds (Coinbase, Kraken, Bitstamp, Gemini, Crypto.com).

Hybrid agent: old proven passive A-S MM for BTC/ETH + SOL directional sniper.
CF RTI engine integrated for orderbook-based strike fallback.

Mode: MM_MODE=true  -> Probability-based 2-sided quoting (market maker)
      MM_MODE=false -> Directional tiered entry (legacy, tag: v1-directional-tiered)

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

# Import CF RTI engine for orderbook-based index computation
from agents.trading.cf_rti_engine import CFRealTimeIndex

load_dotenv(override=True)

# ─── Config ───
BASE_CASH_FLOOR = 40.0
RATCHET_PERCENT = 0.80
DRY_RUN = os.getenv('DRY_RUN', 'false').lower() == 'true'
MM_MODE = os.getenv('MM_MODE', 'false').lower() == 'true'

# ─── Market-Making Config ───
MM_EDGE_C = 7                  # edge below fair value per side (cents) — widened from 3, adverse selection was eating us
MM_REQUOTE_THRESHOLD_C = 5     # re-quote if model moved >=5c (was 2c, caused churn)
MM_SETTLE_GUARD_SEC = 60       # cancel all quotes 60s before settlement
MM_MAX_CONTRACTS = 1           # max contracts per quote side
MM_QUOTE_MIN_C = 15            # don't quote below 15c — extreme prices = pure adverse selection
MM_QUOTE_MAX_C = 88            # max 88c — anything higher leaves <7c profit after fees on TP sell
MM_RECONCILE_INTERVAL_SEC = 10 # safety reconciliation frequency
MM_SIGMA = {"BTC": 2.20, "ETH": 0.071, "SOL": 0.015, "XRP": 0.0003, "DOGE": 0.0001, "BNB": 0.09, "HYPE": 0.009}   # calibrated sigma/sec from backtest
MM_SMOOTHING = 0.55            # CF BRTI 1-min average smoothing factor

# ─── Avellaneda-Stoikov MM Parameters ───
MM_GAMMA = {"BTC": 0.5, "ETH": 0.5, "SOL": 0.5, "XRP": 0.5, "DOGE": 0.5, "BNB": 0.5, "HYPE": 0.5}        # risk aversion — 0.3 filled to max every cycle, wider spreads = fewer but better fills
MM_KAPPA_DEFAULT = 0.5                        # fills/sec bootstrap — 0.02 made spread too wide, only 1 side quoted
MM_KAPPA_WINDOW_SEC = 60                     # rolling window for kappa estimation
MM_SPREAD_FLOOR_C = 3                        # minimum half-spread per side (cents)
MM_MAX_INVENTORY = {"BTC": 8, "ETH": 8, "SOL": 8, "XRP": 3, "DOGE": 3, "BNB": 3, "HYPE": 3}     # max net contracts per coin — sniper coins capped at 3


def get_tiered_max_inventory(cycle_sec: float, max_inv_cap: int) -> int:
    """Return inventory cap based on how far into the 15-min cycle we are.
    Ramps up as we get deeper into the cycle (more info, less adverse-selection risk),
    then drops to 0 near settlement to avoid last-second gamma exposure.
      0-120s : 4  (42-58c bounds protect against AS, let model accumulate/unwind)
      120-300s: 4  (40-60c round trip zone — room to work both sides)
      300-600s: 6  (mid-cycle main book)
      600-840s: 8  (full book — direction established)
      840s+   : 0  (settle guard — belt-and-suspenders)
    """
    if cycle_sec < 120:
        return min(4, max_inv_cap)
    elif cycle_sec < 300:
        return min(4, max_inv_cap)
    elif cycle_sec < 600:
        return min(6, max_inv_cap)
    elif cycle_sec < 840:
        return min(max_inv_cap, 8)
    else:
        return 0


def get_tiered_price_bounds(cycle_sec: float) -> tuple:
    """Return (min_price, max_price) for quotes based on cycle phase.
    No hard bounds — the A-S model + 80/20 suppression handles adverse selection.
    """
    return 1, 99


def get_tiered_edge(cycle_sec: float) -> int:
    """Consistent tight edge — price bounds handle AS protection, not the edge."""
    if cycle_sec < 120:
        return 3
    elif cycle_sec < 300:
        return 4
    else:
        return 5


def get_tiered_contracts(cycle_sec: float) -> int:
    """1 contract everywhere — speed makes multi-contract unnecessary."""
    return 1


def _norm_cdf(x):
    """Standard normal CDF without scipy dependency."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2)))


def _norm_pdf(x):
    """Standard normal PDF."""
    return math.exp(-0.5 * x * x) / math.sqrt(2 * math.pi)

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
        "stop_loss_hard_c": 28,        # hard stop: max loss regardless (emergency exit, go flat) — raised from 20
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
        "mm_edge_c": 10,               # MM edge per side (cents) — widened from 7 for BTC adverse selection
        "mm_settle_guard_sec": 90,     # cancel MM quotes 90s before settlement (BTC: wider guard)
        "mm_momentum_threshold": 0.15, # loosened for sniper — BTC moves $1-5/10s normally, 0.5 too strict
        "ws_pairs": {"coinbase": "BTC-USD", "kraken": "XBT/USD", "bitstamp": "btcusd", "gemini": "BTCUSD", "crypto_com": "BTC_USD"},
        "btc_distance_sniper": True,   # BTC distance-based momentum sniper (probationary)
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
        "mm_edge_c": 7,                # MM edge per side (cents)
        "mm_settle_guard_sec": 60,     # cancel MM quotes 60s before settlement (ETH: keep current)
        "mm_momentum_threshold": 0.08, # loosened for sniper (was 0.25)
        "ws_pairs": {"coinbase": "ETH-USD", "kraken": "ETH/USD", "bitstamp": "ethusd", "gemini": "ETHUSD", "crypto_com": "ETH_USD"},
        "btc_distance_sniper": True,   # Same scalp logic as BTC
        "sniper_position_size": 1,     # 1 contract max for ETH
    },
    "SOL": {
        "series": "KXSOL15M",
        "flip_cooldown_sec": 90,
        "trailing_stop_c": 5,
        "trailing_stop_far_c": 15,
        "trailing_stop_mid_c": 10,
        "trailing_stop_near_c": 5,
        "trailing_stop_far_dist": 0.50,
        "trailing_stop_mid_dist": 0.25,
        "stop_loss_hard_c": 35,
        "momentum_flip_distance": 0.15,
        "conviction_min_distance": 0.30,
        "conviction_min_cycle_sec": 180,
        "conviction_max_adds": 2,
        "conviction_cooldown_sec": 60,
        "conviction_max_price": 75,
        "take_profit_c": 91,
        "reentry_max_price": 59,
        "tier1_max": 45,
        "tier1_end_sec": 420,
        "tier2_max": 65,
        "tier2_end_sec": 600,
        "tier3_max": 85,
        "entry_contracts": 3,
        "momentum_window": 8,
        "sigma_per_sec": 0.015,           # recalibrated for CF RTI
        "mm_edge_c": 7,
        "mm_settle_guard_sec": 60,
        "mm_momentum_threshold": 0.03,    # loosened from 0.10 — SOL moves in small absolute $ amounts
        "ws_pairs": {"coinbase": "SOL-USD", "kraken": "SOL/USD", "bitstamp": "solusd", "gemini": "SOLUSD", "crypto_com": "SOL_USD"},
        "sol_sniper_mode": True,          # SOL uses directional sniper, not MM
        "probation_disabled": True,       # BTC-only probation — re-enable when profitable
    },
    "XRP": {
        "series": "KXXRP15M",
        "flip_cooldown_sec": 90, "trailing_stop_c": 5, "trailing_stop_far_c": 15,
        "trailing_stop_mid_c": 10, "trailing_stop_near_c": 5,
        "trailing_stop_far_dist": 0.020, "trailing_stop_mid_dist": 0.010,
        "stop_loss_hard_c": 35, "momentum_flip_distance": 0.005,
        "conviction_min_distance": 0.010, "conviction_min_cycle_sec": 180,
        "conviction_max_adds": 2, "conviction_cooldown_sec": 60,
        "conviction_max_price": 75, "take_profit_c": 91, "reentry_max_price": 59,
        "tier1_max": 45, "tier1_end_sec": 420, "tier2_max": 65,
        "tier2_end_sec": 600, "tier3_max": 85,
        "entry_contracts": 1, "momentum_window": 8,
        "sigma_per_sec": 0.0003,   # XRP ~$0.60, 15-min range ~$0.01
        "mm_edge_c": 7, "mm_settle_guard_sec": 60,
        "mm_momentum_threshold": 0.0003,  # loosened for sniper
        "ws_pairs": {"coinbase": "XRP-USD", "kraken": "XRP/USD", "bitstamp": "xrpusd", "gemini": "XRPUSD", "crypto_com": "XRP_USD"},
        "sol_sniper_mode": True,
        "probation_disabled": True,    # BTC-only probation — re-enable when profitable
    },
    "DOGE": {
        "series": "KXDOGE15M",
        "flip_cooldown_sec": 90, "trailing_stop_c": 5, "trailing_stop_far_c": 15,
        "trailing_stop_mid_c": 10, "trailing_stop_near_c": 5,
        "trailing_stop_far_dist": 0.005, "trailing_stop_mid_dist": 0.002,
        "stop_loss_hard_c": 35, "momentum_flip_distance": 0.001,
        "conviction_min_distance": 0.002, "conviction_min_cycle_sec": 180,
        "conviction_max_adds": 2, "conviction_cooldown_sec": 60,
        "conviction_max_price": 75, "take_profit_c": 91, "reentry_max_price": 59,
        "tier1_max": 45, "tier1_end_sec": 420, "tier2_max": 65,
        "tier2_end_sec": 600, "tier3_max": 85,
        "entry_contracts": 1, "momentum_window": 8,
        "sigma_per_sec": 0.0001,   # DOGE ~$0.16, tiny moves
        "mm_edge_c": 7, "mm_settle_guard_sec": 60,
        "mm_momentum_threshold": 0.00008,  # loosened for sniper
        "ws_pairs": {"coinbase": "DOGE-USD", "kraken": "XDG/USD", "bitstamp": "dogeusd", "gemini": "DOGEUSD"},
        "sol_sniper_mode": True,
        "probation_disabled": True,    # BTC-only probation — re-enable when profitable
    },
    "BNB": {
        "series": "KXBNB15M",
        "flip_cooldown_sec": 90, "trailing_stop_c": 5, "trailing_stop_far_c": 15,
        "trailing_stop_mid_c": 10, "trailing_stop_near_c": 5,
        "trailing_stop_far_dist": 5.0, "trailing_stop_mid_dist": 2.0,
        "stop_loss_hard_c": 35, "momentum_flip_distance": 1.0,
        "conviction_min_distance": 2.0, "conviction_min_cycle_sec": 180,
        "conviction_max_adds": 2, "conviction_cooldown_sec": 60,
        "conviction_max_price": 75, "take_profit_c": 91, "reentry_max_price": 59,
        "tier1_max": 45, "tier1_end_sec": 420, "tier2_max": 65,
        "tier2_end_sec": 600, "tier3_max": 85,
        "entry_contracts": 1, "momentum_window": 8,
        "sigma_per_sec": 0.09,   # BNB ~$650, larger $ moves
        "mm_edge_c": 7, "mm_settle_guard_sec": 60,
        "mm_momentum_threshold": 0.1,  # BNB loosened
        "ws_pairs": {"kraken": "BNB/USD", "crypto_com": "BNB_USD"},  # limited US exchange support
        "sol_sniper_mode": True,
        "probation_disabled": True,    # BTC-only probation — re-enable when profitable
    },
    "HYPE": {
        "series": "KXHYPE15M",
        "flip_cooldown_sec": 90, "trailing_stop_c": 5, "trailing_stop_far_c": 15,
        "trailing_stop_mid_c": 10, "trailing_stop_near_c": 5,
        "trailing_stop_far_dist": 0.50, "trailing_stop_mid_dist": 0.20,
        "stop_loss_hard_c": 35, "momentum_flip_distance": 0.10,
        "conviction_min_distance": 0.20, "conviction_min_cycle_sec": 180,
        "conviction_max_adds": 2, "conviction_cooldown_sec": 60,
        "conviction_max_price": 75, "take_profit_c": 91, "reentry_max_price": 59,
        "tier1_max": 45, "tier1_end_sec": 420, "tier2_max": 65,
        "tier2_end_sec": 600, "tier3_max": 85,
        "entry_contracts": 1, "momentum_window": 8,
        "sigma_per_sec": 0.009,   # HYPE ~$30
        "mm_edge_c": 7, "mm_settle_guard_sec": 60,
        "mm_momentum_threshold": 0.01,  # HYPE loosened
        "ws_pairs": {"kraken": "HYPE/USD"},  # very limited exchange support
        "sol_sniper_mode": True,
        "probation_disabled": True,    # BTC-only probation — re-enable when profitable
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
    """Return max entry price based on cycle phase."""
    tier1_end = cfg.get("tier1_end_sec", BRTI_TIER1_END_SEC)
    tier2_end = cfg.get("tier2_end_sec", BRTI_TIER2_END_SEC)
    if cycle_sec < tier1_end:
        return cfg.get("tier1_max", BRTI_TIER1_MAX)
    elif cycle_sec < tier2_end:
        return cfg.get("tier2_max", BRTI_TIER2_MAX)
    else:
        return cfg.get("tier3_max", BRTI_TIER3_MAX)

# Synthetic BRTI — real-time feed from constituent exchange WebSockets
# Volume-weighted median of Coinbase, Kraken, Bitstamp, Gemini, Crypto.com
# Built dynamically for all coins in BRTI_COIN_CONFIG
def _build_brti_exchanges():
    """Build per-exchange WS config that subscribes to ALL BRTI coins' pairs.
    Only include pairs for coins that have a listing on that exchange.
    """
    all_coinbase_pairs = [cfg["ws_pairs"]["coinbase"] for cfg in BRTI_COIN_CONFIG.values() if "coinbase" in cfg["ws_pairs"]]
    all_kraken_pairs = [cfg["ws_pairs"]["kraken"] for cfg in BRTI_COIN_CONFIG.values() if "kraken" in cfg["ws_pairs"]]
    return {
        "coinbase": {
            "url": "wss://ws-feed.exchange.coinbase.com",
            "subscribe_ticker": {"type": "subscribe", "channels": [{"name": "ticker", "product_ids": all_coinbase_pairs}]},
            "subscribe_l2": {"type": "subscribe", "channels": [{"name": "level2_batch", "product_ids": all_coinbase_pairs}]},
        },
        "kraken": {
            "url": "wss://ws.kraken.com",
            "subscribe_trade": {"event": "subscribe", "pair": all_kraken_pairs, "subscription": {"name": "trade"}},
            "subscribe_book": {"event": "subscribe", "pair": all_kraken_pairs, "subscription": {"name": "book", "depth": 25}},
        },
        "bitstamp": {
            "url": "wss://ws.bitstamp.net",
            "channels": [cfg["ws_pairs"]["bitstamp"] for cfg in BRTI_COIN_CONFIG.values() if "bitstamp" in cfg["ws_pairs"]],
        },
        "gemini": {
            "symbols": {coin: cfg["ws_pairs"]["gemini"] for coin, cfg in BRTI_COIN_CONFIG.items() if "gemini" in cfg["ws_pairs"]},
        },
        "crypto_com": {
            "url": "wss://stream.crypto.com/exchange/v1/market",
            "pairs": {coin: cfg["ws_pairs"]["crypto_com"] for coin, cfg in BRTI_COIN_CONFIG.items() if "crypto_com" in cfg["ws_pairs"]},
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
                print("LIVE Kalshi client ready")
                try:
                    bal = self.client.get_balance()
                    self.mm_cycle_balance = float(bal.get("balance", 5000)) / 100.0
                    print(f"  Starting balance: ${self.mm_cycle_balance:.2f}")
                except Exception:
                    self.mm_cycle_balance = 55.0
                # Cancel ALL resting BTC orders on startup — prevent stale TPs from previous instance
                try:
                    resting = self.client.get_orders()
                    canceled = 0
                    for o in [x for x in resting.get("orders", []) if x.get("status") == "resting"]:
                        cid = o.get("client_order_id") or ""
                        if cid.startswith(("btc-", "eth-")):
                            try:
                                self.client.cancel_order(o["order_id"])
                                canceled += 1
                            except Exception:
                                pass
                    if canceled:
                        print(f"  Startup cleanup: canceled {canceled} stale orders")
                except Exception:
                    pass
            except Exception as e:
                print(f"Kalshi client init failed ({e}) - dry-run mode")
        else:
            print("DRY_RUN or missing keys - paper mode")

        self.running = True
        self.log_file = "15m_signals.csv"
        self.current_cash_floor = BASE_CASH_FLOOR
        self.cycle_pnl_history = []  # list of pnl_cents for kill switch
        self.cycle_start_balance = 0.0
        self.trading_disabled = False  # kill switch flag
        self.last_balance = 0.0
        self.mock_balance = 1000.0

        # CF RTI engines (one per coin) — only create for coins with constituent exchange support
        _RTI_SUPPORTED = {"BTC", "ETH", "SOL", "XRP"}
        self.rti_engines = {coin: CFRealTimeIndex(coin) for coin in BRTI_COIN_CONFIG if coin in _RTI_SUPPORTED}

        # Settlement history per coin
        self.settlement_history: Dict[str, list] = {coin: [] for coin in COINS}
        self.last_settled_ticker: Dict[str, str] = {}
        self.history_seeded = False

        # Per-ticker contract count (max 3)
        self.ticker_contracts: Dict[str, int] = {}
        self.ticker_contracts_signed: Dict[str, int] = {}  # SIGNED: +2=YES, -2=NO

        # Per-ticker: held contracts and resting sell orders
        self.positions: Dict[str, list] = defaultdict(list)
        self.resting_sells: Dict[str, int] = {}

        # Resting buy orders — drift with market
        self.resting_buys: Dict[str, dict] = {}

        # Cooldown tracking: ticker -> last buy fill timestamp (ms)
        self.last_buy_ts: Dict[str, int] = {}
        # Last fill price per ticker (for averaging-down enforcement)
        self.last_buy_price: Dict[str, int] = {}

        # Current open market tickers per coin
        self.current_tickers: Dict[str, str] = {}
        # Per-ticker timestamp of last subscribe refresh (ms)
        self.ticker_refreshed_ts: Dict[str, int] = {}

        # Kalshi WS ticker cache: ticker -> {yes_bid, yes_ask, ...}
        self.ws_prices: Dict[str, dict] = {}
        self.ws_connected = False

        # BRTI state per coin
        self.brti_state: Dict[str, dict] = {}
        for _coin in BRTI_COIN_CONFIG:
            self.brti_state[_coin] = {
                "ticks": [],
                "strike": 0.0,
                "direction": "",
                "entry_made": False,
                "held_side": "",
                "last_flip_ts": 0.0,
                "entry_price": 0,
                "peak_value": 0,
                "conviction_adds": 0,
                "last_conviction_ts": 0.0,
                "flip_confirm_ticks": 0,
                "mm_requote_cooldown_until": 0.0,
            }

        # ─── Market-Making State ───
        self.mm_mode = MM_MODE
        self.mm_state: Dict[str, dict] = {}
        for _coin in BRTI_COIN_CONFIG:
            self.mm_state[_coin] = {
                "yes_order_id": None,
                "no_order_id": None,
                "yes_price": 0,
                "no_price": 0,
                "quotes_active": False,
                "requote_pending": False,
                "quoting_in_flight": False,
                "inventory": 0,
                "total_yes_bought": 0,
                "total_no_bought": 0,
                "avg_yes_cost": 0.0,
                "avg_no_cost": 0.0,
                "fill_times": [],
                "yes_fill_times": [],
                "no_fill_times": [],
                "sweep_pause_until": {"yes": 0.0, "no": 0.0},
            }

        # Exchange price feeds for synthetic index
        self.exchange_prices: Dict[str, Dict[str, float]] = defaultdict(dict)
        self.exchange_trades: Dict[str, Dict[str, list]] = defaultdict(lambda: defaultdict(list))
        self.exchange_status: Dict[str, str] = {}

        # Per-coin orderbook state for Coinbase L2
        self.coinbase_books: Dict[str, dict] = {}
        # Per-coin orderbook state for Kraken
        self.kraken_books: Dict[str, dict] = {}

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
                    self._ws_last_msg_ts = time.time()
                    print("Kalshi WS connected")

                    await ws.send(json.dumps({"id": 1, "cmd": "subscribe", "params": {"channels": ["fill", "user_orders"]}}))
                    await self._subscribe_open_markets()

                    while self.running:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=15)
                            self._ws_last_msg_ts = time.time()
                            data = json.loads(msg)
                            await self._handle_ws_message(data)
                        except asyncio.TimeoutError:
                            # No message in 15s — check if WS is alive
                            if time.time() - self._ws_last_msg_ts > 30:
                                print("Kalshi WS STALE — no data in 30s, forcing reconnect")
                                break  # exit inner loop, reconnect
                            # Send a ping to keep alive
                            try:
                                await ws.ping()
                            except Exception:
                                print("Kalshi WS DEAD — ping failed, reconnecting")
                                break
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
                    # Capture strike for BRTI strategy
                    if coin in BRTI_COIN_CONFIG:
                        strike = m.get("floor_strike")
                        if strike:
                            self.brti_state[coin]["strike"] = float(strike)
                            print(f"  {coin} strike: ${self.brti_state[coin]['strike']:,.2f}")
                        else:
                            # CF RTI fallback
                            rti_engine = self.rti_engines.get(coin)
                            if rti_engine:
                                computed = rti_engine.compute()
                                if computed and computed > 0:
                                    self.brti_state[coin]["strike"] = float(computed)
                                    print(f"  [{coin}] strike: ${computed:,.2f} (RTI fallback)")
            except Exception as e:
                print(f"  Open market lookup error {coin}: {e}")

        # Reset on cycle rotation — ONLY reset ONCE per cycle
        current_ticker_set = frozenset(self.current_tickers.values())
        already_rotated = getattr(self, '_rotated_for_tickers', frozenset()) == current_ticker_set
        if rotated and not already_rotated:
            self._rotated_for_tickers = current_ticker_set
            self._last_rotation_tickers = set(self.current_tickers.values())
            self.last_buy_ts.clear()
            self.last_buy_price.clear()
            self.ticker_contracts.clear()
            self.ticker_contracts_signed.clear()
            self.positions.clear()
            # Sync positions from Kalshi API — the ONLY truth
            self._sync_kalshi_positions()
            # Cancel all MM quotes on rotation
            if self.mm_mode:
                asyncio.create_task(self.mm_cancel_all_coins("cycle rotation"))
                if self.client and not DRY_RUN:
                    try:
                        bal = self.client.get_balance()
                        self.mm_cycle_balance = float(bal.get("balance", 5000)) / 100.0
                        print(f"  Cycle balance: ${self.mm_cycle_balance:.2f} | max exposure: ${self.mm_cycle_balance * 0.25:.2f} ({int(self.mm_cycle_balance * 0.25 / 0.50)} contracts)")
                    except Exception:
                        self.mm_cycle_balance = getattr(self, 'mm_cycle_balance', 55.0)
                self.cycle_start_balance = self.mm_cycle_balance
            # Reset BRTI cycle state for all coins
            for _coin in BRTI_COIN_CONFIG:
                st = self.brti_state[_coin]
                st["ticks"] = []
                st["direction"] = ""
                st["entry_made"] = False
                st["held_side"] = ""
                st["last_flip_ts"] = 0
                st["entry_price"] = 0
                st["peak_value"] = 0
                st["conviction_adds"] = 0
                st["last_conviction_ts"] = 0
                st["strike"] = 0.0  # reset for new cycle — will be re-captured immediately below
                # Reset MM state fully on cycle rotation
                if self.mm_mode and _coin in self.mm_state:
                    ms = self.mm_state[_coin]
                    # Only reset if no live Kalshi position — preserve everything if holding
                    _ticker = self.current_tickers.get(_coin, "")
                    _kalshi_pos = self.ticker_contracts.get(_ticker, 0)
                    if _kalshi_pos == 0:
                        ms["inventory"] = 0
                        ms["total_yes_bought"] = 0
                        ms["total_no_bought"] = 0
                        ms["avg_yes_cost"] = 0.0
                        ms["avg_no_cost"] = 0.0
                    # else: keep ALL state from live position
                    ms["fill_times"] = []
                    ms["yes_fill_times"] = []
                    ms["no_fill_times"] = []
                    ms["_sol_pending_orders"] = 0  # reset pending count per cycle
                    ms["_sol_tp_active"] = False
                    ms["_sol_last_order_ts"] = 0
                    ms["_flip_count"] = 0  # reset flip counter per cycle
                    ms["_flattened_this_cycle"] = False  # reset flatten flag
                    ms["_entry_pending"] = False
                    ms["_stopped_this_cycle"] = False
                    ms["_pair_posted"] = False
                    ms["_pair_yes_filled"] = False
                    ms["_pair_no_filled"] = False
                    ms["_sniper_peak_bid"] = 0
                    ms["_fade_posted"] = False
                    ms["_pair_last_adjust_ts"] = 0
                    ms["_pair_filled_peak"] = 0
                    ms["_shadow_fill_ts"] = 0  # reset shadow MM fill timer
                    ms["_shadow_fill_side"] = ""
                    ms["_shadow_yes_price"] = 0
                    ms["_shadow_no_price"] = 0
                # Re-capture strike immediately from the market data we just fetched
                _ticker = self.current_tickers.get(_coin, "")
                if _ticker and self.client:
                    try:
                        _mkt_data = self.client.get_markets(series_ticker=BRTI_COIN_CONFIG[_coin]["series"], status="open", limit=1)
                        for _m in _mkt_data.get("markets", []):
                            _fs = _m.get("floor_strike")
                            if _fs:
                                st["strike"] = float(_fs)
                    except Exception:
                        pass
                print(f"  {_coin} BRTI cycle reset (strike: ${st['strike']:,.2f})")

        if tickers and self.ws_connected:
            # Subscribe to ticker (bid/ask) AND orderbook_delta (depth)
            await self.ws.send(json.dumps({
                "id": 2,
                "cmd": "subscribe",
                "params": {"channels": ["ticker", "orderbook_delta"], "market_tickers": tickers}
            }))
            # Clear orderbooks for new cycle tickers
            if not hasattr(self, 'kalshi_books'):
                self.kalshi_books = {}
            for t in tickers:
                self.kalshi_books[t] = {"yes": {}, "no": {}}
            print(f"Subscribed to {len(tickers)} market tickers (ticker+orderbook): {', '.join(tickers[-3:])}")

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
                # Legacy directional code disabled — BTC sniper handles all trading
                # await self._evaluate_trade(ticker)
                pass

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
            cost_price = price if purchased_side == "yes" else (100 - price)

            order_cid = msg.get("client_order_id", "")
            print(f"  FILL: {action.upper()} {side.upper()} {coin or ticker} {count}x @ {cost_price}c (yes_px={price}c) | raw={msg}")

            if action == "buy":
                # Use Kalshi's post_position as TRUTH
                post_pos = float(msg.get("post_position_fp", "0"))
                self.ticker_contracts[ticker] = int(abs(post_pos))
                self.ticker_contracts_signed[ticker] = int(post_pos)
                self.positions[ticker].append({"side": side, "entry_price": cost_price, "count": count})
                if ticker in self.resting_buys:
                    del self.resting_buys[ticker]
                self.last_buy_ts[ticker] = int(datetime.now().timestamp() * 1000)
                self.last_buy_price[ticker] = cost_price
                print(f"  Bought! Now {self.ticker_contracts[ticker]} contracts @ {cost_price}c (HOLD to settlement)")

                # MM fill detection
                if self.mm_mode and order_cid.startswith("mm-") and coin:
                    asyncio.create_task(self.mm_on_fill(coin, purchased_side, cost_price))

                # SOL sniper fill tracking
                if coin and order_cid.startswith("sol-") and coin in self.mm_state:
                    ms = self.mm_state[coin]
                    if purchased_side == "yes":
                        ms["inventory"] += count
                        ms["total_yes_bought"] += count
                        prev_cost = ms["avg_yes_cost"] * (ms["total_yes_bought"] - count)
                        ms["avg_yes_cost"] = (prev_cost + cost_price * count) / ms["total_yes_bought"]
                    else:
                        ms["inventory"] -= count
                        ms["total_no_bought"] += count
                        prev_cost = ms["avg_no_cost"] * (ms["total_no_bought"] - count)
                        ms["avg_no_cost"] = (prev_cost + cost_price * count) / ms["total_no_bought"]
                    # Decrement pending (order filled)
                    ms["_sol_pending_orders"] = max(0, ms.get("_sol_pending_orders", 0) - 1)
                    print(f"  SOL SNIPER FILL: {purchased_side.upper()} {count}x @ {cost_price}c | inventory={ms['inventory']:+d} | pending={ms['_sol_pending_orders']}")

                # Sniper fill tracking — USE KALSHI post_position AS TRUTH
                is_sniper_fill = (coin == "BTC" and order_cid.startswith("btc-")) or \
                                 (coin == "ETH" and order_cid.startswith("eth-snipe"))
                if is_sniper_fill and coin in self.mm_state:
                    ms = self.mm_state[coin]
                    # Set inventory from Kalshi truth
                    buy_post_pos = float(msg.get("post_position_fp", "0"))
                    ms["inventory"] = int(buy_post_pos)
                    # Track costs for TP/flip calculations
                    if purchased_side == "yes":
                        ms["total_yes_bought"] += count
                        prev_cost = ms["avg_yes_cost"] * (ms["total_yes_bought"] - count)
                        ms["avg_yes_cost"] = (prev_cost + cost_price * count) / ms["total_yes_bought"]
                    else:
                        ms["total_no_bought"] += count
                        prev_cost = ms["avg_no_cost"] * (ms["total_no_bought"] - count)
                        ms["avg_no_cost"] = (prev_cost + cost_price * count) / ms["total_no_bought"]
                    ms["_sol_pending_orders"] = max(0, ms.get("_sol_pending_orders", 0) - 1)
                    # Set held_side from Kalshi post_position sign
                    if buy_post_pos > 0:
                        self.brti_state[coin]["held_side"] = "yes"
                    elif buy_post_pos < 0:
                        self.brti_state[coin]["held_side"] = "no"
                    # Fill-price divergence check
                    entry_limit = ms.get("_entry_limit_price", 0)
                    if entry_limit > 0 and abs(cost_price - entry_limit) > 10:
                        print(f"  BTC FILL DIVERGENCE: expected ~{entry_limit}c, got {cost_price}c "
                              f"(gap={abs(cost_price - entry_limit)}c) -- market may disagree with model")
                    ms["_entry_pending"] = False
                    ms["_entry_fill_ts"] = time.time()
                    # Pair MM fill tracking
                    if order_cid.startswith(f"{coin.lower()[:3]}-pair-y") or order_cid.startswith("btc-pair-y") or order_cid.startswith("eth-pair-y"):
                        ms["_pair_yes_filled"] = True
                        print(f"  {coin} PAIR YES FILL: {count}x @ {cost_price}c | waiting for NO side")
                        if ms.get("_pair_no_filled"):
                            total = ms.get("_pair_yes_price", 0) + ms.get("_pair_no_price", 0)
                            print(f"  {coin} PAIR COMPLETE: YES@{ms.get('_pair_yes_price')}c + NO@{ms.get('_pair_no_price')}c = {total}c → +{100-total}c locked ✓")
                            ms["_pair_posted"] = False
                    elif order_cid.startswith(f"{coin.lower()[:3]}-pair-n") or order_cid.startswith("btc-pair-n") or order_cid.startswith("eth-pair-n"):
                        ms["_pair_no_filled"] = True
                        print(f"  {coin} PAIR NO FILL: {count}x @ {cost_price}c | waiting for YES side")
                        if ms.get("_pair_yes_filled"):
                            total = ms.get("_pair_yes_price", 0) + ms.get("_pair_no_price", 0)
                            print(f"  {coin} PAIR COMPLETE: YES@{ms.get('_pair_yes_price')}c + NO@{ms.get('_pair_no_price')}c = {total}c → +{100-total}c locked ✓")
                            ms["_pair_posted"] = False
                    else:
                        print(f"  {coin} FILL: {purchased_side.upper()} {count}x @ {cost_price}c | kalshi_pos={buy_post_pos:+.0f}")

            elif action == "sell":
                self.resting_sells[ticker] = max(0, self.resting_sells.get(ticker, 0) - count)
                if self.positions.get(ticker):
                    for _ in range(min(count, len(self.positions[ticker]))):
                        self.positions[ticker].pop(0)
                # Use Kalshi's post_position as TRUTH — handles flips correctly
                post_pos = float(msg.get("post_position_fp", "0"))
                self.ticker_contracts[ticker] = int(abs(post_pos))
                self.ticker_contracts_signed[ticker] = int(post_pos)
                # Update held_side from post_position direction
                coin_for_side = None
                for c, t in self.current_tickers.items():
                    if t == ticker:
                        coin_for_side = c
                        break
                if coin_for_side:
                    if post_pos > 0:
                        self.brti_state[coin_for_side]["held_side"] = "yes"
                    elif post_pos < 0:
                        self.brti_state[coin_for_side]["held_side"] = "no"
                    else:
                        self.brti_state[coin_for_side]["held_side"] = ""
                print(f"  Exit filled! Kalshi position: {post_pos:+.0f} ({self.ticker_contracts[ticker]} contracts)")

                # SOL sniper sell tracking — SELL reduces position toward zero
                if coin and BRTI_COIN_CONFIG.get(coin, {}).get("sol_sniper_mode") and coin in self.mm_state:
                    ms = self.mm_state[coin]
                    if purchased_side == "yes":  # sold NO → less NO
                        ms["inventory"] += count
                    else:  # sold YES → less YES
                        ms["inventory"] -= count
                    ms["_sol_tp_active"] = False
                    print(f"  SOL SNIPER SELL: inventory={ms['inventory']:+d}")

                # BTC sniper sell tracking — USE KALSHI post_position AS TRUTH
                is_sniper_sell = (coin == "BTC" and order_cid.startswith("btc-")) or \
                                 (coin == "ETH" and order_cid.startswith("eth-"))
                if is_sniper_sell and coin in self.mm_state:
                    ms = self.mm_state[coin]
                    # Set inventory directly from Kalshi — no manual math
                    ms["inventory"] = int(post_pos) if post_pos >= 0 else int(post_pos)
                    # Determine if this is a complete fill or partial
                    was_flip = order_cid.startswith("btc-flip") or order_cid.startswith("btc-ck") or order_cid.startswith("btc-sl")
                    if was_flip:
                        # Flip: only clear exit_pending when position has CHANGED SIGN
                        pre_sign = ms.get("_pre_flip_sign", 0)
                        if pre_sign == 0 or (pre_sign > 0 and post_pos < 0) or (pre_sign < 0 and post_pos > 0) or post_pos == 0:
                            ms["_exit_pending"] = False  # flip complete or flat
                            ms["_tp_order_id"] = None
                            ms["_peak_kalshi_bid"] = 0  # reset peak for new flipped position
                            # Set entry price for new flipped position from fill price
                            fill_price_c = int(float(msg.get("yes_price_dollars", "0")) * 100)
                            if post_pos > 0:
                                # Now holding YES — entry cost is what we paid for YES
                                ms["avg_yes_cost"] = fill_price_c
                                ms["avg_no_cost"] = 0
                            elif post_pos < 0:
                                # Now holding NO — entry cost is 100 - yes_price (NO cost)
                                ms["avg_no_cost"] = 100 - fill_price_c
                                ms["avg_yes_cost"] = 0
                            print(f"  FLIP COMPLETE: new entry price={fill_price_c if post_pos > 0 else 100-fill_price_c}c side={'YES' if post_pos > 0 else 'NO'}")
                        # else: partial fill, still flipping — keep exit_pending True
                    else:
                        # TP/exit: only clear when flat
                        if post_pos == 0:
                            ms["_exit_pending"] = False
                            ms["_tp_order_id"] = None
                            ms["_entry_pending"] = False
                            ms["_peak_kalshi_bid"] = 0
                            ms["_entry_sma_gap"] = 0
                            ms["_entry_side"] = ""
                            ms["_entry_fill_ts"] = 0
                            # Reset cost tracking — fresh for next entry
                            ms["avg_yes_cost"] = 0.0
                            ms["avg_no_cost"] = 0.0
                            ms["total_yes_bought"] = 0
                            ms["total_no_bought"] = 0
                            ms["inventory"] = 0
                            # Cancel ALL resting orders — prevent stale TPs
                            try:
                                resting = self.client.get_orders(ticker=ticker)
                                for o in [x for x in resting.get("orders", []) if x.get("status") == "resting"]:
                                    if (o.get("client_order_id") or "").startswith(("btc-", "eth-")):
                                        try:
                                            self.client.cancel_order(o["order_id"])
                                        except Exception:
                                            pass
                                if resting.get("orders"):
                                    print(f"  Canceled {len(resting['orders'])} stale orders on flat")
                            except Exception:
                                pass
                        # else: partial fill — keep exit_pending True
                    new_held = "YES" if post_pos > 0 else ("NO" if post_pos < 0 else "FLAT")
                    kalshi_post = post_pos
                    print(f"  BTC SELL FILL: {purchased_side.upper()} {count}x | inv={ms['inventory']:+d} | "
                          f"kalshi_post={kalshi_post} | now_holding={new_held} | "
                          f"{'FLIP' if was_flip else 'EXIT'} | oid={order_cid[:20]}")

                    # Dump probability data on position close
                    if new_held == "FLAT" and hasattr(self, '_prob_data') and coin in self._prob_data:
                        pd = self._prob_data[coin]
                        _now = time.time()
                        hold_secs = _now - pd.get("entry_ts", _now)
                        exit_price = int(float(msg.get("yes_price_dollars", "0")) * 100)
                        pnl = exit_price - pd["entry_price"] if pd["entry_side"] == "yes" else (100 - exit_price) - pd["entry_price"]
                        print(f"  PROB SUMMARY {coin}: entry={pd['entry_price']:.0f}c peak={pd['peak_bid']}c "
                              f"exit={exit_price}c pnl={pnl:+.0f}c held={hold_secs:.0f}s "
                              f"drops={len(pd['drops_from_peak'])} ticks={len(pd['ticks'])} "
                              f"side={pd['entry_side']} dist@entry=${pd['entry_dist']:.0f}")
                        if pd["drops_from_peak"]:
                            for d in pd["drops_from_peak"]:
                                ts, peak, current, bucket, brti_t, dist, min_l = d
                                recovered = "?" # will analyze from ticks later
                                print(f"    DROP: peak={peak}c drop={bucket}c brti_toward={brti_t:.0%} "
                                      f"dist=${dist:.0f} min_left={min_l:.0f}")
                        del self._prob_data[coin]

        elif msg_type == "orderbook_snapshot":
            msg = data.get("msg", {})
            ticker = msg.get("market_ticker", "")
            if ticker:
                if not hasattr(self, 'kalshi_books'):
                    self.kalshi_books = {}
                self.kalshi_books[ticker] = {"yes": {}, "no": {}}
                for ps, qs in (msg.get("yes_dollars_fp") or []):
                    try:
                        p = int(round(float(ps) * 100))
                        self.kalshi_books[ticker]["yes"][p] = float(qs)
                    except (ValueError, TypeError):
                        pass
                for ps, qs in (msg.get("no_dollars_fp") or []):
                    try:
                        p = int(round(float(ps) * 100))
                        self.kalshi_books[ticker]["no"][p] = float(qs)
                    except (ValueError, TypeError):
                        pass

        elif msg_type == "orderbook_delta":
            msg = data.get("msg", {})
            ticker = msg.get("market_ticker", "")
            if ticker and hasattr(self, 'kalshi_books') and ticker in self.kalshi_books:
                side = msg.get("side", "")
                if side in ("yes", "no"):
                    try:
                        price = int(round(float(msg.get("price_dollars", "0")) * 100))
                        delta = float(msg.get("delta_fp", 0))
                        cur = self.kalshi_books[ticker][side].get(price, 0)
                        new_qty = cur + delta
                        if new_qty <= 0:
                            self.kalshi_books[ticker][side].pop(price, None)
                        else:
                            self.kalshi_books[ticker][side][price] = new_qty
                    except (ValueError, TypeError):
                        pass
                # Trigger shadow MM requote on book change
                for c, t in self.current_tickers.items():
                    if t == ticker and BRTI_COIN_CONFIG.get(c, {}).get("eth_shadow_mm"):
                        if c in self.mm_state:
                            self.mm_state[c]["requote_pending"] = True
                        break

        elif msg_type == "subscribed":
            channel = data.get("msg", {}).get("channel", "")
            print(f"  WS subscribed: {channel}")

    async def _evaluate_trade(self, ticker):
        """Evaluate BRTI momentum trade on each price update."""
        await self._evaluate_brti(ticker)

    async def _evaluate_brti(self, ticker):
        """BRTI momentum strategy: all coins in BRTI_COIN_CONFIG."""
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
        needs_eval = not st["direction"]
        if st["direction"] == "flat" and not st["entry_made"] and st["ticks"]:
            latest = st["ticks"][-1][1]
            min_clear = cfg.get("trailing_stop_mid_dist", 20)
            if abs(latest - st["strike"]) >= min_clear:
                needs_eval = True
        if needs_eval and cycle_sec >= momentum_window and len(st["ticks"]) >= 10:
            recent_ticks = [v for _, v in st["ticks"][-10:]]
            if recent_ticks:
                recent_avg = sum(recent_ticks) / len(recent_ticks)
                distance_from_strike = recent_avg - st["strike"]
                min_clear_dist = cfg.get("trailing_stop_mid_dist", 20)

                if abs(distance_from_strike) >= min_clear_dist:
                    st["direction"] = "up" if distance_from_strike > 0 else "down"
                    print(f"[{now.strftime('%H:%M:%S')}] {coin} direction (position): {st['direction']} (index ${recent_avg:,.2f} is ${distance_from_strike:+,.2f} from strike ${st['strike']:,.2f})")
                else:
                    cycle_start_ts = now.timestamp() - cycle_sec
                    start_ticks = [v for t, v in st["ticks"] if cycle_start_ts - 5 <= t <= cycle_start_ts + 10]
                    if start_ticks:
                        start_avg = sum(start_ticks) / len(start_ticks)
                        delta = recent_avg - start_avg
                        min_momentum = min_clear_dist * 0.25
                        if delta > min_momentum:
                            st["direction"] = "up"
                        elif delta < -min_momentum:
                            st["direction"] = "down"
                        else:
                            st["direction"] = "flat"
                        print(f"[{now.strftime('%H:%M:%S')}] {coin} direction (momentum): {st['direction']} (${start_avg:,.2f} -> ${recent_avg:,.2f}, D${delta:+,.2f}, need +/-${min_momentum:,.2f}) | strike: ${st['strike']:,.2f}")
                    else:
                        st["direction"] = "up" if distance_from_strike > 0 else "down"
                        print(f"[{now.strftime('%H:%M:%S')}] {coin} direction (vs strike): {st['direction']} (index ${recent_avg:,.2f} vs strike ${st['strike']:,.2f})")

        # ── Phase 2: Initial entry (tiered pricing) — directional mode only ──
        if not self.mm_mode and st["direction"] and not st["entry_made"] and st["direction"] != "flat":
            flip_cooldown = cfg.get("flip_cooldown_sec", BRTI_FLIP_COOLDOWN_SEC)
            if st["last_flip_ts"] and (time.time() - st["last_flip_ts"]) < flip_cooldown:
                return

            target_side = "yes" if st["direction"] == "up" else "no"

            if st["ticks"]:
                current_brti = st["ticks"][-1][1]
                if target_side == "yes" and current_brti < st["strike"]:
                    return
                if target_side == "no" and current_brti > st["strike"]:
                    return

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

            tier2_end = cfg.get("tier2_end_sec", BRTI_TIER2_END_SEC)
            if cycle_sec >= tier2_end:
                smooth_ticks = [v for t, v in st["ticks"] if t > now.timestamp() - BRTI_SMOOTHING_WINDOW]
                if smooth_ticks:
                    smoothed = sum(smooth_ticks) / len(smooth_ticks)
                    dist_past = smoothed - st["strike"] if target_side == "yes" else st["strike"] - smoothed
                    if dist_past < cfg.get("conviction_min_distance", BRTI_CONVICTION_MIN_DISTANCE):
                        return
                else:
                    return

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
            entry_max = _get_tiered_entry_max(cycle_sec, cfg)
            reentry_max = cfg.get("reentry_max_price", entry_max)
            is_reentry = st["conviction_adds"] > 0 or st["peak_value"] > 0
            max_price = min(reentry_max, entry_max) if is_reentry else entry_max
            total_exposure = sum(self.ticker_contracts.values())
            if total_exposure >= 10:
                return

            tier_label = "T1" if cycle_sec < cfg.get("tier1_end_sec", BRTI_TIER1_END_SEC) else ("T2" if cycle_sec < tier2_end else "T3")
            if 1 <= cost <= max_price:
                st["entry_made"] = True
                st["held_side"] = target_side
                st["entry_price"] = cost
                st["peak_value"] = cost
                print(f"[{now.strftime('%H:%M:%S')}] {coin} BRTI-ENTRY {target_side.upper()} 1 @ {cost}c [{tier_label}<={max_price}c] (dir {st['direction']}, strike ${st['strike']:,.2f})")
                entry_count = cfg.get("entry_contracts", 1)
                self._post_buy(ticker, coin, target_side, cost, target_contracts=entry_count, count=entry_count)
                return

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
                print(f"  -> Buy error: {e}")

    async def _post_buy_async(self, ticker, coin, side, price, count=1):
        """Non-blocking buy."""
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
                print(f"  -> Buy error: {e}")

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
                print(f"  -> {coin} Sell {count}x {side.upper()} ({reason})")
            except Exception as e:
                print(f"  -> {coin} Sell error: {e}")

    def _cancel_order(self, order_id, ticker, reason=""):
        """Cancel a resting order."""
        if self.client and not DRY_RUN:
            try:
                self.client.cancel_order(order_id)
                print(f"  Cancelled {order_id[:20]}... ({reason})")
            except Exception as e:
                print(f"  Cancel error: {e}")
        else:
            print(f"  PAPER cancel {order_id[:20]}... ({reason})")
        if ticker in self.resting_buys:
            del self.resting_buys[ticker]

    # ─── Market-Making Engine ─────────────────────────────────

    def mm_compute_fair_value(self, coin):
        """Compute P(YES) from sBRTI probability model. Returns (yes_fair, no_fair) in cents."""
        st = self.brti_state[coin]
        cfg = BRTI_COIN_CONFIG[coin]
        if st["strike"] <= 0 or not st["ticks"]:
            return 50, 50

        now_ts = time.time()
        cycle_sec = (datetime.now().minute % 15) * 60 + datetime.now().second
        secs_remaining = max(1, 900 - cycle_sec)

        recent = [v for t, v in st["ticks"] if t > now_ts - 10]
        if not recent:
            recent = [st["ticks"][-1][1]]
        smoothed_brti = sum(recent) / len(recent)
        distance = smoothed_brti - st["strike"]

        sigma = cfg.get("sigma_per_sec", MM_SIGMA.get(coin, 2.20))
        effective_sigma = sigma * (secs_remaining ** 0.5) * MM_SMOOTHING
        if effective_sigma <= 0:
            return 50, 50

        p_yes = _norm_cdf(distance / effective_sigma)
        yes_fair = max(1, min(99, round(p_yes * 100)))
        no_fair = 100 - yes_fair
        return yes_fair, no_fair

    def mm_compute_quotes(self, coin):
        """Compute bid prices for both sides (legacy fixed-edge). Returns (yes_bid, no_bid) or None."""
        yes_fair, no_fair = self.mm_compute_fair_value(coin)
        cfg = BRTI_COIN_CONFIG[coin]
        edge = cfg.get("mm_edge_c", MM_EDGE_C)
        yes_bid = yes_fair - edge
        no_bid = no_fair - edge

        if yes_bid + no_bid >= 100:
            return None

        if yes_bid < MM_QUOTE_MIN_C:
            yes_bid = 0
        if yes_bid > MM_QUOTE_MAX_C:
            yes_bid = 0
        if no_bid < MM_QUOTE_MIN_C:
            no_bid = 0
        if no_bid > MM_QUOTE_MAX_C:
            no_bid = 0

        if yes_bid == 0 and no_bid == 0:
            return None
        return yes_bid, no_bid

    def mm_estimate_kappa(self, coin):
        """Estimate order arrival rate kappa (fills/sec) from rolling 60s window."""
        ms = self.mm_state[coin]
        now = time.time()
        cutoff = now - MM_KAPPA_WINDOW_SEC
        ms["fill_times"] = [t for t in ms["fill_times"] if t > cutoff]
        n = len(ms["fill_times"])
        if n < 2:
            return MM_KAPPA_DEFAULT
        window = min(MM_KAPPA_WINDOW_SEC, now - ms["fill_times"][0])
        if window <= 0:
            return MM_KAPPA_DEFAULT
        return max(MM_KAPPA_DEFAULT, n / window)

    def mm_compute_quotes_as(self, coin):
        """Avellaneda-Stoikov quote computation.
        Returns (yes_bid, no_bid) in cents, or None if no valid quote can be placed."""
        st = self.brti_state[coin]
        cfg = BRTI_COIN_CONFIG[coin]
        ms = self.mm_state[coin]

        if st["strike"] <= 0 or not st["ticks"]:
            return None

        now_ts = time.time()
        cycle_sec = (datetime.now().minute % 15) * 60 + datetime.now().second

        settle_guard = cfg.get("mm_settle_guard_sec", MM_SETTLE_GUARD_SEC)
        secs_remaining = max(float(settle_guard), float(900 - cycle_sec))
        T_t = secs_remaining / 900.0

        recent_1s = [v for t, v in st["ticks"] if t > now_ts - 1]
        recent_2s = [v for t, v in st["ticks"] if t > now_ts - 2]
        recent_3s = [v for t, v in st["ticks"] if t > now_ts - 3]
        recent_10s = [v for t, v in st["ticks"] if t > now_ts - 10]
        if not recent_1s:
            recent_1s = [st["ticks"][-1][1]]
        if not recent_2s:
            recent_2s = recent_1s
        if not recent_3s:
            recent_3s = recent_2s
        if not recent_10s:
            recent_10s = recent_3s
        avg_1s = sum(recent_1s) / len(recent_1s)
        avg_2s = sum(recent_2s) / len(recent_2s)
        avg_3s = sum(recent_3s) / len(recent_3s)
        avg_10s = sum(recent_10s) / len(recent_10s)

        # Shadow log every 30s
        if int(now_ts) % 30 == 0 and st.get("_last_window_log", 0) != int(now_ts):
            st["_last_window_log"] = int(now_ts)
            d1 = avg_1s - st["strike"]
            d2 = avg_2s - st["strike"]
            d3 = avg_3s - st["strike"]
            m1 = avg_1s - avg_10s
            m2 = avg_2s - avg_10s
            m3 = avg_3s - avg_10s
            print(f"  {coin} WINDOW: 1s=${avg_1s:,.2f}(D{d1:+,.0f} m{m1:+,.1f}) 2s=${avg_2s:,.2f}(D{d2:+,.0f} m{m2:+,.1f}) 3s=${avg_3s:,.2f}(D{d3:+,.0f} m{m3:+,.1f}) 10s=${avg_10s:,.2f}")

        latest_tick = st["ticks"][-1][1] if st["ticks"] else avg_3s
        spot = latest_tick
        momentum = latest_tick - avg_10s
        momentum_proj = momentum * min(30.0, secs_remaining) * 0.05

        distance = (spot + momentum_proj) - st["strike"]

        sigma_brti = cfg.get("sigma_per_sec", MM_SIGMA.get(coin, 2.20))
        ticks_30s = [v for t, v in st["ticks"] if t > now_ts - 30]
        if len(ticks_30s) >= 5:
            diffs = [ticks_30s[i] - ticks_30s[i-1] for i in range(1, len(ticks_30s))]
            if diffs:
                realized_std = (sum(d*d for d in diffs) / len(diffs)) ** 0.5
                sigma_brti = 0.5 * sigma_brti + 0.5 * realized_std

        effective_sigma = sigma_brti * math.sqrt(secs_remaining) * MM_SMOOTHING
        if effective_sigma <= 0:
            return None

        p_yes = _norm_cdf(distance / effective_sigma)
        s = max(1.0, min(99.0, p_yes * 100.0))

        z = distance / (sigma_brti * math.sqrt(secs_remaining))
        phi_z = _norm_pdf(z)
        sigma_c = 100.0 * phi_z / math.sqrt(secs_remaining)

        ticker = self.current_tickers.get(coin, "")
        real_held = self.ticker_contracts.get(ticker, 0)
        held_side = self.brti_state[coin].get("held_side", "")
        if real_held > 0 and held_side:
            q = real_held if held_side == "yes" else -real_held
        else:
            q = ms["inventory"]
        gamma = MM_GAMMA.get(coin, 0.5)
        kappa = self.mm_estimate_kappa(coin)

        inv_term = q * gamma * (sigma_c ** 2) * T_t
        r_yes = s - inv_term
        r_no = (100.0 - s) + inv_term

        as_delta = gamma * (sigma_c ** 2) * T_t + (2.0 / gamma) * math.log(1.0 + gamma / kappa)
        tiered_floor = float(get_tiered_edge(cycle_sec))
        half = max(tiered_floor, as_delta / 2.0)

        abs_mom = abs(momentum)
        if abs_mom > 0:
            half += abs_mom * 1.5

        yes_bid = int(round(r_yes - half))
        no_bid = int(round(r_no - half))

        # 80/20 suppression
        if secs_remaining <= 180:
            raw_p = _norm_cdf((avg_10s - st["strike"]) / effective_sigma) if effective_sigma > 0 else 0.5
            s_check = max(1.0, min(99.0, raw_p * 100.0))
        else:
            s_check = s
        if s_check >= 80:
            no_bid = 0
        if s_check <= 20:
            yes_bid = 0

        tiered_max = get_tiered_max_inventory(cycle_sec, MM_MAX_INVENTORY.get(coin, 8))
        if tiered_max == 0:
            return None

        if abs(q) >= 6:
            if q > 0:
                yes_bid = 0
            else:
                no_bid = 0

        if yes_bid > 0 and no_bid > 0 and yes_bid + no_bid >= 100:
            return None

        if yes_bid > 0 and ms['total_no_bought'] > 0 and yes_bid + ms['avg_no_cost'] >= 100:
            yes_bid = 0
        if no_bid > 0 and ms['total_yes_bought'] > 0 and no_bid + ms['avg_yes_cost'] >= 100:
            no_bid = 0

        if yes_bid == 0 and no_bid == 0:
            return None

        tier_min, tier_max = get_tiered_price_bounds(cycle_sec)
        if not (tier_min <= yes_bid <= tier_max):
            yes_bid = 0
        if not (tier_min <= no_bid <= tier_max):
            no_bid = 0

        if abs(q) >= 6:
            if q > 0:
                yes_bid = 0
            else:
                no_bid = 0

        if yes_bid == 0 and no_bid == 0:
            return None

        return yes_bid, no_bid

    async def mm_cancel_all_quotes(self, coin, reason=""):
        """Cancel all resting MM orders for a coin."""
        ms = self.mm_state[coin]
        ids_to_cancel = []
        if ms["yes_order_id"]:
            ids_to_cancel.append(ms["yes_order_id"])
        if ms["no_order_id"]:
            ids_to_cancel.append(ms["no_order_id"])

        if ids_to_cancel and self.client and not DRY_RUN:
            try:
                await asyncio.to_thread(self.client.batch_cancel_orders, ids_to_cancel)
            except Exception:
                for oid in ids_to_cancel:
                    try:
                        await asyncio.to_thread(self.client.cancel_order, oid)
                    except Exception:
                        pass

        if ids_to_cancel:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {coin} MM cancel {len(ids_to_cancel)} quotes ({reason})")
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
            return
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
        if ms["quotes_active"]:
            await self.mm_cancel_all_quotes(coin, "requote")

        quotes = self.mm_compute_quotes_as(coin)
        if not quotes:
            return
        yes_bid, no_bid = quotes

        # Taker-fill guard
        mkt = self.ws_prices.get(ticker, {})
        mkt_yes_ask = mkt.get("yes_ask", 0)
        mkt_yes_bid = mkt.get("yes_bid", 0)
        if yes_bid > 0 and mkt_yes_ask > 0 and yes_bid >= mkt_yes_ask:
            yes_bid = mkt_yes_ask - 1
            if yes_bid < MM_QUOTE_MIN_C:
                yes_bid = 0
        no_ask = (100 - mkt_yes_bid) if mkt_yes_bid > 0 else 0
        if no_bid > 0 and no_ask > 0 and no_bid >= no_ask:
            no_bid = no_ask - 1
            if no_bid < MM_QUOTE_MIN_C:
                no_bid = 0
        if yes_bid == 0 and no_bid == 0:
            return

        yes_oid = None
        no_oid = None

        if self.client and not DRY_RUN:
            ts_ms = int(time.time() * 1000)
            cycle_sec = (datetime.now().minute % 15) * 60 + datetime.now().second
            quote_count = get_tiered_contracts(cycle_sec)
            batch = []
            yes_cid = None
            no_cid = None
            if yes_bid > 0:
                yes_cid = f"mm-yes-{ticker}-{ts_ms}"
                batch.append({
                    "ticker": ticker, "client_order_id": yes_cid,
                    "side": "yes", "action": "buy", "count": quote_count,
                    "type": "limit", "yes_price": yes_bid,
                })
            if no_bid > 0:
                no_cid = f"mm-no-{ticker}-{ts_ms}"
                batch.append({
                    "ticker": ticker, "client_order_id": no_cid,
                    "side": "no", "action": "buy", "count": quote_count,
                    "type": "limit", "no_price": no_bid,
                })

            if batch:
                try:
                    result = await asyncio.to_thread(self.client.batch_create_orders, batch)
                    for ro in result.get("orders", []):
                        inner = ro.get("order", ro)
                        cid = inner.get("client_order_id", "")
                        oid = inner.get("order_id", "")
                        if cid == yes_cid:
                            yes_oid = oid
                        elif cid == no_cid:
                            no_oid = oid
                except Exception as e:
                    print(f"  -> {coin} MM batch order error: {e}")
                    if yes_bid > 0:
                        try:
                            r = await asyncio.to_thread(
                                self.client.create_order,
                                ticker=ticker, client_order_id=f"mm-yes-{ticker}-{ts_ms}f",
                                side="yes", action="buy", count=quote_count,
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
                                side="no", action="buy", count=quote_count,
                                type="limit", no_price=no_bid,
                            )
                            no_oid = r.get("order", {}).get("order_id")
                        except Exception:
                            pass

        ms["yes_order_id"] = yes_oid
        ms["no_order_id"] = no_oid
        ms["yes_price"] = yes_bid if yes_oid else 0
        ms["no_price"] = no_bid if no_oid else 0
        ms["quotes_active"] = bool(yes_oid or no_oid)

        yes_fair, no_fair = self.mm_compute_fair_value(coin)
        if ms["quotes_active"]:
            q = ms["inventory"]
            kappa = self.mm_estimate_kappa(coin)
            gamma = MM_GAMMA.get(coin, 0.5)
            cycle_sec = (datetime.now().minute % 15) * 60 + datetime.now().second
            t_min, t_max = get_tiered_price_bounds(cycle_sec)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {coin} MM QUOTE [Stoikov]: YES bid {yes_bid}c / NO bid {no_bid}c | fair:{yes_fair}/{no_fair} | inv={q:+d} | g={gamma} k={kappa:.4f}/s | bounds:[{t_min}-{t_max}c]")

    async def mm_on_fill(self, coin, filled_side, fill_price):
        """Handle MM fill — Avellaneda-Stoikov pure spread capture.
        Both sides stay open; inventory skew nudges quotes toward the flattening direction.
        Positions ride to settlement."""
        ms = self.mm_state[coin]
        now = time.time()

        ms["fill_times"].append(now)

        side_times = ms[f"{filled_side}_fill_times"]
        side_times.append(now)
        side_times[:] = [t for t in side_times if now - t <= 2.0]
        if len(side_times) >= 5:
            print(f"  RAPID FILLS: {len(side_times)} in 2s on {filled_side.upper()} (no pause — bounds protect)")

        if filled_side == "yes":
            ms["inventory"] += 1
            ms["total_yes_bought"] += 1
            prev_cost = ms["avg_yes_cost"] * (ms["total_yes_bought"] - 1)
            ms["avg_yes_cost"] = (prev_cost + fill_price) / ms["total_yes_bought"]
        else:
            ms["inventory"] -= 1
            ms["total_no_bought"] += 1
            prev_cost = ms["avg_no_cost"] * (ms["total_no_bought"] - 1)
            ms["avg_no_cost"] = (prev_cost + fill_price) / ms["total_no_bought"]

        kappa = self.mm_estimate_kappa(coin)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {coin} MM FILL [Stoikov]: {filled_side.upper()} @ {fill_price}c | inventory={ms['inventory']:+d} | k={kappa:.4f}/s")

        if ms["total_yes_bought"] > 0 and ms["total_no_bought"] > 0:
            pairs = min(ms["total_yes_bought"], ms["total_no_bought"])
            spread = 100.0 - ms["avg_yes_cost"] - ms["avg_no_cost"]
            rt_label = "PROFIT" if spread >= 0 else "LOSS"
            print(f"  {rt_label} {coin} ROUND-TRIP: {pairs}x pairs | avg spread captured={spread:.1f}c (YES@{ms['avg_yes_cost']:.1f}c + NO@{ms['avg_no_cost']:.1f}c)")

        coin_inv = abs(ms.get("inventory", 0))
        if coin_inv >= 6:
            q = ms.get("inventory", 0)
            accumulating_side = "no" if q < 0 else "yes"
            oid_key = f"{accumulating_side}_order_id"
            acc_oid = ms.get(oid_key)
            if acc_oid and self.client and not DRY_RUN:
                try:
                    await asyncio.to_thread(self.client.cancel_order, acc_oid)
                except Exception:
                    pass
                ms[oid_key] = None
            print(f"  CAP: {coin} inv={q:+d} (+/-6) — blocked {accumulating_side.upper()}, unwind side live")
            ms["requote_pending"] = True
            return

        ms["requote_pending"] = True

    # ─── SOL Directional Sniper ──────────────────────────────

    async def sol_sniper_tick(self, coin):
        """SOL directional sniper — cheap entries, 2x take profit.

        Phase 1 (min 0-11): Buy < 35c on momentum confirmation. TP at 2x.
        Phase 2 (min 11-14): Confident sniper in 45-85c zone. TP at 2x.
        All orders are limit/maker to minimize fees.
        """
        st = self.brti_state[coin]
        ms = self.mm_state[coin]
        cfg = BRTI_COIN_CONFIG[coin]

        if st["strike"] <= 0 or not st["ticks"]:
            return

        cycle_sec = (datetime.now().minute % 15) * 60 + datetime.now().second
        now_ts = time.time()
        ticker = self.current_tickers.get(coin, "")
        if not ticker or not self.client or DRY_RUN:
            return

        # Current price and momentum
        latest = st["ticks"][-1][1]
        recent_10s = [v for t, v in st["ticks"] if t > now_ts - 10]
        recent_30s = [v for t, v in st["ticks"] if t > now_ts - 30]
        if not recent_10s or not recent_30s:
            return
        avg_10s = sum(recent_10s) / len(recent_10s)
        avg_30s = sum(recent_30s) / len(recent_30s)
        momentum = avg_10s - avg_30s

        # Fair value
        sigma = cfg.get("sigma_per_sec", 0.015)
        secs_remaining = max(60.0, float(900 - cycle_sec))
        effective_sigma = sigma * (secs_remaining ** 0.5) * MM_SMOOTHING
        if effective_sigma <= 0:
            return
        distance = latest - st["strike"]
        p_yes = _norm_cdf(distance / effective_sigma)
        fair_yes = max(1, min(99, int(p_yes * 100)))
        fair_no = 100 - fair_yes

        q = ms.get("inventory", 0)
        max_inv = MM_MAX_INVENTORY.get(coin, 8)

        # --- Pending order tracking (prevents race: fills arriving before cap check) ---
        # Auto-expire stale pending counter if no orders placed recently (30s)
        last_order = ms.get("_sol_last_order_ts", 0)
        if now_ts - last_order > 30:
            ms["_sol_pending_orders"] = 0  # stale, orders must have been cancelled/expired
        pending = ms.get("_sol_pending_orders", 0)
        # Hard cap on concurrent pending orders (prevent runaway)
        if pending >= 2:
            return
        ticker_contracts_now = abs(self.ticker_contracts.get(ticker, 0))
        effective_exposure = max(abs(q), ticker_contracts_now) + pending
        if effective_exposure >= max_inv:
            return  # at or above cap counting pending

        # --- TAKE PROFIT: sell at 2x entry cost (limit order, maker) ---
        if q != 0:
            if q > 0 and ms.get("avg_yes_cost", 0) > 0:
                target_sell = int(ms["avg_yes_cost"] * 2)
                if fair_yes >= target_sell and not ms.get("_sol_tp_active"):
                    try:
                        tp_id = f"sol-tp-{int(now_ts*1000)}"
                        sell_price = max(target_sell, fair_yes - 3)
                        await asyncio.to_thread(self.client.create_order,
                            ticker=ticker, client_order_id=tp_id,
                            side="yes", action="sell", count=abs(q), type="limit",
                            yes_price=sell_price)
                        ms["_sol_tp_active"] = True
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] SOL TP: SELL YES {abs(q)}x @ {sell_price}c "
                              f"(entry@{ms['avg_yes_cost']:.0f}c, 2x={target_sell}c)")
                    except Exception as e:
                        print(f"  SOL TP error: {e}")
                return

            elif q < 0 and ms.get("avg_no_cost", 0) > 0:
                target_sell = int(ms["avg_no_cost"] * 2)
                if fair_no >= target_sell and not ms.get("_sol_tp_active"):
                    try:
                        tp_id = f"sol-tp-{int(now_ts*1000)}"
                        sell_price = max(target_sell, fair_no - 3)
                        await asyncio.to_thread(self.client.create_order,
                            ticker=ticker, client_order_id=tp_id,
                            side="no", action="sell", count=abs(q), type="limit",
                            no_price=sell_price)
                        ms["_sol_tp_active"] = True
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] SOL TP: SELL NO {abs(q)}x @ {sell_price}c "
                              f"(entry@{ms['avg_no_cost']:.0f}c, 2x={target_sell}c)")
                    except Exception as e:
                        print(f"  SOL TP error: {e}")
                return

        ms["_sol_tp_active"] = False

        if abs(q) >= max_inv:
            return

        if cycle_sec >= 840:
            return

        # Cooldown: don't spam orders
        if now_ts - ms.get("_sol_last_order_ts", 0) < 3:
            return

        # --- PHASE 1: Minutes 0-11 — cheap shots (<40c, loosened from 35c) ---
        if cycle_sec < 660:
            min_momentum = cfg.get("mm_momentum_threshold", 0.03)
            # Lotto tickets: if price is very cheap (<25c), accept ANY move in direction
            # Regular shots: normal momentum threshold
            yes_mom_ok = (momentum > min_momentum) or (fair_yes < 25 and momentum > 0)
            no_mom_ok = (momentum < -min_momentum) or (fair_no < 25 and momentum < 0)

            if fair_yes < 40 and yes_mom_ok and fair_yes >= 1:
                buy_price = max(1, fair_yes - 3)
                try:
                    oid = f"sol-snipe-{int(now_ts*1000)}"
                    await asyncio.to_thread(self.client.create_order,
                        ticker=ticker, client_order_id=oid,
                        side="yes", action="buy", count=1, type="limit",
                        yes_price=buy_price)
                    ms["_sol_last_order_ts"] = now_ts
                    ms["_sol_pending_orders"] = pending + 1  # track pending
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] SOL SNIPE: YES @ {buy_price}c "
                          f"(fair={fair_yes}, mom={momentum:+.3f}, pending={pending+1})")
                except Exception as e:
                    print(f"  SOL snipe error: {e}")

            elif fair_no < 40 and no_mom_ok and fair_no >= 1:
                buy_price = max(1, fair_no - 3)
                try:
                    oid = f"sol-snipe-{int(now_ts*1000)}"
                    await asyncio.to_thread(self.client.create_order,
                        ticker=ticker, client_order_id=oid,
                        side="no", action="buy", count=1, type="limit",
                        no_price=buy_price)
                    ms["_sol_last_order_ts"] = now_ts
                    ms["_sol_pending_orders"] = pending + 1
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] SOL SNIPE: NO @ {buy_price}c "
                          f"(fair={fair_no}, mom={momentum:+.3f}, pending={pending+1})")
                except Exception as e:
                    print(f"  SOL snipe error: {e}")

        # --- PHASE 2: Minutes 11-14 — confident directional (40-88c zone) ---
        elif cycle_sec < 840:
            strong_momentum = cfg.get("mm_momentum_threshold", 0.03) * 1.5  # 1.5x (was 2x)

            edges = []
            for t, v in st["ticks"]:
                if t > now_ts - 30:
                    d = v - st["strike"]
                    s_temp = _norm_cdf(d / effective_sigma) * 100
                    edges.append(s_temp - 50)

            if len(edges) < 5:
                return

            pos = sum(1 for e in edges if e > 3)
            neg = sum(1 for e in edges if e < -3)
            total = pos + neg
            if total == 0:
                return

            confidence = max(pos, neg) / total
            if confidence < 0.55:  # loosened from 0.65
                return

            favored = "yes" if pos > neg else "no"
            fair_price = fair_yes if favored == "yes" else fair_no

            if 40 <= fair_price <= 88 and abs(momentum) > strong_momentum:
                buy_price = max(40, min(83, fair_price - 3))
                try:
                    oid = f"sol-late-{int(now_ts*1000)}"
                    kw = {"yes_price": buy_price} if favored == "yes" else {"no_price": buy_price}
                    await asyncio.to_thread(self.client.create_order,
                        ticker=ticker, client_order_id=oid,
                        side=favored, action="buy", count=1, type="limit",
                        **kw)
                    ms["_sol_last_order_ts"] = now_ts
                    ms["_sol_pending_orders"] = pending + 1
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] SOL LATE SNIPE: {favored.upper()} @ {buy_price}c "
                          f"(fair={fair_price}, conf={confidence:.0%}, mom={momentum:+.3f}, pending={pending+1})")
                except Exception as e:
                    print(f"  SOL late snipe error: {e}")

    # ─── ETH Shadow Market Maker ──────────────────────────────

    async def eth_shadow_mm_tick(self, coin):
        """Shadow the sharps: copy Kalshi orderbook prices, profit from retail flow."""
        ticker = self.current_tickers.get(coin, "")
        if not ticker or not self.client:
            return
        st = self.brti_state.get(coin, {})
        ms = self.mm_state.get(coin, {})
        if not st.get("strike") or st["strike"] <= 0:
            return

        ETH_MAX_INV = 2          # max contracts per side
        ETH_MIN_SPREAD = 5       # minimum spread to quote (cents)
        ETH_FLATTEN_TIMEOUT = 30 # seconds before flattening one-sided fill
        ETH_SETTLE_GUARD = 60    # cancel quotes before settlement

        cycle_sec = (datetime.now().minute % 15) * 60 + datetime.now().second
        secs_remaining = max(1, 900 - cycle_sec)

        # Read Kalshi orderbook for this ticker
        book = self.kalshi_books.get(ticker, {}) if hasattr(self, 'kalshi_books') else {}
        if not book or (not book.get("yes") and not book.get("no")):
            return  # no book data yet

        # Compute best bid/ask from orderbook
        yes_bids = {p: q for p, q in book.get("yes", {}).items() if q > 0}
        no_bids = {p: q for p, q in book.get("no", {}).items() if q > 0}
        best_yes_bid = max(yes_bids.keys()) if yes_bids else 0
        best_no_bid = max(no_bids.keys()) if no_bids else 0
        yes_ask = (100 - best_no_bid) if best_no_bid > 0 else 100
        spread = yes_ask - best_yes_bid if best_yes_bid > 0 else 0

        inv = ms.get("inventory", 0)

        # Cross-check with ticker channel
        ws_prices = self.ws_prices.get(ticker, {})
        ws_yes_bid = ws_prices.get("yes_bid", 0)
        if ws_yes_bid > 0 and best_yes_bid > 0 and abs(ws_yes_bid - best_yes_bid) > 5:
            return  # book and ticker diverge — stale data, skip

        # Settlement guard: cancel all quotes in last 60s
        if secs_remaining < ETH_SETTLE_GUARD:
            # Cancel any resting ETH orders
            try:
                resting = await asyncio.to_thread(self.client.get_orders, ticker=ticker)
                for o in [x for x in resting.get("orders", []) if x.get("status") == "resting"]:
                    if (o.get("client_order_id") or "").startswith("eth-"):
                        try:
                            await asyncio.to_thread(self.client.cancel_order, o["order_id"])
                        except Exception:
                            pass
            except Exception:
                pass
            # If holding inventory, sell to flatten before settlement
            if inv != 0:
                sell_side = "yes" if inv > 0 else "no"
                sell_price = max(1, (best_yes_bid - 1) if inv > 0 else (best_no_bid - 1))
                try:
                    oid = f"eth-sg-{int(time.time()*1000)}"
                    kw = {"yes_price": sell_price} if sell_side == "yes" else {"no_price": sell_price}
                    await asyncio.to_thread(self.client.create_order,
                        ticker=ticker, client_order_id=oid,
                        side=sell_side, action="sell", count=abs(inv), type="limit", **kw)
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] ETH SHADOW SETTLE: SELL {sell_side.upper()} {abs(inv)}x @ {sell_price}c")
                except Exception as e:
                    print(f"  ETH settle error: {e}")
            return

        # Flatten timer: if one side filled >30s ago and other side hasn't, dump
        fill_ts = ms.get("_shadow_fill_ts", 0)
        if fill_ts > 0 and inv != 0 and time.time() - fill_ts > ETH_FLATTEN_TIMEOUT:
            sell_side = "yes" if inv > 0 else "no"
            sell_bid = best_yes_bid if inv > 0 else best_no_bid
            sell_price = max(1, sell_bid - 1) if sell_bid > 0 else 1
            # Cancel resting orders first
            try:
                resting = await asyncio.to_thread(self.client.get_orders, ticker=ticker)
                for o in [x for x in resting.get("orders", []) if x.get("status") == "resting"]:
                    if (o.get("client_order_id") or "").startswith("eth-"):
                        try:
                            await asyncio.to_thread(self.client.cancel_order, o["order_id"])
                        except Exception:
                            pass
            except Exception:
                pass
            try:
                oid = f"eth-flat-{int(time.time()*1000)}"
                kw = {"yes_price": sell_price} if sell_side == "yes" else {"no_price": sell_price}
                await asyncio.to_thread(self.client.create_order,
                    ticker=ticker, client_order_id=oid,
                    side=sell_side, action="sell", count=abs(inv), type="limit", **kw)
                print(f"[{datetime.now().strftime('%H:%M:%S')}] ETH SHADOW FLATTEN: {sell_side.upper()} {abs(inv)}x @ {sell_price}c "
                      f"(one-side fill {time.time()-fill_ts:.0f}s ago)")
            except Exception as e:
                print(f"  ETH flatten error: {e}")
            ms["_shadow_fill_ts"] = 0
            return

        # Spread too tight — cancel any resting quotes, wait
        if spread < ETH_MIN_SPREAD:
            if int(time.time()) % 30 == 0:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] ETH SHADOW: spread={spread}c (need {ETH_MIN_SPREAD}c+) — waiting")
            return

        # --- QUOTE at sharps' prices ---
        # Place YES bid at best_yes_bid (join the sharps' queue)
        # Place NO bid at best_no_bid (= sell YES at yes_ask = join the ask)
        # Only quote sides where we have room
        can_buy_yes = inv < ETH_MAX_INV
        can_buy_no = inv > -ETH_MAX_INV

        # Check what we currently have resting
        current_yes_price = ms.get("_shadow_yes_price", 0)
        current_no_price = ms.get("_shadow_no_price", 0)

        # Only requote if sharps' price changed
        need_yes_update = can_buy_yes and best_yes_bid > 0 and best_yes_bid != current_yes_price
        need_no_update = can_buy_no and best_no_bid > 0 and best_no_bid != current_no_price

        if not need_yes_update and not need_no_update:
            return  # prices unchanged, keep resting orders

        # Cancel existing quotes before placing new ones
        try:
            resting = await asyncio.to_thread(self.client.get_orders, ticker=ticker)
            for o in [x for x in resting.get("orders", []) if x.get("status") == "resting"]:
                cid = o.get("client_order_id") or ""
                if cid.startswith("eth-q"):
                    try:
                        await asyncio.to_thread(self.client.cancel_order, o["order_id"])
                    except Exception:
                        pass
        except Exception:
            pass

        # Place YES bid (buy YES at sharps' bid)
        if can_buy_yes and best_yes_bid > 0 and best_yes_bid < 95:
            try:
                oid = f"eth-qy-{int(time.time()*1000)}"
                await asyncio.to_thread(self.client.create_order,
                    ticker=ticker, client_order_id=oid,
                    side="yes", action="buy", count=1, type="limit",
                    yes_price=best_yes_bid)
                ms["_shadow_yes_price"] = best_yes_bid
            except Exception as e:
                if "above" not in str(e).lower() and "below" not in str(e).lower():
                    print(f"  ETH quote YES error: {e}")

        # Place NO bid (buy NO at sharps' NO bid = sell YES at ask)
        if can_buy_no and best_no_bid > 0 and best_no_bid < 95:
            try:
                oid = f"eth-qn-{int(time.time()*1000)}"
                await asyncio.to_thread(self.client.create_order,
                    ticker=ticker, client_order_id=oid,
                    side="no", action="buy", count=1, type="limit",
                    no_price=best_no_bid)
                ms["_shadow_no_price"] = best_no_bid
            except Exception as e:
                if "above" not in str(e).lower() and "below" not in str(e).lower():
                    print(f"  ETH quote NO error: {e}")

        # Log periodically
        if int(time.time()) % 10 == 0 or ms.get("_shadow_first_log", True):
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ETH SHADOW: "
                  f"bid={best_yes_bid}c ask={yes_ask}c spread={spread}c "
                  f"inv={inv:+d} | quoting YES@{best_yes_bid}c NO@{best_no_bid}c "
                  f"({secs_remaining//60}m left)")
            ms["_shadow_first_log"] = False

    # ─── BTC Distance-Based Momentum Sniper ─────────────────

    async def btc_distance_sniper_tick(self, coin):
        """BTC Distance-Based Flip Sniper.

        Kalshi orderbook = primary signal. BRTI = confirmation.
        Always holds 2 contracts on the momentum side.
        Flips with single 4-contract sell when momentum reverses.

        States: FLAT -> HOLDING -> FLIP -> HOLDING (opposite) -> ...
        Exit: 90c hard TP, 30c stop loss, projected-cushion kill
        """
        # Kill switch removed — user will monitor manually
        # if self.trading_disabled:
        #     return

        st = self.brti_state[coin]
        ms = self.mm_state[coin]
        cfg = BRTI_COIN_CONFIG[coin]

        if st["strike"] <= 0 or not st["ticks"]:
            return

        cycle_sec = (datetime.now().minute % 15) * 60 + datetime.now().second
        now_ts = time.time()
        ticker = self.current_tickers.get(coin, "")
        if not ticker or not self.client or DRY_RUN:
            return

        # --- Constants (per-coin) ---
        POSITION_SIZE = 2
        HARD_TP_C = 90

        # --- Current state ---
        latest = st["ticks"][-1][1]
        distance = latest - st["strike"]
        abs_distance = abs(distance)
        above_strike = distance > 0

        # Fair value from BRTI
        sigma = cfg.get("sigma_per_sec", 2.20)
        secs_remaining = max(1.0, float(900 - cycle_sec))
        effective_sigma = sigma * (secs_remaining ** 0.5) * MM_SMOOTHING
        if effective_sigma <= 0:
            return
        p_yes = _norm_cdf(distance / effective_sigma)
        fair_yes = max(1, min(99, int(p_yes * 100)))
        fair_no = 100 - fair_yes

        # Kalshi market prices (PRIMARY signal)
        mkt = self.ws_prices.get(ticker, {})
        kalshi_yes_bid = mkt.get("yes_bid", 0)
        kalshi_yes_ask = mkt.get("yes_ask", 0)
        kalshi_no_bid = 100 - kalshi_yes_ask if kalshi_yes_ask > 0 else 0
        kalshi_no_ask = 100 - kalshi_yes_bid if kalshi_yes_bid > 0 else 0

        # Position state — KALSHI IS THE ONLY TRUTH
        # ticker_contracts_signed stores the SIGNED position from post_position_fp
        # Positive = YES, Negative = NO, Zero = flat
        q = self.ticker_contracts_signed.get(ticker, 0)
        if q > 0:
            held_side = "yes"
        elif q < 0:
            held_side = "no"
        else:
            held_side = ""
        st["held_side"] = held_side
        ms["inventory"] = q
        holding_yes = q > 0
        holding_no = q < 0
        is_flat = q == 0

        # Exit pending guard — HARD BLOCK until fill confirms or 15s timeout
        if ms.get("_exit_pending", False):
            if now_ts - ms.get("_exit_pending_ts", 0) > 15:
                ms["_exit_pending"] = False  # stale after 15s
            else:
                return  # EXIT ORDER IN FLIGHT — do absolutely nothing

        # --- Kalshi velocity tracking ---
        # Track Kalshi bid history for velocity calculation
        if "_kalshi_bid_history" not in ms:
            ms["_kalshi_bid_history"] = []  # [(ts, yes_bid, no_bid)]
        if kalshi_yes_bid > 0:
            hist = ms["_kalshi_bid_history"]
            # Only append if price changed or 1s+ since last entry
            if not hist or kalshi_yes_bid != hist[-1][1] or now_ts - hist[-1][0] >= 1:
                hist.append((now_ts, kalshi_yes_bid, kalshi_no_bid))
            # Trim to last 60s
            while hist and hist[0][0] < now_ts - 60:
                hist.pop(0)

        # --- SMA Momentum Signal ---
        # 5-period SMA vs 12-period SMA cross + price vs 15-min avg
        ticks_5s = [v for t, v in st["ticks"] if t > now_ts - 5]
        ticks_30s = [v for t, v in st["ticks"] if t > now_ts - 30]
        ticks_15m = [v for t, v in st["ticks"]]
        if len(ticks_5s) < 3 or len(ticks_30s) < 10:
            return
        sma_fast = sum(ticks_5s) / len(ticks_5s)
        sma_slow = sum(ticks_30s) / len(ticks_30s)
        avg_15m = sum(ticks_15m) / len(ticks_15m) if ticks_15m else latest
        sma_min_gap = sigma  # BTC ~$2.20, ETH ~$0.07
        sma_gap = sma_fast - sma_slow  # positive = bullish, negative = bearish

        # Gap must be widening over 5s (accelerating), not just tick-to-tick noise
        # Compare current gap to gap from 5 seconds ago
        if "_sma_gap_history" not in ms:
            ms["_sma_gap_history"] = []
        ms["_sma_gap_history"].append((now_ts, sma_gap))
        # Trim to last 10s
        ms["_sma_gap_history"] = [(t, g) for t, g in ms["_sma_gap_history"] if t > now_ts - 10]
        gap_5s_ago = [g for t, g in ms["_sma_gap_history"] if t < now_ts - 4]
        if gap_5s_ago:
            gap_widening = abs(sma_gap) > abs(gap_5s_ago[-1]) + 0.01
        else:
            gap_widening = abs(sma_gap) > sma_min_gap  # first 5s: just check threshold

        # YES: fast > slow by 1x sigma, price > 15m avg, gap widening
        # NO: opposite
        sma_bullish = sma_gap > sma_min_gap and latest > avg_15m and gap_widening
        sma_bearish = (-sma_gap) > sma_min_gap and latest < avg_15m and gap_widening

        # Periodic signal log
        if int(now_ts) % 10 == 0 and is_flat:
            signal = "BUY_YES" if sma_bullish else ("BUY_NO" if sma_bearish else "WAIT")
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {coin} SMA: "
                  f"gap={sma_gap:+.1f}(need{sma_min_gap:+.1f}) "
                  f"{'ACCEL' if gap_widening else 'DECEL'} → {signal} "
                  f"| bid={kalshi_yes_bid}/{kalshi_no_bid} dist=${abs_distance:.0f}")

        cycle_min = cycle_sec // 60
        POSITION_SIZE = 2
        prefix = 'btc' if coin == 'BTC' else 'eth'

        # ===================================================
        # HOLDING — mode determines exit logic
        # ===================================================
        if not is_flat:
            held_side = "yes" if holding_yes else "no"
            our_kalshi_bid = kalshi_yes_bid if holding_yes else kalshi_no_bid
            avg_cost = ms.get("avg_yes_cost", 0) if holding_yes else ms.get("avg_no_cost", 0)
            mode = ms.get("_entry_mode", "sniper")

            if mode == "pair":
                # PAIR MM: check for trend, adjust unfilled side, or wait

                # TREND DETECTION: if filled side bid > 75c, market has picked a direction
                # Cancel unfilled side, switch to sniper mode with 90c TP
                yes_filled = ms.get("_pair_yes_filled", False)
                no_filled = ms.get("_pair_no_filled", False)
                if (yes_filled and not no_filled and kalshi_yes_bid >= 75) or \
                   (no_filled and not yes_filled and kalshi_no_bid >= 75):
                    trending_side = "yes" if yes_filled else "no"
                    trending_bid = kalshi_yes_bid if yes_filled else kalshi_no_bid
                    # Cancel the unfilled side
                    try:
                        resting = await asyncio.to_thread(self.client.get_orders, ticker=ticker)
                        for o in [x for x in resting.get("orders", []) if x.get("status") == "resting"]:
                            cid = o.get("client_order_id") or ""
                            if cid.startswith(f"{prefix}-pair"):
                                await asyncio.to_thread(self.client.cancel_order, o["order_id"])
                    except Exception:
                        pass
                    # Switch to sniper mode + add 1 contract on trending side
                    ms["_entry_mode"] = "sniper"
                    ms["_tp_order_id"] = None
                    # Add 1 more contract at current bid
                    try:
                        add_price = trending_bid
                        oid = f"{prefix}-add-{int(now_ts*1000)}"
                        kw = {"yes_price": add_price} if trending_side == "yes" else {"no_price": add_price}
                        await asyncio.to_thread(self.client.create_order,
                            ticker=ticker, client_order_id=oid,
                            side=trending_side, action="buy", count=POSITION_SIZE,
                            type="limit", **kw)
                        ms["_trend_add_price"] = add_price
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] {coin} TREND: "
                              f"{trending_side.upper()} bid={trending_bid}c → sniper mode + ADD 1x@{add_price}c")
                    except Exception as e:
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] {coin} TREND: "
                              f"{trending_side.upper()} bid={trending_bid}c → sniper mode (add failed: {e})")
                    return

                # Pair adjustment logic below
                # Gradually increase unfilled side price to improve fill probability
                # Sacrifice some profit to lock the round trip
                yes_filled = ms.get("_pair_yes_filled", False)
                no_filled = ms.get("_pair_no_filled", False)
                if yes_filled and no_filled:
                    return  # both filled, holding to settlement

                # Adjust unfilled side — speed depends on how profitable we are
                pair_age = now_ts - ms.get("_entry_pending_ts", now_ts)
                last_adjust = ms.get("_pair_last_adjust_ts", 0)

                if yes_filled and not no_filled:
                    filled_side_bid = kalshi_yes_bid
                    filled_cost = ms.get("_pair_yes_price", 0)
                    old_unfilled = ms.get("_pair_no_price", 0)
                    profit_so_far = filled_side_bid - filled_cost  # unrealized gain

                    # Track peak bid on filled side
                    peak_key = "_pair_filled_peak"
                    if filled_side_bid > ms.get(peak_key, 0):
                        ms[peak_key] = filled_side_bid
                    filled_peak = ms.get(peak_key, filled_side_bid)
                    dropping = filled_side_bid < filled_peak - 5

                    # AGGRESSIVE LOCK:
                    # Any profit: reprice unfilled to current bid (tracks the move up)
                    # Only reprice if new price > current resting price (don't lower it)
                    if profit_so_far >= 5 and kalshi_no_bid > 0 and kalshi_no_bid > old_unfilled:
                        new_no = min(kalshi_no_bid, 103 - filled_cost)  # max 103c total
                        if new_no > old_unfilled and filled_cost + new_no <= 103:
                            try:
                                resting = await asyncio.to_thread(self.client.get_orders, ticker=ticker)
                                for o in [x for x in resting.get("orders", []) if x.get("status") == "resting"]:
                                    cid = o.get("client_order_id") or ""
                                    if cid.startswith(f"{prefix}-pair-n"):
                                        await asyncio.to_thread(self.client.cancel_order, o["order_id"])
                                no_oid = f"{prefix}-pair-n-{int(now_ts*1000)}"
                                await asyncio.to_thread(self.client.create_order,
                                    ticker=ticker, client_order_id=no_oid,
                                    side="no", action="buy", count=POSITION_SIZE,
                                    type="limit", no_price=new_no)
                                ms["_pair_no_price"] = new_no
                                ms["_pair_last_adjust_ts"] = now_ts
                                profit = 100 - filled_cost - new_no
                                print(f"[{datetime.now().strftime('%H:%M:%S')}] {coin} LOCK: "
                                      f"YES@{filled_cost}c up {profit_so_far}c → NO {old_unfilled}c→{new_no}c "
                                      f"({'+'if profit>0 else ''}{profit}c)")
                            except Exception as e:
                                print(f"  {coin} lock error: {e}")

                    # NORMAL ADJUST: +2c every 30s
                    elif pair_age > 30 and now_ts - last_adjust > 30:
                        new_no = old_unfilled + 2
                        if filled_cost + new_no > 97:
                            pass  # can't adjust further
                        else:
                            try:
                                resting = await asyncio.to_thread(self.client.get_orders, ticker=ticker)
                                for o in [x for x in resting.get("orders", []) if x.get("status") == "resting"]:
                                    cid = o.get("client_order_id") or ""
                                    if cid.startswith(f"{prefix}-pair-n"):
                                        await asyncio.to_thread(self.client.cancel_order, o["order_id"])
                                no_oid = f"{prefix}-pair-n-{int(now_ts*1000)}"
                                await asyncio.to_thread(self.client.create_order,
                                    ticker=ticker, client_order_id=no_oid,
                                    side="no", action="buy", count=POSITION_SIZE,
                                    type="limit", no_price=new_no)
                                ms["_pair_no_price"] = new_no
                                ms["_pair_last_adjust_ts"] = now_ts
                                profit = 100 - filled_cost - new_no
                                print(f"[{datetime.now().strftime('%H:%M:%S')}] {coin} PAIR ADJUST: "
                                      f"NO {old_unfilled}c→{new_no}c (profit now +{profit}c)")
                            except Exception as e:
                                print(f"  {coin} pair adjust error: {e}")

                elif no_filled and not yes_filled:
                    filled_side_bid = kalshi_no_bid
                    filled_cost = ms.get("_pair_no_price", 0)
                    old_unfilled = ms.get("_pair_yes_price", 0)
                    profit_so_far = filled_side_bid - filled_cost

                    # Track peak + detect drop
                    peak_key = "_pair_filled_peak"
                    if filled_side_bid > ms.get(peak_key, 0):
                        ms[peak_key] = filled_side_bid
                    filled_peak = ms.get(peak_key, filled_side_bid)
                    dropping = filled_side_bid < filled_peak - 5

                    # AGGRESSIVE LOCK:
                    if profit_so_far >= 5 and kalshi_yes_bid > 0 and kalshi_yes_bid > old_unfilled:
                        new_yes = min(kalshi_yes_bid, 103 - filled_cost)
                        if new_yes > old_unfilled and filled_cost + new_yes <= 103:
                            try:
                                resting = await asyncio.to_thread(self.client.get_orders, ticker=ticker)
                                for o in [x for x in resting.get("orders", []) if x.get("status") == "resting"]:
                                    cid = o.get("client_order_id") or ""
                                    if cid.startswith(f"{prefix}-pair-y"):
                                        await asyncio.to_thread(self.client.cancel_order, o["order_id"])
                                yes_oid = f"{prefix}-pair-y-{int(now_ts*1000)}"
                                await asyncio.to_thread(self.client.create_order,
                                    ticker=ticker, client_order_id=yes_oid,
                                    side="yes", action="buy", count=POSITION_SIZE,
                                    type="limit", yes_price=new_yes)
                                ms["_pair_yes_price"] = new_yes
                                ms["_pair_last_adjust_ts"] = now_ts
                                profit = 100 - filled_cost - new_yes
                                print(f"[{datetime.now().strftime('%H:%M:%S')}] {coin} LOCK: "
                                      f"NO@{filled_cost}c up {profit_so_far}c → YES {old_unfilled}c→{new_yes}c "
                                      f"({'+'if profit>0 else ''}{profit}c)")
                            except Exception as e:
                                print(f"  {coin} lock error: {e}")

                    # NORMAL ADJUST
                    elif pair_age > 30 and now_ts - last_adjust > 30:
                        new_yes = old_unfilled + 2
                        if new_yes + filled_cost > 97:
                            pass
                        else:
                            try:
                                resting = await asyncio.to_thread(self.client.get_orders, ticker=ticker)
                                for o in [x for x in resting.get("orders", []) if x.get("status") == "resting"]:
                                    cid = o.get("client_order_id") or ""
                                    if cid.startswith(f"{prefix}-pair-y"):
                                        await asyncio.to_thread(self.client.cancel_order, o["order_id"])
                                yes_oid = f"{prefix}-pair-y-{int(now_ts*1000)}"
                                await asyncio.to_thread(self.client.create_order,
                                    ticker=ticker, client_order_id=yes_oid,
                                    side="yes", action="buy", count=POSITION_SIZE,
                                    type="limit", yes_price=new_yes)
                                ms["_pair_yes_price"] = new_yes
                                ms["_pair_last_adjust_ts"] = now_ts
                                profit = 100 - new_yes - filled_cost
                                print(f"[{datetime.now().strftime('%H:%M:%S')}] {coin} PAIR ADJUST: "
                                      f"YES {old_unfilled}c→{new_yes}c (profit now +{profit}c)")
                            except Exception as e:
                                print(f"  {coin} pair adjust error: {e}")
                return

            # SNIPER mode: 90c TP + trend fade detection
            # Track peak bid for trend fade
            if our_kalshi_bid > ms.get("_sniper_peak_bid", 0):
                ms["_sniper_peak_bid"] = our_kalshi_bid

            # Place 90c TP
            if not ms.get("_tp_order_id"):
                actual_held = abs(q)
                if actual_held > 0:
                    try:
                        oid = f"{prefix}-tp-{int(now_ts*1000)}"
                        yes_tp = HARD_TP_C if held_side == "yes" else (100 - HARD_TP_C)
                        await asyncio.to_thread(self.client.create_order,
                            ticker=ticker, client_order_id=oid,
                            side=held_side, action="sell", count=actual_held,
                            type="limit", yes_price=yes_tp)
                        ms["_tp_order_id"] = oid
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] {coin} TP: "
                              f"SELL {held_side.upper()} {actual_held}x @ 90c (entry@{avg_cost:.0f}c)")
                    except Exception as e:
                        print(f"  {coin} TP error: {e}")

            # PAIR UNWIND: instead of stop loss, buy other side to lock round trip
            # Triggers on: bid dropped 10c from peak (trend fading)
            #           OR position is 15c+ underwater (adverse — unwind cheaply)
            # Allows total up to 100c (breakeven) to escape adverse positions
            peak_bid = ms.get("_sniper_peak_bid", 0)
            drop_from_peak = peak_bid - our_kalshi_bid if peak_bid > 0 else 0
            loss_from_entry = avg_cost - our_kalshi_bid if our_kalshi_bid > 0 else 0
            unwind_threshold = max(5, int(avg_cost * 0.30))  # 30% of entry, min 5c
            should_unwind = (drop_from_peak >= 10 or loss_from_entry >= unwind_threshold)

            if should_unwind and not ms.get("_fade_posted", False):
                other_side = "no" if held_side == "yes" else "yes"
                other_bid = kalshi_no_bid if held_side == "yes" else kalshi_yes_bid
                if other_bid > 0:
                    total = avg_cost + other_bid
                    if total <= 103:
                        # Pair unwind — buy other side to lock round trip
                        if ms.get("_tp_order_id"):
                            try:
                                await asyncio.to_thread(self.client.cancel_order, ms["_tp_order_id"])
                            except Exception:
                                pass
                            ms["_tp_order_id"] = None
                        try:
                            oid = f"{prefix}-fade-{int(now_ts*1000)}"
                            kw = {"yes_price": other_bid} if other_side == "yes" else {"no_price": other_bid}
                            await asyncio.to_thread(self.client.create_order,
                                ticker=ticker, client_order_id=oid,
                                side=other_side, action="buy", count=POSITION_SIZE,
                                type="limit", **kw)
                            ms["_fade_posted"] = True
                            profit = 100 - total
                            reason = "FADE" if drop_from_peak >= 10 else "UNWIND"
                            print(f"[{datetime.now().strftime('%H:%M:%S')}] {coin} {reason}: "
                                  f"entry={avg_cost:.0f}c now={our_kalshi_bid}c → "
                                  f"posting {other_side.upper()}@{other_bid}c "
                                  f"(total={total}c {'+'if profit>0 else ''}{profit}c)")
                        except Exception as e:
                            print(f"  {coin} unwind error: {e}")
                    elif cycle_min >= 12:
                        # Late cycle + can't pair — sell now, no time to recover
                        if ms.get("_tp_order_id"):
                            try:
                                await asyncio.to_thread(self.client.cancel_order, ms["_tp_order_id"])
                            except Exception:
                                pass
                            ms["_tp_order_id"] = None
                        sell_price = max(1, our_kalshi_bid) if our_kalshi_bid > 0 else 1
                        try:
                            oid = f"{prefix}-exit-{int(now_ts*1000)}"
                            kw = {"yes_price": sell_price} if held_side == "yes" else {"no_price": sell_price}
                            await asyncio.to_thread(self.client.create_order,
                                ticker=ticker, client_order_id=oid,
                                side=held_side, action="sell", count=abs(q),
                                type="limit", **kw)
                            ms["_exit_pending"] = True
                            ms["_exit_pending_ts"] = now_ts
                            ms["_fade_posted"] = True
                            print(f"[{datetime.now().strftime('%H:%M:%S')}] {coin} CUT: "
                                  f"entry={avg_cost:.0f}c sell@{sell_price}c "
                                  f"(pair too expensive at {total}c, cutting loss)")
                        except Exception as e:
                            print(f"  {coin} cut error: {e}")
            return

        # ===================================================
        # FLAT — two modes by cycle time
        # ===================================================
        kalshi_held = abs(self.ticker_contracts_signed.get(ticker, 0))
        if kalshi_held > 0:
            return
        if q != 0:
            ms["inventory"] = 0

        # Cancel stale entries after 5s
        if ms.get("_entry_pending", False):
            if now_ts - ms.get("_entry_pending_ts", now_ts) > 5:
                try:
                    resting = await asyncio.to_thread(self.client.get_orders, ticker=ticker)
                    for o in [x for x in resting.get("orders", []) if x.get("status") == "resting"]:
                        cid = o.get("client_order_id") or ""
                        if cid.startswith((f"{prefix}-snipe", f"{prefix}-pair")):
                            try:
                                await asyncio.to_thread(self.client.cancel_order, o["order_id"])
                            except Exception:
                                pass
                except Exception:
                    pass
                ms["_entry_pending"] = False
            else:
                return

        # Log every 30s
        if int(now_ts) % 30 == 0:
            pair_status = "PAIR_ACTIVE" if ms.get("_pair_posted", False) else ""
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {coin} WAIT: "
                  f"min={cycle_min} bid={kalshi_yes_bid}/{kalshi_no_bid} dist=${abs_distance:.0f} "
                  f"{pair_status}")

        # ─── MODE 1: PAIR MM (mins 0-12) ───
        # Post YES bid + NO bid simultaneously. Oscillation fills both for locked profit.
        # One pair at a time. Hold to settlement if only one side fills.
        if cycle_min < 13:
            # Already have a pair posted or filled — wait
            if ms.get("_pair_posted", False):
                return

            # Need Kalshi pricing
            if kalshi_yes_bid <= 0 or kalshi_no_bid <= 0:
                return

            # Post pair: YES at discount, NO at discount
            # Target: total cost < 95c for 5c+ guaranteed profit
            # Place each 5c below current bid to buy the dips
            yes_price = max(1, kalshi_yes_bid - 5)
            no_price = max(1, kalshi_no_bid - 5)
            total = yes_price + no_price

            if total >= 95:
                return  # not enough margin

            try:
                # Post YES bid
                yes_oid = f"{prefix}-pair-y-{int(now_ts*1000)}"
                await asyncio.to_thread(self.client.create_order,
                    ticker=ticker, client_order_id=yes_oid,
                    side="yes", action="buy", count=POSITION_SIZE,
                    type="limit", yes_price=yes_price)

                # Post NO bid
                no_oid = f"{prefix}-pair-n-{int(now_ts*1000)}"
                await asyncio.to_thread(self.client.create_order,
                    ticker=ticker, client_order_id=no_oid,
                    side="no", action="buy", count=POSITION_SIZE,
                    type="limit", no_price=no_price)

                ms["_pair_posted"] = True
                ms["_pair_yes_price"] = yes_price
                ms["_pair_no_price"] = no_price
                ms["_pair_yes_oid"] = yes_oid
                ms["_pair_no_oid"] = no_oid
                ms["_entry_mode"] = "pair"
                profit_if_both = 100 - total
                print(f"[{datetime.now().strftime('%H:%M:%S')}] {coin} PAIR: "
                      f"YES@{yes_price}c + NO@{no_price}c = {total}c "
                      f"(+{profit_if_both}c if both fill) min={cycle_min}")
            except Exception as e:
                print(f"  {coin} pair error: {e}")
            return

        # ─── MODE 2: DIRECTIONAL SNIPER (mins 12+) — DISABLED ───
        return  # DISABLED: sniper off, code preserved below for re-enable

        # Buy >65c side, 90c TP, hold to settlement
        if ms.get("_stopped_this_cycle", False):
            return

        if kalshi_yes_bid >= 65:
            side = "yes"
            buy_price = kalshi_yes_bid
        elif kalshi_no_bid >= 65:
            side = "no"
            buy_price = kalshi_no_bid
        else:
            return

        if buy_price > 85:
            return

        if coin == "ETH" and abs_distance < 0.50:
            return

        ms["_tp_order_id"] = None
        ms["_entry_mode"] = "sniper"
        try:
            oid = f"{prefix}-snipe-{int(now_ts*1000)}"
            kw = {"yes_price": buy_price} if side == "yes" else {"no_price": buy_price}
            await asyncio.to_thread(self.client.create_order,
                ticker=ticker, client_order_id=oid,
                side=side, action="buy", count=POSITION_SIZE, type="limit", **kw)
            ms["_entry_pending"] = True
            ms["_entry_pending_ts"] = now_ts
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {coin} SNIPER: {side.upper()} {POSITION_SIZE}x @ {buy_price}c "
                  f"| dist=${abs_distance:.0f} min={cycle_min}")
        except Exception as e:
            print(f"  {coin} sniper error: {e}")

        # Dead code below — kept for reference but never reached
        if False:
            our_fair = fair_yes if holding_yes else fair_no
            our_kalshi_bid = kalshi_yes_bid if holding_yes else kalshi_no_bid
            avg_cost = ms.get("avg_yes_cost", 0) if holding_yes else ms.get("avg_no_cost", 0)
            minutes_remaining = secs_remaining / 60.0

            # --- PROBABILITY DATA COLLECTOR ---
            # Track position lifecycle for building flip probability model
            if not hasattr(self, '_prob_data'):
                self._prob_data = {}
            pd = self._prob_data.get(coin)
            if pd is None or pd.get("entry_price") != avg_cost:
                # New position — initialize tracking
                self._prob_data[coin] = {
                    "entry_price": avg_cost,
                    "entry_ts": now_ts,
                    "entry_side": held_side,
                    "entry_dist": abs_distance,
                    "entry_cycle_sec": cycle_sec,
                    "peak_bid": our_kalshi_bid,
                    "peak_ts": now_ts,
                    "drops_from_peak": [],  # list of (ts, peak, current, drop_size, brti_toward%, dist, min_left, recovered_within_10s)
                    "ticks": [],  # (ts, kalshi_bid, brti_toward, dist, min_left)
                }
                pd = self._prob_data[coin]

            # Update peak
            if our_kalshi_bid > pd["peak_bid"]:
                pd["peak_bid"] = our_kalshi_bid
                pd["peak_ts"] = now_ts

            # Record tick (every 2s to avoid bloat)
            if not pd["ticks"] or now_ts - pd["ticks"][-1][0] >= 2:
                pd["ticks"].append((now_ts, our_kalshi_bid, sma_gap, abs_distance, minutes_remaining))

            # Detect drops from peak (>5c)
            drop_from_peak = pd["peak_bid"] - our_kalshi_bid
            if drop_from_peak >= 5:
                # Check if we already logged this drop level
                logged_levels = [d[3] for d in pd["drops_from_peak"]]
                drop_bucket = (drop_from_peak // 5) * 5  # bucket: 5, 10, 15, 20...
                if drop_bucket not in logged_levels:
                    pd["drops_from_peak"].append((
                        now_ts, pd["peak_bid"], our_kalshi_bid, drop_bucket,
                        sma_gap, abs_distance, minutes_remaining
                    ))
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] PROB DATA {coin}: "
                          f"peak={pd['peak_bid']}c now={our_kalshi_bid}c drop={drop_from_peak}c "
                          f"entry={avg_cost:.0f}c pnl={our_kalshi_bid - avg_cost:+.0f}c "
                          f"sma_gap={sma_gap:+.1f} dist=${abs_distance:.0f} "
                          f"min_left={minutes_remaining:.0f} 📈")

            # --- EXIT 1: Take Profit (maker) ---
            # Entry <= 84c: TP at entry + 5c
            # Entry 85c: TP at 90c
            tp_price = int(avg_cost + 10) if avg_cost <= 84 else HARD_TP_C
            tp_price = min(tp_price, HARD_TP_C)

            if not ms.get("_tp_order_id"):
                actual_held = abs(q)
                if actual_held > 0:
                    try:
                        oid = f"{'btc' if coin == 'BTC' else 'eth'}-tp-{int(now_ts*1000)}"
                        # Always use yes_price for TP — Kalshi matches better on YES side
                        # Selling YES at Xc = selling our YES position at Xc
                        # Selling NO at Xc = equivalent to yes_price=(100-X)
                        yes_tp = tp_price if held_side == "yes" else (100 - tp_price)
                        await asyncio.to_thread(self.client.create_order,
                            ticker=ticker, client_order_id=oid,
                            side=held_side, action="sell", count=actual_held, type="limit",
                            yes_price=yes_tp)
                        ms["_tp_order_id"] = oid
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] BTC TP: SELL {held_side.upper()} {actual_held}x @ {tp_price}c "
                              f"(entry@{avg_cost:.0f}c, +{tp_price-int(avg_cost)}c) — maker")
                    except Exception as e:
                        print(f"  BTC TP error: {e}")

            # --- EXIT 2: SMA Momentum Exit + Hard Backstop ---
            loss_from_entry = avg_cost - our_kalshi_bid if our_kalshi_bid > 0 else 0
            entry_gap = ms.get("_entry_sma_gap", 0)
            entry_side = ms.get("_entry_side", held_side)

            # SMA reversal: gap has closed or flipped against our position
            pnl_from_entry = our_kalshi_bid - avg_cost if our_kalshi_bid > 0 else 0
            if entry_side == "yes":
                gap_reversed = sma_gap < -sma_min_gap
                gap_closed = sma_gap < 0
            else:
                gap_reversed = sma_gap > sma_min_gap
                gap_closed = sma_gap > 0

            # How long have we held?
            hold_secs = now_ts - ms.get("_entry_fill_ts", now_ts)

            # Exit when momentum dies
            # Reversal/flip: anytime (urgent)
            # Stall: only after 30s hold AND underwater (give TP time to fill)
            # Hard backstop: anytime
            should_exit = False
            should_flip = False
            exit_reason = ""
            exit_maker = False

            if gap_reversed and gap_widening:
                should_flip = True
                exit_reason = f"SMA_FLIP gap={sma_gap:+.1f} pnl={pnl_from_entry:+.0f}c held={hold_secs:.0f}s"
            elif gap_reversed:
                should_exit = True
                exit_reason = f"SMA_REVERSED gap={sma_gap:+.1f} pnl={pnl_from_entry:+.0f}c held={hold_secs:.0f}s"
            elif gap_closed and pnl_from_entry < -3 and hold_secs > 30:
                should_exit = True
                exit_reason = f"SMA_STALLED gap={sma_gap:+.1f} pnl={pnl_from_entry:+.0f}c held={hold_secs:.0f}s"
                exit_maker = True
            elif loss_from_entry > 20:
                should_exit = True
                exit_reason = f"HARD_STOP loss={loss_from_entry:.0f}c"

            if should_exit or should_flip:
                sell_count = abs(q) + POSITION_SIZE if should_flip else abs(q)
                new_side = "NO" if held_side == "yes" else "YES"
                action_label = f"FLIP→{new_side}" if should_flip else "EXIT"

                print(f"[{datetime.now().strftime('%H:%M:%S')}] {coin} {action_label}: "
                      f"entry={avg_cost:.0f}c now={our_kalshi_bid}c {exit_reason} "
                      f"held={held_side} sell={sell_count}x ⚡")
                # Cancel ALL resting orders
                ms["_tp_order_id"] = None
                try:
                    resting = await asyncio.to_thread(self.client.get_orders, ticker=ticker)
                    for o in [x for x in resting.get("orders", []) if x.get("status") == "resting"]:
                        try:
                            await asyncio.to_thread(self.client.cancel_order, o["order_id"])
                        except Exception:
                            pass
                except Exception:
                    pass
                # Exit/flip
                if exit_maker and our_kalshi_bid > 0:
                    sell_price = our_kalshi_bid
                else:
                    sell_price = max(1, our_kalshi_bid - 1) if our_kalshi_bid > 0 else 1
                try:
                    prefix = 'btc' if coin == 'BTC' else 'eth'
                    oid = f"{prefix}-{'flip' if should_flip else 'exit'}-{int(now_ts*1000)}"
                    kw = {"yes_price": sell_price} if held_side == "yes" else {"no_price": sell_price}
                    await asyncio.to_thread(self.client.create_order,
                        ticker=ticker, client_order_id=oid,
                        side=held_side, action="sell", count=sell_count, type="limit", **kw)
                    ms["_exit_pending"] = True
                    ms["_exit_pending_ts"] = now_ts
                    if should_flip:
                        ms["_pre_flip_sign"] = 1 if q > 0 else -1
                    print(f"  SELL {held_side.upper()} {sell_count}x @ {sell_price}c "
                          f"({'maker' if exit_maker else 'taker'}) "
                          f"{'→ expecting ' + new_side + ' ' + str(POSITION_SIZE) + 'x' if should_flip else ''}")
                except Exception as e:
                    print(f"  {coin} exit error: {e}")
                return

            # Still holding -- no exit or flip triggered
            return

        # ===================================================
        # FLAT -- check entry criteria
        # ===================================================

        # HARD RULE: Buy ONLY from truly flat — both inventory AND Kalshi must be 0
        kalshi_held = abs(self.ticker_contracts_signed.get(ticker, 0))
        if kalshi_held > 0:
            return  # Kalshi shows we hold something — don't buy more
        if q != 0:
            ms["inventory"] = 0  # desync — trust Kalshi's 0, reset inventory
        # Block if we already have a pending entry order in flight
        # But cancel stale entries after 5s — don't leave resting orders in bad spots
        if ms.get("_entry_pending", False):
            entry_age = now_ts - ms.get("_entry_pending_ts", now_ts)
            if entry_age > 5:
                # Stale entry — cancel it and allow fresh entry
                try:
                    resting = await asyncio.to_thread(self.client.get_orders, ticker=ticker)
                    for o in [x for x in resting.get("orders", []) if x.get("status") == "resting"]:
                        cid = o.get("client_order_id") or ""
                        if cid.startswith(("btc-snipe", "eth-snipe")):
                            try:
                                await asyncio.to_thread(self.client.cancel_order, o["order_id"])
                                print(f"[{datetime.now().strftime('%H:%M:%S')}] {coin} STALE ENTRY: canceled after {entry_age:.0f}s")
                            except Exception:
                                pass
                except Exception:
                    pass
                ms["_entry_pending"] = False
            else:
                return

        # Side from SMA signal
        if sma_bullish:
            side = "yes"
        elif sma_bearish:
            side = "no"
        else:
            return

        our_fair = fair_yes if side == "yes" else fair_no

        # Kalshi price gates
        if side == "yes":
            market_price = kalshi_yes_ask
            our_bid = kalshi_yes_bid
        else:
            market_price = kalshi_no_ask
            our_bid = kalshi_no_bid
        if market_price <= 0:
            return

        # Time-based min bid
        cycle_min = cycle_sec // 60
        if cycle_min < 8:
            min_bid = 1
        elif cycle_min < 10:
            min_bid = 35
        elif cycle_min < 12:
            min_bid = 45
        else:
            min_bid = 75

        if our_bid < min_bid or market_price > 85:
            return

        # Extreme entries (<20c or >80c): require 2x sigma gap
        if (our_bid > 80 or our_bid < 20) and abs(sma_gap) < sigma * 2:
            return

        # Minimum distance: at least 7x sigma (BTC ~$15, ETH ~$0.50)
        if abs_distance < sigma * 7:
            return

        # --- ENTER (maker order) ---
        ms["_tp_order_id"] = None
        ms["_peak_kalshi_bid"] = 0
        # Use Kalshi bid (not ask) for maker entry — sit in the queue, no taker fees
        if side == "yes":
            maker_bid = kalshi_yes_bid
        else:
            maker_bid = kalshi_no_bid
        buy_price = maker_bid if maker_bid > 0 else max(1, our_fair - 3)
        buy_price = max(1, min(85, buy_price))  # cap at 85c

        try:
            oid = f"btc-snipe-{int(now_ts*1000)}" if coin == "BTC" else f"eth-snipe-{int(now_ts*1000)}"
            kw = {"yes_price": buy_price} if side == "yes" else {"no_price": buy_price}
            await asyncio.to_thread(self.client.create_order,
                ticker=ticker, client_order_id=oid,
                side=side, action="buy", count=POSITION_SIZE, type="limit", **kw)
            ms["_entry_pending"] = True
            ms["_entry_pending_ts"] = now_ts
            ms["_entry_sma_gap"] = sma_gap
            ms["_entry_side"] = side
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {coin} SNIPE: {side.upper()} {POSITION_SIZE}x @ {buy_price}c "
                  f"| gap={sma_gap:+.1f} mkt={market_price} dist=${abs_distance:.0f} "
                  f"({secs_remaining/60:.0f}m left)")
        except Exception as e:
            print(f"  {coin} snipe error: {e}")

    # ─── MM Requote Loop ─────────────────────────────────────

    async def mm_requote_loop(self):
        """Main MM event loop: update A-S quotes on every sBRTI tick."""
        while self.running:
            for coin, cfg in BRTI_COIN_CONFIG.items():
                try:
                    ms = self.mm_state[coin]
                    st = self.brti_state[coin]

                    if not ms["requote_pending"]:
                        continue
                    ms["requote_pending"] = False

                    if st["strike"] <= 0 or not st["ticks"]:
                        continue
                    ticker = self.current_tickers.get(coin, "")
                    if not ticker:
                        continue

                    if BRTI_COIN_CONFIG[coin].get("probation_disabled"):
                        continue  # BTC-only probation

                    # ETH shadow market maker — copy sharps' orderbook prices
                    if BRTI_COIN_CONFIG[coin].get("eth_shadow_mm"):
                        await self.eth_shadow_mm_tick(coin)
                        continue

                    # BTC distance-based momentum sniper [DISABLED]
                    if False and BRTI_COIN_CONFIG[coin].get("btc_distance_sniper"):  # DISABLED
                        await self.btc_distance_sniper_tick(coin)
                        continue

                    # SOL uses directional sniper, not MM [DISABLED]
                    if False and BRTI_COIN_CONFIG[coin].get("sol_sniper_mode"):  # DISABLED
                        await self.sol_sniper_tick(coin)
                        continue

                    cycle_sec = (datetime.now().minute % 15) * 60 + datetime.now().second

                    # ── MM Take Profit: if position value hits 95c, sell all and re-quote ──
                    inv = ms.get("inventory", 0)
                    if inv != 0:
                        prices = self.ws_prices.get(ticker, {})
                        yes_bid_tp = prices.get("yes_bid", 0)
                        yes_ask_tp = prices.get("yes_ask", 0)
                        if yes_bid_tp > 0 and yes_ask_tp > 0:
                            if inv > 0:
                                current_val = yes_bid_tp
                            else:
                                current_val = 100 - yes_ask_tp
                            tp_threshold = 93 if coin == "ETH" else 95
                            if current_val >= tp_threshold:
                                held_count = abs(inv)
                                sell_side = "yes" if inv > 0 else "no"
                                if self.client and not DRY_RUN:
                                    try:
                                        tp_id = f"mm-tp-{coin.lower()}-{int(time.time()*1000)}"
                                        await asyncio.to_thread(
                                            self.client.create_order,
                                            ticker=ticker, client_order_id=tp_id,
                                            side=sell_side, action="sell", count=held_count, type="limit",
                                            yes_price=yes_bid_tp if sell_side == "yes" else None,
                                            no_price=(100 - yes_ask_tp) if sell_side == "no" else None,
                                        )
                                        avg_cost = ms["avg_yes_cost"] if inv > 0 else ms["avg_no_cost"]
                                        print(f"[{datetime.now().strftime('%H:%M:%S')}] {coin} MM TAKE PROFIT: {sell_side.upper()} {held_count}x @ {current_val}c (avg entry: {avg_cost:.0f}c) — re-quoting")
                                        ms["inventory"] = 0
                                        ms["total_yes_bought"] = 0
                                        ms["total_no_bought"] = 0
                                        ms["avg_yes_cost"] = 0.0
                                        ms["avg_no_cost"] = 0.0
                                        self.ticker_contracts[ticker] = 0
                                    except Exception as e:
                                        print(f"  -> {coin} MM TP error: {e}")
                                continue

                    # Settlement guard
                    settle_guard = cfg.get("mm_settle_guard_sec", MM_SETTLE_GUARD_SEC)
                    if cycle_sec >= (900 - settle_guard):
                        if ms["quotes_active"]:
                            inv = ms["inventory"]
                            avg_y = ms["avg_yes_cost"]
                            avg_n = ms["avg_no_cost"]
                            print(f"[{datetime.now().strftime('%H:%M:%S')}] {coin} MM SETTLE GUARD: pulling quotes | YES={ms['total_yes_bought']}x@{avg_y:.1f}c NO={ms['total_no_bought']}x@{avg_n:.1f}c inv={inv:+d}")
                            await self.mm_cancel_all_quotes(coin, "settlement guard")
                        continue

                    if cycle_sec < ROLLOVER_GUARD_SEC:
                        continue

                    quotes = self.mm_compute_quotes_as(coin)
                    if not quotes:
                        if ms["quotes_active"]:
                            await self.mm_cancel_all_quotes(coin, "no valid quote")
                        continue

                    yes_bid, no_bid = quotes

                    if ms["quotes_active"]:
                        yes_moved = abs(yes_bid - ms["yes_price"]) >= MM_REQUOTE_THRESHOLD_C if yes_bid > 0 and ms["yes_price"] > 0 else yes_bid != ms["yes_price"]
                        no_moved = abs(no_bid - ms["no_price"]) >= MM_REQUOTE_THRESHOLD_C if no_bid > 0 and ms["no_price"] > 0 else no_bid != ms["no_price"]
                        if not yes_moved and not no_moved:
                            continue

                    await self.mm_place_quotes(coin)
                except Exception as e:
                    print(f"  MM requote error ({coin}): {e}")

            await asyncio.sleep(0.01)

    async def mm_safety_reconcile_loop(self):
        """Every 10s: verify resting orders match local state."""
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
                            and (o.get("client_order_id") or "").startswith(("mm-", "sol-", "btc-", "eth-"))
                        ]
                        ms = self.mm_state[coin]

                        if len(resting_mm) > 2:
                            print(f"  SAFETY: {coin} has {len(resting_mm)} resting MM orders (max 2) — canceling ALL")
                            for o in resting_mm:
                                try:
                                    await asyncio.to_thread(self.client.cancel_order, o["order_id"])
                                except Exception:
                                    pass
                            ms["yes_order_id"] = None
                            ms["no_order_id"] = None
                            ms["quotes_active"] = False

                        elif not ms["quotes_active"] and resting_mm:
                            if not BRTI_COIN_CONFIG[coin].get("sol_sniper_mode") and not BRTI_COIN_CONFIG[coin].get("btc_distance_sniper"):
                                print(f"  SAFETY: {coin} local=inactive but {len(resting_mm)} orphan orders — canceling")
                                for o in resting_mm:
                                    try:
                                        await asyncio.to_thread(self.client.cancel_order, o["order_id"])
                                    except Exception:
                                        pass

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

    # ─── Position Sync ─────────────────────────────────────

    def _sync_kalshi_positions(self):
        """Read actual positions from Kalshi API. Called on every cycle rotation.
        This is the ONLY source of truth for what we hold."""
        if not self.client or DRY_RUN:
            return
        try:
            positions = self.client.get_positions(count_filter="position", settlement_status="unsettled")
            for mp in positions.get("market_positions", []):
                ticker = mp.get("ticker", "")
                position_fp = float(mp.get("position_fp", 0))
                if position_fp == 0 or not ticker:
                    continue
                coin = None
                for c, cfg in BRTI_COIN_CONFIG.items():
                    if ticker.startswith(cfg["series"]):
                        coin = c
                        break
                if not coin:
                    continue
                held_side = "yes" if position_fp > 0 else "no"
                count = int(abs(position_fp))
                self.ticker_contracts[ticker] = count
                self.ticker_contracts_signed[ticker] = int(position_fp)
                self.current_tickers[coin] = ticker
                # Set held_side in brti_state so sniper knows direction
                self.brti_state[coin]["held_side"] = held_side
                # Set mm_state inventory
                if coin in self.mm_state:
                    ms = self.mm_state[coin]
                    ms["inventory"] = count if held_side == "yes" else -count
                    exposure = float(mp.get("market_exposure_dollars", "0"))
                    entry_price = int(exposure / count * 100) if count > 0 else 50
                    if held_side == "yes":
                        ms["avg_yes_cost"] = entry_price
                    else:
                        ms["avg_no_cost"] = entry_price
                    # Determine mode based on position state
                    # Check if we have resting orders on the OTHER side (pair mode)
                    try:
                        resting = self.client.get_orders(ticker=ticker)
                        other_side_resting = False
                        for o in [x for x in resting.get("orders", []) if x.get("status") == "resting"]:
                            cid = o.get("client_order_id") or ""
                            o_side = o.get("side", "")
                            if o_side != held_side and cid.startswith(("btc-pair", "eth-pair", "btc-fade", "eth-fade")):
                                other_side_resting = True
                        if other_side_resting:
                            ms["_entry_mode"] = "pair"
                            if held_side == "yes":
                                ms["_pair_yes_filled"] = True
                            else:
                                ms["_pair_no_filled"] = True
                            ms["_pair_posted"] = True
                            print(f"  {coin} POSITION SYNC: {held_side.upper()} {count}x @ ~{entry_price}c — PAIR MODE (other side resting)")
                        else:
                            ms["_entry_mode"] = "sniper"
                            ms["_sniper_peak_bid"] = 0
                            print(f"  {coin} POSITION SYNC: {held_side.upper()} {count}x @ ~{entry_price}c — SNIPER MODE")
                    except Exception:
                        ms["_entry_mode"] = "sniper"
                        print(f"  {coin} POSITION SYNC: {held_side.upper()} {count}x @ ~{entry_price}c (ticker: {ticker})")
        except Exception as e:
            print(f"  Position sync error: {e}")

    # ─── Settlement History ───────────────────────────────────

    def seed_open_positions(self):
        """On restart, detect inherited positions."""
        self._sync_kalshi_positions()

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
                print(f"  {coin_name}: seeded {len(self.settlement_history[coin_name])} -> {self.settlement_history[coin_name]}")
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
                    print(f"  {coin_name} settled {result.upper()} -> {yes_ct}Y/{10 - yes_ct}N (last 10)")
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
                    if not st.get("_tick_pending", False):
                        continue
                    st["_tick_pending"] = False
                    coin_ticker = self.current_tickers.get(coin, "")
                    if not coin_ticker:
                        continue

                    # ── Entry check ──
                    flip_cooldown = cfg.get("flip_cooldown_sec", BRTI_FLIP_COOLDOWN_SEC)
                    if not self.mm_mode and st["direction"] and not st["entry_made"] and st["direction"] != "flat" and not st["held_side"] \
                            and (not st["last_flip_ts"] or (time.time() - st["last_flip_ts"]) >= flip_cooldown):
                        prices = self.ws_prices.get(coin_ticker, {})
                        yes_ask = prices.get("yes_ask", 0)
                        yes_bid = prices.get("yes_bid", 0)
                        if yes_ask <= 0 or yes_bid <= 0:
                            continue
                        if yes_ask > 0 and yes_bid > 0:
                            target_side = "yes" if st["direction"] == "up" else "no"
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
                            if secs_remaining <= 120:
                                smooth_ticks = [v for t, v in st["ticks"] if t > now.timestamp() - BRTI_SMOOTHING_WINDOW]
                                if smooth_ticks:
                                    smoothed = sum(smooth_ticks) / len(smooth_ticks)
                                    dist_past = smoothed - st["strike"] if target_side == "yes" else st["strike"] - smoothed
                                    if dist_past < cfg.get("conviction_min_distance", 50):
                                        continue
                                else:
                                    continue
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
                            total_exposure = sum(self.ticker_contracts.values())
                            if total_exposure >= 10:
                                continue
                            entry_max = _get_tiered_entry_max(cycle_sec, cfg)
                            entry_count = cfg.get("entry_contracts", 1)
                            tier_label = "T1" if cycle_sec < cfg.get("tier1_end_sec", BRTI_TIER1_END_SEC) else ("T2" if cycle_sec < tier2_end else "T3")
                            if 1 <= cost <= entry_max:
                                st["entry_made"] = True
                                st["held_side"] = target_side
                                st["entry_price"] = cost
                                st["peak_value"] = cost
                                self.ticker_contracts[coin_ticker] = self.ticker_contracts.get(coin_ticker, 0) + entry_count
                                print(f"[{now.strftime('%H:%M:%S')}] {coin} BRTI-ENTRY(fast) {target_side.upper()} {entry_count}x @ {cost}c [{tier_label}<={entry_max}c] (dir {st['direction']}, strike ${st['strike']:,.2f})")
                                await self._post_buy_async(coin_ticker, coin, target_side, cost, count=entry_count)
                        continue

                    # ── Flip/stop checks (only when holding) ──
                    if not st["held_side"]:
                        continue
                    if self.mm_mode:
                        continue
                    held = self.ticker_contracts.get(coin_ticker, 0)
                    flip_cooldown = cfg.get("flip_cooldown_sec", BRTI_FLIP_COOLDOWN_SEC)
                    if held <= 0 or (time.time() - st["last_flip_ts"]) <= flip_cooldown:
                        continue
                    prices = self.ws_prices.get(coin_ticker, {})
                    yes_bid = prices.get("yes_bid", 0)
                    yes_ask = prices.get("yes_ask", 0)
                    if yes_bid <= 0 or yes_ask <= 0:
                        continue

                    if st["held_side"] == "yes":
                        current_value = yes_bid
                    else:
                        current_value = 100 - yes_ask

                    if current_value > st["peak_value"]:
                        st["peak_value"] = current_value

                    take_profit_c = cfg.get("take_profit_c", BRTI_TAKE_PROFIT_C)

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
                    safe_sell_count = held
                    conviction_max_adds = cfg.get("conviction_max_adds", BRTI_CONVICTION_MAX_ADDS)
                    conviction_min_cycle_sec = cfg.get("conviction_min_cycle_sec", BRTI_CONVICTION_MIN_CYCLE_SEC)
                    conviction_cooldown_sec = cfg.get("conviction_cooldown_sec", BRTI_CONVICTION_COOLDOWN_SEC)
                    conviction_min_distance = cfg.get("conviction_min_distance", BRTI_CONVICTION_MIN_DISTANCE)
                    conviction_max_price = cfg.get("conviction_max_price", BRTI_CONVICTION_MAX_PRICE)
                    _now_flip = datetime.now()
                    _cycle_sec_flip = (_now_flip.minute % 15) * 60 + _now_flip.second
                    entry_max = _get_tiered_entry_max(_cycle_sec_flip, cfg)

                    # ── Take profit ──
                    if current_value >= take_profit_c:
                        old_side = st["held_side"]
                        profit = current_value - st["entry_price"]
                        entry_contracts = cfg.get("entry_contracts", 1)
                        safe_sell_count = max(1, min(held, entry_contracts + st.get("conviction_adds", 0)))
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] {coin} TAKE PROFIT: {old_side.upper()} {safe_sell_count}x @ {current_value}c (entry:{st['entry_price']}c pnl:+{profit}c)")
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
                                st["entry_made"] = True
                                st["peak_value"] = 0
                                st["entry_price"] = 0
                                print(f"  -> {coin} Sold {held}x @ {current_value}c — cycle done, no re-entry")
                            except Exception as e:
                                print(f"  -> {coin} Take profit error: {e}")
                        continue

                    latest_brti = st["ticks"][-1][1]
                    distance = latest_brti - st["strike"]
                    drop_from_peak = st["peak_value"] - current_value

                    # ── Conviction buy ──
                    cycle_now = datetime.now()
                    cycle_sec = (cycle_now.minute % 15) * 60 + cycle_now.second
                    total_exposure = sum(self.ticker_contracts.values())
                    if not self.mm_mode and (st["conviction_adds"] < conviction_max_adds
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
                                print(f"[{cycle_now.strftime('%H:%M:%S')}] {coin} CONVICTION BUY: {st['held_side'].upper()} 1x @ {buy_price}c (smoothed ${proj_distance:+,.0f} from strike, add {st['conviction_adds']}/{conviction_max_adds})")
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
                                        print(f"  -> {coin} Conviction buy error: {e}")

                    # ── Check trailing stop / stop loss ──
                    should_flip = False
                    new_side = ""
                    reason = ""
                    was_profitable = st["peak_value"] > st["entry_price"]
                    loss_from_entry = st["entry_price"] - current_value

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

                    # ── REGIME EXIT ──
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
                            old_side = st["held_side"]
                            pnl_val = current_value - st["entry_price"]
                            print(f"[{datetime.now().strftime('%H:%M:%S')}] {coin} REGIME EXIT: SELL {old_side.upper()} @ {current_value}c (pnl:{pnl_val:+d}c, proj ${wrong_side_distance:,.0f} wrong side) — flat, re-evaluating")
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
                                    st["direction"] = ""
                                except Exception as e:
                                    print(f"  -> {coin} Regime exit error: {e}")
                            await asyncio.sleep(0.5)
                            continue

                    # ── HARD STOP ──
                    if loss_from_entry >= hard_stop:
                            old_side = st["held_side"]
                            pnl_val = current_value - st["entry_price"]
                            print(f"[{datetime.now().strftime('%H:%M:%S')}] {coin} HARD STOP: SELL {old_side.upper()} @ {current_value}c (entry:{st['entry_price']}c loss:{loss_from_entry}c) — flat")
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
                                    st["direction"] = ""
                                    print(f"  -> {coin} Direction reset — will re-evaluate before re-entry")
                                except Exception as e:
                                    print(f"  -> {coin} Hard stop error: {e}")
                            await asyncio.sleep(0.5)
                            continue

                    # Old flip logic disabled — regime exit goes flat instead
                    should_flip = False
                    if should_flip:
                        st["flip_confirm_ticks"] += 1
                        if st["flip_confirm_ticks"] < 4:
                            continue
                        st["flip_confirm_ticks"] = 0
                    else:
                        st["flip_confirm_ticks"] = 0

                    if should_flip:
                        old_side = st["held_side"]
                        sell_price = current_value
                        new_cost = yes_ask if new_side == "yes" else (100 - yes_bid)
                        pnl = sell_price - st["entry_price"]
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] {coin} FLIP: {reason} | SELL {old_side.upper()} @ {sell_price}c (pnl:{pnl:+d}c) -> BUY {new_side.upper()} @ {new_cost}c")

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

                                flip_buy_count = held + 1
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
                                    print(f"  -> {coin} Flipped to {new_side.upper()} {flip_buy_count}x @ {new_cost}c (was {held}x)")
                                else:
                                    st["held_side"] = ""
                                    print(f"  -> {coin} Sold, new side too expensive ({new_cost}c)")
                                st["last_flip_ts"] = time.time()
                            except Exception as e:
                                print(f"  -> {coin} Flip error: {e}")
                except Exception as e:
                    print(f"  Fast flip loop error ({coin}): {e}")
            await asyncio.sleep(0.05)

    # ─── Periodic Tasks ───────────────────────────────────────

    def compute_synthetic_brti(self, coin):
        """Volume-weighted median of exchange trade prices for a specific coin."""
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
            st["ticks"].append((now, synthetic))
            cutoff = now - 960
            st["ticks"] = [(t, v) for t, v in st["ticks"] if t > cutoff]
            st["_tick_pending"] = True
            if self.mm_mode and coin in self.mm_state:
                self.mm_state[coin]["requote_pending"] = True

    def _update_rti_orderbook(self, exchange, coin, bids, asks):
        """Feed orderbook data into the CF RTI engine for this coin."""
        if coin in self.rti_engines:
            self.rti_engines[coin].update_orderbook(exchange, bids, asks)

    # ─── Exchange WebSocket Feeds ─────────────────────────────

    async def _coinbase_ws(self):
        """Coinbase Exchange WS: ticker for trades, level2_batch for orderbook."""
        while self.running:
            try:
                async with websockets.connect(
                    BRTI_EXCHANGES["coinbase"]["url"],
                    ping_interval=30, ping_timeout=10,
                    max_size=10_000_000,
                ) as ws:
                    await ws.send(json.dumps(BRTI_EXCHANGES["coinbase"]["subscribe_ticker"]))
                    await ws.send(json.dumps(BRTI_EXCHANGES["coinbase"]["subscribe_l2"]))
                    self.exchange_status["coinbase"] = "connected"
                    print("  [Coinbase] connected (ticker + L2)")

                    async for msg in ws:
                        try:
                            data = json.loads(msg)
                            msg_type = data.get("type", "")

                            if msg_type == "ticker":
                                product_id = data.get("product_id", "")
                                coin = PAIR_TO_COIN.get(("coinbase", product_id))
                                if coin:
                                    price = float(data.get("price", 0))
                                    vol = float(data.get("last_size", 0))
                                    self._record_exchange_trade("coinbase", coin, price, vol)

                            elif msg_type == "snapshot":
                                product_id = data.get("product_id", "")
                                coin = PAIR_TO_COIN.get(("coinbase", product_id))
                                if coin:
                                    bids_raw = data.get("bids", [])
                                    asks_raw = data.get("asks", [])
                                    bids = [(float(p), float(s)) for p, s in bids_raw]
                                    asks = [(float(p), float(s)) for p, s in asks_raw]
                                    self.coinbase_books[coin] = {
                                        "bids": {p: s for p, s in bids},
                                        "asks": {p: s for p, s in asks},
                                    }
                                    self._update_rti_orderbook(
                                        "coinbase", coin,
                                        sorted(bids, key=lambda x: -x[0])[:50],
                                        sorted(asks, key=lambda x: x[0])[:50],
                                    )

                            elif msg_type == "l2update":
                                product_id = data.get("product_id", "")
                                coin = PAIR_TO_COIN.get(("coinbase", product_id))
                                if coin and coin in self.coinbase_books:
                                    book = self.coinbase_books[coin]
                                    for change in data.get("changes", []):
                                        if len(change) < 3:
                                            continue
                                        side_str, price_str, size_str = change[0], change[1], change[2]
                                        price = float(price_str)
                                        size = float(size_str)
                                        side_dict = book["bids"] if side_str == "buy" else book["asks"]
                                        if size == 0:
                                            side_dict.pop(price, None)
                                        else:
                                            side_dict[price] = size
                                    bid_list = sorted(book["bids"].items(), key=lambda x: -x[0])[:50]
                                    ask_list = sorted(book["asks"].items(), key=lambda x: x[0])[:50]
                                    self._update_rti_orderbook("coinbase", coin, bid_list, ask_list)

                        except Exception:
                            pass
            except Exception as e:
                self.exchange_status["coinbase"] = "error"
                print(f"  [Coinbase] WS error: {e}, reconnecting...")
                await asyncio.sleep(5)

    async def _kraken_ws(self):
        """Kraken WS: trade + book channels for all coins."""
        while self.running:
            try:
                async with websockets.connect(
                    BRTI_EXCHANGES["kraken"]["url"],
                    ping_interval=30, ping_timeout=10,
                ) as ws:
                    await ws.send(json.dumps(BRTI_EXCHANGES["kraken"]["subscribe_trade"]))
                    await ws.send(json.dumps(BRTI_EXCHANGES["kraken"]["subscribe_book"]))
                    self.exchange_status["kraken"] = "connected"
                    print("  [Kraken] connected (trade + book)")

                    async for msg in ws:
                        try:
                            data = json.loads(msg)
                            if isinstance(data, list) and len(data) >= 4:
                                pair_name = data[-1] if isinstance(data[-1], str) else ""
                                channel_name = data[-2] if len(data) >= 4 and isinstance(data[-2], str) else ""
                                coin = PAIR_TO_COIN.get(("kraken", pair_name))
                                if not coin:
                                    continue

                                if channel_name == "trade":
                                    trades = data[1] if isinstance(data[1], list) else []
                                    for trade in trades:
                                        if isinstance(trade, list) and len(trade) >= 2:
                                            self._record_exchange_trade(
                                                "kraken", coin,
                                                float(trade[0]), float(trade[1]),
                                            )

                                elif channel_name.startswith("book"):
                                    book_data = data[1] if isinstance(data[1], dict) else {}
                                    if coin not in self.kraken_books:
                                        self.kraken_books[coin] = {"bids": {}, "asks": {}}
                                    kb = self.kraken_books[coin]

                                    for bid in book_data.get("bs", []) + book_data.get("b", []):
                                        if len(bid) >= 2:
                                            p, s = float(bid[0]), float(bid[1])
                                            if s == 0:
                                                kb["bids"].pop(p, None)
                                            else:
                                                kb["bids"][p] = s
                                    for ask in book_data.get("as", []) + book_data.get("a", []):
                                        if len(ask) >= 2:
                                            p, s = float(ask[0]), float(ask[1])
                                            if s == 0:
                                                kb["asks"].pop(p, None)
                                            else:
                                                kb["asks"][p] = s

                                    if len(data) >= 5 and isinstance(data[2], dict):
                                        book_data2 = data[2]
                                        for bid in book_data2.get("b", []):
                                            if len(bid) >= 2:
                                                p, s = float(bid[0]), float(bid[1])
                                                if s == 0:
                                                    kb["bids"].pop(p, None)
                                                else:
                                                    kb["bids"][p] = s
                                        for ask in book_data2.get("a", []):
                                            if len(ask) >= 2:
                                                p, s = float(ask[0]), float(ask[1])
                                                if s == 0:
                                                    kb["asks"].pop(p, None)
                                                else:
                                                    kb["asks"][p] = s

                                    bid_list = sorted(kb["bids"].items(), key=lambda x: -x[0])[:50]
                                    ask_list = sorted(kb["asks"].items(), key=lambda x: x[0])[:50]
                                    self._update_rti_orderbook("kraken", coin, bid_list, ask_list)

                        except Exception:
                            pass
            except Exception as e:
                self.exchange_status["kraken"] = "error"
                print(f"  [Kraken] WS error: {e}, reconnecting...")
                await asyncio.sleep(5)

    async def _bitstamp_ws(self):
        """Bitstamp WS: live trades + order book for all coins."""
        while self.running:
            try:
                async with websockets.connect(
                    BRTI_EXCHANGES["bitstamp"]["url"],
                    ping_interval=30, ping_timeout=10,
                ) as ws:
                    for channel in BRTI_EXCHANGES["bitstamp"]["channels"]:
                        await ws.send(json.dumps({
                            "event": "bts:subscribe",
                            "data": {"channel": f"live_trades_{channel}"},
                        }))
                        await ws.send(json.dumps({
                            "event": "bts:subscribe",
                            "data": {"channel": f"order_book_{channel}"},
                        }))
                    self.exchange_status["bitstamp"] = "connected"
                    print("  [Bitstamp] connected (trades + orderbook)")

                    async for msg in ws:
                        try:
                            data = json.loads(msg)
                            event = data.get("event", "")
                            channel = data.get("channel", "")

                            if event == "trade":
                                pair = channel.replace("live_trades_", "")
                                coin = PAIR_TO_COIN.get(("bitstamp", pair))
                                if coin:
                                    td = data.get("data", {})
                                    self._record_exchange_trade(
                                        "bitstamp", coin,
                                        float(td.get("price", 0)),
                                        float(td.get("amount", 0)),
                                    )

                            elif event == "data" and "order_book_" in channel:
                                pair = channel.replace("order_book_", "")
                                coin = PAIR_TO_COIN.get(("bitstamp", pair))
                                if coin:
                                    od = data.get("data", {})
                                    bids = [(float(p), float(s)) for p, s in od.get("bids", [])[:50]]
                                    asks = [(float(p), float(s)) for p, s in od.get("asks", [])[:50]]
                                    self._update_rti_orderbook("bitstamp", coin, bids, asks)

                        except Exception:
                            pass
            except Exception as e:
                self.exchange_status["bitstamp"] = "error"
                print(f"  [Bitstamp] WS error: {e}, reconnecting...")
                await asyncio.sleep(5)

    async def _gemini_ws_single(self, symbol, coin):
        """Gemini WS for a single symbol: trades + orderbook.
        Throttled: only push to RTI on bid/ask changes, max every 100ms, to avoid slow-consumer disconnects.
        """
        url = f"wss://api.gemini.com/v1/marketdata/{symbol}?trades=true&bids=true&offers=true"
        gemini_book = {"bids": {}, "asks": {}}
        last_push_ts = 0.0
        while self.running:
            try:
                async with websockets.connect(url, ping_interval=30, ping_timeout=10) as ws:
                    self.exchange_status[f"gemini_{coin}"] = "connected"
                    print(f"  [Gemini {coin}] connected")

                    async for msg in ws:
                        try:
                            data = json.loads(msg)
                            book_changed = False  # only push RTI when actual bid/ask update
                            for ev in data.get("events", []):
                                ev_type = ev.get("type", "")
                                if ev_type == "trade":
                                    self._record_exchange_trade(
                                        "gemini", coin,
                                        float(ev.get("price", 0)),
                                        float(ev.get("amount", 0)),
                                    )
                                elif ev_type == "change":
                                    side = ev.get("side", "")
                                    if side not in ("bid", "ask"):
                                        continue  # skip "auction" and other sides
                                    price = float(ev.get("price", 0))
                                    remaining = float(ev.get("remaining", 0))
                                    if side == "bid":
                                        if remaining == 0:
                                            gemini_book["bids"].pop(price, None)
                                        else:
                                            gemini_book["bids"][price] = remaining
                                    else:  # ask
                                        if remaining == 0:
                                            gemini_book["asks"].pop(price, None)
                                        else:
                                            gemini_book["asks"][price] = remaining
                                    book_changed = True

                            # Throttle: only push to RTI on real book changes, max every 100ms
                            now = time.time()
                            if book_changed and (now - last_push_ts) >= 0.1:
                                if gemini_book["bids"] or gemini_book["asks"]:
                                    bid_list = sorted(gemini_book["bids"].items(), key=lambda x: -x[0])[:50]
                                    ask_list = sorted(gemini_book["asks"].items(), key=lambda x: x[0])[:50]
                                    self._update_rti_orderbook("gemini", coin, bid_list, ask_list)
                                    last_push_ts = now

                        except Exception as e:
                            if "book" in str(e).lower():
                                print(f"  [Gemini {coin}] parse error: {e}")
            except Exception as e:
                self.exchange_status[f"gemini_{coin}"] = "error"
                print(f"  [Gemini {coin}] WS error: {e}, reconnecting...")
                await asyncio.sleep(5)

    async def _gemini_ws(self):
        """Launch Gemini WS connections for all BRTI coins."""
        tasks = []
        for coin, symbol in BRTI_EXCHANGES["gemini"]["symbols"].items():
            tasks.append(asyncio.create_task(self._gemini_ws_single(symbol, coin)))
        await asyncio.gather(*tasks)

    async def _crypto_com_ws(self):
        """Crypto.com Exchange WS: orderbook for all coins.
        - Depth 50 (was 10) for better RTI sampling
        - Replies to public/heartbeat within 5s or connection gets killed
        """
        while self.running:
            try:
                async with websockets.connect(
                    BRTI_EXCHANGES["crypto_com"]["url"],
                    ping_interval=30, ping_timeout=10,
                ) as ws:
                    channels = []
                    for coin, pair in BRTI_EXCHANGES["crypto_com"]["pairs"].items():
                        channels.append(f"book.{pair}.50")  # depth 50 (was 10)
                    sub_msg = {
                        "id": 1,
                        "method": "subscribe",
                        "params": {"channels": channels},
                    }
                    await ws.send(json.dumps(sub_msg))
                    self.exchange_status["crypto_com"] = "connected"
                    print("  [Crypto.com] connected (orderbook depth=50)")

                    async for msg in ws:
                        try:
                            data = json.loads(msg)

                            # CRITICAL: reply to heartbeat within 5s or connection dies
                            if data.get("method") == "public/heartbeat":
                                await ws.send(json.dumps({
                                    "id": data.get("id", 0),
                                    "method": "public/respond-heartbeat",
                                }))
                                continue

                            result = data.get("result", {})
                            channel = result.get("channel", "")
                            if not channel.startswith("book."):
                                continue
                            parts = channel.split(".")
                            if len(parts) < 2:
                                continue
                            pair_str = parts[1]
                            coin = None
                            for c, p in BRTI_EXCHANGES["crypto_com"]["pairs"].items():
                                if p == pair_str:
                                    coin = c
                                    break
                            if not coin:
                                continue

                            # data is an array; iterate all updates in the frame
                            book_updates = result.get("data", [])
                            if not isinstance(book_updates, list):
                                book_updates = [book_updates]
                            for book_data in book_updates:
                                if not isinstance(book_data, dict):
                                    continue
                                bids = [(float(b[0]), float(b[1])) for b in book_data.get("bids", [])[:50]]
                                asks = [(float(a[0]), float(a[1])) for a in book_data.get("asks", [])[:50]]
                                self._update_rti_orderbook("crypto_com", coin, bids, asks)
                                if bids and asks:
                                    mid = (bids[0][0] + asks[0][0]) / 2
                                    self._record_exchange_trade("crypto_com", coin, mid, 0.001)

                        except Exception as e:
                            if "book" in str(e).lower() or "heartbeat" in str(e).lower():
                                print(f"  [Crypto.com] parse error: {e}")
            except Exception as e:
                self.exchange_status["crypto_com"] = "error"
                print(f"  [Crypto.com] WS error: {e}, reconnecting...")
                await asyncio.sleep(5)

    async def settlement_check_loop(self):
        """Market subscription + settlement checks. Polls every 1s until Kalshi pricing received, then every 30s."""
        while self.running:
            try:
                for coin, series in COINS.items():
                    self.check_new_settlement(coin, series)

                if self.ws_connected:
                    await self._subscribe_open_markets()

                balance = self.get_balance() or self.last_balance
                if balance > self.current_cash_floor:
                    new_floor = balance * RATCHET_PERCENT
                    if new_floor > self.current_cash_floor:
                        self.current_cash_floor = new_floor
                self.last_balance = balance

                minutes_remaining = 15 - (datetime.now().minute % 15)
                coin_parts = []
                for _coin in BRTI_COIN_CONFIG:
                    _st = self.brti_state[_coin]
                    _val = f"${_st['ticks'][-1][1]:,.2f}" if _st["ticks"] else "?"
                    _age = f"{time.time() - _st['ticks'][-1][0]:.1f}s" if _st["ticks"] else "?"
                    _ticker = self.current_tickers.get(_coin, "")
                    _signed = self.ticker_contracts_signed.get(_ticker, 0)
                    _held = abs(_signed)
                    _side = "YES" if _signed > 0 else ("NO" if _signed < 0 else "flat")
                    # Direction from current price vs strike
                    _price = _st['ticks'][-1][1] if _st['ticks'] else 0
                    _dir = "up" if _price > _st['strike'] and _st['strike'] > 0 else ("down" if _price < _st['strike'] and _st['strike'] > 0 else "wait")
                    coin_parts.append(f"{_coin}:{_val}({_age}) stk${_st['strike']:,.0f} {_dir} {_side}{_held}x")
                feeds = sum(1 for s in self.exchange_status.values() if s == "connected")
                num_feeds_expected = len(BRTI_COIN_CONFIG) + 4
                status = " | ".join(coin_parts) + f" | feeds:{feeds}/{num_feeds_expected}"
                probation_tag = " [BTC ONLY]"
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Min left: {minutes_remaining} | {status} | WS: {'Y' if self.ws_connected else 'N'}{probation_tag}")

            except Exception as e:
                print(f"Settlement check error: {e}")

            # Poll rate: 1s until Kalshi pricing received, then sleep until next cycle boundary
            btc_ticker = self.current_tickers.get("BTC", "")
            has_kalshi_pricing = bool(self.ws_prices.get(btc_ticker, {}).get("yes_bid", 0))
            if not has_kalshi_pricing:
                await asyncio.sleep(1)  # fast poll until we have pricing
            else:
                # Sleep until next cycle boundary (:00, :15, :30, :45) + 1s buffer
                now2 = datetime.now()
                cycle_sec_now = (now2.minute % 15) * 60 + now2.second
                sec_to_boundary = max(1, 900 - cycle_sec_now + 1)
                await asyncio.sleep(sec_to_boundary)

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
        print(f"Crypto 15m Agent - {mode_str} - {coins_str}")
        if self.mm_mode:
            print(f"   Mode: MM | requote:{MM_REQUOTE_THRESHOLD_C}c | contracts:{MM_MAX_CONTRACTS}")
            print(f"   Safety: reconcile every {MM_RECONCILE_INTERVAL_SEC}s | max 2 resting/market | cancel-on-fill | cancel-on-rotate")
        else:
            print(f"   Mode: Directional tiered entry (rollback: git checkout v1-directional-tiered)")
        for _coin, _cfg in BRTI_COIN_CONFIG.items():
            _guard = _cfg.get("mm_settle_guard_sec", MM_SETTLE_GUARD_SEC)
            sniper_tag = " [BTC SNIPER]" if _cfg.get("btc_distance_sniper") else (" [SHADOW MM]" if _cfg.get("eth_shadow_mm") else (" [SNIPER]" if _cfg.get("sol_sniper_mode") else ""))
            if _cfg.get("probation_disabled"):
                sniper_tag += " [DISABLED]"
            print(f"   {_coin}: sigma/sec={_cfg.get('sigma_per_sec', '?')} | max_inv:{MM_MAX_INVENTORY.get(_coin, 8)} | g={MM_GAMMA.get(_coin, 0.5)} | k_default={MM_KAPPA_DEFAULT:.4f}/s | spread_floor:{MM_SPREAD_FLOOR_C}c | settle_guard:{_guard}s{sniper_tag}")
        print(f"   Source: synthetic (Coinbase+Kraken+Bitstamp+Gemini+Crypto.com WebSockets) + CF RTI orderbook")
        print(f"   DRY_RUN: {DRY_RUN} | MM_MODE: {self.mm_mode}")

        print("Seeding settlement history...")
        self.seed_settlement_history()

        if self.client and self.key_id and self.private_key_path:
            asyncio.create_task(self.kalshi_websocket())
        asyncio.create_task(self.settlement_check_loop())
        asyncio.create_task(self._coinbase_ws())
        asyncio.create_task(self._kraken_ws())
        asyncio.create_task(self._bitstamp_ws())
        asyncio.create_task(self._gemini_ws())
        asyncio.create_task(self._crypto_com_ws())
        # Legacy directional loop disabled — BTC sniper handles all trading
        # asyncio.create_task(self.brti_fast_flip_loop())
        if self.mm_mode:
            asyncio.create_task(self.mm_requote_loop())
            asyncio.create_task(self.mm_safety_reconcile_loop())
        brti_coins_str = "+".join(BRTI_COIN_CONFIG.keys())
        print(f"Synthetic index feeds launching ({brti_coins_str}, 5 exchanges) + fast flip loop + {'MM quoting' if self.mm_mode else 'directional entry'}")

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
