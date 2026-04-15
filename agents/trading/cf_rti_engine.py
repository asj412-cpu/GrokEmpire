"""
CF Benchmarks Real Time Index Engine
=====================================
Implements the exact CME CF Cryptocurrency Real Time Index methodology
from the official methodology guide v16.5 (22 Feb 2026).

Key difference from our old sBRTI: this uses ORDER BOOK data (bids/asks),
not trade data. The real BRTI aggregates order books from all constituent
exchanges, computes a mid price-volume curve, and applies exponential weighting.

Constituent exchanges per coin:
  BTC: Bitstamp, Coinbase, itBit, Kraken, Gemini, Bullish, Crypto.com, LMAX Digital
  ETH: Bitstamp, Coinbase, itBit, Kraken, Gemini, Bullish, Crypto.com, LMAX Digital
  SOL: Coinbase, Kraken, Gemini, LMAX Digital, Bitstamp, Crypto.com
  XRP: Bitstamp, Kraken, LMAX Digital, Coinbase, Crypto.com
"""

import math
import time
from typing import Dict, List, Tuple, Optional
from collections import defaultdict


# CF Benchmarks RTI parameters per coin (from methodology PDF Section 6.2)
RTI_PARAMS = {
    "BTC": {"spacing": 1, "deviation_pct": 0.005, "erroneous_pct": 0.05},      # s=1 BTC, D=0.5%
    "ETH": {"spacing": 25, "deviation_pct": 0.01, "erroneous_pct": 0.05},      # s=25 ETH, D=1%
    "SOL": {"spacing": 100, "deviation_pct": 0.01, "erroneous_pct": 0.05},     # s=100 SOL, D=1%
    "XRP": {"spacing": 10000, "deviation_pct": 0.01, "erroneous_pct": 0.10},   # s=10000 XRP, D=1%
}

# Constituent exchanges per coin (from constituent exchanges PDF v13.4)
CONSTITUENT_EXCHANGES = {
    "BTC": ["bitstamp", "coinbase", "itbit", "kraken", "gemini", "bullish", "crypto_com", "lmax"],
    "ETH": ["bitstamp", "coinbase", "itbit", "kraken", "gemini", "bullish", "crypto_com", "lmax"],
    "SOL": ["coinbase", "kraken", "gemini", "lmax", "bitstamp", "crypto_com"],
    "XRP": ["bitstamp", "kraken", "lmax", "coinbase", "crypto_com"],
}


class OrderBook:
    """Represents a single exchange's order book for one coin."""

    def __init__(self, exchange: str, coin: str):
        self.exchange = exchange
        self.coin = coin
        self.bids: List[Tuple[float, float]] = []  # [(price, size), ...] descending by price
        self.asks: List[Tuple[float, float]] = []  # [(price, size), ...] ascending by price
        self.last_update: float = 0.0               # timestamp of last update

    def update(self, bids: List[Tuple[float, float]], asks: List[Tuple[float, float]]):
        """Update the order book with new bid/ask data."""
        self.bids = sorted(bids, key=lambda x: -x[0])  # descending by price
        self.asks = sorted(asks, key=lambda x: x[0])    # ascending by price
        self.last_update = time.time()

    def is_valid(self, calc_time: float) -> bool:
        """Check if this order book is valid for calculation (not stale, not erroneous)."""
        # Rule 5.1: disregard if retrieval time is >30s older than calculation time
        if calc_time - self.last_update > 30:
            return False
        # Rule 5.2.1: must have both bids and asks
        if not self.bids or not self.asks:
            return False
        # Rule 5.2.1: must not cross (best bid < best ask)
        if self.bids[0][0] >= self.asks[0][0]:
            return False
        return True

    def mid_price(self) -> float:
        """Current mid-price (highest bid + lowest ask) / 2."""
        if self.bids and self.asks:
            return (self.bids[0][0] + self.asks[0][0]) / 2
        return 0.0


class CFRealTimeIndex:
    """
    Computes the CME CF Cryptocurrency Real Time Index using the exact
    methodology from the official methodology guide.

    Usage:
        rti = CFRealTimeIndex("BTC")
        rti.update_orderbook("coinbase", bids=[(74500, 1.5), ...], asks=[(74501, 0.8), ...])
        rti.update_orderbook("kraken", bids=[...], asks=[...])
        value = rti.compute()
    """

    def __init__(self, coin: str):
        self.coin = coin
        self.params = RTI_PARAMS.get(coin, RTI_PARAMS["BTC"])
        self.exchanges = CONSTITUENT_EXCHANGES.get(coin, [])
        self.order_books: Dict[str, OrderBook] = {}
        self.last_value: float = 0.0
        self.last_compute_time: float = 0.0

        # Initialize order books for each constituent exchange
        for ex in self.exchanges:
            self.order_books[ex] = OrderBook(ex, coin)

    def update_orderbook(self, exchange: str, bids: List[Tuple[float, float]],
                          asks: List[Tuple[float, float]]):
        """Update a constituent exchange's order book."""
        if exchange in self.order_books:
            self.order_books[exchange].update(bids, asks)

    def _compute_dynamic_order_size_cap(self, consolidated_asks, consolidated_bids) -> float:
        """
        Compute dynamic order size cap C_T (Section 4.1.3).

        Steps:
        1. Take ask orders within 5% of best ask price (up to 50)
        2. Take bid orders within 5% of best bid price (up to 50)
        3. Combine sizes, sort ascending
        4. Trim 1% from each end
        5. Cap = trimmed_mean + 5 * winsorized_std_dev
        """
        if not consolidated_asks or not consolidated_bids:
            return float('inf')

        best_ask = consolidated_asks[0][0]
        best_bid = consolidated_bids[0][0]

        # Eq 4a: ask sizes within 5% of best ask (up to 50)
        ask_sizes = [s for p, s in consolidated_asks if p <= 1.05 * best_ask][:50]
        # Eq 4b: bid sizes within 5% of best bid (up to 50)
        bid_sizes = [s for p, s in consolidated_bids if p >= 0.95 * best_bid][:50]

        # Eq 4c: combine and sort
        S = sorted(ask_sizes + bid_sizes)
        n = len(S)
        if n < 3:
            return float('inf')

        # Eq 4d: trimming size
        k = int(0.01 * n)

        # Eq 4e: trimmed mean
        if n - 2 * k <= 0:
            trimmed_mean = sum(S) / n
        else:
            trimmed_mean = sum(S[k:n - k]) / (n - 2 * k)

        # Eq 4f: winsorized sample
        S_w = S.copy()
        for i in range(k):
            S_w[i] = S[k]
        for i in range(n - k, n):
            S_w[i] = S[n - k - 1]

        # Eq 4g: winsorized mean
        w_mean = sum(S_w) / n

        # Eq 4h: winsorized std dev
        w_var = sum((x - w_mean) ** 2 for x in S_w) / (n - 1) if n > 1 else 0
        w_std = math.sqrt(w_var)

        # Eq 5: cap = trimmed_mean + 5σ
        cap = trimmed_mean + 5 * w_std
        return max(cap, 0.001)  # avoid zero cap

    def _build_consolidated_book(self, calc_time: float):
        """
        Build consolidated order book from all valid constituent exchanges.
        Applies order size cap and potentially erroneous data filtering.

        Returns (consolidated_bids, consolidated_asks) as lists of (price, capped_size).
        """
        all_bids = []
        all_asks = []
        mid_prices = []

        # Collect valid order books
        valid_books = []
        for ex, book in self.order_books.items():
            if book.is_valid(calc_time):
                valid_books.append(book)
                mid_prices.append(book.mid_price())

        if not valid_books:
            return [], []

        # Rule 5.3: filter potentially erroneous data
        if len(mid_prices) >= 2:
            median_mid = sorted(mid_prices)[len(mid_prices) // 2]
            erroneous_threshold = self.params["erroneous_pct"]
            filtered_books = []
            for book in valid_books:
                deviation = abs(book.mid_price() - median_mid) / median_mid if median_mid > 0 else 0
                if deviation <= erroneous_threshold:
                    filtered_books.append(book)
            valid_books = filtered_books if filtered_books else valid_books

        # Aggregate all orders
        for book in valid_books:
            all_bids.extend(book.bids)
            all_asks.extend(book.asks)

        # Sort: bids descending, asks ascending
        all_bids.sort(key=lambda x: -x[0])
        all_asks.sort(key=lambda x: x[0])

        # Aggregate by price level (sum sizes at same price)
        bid_levels = defaultdict(float)
        for price, size in all_bids:
            bid_levels[price] += size
        ask_levels = defaultdict(float)
        for price, size in all_asks:
            ask_levels[price] += size

        # Compute dynamic order size cap from uncapped book
        uncapped_bids = sorted(bid_levels.items(), key=lambda x: -x[0])
        uncapped_asks = sorted(ask_levels.items(), key=lambda x: x[0])
        cap = self._compute_dynamic_order_size_cap(uncapped_asks, uncapped_bids)

        # Apply cap: each price level's size is min(size, cap)
        capped_bids = [(p, min(s, cap)) for p, s in uncapped_bids]
        capped_asks = [(p, min(s, cap)) for p, s in uncapped_asks]

        return capped_bids, capped_asks

    def compute(self) -> Optional[float]:
        """
        Compute the CME CF Real Time Index value.

        Returns the index value, or None if calculation fails.
        """
        calc_time = time.time()
        spacing = self.params["spacing"]
        deviation_max = self.params["deviation_pct"]

        # Step 1-2: Build consolidated order book
        bids, asks = self._build_consolidated_book(calc_time)
        if not bids or not asks:
            return None

        # Step 3: Compute price-volume curves at spacing granularity
        # askPV(v): marginal ask price at cumulative volume v
        # bidPV(v): marginal bid price at cumulative volume v
        # We sample at v = s, 2s, 3s, ...

        # Build cumulative volume → price mappings
        ask_cum = []  # [(cumulative_volume, price), ...]
        cum_vol = 0
        for price, size in asks:
            cum_vol += size
            ask_cum.append((cum_vol, price))

        bid_cum = []
        cum_vol = 0
        for price, size in bids:
            cum_vol += size
            bid_cum.append((cum_vol, price))

        if not ask_cum or not bid_cum:
            return None

        max_volume = min(ask_cum[-1][0], bid_cum[-1][0])
        if max_volume < spacing:
            # Not enough depth — use simple mid price
            mid = (bids[0][0] + asks[0][0]) / 2
            self.last_value = mid
            self.last_compute_time = calc_time
            return mid

        def price_at_volume(cum_data, v):
            """Get the marginal price at cumulative volume v (Eq 1a/1b with spacing, Eq 1c/1d)."""
            # Find the price level where cumulative volume first exceeds v
            # Apply spacing: round v to nearest spacing multiple
            for cum_vol, price in cum_data:
                if cum_vol >= v:
                    return price
            return cum_data[-1][1] if cum_data else 0

        # Step 3c/3d: Compute midPV and midSV at each spacing point
        volumes = []
        mid_pvs = []
        mid_svs = []

        v = spacing
        while v <= max_volume:
            ask_p = price_at_volume(ask_cum, v)
            bid_p = price_at_volume(bid_cum, v)

            if ask_p <= 0 or bid_p <= 0:
                break

            mid_p = (ask_p + bid_p) / 2
            if mid_p <= 0:
                break

            mid_s = (ask_p / mid_p) - 1  # Eq 1f: percentage deviation of ask from mid

            volumes.append(v)
            mid_pvs.append(mid_p)
            mid_svs.append(mid_s)

            v += spacing

        if not volumes:
            return None

        # Step 4: Find utilized depth v_T (Eq 2)
        # Max volume where midSV ≤ D
        utilized_depth = spacing  # minimum
        for i, (vol, sv) in enumerate(zip(volumes, mid_svs)):
            if sv <= deviation_max:
                utilized_depth = vol
            else:
                break

        # Step 5-6: Compute weighted index (Eq 3)
        # λ = 1 / (0.3 * v_T)
        lam = 1.0 / (0.3 * utilized_depth) if utilized_depth > 0 else 1.0

        # Normalization factor: sum of λ * e^(-λv) for v in {s, 2s, ..., v_T}
        weight_sum = 0
        for vol in volumes:
            if vol > utilized_depth:
                break
            weight_sum += lam * math.exp(-lam * vol)

        if weight_sum <= 0:
            return None

        # CCRTI = Σ midPV(v) * (1/NF) * λ * e^(-λv)
        index_value = 0
        for vol, mid_p in zip(volumes, mid_pvs):
            if vol > utilized_depth:
                break
            weight = lam * math.exp(-lam * vol) / weight_sum
            index_value += mid_p * weight

        self.last_value = index_value
        self.last_compute_time = calc_time
        return index_value

    def get_status(self) -> dict:
        """Get current status of all constituent exchange feeds."""
        now = time.time()
        status = {}
        for ex, book in self.order_books.items():
            age = now - book.last_update if book.last_update > 0 else float('inf')
            status[ex] = {
                "valid": book.is_valid(now),
                "age_sec": round(age, 1),
                "bids": len(book.bids),
                "asks": len(book.asks),
                "mid": round(book.mid_price(), 2) if book.bids and book.asks else None,
            }
        return status


# ─── Exchange WebSocket Adapters ─────────────────────────────────────
# Each exchange has a different WS API for order book data.
# These adapters normalize the data into (price, size) tuples.

EXCHANGE_WS_CONFIG = {
    "coinbase": {
        "url": "wss://ws-feed.exchange.coinbase.com",
        "pairs": {"BTC": "BTC-USD", "ETH": "ETH-USD", "SOL": "SOL-USD", "XRP": "XRP-USD"},
        "channel": "level2_batch",  # L2 orderbook updates
    },
    "kraken": {
        "url": "wss://ws.kraken.com",
        "pairs": {"BTC": "XBT/USD", "ETH": "ETH/USD", "SOL": "SOL/USD", "XRP": "XRP/USD"},
        "channel": "book",  # depth=25 orderbook
    },
    "bitstamp": {
        "url": "wss://ws.bitstamp.net",
        "pairs": {"BTC": "btcusd", "ETH": "ethusd", "SOL": "solusd", "XRP": "xrpusd"},
        "channel": "order_book",
    },
    "gemini": {
        "url_template": "wss://api.gemini.com/v1/marketdata/{symbol}?top_of_book=false",
        "pairs": {"BTC": "BTCUSD", "ETH": "ETHUSD", "SOL": "SOLUSD", "XRP": "XRPUSD"},
    },
    # TODO: Add these when we have API access
    "lmax": {
        "url": None,  # LMAX Digital requires institutional API access
        "pairs": {"BTC": "BTC/USD", "ETH": "ETH/USD", "SOL": "SOL/USD", "XRP": "XRP/USD"},
        "note": "Institutional exchange — may require separate API agreement",
    },
    "crypto_com": {
        "url": "wss://stream.crypto.com/exchange/v1/market",
        "pairs": {"BTC": "BTC_USD", "ETH": "ETH_USD", "SOL": "SOL_USD", "XRP": "XRP_USD"},
        "channel": "book",
    },
    "itbit": {
        "url": None,  # itBit/Paxos — check current API availability
        "pairs": {"BTC": "XBTUSD", "ETH": "ETHUSD"},
        "note": "Now Paxos — API may have changed",
    },
    "bullish": {
        "url": None,  # Bullish Exchange — check API availability
        "pairs": {"BTC": "BTC-USD", "ETH": "ETH-USD"},
        "note": "Newer exchange — need to verify WS API",
    },
}


def test_computation():
    """Test the RTI computation with synthetic order book data."""
    rti = CFRealTimeIndex("BTC")

    # Simulate two exchanges with similar books
    # Exchange 1: slightly higher prices
    rti.update_orderbook("coinbase",
        bids=[(74500.00, 0.5), (74499.50, 1.0), (74499.00, 1.5), (74498.00, 2.0), (74497.00, 3.0)],
        asks=[(74500.50, 0.8), (74501.00, 1.2), (74501.50, 1.0), (74502.00, 2.5), (74503.00, 3.0)],
    )
    # Exchange 2: slightly lower prices
    rti.update_orderbook("kraken",
        bids=[(74499.80, 0.6), (74499.30, 1.1), (74498.80, 1.3), (74497.80, 1.8), (74496.80, 2.5)],
        asks=[(74500.30, 0.7), (74500.80, 1.0), (74501.30, 1.5), (74502.30, 2.0), (74503.30, 2.8)],
    )

    value = rti.compute()
    if value:
        print(f"BTC RTI: ${value:,.2f}")
        print(f"Status: {rti.get_status()}")
    else:
        print("Computation failed")

    # Test with ETH
    rti_eth = CFRealTimeIndex("ETH")
    rti_eth.update_orderbook("coinbase",
        bids=[(2340.00, 10), (2339.50, 20), (2339.00, 30), (2338.00, 50)],
        asks=[(2340.50, 15), (2341.00, 25), (2341.50, 20), (2342.00, 40)],
    )
    rti_eth.update_orderbook("kraken",
        bids=[(2339.80, 12), (2339.30, 18), (2338.80, 25), (2337.80, 45)],
        asks=[(2340.30, 10), (2340.80, 22), (2341.30, 18), (2342.30, 35)],
    )

    value_eth = rti_eth.compute()
    if value_eth:
        print(f"ETH RTI: ${value_eth:,.2f}")
    else:
        print("ETH computation failed")


if __name__ == "__main__":
    test_computation()
