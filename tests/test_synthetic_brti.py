"""
Synthetic BRTI vs Real BRTI comparison test.

Connects to Coinbase, Kraken, Bitstamp, Gemini WebSockets simultaneously.
Computes volume-weighted median of last trade prices every second.
Polls cfbenchmarks.com for real BRTI every 15s.
Compares synthetic vs real over 5 minutes.
"""
import asyncio
import json
import time
import re
import statistics
import requests
import websockets
from datetime import datetime
from collections import defaultdict

# Exchange WebSocket configs
EXCHANGES = {
    "coinbase": {
        "url": "wss://ws-feed.exchange.coinbase.com",
        "subscribe": {"type": "subscribe", "channels": [{"name": "ticker", "product_ids": ["BTC-USD"]}]},
        "price_field": "price",
        "volume_field": "last_size",
    },
    "kraken": {
        "url": "wss://ws.kraken.com",
        "subscribe": {"event": "subscribe", "pair": ["XBT/USD"], "subscription": {"name": "trade"}},
    },
    "bitstamp": {
        "url": "wss://ws.bitstamp.net",
        "subscribe": {"event": "bts:subscribe", "data": {"channel": "live_trades_btcusd"}},
    },
    "gemini": {
        "url": "wss://api.gemini.com/v1/marketdata/BTCUSD?trades=true&bids=false&offers=false",
        # Gemini auto-streams, no subscribe needed
    },
}

# Shared state
exchange_prices = {}      # exchange -> latest trade price
exchange_volumes = {}     # exchange -> rolling 60s volume
exchange_trades = defaultdict(list)  # exchange -> [(ts, price, volume), ...]
exchange_status = {}      # exchange -> "connected" / "error"

synthetic_history = []    # [(ts, synthetic_price)]
real_brti_history = []    # [(ts, real_price)]
comparison_log = []       # [(ts, synthetic, real, delta, delta_pct)]

DURATION_SEC = 300  # 5 minutes
BRTI_POLL_SEC = 15


def compute_synthetic_brti():
    """Volume-weighted median of exchange prices."""
    prices_with_volume = []
    for ex, trades in exchange_trades.items():
        if not trades:
            continue
        # Use trades from last 5 seconds
        cutoff = time.time() - 5
        recent = [(p, v) for t, p, v in trades if t > cutoff]
        if not recent:
            # Fall back to latest known price with minimal volume
            if ex in exchange_prices:
                prices_with_volume.append((exchange_prices[ex], 0.001))
            continue
        # Volume-weighted price for this exchange
        total_vol = sum(v for _, v in recent)
        vwap = sum(p * v for p, v in recent) / total_vol if total_vol > 0 else recent[-1][0]
        prices_with_volume.append((vwap, total_vol))

    if not prices_with_volume:
        return None

    # Volume-weighted median: sort by price, find median by cumulative volume
    prices_with_volume.sort(key=lambda x: x[0])
    total_volume = sum(v for _, v in prices_with_volume)
    if total_volume <= 0:
        # Equal weight fallback
        return statistics.median([p for p, _ in prices_with_volume])

    cumulative = 0
    for price, vol in prices_with_volume:
        cumulative += vol
        if cumulative >= total_volume / 2:
            return price
    return prices_with_volume[-1][0]


async def coinbase_ws():
    """Coinbase Exchange WebSocket — ticker channel."""
    try:
        async with websockets.connect(EXCHANGES["coinbase"]["url"], ping_interval=30) as ws:
            await ws.send(json.dumps(EXCHANGES["coinbase"]["subscribe"]))
            exchange_status["coinbase"] = "connected"
            print("  ✅ Coinbase connected")
            async for msg in ws:
                try:
                    data = json.loads(msg)
                    if data.get("type") == "ticker":
                        price = float(data.get("price", 0))
                        vol = float(data.get("last_size", 0))
                        if price > 0:
                            exchange_prices["coinbase"] = price
                            exchange_trades["coinbase"].append((time.time(), price, vol))
                            # Trim to last 60s
                            cutoff = time.time() - 60
                            exchange_trades["coinbase"] = [(t,p,v) for t,p,v in exchange_trades["coinbase"] if t > cutoff]
                except Exception:
                    pass
    except Exception as e:
        exchange_status["coinbase"] = f"error: {e}"
        print(f"  ✗ Coinbase error: {e}")


async def kraken_ws():
    """Kraken WebSocket — trade channel."""
    try:
        async with websockets.connect(EXCHANGES["kraken"]["url"], ping_interval=30) as ws:
            await ws.send(json.dumps(EXCHANGES["kraken"]["subscribe"]))
            exchange_status["kraken"] = "connected"
            print("  ✅ Kraken connected")
            async for msg in ws:
                try:
                    data = json.loads(msg)
                    # Kraken trade format: [channelID, [[price, volume, time, side, type, misc], ...], channelName, pair]
                    if isinstance(data, list) and len(data) >= 3:
                        trades = data[1] if isinstance(data[1], list) else []
                        for trade in trades:
                            if isinstance(trade, list) and len(trade) >= 2:
                                price = float(trade[0])
                                vol = float(trade[1])
                                if price > 0:
                                    exchange_prices["kraken"] = price
                                    exchange_trades["kraken"].append((time.time(), price, vol))
                        cutoff = time.time() - 60
                        exchange_trades["kraken"] = [(t,p,v) for t,p,v in exchange_trades["kraken"] if t > cutoff]
                except Exception:
                    pass
    except Exception as e:
        exchange_status["kraken"] = f"error: {e}"
        print(f"  ✗ Kraken error: {e}")


async def bitstamp_ws():
    """Bitstamp WebSocket — live trades."""
    try:
        async with websockets.connect(EXCHANGES["bitstamp"]["url"], ping_interval=30) as ws:
            await ws.send(json.dumps(EXCHANGES["bitstamp"]["subscribe"]))
            exchange_status["bitstamp"] = "connected"
            print("  ✅ Bitstamp connected")
            async for msg in ws:
                try:
                    data = json.loads(msg)
                    if data.get("event") == "trade":
                        trade_data = data.get("data", {})
                        price = float(trade_data.get("price", 0))
                        vol = float(trade_data.get("amount", 0))
                        if price > 0:
                            exchange_prices["bitstamp"] = price
                            exchange_trades["bitstamp"].append((time.time(), price, vol))
                            cutoff = time.time() - 60
                            exchange_trades["bitstamp"] = [(t,p,v) for t,p,v in exchange_trades["bitstamp"] if t > cutoff]
                except Exception:
                    pass
    except Exception as e:
        exchange_status["bitstamp"] = f"error: {e}"
        print(f"  ✗ Bitstamp error: {e}")


async def gemini_ws():
    """Gemini WebSocket — auto-streams trades."""
    try:
        async with websockets.connect(EXCHANGES["gemini"]["url"], ping_interval=30) as ws:
            exchange_status["gemini"] = "connected"
            print("  ✅ Gemini connected")
            async for msg in ws:
                try:
                    data = json.loads(msg)
                    events = data.get("events", [])
                    for ev in events:
                        if ev.get("type") == "trade":
                            price = float(ev.get("price", 0))
                            vol = float(ev.get("amount", 0))
                            if price > 0:
                                exchange_prices["gemini"] = price
                                exchange_trades["gemini"].append((time.time(), price, vol))
                                cutoff = time.time() - 60
                                exchange_trades["gemini"] = [(t,p,v) for t,p,v in exchange_trades["gemini"] if t > cutoff]
                except Exception:
                    pass
    except Exception as e:
        exchange_status["gemini"] = f"error: {e}"
        print(f"  ✗ Gemini error: {e}")


def fetch_real_brti():
    """Fetch latest BRTI from cfbenchmarks."""
    try:
        html = requests.get("https://www.cfbenchmarks.com/data/indices/BRTI", timeout=30).text
        m = re.search(r'"rtis":\[(.*?)\]', html)
        if m:
            data = json.loads('[' + m.group(1) + ']')
            latest = data[-1]
            return latest['time'] / 1000, float(latest['value'])
    except Exception as e:
        print(f"  BRTI fetch error: {e}")
    return None, None


async def comparison_loop():
    """Every second: compute synthetic. Every 15s: fetch real BRTI. Compare."""
    start = time.time()
    last_brti_poll = 0
    latest_real_brti = None
    latest_real_ts = 0

    # Wait for exchanges to connect
    await asyncio.sleep(3)

    tick = 0
    while time.time() - start < DURATION_SEC:
        tick += 1
        synthetic = compute_synthetic_brti()
        if synthetic:
            synthetic_history.append((time.time(), synthetic))

        # Poll real BRTI every 15s
        if time.time() - last_brti_poll > BRTI_POLL_SEC:
            brti_ts, brti_val = fetch_real_brti()
            if brti_val:
                latest_real_brti = brti_val
                latest_real_ts = brti_ts
                real_brti_history.append((brti_ts, brti_val))
            last_brti_poll = time.time()

        # Compare when we have both
        if synthetic and latest_real_brti:
            delta = synthetic - latest_real_brti
            delta_pct = delta / latest_real_brti * 100
            real_age = time.time() - latest_real_ts
            comparison_log.append((time.time(), synthetic, latest_real_brti, delta, delta_pct, real_age))

            if tick % 10 == 0:  # Print every 10s
                ex_str = " | ".join(f"{ex}=${p:,.2f}" for ex, p in sorted(exchange_prices.items()))
                print(f"  [{tick:>4}s] Synthetic: ${synthetic:,.2f} | Real BRTI: ${latest_real_brti:,.2f} (age:{real_age:.0f}s) | Δ${delta:+,.2f} ({delta_pct:+.3f}%) | {ex_str}")

        await asyncio.sleep(1)


async def main():
    print(f"{'='*100}")
    print(f"SYNTHETIC BRTI vs REAL BRTI — {DURATION_SEC}s comparison test")
    print(f"{'='*100}")
    print(f"Connecting to 4 exchanges...")

    # Launch all WebSockets + comparison loop
    tasks = [
        asyncio.create_task(coinbase_ws()),
        asyncio.create_task(kraken_ws()),
        asyncio.create_task(bitstamp_ws()),
        asyncio.create_task(gemini_ws()),
        asyncio.create_task(comparison_loop()),
    ]

    # Wait for comparison to finish, then cancel WS tasks
    await tasks[4]  # comparison_loop
    for t in tasks[:4]:
        t.cancel()

    # Results
    print(f"\n{'='*100}")
    print(f"RESULTS — {len(comparison_log)} comparison points over {DURATION_SEC}s")
    print(f"{'='*100}")

    if not comparison_log:
        print("No data collected!")
        return

    deltas = [abs(d) for _, _, _, d, _, _ in comparison_log]
    delta_pcts = [abs(dp) for _, _, _, _, dp, _ in comparison_log]

    print(f"  Exchanges connected: {', '.join(ex for ex, st in exchange_status.items() if st == 'connected')}")
    print(f"  Comparison points: {len(comparison_log)}")
    print(f"  Absolute delta:")
    print(f"    Mean:   ${sum(deltas)/len(deltas):,.2f}")
    print(f"    Median: ${sorted(deltas)[len(deltas)//2]:,.2f}")
    print(f"    Max:    ${max(deltas):,.2f}")
    print(f"    Min:    ${min(deltas):,.2f}")
    print(f"  Percentage delta:")
    print(f"    Mean:   {sum(delta_pcts)/len(delta_pcts):.4f}%")
    print(f"    Median: {sorted(delta_pcts)[len(delta_pcts)//2]:.4f}%")
    print(f"    Max:    {max(delta_pcts):.4f}%")
    print(f"  Within $5:  {sum(1 for d in deltas if d <= 5)/len(deltas)*100:.1f}%")
    print(f"  Within $10: {sum(1 for d in deltas if d <= 10)/len(deltas)*100:.1f}%")
    print(f"  Within $25: {sum(1 for d in deltas if d <= 25)/len(deltas)*100:.1f}%")
    print(f"  Within $50: {sum(1 for d in deltas if d <= 50)/len(deltas)*100:.1f}%")

    # Note: real BRTI has ~60-120s lag from cfbenchmarks HTML
    real_ages = [age for _, _, _, _, _, age in comparison_log]
    avg_age = sum(real_ages) / len(real_ages)
    print(f"\n  ⚠ Real BRTI data age: avg {avg_age:.0f}s — our synthetic is ~{avg_age:.0f}s AHEAD")
    print(f"  True accuracy is likely BETTER than measured (we're comparing fresh vs stale)")

if __name__ == "__main__":
    asyncio.run(main())
