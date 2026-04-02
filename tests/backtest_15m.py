"""
Backtest: 15m crypto agent — multi-coin distance optimization
Pulls 72h of real candles per coin from Kraken, uses price-proportional
distance tables, reports per-coin and aggregate PnL.
"""

import ccxt
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import time

# ── Coins & Kalshi mappings ─────────────────────────────────────────────────
COINS = {
    "BTC":  {"pair": "BTC/USD",  "kalshi": "KXBTC15M",  "strike_round": 100},
    "ETH":  {"pair": "ETH/USD",  "kalshi": "KXETH15M",  "strike_round": 5},
    "SOL":  {"pair": "SOL/USD",  "kalshi": "KXSOL15M",  "strike_round": 0.5},
    "DOGE": {"pair": "DOGE/USD", "kalshi": "KXDOGE15M", "strike_round": 0.001},
}

# Base distance table as % of price (derived from BTC: $200/$85000 ≈ 0.24%)
# These are the min distances at each time bucket as fraction of coin price
BASE_DIST_PCT = {2: 0.0004, 5: 0.001, 10: 0.0015, 15: 0.0025}

THRESHOLDS = {"yes": 0.54, "no": 0.15, "min_vol": 40000}

BASE_CASH_FLOOR = 40.0
RATCHET_PERCENT = 0.80
RISK_MULTIPLIER = 0.08
KELLY = 0.25 * (0.55 * 40 - 0.45 * 25) / 40
START_BALANCE = 1000.0


def fetch_72h(pair):
    """Fetch 72h of 5-min candles from Kraken."""
    exchange = ccxt.kraken()
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    since = now_ms - (72 * 60 * 60 * 1000)
    all_candles = []

    while since < now_ms:
        candles = exchange.fetch_ohlcv(pair, "5m", since=since, limit=720)
        if not candles:
            break
        new = [c for c in candles if c[0] not in {x[0] for x in all_candles}]
        all_candles.extend(new)
        if candles[-1][0] + 300000 >= now_ms:
            break
        since = candles[-1][0] + 300000
        time.sleep(0.5)

    df = pd.DataFrame(all_candles, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["dt"] = pd.to_datetime(df["timestamp"], unit="ms")
    df = df.drop_duplicates(subset="timestamp").sort_values("timestamp").reset_index(drop=True)
    return df


def compute_rsi(closes, period=14):
    delta = closes.diff()
    gain = delta.where(delta > 0, 0).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))


def synthetic_strike(price, round_to):
    if round_to >= 1:
        return round(price / round_to) * round_to
    return round(price / round_to) * round_to


def synthetic_mid(current_price, strike, minutes_left, ref_price):
    """Logistic mid model scaled to coin price."""
    distance = current_price - strike
    norm_dist = distance / (ref_price * 0.003)  # normalize by ~0.3% of price
    time_factor = max(0.5, (15 - minutes_left) / 15.0 * 2)
    z = norm_dist * time_factor
    mid = 1.0 / (1.0 + np.exp(-z))
    mid = np.clip(mid + np.random.normal(0, 0.02), 0.02, 0.98)
    return mid


def run_coin_backtest(df, coin_name, coin_cfg, dist_scale=1.0):
    """Run backtest for a single coin at a given distance scale."""
    rsi_series = compute_rsi(df["close"])
    ref_price = df["close"].median()

    # Build absolute distance table from pct * price * scale
    min_distance_table = {
        k: ref_price * v * dist_scale for k, v in BASE_DIST_PCT.items()
    }

    balance = START_BALANCE
    cash_floor = BASE_CASH_FLOOR
    trades = []
    last_traded_ticker = None

    df["window"] = df["dt"].dt.floor("15min")
    windows = df.groupby("window")

    for window_start, wdf in windows:
        if len(wdf) < 2:
            continue

        strike = synthetic_strike(wdf["open"].iloc[0], coin_cfg["strike_round"])

        for eval_idx in range(len(wdf)):
            row = wdf.iloc[eval_idx]
            idx = row.name
            current_price = row["close"]
            minutes_left = 15 - (row["dt"].minute % 15)
            mid = synthetic_mid(current_price, strike, minutes_left, ref_price)
            distance = abs(current_price - strike)

            rsi_val = rsi_series.iloc[idx] if idx < len(rsi_series) and not np.isnan(rsi_series.iloc[idx]) else 50.0

            if minutes_left <= 2:
                min_dist = min_distance_table[2]
            elif minutes_left <= 5:
                min_dist = min_distance_table[5]
            elif minutes_left <= 10:
                min_dist = min_distance_table[10]
            else:
                min_dist = min_distance_table[15]

            if mid >= 0.88 or mid <= 0.12:
                continue

            decision = "HOLD"
            entry_price = None

            if mid <= THRESHOLDS["no"]:
                decision = "BUY NO"
                entry_price = max(1, min(90, int(round((1.0 - mid) * 100))))
            elif mid >= THRESHOLDS["yes"] and distance > min_dist:
                decision = "BUY YES"
                entry_price = max(1, min(90, int(round(mid * 100))))

            if decision == "BUY YES" and rsi_val < 60:
                decision = "HOLD"
            elif decision == "BUY NO" and rsi_val > 40:
                decision = "HOLD"

            if decision == "HOLD":
                continue

            ticker = f"{coin_name}-{int(strike*1000)}-{window_start.strftime('%H%M')}"
            if ticker == last_traded_ticker:
                continue

            riskable = max(0.0, balance - cash_floor)
            if riskable <= 0:
                continue

            contracts = max(1, int(riskable * RISK_MULTIPLIER * KELLY))
            cost = contracts * (entry_price / 100.0)

            window_close_price = wdf["close"].iloc[-1]
            settled_yes = window_close_price >= strike

            if decision == "BUY YES":
                payout = contracts * 1.0 if settled_yes else 0.0
            else:
                payout = contracts * 1.0 if not settled_yes else 0.0

            pnl = payout - cost
            balance += pnl

            if balance > cash_floor:
                new_floor = balance * RATCHET_PERCENT
                if new_floor > cash_floor:
                    cash_floor = new_floor

            trades.append({
                "time": row["dt"],
                "decision": decision,
                "mid": mid,
                "entry_price_c": entry_price,
                "contracts": contracts,
                "pnl": pnl,
                "balance": balance,
            })
            last_traded_ticker = ticker
            break

    trades_df = pd.DataFrame(trades)
    total_pnl = trades_df["pnl"].sum() if len(trades_df) > 0 else 0.0
    wins = len(trades_df[trades_df["pnl"] > 0]) if len(trades_df) > 0 else 0
    losses = len(trades_df[trades_df["pnl"] < 0]) if len(trades_df) > 0 else 0
    avg_pnl = trades_df["pnl"].mean() if len(trades_df) > 0 else 0.0
    max_loss = trades_df["pnl"].min() if len(trades_df) > 0 else 0.0
    yes_ct = len(trades_df[trades_df["decision"] == "BUY YES"]) if len(trades_df) > 0 else 0
    no_ct = len(trades_df[trades_df["decision"] == "BUY NO"]) if len(trades_df) > 0 else 0

    return {
        "trades": len(trades_df),
        "wins": wins,
        "losses": losses,
        "win_rate": wins / max(1, len(trades_df)) * 100,
        "total_pnl": total_pnl,
        "avg_pnl": avg_pnl,
        "max_loss": max_loss,
        "final_bal": balance,
        "yes": yes_ct,
        "no": no_ct,
        "ref_price": ref_price,
    }


def main():
    # Fetch all coin data first
    coin_data = {}
    for coin, cfg in COINS.items():
        print(f"Fetching {coin} ({cfg['pair']})...")
        try:
            df = fetch_72h(cfg["pair"])
            if len(df) > 50:
                coin_data[coin] = df
                print(f"  ✓ {len(df)} candles, price ~${df['close'].median():,.4f}")
            else:
                print(f"  ✗ Only {len(df)} candles, skipping")
        except Exception as e:
            print(f"  ✗ Failed: {e}")
        time.sleep(1)

    scale_factors = [0.25, 0.50, 0.75, 1.0, 1.25, 1.50, 2.0]

    # Per-coin results
    all_results = {}  # coin -> [results per scale]

    for coin, df in coin_data.items():
        cfg = COINS[coin]
        ref = df["close"].median()
        print(f"\n{'='*80}")
        print(f"  {coin}  (ref price: ${ref:,.4f})")
        dist_abs = {k: ref * v for k, v in BASE_DIST_PCT.items()}
        print(f"  Base dist table (1.0x): 2m=${dist_abs[2]:.4f}  5m=${dist_abs[5]:.4f}  10m=${dist_abs[10]:.4f}  15m=${dist_abs[15]:.4f}")
        print(f"{'='*80}")
        print(f"  {'Scale':>5} | {'Trades':>6} | {'W/L':>7} | {'Win%':>6} | {'PnL':>9} | {'Avg':>7} | {'MaxLoss':>8} | {'Y/N':>5}")
        print(f"  {'-'*5}-+-{'-'*6}-+-{'-'*7}-+-{'-'*6}-+-{'-'*9}-+-{'-'*7}-+-{'-'*8}-+-{'-'*5}")

        coin_results = []
        for scale in scale_factors:
            np.random.seed(42)
            r = run_coin_backtest(df, coin, cfg, dist_scale=scale)
            r["scale"] = scale
            coin_results.append(r)

            best_marker = ""
            if scale == max(coin_results, key=lambda x: x["total_pnl"])["scale"]:
                best_marker = " ◀"

            print(f"  {scale:>4.2f}x | {r['trades']:>6} | {r['wins']:>3}/{r['losses']:<3} | {r['win_rate']:>5.1f}% | ${r['total_pnl']:>7.2f} | ${r['avg_pnl']:>5.2f} | ${r['max_loss']:>7.2f} | {r['yes']:>2}/{r['no']:<2}{best_marker}")

        best = max(coin_results, key=lambda x: x["total_pnl"])
        print(f"  ★ {coin} optimal: {best['scale']}x → ${best['total_pnl']:.2f} PnL, {best['trades']} trades")
        all_results[coin] = coin_results

    # Aggregate: combined PnL per scale across all coins
    print(f"\n{'='*80}")
    print(f"  AGGREGATE (all coins combined)")
    print(f"{'='*80}")
    print(f"  {'Scale':>5} | {'Trades':>6} | {'W/L':>7} | {'Win%':>6} | {'PnL':>9} | {'Avg':>7} | {'MaxLoss':>8}")
    print(f"  {'-'*5}-+-{'-'*6}-+-{'-'*7}-+-{'-'*6}-+-{'-'*9}-+-{'-'*7}-+-{'-'*8}")

    agg_results = []
    for i, scale in enumerate(scale_factors):
        tot_trades = sum(all_results[c][i]["trades"] for c in all_results)
        tot_wins = sum(all_results[c][i]["wins"] for c in all_results)
        tot_losses = sum(all_results[c][i]["losses"] for c in all_results)
        tot_pnl = sum(all_results[c][i]["total_pnl"] for c in all_results)
        avg_pnl = tot_pnl / max(1, tot_trades)
        worst_loss = min(all_results[c][i]["max_loss"] for c in all_results)
        agg_results.append({"scale": scale, "pnl": tot_pnl, "trades": tot_trades})

        best_marker = ""
        if scale == max(agg_results, key=lambda x: x["pnl"])["scale"]:
            best_marker = " ◀"

        print(f"  {scale:>4.2f}x | {tot_trades:>6} | {tot_wins:>3}/{tot_losses:<3} | {tot_wins/max(1,tot_trades)*100:>5.1f}% | ${tot_pnl:>7.2f} | ${avg_pnl:>5.2f} | ${worst_loss:>7.2f}{best_marker}")

    best_agg = max(agg_results, key=lambda x: x["pnl"])
    print(f"\n  ★ Overall optimal: {best_agg['scale']}x → ${best_agg['pnl']:.2f} combined PnL, {best_agg['trades']} trades")

    # Per-coin optimal recommendation
    print(f"\n  RECOMMENDED PER-COIN DISTANCE SCALES:")
    for coin in all_results:
        best = max(all_results[coin], key=lambda x: x["total_pnl"])
        ref = coin_data[coin]["close"].median()
        table = {k: ref * v * best["scale"] for k, v in BASE_DIST_PCT.items()}
        print(f"    {coin:>4}: {best['scale']}x  (2m=${table[2]:.4f}  5m=${table[5]:.4f}  10m=${table[10]:.4f}  15m=${table[15]:.4f})")


if __name__ == "__main__":
    main()
