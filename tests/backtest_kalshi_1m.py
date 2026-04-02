"""
Backtest: 15m crypto agent using REAL Kalshi settlement data + REAL 1-min candles.
Matches the production agent logic exactly:
  - Momentum layer (any time): BUY NO when mid<=0.15, BUY YES when mid>=0.54
  - Value Hunter layer (first 5 min): buy cheap side (15-54c) aligned with price vs strike
  - Price guards, RSI filter, dynamic risk multiplier
"""

import requests
import ccxt
import time as time_mod
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone

# ── Agent config (matches crypto_15m_agent.py exactly) ───────────────────────
THRESHOLDS = {
    "BTC": {"yes": 0.54, "no": 0.15, "min_vol": 40000},
    "ETH": {"yes": 0.54, "no": 0.15, "min_vol": 30000},
    "SOL": {"yes": 0.54, "no": 0.15, "min_vol": 25000},
    "DOGE": {"yes": 0.54, "no": 0.15, "min_vol": 20000},
}

VALUE_HUNTER_WINDOW_MINUTES = 5
VALUE_LOW = 0.15
VALUE_HIGH = 0.54
MAX_RISK_MULTIPLIER = 0.18

BASE_DIST_PCT = {2: 0.0004, 5: 0.001, 10: 0.0015, 15: 0.0025}
DIST_SCALES = {"BTC": 0.25, "ETH": 0.25, "SOL": 0.50, "DOGE": 0.50}

BASE_CASH_FLOOR = 40.0
RATCHET_PERCENT = 0.80
RISK_MULTIPLIER = 0.08
START_BALANCE = 1000.0

COINS = {
    "BTC":  {"series": "KXBTC15M", "pair": "BTC/USD"},
    "ETH":  {"series": "KXETH15M", "pair": "ETH/USD"},
    "SOL":  {"series": "KXSOL15M", "pair": "SOL/USD"},
    "DOGE": {"series": "KXDOGE15M", "pair": "DOGE/USD"},
}

API_BASE = "https://api.elections.kalshi.com/trade-api/v2"


def fetch_settled_markets(series_ticker, min_close_ts):
    markets = []
    cursor = ""
    while True:
        url = f"{API_BASE}/markets?series_ticker={series_ticker}&status=settled&limit=100&min_close_ts={min_close_ts}"
        if cursor:
            url += f"&cursor={cursor}"
        data = requests.get(url).json()
        batch = data.get("markets", [])
        if not batch:
            break
        markets.extend(batch)
        cursor = data.get("cursor", "")
        if not cursor:
            break
        time_mod.sleep(0.3)
        if len(markets) % 500 == 0:
            print(f"    ...{len(markets)} markets fetched")
    return markets


def fetch_1min_candles(pair):
    exchange = ccxt.kraken()
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    since = now_ms - (24 * 60 * 60 * 1000)
    all_candles = []
    while since < now_ms:
        candles = exchange.fetch_ohlcv(pair, "1m", since=since, limit=720)
        if not candles:
            break
        new = [c for c in candles if c[0] not in {x[0] for x in all_candles[-720:]}]
        all_candles.extend(new)
        since = candles[-1][0] + 60000
        time_mod.sleep(0.35)
    df = pd.DataFrame(all_candles, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["dt"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df = df.drop_duplicates(subset="timestamp").sort_values("timestamp").reset_index(drop=True)
    return df


def build_price_lookup(candle_df):
    return {int(row["timestamp"] // 60000) * 60000: row["close"] for _, row in candle_df.iterrows()}


def get_price_at(lookup, ts_ms):
    key = int(ts_ms // 60000) * 60000
    for off in range(6):
        if key + off * 60000 in lookup: return lookup[key + off * 60000]
        if key - off * 60000 in lookup: return lookup[key - off * 60000]
    return None


def compute_rsi(prices, period=14):
    if len(prices) < period + 1:
        return 50.0
    s = pd.Series(prices)
    delta = s.diff()
    gain = delta.where(delta > 0, 0).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    val = rsi.iloc[-1]
    return val if not np.isnan(val) else 50.0


def price_to_mid(current_price, strike, minutes_left):
    if strike == 0 or current_price is None:
        return 0.50
    distance = current_price - strike
    norm = distance / (strike * 0.003)
    progress = (15 - minutes_left) / 15.0
    time_factor = max(0.5, progress * 2.5)
    z = norm * time_factor
    return float(np.clip(1.0 / (1.0 + np.exp(-z)), 0.02, 0.98))


def dynamic_risk_multiplier(mid, decision_type, minutes_remaining):
    base = RISK_MULTIPLIER
    boost = 0.0
    if decision_type == "VALUE":
        boost += 0.08
    if VALUE_LOW <= mid <= VALUE_HIGH:
        boost += 0.04
    if minutes_remaining >= 12:
        boost += 0.02
    return min(base + boost, MAX_RISK_MULTIPLIER)


def run_coin_backtest(coin_name, markets, price_lookup):
    th = THRESHOLDS[coin_name]
    scale = DIST_SCALES[coin_name]
    markets.sort(key=lambda m: m["close_time"])

    balance = START_BALANCE
    cash_floor = BASE_CASH_FLOOR
    trades = []
    last_traded_ticker = None
    price_history = []
    matched = missed = 0

    for mkt in markets:
        strike = float(mkt.get("floor_strike", 0))
        result = mkt.get("result", "")
        volume = float(mkt.get("volume_fp", 0))
        ticker = mkt.get("ticker", "")
        close_time_str = mkt.get("close_time", "")
        if not strike or not result or not ticker or not close_time_str:
            continue

        close_dt = datetime.fromisoformat(close_time_str.replace("Z", "+00:00"))

        # Evaluate at multiple points: 12min, 8min, 5min remaining
        # (first 3, 7, 10 minutes of the cycle)
        traded_this_window = False
        for eval_min_remaining in [12, 8, 5]:
            if traded_this_window:
                break

            eval_dt = close_dt - timedelta(minutes=eval_min_remaining)
            eval_ts_ms = int(eval_dt.timestamp() * 1000)
            current_price = get_price_at(price_lookup, eval_ts_ms)
            if current_price is None:
                missed += 1
                continue
            matched += 1

            price_history.append(current_price)
            distance = abs(current_price - strike)
            mid = price_to_mid(current_price, strike, eval_min_remaining)
            minutes_remaining = eval_min_remaining

            # Min distance
            if minutes_remaining <= 2:
                min_distance = current_price * BASE_DIST_PCT[2] * scale
            elif minutes_remaining <= 5:
                min_distance = current_price * BASE_DIST_PCT[5] * scale
            elif minutes_remaining <= 10:
                min_distance = current_price * BASE_DIST_PCT[10] * scale
            else:
                min_distance = current_price * BASE_DIST_PCT[15] * scale

            # === MOMENTUM LAYER ===
            decision = "HOLD"
            decision_type = "MOMENTUM"
            entry_price = None

            if mid <= th["no"] and volume >= th["min_vol"]:
                decision = "BUY NO"
                entry_price = max(1, min(90, int(round((1.0 - mid) * 100))))
            elif mid >= th["yes"] and distance > min_distance:
                decision = "BUY YES"
                entry_price = max(1, min(90, int(round(mid * 100))))

            # === VALUE HUNTER LAYER (first 5 min of cycle only) ===
            if decision == "HOLD" and minutes_remaining >= (15 - VALUE_HUNTER_WINDOW_MINUTES):
                if VALUE_LOW <= mid <= VALUE_HIGH:
                    if current_price > strike:
                        decision = "BUY YES"
                        decision_type = "VALUE"
                    elif current_price < strike:
                        decision = "BUY NO"
                        decision_type = "VALUE"
                    elif mid < 0.33:
                        decision = "BUY NO"
                        decision_type = "VALUE"
                    else:
                        decision = "BUY YES"
                        decision_type = "VALUE"

                    entry_price = max(1, min(90, int(round(mid * 100)))) if decision == "BUY YES" else max(1, min(90, int(round((1.0 - mid) * 100))))

            # Price guards
            if mid >= 0.88 or mid <= 0.12:
                decision = "HOLD"

            # RSI filter
            rsi = compute_rsi(price_history)
            if decision == "BUY YES" and rsi < 60:
                decision = "HOLD"
            elif decision == "BUY NO" and rsi > 40:
                decision = "HOLD"

            if decision == "HOLD":
                continue
            if ticker == last_traded_ticker:
                continue

            riskable = max(0.0, balance - cash_floor)
            if riskable <= 0:
                continue

            risk_mult = dynamic_risk_multiplier(mid, decision_type, minutes_remaining)
            WIN_RATE, AVG_WIN, AVG_LOSS = 0.55, 40, 25
            kelly = (WIN_RATE * AVG_WIN - (1 - WIN_RATE) * AVG_LOSS) / AVG_WIN * 0.25
            contracts = max(1, int(riskable * risk_mult * kelly))
            cost = contracts * (entry_price / 100.0)

            # REAL settlement
            if decision == "BUY YES":
                payout = contracts * 1.0 if result == "yes" else 0.0
            else:
                payout = contracts * 1.0 if result == "no" else 0.0

            pnl = payout - cost
            balance += pnl

            if balance > cash_floor:
                new_floor = balance * RATCHET_PERCENT
                if new_floor > cash_floor:
                    cash_floor = new_floor

            trades.append({
                "time": close_time_str,
                "ticker": ticker,
                "decision": decision,
                "type": decision_type,
                "mid": mid,
                "entry_price_c": entry_price,
                "contracts": contracts,
                "result": result,
                "pnl": pnl,
                "balance": balance,
                "strike": strike,
                "price": current_price,
                "distance": distance,
                "rsi": rsi,
                "minutes_left": minutes_remaining,
            })
            last_traded_ticker = ticker
            traded_this_window = True

    print(f"  Price coverage: {matched}/{matched + missed}")
    return trades, balance


def print_summary(coin, trades, final_bal, num_mkts):
    df = pd.DataFrame(trades)
    pnl = df["pnl"].sum() if len(df) > 0 else 0.0
    wins = len(df[df["pnl"] > 0]) if len(df) > 0 else 0
    losses = len(df[df["pnl"] < 0]) if len(df) > 0 else 0

    print(f"\n{'='*70}")
    print(f"  {coin}  ({num_mkts} markets)")
    print(f"{'='*70}")
    print(f"  Trades: {len(df)} | Wins: {wins} | Losses: {losses} | Win%: {wins/max(1,len(df))*100:.1f}%")
    print(f"  PnL: ${pnl:.2f} | Final: ${final_bal:.2f} | ROI: {(final_bal-START_BALANCE)/START_BALANCE*100:.1f}%")

    if len(df) > 0:
        print(f"  Avg: ${df['pnl'].mean():.2f} | MaxWin: ${df['pnl'].max():.2f} | MaxLoss: ${df['pnl'].min():.2f}")
        mom = df[df["type"] == "MOMENTUM"]
        val = df[df["type"] == "VALUE"]
        if len(mom) > 0:
            mom_w = len(mom[mom["pnl"] > 0])
            print(f"  MOMENTUM: {len(mom)} trades ({mom_w}W/{len(mom)-mom_w}L) ${mom['pnl'].sum():.2f}")
        if len(val) > 0:
            val_w = len(val[val["pnl"] > 0])
            print(f"  VALUE:    {len(val)} trades ({val_w}W/{len(val)-val_w}L) ${val['pnl'].sum():.2f}")

        losers = df[df["pnl"] < 0].head(3)
        if len(losers) > 0:
            print(f"  Sample losses:")
            for _, t in losers.iterrows():
                print(f"    {t['type']:>8} {t['decision']} mid={t['mid']:.3f} entry={t['entry_price_c']}c result={t['result']} PnL=${t['pnl']:.2f} @{t['minutes_left']}min")

    return {"coin": coin, "trades": len(df), "wins": wins, "losses": losses, "pnl": pnl,
            "win_rate": wins/max(1,len(df))*100, "avg_pnl": df["pnl"].mean() if len(df)>0 else 0}


def main():
    now = datetime.now(timezone.utc)
    lookback = now - timedelta(hours=12)
    min_ts = int(lookback.timestamp())

    print(f"Backtest: {lookback.strftime('%Y-%m-%d %H:%M')} → {now.strftime('%Y-%m-%d %H:%M')}")
    print(f"Real Kalshi settlements + Real Kraken 1-min candles")
    print(f"Thresholds: yes>=0.54 no<=0.15 | Value zone: {VALUE_LOW}-{VALUE_HIGH}")
    print()

    results = []
    all_trades = []

    for coin, cfg in COINS.items():
        print(f"Fetching {coin}...")
        markets = fetch_settled_markets(cfg["series"], min_ts)
        print(f"  ✓ {len(markets)} Kalshi markets")
        if len(markets) < 5:
            continue

        print(f"  Fetching 1-min candles...")
        try:
            cdf = fetch_1min_candles(cfg["pair"])
            print(f"  ✓ {len(cdf)} candles")
        except Exception as e:
            print(f"  ✗ {e}")
            continue

        lookup = build_price_lookup(cdf)
        trades, final_bal = run_coin_backtest(coin, markets, lookup)
        r = print_summary(coin, trades, final_bal, len(markets))
        results.append(r)
        all_trades.extend(trades)

    if results:
        tot_t = sum(r["trades"] for r in results)
        tot_w = sum(r["wins"] for r in results)
        tot_l = sum(r["losses"] for r in results)
        tot_pnl = sum(r["pnl"] for r in results)
        print(f"\n{'='*70}")
        print(f"  AGGREGATE: {tot_t} trades ({tot_w}W/{tot_l}L) {tot_w/max(1,tot_t)*100:.1f}% | ${tot_pnl:.2f} PnL")
        print(f"{'='*70}")


if __name__ == "__main__":
    main()
