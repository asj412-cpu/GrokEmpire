"""
Real-trade-tape backtest:
For each 8/10 fade signal, check the actual trade tape for the FIRST 8 MINUTES.
- Find the lowest price in our entry range (e.g. 18-49c)
- That's our actual fill price
- Then check if a 2x sell would have filled (max price during cycle ≥ 2x entry)
- Otherwise hold to settlement
"""
import requests, time as time_mod
import sys
from datetime import datetime, timedelta, timezone

API = "https://api.elections.kalshi.com/trade-api/v2"
COINS = ["BTC", "ETH", "SOL", "XRP", "BNB", "HYPE", "DOGE"]

# Entry range from CLI args
ENTRY_LOW = int(sys.argv[1]) if len(sys.argv) > 1 else 18
ENTRY_HIGH = int(sys.argv[2]) if len(sys.argv) > 2 else 49

start_ts = int(datetime(2026, 4, 2, 19, 30, tzinfo=timezone.utc).timestamp())
end_ts = int(datetime(2026, 4, 6, 23, 59, tzinfo=timezone.utc).timestamp())

print(f"Testing entry range: {ENTRY_LOW}-{ENTRY_HIGH}c")
print()

# Fetch settled markets (warmup needed for fade window)
all_markets = {}
for coin in COINS:
    series = f"KX{coin}15M"
    markets, cursor = [], ""
    while True:
        url = f"{API}/markets?series_ticker={series}&status=settled&limit=100&min_close_ts={start_ts - 3600}&max_close_ts={end_ts}"
        if cursor: url += f"&cursor={cursor}"
        d = requests.get(url).json()
        batch = d.get("markets", [])
        if not batch: break
        markets.extend(batch)
        cursor = d.get("cursor", "")
        if not cursor: break
        time_mod.sleep(0.2)
    all_markets[coin] = sorted(markets, key=lambda m: m["close_time"])

print(f"Markets per coin: {len(all_markets['BTC'])}\n")
print(f"{'Coin':<5} | {'Sigs':>5} | {'Fills':>5} | {'Hold W%':>8} | {'Exits':>5} | {'Final W%':>9} | {'PnL':>9}")
print("-"*70)

total_sigs = total_fills = total_hold_wins = total_exits = total_final_wins = 0
total_pnl = 0.0

for coin in COINS:
    markets = all_markets[coin]
    results_list = [m.get("result","") for m in markets]
    series = f"KX{coin}15M"

    sigs = fills = hold_wins = exits = final_wins = 0
    pnl = 0.0
    sample_data = []

    for i in range(10, len(markets)):
        prev = results_list[i-10:i]
        yc = sum(1 for r in prev if r == "yes")
        nc = 10 - yc
        if nc < 8 and yc < 8:
            continue

        sigs += 1

        side = "yes" if nc >= 8 else "no"
        ticker = markets[i]["ticker"]
        result = markets[i].get("result","")

        # Window: full 15 min cycle
        close_dt = datetime.fromisoformat(markets[i]["close_time"].replace("Z","+00:00"))
        cycle_start_ts = int((close_dt - timedelta(minutes=15)).timestamp())
        buy_window_end_ts = int((close_dt - timedelta(minutes=7)).timestamp())
        cycle_end_ts = int(close_dt.timestamp())

        # Fetch all trades for this market
        try:
            r = requests.get(f"{API}/markets/trades?ticker={ticker}&limit=500")
            trades = r.json().get("trades", [])
        except Exception:
            continue

        # Convert to (timestamp, target_side_price)
        target_prices_in_window = []  # for buy window (first 8 min)
        target_prices_after_buy = []  # for exit check (after first fill onwards)

        for t in trades:
            try:
                yes_price = int(float(t.get("yes_price_dollars", "0")) * 100)
                t_ts = datetime.fromisoformat(t["created_time"].replace("Z","+00:00")).timestamp()
            except (ValueError, TypeError, KeyError):
                continue

            target_price = yes_price if side == "yes" else (100 - yes_price)

            if cycle_start_ts <= t_ts <= buy_window_end_ts:
                target_prices_in_window.append((t_ts, target_price))
            if cycle_start_ts <= t_ts <= cycle_end_ts:
                target_prices_after_buy.append((t_ts, target_price))

        if not target_prices_in_window:
            continue

        # Find first price in our entry range (lowest preferred)
        # Sort by price ascending, take cheapest available
        sorted_window = sorted(target_prices_in_window, key=lambda x: x[1])
        fill_price = None
        fill_ts = None
        for t_ts, p in sorted_window:
            if ENTRY_LOW <= p <= ENTRY_HIGH:
                fill_price = p
                fill_ts = t_ts
                break

        if fill_price is None:
            continue

        fills += 1
        won_hold = (side == "yes" and result == "yes") or (side == "no" and result == "no")
        if won_hold:
            hold_wins += 1

        # Check if a 2x or 3x sell would have filled after the fill timestamp
        sell_target = min(fill_price * (3 if fill_price <= 20 else 2), 95)
        exit_filled = False
        for t_ts, p in target_prices_after_buy:
            if t_ts > fill_ts and p >= sell_target:
                exit_filled = True
                break

        if exit_filled:
            exits += 1
            pnl += (sell_target - fill_price) / 100
            final_wins += 1
        else:
            # Hold to settlement
            if won_hold:
                pnl += (1.0 - fill_price/100)
                final_wins += 1
            else:
                pnl -= fill_price / 100

        if len(sample_data) < 5:
            outcome = "EXIT" if exit_filled else ("WIN" if won_hold else "LOSS")
            sample_data.append(f"{ticker[-12:]} {side} fill={fill_price}c→{sell_target}c {outcome}")

        time_mod.sleep(0.05)

    fill_rate = fills/max(1,sigs)*100
    hold_wr = hold_wins/max(1,fills)*100
    final_wr = final_wins/max(1,fills)*100
    print(f"{coin:<5} | {sigs:>5} | {fills:>3} ({fill_rate:>3.0f}%) | {hold_wr:>6.0f}% | {exits:>5} | {final_wr:>6.0f}% | ${pnl:>7.2f}")

    for s in sample_data:
        print(f"        {s}")

    total_sigs += sigs
    total_fills += fills
    total_hold_wins += hold_wins
    total_exits += exits
    total_final_wins += final_wins
    total_pnl += pnl

print("-"*70)
print(f"{'TOTAL':<5} | {total_sigs:>5} | {total_fills:>5} | {total_hold_wins/max(1,total_fills)*100:>6.0f}% | {total_exits:>5} | {total_final_wins/max(1,total_fills)*100:>6.0f}% | ${total_pnl:>7.2f}")
print(f"\nFill rate: {total_fills/max(1,total_sigs)*100:.0f}% | Hold WR: {total_hold_wins/max(1,total_fills)*100:.0f}% | Final WR (with exits): {total_final_wins/max(1,total_fills)*100:.0f}%")
