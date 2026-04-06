"""
For each 8/10 fade signal in the 4-day window, fetch the actual trade tape
for that market and check whether the target side ever traded at 5-15c
during the first 8 minutes of the cycle.
"""
import requests, time as time_mod
from datetime import datetime, timedelta, timezone

API = "https://api.elections.kalshi.com/trade-api/v2"
COINS = ["BTC", "ETH", "SOL", "XRP", "BNB", "HYPE", "DOGE"]

start_ts = int(datetime(2026, 4, 2, 19, 30, tzinfo=timezone.utc).timestamp())
end_ts = int(datetime(2026, 4, 6, 23, 59, tzinfo=timezone.utc).timestamp())

# Fetch settled markets
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
    print(f"{coin}: {len(markets)} markets")

print("\nChecking 8/10 signals against actual trade tape...")
print("="*90)

# For each coin, find 8/10 signals and check if a fill at 5-15c was possible
results_summary = {}
for coin in COINS:
    markets = all_markets[coin]
    results_list = [m.get("result","") for m in markets]
    series = f"KX{coin}15M"

    signals_total = 0
    signals_filled = 0
    signals_won = 0
    sample_data = []

    for i in range(10, len(markets)):
        prev = results_list[i-10:i]
        yc = sum(1 for r in prev if r == "yes")
        nc = 10 - yc
        if nc < 8 and yc < 8:
            continue

        signals_total += 1
        if signals_total > 30:  # Sample limit per coin to avoid API blast
            break

        side = "yes" if nc >= 8 else "no"
        ticker = markets[i]["ticker"]
        result = markets[i].get("result","")

        # Compute first 8 min window
        close_dt = datetime.fromisoformat(markets[i]["close_time"].replace("Z","+00:00"))
        window_start_ts = int((close_dt - timedelta(minutes=15)).timestamp())
        window_end_ts = int((close_dt - timedelta(minutes=7)).timestamp())

        # Fetch trades for this market
        try:
            r = requests.get(f"{API}/markets/trades?ticker={ticker}&limit=200&min_ts={window_start_ts}&max_ts={window_end_ts}")
            trades = r.json().get("trades", [])
        except Exception as e:
            continue

        # Check if our target side traded at 5-15c
        min_target_price = 100
        max_target_price = 0
        for t in trades:
            yes_price = int(float(t.get("yes_price_dollars", "0")) * 100)
            if side == "yes":
                target_price = yes_price
            else:
                target_price = 100 - yes_price
            min_target_price = min(min_target_price, target_price)
            max_target_price = max(max_target_price, target_price)

        if 18 <= min_target_price <= 49 or (min_target_price < 18 and max_target_price >= 18):
            signals_filled += 1
            won = (side == "yes" and result == "yes") or (side == "no" and result == "no")
            if won:
                signals_won += 1
            sample_data.append((ticker[-12:], side, min_target_price, max_target_price, result, "WIN" if won else "LOSS"))
        elif min_target_price < 5:
            sample_data.append((ticker[-12:], side, min_target_price, max_target_price, result, "TOO_LOW"))
        else:
            sample_data.append((ticker[-12:], side, min_target_price, max_target_price, result, "ABOVE_15"))

        time_mod.sleep(0.1)

    fill_rate = signals_filled / max(1, signals_total) * 100
    win_rate_filled = signals_won / max(1, signals_filled) * 100
    results_summary[coin] = (signals_total, signals_filled, signals_won, fill_rate, win_rate_filled)

    print(f"\n{coin}: {signals_total} 8/10 signals checked")
    print(f"  {signals_filled} ({fill_rate:.0f}%) had a tradeable price in 5-15c during first 8 min")
    print(f"  Of fills: {signals_won}W/{signals_filled - signals_won}L = {win_rate_filled:.0f}% WR")
    print(f"  Sample: ", end="")
    for s in sample_data[:5]:
        print(f"\n    {s[0]} {s[1]} min={s[2]}c max={s[3]}c → {s[4]} [{s[5]}]", end="")
    print()

print("\n" + "="*90)
print("AGGREGATE")
print("="*90)
tot_sig = sum(v[0] for v in results_summary.values())
tot_fill = sum(v[1] for v in results_summary.values())
tot_won = sum(v[2] for v in results_summary.values())
print(f"Total 8/10 signals checked: {tot_sig}")
print(f"Tradeable at 5-15c: {tot_fill} ({tot_fill/max(1,tot_sig)*100:.0f}%)")
print(f"Win rate of those fills: {tot_won}/{tot_fill} = {tot_won/max(1,tot_fill)*100:.0f}%")
