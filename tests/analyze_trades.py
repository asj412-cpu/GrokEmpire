"""Analyze actual Kalshi trades from Apr 2-6"""
import os
from kalshi_client.client import KalshiClient
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv

load_dotenv(override=True)

with open(os.getenv("KALSHI_PRIVATE_KEY"), 'rb') as f:
    pk = serialization.load_pem_private_key(f.read(), password=None)
client = KalshiClient(
    key_id=os.getenv("KALSHI_KEY_ID"),
    private_key=pk,
    exchange_api_base='https://api.elections.kalshi.com/trade-api/v2'
)

bal = client.get_balance()
print(f"Cash: ${bal.get('balance',0)/100:.2f} | Portfolio: ${bal.get('portfolio_value',0)/100:.2f}")
print()

# Get all our orders
all_orders = []
cursor = ""
while True:
    orders = client.get_orders(limit=100, cursor=cursor if cursor else None)
    batch = orders.get("orders", [])
    if not batch:
        break
    all_orders.extend(batch)
    cursor = orders.get("cursor", "")
    if not cursor:
        break

# Filter to our fade/exit orders
our_orders = [o for o in all_orders if (o.get("client_order_id","").startswith("fade-") or o.get("client_order_id","").startswith("exit-"))]

print(f"Total orders: {len(all_orders)}, Our orders: {len(our_orders)}")
print()

buys = []
sells = []
for o in our_orders:
    cid = o.get("client_order_id", "")
    action = o.get("action", "")
    side = o.get("side", "")
    status = o.get("status", "")
    ticker = o.get("ticker", "")
    yes_price = o.get("yes_price", 0) or 0
    no_price = o.get("no_price", 0) or 0
    price = yes_price if side == "yes" else no_price
    remaining = o.get("remaining_count", 0)
    place_count = o.get("place_count", 0)
    filled = place_count - remaining if place_count else 0
    ts = o.get("created_time", "")[:19]

    entry = {"ts": ts, "action": action, "side": side, "status": status,
             "ticker": ticker, "price": price, "filled": filled, "cid": cid[:30]}

    if action == "buy":
        buys.append(entry)
    else:
        sells.append(entry)

print(f"Buys: {len(buys)} | Sells: {len(sells)}")
print()

# Show buys by status
filled_buys = [b for b in buys if b["filled"] > 0]
cancelled_buys = [b for b in buys if b["status"] == "canceled"]
print(f"Filled buys: {len(filled_buys)} | Cancelled buys: {len(cancelled_buys)}")

total_buy_cost = sum(b["price"] * b["filled"] / 100 for b in filled_buys)
print(f"Total buy cost: ${total_buy_cost:.2f}")
print()

# Entry price distribution
if filled_buys:
    prices = [b["price"] for b in filled_buys]
    print(f"Entry prices: min={min(prices)}c max={max(prices)}c avg={sum(prices)/len(prices):.0f}c")

    # Bucket
    for lo, hi in [(5,10),(10,20),(20,30),(30,40),(40,50)]:
        ct = sum(1 for p in prices if lo <= p < hi)
        if ct:
            print(f"  {lo}-{hi}c: {ct} trades")
    print()

# Show sells
filled_sells = [s for s in sells if s["filled"] > 0]
total_sell_rev = sum(s["price"] * s["filled"] / 100 for s in filled_sells)
print(f"Filled sells: {len(filled_sells)} | Revenue: ${total_sell_rev:.2f}")
print()

# Show sample trades
print("=== RECENT FILLED BUYS ===")
for b in filled_buys[-20:]:
    print(f"  {b['ts']} {b['side']:>3} {b['ticker'][-20:]:>20} {b['filled']}x @ {b['price']}c [{b['status']}]")

print()
print("=== FILLED SELLS ===")
for s in filled_sells[-20:]:
    print(f"  {s['ts']} {s['side']:>3} {s['ticker'][-20:]:>20} {s['filled']}x @ {s['price']}c [{s['status']}]")
