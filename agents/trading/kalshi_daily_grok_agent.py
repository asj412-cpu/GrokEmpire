import asyncio
import os
import csv
from datetime import datetime
import json
from openai import OpenAI
from kalshi_client.client import KalshiClient
from collections import deque
import time
import random
from tools.data_sources import *
from cryptography.hazmat.primitives import serialization

BASE_CASH_FLOOR = 40.0
RATCHET_PERCENT = 0.80
DAILY_RISK_PCT = float(os.getenv('DAILY_RISK_PCT', '0.25'))
DRY_RUN = os.getenv('DRY_RUN', 'false').lower() == 'true'

class KalshiDailyGrokAgent:
    def __init__(self):
        key_id = os.getenv("KALSHI_KEY_ID")
        private_key_path = os.getenv("KALSHI_PRIVATE_KEY")
        self.client = None
        if key_id and private_key_path:
            try:
                with open(private_key_path, 'rb') as f:
                    private_key = serialization.load_pem_private_key(f.read(), password=None)
                self.client = KalshiClient(key_id=key_id, private_key=private_key, exchange_api_base='https://108.139.47.10/trade-api/v2')
                self.client.session.headers['Host'] = 'api.elections.kalshi.com'
            except Exception as e:
                print(f"Kalshi client init failed ({e}) — dry run mode")
                self.client = None
        else:
            print("No Kalshi keys — dry run mode only")
        if DRY_RUN:
            self.client = None
            print("DRY_RUN=True: forcing full mocks")
        else:
            print("Live Kalshi client ready")

        self.grok = OpenAI(
            api_key=os.getenv("XAI_API_KEY") or os.getenv("OPENAI_API_KEY"),
            base_url="https://api.x.ai/v1"
        )
        GROK_MODEL = "grok-4-1-fast-reasoning"
        self.running = True
        self.log_file = "daily_trades.csv"
        self.current_cash_floor = BASE_CASH_FLOOR
        self.last_balance = 0.0
        self.positions = {}

        if not os.path.exists(self.log_file):
            with open(self.log_file, "w", newline="") as f:
                csv.writer(f).writerow([
                    "timestamp", "ticker", "title", "side", "contracts", "price", "status",
                    "available_cash", "risk_per_trade", "grok_rationale", "confidence", "pnl"
                ])

    async def get_balance(self):
        if not self.client:
            return 49.0  # mock for dry run
        try:
            data = await self.client.get_balance()
            balance_cents = data.get("balance", 0) if isinstance(data, dict) else 0
            return balance_cents / 100.0
        except Exception as e:
            print(f"Balance check error: {e}")
            return 49.0  # fallback to $49

    def extract_city(self, ticker):
        # Simple extract for weather
        if 'NY' in ticker:
            return 'NYC'
        if 'LA' in ticker:
            return 'LA'
        return 'NYC'  # default

    async def aggregate_markets(self):
        if not self.client:
            from tools.data_sources import get_econ_summary, get_crypto_prices, get_fear_greed
            econ = get_econ_summary()
            crypto = get_crypto_prices()
            fg = get_fear_greed()
            # Rich mocks for DRY_RUN demo
            mock_markets = [
                {
                    'ticker': 'CPI-M0.2-ABOVE',
                    'title': 'CPI MoM > 0.2% next release?',
                    'yes_bid': 45,
                    'yes_ask': 48,
                    'mid': 46.5,
                    'settle_hours': 24,
                    'volume': 5000,
                    'category': 'econ',
                    'data': {'econ': econ}
                },
                {
                    'ticker': 'BTC-100K-EOW',
                    'title': 'BTC > $100k by EOW?',
                    'yes_bid': 55,
                    'yes_ask': 58,
                    'mid': 56.5,
                    'settle_hours': 36,
                    'volume': 8000,
                    'category': 'crypto',
                    'data': {'crypto': crypto}
                },
                {
                    'ticker': 'VIX-20-ABOVE',
                    'title': 'VIX closes above 20 tomorrow?',
                    'yes_bid': 62,
                    'yes_ask': 65,
                    'mid': 63.5,
                    'settle_hours': 28,
                    'volume': 3000,
                    'category': 'volatility',
                    'data': {'fear_greed': fg}
                }
            ]
            print("DRY_RUN: Using rich mock markets with live data")
            return mock_markets

        try:
            print("Starting aggregation")
            data = await self.client.get_markets(status="open", limit=1000)
            print(f"Data type: {type(data)}, keys: {data.keys() if isinstance(data, dict) else 'not dict'}")
        except Exception as e:
            print(f"Get markets error: {e}")
            # Fallback mock
            mock_markets = []
            print("Using fallback mocks")
            return mock_markets

        short_markets = []
        for m in data.get("markets", []):
            settle_seconds = m.get("settlement_timer_seconds_from_now", 0)
            settle_hours = settle_seconds / 3600.0
            volume = m.get("volume_fp", 0)
            if 0 < settle_hours < 48 and volume > 1000:
                enriched = {
                    'ticker': m['ticker'],
                    'title': m['title'],
                    'yes_bid': m.get('yes_bid_dollars', 0),
                    'yes_ask': m.get('yes_ask_dollars', 0),
                    'mid': (m.get('yes_bid_dollars', 0) + m.get('yes_ask_dollars', 0)) / 2,
                    'settle_hours': settle_hours,
                    'volume': volume,
                    'category': m.get('category', ''),
                    'data': {}
                }
                # Enrich based on title
                title_lower = m['title'].lower()
                if 'weather' in title_lower or 'temp' in title_lower:
                    city = self.extract_city(m['ticker'])
                    enriched['data']['noaa'] = get_noaa_forecast(city)
                if 'fed' in title_lower or 'cpi' in title_lower or 'inflation' in title_lower:
                    enriched['data']['econ'] = get_econ_summary()
                if 'crypto' in title_lower or 'btc' in title_lower:
                    enriched['data']['crypto'] = get_crypto_prices()
                # General
                enriched['data']['news'] = get_google_news()
                enriched['data']['fear_greed'] = get_fear_greed()
                short_markets.append(enriched)
        print(f"Aggregated {len(short_markets)} short-settle markets")
        return short_markets

    async def grok_select_trades(self, markets):
        if not markets:
            return []

        # Global real-time data snapshot
        from tools.data_sources import get_econ_summary, get_crypto_prices, get_fear_greed, get_vix
        global_data = {
            'econ': get_econ_summary(),
            'crypto': get_crypto_prices(),
            'fear_greed': get_fear_greed(),
            'vix': get_vix()
        }

        prompt = f"""
You are a Kalshi trading expert. Use the provided real-time data including current FRED CPI, Fear & Greed index, VIX, BLS CPI, crypto prices, Truflation, Cleveland nowcasts. 

GLOBAL DATA:
{json.dumps(global_data, indent=2)}

MARKETS (48h settle):
{json.dumps(markets[:50], indent=2)}

Available cash: $49, risk 25% daily ($12.25 total).
Select top 3-5 trades: ticker, side ('yes'/'no'), contracts (1-100), confidence (60-100), rationale.
Calculate true edge: market-implied prob vs your data-driven prob.

Return ONLY valid JSON array: [{{"ticker": "TICKER", "side": "yes/no", "contracts": 10, "confidence": 75, "rationale": "Detailed why + edge calc"}}]
"""
        try:
            content = prompt
            response = self.grok.chat.completions.create(
                model="grok-4-1-fast-reasoning",
                messages=[{"role": "user", "content": content}]
            )
            content = response.choices[0].message.content
            print(f"Grok response: {content}")
            selections = json.loads(content)
            if isinstance(selections, list):
                print("\n🧠 GROK RECOMMENDATIONS:")
                for i, sel in enumerate(selections, 1):
                    conf = sel.get('confidence', 'N/A')
                    print(f"  {i}. {sel['ticker']} | {sel['side'].upper()} | {sel['contracts']} contracts | Conf: {conf}%")
                    print(f"     Rationale: {sel['rationale']}")
                print()
            return selections if isinstance(selections, list) else []
        except Exception as e:
            print(f"Grok error: {e}")
            return []

    def get_cumulative_pnl(self):
        total = 0.0
        if os.path.exists(self.log_file):
            with open(self.log_file, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    pnl_str = row.get('pnl', '0')
                    try:
                        total += float(pnl_str)
                    except ValueError:
                        pass
        print(f"💰 CUMULATIVE PAPER PnL: ${total:.2f}")
        return total

    async def place_trades(self, selections):
        balance = await self.get_balance() or 49.0
        available = max(0.0, balance - self.current_cash_floor)
        risk_total = available * DAILY_RISK_PCT
        risk_per_trade = risk_total / len(selections) if selections else 0

        print(f"📊 DAILY RISK ALLOCATION (25% budget):")
        print(f"   Available: ${available:.2f} | Total risk: ${risk_total:.2f} ({DAILY_RISK_PCT*100}%)")
        print(f"   Per trade ({len(selections)}): ${risk_per_trade:.2f}")
        print()

        for sel in selections:
            ticker = sel['ticker']
            side = sel['side']
            contracts = min(100, max(1, sel.get('contracts', 1)))
            price = 90 if side == 'yes' else 10  # rough
            rationale = sel.get('rationale', '')

            conf = sel.get('confidence', 70)

            pnl_str = ""

            if DRY_RUN:
                mock_return = (conf / 100.0 - 0.5) * 2 * random.uniform(0.6, 1.4)
                pnl = contracts * (price / 100.0) * mock_return
                pnl_str = f"{pnl:.2f}"
                status = "DRY_RUN_SUCCESS"
                self.positions[ticker] = {'side': side, 'contracts': contracts, 'entry_price': price/100.0}
            else:
                try:
                    result = await self.client.place_order(ticker, side, contracts, price=price)
                    status = "SUCCESS" if result else "FAILED"
                    if status == "SUCCESS":
                        self.positions[ticker] = {'side': side, 'contracts': contracts, 'entry_price': price/100.0}
                except Exception as e:
                    status = f"ERROR: {e}"

            with open(self.log_file, "a", newline="") as f:
                csv.writer(f).writerow([
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"), ticker, "", side, contracts, price, status,
                    available, risk_per_trade, rationale, conf, pnl_str
                ])
            conf = sel.get('confidence', 'N/A')
            print(f"✅ EXECUTED: {ticker} | {side.upper()} | {contracts} contracts @ {price}¢ | Conf: {conf}%")
            print(f"   Risk: ${risk_per_trade:.2f} | Rationale: {rationale}")
            print()

    async def monitor_positions(self):
        while self.running:
            try:
                if self.client and self.positions:
                    data = await self.client.get_positions()
                    for p in data.get('positions', []):
                        ticker = p['ticker']
                        if ticker in self.positions:
                            pos = self.positions[ticker]
                            current_mid = (p.get('yes_bid_dollars', 0) + p.get('yes_ask_dollars', 0)) / 2
                            pnl_pct = (current_mid - pos['entry_price']) / pos['entry_price'] if pos['side'] == 'yes' else (pos['entry_price'] - current_mid) / pos['entry_price']
                            if pnl_pct <= -0.25 or pnl_pct >= 0.50:
                                await self.client.close_position(ticker)
                                del self.positions[ticker]
                                print(f"Early exit {ticker} PnL {pnl_pct:.1%}")
            except Exception as e:
                print(f"Monitor error: {e}")
            await asyncio.sleep(300)  # 5 min

    async def daily_reconcile(self):
        # Daily PnL reconcile
        try:
            if self.client:
                positions = await self.client.get_positions()
                # Match to log, update PnL
                # For simplicity, log current positions
                print(f"Daily reconcile: {len(positions.get('positions', []))} open positions")
        except Exception as e:
            print(f"Reconcile error: {e}")

    async def run(self):
        print("🚀 [DAILY GROK] Kalshi Daily Agent Starting (4h cycles)")
        asyncio.create_task(self.monitor_positions())

        while self.running:
            print("🚀 [DAILY GROK] Cycle start...")
            markets = await self.aggregate_markets()
            selections = await self.grok_select_trades(markets)
            print(f"[DAILY GROK] Grok selected {len(selections)} trades: {selections}")
            await self.place_trades(selections)
            self.get_cumulative_pnl()
            print("[DAILY GROK] Cycle end, sleep 4h")
            await asyncio.sleep(14400)

async def main():
    agent = KalshiDailyGrokAgent()
    try:
        await agent.run()
    except KeyboardInterrupt:
        print("\nShutting down...")

if __name__ == "__main__":
    asyncio.run(main())