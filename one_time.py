import asyncio
from datetime import datetime
from agents.trading.kalshi_daily_grok_agent import KalshiDailyGrokAgent

async def main():
    print(f"[DAILY GROK START] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    agent = KalshiDailyGrokAgent()
    markets = await agent.aggregate_markets()
    print(f"Markets aggregated: {len(markets)}")
    selections = await agent.grok_select_trades(markets)
    await agent.place_trades(selections)
    print("[PAPER CYCLE COMPLETE] Check daily_trades.csv for log. Simulated PnL range +/- 20% risk.")

asyncio.run(main())
