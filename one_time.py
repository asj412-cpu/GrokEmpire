import asyncio
from agents.trading.kalshi_daily_grok_agent import KalshiDailyGrokAgent

async def main():
    agent = KalshiDailyGrokAgent()
    markets = await agent.nightly_aggregate()
    selections = await agent.grok_select_trades(markets)
    await agent.place_trades(selections)
    print("One-time run complete")

asyncio.run(main())
