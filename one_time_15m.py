import asyncio
from datetime import datetime
from agents.trading.crypto_15m_agent import Crypto15mAgent

async def main():
    print(f"[15M AGENT START] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    agent = Crypto15mAgent()
    # Single discovery cycle (mocks if DRY_RUN)
    await agent.kalshi_discovery()
    print("[15M PAPER CYCLE COMPLETE] Check 15m_signals.csv for signals.")

asyncio.run(main())