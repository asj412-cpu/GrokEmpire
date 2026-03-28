import asyncio
import os
from agents.trading.crypto_15m_agent import Crypto15mAgent

async def main():
    print("[SUPERVISOR] Starting continuous 15m paper trading agent for data accumulation...")
    agent = Crypto15mAgent()
    try:
        await agent.run()
    except KeyboardInterrupt:
        agent.running = False
        print("[SUPERVISOR] Shutting down 15m agent...")
    except Exception as e:
        print(f"[SUPERVISOR] Error in 15m agent: {e}")

if __name__ == "__main__":
    asyncio.run(main())
