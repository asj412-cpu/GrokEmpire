#!/usr/bin/env python3
import csv
import argparse
from typing import List, Dict


def load_trades(path: str) -> List[Dict]:
    trades = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            trades.append(r)
    return trades


def parse_int(val, default=0):
    try:
        return int(float(val))
    except Exception:
        return default


def compute_pnl(trades: List[Dict]):
    results = []
    equity = 0.0
    equity_curve = []
    returns = []

    for t in trades:
        status = (t.get("status") or "").upper()
        client_order_id = (t.get("client_order_id") or "").strip()

        if not client_order_id and status not in ("SUCCESS", "PLACED", "FILLED"):
            continue

        decision = (t.get("decision") or "").upper()
        contracts = parse_int(t.get("contracts", 0), 0)
        price_c = parse_int(t.get("price", t.get("entry_price", 0)), 0)
        outcome = (t.get("outcome") or "").upper()

        if contracts <= 0 or price_c <= 0:
            continue

        if decision.startswith("BUY YES"):
            correct = outcome == "YES"
        elif decision.startswith("BUY NO"):
            correct = outcome == "NO"
        else:
            continue

        if correct:
            profit_c = 100 - price_c
        else:
            profit_c = -price_c

        profit_d = (profit_c * contracts) / 100.0
        ret_pct = profit_d / (contracts * price_c / 100.0) if price_c > 0 else 0.0
        equity += profit_d
        equity_curve.append(equity)
        returns.append(ret_pct)

        results.append({
            "timestamp": t.get("timestamp"),
            "coin": t.get("coin"),
            "ticker": t.get("ticker"),
            "decision": decision,
            "contracts": contracts,
            "price_c": price_c,
            "outcome": outcome,
            "profit": profit_d,
            "return_pct": ret_pct,
            "cum_equity": equity,
        })

    total = len(results)
    wins = sum(1 for r in results if r["profit"] > 0)
    losses = total - wins
    total_pnl = equity
    avg_pnl = total_pnl / total if total else 0.0
    avg_ret = sum(returns) / total if total else 0.0

    # max drawdown
    peak = equity_curve[0] if equity_curve else 0
    max_dd = 0.0
    for v in equity_curve:
        if v > peak:
            peak = v
        dd = peak - v
        max_dd = max(max_dd, dd)

    # Sharpe ratio (assume 1/15min risk-free rate ~0)
    sharpe = (sum(returns) / len(returns)) / (sum(r**2 for r in returns)**0.5 / len(returns)) if returns else 0.0 if total else 0.0

    metrics = {
        "trades": total,
        "wins": wins,
        "losses": losses,
        "win_rate": wins / total if total else 0.0,
        "total_pnl": total_pnl,
        "avg_pnl": avg_pnl,
        "avg_return_pct": avg_ret * 100,
        "sharpe_ratio": sharpe,
        "max_drawdown": max_dd,
    p = argparse.ArgumentParser(description="Enhanced backtester for 15m signals CSV")
    p.add_argument("--input", "-i", default="15m_signals.csv", help="signals CSV path")
    p.add_argument("--plot", "-p", help="Save equity curve plot to PNG")
    p.add_argument("--export", "-e", help="Export trades to CSV")
    args = p.parse_args()

    trades = load_trades(args.input)
    metrics, results, equity_curve = compute_pnl(trades)

    print("=== Backtest Summary ===")
    print(f"Trades: {metrics['trades']:,}")
    print(f"Wins/Losses: {metrics['wins']:,}/{metrics['losses']:,}")
    print(f"Win Rate: {metrics['win_rate']:.2%}")
    print(f"Total P&L: ${metrics['total_pnl']:.2f}")
    print(f"Avg PnL/Trade: ${metrics['avg_pnl']:.2f}")
    print(f"Avg Return: {metrics['avg_return_pct']:.2f}%")
    print(f"Sharpe Ratio: {metrics['sharpe_ratio']:.2f}")
    print(f"Profit Factor: {metrics['profit_factor']:.2f}")
    print(f"Max Drawdown: ${metrics['max_drawdown']:.2f}")

    if args.export:
        import pandas as pd
        pd.DataFrame(results).to_csv(args.export, index=False)
        print(f"Exported trades to {args.export}")

    if args.plot and equity_curve:
        import matplotlib.pyplot as plt
        plt.figure(figsize=(12, 6))
        plt.plot(equity_curve)
        plt.title("Equity Curve")
        plt.xlabel("Trade #")
        plt.ylabel("Cumulative P&L ($)")
        plt.grid(True)
        plt.savefig(args.plot)
        print(f"Equity curve saved to {args.plot}")    print(f" Trades: {metrics['trades']}")
    print(f" Wins: {metrics['wins']}")
    print(f" Losses: {metrics['losses']}")
    print(f" Win rate: {metrics['win_rate']:.2%}")
    print(f" Total P&L: ${metrics['total_pnl']:.2f}")
    print(f" Avg per trade: ${metrics['avg_pnl']:.2f}")
    print(f" Max drawdown: ${metrics['max_drawdown']:.2f}")


if __name__ == "__main__":
    main()
