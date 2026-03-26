#!/usr/bin/env python3
import os
import csv
import asyncio
from typing import Dict, Any

try:
    from kalshi_client.client import KalshiClient
except Exception:
    try:
        from kalshi_client import KalshiClient
    except Exception:
        KalshiClient = None


# Minimal coin->series mapping (keeps in sync with agent)
COINS = {
    "BTC": "KXBTC15M",
}


def get_outcome_from_market(m: Dict[str, Any]) -> str:
    # Try several common field names
    for key in ("outcome", "result", "resolution", "resolution_name", "winning_outcome"):
        v = m.get(key)
        if v:
            s = str(v).upper()
            if "YES" in s:
                return "YES"
            if "NO" in s:
                return "NO"

    # Some APIs return settled price or winner flag
    yes_price = m.get("yes_price") or m.get("yes_bid_dollars") or m.get("yes_ask_dollars")
    no_price = m.get("no_price") or m.get("no_bid_dollars") or m.get("no_ask_dollars")
    try:
        if yes_price and float(yes_price) >= 99.5:
            return "YES"
        if no_price and float(no_price) >= 99.5:
            return "NO"
    except Exception:
        pass

    return ""


async def fetch_series_closed_markets(client, series: str):
    # try common method names
    fn = getattr(client, "get_markets", None)
    if not fn:
        raise RuntimeError("KalshiClient has no get_markets method available")

    try:
        data = await fn(series_ticker=series, status="closed", limit=500)
    except TypeError:
        # maybe method expects different args
        data = await fn(series, "closed")

    markets = []
    if isinstance(data, dict):
        markets = data.get("markets") or data.get("results") or []
    elif isinstance(data, list):
        markets = data

    return markets


async def main(input_csv: str, output_csv: str):
    if KalshiClient is None:
        print("kalshi_client library not importable. Install it or check PYTHONPATH.")
        return

    key_id = os.getenv("KALSHI_KEY_ID")
    private_key_env = os.getenv("KALSHI_PRIVATE_KEY")

    # Try to load PEM private key into an object if possible (accepts path or PEM text)
    priv_obj = None
    if private_key_env:
        try:
            # if env value is a path to a file, read it
            if os.path.exists(private_key_env):
                with open(private_key_env, "rb") as f:
                    key_bytes = f.read()
            else:
                key_bytes = private_key_env.encode()

            from cryptography.hazmat.primitives import serialization
            # Try PEM / PKCS#1 or PKCS#8
            try:
                priv_obj = serialization.load_pem_private_key(key_bytes, password=None)
            except Exception:
                # Try OpenSSH private key format
                try:
                    priv_obj = serialization.load_ssh_private_key(key_bytes, password=None)
                except Exception as e2:
                    raise e2
        except Exception as e:
            print(f"Warning: could not load private key as a supported object: {e}. Passing raw value to client.")
            priv_obj = private_key_env

    client = KalshiClient(key_id=key_id, private_key=priv_obj)

    # load input rows
    rows = []
    with open(input_csv, newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        for r in reader:
            rows.append(r)

    tickers = set(r.get("ticker") for r in rows if r.get("ticker"))

    market_map = {}
    for coin, series in COINS.items():
        print(f"Fetching closed markets for series {series}...")
        try:
            markets = await fetch_series_closed_markets(client, series)
            for m in markets:
                t = m.get("ticker") or m.get("market_ticker") or m.get("id")
                if t and t in tickers:
                    market_map[t] = m
        except Exception as e:
            print(f"Error fetching series {series}: {e}")

    # annotate rows
    out_field = "outcome"
    if out_field not in fieldnames:
        fieldnames = fieldnames + [out_field]

    with open(output_csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            t = r.get("ticker")
            out = r.get(out_field, "")
            if (not out) and t and t in market_map:
                out = get_outcome_from_market(market_map[t])
            r[out_field] = out
            writer.writerow(r)

    print(f"Wrote annotated CSV to {output_csv}. Found outcomes for {sum(1 for v in rows if v.get('outcome'))} rows.")


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--input", "-i", default="15m_signals.csv")
    p.add_argument("--output", "-o", default="15m_signals_out.csv")
    args = p.parse_args()

    asyncio.run(main(args.input, args.output))
