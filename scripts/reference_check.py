"""Check the Go metrics engine against an independent pandas implementation.

Reads the raw fixture trades (testdata/archive/*.parquet), recomputes every
10s and 60s window listed in testdata/metrics.golden.jsonl, and compares
the numbers. Exact values (volume, trade count, high, low, last) must match
exactly; VWAP, volatility and spread must match within a tiny tolerance.

    python3 -m venv .venv && .venv/bin/pip install -r scripts/requirements.txt
    .venv/bin/python scripts/reference_check.py
"""

import json
import math
import sys
from decimal import Decimal, getcontext
from pathlib import Path

import pandas as pd

getcontext().prec = 50
ROOT = Path(__file__).resolve().parent.parent
SECONDS_PER_YEAR = 365 * 86_400


def load_trades() -> pd.DataFrame:
    files = sorted((ROOT / "testdata/archive").rglob("*.parquet"))
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    # Replay order: event time, then exchange, then trade id.
    df = df.sort_values(["event_time_ns", "exchange", "trade_id"], kind="stable")
    return df.drop_duplicates(["exchange", "trade_id"]).reset_index(drop=True)


def window_stats(w: pd.DataFrame, secs: int) -> dict:
    prices = [Decimal(p) for p in w["price"]]
    sizes = [Decimal(s) for s in w["size"]]
    volume = sum(sizes)
    vwap = sum(p * q for p, q in zip(prices, sizes)) / volume

    # Realised variance per exchange from consecutive trades on that exchange,
    # averaged across exchanges, then annualised.
    variances, vwaps = [], {}
    for exch, g in w.groupby("exchange", sort=True):
        px = [float(p) for p in g["price"]]
        r = [math.log(b / a) for a, b in zip(px, px[1:])]
        if r:
            variances.append(sum(x * x for x in r))
        gq = [Decimal(s) for s in g["size"]]
        vwaps[exch] = sum(Decimal(p) * q for p, q in zip(g["price"], gq)) / sum(gq)
    vol = 0.0
    if variances:
        vol = math.sqrt(sum(variances) / len(variances)) * math.sqrt(SECONDS_PER_YEAR / secs)
    spread = 0.0
    if len(vwaps) >= 2:
        a, b = [vwaps[k] for k in sorted(vwaps)[:2]]
        spread = float((a - b) * 10_000 / ((a + b) / 2))
    last = w.sort_values("event_time_ns", kind="stable")["price"].iloc[-1]
    return {
        "trades": len(w),
        "volume": volume,
        "vwap": vwap,
        "high": max(prices),
        "low": min(prices),
        "last": Decimal(last),
        "vol": vol,
        "spread_bps": spread,
    }


def close(a: float, b: float, rel: float) -> bool:
    return abs(a - b) <= rel * max(1.0, abs(a), abs(b))


def main() -> int:
    trades = load_trades()
    golden = [json.loads(l) for l in (ROOT / "testdata/metrics.golden.jsonl").read_text().splitlines()]
    windows = [g for g in golden if "symbol" in g]
    failures = 0
    for g in windows:
        secs, start = g["window_secs"], g["start_ns"]
        end = start + secs * 1_000_000_000
        w = trades[(trades.symbol == g["symbol"]) & (trades.event_time_ns >= start) & (trades.event_time_ns < end)]
        ref = window_stats(w, secs)
        checks = [
            ("trades", ref["trades"] == g["trades"]),
            ("volume", ref["volume"] == Decimal(g["volume"])),
            ("high", ref["high"] == Decimal(g["high"])),
            ("low", ref["low"] == Decimal(g["low"])),
            ("last", ref["last"] == Decimal(g["last"])),
            ("vwap", abs(ref["vwap"] - Decimal(g["vwap"])) < Decimal("1e-9")),
            ("vol", close(ref["vol"], float(g["vol"]), 1e-7)),
            ("spread_bps", close(ref["spread_bps"], float(g["spread_bps"]), 1e-5)),
        ]
        for name, ok in checks:
            if not ok:
                failures += 1
                print(f"MISMATCH {g['symbol']} {secs}s @ {start}: {name} pandas={ref[name]} go={g[name]}")
    print(f"checked {len(windows)} windows from {len(trades)} trades: {failures} mismatches")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
