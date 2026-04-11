"""
Diagnostic: inspect raw Keepa CSV series for a given ASIN.
Checks type-18 (buy box + shipping), type-0 (Amazon), type-1 (new 3P),
and type-10 (new FBA) for any price entry at or near $19.52 (1952 cents).

Usage:
    python inspect_csv_series.py [ASIN]

Defaults to B0DPGJV2DW if no ASIN is provided.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from typing import Any

from dotenv import load_dotenv

load_dotenv()

from src.keepa_client import (
    CSV_BUY_BOX_SHIPPING,
    KEEPA_EPOCH_OFFSET_MINUTES,
    KEEPA_PRODUCT_URL,
    KeepaClient,
    _extract_csv_series,
    _extract_min_interval_value,
    _extract_min_interval_timestamp,
    _parse_keepa_price_with_shipping_series,
    _coerce_int,
    _coerce_float,
)

TARGET_CENTS = 1952
NEAR_BAND_CENTS = 50  # ± $0.50


def keepa_minutes_to_utc(keepa_minutes: int) -> str:
    unix_ts = (keepa_minutes + KEEPA_EPOCH_OFFSET_MINUTES) * 60
    return datetime.fromtimestamp(unix_ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def parse_pair_series(raw_series: list[Any]) -> list[tuple[int, float]]:
    """Parse a plain [keepa_minutes, price_cents, keepa_minutes, price_cents, ...] series."""
    points: list[tuple[int, float]] = []
    for i in range(0, len(raw_series) - 1, 2):
        ts = _coerce_int(raw_series[i])
        price = _coerce_float(raw_series[i + 1])
        if ts is None or price is None:
            continue
        points.append((ts, price))
    return points


def inspect_series(
    product: dict,
    csv_index: int,
    label: str,
    is_triplet: bool,
) -> None:
    raw = _extract_csv_series(product, csv_index)
    step = 3 if is_triplet else 2
    total_entries = len(raw) // step

    print(f"\n{'=' * 70}")
    print(f"CSV index {csv_index} — {label}")
    print(f"Raw element count: {len(raw)}  ({total_entries} entries)")

    if not raw:
        print("  Series is EMPTY — no data available for this ASIN.")
        return

    # Show first/last 5 raw entries
    if is_triplet:
        print(f"\nFirst 5 raw triplets [keepa_minutes, price_cents, shipping_cents]:")
        for i in range(0, min(15, len(raw) - 2), 3):
            print(f"  [{raw[i]}, {raw[i+1]}, {raw[i+2]}]")
        print(f"\nLast 5 raw triplets:")
        last_start = max(0, (total_entries - 5) * 3)
        for i in range(last_start, len(raw) - 2, 3):
            print(f"  [{raw[i]}, {raw[i+1]}, {raw[i+2]}]")
    else:
        print(f"\nFirst 5 raw pairs [keepa_minutes, price_cents]:")
        for i in range(0, min(10, len(raw) - 1), 2):
            print(f"  [{raw[i]}, {raw[i+1]}]")
        print(f"\nLast 5 raw pairs:")
        last_start = max(0, (total_entries - 5) * 2)
        for i in range(last_start, len(raw) - 1, 2):
            print(f"  [{raw[i]}, {raw[i+1]}]")

    # Parse
    if is_triplet:
        points = _parse_keepa_price_with_shipping_series(raw)
    else:
        points = parse_pair_series(raw)

    valid = [(ts, v) for ts, v in points if v >= 0]
    if not valid:
        print("\n  No valid (non-negative) price points after parsing.")
        return

    earliest = valid[0]
    latest = valid[-1]
    min_p = min(valid, key=lambda p: p[1])
    max_p = max(valid, key=lambda p: p[1])

    print(f"\nParsed price range ({len(valid)} valid points):")
    print(f"  Earliest : {keepa_minutes_to_utc(earliest[0])}  ${earliest[1]/100:.2f}")
    print(f"  Latest   : {keepa_minutes_to_utc(latest[0])}  ${latest[1]/100:.2f}")
    print(f"  Min price: {keepa_minutes_to_utc(min_p[0])}  ${min_p[1]/100:.2f}")
    print(f"  Max price: {keepa_minutes_to_utc(max_p[0])}  ${max_p[1]/100:.2f}")

    # Exact match
    exact = [(i, ts, v) for i, (ts, v) in enumerate(valid) if v == TARGET_CENTS]
    print(f"\nExact match at $19.52 (1952 cents): {len(exact)} hit(s)")
    for idx, ts, v in exact:
        # Show surrounding context: 3 points before and after
        lo = max(0, idx - 3)
        hi = min(len(valid) - 1, idx + 3)
        print(f"\n  *** FOUND at valid_index={idx} ***")
        print(f"  Context window (index {lo}..{hi}):")
        for j in range(lo, hi + 1):
            cts, cv = valid[j]
            marker = " <-- $19.52" if j == idx else ""
            print(f"    [{j}] {keepa_minutes_to_utc(cts)}  ${cv/100:.2f}{marker}")

    if not exact:
        print(f"  Not found.")

    # Near match (within ± $0.50)
    near_lo = TARGET_CENTS - NEAR_BAND_CENTS
    near_hi = TARGET_CENTS + NEAR_BAND_CENTS
    near = [(i, ts, v) for i, (ts, v) in enumerate(valid) if near_lo <= v <= near_hi and v != TARGET_CENTS]
    print(f"\nNear matches ${(TARGET_CENTS - NEAR_BAND_CENTS)/100:.2f}–${(TARGET_CENTS + NEAR_BAND_CENTS)/100:.2f} (excl. exact): {len(near)} hit(s)")
    for idx, ts, v in near:
        lo = max(0, idx - 3)
        hi = min(len(valid) - 1, idx + 3)
        print(f"\n  Near match at valid_index={idx}, price=${v/100:.2f}")
        print(f"  Context window (index {lo}..{hi}):")
        for j in range(lo, hi + 1):
            cts, cv = valid[j]
            marker = f" <-- ${cv/100:.2f}" if j == idx else ""
            print(f"    [{j}] {keepa_minutes_to_utc(cts)}  ${cv/100:.2f}{marker}")


def fetch_product_with_stats(client: KeepaClient, asin: str, stats_days: int) -> dict:
    """Fetch a single product using a custom stats window (bypasses get_product's hardcoded stats=90)."""
    from src.keepa_client import HISTORY_QUERY_DAYS
    params = {
        "key": client.api_key,
        "domain": client.domain_id,
        "asin": asin,
        "stats": stats_days,
        "history": 1,
        "offers": 20,
        "stock": 1,
        "days": HISTORY_QUERY_DAYS,
    }
    payload = client._request_json(KEEPA_PRODUCT_URL, params=params)
    products = payload.get("products")
    if not isinstance(products, list) or not products:
        raise RuntimeError("Keepa API returned no products.")
    return products[0]


def print_min_in_interval(stats: dict, stats_days: int) -> None:
    raw = stats.get("minInInterval")
    entry = raw[CSV_BUY_BOX_SHIPPING] if isinstance(raw, list) and len(raw) > CSV_BUY_BOX_SHIPPING else None
    print(f"\n=== stats['minInInterval'][{CSV_BUY_BOX_SHIPPING}]  (stats={stats_days}) ===")
    print(f"  Raw entry : {entry}")
    val_cents = _extract_min_interval_value(raw, CSV_BUY_BOX_SHIPPING)
    ts_keepa  = _extract_min_interval_timestamp(raw, CSV_BUY_BOX_SHIPPING)
    if val_cents is not None:
        print(f"  Min price : ${val_cents / 100:.2f}  ({val_cents} cents)")
    else:
        print(f"  Min price : None")
    if ts_keepa is not None:
        ts_unix = (ts_keepa + KEEPA_EPOCH_OFFSET_MINUTES) * 60
        ts_utc  = datetime.fromtimestamp(ts_unix, tz=timezone.utc).isoformat().replace("+00:00", "Z")
        print(f"  Timestamp : {ts_utc}  (keepa_min={ts_keepa})")
    else:
        print(f"  Timestamp : None")


def main() -> None:
    asin = sys.argv[1].strip().upper() if len(sys.argv) > 1 else "B0DPGJV2DW"
    api_key = os.getenv("KEEPA_API_KEY", "").strip()
    domain_id = int(os.getenv("KEEPA_DOMAIN_ID", "1").strip() or "1")

    if not api_key:
        print("ERROR: KEEPA_API_KEY not set in environment.", file=sys.stderr)
        sys.exit(1)

    client = KeepaClient(api_key=api_key, domain_id=domain_id)

    # ── stats=90 fetch (baseline) ──────────────────────────────────────────────
    print(f"Fetching product data for ASIN: {asin} (domain {domain_id}, stats=90) ...")
    product_90 = fetch_product_with_stats(client, asin, stats_days=90)
    stats_90 = product_90.get("stats") if isinstance(product_90.get("stats"), dict) else {}
    print_min_in_interval(stats_90, stats_days=90)

    # ── stats=180 fetch ────────────────────────────────────────────────────────
    print(f"\nFetching product data for ASIN: {asin} (domain {domain_id}, stats=180) ...")
    product_180 = fetch_product_with_stats(client, asin, stats_days=180)
    stats_180 = product_180.get("stats") if isinstance(product_180.get("stats"), dict) else {}
    print_min_in_interval(stats_180, stats_days=180)

    # ── comparison ─────────────────────────────────────────────────────────────
    val_90  = _extract_min_interval_value(stats_90.get("minInInterval"),  CSV_BUY_BOX_SHIPPING)
    val_180 = _extract_min_interval_value(stats_180.get("minInInterval"), CSV_BUY_BOX_SHIPPING)
    print(f"\n=== Comparison ===")
    print(f"  stats=90  min : ${val_90/100:.2f}"  if val_90  is not None else "  stats=90  min : None")
    print(f"  stats=180 min : ${val_180/100:.2f}" if val_180 is not None else "  stats=180 min : None")
    if val_90 is not None and val_180 is not None:
        if val_90 == val_180:
            print(f"  -> Same value. The $19.52 low occurred within the last 90 days (or is Keepa's all-time floor).")
        else:
            print(f"  -> DIFFERENT. 180-day window exposes a lower/different price not visible in the 90-day stats.")

    # ── full CSV series inspection (using stats=90 product, data is the same) ──
    product = product_90

    series_configs = [
        (CSV_BUY_BOX_SHIPPING, "Buy Box + Shipping",  True),   # 18
        (0,                    "Amazon (1P seller)",   False),
        (1,                    "New 3P lowest",         False),
        (10,                   "New FBA lowest",        False),
    ]

    for csv_index, label, is_triplet in series_configs:
        inspect_series(product, csv_index, label, is_triplet)

    print(f"\n{'=' * 70}")
    print("Done.")


if __name__ == "__main__":
    main()
