from __future__ import annotations

import argparse
import json
import os
import re
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.keepa_client import KEEPA_EPOCH_OFFSET_MINUTES, KeepaClient, KeepaError
from src.models import FilterResult, KeepaMetrics, PolicyDecision
from src.policy import evaluate_keepa_only
from src.profitability import (
    INBOUND_SHIPPING_USD_PER_LB,
    compute_inbound_shipping_fee,
    compute_profitability_metrics,
    parse_buy_box_range_midpoint,
)
from src.sp_api_client import SPAPIClient, SPAPIError


def _load_dotenv_if_available() -> None:
    try:
        from dotenv import load_dotenv
    except Exception:
        return
    load_dotenv()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export one ASIN evaluation case as JSON.")
    parser.add_argument("asin", help="ASIN to evaluate")
    parser.add_argument(
        "--landed-cost",
        type=float,
        default=None,
        help="Optional landed cost per unit (to compute Profit/ROI/Margin in fee context)",
    )
    return parser.parse_args()


def _next_eval_id(evals_dir: Path) -> int:
    max_id = 0
    for path in evals_dir.glob("eval_*.json"):
        match = re.search(r"^eval_(\d+)", path.name)
        if not match:
            continue
        try:
            max_id = max(max_id, int(match.group(1)))
        except ValueError:
            continue
    return max_id + 1


def _keepa_minutes_to_iso(keepa_minutes: int) -> str:
    unix_minutes = keepa_minutes + KEEPA_EPOCH_OFFSET_MINUTES
    dt = datetime.fromtimestamp(unix_minutes * 60, tz=timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def _serialize_buy_box_price_history(
    history: list[tuple[int, float]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for ts, price in history:
        rows.append(
            {
                "keepa_minutes": int(ts),
                "timestamp_utc": _keepa_minutes_to_iso(int(ts)),
                "price_usd": float(price),
            }
        )
    return rows


def _serialize_buy_box_seller_history(
    history: list[tuple[int, str]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for ts, seller_id in history:
        rows.append(
            {
                "keepa_minutes": int(ts),
                "timestamp_utc": _keepa_minutes_to_iso(int(ts)),
                "seller_id": str(seller_id),
            }
        )
    return rows


def _build_sp_api_client() -> tuple[SPAPIClient | None, str | None]:
    client_id = os.getenv("SP_API_LWA_CLIENT_ID", "").strip()
    client_secret = os.getenv("SP_API_LWA_CLIENT_SECRET", "").strip()
    refresh_token = os.getenv("SP_API_REFRESH_TOKEN", "").strip()
    if not client_id or not client_secret or not refresh_token:
        return None, "SP-API credentials missing in environment"
    try:
        client = SPAPIClient(
            lwa_client_id=client_id,
            lwa_client_secret=client_secret,
            refresh_token=refresh_token,
            marketplace_id=os.getenv("SP_API_MARKETPLACE_ID", "ATVPDKIKX0DER").strip()
            or "ATVPDKIKX0DER",
            endpoint=os.getenv("SP_API_ENDPOINT", "https://sellingpartnerapi-na.amazon.com").strip()
            or "https://sellingpartnerapi-na.amazon.com",
        )
    except Exception as exc:
        return None, f"SP-API client init failed: {exc}"
    return client, None


def _coerce_float_env(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    if value < 0:
        return default
    return value


def build_fee_context_from_components(
    *,
    estimated_sell_price: float | None,
    buy_box_range_current: str | None,
    amazon_fees_total: float | None,
    referral_fee: float | None,
    fba_fulfillment_fee: float | None,
    fee_breakdown: dict[str, float] | None,
    package_weight_grams: float | None,
    inbound_shipping_rate_per_lb: float,
    inbound_shipping_fee: float | None,
    landed_cost_per_unit: float | None,
    profit_per_unit: float | None,
    roi_percent: float | None,
    margin_percent: float | None,
    errors: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "estimated_sell_price_mid_bb": estimated_sell_price,
        "buy_box_range_current": buy_box_range_current,
        "amazon_fees_total": amazon_fees_total,
        "referral_fee": referral_fee,
        "fba_fulfillment_fee": fba_fulfillment_fee,
        "fee_breakdown": dict(fee_breakdown or {}),
        "package_weight_grams": package_weight_grams,
        "inbound_shipping_rate_per_lb": inbound_shipping_rate_per_lb,
        "inbound_shipping_fee": inbound_shipping_fee,
        "landed_cost_per_unit": landed_cost_per_unit,
        "profit_per_unit": profit_per_unit,
        "roi_percent": roi_percent,
        "margin_percent": margin_percent,
        "errors": list(errors or []),
    }


def _build_fee_context(
    asin: str,
    keepa_metrics: KeepaMetrics,
    landed_cost_per_unit: float | None,
) -> dict[str, Any]:
    errors: list[str] = []

    estimated_sell_price = parse_buy_box_range_midpoint(keepa_metrics.buy_box_range_current)
    if estimated_sell_price is None:
        errors.append("Cannot parse Buy Box Range")

    inbound_rate = _coerce_float_env("INBOUND_SHIPPING_USD_PER_LB", INBOUND_SHIPPING_USD_PER_LB)
    inbound_shipping_fee: float | None = None
    if keepa_metrics.package_weight_grams is None or keepa_metrics.package_weight_grams <= 0:
        errors.append("Missing Keepa package weight")
    else:
        inbound_shipping_fee = compute_inbound_shipping_fee(
            keepa_metrics.package_weight_grams,
            rate_per_lb=inbound_rate,
        )
        if inbound_shipping_fee is None:
            errors.append("Cannot compute inbound shipping fee")

    amazon_fees_total: float | None = None
    referral_fee: float | None = None
    fba_fulfillment_fee: float | None = None
    fee_breakdown: dict[str, float] = {}

    sp_client, sp_error = _build_sp_api_client()
    if sp_client is None:
        errors.append(sp_error or "SP-API fee estimate failed")
    elif estimated_sell_price is not None:
        try:
            fee_estimate = sp_client.get_fba_fees_estimate(asin=asin, price=estimated_sell_price)
            amazon_fees_total = fee_estimate.total_fees
            referral_fee = fee_estimate.referral_fee
            fba_fulfillment_fee = fee_estimate.fba_fulfillment_fee
            fee_breakdown = dict(fee_estimate.breakdown)
        except SPAPIError as exc:
            errors.append(f"SP-API fee estimate failed: {exc}")

    profit_per_unit: float | None = None
    roi_percent: float | None = None
    margin_percent: float | None = None
    if (
        landed_cost_per_unit is not None
        and landed_cost_per_unit > 0
        and estimated_sell_price is not None
        and amazon_fees_total is not None
        and inbound_shipping_fee is not None
    ):
        profit_per_unit, roi_percent, margin_percent = compute_profitability_metrics(
            estimated_sell_price=estimated_sell_price,
            landed_cost_per_unit=landed_cost_per_unit,
            amazon_fees_total=amazon_fees_total,
            inbound_shipping_fee=inbound_shipping_fee,
        )

    return build_fee_context_from_components(
        estimated_sell_price=estimated_sell_price,
        buy_box_range_current=keepa_metrics.buy_box_range_current,
        amazon_fees_total=amazon_fees_total,
        referral_fee=referral_fee,
        fba_fulfillment_fee=fba_fulfillment_fee,
        fee_breakdown=fee_breakdown,
        package_weight_grams=keepa_metrics.package_weight_grams,
        inbound_shipping_rate_per_lb=inbound_rate,
        inbound_shipping_fee=inbound_shipping_fee,
        landed_cost_per_unit=landed_cost_per_unit,
        profit_per_unit=profit_per_unit,
        roi_percent=roi_percent,
        margin_percent=margin_percent,
        errors=errors,
    )


def _build_key_metrics(keepa_metrics: KeepaMetrics) -> dict[str, Any]:
    metrics = asdict(keepa_metrics)
    metrics.pop("buy_box_price_history", None)
    metrics.pop("buy_box_seller_history", None)
    return metrics


def build_gate_decision_from_filter_result(result: FilterResult) -> dict[str, Any]:
    return {
        "qualified": result.qualified,
        "decision": result.decision,
        "reason": result.reason,
        "recommended_qty": result.recommended_qty,
        "audit_fields": dict(result.audit_fields),
    }


def build_gate_decision_from_policy_decision(result: PolicyDecision) -> dict[str, Any]:
    return {
        "qualified": result.decision in {"BUY", "TEST"},
        "decision": result.decision,
        "reason": result.reasons_text(),
        "reasons": list(result.reasons),
        "recommended_qty": result.recommended_qty,
        "downside_risk": result.downside_risk,
        "needs_human_review": result.needs_human_review,
        "audit_fields": dict(result.audit_fields),
    }


def export_eval_case(
    *,
    asin: str,
    keepa_metrics: KeepaMetrics | None,
    gate_decision: dict[str, Any] | None,
    fee_context: dict[str, Any] | None,
    policy_version: str,
    keepa_domain_id: int,
    marketplace_id: str,
    currency: str = "USD",
    errors: list[str] | None = None,
    evals_dir: Path | None = None,
    created_at: datetime | None = None,
) -> Path:
    asin_clean = (asin or "").strip().upper()
    if not asin_clean:
        raise ValueError("ASIN is required for eval export.")

    now = (created_at or datetime.now(timezone.utc)).replace(microsecond=0)
    out_dir = evals_dir or (Path.cwd() / "evals")
    out_dir.mkdir(parents=True, exist_ok=True)

    eval_num = _next_eval_id(out_dir)
    eval_id = f"eval_{eval_num:04d}"
    out_name = f"{eval_id}_{asin_clean}_{now.strftime('%Y%m%d_%H%M%S')}.json"
    out_path = out_dir / out_name

    payload: dict[str, Any] = {
        "eval_id": eval_id,
        "asin": asin_clean,
        "created_at_utc": now.isoformat().replace("+00:00", "Z"),
        "pipeline_context": {
            "policy_version": (policy_version or "").strip(),
            "keepa_domain_id": int(keepa_domain_id),
            "marketplace_id": (marketplace_id or "").strip() or "ATVPDKIKX0DER",
            "currency": (currency or "USD").strip() or "USD",
        },
        "gate_decision": gate_decision,
        "key_metrics": None,
        "raw_history": {"buy_box_price_history": [], "buy_box_seller_history": []},
        "fee_context": fee_context,
        "errors": list(errors or []),
        "manual_label": {
            "my_decision": None,
            "gate_correct": None,
            "my_reasoning": None,
            "gate_missed": None,
            "expected_llm_override": None,
        },
    }

    if keepa_metrics is not None:
        payload["key_metrics"] = _build_key_metrics(keepa_metrics)
        payload["raw_history"]["buy_box_price_history"] = _serialize_buy_box_price_history(
            keepa_metrics.buy_box_price_history
        )
        payload["raw_history"]["buy_box_seller_history"] = _serialize_buy_box_seller_history(
            keepa_metrics.buy_box_seller_history
        )

    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return out_path


def main() -> int:
    _load_dotenv_if_available()
    args = _parse_args()
    asin = args.asin.strip().upper()
    if not asin:
        raise SystemExit("ASIN is required.")

    keepa_key = os.getenv("KEEPA_API_KEY", "").strip()
    if not keepa_key:
        raise SystemExit("Missing KEEPA_API_KEY in environment.")
    domain_id_raw = os.getenv("KEEPA_DOMAIN_ID", "1").strip() or "1"
    try:
        domain_id = int(domain_id_raw)
    except ValueError as exc:
        raise SystemExit("KEEPA_DOMAIN_ID must be an integer.") from exc

    keepa_client = KeepaClient(api_key=keepa_key, domain_id=domain_id)

    keepa_error: str | None = None
    keepa_metrics: KeepaMetrics | None = None
    try:
        keepa_metrics = keepa_client.get_metrics(asin)
    except KeepaError as exc:
        keepa_error = str(exc)
    except Exception as exc:  # pragma: no cover - safety net
        keepa_error = str(exc)

    gate_decision: dict[str, Any] | None = None
    fee_context: dict[str, Any] | None = None
    errors: list[str] = []

    if keepa_metrics is None:
        errors.append(f"Keepa fetch failed: {keepa_error or 'unknown error'}")
    else:
        decision = evaluate_keepa_only(keepa_metrics)
        gate_decision = build_gate_decision_from_filter_result(decision)
        fee_context = _build_fee_context(
            asin=asin,
            keepa_metrics=keepa_metrics,
            landed_cost_per_unit=args.landed_cost,
        )

    out_path = export_eval_case(
        asin=asin,
        keepa_metrics=keepa_metrics,
        gate_decision=gate_decision,
        fee_context=fee_context,
        policy_version=os.getenv("POLICY_VERSION", "").strip(),
        keepa_domain_id=domain_id,
        marketplace_id=os.getenv("SP_API_MARKETPLACE_ID", "ATVPDKIKX0DER").strip()
        or "ATVPDKIKX0DER",
        errors=errors,
    )
    print(str(out_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
