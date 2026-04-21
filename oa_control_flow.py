from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import datetime, timezone

from export_eval_case import (
    build_fee_context_from_components,
    build_gate_decision_from_policy_decision,
    export_eval_case,
)
from src.keepa_client import KeepaClient
from src.llm_review import llm_decision_pipeline
from src.models import KeepaMetrics
from src.policy import evaluate_lead
from src.profitability import (
    INBOUND_SHIPPING_USD_PER_LB,
    compute_inbound_shipping_fee,
    compute_profitability_metrics,
    parse_buy_box_range_midpoint,
)
from src.seller_filter_runner import run_seller_filter
from src.sp_api_client import SPAPIClient, SPAPIError
from src.sheets_client import (
    find_first_row_to_evaluate,
    get_headers,
    open_sheet,
    read_row_as_dict,
    write_row_fields,
)


def _load_dotenv_if_available() -> None:
    try:
        from dotenv import load_dotenv
    except Exception:
        return
    load_dotenv()


def _load_config() -> dict[str, str]:
    required_vars = [
        "SHEET_ID",
        "WORKSHEET_NAME",
        "GOOGLE_SERVICE_ACCOUNT_JSON",
        "KEEPA_API_KEY",
        "POLICY_VERSION",
    ]
    config = {name: os.getenv(name, "").strip() for name in required_vars}
    config["KEEPA_DOMAIN_ID"] = os.getenv("KEEPA_DOMAIN_ID", "1").strip() or "1"
    config["FINDER_PROFILES_PATH"] = os.getenv("FINDER_PROFILES_PATH", "").strip()
    config["SP_API_LWA_CLIENT_ID"] = os.getenv("SP_API_LWA_CLIENT_ID", "").strip()
    config["SP_API_LWA_CLIENT_SECRET"] = os.getenv("SP_API_LWA_CLIENT_SECRET", "").strip()
    config["SP_API_REFRESH_TOKEN"] = os.getenv("SP_API_REFRESH_TOKEN", "").strip()
    config["SP_API_MARKETPLACE_ID"] = os.getenv("SP_API_MARKETPLACE_ID", "ATVPDKIKX0DER").strip()
    config["SP_API_ENDPOINT"] = os.getenv(
        "SP_API_ENDPOINT",
        "https://sellingpartnerapi-na.amazon.com",
    ).strip()
    config["INBOUND_SHIPPING_USD_PER_LB"] = (
        os.getenv("INBOUND_SHIPPING_USD_PER_LB", f"{INBOUND_SHIPPING_USD_PER_LB}")
        .strip()
    )
    missing = [name for name, value in config.items() if not value and name in required_vars]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
    return config


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="OA Control System runner")
    parser.add_argument(
        "--mode",
        choices=["sheet1", "seller_filter"],
        default="sheet1",
        help="sheet1 = evaluate one lead row, seller_filter = reverse source filter run",
    )
    parser.add_argument("--seller_id", default="", help="Amazon seller ID for seller_filter mode")
    parser.add_argument(
        "--limit",
        type=int,
        default=500,
        help="Max ASINs to source in seller_filter mode (max 500)",
    )
    parser.add_argument(
        "--profile",
        default="default_us",
        help="Product Finder profile name in config/finder_profiles.json",
    )
    return parser.parse_args(argv)


def _require_headers(headers: list[str], required: list[str]) -> None:
    missing = [name for name in required if name not in headers]
    if missing:
        raise ValueError(f"Missing required sheet headers: {', '.join(missing)}")


def _ensure_sheet_headers(ws, headers: list[str], to_add: list[str]) -> list[str]:
    missing = [name for name in to_add if name not in headers]
    if not missing:
        return headers
    existing = [h for h in headers if h]
    ws.update(
        range_name="A1",
        values=[existing + missing],
        value_input_option="RAW",
    )
    return existing + missing


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _timestamp_utc_iso(dt: datetime | None = None) -> str:
    now = dt or _utc_now()
    return now.isoformat().replace("+00:00", "Z")


def _fmt_int(value: int | None) -> str:
    return "" if value is None else str(int(value))


def _fmt_price(value: float | None) -> str:
    return "" if value is None else f"{value:.2f}"


def _fmt_percent(value: float | None) -> str:
    return "" if value is None else f"{value:.2f}"


def _parse_number(value: str | None) -> float | None:
    raw = (value or "").strip()
    if not raw:
        return None
    matches = re.findall(r"-?\d+(?:\.\d+)?", raw.replace(",", ""))
    if not matches:
        return None
    try:
        return float(matches[0])
    except ValueError:
        return None


def _build_sp_api_client(config: dict[str, str]) -> tuple[SPAPIClient | None, str | None]:
    client_id = config.get("SP_API_LWA_CLIENT_ID", "")
    client_secret = config.get("SP_API_LWA_CLIENT_SECRET", "")
    refresh_token = config.get("SP_API_REFRESH_TOKEN", "")
    if not client_id or not client_secret or not refresh_token:
        return None, "SP-API credentials missing"
    try:
        client = SPAPIClient(
            lwa_client_id=client_id,
            lwa_client_secret=client_secret,
            refresh_token=refresh_token,
            marketplace_id=config.get("SP_API_MARKETPLACE_ID", "ATVPDKIKX0DER"),
            endpoint=config.get("SP_API_ENDPOINT", "https://sellingpartnerapi-na.amazon.com"),
        )
    except Exception as exc:
        return None, f"SP-API client init failed: {exc}"
    return client, None


def _keepa_missing_metrics_with_error(error_message: str) -> KeepaMetrics:
    return KeepaMetrics(
        est_sales_month=None,
        offer_count_delta_14d=None,
        buy_box_90d_avg=None,
        buy_box_90d_low=None,
        amazon_buy_box_pct_90d=None,
        buy_box_stability="UNSTABLE",
        brand=None,
        buy_box_range_current=None,
        buy_box_range_21d_low=None,
        buy_box_range_21d_high=None,
        buy_box_samples_21d=0,
        buy_box_total_events_21d=0,
        buy_box_relative_spread_21d=None,
        buy_box_range_issue="buy box range unavailable due to Keepa API error",
        current_buy_box_price=None,
        competitive_sellers_near_bb=None,
        competitive_fba_sellers_near_bb=None,
        competitive_fbm_sellers_near_bb=None,
        competitive_weighted_stock_units=None,
        competitive_stock_known_sellers=None,
        competitive_stock_total_sellers=None,
        competitive_allowed_delta=None,
        competitive_ceiling_price=None,
        competitive_debug=None,
        competitive_issue="Keepa offers unavailable",
        buy_box_fba_share_90d=None,
        buy_box_fbm_share_90d=None,
        package_weight_grams=None,
        buy_box_price_history=[],
        buy_box_seller_history=[],
        missing_fields=[
            "Est Sales / Month",
            "Offer Count Delta (14d)",
            "Buy Box 90d Avg",
            "Buy Box 90d Low",
            "Amazon Buy Box % (90d)",
            "Buy Box Range (Current)",
            "Competitive Sellers Near BB",
            "Buy Box Price History",
        ],
        source_error=error_message,
    )


def _run_sheet1(config: dict[str, str]) -> int:
    ws = open_sheet(
        sheet_id=config["SHEET_ID"],
        worksheet_name=config["WORKSHEET_NAME"],
        service_account_json=config["GOOGLE_SERVICE_ACCOUNT_JSON"],
    )
    headers = get_headers(ws)
    headers = _ensure_sheet_headers(
        ws,
        headers,
        [
            "Estimated Sell Price (Mid BB)",
            "Amazon Fees Total",
            "Referral Fee",
            "FBA Fulfillment Fee",
            "Inbound Shipping Fee",
            "Profit / Unit",
            "LLM Review Output",
        ],
    )

    _require_headers(
        headers,
        [
            "LeadID",
            "ASIN",
            "EVALUATE",
            "Decision",
            "Supplier Verified",
            "Exact Match Verified",
            "Gated",
            "IP Clean",
            "Buy Box Range (Current)",
            "ROI %",
            "Margin %",
            "Apparel?",
            "Est Sales / Month",
            "Offer Count \u0394 (14d)",
            "Buy Box 90d Avg",
            "Buy Box 90d Low",
            "Amazon Buy Box % (90d)",
            "Buy Box Stability",
            "Recommended Qty",
            "Downside Risk",
            "Reasons",
            "Needs Human Review",
            "Decision Timestamp",
            "Policy Version",
        ],
    )

    row = find_first_row_to_evaluate(ws, headers)
    if row is None:
        print("No eligible row found (EVALUATE=YES and Decision blank).")
        return 0

    row_data = read_row_as_dict(ws, headers, row)
    asin = (row_data.get("ASIN") or "").strip()

    try:
        keepa_domain_id = int(config["KEEPA_DOMAIN_ID"])
    except ValueError as exc:
        raise ValueError("KEEPA_DOMAIN_ID must be an integer (US marketplace is 1).") from exc

    keepa_client = KeepaClient(
        api_key=config["KEEPA_API_KEY"],
        domain_id=keepa_domain_id,
    )
    try:
        keepa_metrics = keepa_client.get_metrics(asin)
    except Exception as exc:
        keepa_metrics = _keepa_missing_metrics_with_error(str(exc))
    sp_api_client, sp_api_client_error = _build_sp_api_client(config)

    pre_policy_updates: dict[str, str] = {
        "Buy Box Stability": keepa_metrics.buy_box_stability,
    }
    if keepa_metrics.buy_box_range_current:
        pre_policy_updates["Buy Box Range (Current)"] = keepa_metrics.buy_box_range_current

    optional_pre_policy_fields: dict[str, str] = {
        "BuyBoxRange21d_Low": _fmt_price(keepa_metrics.buy_box_range_21d_low),
        "BuyBoxRange21d_High": _fmt_price(keepa_metrics.buy_box_range_21d_high),
        "BuyBoxSamples21d": _fmt_int(keepa_metrics.buy_box_samples_21d),
        "Competitive FBA Sellers Near BB": _fmt_int(keepa_metrics.competitive_fba_sellers_near_bb),
        "Competitive FBM Sellers Near BB": _fmt_int(keepa_metrics.competitive_fbm_sellers_near_bb),
        "Competitive Weighted Stock Units": _fmt_price(keepa_metrics.competitive_weighted_stock_units),
        "Competitive Stock Known Sellers": _fmt_int(keepa_metrics.competitive_stock_known_sellers),
        "Competitive Stock Total Sellers": _fmt_int(keepa_metrics.competitive_stock_total_sellers),
        "Buy Box FBA Share % (90d)": _fmt_percent(keepa_metrics.buy_box_fba_share_90d),
        "Buy Box FBM Share % (90d)": _fmt_percent(keepa_metrics.buy_box_fbm_share_90d),
    }
    if keepa_metrics.buy_box_relative_spread_21d is not None:
        optional_pre_policy_fields["BuyBoxRange21d_SpreadPct"] = (
            f"{keepa_metrics.buy_box_relative_spread_21d * 100.0:.2f}"
        )

    for field, value in optional_pre_policy_fields.items():
        if field in headers:
            pre_policy_updates[field] = value

    competitive_field_name: str | None = None
    if "Competitive Sellers Near BB" in headers:
        competitive_field_name = "Competitive Sellers Near BB"
    elif "Competitive Sellers Near BB (Manual)" in headers:
        competitive_field_name = "Competitive Sellers Near BB (Manual)"
    if (
        competitive_field_name is not None
        and keepa_metrics.competitive_sellers_near_bb is not None
    ):
        pre_policy_updates[competitive_field_name] = _fmt_int(
            keepa_metrics.competitive_sellers_near_bb
        )
    if "Competitive Sellers Near BB Debug" in headers and keepa_metrics.competitive_debug:
        pre_policy_updates["Competitive Sellers Near BB Debug"] = keepa_metrics.competitive_debug

    write_row_fields(ws, headers, row, pre_policy_updates)

    if keepa_metrics.buy_box_range_current:
        row_data["Buy Box Range (Current)"] = keepa_metrics.buy_box_range_current
    row_data["Buy Box Stability"] = keepa_metrics.buy_box_stability
    if keepa_metrics.competitive_sellers_near_bb is not None:
        if competitive_field_name is not None:
            row_data[competitive_field_name] = _fmt_int(keepa_metrics.competitive_sellers_near_bb)
        row_data["Competitive Sellers Near BB"] = _fmt_int(keepa_metrics.competitive_sellers_near_bb)

    estimated_sell_price: float | None = parse_buy_box_range_midpoint(
        row_data.get("Buy Box Range (Current)")
    )
    landed_cost = _parse_number(
        row_data.get("Landed Cost / Unit (all-in)") or row_data.get("Landed Cost / Unit")
    )
    inbound_rate = _parse_number(config.get("INBOUND_SHIPPING_USD_PER_LB"))
    if inbound_rate is None or inbound_rate < 0:
        inbound_rate = INBOUND_SHIPPING_USD_PER_LB
    inbound_shipping_fee: float | None = None
    if keepa_metrics.package_weight_grams is not None and keepa_metrics.package_weight_grams > 0:
        inbound_shipping_fee = compute_inbound_shipping_fee(
            keepa_metrics.package_weight_grams,
            rate_per_lb=inbound_rate,
        )

    amazon_fees_total: float | None = None
    referral_fee: float | None = None
    fba_fulfillment_fee: float | None = None
    fee_breakdown: dict[str, float] = {}
    profit_per_unit: float | None = None
    roi_percent: float | None = None
    margin_percent: float | None = None

    profitability_errors: list[str] = []
    if estimated_sell_price is None:
        profitability_errors.append("Cannot parse Buy Box Range")
    if landed_cost is None or landed_cost <= 0:
        profitability_errors.append("Missing or invalid Landed Cost / Unit")
    if keepa_metrics.package_weight_grams is None or keepa_metrics.package_weight_grams <= 0:
        profitability_errors.append("Missing Keepa package weight")
    if inbound_shipping_fee is None:
        profitability_errors.append("Cannot compute inbound shipping fee")
    if sp_api_client is None:
        profitability_errors.append(sp_api_client_error or "SP-API fee estimate failed")
    elif estimated_sell_price is not None and asin:
        try:
            fees = sp_api_client.get_fba_fees_estimate(asin=asin, price=estimated_sell_price)
            amazon_fees_total = fees.total_fees
            referral_fee = fees.referral_fee
            fba_fulfillment_fee = fees.fba_fulfillment_fee
            fee_breakdown = dict(fees.breakdown)
        except SPAPIError as exc:
            profitability_errors.append(f"SP-API fee estimate failed: {exc}")

    if (
        estimated_sell_price is not None
        and landed_cost is not None
        and landed_cost > 0
        and amazon_fees_total is not None
        and inbound_shipping_fee is not None
    ):
        profit_per_unit, roi_percent, margin_percent = compute_profitability_metrics(
            estimated_sell_price=estimated_sell_price,
            landed_cost_per_unit=landed_cost,
            amazon_fees_total=amazon_fees_total,
            inbound_shipping_fee=inbound_shipping_fee,
        )

    if profitability_errors:
        row_data["ROI %"] = ""
        row_data["Margin %"] = ""
        row_data["Profitability Calc Error"] = "; ".join(profitability_errors)
    else:
        row_data["ROI %"] = _fmt_percent(roi_percent)
        row_data["Margin %"] = _fmt_percent(margin_percent)
        row_data["Profitability Calc Error"] = ""

    decision = evaluate_lead(row_data=row_data, keepa=keepa_metrics)
    _fee_context = {
        "amazon_fees_total": amazon_fees_total,
        "estimated_sell_price": estimated_sell_price,
        "landed_cost_per_unit": landed_cost,
        "inbound_shipping_fee": inbound_shipping_fee,
    }
    decision = llm_decision_pipeline(decision, keepa_metrics, row_data, _fee_context)

    updates: dict[str, str] = {
        "Est Sales / Month": _fmt_int(keepa_metrics.est_sales_month),
        "Offer Count \u0394 (14d)": _fmt_int(keepa_metrics.offer_count_delta_14d),
        "Buy Box 90d Avg": _fmt_price(keepa_metrics.buy_box_90d_avg),
        "Buy Box 90d Low": _fmt_price(keepa_metrics.buy_box_90d_low),
        "Amazon Buy Box % (90d)": _fmt_percent(keepa_metrics.amazon_buy_box_pct_90d),
        "Buy Box Stability": keepa_metrics.buy_box_stability,
        "Estimated Sell Price (Mid BB)": _fmt_price(estimated_sell_price),
        "Amazon Fees Total": _fmt_price(amazon_fees_total),
        "Referral Fee": _fmt_price(referral_fee),
        "FBA Fulfillment Fee": _fmt_price(fba_fulfillment_fee),
        "Inbound Shipping Fee": _fmt_price(inbound_shipping_fee),
        "Profit / Unit": _fmt_price(profit_per_unit),
        "ROI %": _fmt_percent(roi_percent),
        "Margin %": _fmt_percent(margin_percent),
        "Decision": decision.decision,
        "Recommended Qty": str(decision.recommended_qty),
        "Downside Risk": decision.downside_risk,
        "Reasons": decision.reasons_text(),
        "Needs Human Review": "YES" if decision.needs_human_review else "NO",
        "Decision Timestamp": _timestamp_utc_iso(),
        "Policy Version": config["POLICY_VERSION"],
    }
    if keepa_metrics.buy_box_range_current:
        updates["Buy Box Range (Current)"] = keepa_metrics.buy_box_range_current
    if (
        competitive_field_name is not None
        and keepa_metrics.competitive_sellers_near_bb is not None
    ):
        updates[competitive_field_name] = _fmt_int(keepa_metrics.competitive_sellers_near_bb)
    if "Competitive Sellers Near BB Debug" in headers and keepa_metrics.competitive_debug:
        updates["Competitive Sellers Near BB Debug"] = keepa_metrics.competitive_debug
    optional_output_fields: dict[str, str] = {
        "Competitive FBA Sellers Near BB": _fmt_int(keepa_metrics.competitive_fba_sellers_near_bb),
        "Competitive FBM Sellers Near BB": _fmt_int(keepa_metrics.competitive_fbm_sellers_near_bb),
        "Competitive Weighted Stock Units": _fmt_price(keepa_metrics.competitive_weighted_stock_units),
        "Competitive Stock Known Sellers": _fmt_int(keepa_metrics.competitive_stock_known_sellers),
        "Competitive Stock Total Sellers": _fmt_int(keepa_metrics.competitive_stock_total_sellers),
        "Buy Box FBA Share % (90d)": _fmt_percent(keepa_metrics.buy_box_fba_share_90d),
        "Buy Box FBM Share % (90d)": _fmt_percent(keepa_metrics.buy_box_fbm_share_90d),
    }
    for field, value in optional_output_fields.items():
        if field in headers:
            updates[field] = value
    for field, value in decision.audit_fields.items():
        if field in headers:
            updates[field] = value

    write_row_fields(ws, headers, row, updates)

    try:
        eval_fee_context = build_fee_context_from_components(
            estimated_sell_price=estimated_sell_price,
            buy_box_range_current=keepa_metrics.buy_box_range_current,
            amazon_fees_total=amazon_fees_total,
            referral_fee=referral_fee,
            fba_fulfillment_fee=fba_fulfillment_fee,
            fee_breakdown=fee_breakdown,
            package_weight_grams=keepa_metrics.package_weight_grams,
            inbound_shipping_rate_per_lb=inbound_rate,
            inbound_shipping_fee=inbound_shipping_fee,
            landed_cost_per_unit=landed_cost,
            profit_per_unit=profit_per_unit,
            roi_percent=roi_percent,
            margin_percent=margin_percent,
            errors=profitability_errors,
        )
        eval_errors: list[str] = []
        if keepa_metrics.source_error:
            eval_errors.append(f"Keepa API error: {keepa_metrics.source_error}")
        eval_path = export_eval_case(
            asin=asin,
            keepa_metrics=keepa_metrics,
            gate_decision=build_gate_decision_from_policy_decision(decision),
            fee_context=eval_fee_context,
            policy_version=config["POLICY_VERSION"],
            keepa_domain_id=keepa_domain_id,
            marketplace_id=config.get("SP_API_MARKETPLACE_ID", "ATVPDKIKX0DER"),
            errors=eval_errors,
        )
    except Exception as exc:
        eval_path = None
        print(f"Eval export warning: {exc}")

    lead_id = row_data.get("LeadID", "").strip() or "-"
    asin_out = asin or "-"
    print(
        f"Evaluated row={row} LeadID={lead_id} ASIN={asin_out} "
        f"Decision={decision.decision} Risk={decision.downside_risk}"
    )
    print(f"Reasoning: {decision.reasons_text()}")
    thought_fields = [
        "Addressable FBA Units / Mo",
        "Effective Competitor Count",
        "Base Qty Confidence",
        "Stock Pressure Multiplier",
        "Qty Confidence Multiplier",
        "Competitive Stock Days Ahead",
        "Entrant Units Est / Mo",
    ]
    thought_parts: list[str] = []
    for field in thought_fields:
        value = decision.audit_fields.get(field)
        if value:
            thought_parts.append(f"{field}={value}")
    if thought_parts:
        print("Model trace: " + " | ".join(thought_parts))
    if eval_path is not None:
        print(f"Eval case saved: {eval_path}")
    return 0


def main(argv: list[str] | None = None) -> int:
    _load_dotenv_if_available()
    args = _parse_args(argv if argv is not None else sys.argv[1:])
    config = _load_config()

    if args.mode == "seller_filter":
        return run_seller_filter(
            config=config,
            seller_id=args.seller_id,
            limit=min(args.limit, 500),
            profile=args.profile,
        )
    return _run_sheet1(config)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ValueError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        raise SystemExit(1)
    except Exception as exc:
        print(f"Runtime error: {exc}", file=sys.stderr)
        raise SystemExit(1)
