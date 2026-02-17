from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

from src.keepa_client import KeepaClient
from src.models import KeepaMetrics
from src.policy import evaluate_lead
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
    missing = [name for name, value in config.items() if not value]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
    return config


def _require_headers(headers: list[str], required: list[str]) -> None:
    missing = [name for name in required if name not in headers]
    if missing:
        raise ValueError(f"Missing required sheet headers: {', '.join(missing)}")


def _timestamp_utc_iso() -> str:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    return now.isoformat().replace("+00:00", "Z")


def _fmt_int(value: int | None) -> str:
    return "" if value is None else str(int(value))


def _fmt_price(value: float | None) -> str:
    return "" if value is None else f"{value:.2f}"


def _fmt_percent(value: float | None) -> str:
    return "" if value is None else f"{value:.2f}"


def _keepa_missing_metrics_with_error(error_message: str) -> KeepaMetrics:
    return KeepaMetrics(
        est_sales_month=None,
        offer_count_delta_14d=None,
        buy_box_90d_avg=None,
        buy_box_90d_low=None,
        amazon_buy_box_pct_90d=None,
        buy_box_stability="UNSTABLE",
        buy_box_range_current=None,
        buy_box_range_21d_low=None,
        buy_box_range_21d_high=None,
        buy_box_samples_21d=0,
        buy_box_total_events_21d=0,
        buy_box_relative_spread_21d=None,
        buy_box_range_issue="buy box range unavailable due to Keepa API error",
        current_buy_box_price=None,
        competitive_sellers_near_bb=None,
        competitive_allowed_delta=None,
        competitive_ceiling_price=None,
        competitive_debug=None,
        competitive_issue="Keepa offers unavailable",
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


def main() -> int:
    _load_dotenv_if_available()

    config = _load_config()

    ws = open_sheet(
        sheet_id=config["SHEET_ID"],
        worksheet_name=config["WORKSHEET_NAME"],
        service_account_json=config["GOOGLE_SERVICE_ACCOUNT_JSON"],
    )
    headers = get_headers(ws)

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

    pre_policy_updates: dict[str, str] = {
        "Buy Box Stability": keepa_metrics.buy_box_stability,
    }
    if keepa_metrics.buy_box_range_current:
        pre_policy_updates["Buy Box Range (Current)"] = keepa_metrics.buy_box_range_current

    optional_pre_policy_fields: dict[str, str] = {
        "BuyBoxRange21d_Low": _fmt_price(keepa_metrics.buy_box_range_21d_low),
        "BuyBoxRange21d_High": _fmt_price(keepa_metrics.buy_box_range_21d_high),
        "BuyBoxSamples21d": _fmt_int(keepa_metrics.buy_box_samples_21d),
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

    decision = evaluate_lead(row_data=row_data, keepa=keepa_metrics)

    updates: dict[str, str] = {
        "Est Sales / Month": _fmt_int(keepa_metrics.est_sales_month),
        "Offer Count \u0394 (14d)": _fmt_int(keepa_metrics.offer_count_delta_14d),
        "Buy Box 90d Avg": _fmt_price(keepa_metrics.buy_box_90d_avg),
        "Buy Box 90d Low": _fmt_price(keepa_metrics.buy_box_90d_low),
        "Amazon Buy Box % (90d)": _fmt_percent(keepa_metrics.amazon_buy_box_pct_90d),
        "Buy Box Stability": keepa_metrics.buy_box_stability,
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
    for field, value in decision.audit_fields.items():
        if field in headers:
            updates[field] = value

    write_row_fields(ws, headers, row, updates)

    lead_id = row_data.get("LeadID", "").strip() or "-"
    asin_out = asin or "-"
    print(
        f"Evaluated row={row} LeadID={lead_id} ASIN={asin_out} "
        f"Decision={decision.decision} Risk={decision.downside_risk}"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ValueError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        raise SystemExit(1)
    except Exception as exc:
        print(f"Runtime error: {exc}", file=sys.stderr)
        raise SystemExit(1)
