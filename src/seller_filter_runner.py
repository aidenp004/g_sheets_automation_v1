from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from src.keepa_client import KeepaClient, KeepaRateLimitError
from src.models import KeepaMetrics
from src.policy import evaluate_keepa_only
from src.sheets_client import (
    append_row_by_headers,
    get_headers,
    get_or_create_worksheet,
    open_spreadsheet,
)

FINDER_QUALIFIED_HEADERS = [
    "Date Pulled",
    "Run ID",
    "Policy Version",
    "Seller ID",
    "Brand",
    "ASIN",
    "Amazon Link",
    "Buy Box Range (Current)",
    "Est Sales / Month",
    "Competitive Sellers Near BB",
    "Competitive FBA Sellers Near BB",
    "Competitive FBM Sellers Near BB",
    "Competitive Weighted Stock Units",
    "Competitive Stock Known Sellers",
    "Competitive Stock Total Sellers",
    "Offer Count \u0394 (14d)",
    "Buy Box 90d Avg",
    "Buy Box 90d Low",
    "Amazon Buy Box % (90d)",
    "Buy Box FBA Share % (90d)",
    "Buy Box FBM Share % (90d)",
    "Buy Box Stability",
    "Decision",
    "Recommended Qty",
    "Addressable FBA Units / Mo",
    "Effective Competitor Count",
    "Base Qty Confidence",
    "Stock Pressure Multiplier",
    "Qty Confidence Multiplier",
    "Competitive Stock Days Ahead",
    "Stock Signal Reliability",
    "Entrant Units Est / Mo",
    "Reasons",
]

FINDER_REJECT_HEADERS = [
    "Date Pulled",
    "Run ID",
    "Policy Version",
    "Seller ID",
    "Brand",
    "ASIN",
    "Decision",
    "Reject Reason",
    "Est Sales / Month",
    "Amazon Buy Box % (90d)",
]

DEFAULT_FINDER_PROFILE_PATH = Path(__file__).resolve().parent.parent / "config" / "finder_profiles.json"


def run_seller_filter(
    config: dict[str, str],
    seller_id: str,
    limit: int,
    profile: str,
) -> int:
    seller_clean = _normalized_upper(seller_id)
    if not seller_clean:
        raise ValueError("--seller_id is required for --mode seller_filter")
    if limit <= 0:
        raise ValueError("--limit must be > 0")

    try:
        keepa_domain_id = int(config["KEEPA_DOMAIN_ID"])
    except ValueError as exc:
        raise ValueError("KEEPA_DOMAIN_ID must be an integer (US marketplace is 1).") from exc

    profile_path = _resolve_profiles_path(config.get("FINDER_PROFILES_PATH", ""))
    profiles = _load_finder_profiles(profile_path)
    profile_name = (profile or "").strip() or "default_us"
    if profile_name not in profiles:
        available = ", ".join(sorted(profiles.keys())) or "(none)"
        raise ValueError(
            f"Unknown finder profile '{profile_name}'. Available profiles: {available}"
        )
    profile_cfg = profiles[profile_name]

    selection_template = profile_cfg.get("selection_template")
    if not isinstance(selection_template, dict) or not selection_template:
        raise ValueError(
            f"Finder profile '{profile_name}' has invalid selection_template (must be object)."
        )

    max_pages = _positive_int(profile_cfg.get("max_pages"), "max_pages")
    candidate_limit = _positive_int(profile_cfg.get("candidate_limit"), "candidate_limit")
    detail_limit = _positive_int(profile_cfg.get("detail_limit"), "detail_limit")
    cooldown_days = _positive_int(profile_cfg.get("cooldown_days"), "cooldown_days")

    selection = _inject_placeholders(selection_template, seller_clean)
    if not isinstance(selection, dict):
        raise ValueError(
            f"Finder profile '{profile_name}' produced invalid selection payload."
        )
    per_page = _coerce_int(selection.get("perPage")) or 10000
    selection["perPage"] = max(1, min(10000, per_page))
    selection.pop("page", None)

    keepa_client = KeepaClient(
        api_key=config["KEEPA_API_KEY"],
        domain_id=keepa_domain_id,
    )

    spreadsheet = open_spreadsheet(
        sheet_id=config["SHEET_ID"],
        service_account_json=config["GOOGLE_SERVICE_ACCOUNT_JSON"],
    )
    qualified_ws = get_or_create_worksheet(
        spreadsheet, "FinderQualified", FINDER_QUALIFIED_HEADERS
    )
    rejects_ws = get_or_create_worksheet(
        spreadsheet, "FinderRejects", FINDER_REJECT_HEADERS
    )

    now_utc = _utc_now()
    date_pulled = _timestamp_utc_iso(now_utc)
    run_id = f"{seller_clean}_{now_utc.strftime('%Y%m%d_%H%M%S')}"

    source_limit = min(limit, candidate_limit)
    try:
        candidate_asins = keepa_client.get_product_finder_asins(
            selection=selection,
            max_pages=max_pages,
            candidate_limit=source_limit,
        )
    except Exception as exc:
        print(f"Product Finder candidate fetch failed: {exc}", file=sys.stderr)
        return 1

    if not candidate_asins:
        print(
            f"Seller filter run_id={run_id} seller={seller_clean} profile={profile_name} "
            "sourced=0"
        )
        return 0

    cooldown_asins = _recent_asin_cooldown_set(
        qualified_ws=qualified_ws,
        rejects_ws=rejects_ws,
        seller_id=seller_clean,
        now_utc=now_utc,
        cooldown_days=cooldown_days,
    )

    stage2_candidates = [asin for asin in candidate_asins if asin not in cooldown_asins]
    skipped_cooldown = len(candidate_asins) - len(stage2_candidates)
    evaluate_cap = min(detail_limit, len(stage2_candidates))
    evaluate_asins = stage2_candidates[:evaluate_cap]
    skipped_detail_cap = len(stage2_candidates) - len(evaluate_asins)

    processed = 0
    qualified_count = 0
    reject_count = 0
    rate_limit_waits = 0

    for asin in evaluate_asins:
        keepa_metrics: KeepaMetrics | None = None
        filter_result = None
        rate_attempt = 0
        while True:
            try:
                keepa_metrics = keepa_client.get_metrics(asin)
                filter_result = evaluate_keepa_only(keepa_metrics)
                break
            except KeepaRateLimitError as exc:
                rate_attempt += 1
                rate_limit_waits += 1
                wait_seconds = _rate_limit_wait_seconds(exc.refill_in_ms, rate_attempt)
                print(
                    f"[{asin}] Keepa rate limit hit; waiting {wait_seconds:.1f}s "
                    f"(attempt {rate_attempt}, refillInMs={exc.refill_in_ms}, tokensLeft={exc.tokens_left})"
                )
                time.sleep(wait_seconds)
                continue
            except Exception as exc:
                print(f"[{asin}] Keepa fetch failed -> DEFER. reason={exc}")
                reject_row = _build_reject_row(
                    date_pulled=date_pulled,
                    run_id=run_id,
                    policy_version=config["POLICY_VERSION"],
                    seller_id=seller_clean,
                    asin=asin,
                    metrics=None,
                    decision="DEFER",
                    reject_reason=f"Keepa error: {exc}",
                )
                append_row_by_headers(rejects_ws, reject_row)
                reject_count += 1
                processed += 1
                keepa_metrics = None
                break

        if keepa_metrics is None or filter_result is None:
            continue

        if filter_result.qualified:
            print(
                f"[{asin}] decision={filter_result.decision} qty={_fmt_int(filter_result.recommended_qty)} "
                f"{_format_model_trace(filter_result.audit_fields)} reason={_short_text(filter_result.reason)}"
            )
            qualified_row = _build_qualified_row(
                date_pulled=date_pulled,
                run_id=run_id,
                policy_version=config["POLICY_VERSION"],
                seller_id=seller_clean,
                asin=asin,
                metrics=keepa_metrics,
                decision=filter_result.decision,
                recommended_qty=filter_result.recommended_qty,
                reason=filter_result.reason,
                audit_fields=filter_result.audit_fields,
            )
            append_row_by_headers(qualified_ws, qualified_row)
            qualified_count += 1
        else:
            print(
                f"[{asin}] decision={filter_result.decision} "
                f"{_format_model_trace(filter_result.audit_fields)} reason={_short_text(filter_result.reason)}"
            )
            reject_row = _build_reject_row(
                date_pulled=date_pulled,
                run_id=run_id,
                policy_version=config["POLICY_VERSION"],
                seller_id=seller_clean,
                asin=asin,
                metrics=keepa_metrics,
                decision=filter_result.decision,
                reject_reason=filter_result.reason,
            )
            append_row_by_headers(rejects_ws, reject_row)
            reject_count += 1
        processed += 1

    print(
        f"Seller filter run_id={run_id} seller={seller_clean} profile={profile_name} "
        f"sourced={len(candidate_asins)} skipped_cooldown={skipped_cooldown} "
        f"skipped_detail_cap={skipped_detail_cap} processed={processed} "
        f"qualified={qualified_count} rejects={reject_count} "
        f"rate_limit_waits={rate_limit_waits}"
    )
    return 0


def _resolve_profiles_path(config_path: str) -> Path:
    raw = (config_path or "").strip()
    if raw:
        return Path(raw).expanduser()
    return DEFAULT_FINDER_PROFILE_PATH


def _load_finder_profiles(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        raise ValueError(f"Finder profiles file not found: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Finder profiles JSON parse error in {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError("Finder profiles file must be a JSON object.")

    profiles: dict[str, dict[str, Any]] = {}
    for name, config_value in payload.items():
        if not isinstance(name, str):
            continue
        if not isinstance(config_value, dict):
            continue
        profiles[name] = config_value
    if not profiles:
        raise ValueError("Finder profiles file has no valid named profiles.")
    return profiles


def _inject_placeholders(value: Any, seller_id: str) -> Any:
    if isinstance(value, dict):
        return {k: _inject_placeholders(v, seller_id) for k, v in value.items()}
    if isinstance(value, list):
        return [_inject_placeholders(v, seller_id) for v in value]
    if isinstance(value, str):
        return value.replace("{{seller_id}}", seller_id)
    return value


def _positive_int(value: Any, field_name: str) -> int:
    parsed = _coerce_int(value)
    if parsed is None or parsed <= 0:
        raise ValueError(f"Invalid finder profile value for {field_name}: {value!r}")
    return parsed


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _timestamp_utc_iso(dt: datetime | None = None) -> str:
    now = dt or _utc_now()
    return now.isoformat().replace("+00:00", "Z")


def _parse_iso_datetime(value: str) -> datetime | None:
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _normalized_upper(value: str) -> str:
    return (value or "").strip().upper()


def _recent_asin_cooldown_set(
    qualified_ws,
    rejects_ws,
    seller_id: str,
    now_utc: datetime,
    cooldown_days: int,
) -> set[str]:
    cutoff = now_utc - timedelta(days=cooldown_days)
    seller_norm = _normalized_upper(seller_id)
    recent: set[str] = set()

    for ws in (qualified_ws, rejects_ws):
        headers = get_headers(ws)
        index_map = {header: idx for idx, header in enumerate(headers)}
        required = ["Date Pulled", "Seller ID", "ASIN"]
        if any(name not in index_map for name in required):
            continue
        rows = ws.get_all_values()
        for row in rows[1:]:
            seller = row[index_map["Seller ID"]].strip() if index_map["Seller ID"] < len(row) else ""
            asin = row[index_map["ASIN"]].strip().upper() if index_map["ASIN"] < len(row) else ""
            date_text = row[index_map["Date Pulled"]].strip() if index_map["Date Pulled"] < len(row) else ""
            if not seller or not asin:
                continue
            if _normalized_upper(seller) != seller_norm:
                continue
            dt = _parse_iso_datetime(date_text)
            if dt is None:
                continue
            if dt >= cutoff:
                recent.add(asin)

    return recent


def _rate_limit_wait_seconds(refill_in_ms: int | None, attempt: int) -> float:
    if refill_in_ms is not None and refill_in_ms >= 0:
        return min(180.0, (refill_in_ms / 1000.0) + 1.0)
    return min(60.0, max(2.0, attempt * 4.0))


def _fmt_int(value: int | None) -> str:
    return "" if value is None else str(int(value))


def _fmt_price(value: float | None) -> str:
    return "" if value is None else f"{value:.2f}"


def _fmt_percent(value: float | None) -> str:
    return "" if value is None else f"{value:.2f}"


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _short_text(value: str, max_len: int = 220) -> str:
    raw = (value or "").strip()
    if len(raw) <= max_len:
        return raw
    return raw[: max_len - 3] + "..."


def _format_model_trace(audit_fields: dict[str, str]) -> str:
    if not audit_fields:
        return ""
    keys = [
        "Addressable FBA Units / Mo",
        "Effective Competitor Count",
        "Base Qty Confidence",
        "Stock Pressure Multiplier",
        "Qty Confidence Multiplier",
        "Competitive Stock Days Ahead",
        "Entrant Units Est / Mo",
    ]
    parts: list[str] = []
    for key in keys:
        value = (audit_fields.get(key) or "").strip()
        if value:
            parts.append(f"{key}={value}")
    if not parts:
        return ""
    return "trace: " + " | ".join(parts)


def _build_qualified_row(
    date_pulled: str,
    run_id: str,
    policy_version: str,
    seller_id: str,
    asin: str,
    metrics: KeepaMetrics,
    decision: str,
    recommended_qty: int | None,
    reason: str,
    audit_fields: dict[str, str] | None = None,
) -> dict[str, str]:
    row = {
        "Date Pulled": date_pulled,
        "Run ID": run_id,
        "Policy Version": policy_version,
        "Seller ID": seller_id,
        "Brand": metrics.brand or "",
        "ASIN": asin,
        "Amazon Link": f"https://www.amazon.com/dp/{asin}",
        "Buy Box Range (Current)": metrics.buy_box_range_current or "",
        "Est Sales / Month": _fmt_int(metrics.est_sales_month),
        "Competitive Sellers Near BB": _fmt_int(metrics.competitive_sellers_near_bb),
        "Competitive FBA Sellers Near BB": _fmt_int(metrics.competitive_fba_sellers_near_bb),
        "Competitive FBM Sellers Near BB": _fmt_int(metrics.competitive_fbm_sellers_near_bb),
        "Competitive Weighted Stock Units": _fmt_price(metrics.competitive_weighted_stock_units),
        "Competitive Stock Known Sellers": _fmt_int(metrics.competitive_stock_known_sellers),
        "Competitive Stock Total Sellers": _fmt_int(metrics.competitive_stock_total_sellers),
        "Offer Count \u0394 (14d)": _fmt_int(metrics.offer_count_delta_14d),
        "Buy Box 90d Avg": _fmt_price(metrics.buy_box_90d_avg),
        "Buy Box 90d Low": _fmt_price(metrics.buy_box_90d_low),
        "Amazon Buy Box % (90d)": _fmt_percent(metrics.amazon_buy_box_pct_90d),
        "Buy Box FBA Share % (90d)": _fmt_percent(metrics.buy_box_fba_share_90d),
        "Buy Box FBM Share % (90d)": _fmt_percent(metrics.buy_box_fbm_share_90d),
        "Buy Box Stability": metrics.buy_box_stability,
        "Decision": decision,
        "Recommended Qty": _fmt_int(recommended_qty),
        "Addressable FBA Units / Mo": "",
        "Effective Competitor Count": "",
        "Base Qty Confidence": "",
        "Stock Pressure Multiplier": "",
        "Qty Confidence Multiplier": "",
        "Competitive Stock Days Ahead": "",
        "Stock Signal Reliability": "",
        "Entrant Units Est / Mo": "",
        "Reasons": reason,
    }
    if audit_fields:
        for field in (
            "Addressable FBA Units / Mo",
            "Effective Competitor Count",
            "Base Qty Confidence",
            "Stock Pressure Multiplier",
            "Qty Confidence Multiplier",
            "Competitive Stock Days Ahead",
            "Stock Signal Reliability",
            "Entrant Units Est / Mo",
        ):
            if field in audit_fields:
                row[field] = str(audit_fields[field])
    return row


def _build_reject_row(
    date_pulled: str,
    run_id: str,
    policy_version: str,
    seller_id: str,
    asin: str,
    metrics: KeepaMetrics | None,
    decision: str,
    reject_reason: str,
) -> dict[str, str]:
    return {
        "Date Pulled": date_pulled,
        "Run ID": run_id,
        "Policy Version": policy_version,
        "Seller ID": seller_id,
        "Brand": metrics.brand if metrics and metrics.brand else "",
        "ASIN": asin,
        "Decision": decision,
        "Reject Reason": reject_reason,
        "Est Sales / Month": _fmt_int(metrics.est_sales_month if metrics else None),
        "Amazon Buy Box % (90d)": _fmt_percent(metrics.amazon_buy_box_pct_90d if metrics else None),
    }
