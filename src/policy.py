from __future__ import annotations

import math
import re

from src.models import FilterResult, KeepaMetrics, PolicyDecision

TEST_QTY_DEFAULT = 8
TEST_QTY_HIGH_RISK = 6
SPIKE_TEST_QTY_HIGH_VOLUME = 10
BUY_QTY_CAP = 50
FBM_COMPETITOR_WEIGHT = 0.35
NEW_ENTRANT_PENALTY = 1.5
BASE_CONFIDENCE_HAIRCUT = 0.75
UNSTABLE_CONFIDENCE_HAIRCUT = 0.85
HIGH_CHURN_CONFIDENCE_HAIRCUT = 0.85
MED_CHURN_CONFIDENCE_HAIRCUT = 0.92
HIGH_SPREAD_CONFIDENCE_HAIRCUT = 0.85
MED_SPREAD_CONFIDENCE_HAIRCUT = 0.92
STOCK_MISSING_CONFIDENCE_HAIRCUT = 0.92
STOCK_LOW_COVERAGE_HAIRCUT = 0.96
STOCK_MED_COVERAGE_HAIRCUT = 0.90
STOCK_HIGH_COVERAGE_HAIRCUT = 0.82
STOCK_EXTREME_COVERAGE_HAIRCUT = 0.72
STOCK_LOW_PRESSURE_BONUS = 1.05
STOCK_MODERATE_PRESSURE_BONUS = 1.02

SPIKE_PERCENTILE = 85
SPIKE_MIN_WINDOW_MINUTES = 6 * 60
SPIKE_HAIRCUT = 0.5
SPIKE_MIN_SALES_MONTH = 50
SPIKE_MIN_SHARE_PERCENT = 10.0
SPIKE_MIN_EFFECTIVE_UNITS_MONTH = 10.0


def evaluate_lead(row_data: dict[str, str], keepa: KeepaMetrics) -> PolicyDecision:
    reasons: list[str] = []
    needs_human_review = False
    hard_reject = False

    exact_match = _normalize_enum(row_data.get("Exact Match Verified"))
    gated = _normalize_enum(row_data.get("Gated"))
    ip_clean = _normalize_enum(row_data.get("IP Clean"))
    supplier_verified = _normalize_enum(row_data.get("Supplier Verified"))
    apparel = _normalize_enum(row_data.get("Apparel?")) == "YES"

    roi = _parse_number(row_data.get("ROI %"))
    margin = _parse_number(row_data.get("Margin %"))
    profitability_calc_error = (row_data.get("Profitability Calc Error") or "").strip()

    min_roi = 30.0 if apparel else 20.0
    min_margin = 15.0 if apparel else 12.0

    if exact_match == "NO":
        hard_reject = True
        reasons.append("Exact Match Verified is NO.")
    elif exact_match != "YES":
        needs_human_review = True
        reasons.append("Exact Match Verified is not YES.")

    if gated == "YES":
        hard_reject = True
        reasons.append("ASIN is gated.")
    elif gated in {"UNKNOWN", ""}:
        needs_human_review = True
        reasons.append("Gated status is UNKNOWN or blank.")

    if ip_clean == "NO":
        hard_reject = True
        reasons.append("IP Clean is NO.")
    elif ip_clean in {"UNKNOWN", ""}:
        needs_human_review = True
        reasons.append("IP Clean is UNKNOWN or blank.")

    if supplier_verified != "YES":
        needs_human_review = True
        reasons.append("Supplier Verified is not YES.")

    if profitability_calc_error:
        needs_human_review = True
        reasons.append(profitability_calc_error)

    if roi is None:
        needs_human_review = True
        reasons.append("ROI % is missing or invalid.")
    if margin is None:
        needs_human_review = True
        reasons.append("Margin % is missing or invalid.")

    if keepa.source_error:
        needs_human_review = True
        reasons.append(f"Keepa API error: {keepa.source_error}")

    if keepa.missing_fields:
        needs_human_review = True
        missing_joined = ", ".join(keepa.missing_fields)
        reasons.append(f"Keepa fields missing: {missing_joined}.")
    if keepa.buy_box_range_issue:
        needs_human_review = True
        reasons.append(f"Buy Box range issue: {keepa.buy_box_range_issue}.")
    if keepa.competitive_issue:
        needs_human_review = True
        reasons.append(f"Competitive sellers signal issue: {keepa.competitive_issue}.")

    if keepa.est_sales_month is not None and keepa.est_sales_month < 30:
        hard_reject = True
        reasons.append(f"Est Sales / Month ({keepa.est_sales_month}) is below 30.")

    if keepa.amazon_buy_box_pct_90d is not None and keepa.amazon_buy_box_pct_90d > 50:
        hard_reject = True
        reasons.append(
            f"Amazon Buy Box % (90d) is {keepa.amazon_buy_box_pct_90d:.2f}% (>50%)."
        )

    offer_spike = keepa.offer_count_delta_14d is not None and keepa.offer_count_delta_14d > 10
    if offer_spike:
        needs_human_review = True
        reasons.append(
            f"Offer Count Delta (14d) is +{keepa.offer_count_delta_14d} (spike > +10)."
        )

    price_floor_breach = False
    if keepa.buy_box_90d_low is not None and roi is not None:
        break_even_floor = _estimate_break_even_floor(
            row_data=row_data,
            roi_percent=roi,
        )
        if break_even_floor is None:
            needs_human_review = True
            if keepa.buy_box_range_issue:
                reasons.append("Break-even floor unavailable because Buy Box range is unavailable.")
            else:
                reasons.append("Could not derive break-even floor from Buy Box Range (Current).")
        elif keepa.buy_box_90d_low < break_even_floor:
            price_floor_breach = True
            needs_human_review = True
            reasons.append(
                "Buy Box 90d Low is below estimated break-even floor (price downside risk)."
            )

    downside_risk = _classify_risk(
        keepa=keepa,
        offer_spike=offer_spike,
        price_floor_breach=price_floor_breach,
    )

    base_profitability_fail_reasons: list[str] = []
    if roi is not None and roi < min_roi:
        base_profitability_fail_reasons.append(
            f"ROI % ({roi:.2f}) is below threshold {min_roi:.2f}."
        )
    if margin is not None and margin < min_margin:
        base_profitability_fail_reasons.append(
            f"Margin % ({margin:.2f}) is below threshold {min_margin:.2f}."
        )
    base_profitability_fails = bool(base_profitability_fail_reasons)

    spike_eval = _evaluate_spike_path(
        row_data=row_data,
        keepa=keepa,
        min_roi=min_roi,
        min_margin=min_margin,
    )

    if hard_reject:
        return PolicyDecision(
            decision="REJECT",
            recommended_qty=0,
            downside_risk="HIGH",
            needs_human_review=needs_human_review,
            reasons=_dedupe_reasons(reasons + base_profitability_fail_reasons),
            audit_fields=spike_eval["audit_fields"],
        )

    if needs_human_review:
        return PolicyDecision(
            decision="DEFER",
            recommended_qty=0,
            downside_risk=downside_risk,
            needs_human_review=True,
            reasons=_dedupe_reasons(reasons + base_profitability_fail_reasons),
            audit_fields=spike_eval["audit_fields"],
        )

    if base_profitability_fails:
        if spike_eval["qualifies"]:
            sales_month = keepa.est_sales_month or 0
            test_qty = SPIKE_TEST_QTY_HIGH_VOLUME if sales_month >= 200 else TEST_QTY_DEFAULT
            spike_reason = (
                "Base price fails criteria; spike regime qualifies "
                f"(SpikeShare {spike_eval['spike_share_percent']:.2f}%, "
                f"SalesPerMonth {sales_month}). TEST recommended."
            )
            return PolicyDecision(
                decision="TEST",
                recommended_qty=test_qty,
                downside_risk=downside_risk,
                needs_human_review=False,
                reasons=_dedupe_reasons(base_profitability_fail_reasons + [spike_reason]),
                audit_fields=spike_eval["audit_fields"],
            )

        return PolicyDecision(
            decision="REJECT",
            recommended_qty=0,
            downside_risk="HIGH",
            needs_human_review=False,
            reasons=_dedupe_reasons(base_profitability_fail_reasons + spike_eval["reasons"]),
            audit_fields=spike_eval["audit_fields"],
        )

    if downside_risk == "LOW":
        buy_qty, qty_summary, qty_audit = compute_recommended_qty("BUY", keepa, row_data, downside_risk)
        if buy_qty == 0:
            return PolicyDecision(
                decision="DEFER",
                recommended_qty=0,
                downside_risk="MED",
                needs_human_review=True,
                reasons=[qty_summary],
                audit_fields=spike_eval["audit_fields"],
            )
        combined_audit = _merge_audit_fields(spike_eval["audit_fields"], qty_audit)
        return PolicyDecision(
            decision="BUY",
            recommended_qty=buy_qty,
            downside_risk="LOW",
            needs_human_review=False,
            reasons=[f"All deterministic gates passed. {qty_summary}"],
            audit_fields=combined_audit,
        )

    test_qty, _, _ = compute_recommended_qty("TEST", keepa, row_data, downside_risk)
    return PolicyDecision(
        decision="TEST",
        recommended_qty=test_qty,
        downside_risk=downside_risk,
        needs_human_review=False,
        reasons=_dedupe_reasons(reasons) or ["Risk not low; assigned TEST quantity."],
        audit_fields=spike_eval["audit_fields"],
    )


def compute_recommended_qty(
    decision: str,
    keepa: KeepaMetrics,
    row_data: dict,
    downside_risk: str,
) -> tuple[int, str, dict[str, str]]:
    """
    Compute recommended purchase quantity, a reasons-field summary, and qty-model
    audit fields for the given decision outcome.

    Returns:
        (qty, summary, audit_fields)
        - qty is 0 for DEFER/REJECT or when the BUY qty model cannot produce an estimate.
        - summary is non-empty only when qty > 0 or when BUY model fails (error message).
        - audit_fields is non-empty only for a successful BUY calculation.

    The audit_fields element is used internally by evaluate_lead() to build
    combined_audit; callers that only need qty + summary may ignore it with ``_``.
    """
    if decision == "BUY":
        competitors_near_bb = _parse_number(
            row_data.get("Competitive Sellers Near BB")
            or row_data.get("Competitive Sellers Near BB (Manual)")
        )
        qty_estimate, qty_issue = _estimate_buy_qty_for_new_fba_entrant(
            keepa=keepa,
            fallback_competitors=competitors_near_bb,
        )
        if qty_estimate is None or qty_issue is not None:
            return (
                0,
                f"Cannot compute BUY qty: {qty_issue or 'quantity model unavailable'}.",
                {},
            )
        return (
            int(qty_estimate["recommended_qty"]),
            str(qty_estimate["summary"]),
            dict(qty_estimate["audit_fields"]),
        )

    if decision == "TEST":
        if downside_risk == "HIGH":
            return TEST_QTY_HIGH_RISK, f"TEST quantity {TEST_QTY_HIGH_RISK} (high downside risk).", {}
        return TEST_QTY_DEFAULT, f"TEST quantity {TEST_QTY_DEFAULT} (standard risk).", {}

    return 0, "", {}


def evaluate_keepa_only(keepa: KeepaMetrics) -> FilterResult:
    if keepa.source_error:
        return FilterResult(
            qualified=False,
            decision="DEFER",
            reason=f"Keepa error: {keepa.source_error}",
        )
    if keepa.buy_box_range_issue:
        return FilterResult(
            qualified=False,
            decision="DEFER",
            reason=f"Buy Box range issue: {keepa.buy_box_range_issue}",
        )
    if keepa.competitive_issue:
        return FilterResult(
            qualified=False,
            decision="DEFER",
            reason=f"Competitive sellers issue: {keepa.competitive_issue}",
        )
    if keepa.est_sales_month is None:
        return FilterResult(
            qualified=False,
            decision="DEFER",
            reason="Missing Est Sales / Month",
        )
    if keepa.amazon_buy_box_pct_90d is None:
        return FilterResult(
            qualified=False,
            decision="DEFER",
            reason="Missing Amazon Buy Box % (90d)",
        )
    if keepa.offer_count_delta_14d is None:
        return FilterResult(
            qualified=False,
            decision="DEFER",
            reason="Missing Offer Count Delta (14d)",
        )
    has_competition_pool = any(
        value is not None and value >= 0
        for value in (
            keepa.competitive_sellers_near_bb,
            keepa.competitive_fba_sellers_near_bb,
            keepa.competitive_fbm_sellers_near_bb,
        )
    )
    if not has_competition_pool:
        return FilterResult(
            qualified=False,
            decision="DEFER",
            reason="Missing competitive seller pool",
        )
    if keepa.buy_box_fba_share_90d is None and keepa.buy_box_fbm_share_90d is None:
        return FilterResult(
            qualified=False,
            decision="DEFER",
            reason="Missing buy box fulfillment share (90d)",
        )

    if keepa.est_sales_month < 30:
        return FilterResult(
            qualified=False,
            decision="REJECT",
            reason=f"Low demand: Est Sales / Month {keepa.est_sales_month} < 30",
        )
    if keepa.amazon_buy_box_pct_90d > 50:
        return FilterResult(
            qualified=False,
            decision="REJECT",
            reason=f"Amazon Buy Box % too high: {keepa.amazon_buy_box_pct_90d:.2f}%",
        )
    if keepa.offer_count_delta_14d > 10:
        return FilterResult(
            qualified=False,
            decision="REJECT",
            reason=f"Offer spike: Delta +{keepa.offer_count_delta_14d} > +10",
        )

    risk_score = 0
    if keepa.buy_box_stability == "UNSTABLE":
        risk_score += 1
    if keepa.amazon_buy_box_pct_90d >= 35:
        risk_score += 1

    if risk_score >= 2:
        return FilterResult(
            qualified=True,
            decision="TEST",
            reason="Qualified Keepa-only (elevated risk; test recommended)",
            recommended_qty=TEST_QTY_HIGH_RISK,
        )
    if risk_score == 1:
        return FilterResult(
            qualified=True,
            decision="TEST",
            reason="Qualified Keepa-only (moderate risk; test recommended)",
            recommended_qty=TEST_QTY_DEFAULT,
        )

    qty_estimate, qty_issue = _estimate_buy_qty_for_new_fba_entrant(
        keepa=keepa,
        fallback_competitors=None,
    )
    if qty_estimate is None or qty_issue is not None:
        return FilterResult(
            qualified=False,
            decision="DEFER",
            reason=f"Quantity model unavailable: {qty_issue or 'missing signals'}",
            audit_fields=(qty_estimate or {}).get("audit_fields", {}),
        )
    recommended_qty = int(qty_estimate["recommended_qty"])
    return FilterResult(
        qualified=True,
        decision="BUY",
        reason=f"Qualified Keepa-only (all Keepa gates passed). {qty_estimate['summary']}",
        recommended_qty=recommended_qty,
        audit_fields=qty_estimate["audit_fields"],
    )


def compute_spike_threshold(price_history: list) -> float:
    prices = _extract_price_values(price_history)
    if len(prices) < 5:
        return 0.0
    prices.sort()
    return round(_percentile(prices, SPIKE_PERCENTILE), 2)


def identify_spike_windows(price_history: list, threshold: float) -> list:
    points = _normalize_price_points(price_history)
    if threshold <= 0 or len(points) < 2:
        return []

    intervals = [points[i + 1][0] - points[i][0] for i in range(len(points) - 1)]
    intervals = [value for value in intervals if value > 0]
    default_interval = int(sorted(intervals)[len(intervals) // 2]) if intervals else 60

    windows: list[dict[str, float]] = []
    window_start: int | None = None
    window_end: int | None = None
    window_duration = 0
    weighted_price_sum = 0.0

    def close_window() -> None:
        nonlocal window_start, window_end, window_duration, weighted_price_sum
        if window_start is None or window_end is None:
            return
        if window_duration >= SPIKE_MIN_WINDOW_MINUTES:
            avg_price = weighted_price_sum / window_duration if window_duration > 0 else threshold
            windows.append(
                {
                    "start_ts": float(window_start),
                    "end_ts": float(window_end),
                    "duration_minutes": float(window_duration),
                    "avg_price": round(avg_price, 2),
                }
            )
        window_start = None
        window_end = None
        window_duration = 0
        weighted_price_sum = 0.0

    for idx, (ts, price) in enumerate(points):
        next_ts = points[idx + 1][0] if idx + 1 < len(points) else ts + default_interval
        segment_minutes = max(0, next_ts - ts)
        if segment_minutes <= 0:
            continue

        if price >= threshold:
            if window_start is None:
                window_start = ts
            window_end = next_ts
            window_duration += segment_minutes
            weighted_price_sum += price * segment_minutes
        else:
            close_window()

    close_window()
    return windows


def compute_spike_share(spike_windows: list, buybox_history: list) -> float:
    if not spike_windows:
        return 0.0

    spike_minutes = sum(
        float(window.get("duration_minutes", 0.0))
        for window in spike_windows
        if isinstance(window, dict)
    )
    if spike_minutes <= 0:
        return 0.0

    timestamps = _extract_timestamps(buybox_history)
    if len(timestamps) >= 2:
        total_minutes = max(timestamps) - min(timestamps)
    else:
        starts = [
            float(window.get("start_ts", 0.0))
            for window in spike_windows
            if isinstance(window, dict)
        ]
        ends = [
            float(window.get("end_ts", 0.0))
            for window in spike_windows
            if isinstance(window, dict)
        ]
        total_minutes = (max(ends) - min(starts)) if starts and ends else 0.0

    if total_minutes <= 0:
        return 0.0
    return round((spike_minutes / total_minutes) * 100.0, 2)


def _evaluate_spike_path(
    row_data: dict[str, str],
    keepa: KeepaMetrics,
    min_roi: float,
    min_margin: float,
) -> dict[str, object]:
    price_history = keepa.buy_box_price_history
    threshold = compute_spike_threshold(price_history)
    windows = identify_spike_windows(price_history, threshold)
    spike_share_percent = compute_spike_share(windows, price_history)

    sales_month = keepa.est_sales_month or 0
    effective_spike_units = sales_month * (spike_share_percent / 100.0) * SPIKE_HAIRCUT

    cost_basis = _parse_number(row_data.get("Landed Cost / Unit (all-in)"))
    if cost_basis is None:
        cost_basis = _estimate_unit_cost_from_base_roi(row_data=row_data)

    spike_roi = _compute_roi_percent(threshold, cost_basis)
    spike_margin = _compute_margin_percent(threshold, cost_basis)

    reasons: list[str] = []
    if threshold <= 0:
        reasons.append("Spike path failed: insufficient Buy Box history for threshold.")
    if not windows:
        reasons.append("Spike path failed: no stable spike windows at/above threshold.")
    if sales_month < SPIKE_MIN_SALES_MONTH:
        reasons.append(f"Spike path failed: SalesPerMonth {sales_month} < {SPIKE_MIN_SALES_MONTH}.")
    if spike_share_percent < SPIKE_MIN_SHARE_PERCENT:
        reasons.append(
            f"Spike path failed: SpikeShare {spike_share_percent:.2f}% < {SPIKE_MIN_SHARE_PERCENT:.2f}%."
        )
    if effective_spike_units < SPIKE_MIN_EFFECTIVE_UNITS_MONTH:
        reasons.append(
            "Spike path failed: EffectiveSpikeUnitsMo "
            f"{effective_spike_units:.2f} < {SPIKE_MIN_EFFECTIVE_UNITS_MONTH:.2f}."
        )
    if spike_roi is None:
        reasons.append("Spike path failed: cannot compute spike ROI (missing cost basis).")
    elif spike_roi < min_roi:
        reasons.append(f"Spike path failed: spike ROI {spike_roi:.2f}% < {min_roi:.2f}%.")
    if spike_margin is None:
        reasons.append("Spike path failed: cannot compute spike margin (missing cost basis).")
    elif spike_margin < min_margin:
        reasons.append(f"Spike path failed: spike margin {spike_margin:.2f}% < {min_margin:.2f}%.")

    qualifies = len(reasons) == 0
    audit_fields = {
        "Spike Threshold": _fmt_optional_float(threshold),
        "Spike Share %": _fmt_optional_float(spike_share_percent),
        "Effective Spike Units / Mo": _fmt_optional_float(effective_spike_units),
        "Spike ROI %": _fmt_optional_float(spike_roi),
        "Spike Margin %": _fmt_optional_float(spike_margin),
        "Spike Windows Count": str(len(windows)),
        "Spike Path Qualified": "YES" if qualifies else "NO",
    }
    return {
        "qualifies": qualifies,
        "reasons": reasons,
        "spike_share_percent": spike_share_percent,
        "audit_fields": audit_fields,
    }


def _normalize_enum(value: str | None) -> str:
    return (value or "").strip().upper()


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


def _parse_buy_box_range_midpoint(value: str | None) -> float | None:
    raw = (value or "").strip()
    if not raw:
        return None
    matches = re.findall(r"\d+(?:\.\d+)?", raw.replace(",", ""))
    if not matches:
        return None
    try:
        prices = [float(matches[0])]
        if len(matches) >= 2:
            prices.append(float(matches[1]))
    except ValueError:
        return None
    return sum(prices) / len(prices)


def _estimate_break_even_floor(row_data: dict[str, str], roi_percent: float) -> float | None:
    if roi_percent <= -99.0:
        return None
    current_buy_box = _parse_buy_box_range_midpoint(row_data.get("Buy Box Range (Current)"))
    if current_buy_box is None:
        return None
    return current_buy_box / (1.0 + (roi_percent / 100.0))


def _estimate_unit_cost_from_base_roi(row_data: dict[str, str]) -> float | None:
    roi = _parse_number(row_data.get("ROI %"))
    if roi is None or roi <= -99.0:
        return None
    current_buy_box = _parse_buy_box_range_midpoint(row_data.get("Buy Box Range (Current)"))
    if current_buy_box is None or current_buy_box <= 0:
        return None
    return current_buy_box / (1.0 + (roi / 100.0))


def _compute_roi_percent(price: float | None, cost: float | None) -> float | None:
    if price is None or cost is None or cost <= 0:
        return None
    return ((price - cost) / cost) * 100.0


def _compute_margin_percent(price: float | None, cost: float | None) -> float | None:
    if price is None or cost is None or price <= 0:
        return None
    return ((price - cost) / price) * 100.0


def _classify_risk(
    keepa: KeepaMetrics,
    offer_spike: bool,
    price_floor_breach: bool,
) -> str:
    score = 0
    if keepa.buy_box_stability == "UNSTABLE":
        score += 1
    if offer_spike:
        score += 2
    if price_floor_breach:
        score += 2
    if keepa.amazon_buy_box_pct_90d is not None and keepa.amazon_buy_box_pct_90d >= 35:
        score += 1

    if score >= 3:
        return "HIGH"
    if score >= 1:
        return "MED"
    return "LOW"


def _estimate_buy_qty_for_new_fba_entrant(
    keepa: KeepaMetrics,
    fallback_competitors: float | None,
) -> tuple[dict[str, object] | None, str | None]:
    sales_month = keepa.est_sales_month
    if sales_month is None or sales_month <= 0:
        return None, "Est Sales / Month missing"

    fba_share_pct = keepa.buy_box_fba_share_90d
    if fba_share_pct is None:
        return None, "Buy Box FBA share (90d) missing"
    fba_share_pct = max(0.0, min(100.0, fba_share_pct))
    addressable_fba_units = sales_month * (fba_share_pct / 100.0)
    if addressable_fba_units <= 0:
        return None, "Buy Box FBA share implies zero addressable demand"

    comp_fba = keepa.competitive_fba_sellers_near_bb
    comp_fbm = keepa.competitive_fbm_sellers_near_bb
    comp_total = keepa.competitive_sellers_near_bb

    if comp_fba is None:
        if comp_total is not None and comp_total > 0:
            comp_fba = comp_total
        elif fallback_competitors is not None and fallback_competitors > 0:
            comp_fba = int(math.ceil(fallback_competitors))
    if comp_fba is None:
        return None, "Competitive FBA seller pool missing"
    comp_fba = max(1, int(comp_fba))

    if comp_fbm is None:
        if comp_total is not None:
            comp_fbm = max(0, comp_total - comp_fba)
        else:
            comp_fbm = 0

    effective_competitors = (
        float(comp_fba)
        + (float(comp_fbm) * FBM_COMPETITOR_WEIGHT)
        + NEW_ENTRANT_PENALTY
    )
    if effective_competitors <= 0:
        return None, "Effective competitor count invalid"

    base_confidence = _quantity_confidence_multiplier(keepa)
    stock_modifier, stock_days_ahead, stock_reliability = _stock_pressure_modifier(
        keepa=keepa,
        addressable_fba_units_month=addressable_fba_units,
    )
    confidence = max(0.30, min(0.90, base_confidence * stock_modifier))
    entrant_units_est = (addressable_fba_units / effective_competitors) * confidence
    if entrant_units_est < 0.5:
        return None, (
            f"Entrant unit estimate too low ({entrant_units_est:.2f}/mo) to justify purchase; "
            "market may be over-competed or demand too thin"
        )
    recommended_qty = min(BUY_QTY_CAP, max(1, int(math.ceil(entrant_units_est))))

    summary = (
        "Qty model="
        f"AddrFBA {addressable_fba_units:.1f}/mo, "
        f"EffComp {effective_competitors:.2f}, "
        f"Conf {confidence:.2f} (base {base_confidence:.2f} * stock {stock_modifier:.2f}), "
        f"Entrant {entrant_units_est:.1f}/mo."
    )
    audit_fields = {
        "Buy Box FBA Share % (90d)": f"{fba_share_pct:.2f}",
        "Buy Box FBM Share % (90d)": _fmt_optional_float(keepa.buy_box_fbm_share_90d),
        "Competitive FBA Sellers Near BB": str(int(comp_fba)),
        "Competitive FBM Sellers Near BB": str(int(comp_fbm)),
        "Addressable FBA Units / Mo": f"{addressable_fba_units:.2f}",
        "Effective Competitor Count": f"{effective_competitors:.2f}",
        "Qty Confidence Multiplier": f"{confidence:.2f}",
        "Base Qty Confidence": f"{base_confidence:.2f}",
        "Stock Pressure Multiplier": f"{stock_modifier:.2f}",
        "Competitive Weighted Stock Units": _fmt_optional_float(keepa.competitive_weighted_stock_units),
        "Competitive Stock Known Sellers": _fmt_optional_int(keepa.competitive_stock_known_sellers),
        "Competitive Stock Total Sellers": _fmt_optional_int(keepa.competitive_stock_total_sellers),
        "Competitive Stock Days Ahead": _fmt_optional_float(stock_days_ahead),
        "Stock Signal Reliability": _fmt_optional_float(
            None if stock_reliability is None else (stock_reliability * 100.0)
        ),
        "Entrant Units Est / Mo": f"{entrant_units_est:.2f}",
    }
    return {
        "recommended_qty": recommended_qty,
        "summary": summary,
        "audit_fields": audit_fields,
    }, None


def _quantity_confidence_multiplier(keepa: KeepaMetrics) -> float:
    multiplier = BASE_CONFIDENCE_HAIRCUT
    if keepa.buy_box_stability == "UNSTABLE":
        multiplier *= UNSTABLE_CONFIDENCE_HAIRCUT

    offer_delta = keepa.offer_count_delta_14d
    if offer_delta is not None:
        if offer_delta >= 8:
            multiplier *= HIGH_CHURN_CONFIDENCE_HAIRCUT
        elif offer_delta >= 4:
            multiplier *= MED_CHURN_CONFIDENCE_HAIRCUT

    spread = keepa.buy_box_relative_spread_21d
    if spread is not None:
        if spread >= 0.25:
            multiplier *= HIGH_SPREAD_CONFIDENCE_HAIRCUT
        elif spread >= 0.15:
            multiplier *= MED_SPREAD_CONFIDENCE_HAIRCUT

    return max(0.35, min(0.9, multiplier))


def _stock_pressure_modifier(
    keepa: KeepaMetrics,
    addressable_fba_units_month: float,
) -> tuple[float, float | None, float | None]:
    weighted_stock = keepa.competitive_weighted_stock_units
    known_sellers = keepa.competitive_stock_known_sellers
    total_sellers = keepa.competitive_stock_total_sellers

    if weighted_stock is None or known_sellers is None or known_sellers <= 0:
        return STOCK_MISSING_CONFIDENCE_HAIRCUT, None, None

    daily_addressable = max(0.1, addressable_fba_units_month / 30.0)
    stock_days_ahead = weighted_stock / daily_addressable

    modifier = 1.0
    if stock_days_ahead >= 60:
        modifier *= STOCK_EXTREME_COVERAGE_HAIRCUT
    elif stock_days_ahead >= 30:
        modifier *= STOCK_HIGH_COVERAGE_HAIRCUT
    elif stock_days_ahead >= 15:
        modifier *= STOCK_MED_COVERAGE_HAIRCUT
    elif stock_days_ahead <= 5:
        modifier *= STOCK_LOW_PRESSURE_BONUS
    elif stock_days_ahead <= 10:
        modifier *= STOCK_MODERATE_PRESSURE_BONUS

    reliability: float | None = None
    if total_sellers is not None and total_sellers > 0:
        reliability = max(0.0, min(1.0, float(known_sellers) / float(total_sellers)))
        if reliability < 0.5:
            modifier *= STOCK_MISSING_CONFIDENCE_HAIRCUT
        elif reliability < 0.75:
            modifier *= STOCK_LOW_COVERAGE_HAIRCUT

    return modifier, stock_days_ahead, reliability


def _merge_audit_fields(base: dict[str, str], extra: dict[str, str]) -> dict[str, str]:
    merged = dict(base)
    merged.update(extra)
    return merged


def _dedupe_reasons(reasons: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for reason in reasons:
        if reason in seen:
            continue
        seen.add(reason)
        deduped.append(reason)
    return deduped


def _extract_price_values(price_history: list) -> list[float]:
    prices: list[float] = []
    for item in price_history:
        if not isinstance(item, (list, tuple)) or len(item) < 2:
            continue
        try:
            price = float(item[1])
        except (TypeError, ValueError):
            continue
        if price > 0:
            prices.append(price)
    return prices


def _normalize_price_points(price_history: list) -> list[tuple[int, float]]:
    points: list[tuple[int, float]] = []
    for item in price_history:
        if not isinstance(item, (list, tuple)) or len(item) < 2:
            continue
        try:
            ts = int(item[0])
            price = float(item[1])
        except (TypeError, ValueError):
            continue
        if price <= 0:
            continue
        points.append((ts, price))
    points.sort(key=lambda pair: pair[0])
    return points


def _extract_timestamps(history: list) -> list[int]:
    timestamps: list[int] = []
    for item in history:
        if not isinstance(item, (list, tuple)) or not item:
            continue
        try:
            ts = int(item[0])
        except (TypeError, ValueError):
            continue
        timestamps.append(ts)
    return timestamps


def _percentile(sorted_values: list[float], percentile: float) -> float:
    if not sorted_values:
        return 0.0
    if percentile <= 0:
        return sorted_values[0]
    if percentile >= 100:
        return sorted_values[-1]

    rank = (percentile / 100.0) * (len(sorted_values) - 1)
    lower_index = int(math.floor(rank))
    upper_index = int(math.ceil(rank))
    if lower_index == upper_index:
        return sorted_values[lower_index]
    lower_value = sorted_values[lower_index]
    upper_value = sorted_values[upper_index]
    weight = rank - lower_index
    return lower_value + (upper_value - lower_value) * weight


def _fmt_optional_float(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.2f}"


def _fmt_optional_int(value: int | None) -> str:
    if value is None:
        return ""
    return str(int(value))
