from __future__ import annotations

import base64
from datetime import datetime, timezone
import json
import os
import re
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

import requests

from src.keepa_client import KEEPA_EPOCH_OFFSET_MINUTES
from src.models import KeepaMetrics
from src.models import PolicyDecision
from src.policy import _estimate_break_even_floor, compute_recommended_qty

_PROMPTS_DIR = Path(__file__).resolve().parent.parent / "prompts"
ANTHROPIC_REQUEST_TIMEOUT_SECONDS = 90.0

# Regex patterns used by validate_citations
_CITATION_FLOAT_RE = re.compile(r"\$?(\d{1,8}(?:\.\d{1,4})?)")
_CITATION_ISO_TS_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z")
ANTHROPIC_REQUEST_MAX_RETRIES = 3
ANTHROPIC_RETRYABLE_STATUS_CODES = {408, 409, 425, 429, 500, 502, 503, 504}


def _load_system_prompt(filename: str) -> str:
    path = _PROMPTS_DIR / filename
    try:
        return path.read_text(encoding="utf-8").lstrip("\ufeff").strip()
    except OSError:
        return ""


LLM1_SYSTEM_PROMPT = _load_system_prompt("llm1_system_prompt.txt")
LLM2_SYSTEM_PROMPT = _load_system_prompt("llm2_system_prompt.txt")


def build_verified_facts(
    keepa: KeepaMetrics,
    row_data: dict,
    gate_reasons: list[str],
    break_even_floor: float | None,
    amazon_fees_total: float | None = None,
) -> dict:
    """
    Compute a single verified-facts object from pipeline data. Pure Python —
    no LLM calls, no external I/O, no randomness. Every float rounded to 2dp.
    Returns a minimal safe dict on any failure.

    amazon_fees_total: pass the float directly from fee_context rather than
    relying on row_data["Amazon Fees Total"], which is not populated at the
    point llm_decision_pipeline is called.
    """
    try:
        history = keepa.buy_box_price_history  # list[tuple[int, float]] — (keepa_minutes, price_usd)

        # ── Section 1: BB 90d stats ────────────────────────────────────────────
        low_price = keepa.buy_box_90d_low
        low_present = False
        if low_price is not None and history:
            low_present = any(abs(price_usd - low_price) <= 0.02 for _, price_usd in history)

        bb_90d: dict[str, Any] = {
            "low": round(low_price, 2) if low_price is not None else None,
            "low_timestamp_unix": keepa.buy_box_90d_low_timestamp,
            "low_present_in_history": low_present,
            "avg": round(keepa.buy_box_90d_avg, 2) if keepa.buy_box_90d_avg is not None else None,
            "amazon_pct": round(keepa.amazon_buy_box_pct_90d, 2) if keepa.amazon_buy_box_pct_90d is not None else None,
            "fba_share_pct": round(keepa.buy_box_fba_share_90d, 2) if keepa.buy_box_fba_share_90d is not None else None,
            "stability": keepa.buy_box_stability,
        }

        # ── Section 2: History summary ─────────────────────────────────────────
        if history:
            prices = [p for _, p in history]
            hist_avg = round(sum(prices) / len(prices), 2)

            days_below = 0
            if break_even_floor is not None:
                below_dates: set[str] = set()
                for km, p in history:
                    if p < break_even_floor:
                        unix_ts = (km + KEEPA_EPOCH_OFFSET_MINUTES) * 60
                        below_dates.add(
                            datetime.fromtimestamp(unix_ts, tz=timezone.utc).strftime("%Y-%m-%d")
                        )
                days_below = len(below_dates)

            history_30d: dict[str, Any] = {
                "window_start": _keepa_minutes_to_iso(history[0][0]),
                "window_end": _keepa_minutes_to_iso(history[-1][0]),
                "entry_count": len(history),
                "low": round(min(prices), 2),
                "high": round(max(prices), 2),
                "avg": hist_avg,
                "days_below_breakeven": days_below,
            }
        else:
            hist_avg = None
            history_30d = {
                "window_start": None,
                "window_end": None,
                "entry_count": 0,
                "low": None,
                "high": None,
                "avg": None,
                "days_below_breakeven": 0,
            }

        # ── Section 3: Price events ────────────────────────────────────────────
        events: list[dict[str, Any]] = []
        event_counter = [0]  # list avoids nonlocal

        def _next_id() -> str:
            event_counter[0] += 1
            return f"evt_{event_counter[0]:03d}"

        def _neighbors(idx: int) -> tuple[dict | None, dict | None]:
            prior = None
            nxt = None
            if idx > 0:
                km, p = history[idx - 1]
                prior = {"timestamp": _keepa_minutes_to_iso(km), "price": round(p, 2)}
            if idx < len(history) - 1:
                km, p = history[idx + 1]
                nxt = {"timestamp": _keepa_minutes_to_iso(km), "price": round(p, 2)}
            return prior, nxt

        def _recovery_min(idx: int) -> int | None:
            if idx >= len(history) - 1:
                return None
            return history[idx + 1][0] - history[idx][0]

        def _make_event(event_type: str, idx: int) -> dict[str, Any]:
            km, p = history[idx]
            prior, nxt = _neighbors(idx)
            return {
                "event_id": _next_id(),
                "type": event_type,
                "timestamp": _keepa_minutes_to_iso(km),
                "price": round(p, 2),
                "prior": prior,
                "next": nxt,
                "recovery_minutes": _recovery_min(idx),
            }

        abs_min_idx: int | None = None
        if history:
            abs_min_idx = min(range(len(history)), key=lambda i: history[i][1])
            events.append(_make_event("absolute_minimum", abs_min_idx))

        # Local minima — cap at 5 most significant by depth below history avg
        if len(history) >= 3:
            avg_val = hist_avg if hist_avg is not None else 0.0
            local_min_candidates: list[tuple[float, int]] = []
            for i in range(1, len(history) - 1):
                if i == abs_min_idx:
                    continue
                _, prev_p = history[i - 1]
                _, curr_p = history[i]
                _, next_p = history[i + 1]
                if curr_p < prev_p and curr_p < next_p:
                    local_min_candidates.append((avg_val - curr_p, i))
            local_min_candidates.sort(key=lambda x: x[0], reverse=True)
            for _, i in local_min_candidates[:5]:
                events.append(_make_event("local_minimum", i))

        # Sharp recoveries — consecutive pairs where next > prior + $2.00, cap 3 by magnitude
        if len(history) >= 2:
            recovery_candidates: list[tuple[float, int]] = []
            for i in range(len(history) - 1):
                _, p_i = history[i]
                _, p_next = history[i + 1]
                gain = p_next - p_i
                if gain > 2.00:
                    recovery_candidates.append((gain, i))
            recovery_candidates.sort(key=lambda x: x[0], reverse=True)
            for _, i in recovery_candidates[:3]:
                events.append(_make_event("sharp_recovery", i))

        # ── Section 4: Competition ─────────────────────────────────────────────
        competition: dict[str, Any] = {
            "fba_sellers_near_bb": keepa.competitive_fba_sellers_near_bb,
            "fbm_sellers_near_bb": keepa.competitive_fbm_sellers_near_bb,
            "total_near_bb": keepa.competitive_sellers_near_bb,
            "weighted_stock": (
                round(keepa.competitive_weighted_stock_units, 2)
                if keepa.competitive_weighted_stock_units is not None
                else None
            ),
            "offer_count_delta_14d": keepa.offer_count_delta_14d,
            "est_sales_month": keepa.est_sales_month,
        }

        # ── Section 5: Profitability ───────────────────────────────────────────
        landed_cost = (
            _parse_number(row_data.get("Landed Cost / Unit (all-in)"))
            or _parse_number(row_data.get("Landed Cost / Unit"))
        )
        if amazon_fees_total is None:
            amazon_fees_total = _parse_number(row_data.get("Amazon Fees Total"))
        roi_pct = _parse_number(row_data.get("ROI %"))
        margin_pct = _parse_number(row_data.get("Margin %"))

        def _roi(sell_price: float | None) -> float | None:
            if sell_price is None or amazon_fees_total is None or not landed_cost:
                return None
            return round((sell_price - amazon_fees_total - landed_cost) / landed_cost * 100, 2)

        history_low = history_30d.get("low")
        roi_at_bb_90d_low = _roi(low_price) if low_present else None

        profitability: dict[str, Any] = {
            "current_bb_price": (
                round(keepa.current_buy_box_price, 2)
                if keepa.current_buy_box_price is not None
                else None
            ),
            "landed_cost": round(landed_cost, 2) if landed_cost is not None else None,
            "roi_percent": round(roi_pct, 2) if roi_pct is not None else None,
            "margin_percent": round(margin_pct, 2) if margin_pct is not None else None,
            "breakeven_floor": round(break_even_floor, 2) if break_even_floor is not None else None,
            "roi_at_history_30d_low": _roi(history_low),
            "roi_at_bb_90d_low": roi_at_bb_90d_low,
        }

        return {
            "bb_90d": bb_90d,
            "history_30d": history_30d,
            "price_events": events,
            "competition": competition,
            "profitability": profitability,
        }

    except Exception as exc:
        return {
            "bb_90d": {},
            "history_30d": {},
            "price_events": [],
            "competition": {},
            "profitability": {},
            "_error": f"{type(exc).__name__}: {exc}",
        }


def fetch_graph_image(asin: str, keepa_api_key: str) -> str | None:
    """
    Download a 365-day Keepa chart for *asin* and return it as a base64 string
    suitable for an Anthropic vision content block. Returns None on any failure;
    the pipeline degrades gracefully without the image.
    """
    try:
        from keepa import Keepa  # deferred: optional dependency
    except ImportError:
        print("[WARN] fetch_graph_image: keepa package not installed; skipping graph image.", file=sys.stderr)
        return None

    tmp_path: str | None = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
            tmp_path = tmp.name

        api = Keepa(keepa_api_key)
        api.download_graph_image(
            asin=asin,
            filename=tmp_path,
            domain="US",
            bb=1,
            new=1,
            range=365,
            width=1200,
            height=500,
        )
        with open(tmp_path, "rb") as f:
            return base64.standard_b64encode(f.read()).decode("utf-8")
    except Exception as exc:
        print(f"[WARN] fetch_graph_image: failed for ASIN {asin!r}: {type(exc).__name__}: {exc}", file=sys.stderr)
        return None
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)


def validate_citations(llm1_output: dict, verified_facts: dict) -> dict:
    """
    Pure-Python deterministic check: every cited_facts entry in llm1_output must
    exist in verified_facts, and any values mentioned in cited_evidence must match
    within tolerance. Never raises.
    """
    _malformed: dict[str, Any] = {
        "all_valid": False,
        "checked": [],
        "failures": [{"reason": "llm1_output_malformed"}],
    }

    try:
        assessments = llm1_output.get("gate_assessments")
        if not isinstance(assessments, list):
            return _malformed
    except Exception:
        return _malformed

    checked: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []

    try:
        for assessment in assessments:
            if not isinstance(assessment, dict):
                continue

            cited_facts_raw = assessment.get("cited_facts", [])
            cited_evidence = str(assessment.get("cited_evidence") or "")

            if not isinstance(cited_facts_raw, list):
                continue

            for raw_citation in cited_facts_raw:
                citation = str(raw_citation).strip()

                if citation.startswith("verified_facts."):
                    result = _check_field_citation(citation, cited_evidence, verified_facts)
                elif citation.startswith("evt_"):
                    result = _check_event_citation(citation, cited_evidence, verified_facts)
                else:
                    result = {
                        "citation": citation,
                        "exists": False,
                        "value_matches": False,
                        "resolved_value": None,
                        "valid": False,
                    }

                checked.append(result)

                if not result["valid"]:
                    if not result["exists"]:
                        if not (citation.startswith("verified_facts.") or citation.startswith("evt_")):
                            reason = "unrecognised citation format"
                        else:
                            reason = "path not found in verified_facts"
                    else:
                        reason = "value in cited_evidence does not match resolved value"
                    failures.append({"citation": citation, "reason": reason})

    except Exception as exc:
        return {
            "all_valid": False,
            "checked": checked,
            "failures": failures + [{"reason": f"parse error: {type(exc).__name__}: {exc}"}],
        }

    all_valid = all(r["valid"] for r in checked) if checked else True
    return {
        "all_valid": all_valid,
        "checked": checked,
        "failures": failures,
    }


def _check_field_citation(
    citation: str, cited_evidence: str, verified_facts: dict
) -> dict[str, Any]:
    """Navigate a verified_facts.<path> citation and check numeric values in cited_evidence."""
    path_str = citation[len("verified_facts."):]
    parts = [p for p in path_str.split(".") if p]

    node: Any = verified_facts
    for part in parts:
        if not isinstance(node, dict) or part not in node:
            return {
                "citation": citation,
                "exists": False,
                "value_matches": False,
                "resolved_value": None,
                "valid": False,
            }
        node = node[part]

    value_matches = _numeric_appears_in_evidence(node, cited_evidence)
    return {
        "citation": citation,
        "exists": True,
        "value_matches": value_matches,
        "resolved_value": node,
        "valid": value_matches,
    }


def _check_event_citation(
    citation: str, cited_evidence: str, verified_facts: dict
) -> dict[str, Any]:
    """Find an evt_XXX in price_events and verify price/timestamp values in cited_evidence."""
    events = verified_facts.get("price_events")
    if not isinstance(events, list):
        return {
            "citation": citation,
            "exists": False,
            "value_matches": False,
            "resolved_value": None,
            "valid": False,
        }

    event = next(
        (e for e in events if isinstance(e, dict) and e.get("event_id") == citation),
        None,
    )
    if event is None:
        return {
            "citation": citation,
            "exists": False,
            "value_matches": False,
            "resolved_value": None,
            "valid": False,
        }

    price_ok = _numeric_appears_in_evidence(event.get("price"), cited_evidence)
    ts_ok = _timestamp_appears_in_evidence(event.get("timestamp"), cited_evidence)
    value_matches = price_ok and ts_ok

    return {
        "citation": citation,
        "exists": True,
        "value_matches": value_matches,
        "resolved_value": {"price": event.get("price"), "timestamp": event.get("timestamp")},
        "valid": value_matches,
    }


def _numeric_appears_in_evidence(resolved_value: Any, cited_evidence: str) -> bool:
    """
    If resolved_value is numeric, at least one float extracted from cited_evidence
    must be within $0.02. If resolved_value is not numeric, or no floats appear in
    cited_evidence, return True (no numeric claim to refute).
    """
    if resolved_value is None:
        return True
    if isinstance(resolved_value, bool):
        # bool is a subclass of int; float(False)==0.0 would produce spurious failures
        return True
    try:
        target = float(resolved_value)
    except (TypeError, ValueError):
        return True  # non-numeric field — nothing to check

    extracted = [float(m) for m in _CITATION_FLOAT_RE.findall(cited_evidence)]
    if not extracted:
        return True  # analyst made no numeric claim — not a failure
    return any(abs(v - target) <= 0.02 for v in extracted)


def _timestamp_appears_in_evidence(timestamp: Any, cited_evidence: str) -> bool:
    """
    If timestamp is an ISO string, check it appears verbatim in cited_evidence.
    If no ISO timestamps appear in cited_evidence, return True (no claim made).
    """
    if not isinstance(timestamp, str):
        return True
    found = _CITATION_ISO_TS_RE.findall(cited_evidence)
    if not found:
        return True  # analyst made no timestamp claim — not a failure
    return timestamp in found


def llm_review(
    verified_facts: dict,
    graph_image_base64: str | None,
    gate_reasons: list[str],
) -> dict | None:
    """
    Call LLM1 (analyst) with verified_facts, an optional graph image, and the
    gate reasons that triggered review. Returns the parsed JSON response or None.
    """
    try:
        api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
        if not api_key:
            return None

        content: list[dict[str, Any]] = []

        content.append({
            "type": "text",
            "text": (
                f"Gate reasons that triggered this review:\n{json.dumps(gate_reasons, indent=2)}"
                f"\n\nVerified facts:\n{json.dumps(verified_facts, indent=2)}"
            ),
        })

        if graph_image_base64:
            content.append({
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": "image/png",
                    "data": graph_image_base64,
                },
            })

        content.append({
            "type": "text",
            "text": (
                "Based on the verified facts and graph image above, assess each gate reason "
                "and provide your decision in the required JSON format."
            ),
        })

        llm1_model = (
            os.getenv("ANTHROPIC_LLM1_MODEL", "claude-sonnet-4-20250514").strip()
            or "claude-sonnet-4-20250514"
        )
        response, _ = _post_anthropic_messages(
            api_key=api_key,
            payload={
                "model": llm1_model,
                "max_tokens": 1000,
                "temperature": 0,
                "system": LLM1_SYSTEM_PROMPT,
                "messages": [{"role": "user", "content": content}],
            },
            route_name="llm_review",
        )
        if response is None:
            return None

        parsed, _ = _parse_anthropic_json_response(response)
        return parsed
    except Exception:
        return None


def llm_verify(
    verified_facts: dict,
    llm1_output: dict,
) -> dict | None:
    """
    Call LLM2 (verifier) with verified_facts and the analyst's output.
    LLM2 no longer receives raw price or seller history arrays.
    Returns the parsed JSON response or None.
    """
    try:
        api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
        if not api_key:
            return None

        llm2_model = (
            os.getenv("ANTHROPIC_LLM2_MODEL", "claude-sonnet-4-20250514").strip()
            or "claude-sonnet-4-20250514"
        )
        response, _ = _post_anthropic_messages(
            api_key=api_key,
            payload={
                "model": llm2_model,
                "max_tokens": 1000,
                "temperature": 0,
                "system": LLM2_SYSTEM_PROMPT,
                "messages": [
                    {
                        "role": "user",
                        "content": json.dumps(
                            {
                                "verified_facts": verified_facts,
                                "analyst_output": llm1_output,
                            },
                            indent=2,
                        ),
                    }
                ],
            },
            route_name="llm_verify",
        )
        if response is None:
            return None

        parsed, _ = _parse_anthropic_json_response(response)
        return parsed
    except Exception:
        return None


def log_llm_run(
    verified_facts: dict,
    llm1_output: dict | None,
    llm2_output: dict | None,
    final_decision: PolicyDecision,
    *,
    graph_image_fetched: bool = False,
) -> None:
    project_root = Path(__file__).resolve().parent.parent
    logs_dir = project_root / "llm_logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    next_id = _next_log_id(logs_dir)
    file_path = logs_dir / f"llm_run_{next_id:04d}.json"

    payload = {
        "timestamp_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "graph_image_fetched": graph_image_fetched,
        "verified_facts": verified_facts,
        "llm1_output": llm1_output if llm1_output is not None else {},
        "llm2_output": llm2_output if llm2_output is not None else {},
        "final_decision": {
            "decision": final_decision.decision,
            "recommended_qty": final_decision.recommended_qty,
            "downside_risk": final_decision.downside_risk,
            "needs_human_review": final_decision.needs_human_review,
            "reasons": list(final_decision.reasons),
        },
    }
    file_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def llm_decision_pipeline(
    decision: PolicyDecision,
    keepa: KeepaMetrics,
    row_data: dict,
    fee_context: dict | None = None,
) -> PolicyDecision:
    # ── Step 1: hard bypass ───────────────────────────────────────────────────
    if decision.decision in {"BUY", "TEST"}:
        return decision
    _skip_phrases = ["IP Clean", "ASIN is gated", "Exact Match", "Est Sales / Month"]
    for reason in decision.reasons:
        if any(skip in reason for skip in _skip_phrases):
            return decision

    # ── Step 2: activation check ──────────────────────────────────────────────
    _trigger_phrases = ["Buy Box 90d Low", "Offer Count Delta"]
    should_activate = (
        (decision.decision == "DEFER" and decision.needs_human_review)
        or (
            decision.decision == "REJECT"
            and any(
                trigger in reason
                for reason in decision.reasons
                for trigger in _trigger_phrases
            )
        )
    )
    if not should_activate:
        return decision

    # ── Step 3: build verified facts ─────────────────────────────────────────
    roi_pct = _parse_number(row_data.get("ROI %"))
    break_even_floor: float | None = (
        _estimate_break_even_floor(row_data, roi_pct)
        if roi_pct is not None
        else None
    )
    verified_facts = build_verified_facts(
        keepa=keepa,
        row_data=row_data,
        gate_reasons=list(decision.reasons),
        break_even_floor=break_even_floor,
        amazon_fees_total=(fee_context or {}).get("amazon_fees_total"),
    )

    # ── Step 4: fetch graph image (graceful degrade) ──────────────────────────
    asin = str(row_data.get("ASIN") or "").strip().upper()
    keepa_api_key = os.getenv("KEEPA_API_KEY", "").strip()
    graph_image_base64 = fetch_graph_image(asin=asin, keepa_api_key=keepa_api_key)

    # ── Step 5: LLM 1 ────────────────────────────────────────────────────────
    llm1_output = llm_review(
        verified_facts=verified_facts,
        graph_image_base64=graph_image_base64,
        gate_reasons=list(decision.reasons),
    )
    _image_fetched = graph_image_base64 is not None
    if llm1_output is None:
        log_llm_run(verified_facts, None, None, decision, graph_image_fetched=_image_fetched)
        return _copy_policy_decision(decision, needs_human_review=True)

    # ── Step 6: Python citation validator ─────────────────────────────────────
    validation = validate_citations(llm1_output, verified_facts)
    if not validation["all_valid"]:
        log_llm_run(verified_facts, llm1_output, {"validation_failed": validation}, decision, graph_image_fetched=_image_fetched)
        return _copy_policy_decision(decision, needs_human_review=True)

    # ── Step 7: LLM 2 ────────────────────────────────────────────────────────
    llm2_output = llm_verify(
        verified_facts=verified_facts,
        llm1_output=llm1_output,
    )
    if llm2_output is None:
        log_llm_run(verified_facts, llm1_output, None, decision, graph_image_fetched=_image_fetched)
        return _copy_policy_decision(decision, needs_human_review=True)

    # ── Step 8: apply confidence thresholds ───────────────────────────────────
    overall_verified, confidence = _extract_verify_fields(llm2_output)
    llm2_decision = _normalize_decision_name(llm2_output.get("final_decision"))

    final_dec = llm2_decision or decision.decision
    if final_dec in {"BUY", "TEST"}:
        llm_qty, llm_qty_summary, _ = compute_recommended_qty(
            final_dec, keepa, row_data, decision.downside_risk
        )
    else:
        llm_qty, llm_qty_summary = 0, ""

    final_reasons = list(decision.reasons)
    if llm_qty_summary:
        final_reasons.append(llm_qty_summary)

    if overall_verified and confidence > 0.80:
        final = PolicyDecision(
            decision=final_dec,
            recommended_qty=llm_qty,
            downside_risk=decision.downside_risk,
            needs_human_review=False,
            reasons=final_reasons,
            audit_fields=dict(decision.audit_fields),
        )
    elif overall_verified and 0.50 <= confidence <= 0.80:
        final = PolicyDecision(
            decision=final_dec,
            recommended_qty=llm_qty,
            downside_risk=decision.downside_risk,
            needs_human_review=True,
            reasons=final_reasons,
            audit_fields=dict(decision.audit_fields),
        )
    else:
        final = _copy_policy_decision(decision, needs_human_review=True)

    # ── Step 9: log and return ────────────────────────────────────────────────
    log_llm_run(verified_facts, llm1_output, llm2_output, final, graph_image_fetched=_image_fetched)
    return final


def _parse_anthropic_json_response(response: requests.Response) -> tuple[dict | None, str | None]:
    payload = response.json()
    content = payload.get("content")
    if not isinstance(content, list):
        return None, "Anthropic response missing content list"

    text_chunks: list[str] = []
    for block in content:
        if not isinstance(block, dict):
            continue
        if block.get("type") != "text":
            continue
        text = block.get("text")
        if isinstance(text, str):
            text_chunks.append(text)

    if not text_chunks:
        return None, "Anthropic content had no text blocks"

    raw_text = "".join(text_chunks).strip()
    parsed = _parse_json_like_text(raw_text)
    if isinstance(parsed, dict):
        return parsed, None
    return None, f"Model output was not valid JSON object: {raw_text[:220]}"


def _post_anthropic_messages(
    api_key: str,
    payload: dict[str, Any],
    route_name: str,
) -> tuple[requests.Response | None, str | None]:
    timeout_seconds = _env_float("ANTHROPIC_REQUEST_TIMEOUT_SECONDS", ANTHROPIC_REQUEST_TIMEOUT_SECONDS)
    max_retries = _env_int("ANTHROPIC_REQUEST_MAX_RETRIES", ANTHROPIC_REQUEST_MAX_RETRIES)
    max_retries = max(1, min(8, max_retries))

    url = "https://api.anthropic.com/v1/messages"
    headers = {
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }

    last_error: str | None = None
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.post(
                url,
                headers=headers,
                json=payload,
                timeout=timeout_seconds,
            )
        except requests.ReadTimeout:
            last_error = (
                f"ReadTimeout after {timeout_seconds:.1f}s "
                f"(attempt {attempt}/{max_retries})"
            )
            if attempt < max_retries:
                time.sleep(_retry_delay_seconds(attempt))
                continue
            return None, f"{route_name}: {last_error}"
        except requests.RequestException as exc:
            last_error = (
                f"{type(exc).__name__}: {exc} "
                f"(attempt {attempt}/{max_retries})"
            )
            if attempt < max_retries:
                time.sleep(_retry_delay_seconds(attempt))
                continue
            return None, f"{route_name}: {last_error}"

        if response.status_code == 200:
            return response, None

        snippet = response.text[:300].replace("\n", " ").strip()
        last_error = f"Anthropic HTTP {response.status_code}: {snippet}"
        if response.status_code in ANTHROPIC_RETRYABLE_STATUS_CODES and attempt < max_retries:
            time.sleep(_retry_delay_seconds(attempt, retry_after=response.headers.get("retry-after")))
            continue
        return None, f"{route_name}: {last_error}"

    return None, f"{route_name}: {last_error or 'unknown error'}"


def _retry_delay_seconds(attempt: int, retry_after: str | None = None) -> float:
    if retry_after:
        try:
            delay = float(retry_after)
            if delay >= 0:
                return min(30.0, delay + 0.25)
        except ValueError:
            pass
    return min(30.0, 1.5 * attempt)


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    if value <= 0:
        return default
    return value


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    if value <= 0:
        return default
    return value


def _parse_json_like_text(text: str) -> dict | None:
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        pass

    fence_matches = re.findall(r"```(?:json)?\s*(.*?)\s*```", text, flags=re.DOTALL | re.IGNORECASE)
    for candidate in fence_matches:
        try:
            parsed = json.loads(candidate.strip())
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            continue

    left = text.find("{")
    right = text.rfind("}")
    if left >= 0 and right > left:
        candidate = text[left : right + 1]
        try:
            parsed = json.loads(candidate)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            return None
    return None


def _next_log_id(logs_dir: Path) -> int:
    max_id = 0
    for path in logs_dir.glob("llm_run_*.json"):
        match = re.match(r"^llm_run_(\d+)\.json$", path.name)
        if not match:
            continue
        try:
            max_id = max(max_id, int(match.group(1)))
        except ValueError:
            continue
    return max_id + 1


def _extract_verify_fields(llm2_output: dict) -> tuple[bool, float]:
    overall_verified_raw = llm2_output.get("overall_verified")
    if isinstance(overall_verified_raw, bool):
        overall_verified = overall_verified_raw
    else:
        overall_verified = str(overall_verified_raw).strip().lower() == "true"

    confidence_raw = llm2_output.get("confidence")
    try:
        confidence = float(confidence_raw)
    except (TypeError, ValueError):
        print(
            f"[WARN] llm_review: LLM2 confidence field could not be parsed "
            f"(value={confidence_raw!r}); defaulting to 0.0 and deferring to human review.",
            file=sys.stderr,
        )
        confidence = 0.0
    confidence = max(0.0, min(1.0, confidence))
    return overall_verified, confidence


def _copy_policy_decision(decision: PolicyDecision, needs_human_review: bool) -> PolicyDecision:
    return PolicyDecision(
        decision=decision.decision,
        recommended_qty=decision.recommended_qty,
        downside_risk=decision.downside_risk,
        needs_human_review=bool(needs_human_review),
        reasons=list(decision.reasons),
        audit_fields=dict(decision.audit_fields),
    )


def _normalize_decision_name(value: Any) -> str | None:
    raw = str(value or "").strip().upper()
    if raw in {"BUY", "TEST", "REJECT", "DEFER"}:
        return raw
    return None


def _parse_number(value: Any) -> float | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    match = re.search(r"-?\d+(?:\.\d+)?", raw.replace(",", ""))
    if not match:
        return None
    try:
        return float(match.group(0))
    except ValueError:
        return None


def _keepa_minutes_to_iso(keepa_minutes: int) -> str:
    unix_minutes = keepa_minutes + KEEPA_EPOCH_OFFSET_MINUTES
    dt = datetime.fromtimestamp(unix_minutes * 60, tz=timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


