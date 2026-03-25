from __future__ import annotations

import copy
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
import json
import os
import re
import time
from pathlib import Path
from typing import Any

import requests

from src.keepa_client import KEEPA_EPOCH_OFFSET_MINUTES
from src.models import KeepaMetrics
from src.models import PolicyDecision

_PROMPTS_DIR = Path(__file__).resolve().parent.parent / "prompts"
LLM_REVIEW_OUTPUT_AUDIT_FIELD = "LLM Review Output"
LLM_REVIEW_OUTPUT_MAX_CHARS = 45000
ANTHROPIC_REQUEST_TIMEOUT_SECONDS = 90.0
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


def llm_review(eval_json: dict) -> dict | None:
    result, _ = _llm_review_with_error(eval_json)
    return result


def _llm_review_with_error(eval_json: dict) -> tuple[dict | None, str | None]:
    try:
        cleaned_eval_json = copy.deepcopy(eval_json)

        raw_history = cleaned_eval_json.get("raw_history")
        if isinstance(raw_history, dict):
            buy_box_price_history = raw_history.get("buy_box_price_history")
            if isinstance(buy_box_price_history, list):
                for entry in buy_box_price_history:
                    if isinstance(entry, dict):
                        entry.pop("keepa_minutes", None)

            buy_box_seller_history = raw_history.get("buy_box_seller_history")
            if isinstance(buy_box_seller_history, list):
                for entry in buy_box_seller_history:
                    if isinstance(entry, dict):
                        entry.pop("keepa_minutes", None)

        api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
        if not api_key:
            return None, "ANTHROPIC_API_KEY missing"

        response, request_error = _post_anthropic_messages(
            api_key=api_key,
            payload={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 1000,
                "system": LLM1_SYSTEM_PROMPT,
                "messages": [
                    {
                        "role": "user",
                        "content": json.dumps(cleaned_eval_json),
                    }
                ],
            },
            route_name="llm_review",
        )
        if response is None:
            return None, request_error or "Anthropic request failed"

        parsed, parse_error = _parse_anthropic_json_response(response)
        if parsed is None:
            return None, parse_error or "unable to parse model JSON output"
        return parsed, None
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"


def llm_verify(eval_json: dict, llm1_output: dict) -> dict | None:
    result, _ = _llm_verify_with_error(eval_json, llm1_output)
    return result


def _llm_verify_with_error(eval_json: dict, llm1_output: dict) -> tuple[dict | None, str | None]:
    try:
        api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
        if not api_key:
            return None, "ANTHROPIC_API_KEY missing"

        response, request_error = _post_anthropic_messages(
            api_key=api_key,
            payload={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 1000,
                "system": LLM2_SYSTEM_PROMPT,
                "messages": [
                    {
                        "role": "user",
                        "content": json.dumps(
                            {
                                "raw_data": eval_json,
                                "analyst_output": llm1_output,
                            }
                        ),
                    }
                ],
            },
            route_name="llm_verify",
        )
        if response is None:
            return None, request_error or "Anthropic request failed"

        parsed, parse_error = _parse_anthropic_json_response(response)
        if parsed is None:
            return None, parse_error or "unable to parse model JSON output"
        return parsed, None
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"


def log_llm_run(
    eval_json: dict,
    llm1_output: dict,
    llm2_output: dict,
    final_decision: PolicyDecision,
) -> None:
    project_root = Path(__file__).resolve().parent.parent
    logs_dir = project_root / "llm_logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    next_id = _next_log_id(logs_dir)
    file_path = logs_dir / f"llm_run_{next_id:04d}.json"

    if is_dataclass(final_decision):
        final_decision_dict = asdict(final_decision)
    else:
        final_decision_dict = dict(getattr(final_decision, "__dict__", {}))

    payload = {
        "eval_json": eval_json,
        "llm1_output": llm1_output,
        "llm2_output": llm2_output,
        "final_decision": final_decision_dict,
        "timestamp_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    }
    file_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def llm_decision_pipeline(
    decision: PolicyDecision,
    keepa: KeepaMetrics,
    row_data: dict,
) -> PolicyDecision:
    eval_json = _build_eval_json(decision=decision, keepa=keepa, row_data=row_data)
    llm1_output: dict[str, Any] = {}
    llm2_output: dict[str, Any] = {}

    reason_strings = decision.reasons or []
    reason_blob = " | ".join(reason_strings)

    bypass = False
    if decision.decision in {"BUY", "TEST"}:
        bypass = True
    if _contains_phrase(reason_blob, "IP Clean"):
        bypass = True
    if _contains_phrase(reason_blob, "ASIN is gated"):
        bypass = True
    if _contains_phrase(reason_blob, "Exact Match"):
        bypass = True
    if _contains_phrase(reason_blob, "Est Sales / Month"):
        bypass = True

    if bypass:
        llm1_output = {"status": "bypass", "reason": "hard bypass condition met"}
        return _finalize_with_log(
            eval_json=eval_json,
            llm1_output=llm1_output,
            llm2_output=llm2_output,
            final_decision=decision,
        )

    activated = False
    if decision.decision == "DEFER" and bool(decision.needs_human_review):
        activated = True
    if decision.decision == "REJECT" and (
        _contains_phrase(reason_blob, "Buy Box 90d Low")
        or _contains_phrase(reason_blob, "Offer Count Delta")
    ):
        activated = True

    if not activated:
        llm1_output = {"status": "bypass", "reason": "activation conditions not met"}
        return _finalize_with_log(
            eval_json=eval_json,
            llm1_output=llm1_output,
            llm2_output=llm2_output,
            final_decision=decision,
        )

    llm1_result, llm1_error = _llm_review_with_error(eval_json)
    if llm1_result is None:
        llm1_output = {
            "status": "failed",
            "reason": f"llm_review failed: {llm1_error or 'unknown'}",
        }
        fallback = _copy_policy_decision(decision, needs_human_review=True)
        return _finalize_with_log(
            eval_json=eval_json,
            llm1_output=llm1_output,
            llm2_output=llm2_output,
            final_decision=fallback,
        )
    llm1_output = llm1_result

    llm2_result, llm2_error = _llm_verify_with_error(eval_json, llm1_output)
    if llm2_result is None:
        llm2_output = {
            "status": "failed",
            "reason": f"llm_verify failed: {llm2_error or 'unknown'}",
        }
        fallback = _copy_policy_decision(decision, needs_human_review=True)
        return _finalize_with_log(
            eval_json=eval_json,
            llm1_output=llm1_output,
            llm2_output=llm2_output,
            final_decision=fallback,
        )
    llm2_output = llm2_result

    overall_verified, confidence = _extract_verify_fields(llm2_output)
    if overall_verified and confidence > 0.80:
        llm_policy = _build_llm_policy_decision(
            baseline=decision,
            llm1_output=llm1_output,
            needs_human_review=False,
        )
        return _finalize_with_log(
            eval_json=eval_json,
            llm1_output=llm1_output,
            llm2_output=llm2_output,
            final_decision=llm_policy,
        )

    if overall_verified and 0.50 <= confidence <= 0.80:
        llm_policy = _build_llm_policy_decision(
            baseline=decision,
            llm1_output=llm1_output,
            needs_human_review=True,
        )
        return _finalize_with_log(
            eval_json=eval_json,
            llm1_output=llm1_output,
            llm2_output=llm2_output,
            final_decision=llm_policy,
        )

    fallback = _copy_policy_decision(decision, needs_human_review=True)
    return _finalize_with_log(
        eval_json=eval_json,
        llm1_output=llm1_output,
        llm2_output=llm2_output,
        final_decision=fallback,
    )


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


def _finalize_with_log(
    eval_json: dict,
    llm1_output: dict,
    llm2_output: dict,
    final_decision: PolicyDecision,
) -> PolicyDecision:
    enriched_decision = _attach_llm_outputs_to_decision(
        decision=final_decision,
        llm1_output=llm1_output,
        llm2_output=llm2_output,
    )
    try:
        log_llm_run(
            eval_json=eval_json,
            llm1_output=llm1_output,
            llm2_output=llm2_output,
            final_decision=enriched_decision,
        )
    except Exception:
        pass
    return enriched_decision


def _attach_llm_outputs_to_decision(
    decision: PolicyDecision,
    llm1_output: dict,
    llm2_output: dict,
) -> PolicyDecision:
    audit_fields = dict(decision.audit_fields)
    audit_fields[LLM_REVIEW_OUTPUT_AUDIT_FIELD] = _format_llm_review_output(
        llm1_output=llm1_output,
        llm2_output=llm2_output,
    )
    return PolicyDecision(
        decision=decision.decision,
        recommended_qty=decision.recommended_qty,
        downside_risk=decision.downside_risk,
        needs_human_review=decision.needs_human_review,
        reasons=list(decision.reasons),
        audit_fields=audit_fields,
    )


def _format_llm_review_output(llm1_output: dict, llm2_output: dict) -> str:
    try:
        llm1_text = json.dumps(llm1_output or {}, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        llm1_text = str(llm1_output)
    try:
        llm2_text = json.dumps(llm2_output or {}, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        llm2_text = str(llm2_output)

    combined = f"LLM1={llm1_text}\nLLM2={llm2_text}"
    if len(combined) <= LLM_REVIEW_OUTPUT_MAX_CHARS:
        return combined
    return combined[: LLM_REVIEW_OUTPUT_MAX_CHARS - 14] + "\n...[truncated]"


def _contains_phrase(text: str, phrase: str) -> bool:
    return phrase.lower() in text.lower()


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
        confidence = 0.0
    confidence = max(0.0, min(1.0, confidence))
    return overall_verified, confidence


def _build_llm_policy_decision(
    baseline: PolicyDecision,
    llm1_output: dict,
    needs_human_review: bool,
) -> PolicyDecision:
    llm_block = llm1_output.get("final_decision")
    decision_value: str | None = None
    recommended_qty: int | None = None
    downside_risk: str | None = None
    reasons_value: list[str] | None = None

    if isinstance(llm_block, dict):
        decision_value = _normalize_decision_name(llm_block.get("decision"))
        recommended_qty = _parse_int(llm_block.get("recommended_qty"))
        downside_risk = _normalize_risk(llm_block.get("downside_risk"))
        reasons_value = _normalize_reasons(
            llm_block.get("reasons"),
            llm_block.get("reason"),
        )
    elif isinstance(llm_block, str):
        decision_value = _normalize_decision_name(llm_block)

    if decision_value is None:
        decision_value = _normalize_decision_name(llm1_output.get("decision"))
    if decision_value is None:
        decision_value = baseline.decision

    if recommended_qty is None:
        recommended_qty = _parse_int(llm1_output.get("recommended_qty"))
    if recommended_qty is None:
        recommended_qty = baseline.recommended_qty

    if downside_risk is None:
        downside_risk = _normalize_risk(llm1_output.get("downside_risk"))
    if downside_risk is None:
        downside_risk = baseline.downside_risk

    if reasons_value is None:
        reasons_value = _normalize_reasons(
            llm1_output.get("reasons"),
            llm1_output.get("reason"),
        )
    if reasons_value is None:
        reasons_value = list(baseline.reasons)

    return PolicyDecision(
        decision=decision_value,
        recommended_qty=max(0, int(recommended_qty)),
        downside_risk=downside_risk,
        needs_human_review=bool(needs_human_review),
        reasons=reasons_value,
        audit_fields=dict(baseline.audit_fields),
    )


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


def _normalize_risk(value: Any) -> str | None:
    raw = str(value or "").strip().upper()
    if raw in {"LOW", "MED", "HIGH"}:
        return raw
    return None


def _normalize_reasons(primary: Any, secondary: Any) -> list[str] | None:
    reasons: list[str] = []
    if isinstance(primary, list):
        for item in primary:
            text = str(item).strip()
            if text:
                reasons.append(text)
    elif isinstance(primary, str) and primary.strip():
        reasons.append(primary.strip())
    if not reasons and isinstance(secondary, str) and secondary.strip():
        reasons.append(secondary.strip())
    return reasons or None


def _parse_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
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


def _build_eval_json(decision: PolicyDecision, keepa: KeepaMetrics, row_data: dict) -> dict[str, Any]:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    asin = str(row_data.get("ASIN") or "").strip().upper()

    gate_decision = {
        "qualified": decision.decision in {"BUY", "TEST"},
        "decision": decision.decision,
        "reason": decision.reasons_text(),
        "reasons": list(decision.reasons),
        "recommended_qty": decision.recommended_qty,
        "downside_risk": decision.downside_risk,
        "needs_human_review": decision.needs_human_review,
        "audit_fields": dict(decision.audit_fields),
    }

    key_metrics = asdict(keepa)
    key_metrics.pop("buy_box_price_history", None)
    key_metrics.pop("buy_box_seller_history", None)

    buy_box_price_history: list[dict[str, Any]] = []
    for ts, price in keepa.buy_box_price_history:
        buy_box_price_history.append(
            {
                "keepa_minutes": int(ts),
                "timestamp_utc": _keepa_minutes_to_iso(int(ts)),
                "price_usd": float(price),
            }
        )

    buy_box_seller_history: list[dict[str, Any]] = []
    for ts, seller_id in keepa.buy_box_seller_history:
        buy_box_seller_history.append(
            {
                "keepa_minutes": int(ts),
                "timestamp_utc": _keepa_minutes_to_iso(int(ts)),
                "seller_id": str(seller_id),
            }
        )

    profitability_errors_raw = str(row_data.get("Profitability Calc Error") or "").strip()
    profitability_errors = [item.strip() for item in profitability_errors_raw.split(";") if item.strip()]

    fee_context = {
        "estimated_sell_price_mid_bb": _parse_number(row_data.get("Estimated Sell Price (Mid BB)")),
        "buy_box_range_current": row_data.get("Buy Box Range (Current)") or keepa.buy_box_range_current,
        "amazon_fees_total": _parse_number(row_data.get("Amazon Fees Total")),
        "referral_fee": _parse_number(row_data.get("Referral Fee")),
        "fba_fulfillment_fee": _parse_number(row_data.get("FBA Fulfillment Fee")),
        "fee_breakdown": {},
        "package_weight_grams": keepa.package_weight_grams,
        "inbound_shipping_rate_per_lb": _parse_number(os.getenv("INBOUND_SHIPPING_USD_PER_LB")) or 0.77,
        "inbound_shipping_fee": _parse_number(row_data.get("Inbound Shipping Fee")),
        "landed_cost_per_unit": (
            _parse_number(row_data.get("Landed Cost / Unit (all-in)"))
            or _parse_number(row_data.get("Landed Cost / Unit"))
        ),
        "profit_per_unit": _parse_number(row_data.get("Profit / Unit")),
        "roi_percent": _parse_number(row_data.get("ROI %")),
        "margin_percent": _parse_number(row_data.get("Margin %")),
        "errors": profitability_errors,
    }

    eval_json: dict[str, Any] = {
        "eval_id": None,
        "asin": asin,
        "created_at_utc": now.isoformat().replace("+00:00", "Z"),
        "pipeline_context": {
            "policy_version": str(row_data.get("Policy Version") or os.getenv("POLICY_VERSION", "")).strip(),
            "keepa_domain_id": int(_parse_number(os.getenv("KEEPA_DOMAIN_ID")) or 1),
            "marketplace_id": str(os.getenv("SP_API_MARKETPLACE_ID", "ATVPDKIKX0DER")).strip() or "ATVPDKIKX0DER",
            "currency": "USD",
        },
        "gate_decision": gate_decision,
        "key_metrics": key_metrics,
        "raw_history": {
            "buy_box_price_history": buy_box_price_history,
            "buy_box_seller_history": buy_box_seller_history,
        },
        "fee_context": fee_context,
        "errors": [],
        "manual_label": {
            "my_decision": None,
            "gate_correct": None,
            "my_reasoning": None,
            "gate_missed": None,
            "expected_llm_override": None,
        },
    }
    return eval_json
