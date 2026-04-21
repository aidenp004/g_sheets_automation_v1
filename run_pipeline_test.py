"""
One-shot pipeline test against eval_0054 (B0DPGJV2DW, DEFER case).

Steps:
  1. Load eval_0054 and reconstruct KeepaMetrics + PolicyDecision.
  2. Build row_data from fee_context fields.
  3. Patch _post_anthropic_messages to log payload sizes BEFORE the real call.
  4. Run llm_decision_pipeline.
  5. Print the newest llm_log file.

Run:
    python run_pipeline_test.py
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(dotenv_path=".env")

# ── project root on path ───────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent))

from src.models import KeepaMetrics, PolicyDecision
import src.llm_review as llm_mod

EVAL_PATH = Path("evals/eval_0054_B0DPGJV2DW_20260409_002632.json")


# ── 1. Load eval ───────────────────────────────────────────────────────────────
eval_data = json.loads(EVAL_PATH.read_text(encoding="utf-8"))
km_raw = eval_data["key_metrics"]
rh = eval_data["raw_history"]
fc = eval_data["fee_context"]
gd = eval_data["gate_decision"]

# ── 2. Reconstruct KeepaMetrics ───────────────────────────────────────────────
buy_box_price_history: list[tuple[int, float]] = [
    (entry["keepa_minutes"], entry["price_usd"])
    for entry in rh.get("buy_box_price_history", [])
]
buy_box_seller_history: list[tuple[int, str]] = [
    (entry["keepa_minutes"], entry.get("seller_type", ""))
    for entry in rh.get("buy_box_seller_history", [])
]

keepa = KeepaMetrics(
    est_sales_month=km_raw.get("est_sales_month"),
    offer_count_delta_14d=km_raw.get("offer_count_delta_14d"),
    buy_box_90d_avg=km_raw.get("buy_box_90d_avg"),
    buy_box_90d_low=km_raw.get("buy_box_90d_low"),
    amazon_buy_box_pct_90d=km_raw.get("amazon_buy_box_pct_90d"),
    buy_box_stability=km_raw.get("buy_box_stability") or "UNKNOWN",
    buy_box_90d_low_timestamp=km_raw.get("buy_box_90d_low_timestamp"),
    brand=km_raw.get("brand"),
    buy_box_range_current=km_raw.get("buy_box_range_current"),
    buy_box_range_21d_low=km_raw.get("buy_box_range_21d_low"),
    buy_box_range_21d_high=km_raw.get("buy_box_range_21d_high"),
    buy_box_samples_21d=km_raw.get("buy_box_samples_21d") or 0,
    buy_box_total_events_21d=km_raw.get("buy_box_total_events_21d") or 0,
    buy_box_relative_spread_21d=km_raw.get("buy_box_relative_spread_21d"),
    buy_box_range_issue=km_raw.get("buy_box_range_issue"),
    current_buy_box_price=km_raw.get("current_buy_box_price"),
    competitive_sellers_near_bb=km_raw.get("competitive_sellers_near_bb"),
    competitive_fba_sellers_near_bb=km_raw.get("competitive_fba_sellers_near_bb"),
    competitive_fbm_sellers_near_bb=km_raw.get("competitive_fbm_sellers_near_bb"),
    competitive_weighted_stock_units=km_raw.get("competitive_weighted_stock_units"),
    competitive_stock_known_sellers=km_raw.get("competitive_stock_known_sellers"),
    competitive_stock_total_sellers=km_raw.get("competitive_stock_total_sellers"),
    competitive_allowed_delta=km_raw.get("competitive_allowed_delta"),
    competitive_ceiling_price=km_raw.get("competitive_ceiling_price"),
    competitive_debug=km_raw.get("competitive_debug"),
    competitive_issue=km_raw.get("competitive_issue"),
    buy_box_fba_share_90d=km_raw.get("buy_box_fba_share_90d"),
    buy_box_fbm_share_90d=km_raw.get("buy_box_fbm_share_90d"),
    package_weight_grams=km_raw.get("package_weight_grams"),
    buy_box_price_history=buy_box_price_history,
    buy_box_seller_history=buy_box_seller_history,
    missing_fields=km_raw.get("missing_fields") or [],
    source_error=km_raw.get("source_error"),
)

# ── 3. Reconstruct PolicyDecision ─────────────────────────────────────────────
decision = PolicyDecision(
    decision=gd["decision"],
    recommended_qty=gd.get("recommended_qty", 0),
    downside_risk=gd.get("downside_risk", "MED"),
    needs_human_review=gd.get("needs_human_review", True),
    reasons=gd.get("reasons", []),
    audit_fields={k: str(v) for k, v in gd.get("audit_fields", {}).items()},
)

# ── 4. Build row_data ──────────────────────────────────────────────────────────
row_data: dict = {
    "ASIN": eval_data["asin"],
    "Landed Cost / Unit": str(fc.get("landed_cost_per_unit", "")),
    "Amazon Fees Total": str(fc.get("amazon_fees_total", "")),
    "ROI %": str(fc.get("roi_percent", "")),
    "Margin %": str(fc.get("margin_percent", "")),
}

# Strip the old LLM Review Output from audit_fields — it belongs to the
# previous run recorded in the eval file, not this test run.
clean_audit = {
    k: v for k, v in decision.audit_fields.items() if k != "LLM Review Output"
}
decision = PolicyDecision(
    decision=decision.decision,
    recommended_qty=decision.recommended_qty,
    downside_risk=decision.downside_risk,
    needs_human_review=decision.needs_human_review,
    reasons=decision.reasons,
    audit_fields=clean_audit,
)

# ── 5. Patch _post_anthropic_messages to log what gets sent ───────────────────
_original_post = llm_mod._post_anthropic_messages

def _patched_post(api_key, payload, route_name):
    messages = payload.get("messages", [])
    print(f"\n{'='*70}")
    print(f"[PAYLOAD AUDIT]  route={route_name!r}  model={payload.get('model')!r}")
    total_chars = 0
    for i, msg in enumerate(messages):
        content = msg.get("content", "")
        if isinstance(content, str):
            total_chars += len(content)
            print(f"  message[{i}] role={msg.get('role')!r}  content=str  chars={len(content)}")
            # Check for raw arrays
            _check_for_raw_arrays(content, route_name)
        elif isinstance(content, list):
            for j, block in enumerate(content):
                btype = block.get("type", "?")
                if btype == "text":
                    txt = block.get("text", "")
                    total_chars += len(txt)
                    print(f"  message[{i}].content[{j}] type=text  chars={len(txt)}")
                    _check_for_raw_arrays(txt, route_name)
                elif btype == "image":
                    src = block.get("source", {})
                    data = src.get("data", "")
                    print(f"  message[{i}].content[{j}] type=image  base64_chars={len(data)}")
                else:
                    print(f"  message[{i}].content[{j}] type={btype!r}")
    print(f"  Total text chars sent: {total_chars}")
    print(f"{'='*70}")
    return _original_post(api_key, payload, route_name)

def _check_for_raw_arrays(text: str, route_name: str) -> None:
    """Warn if raw 128-entry buy_box_price_history or buy_box_seller_history arrays appear."""
    suspects = ["buy_box_price_history", "buy_box_seller_history", "raw_history", "keepa_minutes"]
    for s in suspects:
        if s in text:
            print(f"  *** WARNING: raw array key {s!r} found in {route_name} payload! ***")

llm_mod._post_anthropic_messages = _patched_post

# ── 6. Run the pipeline ────────────────────────────────────────────────────────
print(f"\nRunning llm_decision_pipeline for ASIN {eval_data['asin']} ...")
print(f"Input decision: {decision.decision}  needs_human_review={decision.needs_human_review}")
print(f"Gate reasons: {decision.reasons}")
print(f"buy_box_price_history entries: {len(keepa.buy_box_price_history)}")

result = llm_mod.llm_decision_pipeline(decision, keepa, row_data)

print(f"\n{'='*70}")
print(f"RESULT  decision={result.decision!r}  needs_human_review={result.needs_human_review}")
print(f"reasons: {result.reasons}")
print(f"{'='*70}\n")

# ── 7. Print the newest log file ───────────────────────────────────────────────
logs_dir = Path("llm_logs")
log_files = sorted(logs_dir.glob("llm_run_*.json")) if logs_dir.exists() else []
if not log_files:
    print("No log files found.")
else:
    newest = log_files[-1]
    print(f"Newest log: {newest.name}")
    log = json.loads(newest.read_text(encoding="utf-8"))

    print(f"\ngraph_image_fetched: {log.get('graph_image_fetched')}")
    print("\n--- verified_facts keys ---")
    vf = log.get("verified_facts", {})
    print(list(vf.keys()))

    # Confirm no raw arrays in what the new pipeline logged (eval_json/verified_facts,
    # llm1_output, llm2_output). The final_decision audit_fields are excluded from
    # this check because they are copied verbatim from the input decision.
    new_pipeline_payload = {
        "eval_json": log.get("eval_json", {}),
        "llm1_output": log.get("llm1_output", {}),
        "llm2_output": log.get("llm2_output", {}),
    }
    new_pipeline_text = json.dumps(new_pipeline_payload)
    raw_suspects = ["buy_box_price_history", "buy_box_seller_history", "raw_history", "keepa_minutes"]
    for s in raw_suspects:
        if s in new_pipeline_text:
            print(f"  *** RAW ARRAY FOUND: {s!r} in verified_facts/llm outputs! ***")
        else:
            print(f"  OK: {s!r} not present in verified_facts or LLM outputs.")

    print("\n--- verified_facts (bb_90d) ---")
    print(json.dumps(vf.get("bb_90d", {}), indent=2))

    print("\n--- verified_facts (price_events) ---")
    for evt in vf.get("price_events", []):
        print(f"  {evt.get('event_id')}  type={evt.get('type')}  price={evt.get('price')}  ts={evt.get('timestamp')}  recovery_min={evt.get('recovery_minutes')}")

    print("\n--- llm1_output ---")
    l1 = log.get("llm1_output", {})
    print(json.dumps(l1, indent=2)[:2000])

    print("\n--- llm2_output ---")
    l2 = log.get("llm2_output", {})
    print(json.dumps(l2, indent=2)[:2000])

    print("\n--- final_decision ---")
    print(json.dumps(log.get("final_decision", {}), indent=2))
