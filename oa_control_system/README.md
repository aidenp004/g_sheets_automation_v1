# OA Control System

Deterministic Amazon OA evaluator using Google Sheets + Keepa, with SP-API fee-based profitability and an optional LLM review layer for specific edge-case gate outcomes.

## What Changed Recently

- Added robust 21-day Buy Box range computation from Keepa history (`src/buy_box_range.py`) using percentile bands instead of min/max.
- Added competitive seller logic from Keepa live offers with FBA/FBM split, seller dedupe, near-band competitiveness, and weighted stock pressure fields.
- Added Keepa-only reverse-sourcing workflow based on Product Finder profiles (`seller_filter` mode) writing to `FinderQualified` / `FinderRejects`.
- Added SP-API Product Fees integration (`src/sp_api_client.py`) so ROI % and Margin % are computed by the system (not VA-entered).
- Added profitability helper module (`src/profitability.py`) using sell-price midpoint from `Buy Box Range (Current)` and inbound shipping from Keepa package weight.
- Added eval-case export pipeline (`export_eval_case.py`) writing JSON cases to `/evals`.
- Added two-stage LLM review pipeline (`src/llm_review.py`) with full run logging in `/llm_logs`.

## Workflows

### 1) `sheet1` mode (primary lead evaluator)

Command:

```powershell
py oa_control_flow.py
```

Flow:

- Finds first row where `EVALUATE=YES` and `Decision` is blank.
- Pulls Keepa metrics for the ASIN.
- Computes SP-API fee context and profitability fields:
- `Estimated Sell Price (Mid BB)`, `Amazon Fees Total`, `Referral Fee`, `FBA Fulfillment Fee`
- `Inbound Shipping Fee`, `Profit / Unit`, `ROI %`, `Margin %`
- Runs deterministic policy decisioning in `src/policy.py` (`evaluate_lead`).
- Passes decision through `llm_decision_pipeline(...)` for selective override/review.
- Writes outputs back to the same row.
- Writes `LLM Review Output` with the latest LLM1/LLM2 payload summary.
- Exports a complete eval JSON to `/evals`.

### 2) `seller_filter` mode (Product Finder reverse sourcing)

Command:

```powershell
py oa_control_flow.py --mode seller_filter --seller_id <SELLER_ID> --profile default_us --limit 500
```

Flow:

- Loads Product Finder profile from `config/finder_profiles.json`.
- Injects `{{seller_id}}` into `selection_template`.
- Pulls and dedupes ASIN candidates from Keepa Product Finder.
- Applies cooldown across `FinderQualified` + `FinderRejects`.
- Fetches detailed Keepa metrics and runs `evaluate_keepa_only(...)`.
- Appends results to `FinderQualified` and `FinderRejects`.
- Handles Keepa 429 by waiting and retrying the same ASIN (does not auto-reject 429).

## LLM Review Layer

All LLM logic lives in `src/llm_review.py`.

Prompt files:

- `prompts/llm1_system_prompt.txt`
- `prompts/llm2_system_prompt.txt`

Log output:

- `llm_logs/llm_run_XXXX.json`

### Models

- LLM1 review analyst: `claude-sonnet-4-20250514`
- LLM2 verifier/judge: `claude-haiku-4-5-20251001`

### Runtime behavior

- `oa_control_flow.py` calls:

```python
decision = llm_decision_pipeline(decision, keepa_metrics, row_data)
```

- Hard bypass returns deterministic decision unchanged for decisions already `BUY`/`TEST`, or reasons containing `IP Clean`, `ASIN is gated`, `Exact Match`, or `Est Sales / Month`.
- Activation occurs only for `DEFER + needs_human_review=True`, or `REJECT` with reasons containing `Buy Box 90d Low` or `Offer Count Delta`.
- LLM1 receives cleaned eval JSON (history entries with `keepa_minutes` removed).
- LLM2 receives full raw eval JSON plus LLM1 output (with `keepa_minutes` included).
- Final decision thresholds:
- `overall_verified=True` and `confidence > 0.80`: use LLM decision, no human review flag.
- `overall_verified=True` and `0.50 <= confidence <= 0.80`: use LLM decision, keep human review flag.
- Otherwise: keep deterministic decision and force human review.
- Any LLM failure falls back safely to deterministic decision with `needs_human_review=True`.

## Product Finder Profile Config

File:

- `config/finder_profiles.json`

Schema per profile:

- `selection_template` (object sent as Keepa `selection`)
- `max_pages` (int)
- `candidate_limit` (int)
- `detail_limit` (int)
- `cooldown_days` (int)

Placeholder:

- `{{seller_id}}` is replaced with normalized seller ID.

## Key Modules

- `oa_control_flow.py`: main runner + mode dispatch + Sheet1 pipeline
- `src/policy.py`: deterministic policy engine (`evaluate_lead`, `evaluate_keepa_only`)
- `src/keepa_client.py`: Keepa metrics, range/seller/share extraction, Product Finder sourcing
- `src/seller_filter_runner.py`: isolated seller_filter orchestration
- `src/profitability.py`: midpoint parsing, inbound shipping, profit/ROI/margin math
- `src/sp_api_client.py`: SP-API fees estimates with retry + in-run cache
- `src/llm_review.py`: LLM review/verify/log orchestration
- `export_eval_case.py`: JSON eval export utility

## Environment Variables

Core:

- `SHEET_ID`
- `WORKSHEET_NAME` (typically `Sheet1`)
- `GOOGLE_SERVICE_ACCOUNT_JSON`
- `KEEPA_API_KEY`
- `KEEPA_DOMAIN_ID` (US = `1`)
- `POLICY_VERSION`

Seller filter:

- `FINDER_PROFILES_PATH` (optional, defaults to `config/finder_profiles.json`)

SP-API / profitability:

- `SP_API_LWA_CLIENT_ID`
- `SP_API_LWA_CLIENT_SECRET`
- `SP_API_REFRESH_TOKEN`
- `SP_API_MARKETPLACE_ID` (default `ATVPDKIKX0DER`)
- `SP_API_ENDPOINT` (default `https://sellingpartnerapi-na.amazon.com`)
- `INBOUND_SHIPPING_USD_PER_LB` (default `0.77`)

LLM review:

- `ANTHROPIC_API_KEY`

## Install

```powershell
py -m pip install --upgrade pip
py -m pip install gspread oauth2client requests pandas python-dotenv
```

## Eval Export Utility

Command:

```powershell
py export_eval_case.py <ASIN> --landed-cost <OPTIONAL_COST>
```

Behavior:

- Runs the ASIN through Keepa + policy (Keepa-only path in export utility).
- Computes fee context with SP-API and profitability helpers when possible.
- Writes `eval_XXXX_<ASIN>_<timestamp>.json` under `/evals`.

## Troubleshooting

- If LLM output fails, inspect latest `llm_logs/llm_run_XXXX.json` for explicit failure reason.
- If `ANTHROPIC_API_KEY` is missing/invalid, pipeline falls back to deterministic decision and marks human review.
- If Keepa fetch fails for a row, decision flow degrades safely and records Keepa error reason.
- If SP-API fee estimate fails, row is marked for human review and BUY is blocked by deterministic gates.

## Notes

- Sheet column order changes are safe as long as header names remain unchanged.
- `sheet1` and `seller_filter` are intentionally isolated workflows.
- Do not commit real secrets in `.env` or `.env.example`.
