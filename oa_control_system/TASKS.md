# TASKS: OA Control System

## Milestone 0 — Repo scaffold (now)
- [ ] Create repo folder with docs (README/SPEC/ARCH/TASKS/NOTES)
- [ ] Add `.gitignore` (ignore `.env`, `*.json` service keys, etc.)
- [ ] Add `.env.example`

## Milestone 1 — Sheets evaluator skeleton (next)
Goal: Evaluate the first eligible lead row and write outputs (placeholders first).
- [ ] Implement `sheets_client.py` to:
  - auth with service account JSON
  - open sheet by ID
  - fetch headers
  - find first row where `EVALUATE=YES` and `Decision` blank
  - read row into dict
  - write outputs back by column name
- [ ] Implement `oa_control_flow.py` main runner

Acceptance:
- Running script prints which row/LeadID/ASIN is evaluated and writes `Decision Timestamp` + `Policy Version`.

## Milestone 2 — Keepa integration (v1 sensor)
Goal: Populate Keepa-derived fields and use them in policy.
- [ ] Implement `keepa_client.py`:
  - `get_product(asins: list[str]) -> raw json`
  - parse required fields for:
    - offer count history -> Offer Count Δ (14d)
    - buy box metrics (90d avg/low)
    - amazon share (90d)
    - buy box stability heuristic
    - estimated sales/month heuristic (best available)
- [ ] Store minimal Keepa snapshot? (optional)
- [ ] Write Keepa fields back to lead row

Acceptance:
- Lead row Keepa fields populate correctly for a known ASIN.

## Milestone 3 — Policy gates (deterministic)
Goal: Apply your criteria and output decision.
- [ ] Implement `policy.py`:
  - thresholds: ROI, margin, velocity, Amazon dominance
  - hard blocks: exact match, gated, IP clean
  - offer spike rule (+10 in 14 days -> risk up / possible DEFER)
  - downside risk classification
- [ ] Implement decision mapping:
  - REJECT for hard blocks
  - DEFER for missing inputs / needs review
  - TEST for medium/high risk when criteria pass
  - BUY for low risk when criteria pass

Acceptance:
- Given mocked inputs, policy returns expected decision and reasons.

## Milestone 4 — Quantity sizing
Goal: Compute recommended quantity consistently.
- [ ] Implement TEST quantity mapping:
  - default 8
  - high risk 6
- [ ] Implement BUY quantity mapping:
  - ceil((sales_month / competitive_sellers_near_bb) * 1.5)
  - cap at 50 (configurable)
- [ ] Ensure “Competitive Sellers Near BB (Manual)” required for BUY; otherwise DEFER + needs review

Acceptance:
- Quantity outputs match your examples.

## Milestone 5 — Logging (optional but recommended)
- [ ] Add `DecisionsLog` tab append-only
- [ ] Write one row per evaluation:
  - timestamp, leadid, asin, decision, qty, reasons, policy version, keepa summary

## Milestone 6 — InventoryBatch tab + feedback proposals (later)
- [ ] Create InventoryBatch schema
- [ ] Create Calibration Proposals tab
- [ ] Create Upgrade Proposals tab
- [ ] Generate proposals only; apply only if Approved by CEO

## Milestone 7 — UX tightening (later)
- [ ] Add “reset EVALUATE to NO after evaluation”
- [ ] Add “row lock” fields to prevent duplicate evaluations
- [ ] Add error handling + retries for Keepa API

