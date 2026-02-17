# ARCHITECTURE: OA Control System

## Components
1) **Google Sheet (Cockpit / State Store)**
- `Leads` tab: one row per lead opportunity
- `InventoryBatch` tab: one row per ASIN batch (later)
- `PolicySettings` tab: thresholds/multipliers/version (later)
- `DecisionsLog` tab: append-only ledger of decisions (later)
- `Calibration Proposals` tab: suggested rule adjustments (later; CEO approval)
- `Upgrade Proposals` tab: suggested TEST→BUY upgrades (later; CEO approval)

2) **Python Policy Engine**
- Reads first row meeting:
  - `EVALUATE=YES`
  - `Decision` is blank
- Pulls Keepa metrics
- Applies deterministic gates and sizing
- Writes outputs back to the same row

3) **Keepa API Sensor**
- Provides structured time-series and aggregates:
  - price history (buy box, Amazon)
  - offers count history
  - sales rank history (as proxy for velocity)
  - buy box history/stability (as available)

4) **Human Actuator**
- You/VA executes the buy based on `Final Decision` and `Final Qty`.
- Manual checks remain for:
  - exact match verification
  - gating/IP edge cases
  - supplier legitimacy
  - “near buy box” seller count input

## Data flow (per-row on demand)
```
Lead Row (manual fields) -> Evaluate=YES
        |
        v
Python evaluator
  - fetch Keepa signals
  - apply gates
  - compute risk + qty
        |
        v
Lead Row outputs written (Decision, Recommended Qty, Reasons, etc.)
        |
        v
Human executes -> Batch created (later)
        |
        v
Outcomes in InventoryBatch -> Proposals (later) -> CEO approval -> PolicySettings update
```

## Determinism and auditability
- Decisions are reproducible given:
  - Lead row inputs
  - Keepa response snapshot (optionally stored)
  - Policy version settings
- Output always includes:
  - `Reasons`
  - `Decision Timestamp`
  - `Policy Version`

## Folder structure (suggested)
```
oa-control-system/
  README.md
  SPEC.md
  ARCHITECTURE.md
  TASKS.md
  NOTES.md
  oa_control_flow.py
  src/
    keepa_client.py
    policy.py
    sheets_client.py
    models.py
  .env.example
  .gitignore
```

## Security model
- Store Keepa key and Google creds outside git:
  - env vars or `.env` (not committed)
- Service account JSON should not be committed.
- No browser automation or purchasing automation in v1.

## Extensibility plan
- Add `DecisionsLog` for append-only history.
- Add `InventoryBatch` feedback tab.
- Add proposal tabs with CEO approval flow.
- Add optional “agent” components later:
  - screenshot interpretation (only to fill structured fields)
  - supplier trust scorecard extraction (only to fill structured fields)
