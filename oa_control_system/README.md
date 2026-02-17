# OA Control System (Internal Cybernetic Loop)

Internal decision-control system for Amazon Online Arbitrage (OA).  
One row per lead in Google Sheets. A Python evaluator reads the first row marked `EVALUATE=YES` with blank `Decision`, pulls market signals from Keepa API, applies deterministic gates (your criteria), and writes back `Decision`, `Recommended Qty`, `Downside Risk`, and `Reasons`.

This project is intentionally **control-first**:
- **State** lives in a Google Sheet (human-legible + auditable).
- **Policy** is deterministic Python (no black-box authority).
- **Humans** execute purchases.
- **Feedback** is recorded in a separate `InventoryBatch` sheet and produces **proposals** (upgrade + calibration) that require CEO approval.

## Current status (where this repo starts)
- Google Sheets connection via Service Account is confirmed.
- A test read returned `[['LeadID ']]` (header read), meaning auth + sheet access works.
- Next step is to implement row selection (A: first row where `EVALUATE=YES` and `Decision` blank) and then Keepa integration + gates.

## What this system does (v1)
Given a lead row, it will:
1. Pull Keepa signals (price history, buy box history, offer count history, sales rank history as available).
2. Apply gates based on your criteria:
   - Hard blocks (mismatch, gated, IP not clean, etc.)
   - Profitability + demand thresholds
   - Amazon dominance constraint
   - Offer spike rule (+10 sellers in 14 days unless sales rising similarly)
   - Price stability rule (90-day low must be break-even or better)
3. Compute downside risk (LOW/MED/HIGH).
4. Output:
   - `Decision` (REJECT / DEFER / TEST / BUY)
   - `Recommended Qty`
   - `Reasons`
   - `Needs Human Review`
   - `Decision Timestamp`
   - `Policy Version`

## Definitions
- **TEST** buy: small quantity (6–10 units) used when there is uncertainty or a red flag.
  - Default: 8 units
  - If downside risk HIGH: 6 units
- **BUY** quantity: based on your logic:
  - Use monthly sales and competitive sellers near buy box:
    - `qty = ceil((EstSalesMonth / CompetitiveSellersNearBB) * 1.5)`
  - 1.5 corresponds to buying ~45 days of stock (45/30).

## Lead Sheet (minimum columns, v1)
These are the agreed minimum columns (ordered; see SPEC.md for details):

Manual inputs:
- LeadID
- EVALUATE (YES/NO)
- ASIN
- Buy URL
- Supplier Verified (YES/NO/REVIEW)
- Exact Match Verified (YES/NO/REVIEW)
- Gated (YES/NO/UNKNOWN)
- IP Clean (YES/NO/UNKNOWN)
- Landed Cost / Unit (all-in)
- Buy Box Range (Current) (e.g., 29.99–34.99)
- ROI %
- Margin %
- Apparel? (YES/NO)

Auto-filled from Keepa:
- Est Sales / Month
- Competitive Sellers Near BB (Manual)  <-- manually entered count (near BB), v1
- Offer Count Δ (14d)
- Buy Box 90d Avg
- Buy Box 90d Low
- Amazon Buy Box % (90d)
- Buy Box Stability (STABLE/UNSTABLE)

Outputs:
- Decision
- Recommended Qty
- Downside Risk (LOW/MED/HIGH)
- Reasons
- Needs Human Review (YES/NO)
- Final Decision
- Final Qty
- BatchID
- Decision Timestamp
- Policy Version

Optional transparency columns (recommended for spike-path auditability):
- Spike Threshold
- Spike Share %
- Effective Spike Units / Mo
- Spike ROI %
- Spike Margin %
- Spike Windows Count
- Spike Path Qualified (YES/NO)

Optional transparency columns for current buy box range (21d):
- BuyBoxRange21d_Low
- BuyBoxRange21d_High
- BuyBoxSamples21d
- BuyBoxRange21d_SpreadPct

## InventoryBatch Sheet (minimal concept; built later)
Separate tab, one row per ASIN purchase batch, used for feedback + upgrade/calibration proposals (CEO approval required).

## Setup
### 1) Python environment (Windows / PowerShell)
Use `py` to ensure installs go into the same interpreter you're running:
```powershell
py -m pip install --upgrade pip
py -m pip install gspread oauth2client requests pandas
```

### 2) Google Sheets API + Service Account
1. Create Google Cloud project
2. Enable Google Sheets API (and Drive API if needed)
3. Create Service Account + JSON key
4. Share your Google Sheet with the service account `client_email` as Editor

### 3) Secrets & Credentials
**⚠️ CRITICAL: Do not commit credentials to git.**

1. **Google Service Account JSON**
   - Download the JSON key from Google Cloud Console
   - Save locally as `service_account.json` (already in `.gitignore`)
   - Set env var: `GOOGLE_SERVICE_ACCOUNT_JSON=service_account.json`

2. **Keepa API Key**
   - Obtain from Keepa account settings
   - Set env var: `KEEPA_API_KEY=your_key_here`

3. **Copy `.env.example` to `.env`** and populate with your actual values:
   ```powershell
   cp .env.example .env
   ```
   - Edit `.env` with your real credentials (this file is in `.gitignore` and won't be committed)
   - **Never** commit `.env` or `service_account.json`

### 4) Run
```powershell
py oa_control_flow.py
```

## Roadmap
See TASKS.md for milestones and next actions.
