# NOTES: Context, decisions, and assumptions

## Key decisions made in chat
- Platform: Google Sheets as state + UI; Python as deterministic policy engine.
- Workflow: per-row on demand (A). Script evaluates first row where `EVALUATE=YES` and `Decision` blank.
- Keepa API: available and will be used for market/time-series signals.
- SellerAmp API: not available; ROI/Margin are trusted VA inputs.
- “Competition” input: must reflect OA offers near buy box; keepa cannot reliably infer this in v1.
  - Solution: manual column `Competitive Sellers Near BB (Manual)`.

## TEST buy definition
- Small buy when uncertain: typically 6–10 units.
- Defaults:
  - MED risk -> 8 units
  - HIGH risk -> 6 units

## BUY sizing definition
You stated:
- Compare sellers near buy box to sales/month and buy ~45 days of stock.
- Implemented as:
  - `qty = ceil((sales_month / competitive_sellers_near_bb) * 1.5)`

## Criteria summary (authoritative)
Supplier:
- Verified and reliable; invoices available if needed.

Exact match:
- UPC, size, color, pack count, region, model all match.
- No bundling errors or substitutions.

Profit:
- Margin >= 12% (>= 15% for apparel)
- ROI >= 20% (>= 30% for apparel in checklist version)
- Use current buy box as reference; landed cost includes shipping, tax, prep, fees.

Sales velocity:
- >= 30 units/month for child ASIN.

IP risk:
- No alerts. If unsure -> do not buy.

Competition trend:
- Offer count not spiking hard.
- Reject if +10 sellers in 14 days unless sales/month rising similarly.

Amazon presence:
- Reject if Amazon holds buy box/in stock > 50% of last 90 days (unless evidence supports sharing).

Price stability:
- 90-day buy box average not collapsing.
- At 90-day low, must be break-even or better.

Auto-reject red flags:
- mismatches, variation traps, gated, recent BB below breakeven, brand-only with kickoffs, counterfeit signals.

## What we validated already
- Google Sheets API auth working via service account.
- Test read printed: `[['LeadID ']]` (header read).

## Open questions to finalize soon
- Keepa parsing details for:
  - Est Sales / Month heuristic
  - Buy box stability heuristic
  - Amazon dominance (percent calculation)
  - Offer count delta 14d calculation details
- How to represent buy box range input (string parsing vs ignore)
- Whether to auto-reset EVALUATE to NO after evaluation (recommended)

## Compatibility
- Windows + Python 3.13 noted in your logs.
- Use `py -m pip ...` to avoid module mismatch.

## Safety / governance
- No auto-calibration. Proposals only; CEO approves via sheet.
- No web agents in v1.
- No automatic purchasing.

