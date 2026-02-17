# SPEC: OA Control System (Internal Cybernetic Loop)

## 1) Purpose
Build a deterministic, auditable decision-control loop for Amazon Online Arbitrage (OA) that:
- Converts a lead into an evaluated decision (BUY/TEST/DEFER/REJECT),
- Sizes purchases using a consistent, bounded risk method,
- Prevents rule-slippage under lead scarcity,
- Incorporates Keepa time-series signals for downside risk,
- Records rationale and supports later feedback proposals requiring explicit CEO approval.

## 2) Non-goals (v1)
- No autonomous purchasing.
- No automatic rule calibration or upgrades without CEO approval.
- No SellerAmp API integration (ROI/Margin are trusted VA inputs).
- No screenshot parsing required for v1 (Keepa API preferred).
- No full app UI; Google Sheets is the cockpit.

## 3) System model (cybernetic mapping)
- **State:** Lead Sheet + InventoryBatch Sheet + Policy Settings.
- **Decide:** Python policy engine.
- **Act:** Human execution (you / VA).
- **Feedback:** Outcomes recorded; proposals created; CEO approves changes.

## 4) Decision gates (in order)
### Gate 0 — Offer validity (landed cost truth)
- Promo/discount assumptions and landed cost surprises are handled as recomputation:
  - If `Landed Cost / Unit`, `ROI %`, or `Margin %` end up below thresholds → reject.
(For v1, VA supplies the finalized all-in landed cost, ROI, and margin; the gate is simply threshold checks.)

### Gate 1 — Sellability: listing integrity & eligibility (hard blocks)
Hard blocks:
- Exact Match Verified != YES → REJECT (unless REVIEW → Needs Human Review)
- Gated == YES → REJECT
- IP Clean == NO → REJECT (UNKNOWN → Needs Human Review)
- Supplier Verified != YES → Needs Human Review or REJECT depending on policy setting

### Gate 2 — Demand and market structure
- Est Sales / Month >= 30 required
- Variations: user requirement is “confirm child actually sells”; v1 approximates via Keepa-derived estimate + manual verification as needed.

### Gate 3 — Price downside risk (timing & sizing)
Signals:
- Offer Count Δ (14d) and Buy Box Stability
- Price stability: Buy Box 90d Low must be >= break-even floor implied by criteria
- Seasonality can be added later as a manual modifier; not in v1

Output:
- Downside Risk = LOW / MED / HIGH
- Decision = BUY / TEST / DEFER based on downside risk and any “Needs Human Review” flags

## 5) User criteria (hard thresholds)
### Profitability
- Margin >= 12% (Apparel: >= 15%)
- ROI >= 20% (Apparel: >= 30% per checklist)
- Use current buy box (range) as reference; VA inputs ROI/margin based on current situation.

### Sales velocity
- Est Sales / Month >= 30

### IP risk
- No IP alerts; if unsure, do not buy (UNKNOWN => Needs Human Review or reject)

### Competition trend
- Reject if Offer Count Δ (14d) > +10, unless sales/month is rising similarly (v1: add a manual override flag or “Needs Human Review” when offer spike present)

### Amazon presence
- Reject if Amazon holds buy box / in-stock > 50% of last 90 days unless evidence supports buy box sharing and competitiveness.
(v1 implementation uses Keepa-derived Amazon Buy Box % (90d) > 50 as hard block.)

### Price stability
- No straight-line price drops (heuristic via Keepa; v1 approximations)
- Profit at 90-day low price must be >= break-even. (For v1, we approximate via ROI/Margin thresholds at low price by using Keepa 90d Low and comparing to a derived floor. If we cannot compute accurately due to missing fee breakdown, we set Needs Human Review.)

## 6) Red flags (auto-reject)
- Listing mismatch and variation traps (Exact Match Verified NO)
- Brand gated and cannot be ungated quickly (Gated YES)
- Recent buy box below breakeven >20% last 30 days (v2+ with time-series parsing)
- Brand-owned listing kick-offs or counterfeit signals (v2+: manual flags)

## 7) Quantity sizing
### TEST buy
- Default 8 units
- If Downside Risk HIGH → 6 units
- If Needs Human Review YES → system may set TEST or DEFER depending on severity

### BUY quantity (normal allocation)
User’s rule:
- Determine competitive sellers near buy box (OA offers near the price).
- `qty = ceil((EstSalesMonth / CompetitiveSellersNearBB) * 1.5)`
- Cap: 50 units (v1 safety cap; adjustable)

Notes:
- Competitive sellers near BB is a manual input in v1:
  - Column: `Competitive Sellers Near BB (Manual)`

## 8) Sheets schema
### Lead Sheet required columns
See README.md; this spec defines behavior and allowed values.

Allowed enums:
- EVALUATE: YES/NO
- Supplier Verified: YES/NO/REVIEW
- Exact Match Verified: YES/NO/REVIEW
- Gated: YES/NO/UNKNOWN
- IP Clean: YES/NO/UNKNOWN
- Apparel?: YES/NO
- Buy Box Stability: STABLE/UNSTABLE
- Decision/Final Decision: REJECT/DEFER/TEST/BUY
- Downside Risk: LOW/MED/HIGH
- Needs Human Review: YES/NO

### InventoryBatch (later)
One row per ASIN purchase batch.

## 9) Keepa integration (v1 requirements)
Inputs:
- ASIN
- Keepa API key

Outputs to fill:
- Est Sales / Month (best available approximation)
- Offer Count Δ (14d)
- Buy Box 90d Avg
- Buy Box 90d Low
- Amazon Buy Box % (90d) or derived dominance
- Buy Box Stability (heuristic)

If Keepa API cannot reliably provide a metric, write:
- `Needs Human Review = YES`
- Put precise missing-metric reason in `Reasons`

## 10) Governance
- No automatic calibration or upgrade.
- System may create proposals (later) in `Calibration Proposals` and `Upgrade Proposals` tabs.
- Changes are applied only when CEO marks `Approve=YES`, and policy version increments.

