from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class KeepaMetrics:
    est_sales_month: int | None
    offer_count_delta_14d: int | None
    buy_box_90d_avg: float | None
    buy_box_90d_low: float | None
    amazon_buy_box_pct_90d: float | None
    buy_box_stability: str
    buy_box_90d_low_timestamp: int | None = None
    brand: str | None = None
    buy_box_range_current: str | None = None
    buy_box_range_21d_low: float | None = None
    buy_box_range_21d_high: float | None = None
    buy_box_samples_21d: int = 0
    buy_box_total_events_21d: int = 0
    buy_box_relative_spread_21d: float | None = None
    buy_box_range_issue: str | None = None
    current_buy_box_price: float | None = None
    competitive_sellers_near_bb: int | None = None
    competitive_fba_sellers_near_bb: int | None = None
    competitive_fbm_sellers_near_bb: int | None = None
    competitive_weighted_stock_units: float | None = None
    competitive_stock_known_sellers: int | None = None
    competitive_stock_total_sellers: int | None = None
    competitive_allowed_delta: float | None = None
    competitive_ceiling_price: float | None = None
    competitive_debug: str | None = None
    competitive_issue: str | None = None
    buy_box_fba_share_90d: float | None = None
    buy_box_fbm_share_90d: float | None = None
    package_weight_grams: float | None = None
    buy_box_price_history: list[tuple[int, float]] = field(default_factory=list)
    buy_box_seller_history: list[tuple[int, str]] = field(default_factory=list)
    missing_fields: list[str] = field(default_factory=list)
    source_error: str | None = None


@dataclass(frozen=True)
class PolicyDecision:
    decision: str
    recommended_qty: int
    downside_risk: str
    needs_human_review: bool
    reasons: list[str]
    audit_fields: dict[str, str] = field(default_factory=dict)

    def reasons_text(self) -> str:
        return "; ".join(self.reasons) if self.reasons else "No reasons recorded."


@dataclass(frozen=True)
class FilterResult:
    qualified: bool
    decision: str
    reason: str
    recommended_qty: int | None = None
    audit_fields: dict[str, str] = field(default_factory=dict)
