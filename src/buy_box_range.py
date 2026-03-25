from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BuyBoxRangeResult:
    low: float | None
    high: float | None
    range_text: str | None
    sample_count: int
    total_events: int
    stability: str
    relative_spread: float | None
    reason: str | None


def compute_buy_box_range(
    raw_history: list[tuple[int, float]],
    now_keepa_minutes: int,
    window_days: int = 21,
    lower_percentile: float = 20.0,
    upper_percentile: float = 80.0,
    min_samples: int = 4,
    max_suppressed_share: float = 0.60,
    stable_spread_threshold: float = 0.25,
    stable_coverage_override: float = 0.85,
) -> BuyBoxRangeResult:
    if not raw_history:
        return BuyBoxRangeResult(
            low=None,
            high=None,
            range_text=None,
            sample_count=0,
            total_events=0,
            stability="UNSTABLE",
            relative_spread=None,
            reason="buy box suppressed or unavailable in last 21 days",
        )

    start = now_keepa_minutes - (window_days * 24 * 60)
    end = now_keepa_minutes
    points = sorted(raw_history, key=lambda pair: pair[0])

    state_at_start: float | None = None
    for ts, value_cents in points:
        if ts <= start:
            state_at_start = value_cents
            continue
        break

    window_events = [(ts, value_cents) for ts, value_cents in points if start < ts <= end]
    total_events = len(window_events)

    durations: list[float] = []
    prices: list[float] = []

    current_ts = start
    current_value = state_at_start
    for ts, next_value in window_events:
        if ts > current_ts and current_value is not None and current_value > 0:
            durations.append(float(ts - current_ts))
            prices.append(current_value / 100.0)
        current_ts = ts
        current_value = next_value

    if end > current_ts and current_value is not None and current_value > 0:
        durations.append(float(end - current_ts))
        prices.append(current_value / 100.0)

    valid_minutes = sum(durations)
    total_minutes = float(max(1, end - start))
    coverage_ratio = valid_minutes / total_minutes
    suppressed_share = 1.0 - (valid_minutes / total_minutes)
    sample_count = len(prices)

    if suppressed_share > max_suppressed_share:
        suppressed_pct = round(suppressed_share * 100.0, 2)
        return BuyBoxRangeResult(
            low=None,
            high=None,
            range_text=None,
            sample_count=sample_count,
            total_events=total_events,
            stability="UNSTABLE",
            relative_spread=None,
            reason=f"buy box suppressed for most of last 21 days ({suppressed_pct:.2f}% suppressed)",
        )
    if not prices or not durations:
        return BuyBoxRangeResult(
            low=None,
            high=None,
            range_text=None,
            sample_count=sample_count,
            total_events=total_events,
            stability="UNSTABLE",
            relative_spread=None,
            reason="buy box suppressed or unavailable in last 21 days",
        )
    if sample_count < min_samples and coverage_ratio < stable_coverage_override:
        return BuyBoxRangeResult(
            low=None,
            high=None,
            range_text=None,
            sample_count=sample_count,
            total_events=total_events,
            stability="UNSTABLE",
            relative_spread=None,
            reason=f"insufficient buy box samples in last 21 days ({sample_count} < {min_samples})",
        )

    low = round(_weighted_percentile(prices, durations, lower_percentile), 2)
    high = round(_weighted_percentile(prices, durations, upper_percentile), 2)
    if high < low:
        low, high = high, low

    midpoint = (low + high) / 2.0 if (low is not None and high is not None) else 0.0
    relative_spread = ((high - low) / midpoint) if midpoint > 0 else None
    stability = "STABLE" if (relative_spread is not None and relative_spread <= stable_spread_threshold) else "UNSTABLE"

    return BuyBoxRangeResult(
        low=low,
        high=high,
        range_text=f"{low:.2f}-{high:.2f}",
        sample_count=sample_count,
        total_events=total_events,
        stability=stability,
        relative_spread=relative_spread,
        reason=None,
    )


def _weighted_percentile(values: list[float], weights: list[float], percentile: float) -> float:
    if not values or not weights or len(values) != len(weights):
        return 0.0

    pairs = sorted(zip(values, weights), key=lambda pair: pair[0])
    total_weight = sum(weight for _, weight in pairs)
    if total_weight <= 0:
        return pairs[-1][0]

    quantile = max(0.0, min(100.0, percentile)) / 100.0
    threshold = total_weight * quantile
    running = 0.0
    for value, weight in pairs:
        running += max(0.0, weight)
        if running >= threshold:
            return value
    return pairs[-1][0]
