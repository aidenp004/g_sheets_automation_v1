from __future__ import annotations

import re

INBOUND_SHIPPING_USD_PER_LB = 0.77
GRAMS_PER_POUND = 453.592


def parse_buy_box_range_midpoint(value: str | None) -> float | None:
    raw = (value or "").strip()
    if not raw:
        return None
    matches = re.findall(r"\d+(?:\.\d+)?", raw.replace(",", ""))
    if not matches:
        return None
    try:
        prices = [float(matches[0])]
        if len(matches) >= 2:
            prices.append(float(matches[1]))
    except ValueError:
        return None
    midpoint = sum(prices) / len(prices)
    if midpoint <= 0:
        return None
    return midpoint


def compute_inbound_shipping_fee(
    package_weight_grams: float,
    rate_per_lb: float = INBOUND_SHIPPING_USD_PER_LB,
) -> float | None:
    if package_weight_grams <= 0 or rate_per_lb < 0:
        return None
    weight_lbs = package_weight_grams / GRAMS_PER_POUND
    return round(weight_lbs * rate_per_lb, 2)


def compute_profitability_metrics(
    estimated_sell_price: float,
    landed_cost_per_unit: float,
    amazon_fees_total: float,
    inbound_shipping_fee: float,
) -> tuple[float, float, float]:
    profit_per_unit = (
        estimated_sell_price
        - landed_cost_per_unit
        - amazon_fees_total
        - inbound_shipping_fee
    )
    roi_percent = (profit_per_unit / landed_cost_per_unit) * 100.0
    margin_percent = (profit_per_unit / estimated_sell_price) * 100.0
    return round(profit_per_unit, 2), round(roi_percent, 2), round(margin_percent, 2)
