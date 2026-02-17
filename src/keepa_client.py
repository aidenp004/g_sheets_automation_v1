from __future__ import annotations

import time
from typing import Any

import requests

from src.buy_box_range import compute_buy_box_range
from src.models import KeepaMetrics

KEEPA_PRODUCT_URL = "https://api.keepa.com/product"
KEEPA_EPOCH_OFFSET_MINUTES = 21_564_000

CSV_COUNT_NEW = 11
CSV_BUY_BOX_SHIPPING = 18

US_AMAZON_SELLER_IDS = {"ATVPDKIKX0DER"}
CURRENT_RANGE_DAYS = 21
HISTORY_QUERY_DAYS = 30
ACTIVE_OFFER_MAX_AGE_MINUTES = 7 * 24 * 60


class KeepaError(RuntimeError):
    pass


class KeepaClient:
    def __init__(
        self,
        api_key: str,
        domain_id: int = 1,
        timeout_seconds: int = 25,
        max_retries: int = 3,
    ) -> None:
        self.api_key = api_key.strip()
        self.domain_id = domain_id
        self.timeout_seconds = timeout_seconds
        self.max_retries = max(1, max_retries)
        self._session = requests.Session()

        if not self.api_key:
            raise ValueError("KEEPA_API_KEY is empty.")

    def get_metrics(self, asin: str) -> KeepaMetrics:
        product = self.get_product(asin)
        stats = product.get("stats") if isinstance(product.get("stats"), dict) else {}
        now_keepa_minutes = _now_keepa_minutes()
        buy_box_price_history = self._extract_buy_box_price_history(product, window_days=90)
        buy_box_price_history_raw = self._extract_buy_box_price_history_raw(
            product,
            window_days=HISTORY_QUERY_DAYS,
        )
        buy_box_seller_history = self._extract_buy_box_seller_history(product)
        range_result = compute_buy_box_range(
            raw_history=buy_box_price_history_raw,
            now_keepa_minutes=now_keepa_minutes,
            window_days=CURRENT_RANGE_DAYS,
        )
        bb_reference_price = self._compute_buy_box_reference_price(stats, range_result)
        comp_count, comp_delta, comp_ceiling, comp_debug, comp_issue = self._compute_competitive_sellers(
            product=product,
            now_keepa_minutes=now_keepa_minutes,
            bb_reference_price=bb_reference_price,
        )

        est_sales_month = self._extract_est_sales_month(product, stats)
        offer_count_delta_14d = self._extract_offer_count_delta_14d(product)
        buy_box_90d_avg, buy_box_90d_low = self._extract_buy_box_90d_metrics(product, stats)
        amazon_buy_box_pct_90d = self._extract_amazon_buy_box_pct_90d(
            product=product,
            stats=stats,
            buy_box_seller_history=buy_box_seller_history,
        )
        buy_box_stability = range_result.stability

        missing_fields: list[str] = []
        if est_sales_month is None:
            missing_fields.append("Est Sales / Month")
        if offer_count_delta_14d is None:
            missing_fields.append("Offer Count Delta (14d)")
        if buy_box_90d_avg is None:
            missing_fields.append("Buy Box 90d Avg")
        if buy_box_90d_low is None:
            missing_fields.append("Buy Box 90d Low")
        if amazon_buy_box_pct_90d is None:
            missing_fields.append("Amazon Buy Box % (90d)")
        if not range_result.range_text:
            missing_fields.append("Buy Box Range (Current)")
        if comp_count is None:
            missing_fields.append("Competitive Sellers Near BB")
        if not buy_box_price_history:
            missing_fields.append("Buy Box Price History")

        return KeepaMetrics(
            est_sales_month=est_sales_month,
            offer_count_delta_14d=offer_count_delta_14d,
            buy_box_90d_avg=buy_box_90d_avg,
            buy_box_90d_low=buy_box_90d_low,
            amazon_buy_box_pct_90d=amazon_buy_box_pct_90d,
            buy_box_stability=buy_box_stability,
            buy_box_range_current=range_result.range_text,
            buy_box_range_21d_low=range_result.low,
            buy_box_range_21d_high=range_result.high,
            buy_box_samples_21d=range_result.sample_count,
            buy_box_total_events_21d=range_result.total_events,
            buy_box_relative_spread_21d=range_result.relative_spread,
            buy_box_range_issue=range_result.reason,
            current_buy_box_price=bb_reference_price,
            competitive_sellers_near_bb=comp_count,
            competitive_allowed_delta=comp_delta,
            competitive_ceiling_price=comp_ceiling,
            competitive_debug=comp_debug,
            competitive_issue=comp_issue,
            buy_box_price_history=buy_box_price_history,
            buy_box_seller_history=buy_box_seller_history,
            missing_fields=missing_fields,
        )

    def get_product(self, asin: str) -> dict[str, Any]:
        asin_clean = (asin or "").strip().upper()
        if not asin_clean:
            raise KeepaError("ASIN is blank.")

        params = {
            "key": self.api_key,
            "domain": self.domain_id,
            "asin": asin_clean,
            "stats": 90,
            "history": 1,
            "offers": 20,
            "stock": 1,
            "days": HISTORY_QUERY_DAYS,
        }

        response: requests.Response | None = None
        last_error: str | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self._session.get(
                    KEEPA_PRODUCT_URL,
                    params=params,
                    timeout=self.timeout_seconds,
                )
            except requests.RequestException as exc:
                last_error = f"Failed to call Keepa API: {exc}"
                if attempt < self.max_retries:
                    time.sleep(attempt * 2)
                    continue
                raise KeepaError(last_error) from exc

            if response.status_code in {429, 500, 502, 503, 504} and attempt < self.max_retries:
                last_error = f"Keepa API HTTP {response.status_code}: {response.text[:400]}"
                time.sleep(attempt * 2)
                continue
            break

        if response is None:
            raise KeepaError(last_error or "Keepa API request failed.")

        if response.status_code != 200:
            raise KeepaError(f"Keepa API HTTP {response.status_code}: {response.text[:400]}")

        try:
            payload = response.json()
        except ValueError as exc:
            raise KeepaError("Keepa API returned non-JSON response.") from exc

        if payload.get("error"):
            raise KeepaError(f"Keepa API error: {payload['error']}")

        products = payload.get("products")
        if not isinstance(products, list) or not products:
            raise KeepaError("Keepa API returned no products.")

        product = products[0]
        if not isinstance(product, dict):
            raise KeepaError("Keepa API returned invalid product format.")

        return product

    @staticmethod
    def _extract_est_sales_month(product: dict[str, Any], stats: dict[str, Any]) -> int | None:
        monthly_sold = _coerce_int(product.get("monthlySold"))
        if monthly_sold is not None and monthly_sold > 0:
            return monthly_sold

        sales_rank_drops_30 = _coerce_int(stats.get("salesRankDrops30"))
        if sales_rank_drops_30 is not None and sales_rank_drops_30 > 0:
            return sales_rank_drops_30

        return None

    def _extract_offer_count_delta_14d(self, product: dict[str, Any]) -> int | None:
        series = _extract_csv_series(product, CSV_COUNT_NEW)
        if not series:
            return None

        points = _parse_time_value_series(series, step=2, value_offset=1)
        if not points:
            return None

        now_keepa = _now_keepa_minutes()
        cutoff = now_keepa - (14 * 24 * 60)

        current_value = _latest_non_negative_value(points)
        if current_value is None:
            return None

        baseline = _value_at_or_before(points, cutoff)
        if baseline is None or baseline < 0:
            return None

        return int(current_value - baseline)

    def _extract_buy_box_90d_metrics(
        self, product: dict[str, Any], stats: dict[str, Any]
    ) -> tuple[float | None, float | None]:
        avg_90_cents = _extract_stats_array_value(stats.get("avg90"), CSV_BUY_BOX_SHIPPING)
        low_90_cents = _extract_min_interval_value(stats.get("minInInterval"), CSV_BUY_BOX_SHIPPING)

        # Fallback to raw history if stats arrays are not populated.
        if avg_90_cents is None or low_90_cents is None:
            series = _extract_csv_series(product, CSV_BUY_BOX_SHIPPING)
            points = _parse_keepa_price_with_shipping_series(series) if series else []
            points = [pair for pair in points if pair[1] >= 0]
            if points:
                cutoff = _now_keepa_minutes() - (90 * 24 * 60)
                in_window = [value for ts, value in points if ts >= cutoff]
                if not in_window:
                    in_window = [value for _, value in points]
                if in_window:
                    if avg_90_cents is None:
                        avg_90_cents = sum(in_window) / len(in_window)
                    if low_90_cents is None:
                        low_90_cents = min(in_window)

        return _cents_to_dollars(avg_90_cents), _cents_to_dollars(low_90_cents)

    def _extract_amazon_buy_box_pct_90d(
        self,
        product: dict[str, Any],
        stats: dict[str, Any],
        buy_box_seller_history: list[tuple[int, str]],
    ) -> float | None:
        buy_box_stats = stats.get("buyBoxStats")
        if isinstance(buy_box_stats, dict):
            amazon_pct = 0.0
            found_percentage = False
            for seller_id, summary in buy_box_stats.items():
                if not isinstance(summary, dict):
                    continue
                percentage_won = _coerce_float(summary.get("percentageWon"))
                if percentage_won is None:
                    continue
                if str(seller_id).strip().upper() in US_AMAZON_SELLER_IDS:
                    amazon_pct += percentage_won
                found_percentage = True
            if found_percentage:
                return round(max(0.0, min(100.0, amazon_pct)), 2)

        history = buy_box_seller_history
        if len(history) < 2:
            return None

        pairs = sorted(history, key=lambda item: item[0])
        start = _now_keepa_minutes() - (90 * 24 * 60)
        end = _now_keepa_minutes()

        current_seller = pairs[0][1]
        current_time = start
        amazon_minutes = 0
        covered_minutes = 0

        for ts, seller in pairs:
            if ts <= start:
                current_seller = seller
                continue
            if ts > end:
                break
            duration = ts - current_time
            if duration > 0:
                if current_seller in US_AMAZON_SELLER_IDS:
                    amazon_minutes += duration
                covered_minutes += duration
            current_time = ts
            current_seller = seller

        if end > current_time:
            tail_duration = end - current_time
            if current_seller in US_AMAZON_SELLER_IDS:
                amazon_minutes += tail_duration
            covered_minutes += tail_duration

        if covered_minutes <= 0:
            return None

        return round((amazon_minutes / covered_minutes) * 100.0, 2)

    @staticmethod
    def _extract_buy_box_price_history(
        product: dict[str, Any], window_days: int = 90
    ) -> list[tuple[int, float]]:
        series = _extract_csv_series(product, CSV_BUY_BOX_SHIPPING)
        points = _parse_keepa_price_with_shipping_series(series) if series else []
        if not points:
            return []

        cutoff = _now_keepa_minutes() - (window_days * 24 * 60)
        filtered: list[tuple[int, float]] = []
        for ts, value_cents in points:
            if ts < cutoff or value_cents < 0:
                continue
            value_usd = _cents_to_dollars(value_cents)
            if value_usd is None:
                continue
            filtered.append((ts, value_usd))
        return filtered

    @staticmethod
    def _extract_buy_box_price_history_raw(
        product: dict[str, Any], window_days: int = HISTORY_QUERY_DAYS
    ) -> list[tuple[int, float]]:
        series = _extract_csv_series(product, CSV_BUY_BOX_SHIPPING)
        points = _parse_keepa_price_with_shipping_series(series) if series else []
        if not points:
            return []

        cutoff = _now_keepa_minutes() - (window_days * 24 * 60)
        return [(ts, value) for ts, value in points if ts >= cutoff]

    @staticmethod
    def _extract_buy_box_seller_history(product: dict[str, Any]) -> list[tuple[int, str]]:
        raw = product.get("buyBoxSellerIdHistory")
        if not isinstance(raw, list) or len(raw) < 2:
            return []

        pairs: list[tuple[int, str]] = []
        cutoff = _now_keepa_minutes() - (90 * 24 * 60)
        for i in range(0, len(raw) - 1, 2):
            keepa_ts = _coerce_int(raw[i])
            seller_id = str(raw[i + 1]).strip().upper()
            if keepa_ts is None:
                continue
            if keepa_ts < cutoff:
                continue
            pairs.append((keepa_ts, seller_id))
        return pairs

    @staticmethod
    def _compute_buy_box_reference_price(
        stats: dict[str, Any], range_result: Any
    ) -> float | None:
        current = stats.get("current")
        if isinstance(current, list) and len(current) > CSV_BUY_BOX_SHIPPING:
            current_cents = _coerce_float(current[CSV_BUY_BOX_SHIPPING])
            if current_cents is not None and current_cents > 0:
                return round(current_cents / 100.0, 2)

        if range_result.low is not None and range_result.high is not None:
            return round((range_result.low + range_result.high) / 2.0, 2)

        return None

    def _compute_competitive_sellers(
        self,
        product: dict[str, Any],
        now_keepa_minutes: int,
        bb_reference_price: float | None,
    ) -> tuple[int | None, float | None, float | None, str | None, str | None]:
        if bb_reference_price is None or bb_reference_price <= 0:
            return None, None, None, None, "missing Buy Box reference price"

        offers = product.get("offers")
        if not isinstance(offers, list) or not offers:
            return None, None, None, None, "Keepa offers unavailable"

        allowed_delta = _allowed_delta(bb_reference_price)
        ceiling_price = round(bb_reference_price + allowed_delta, 2)

        best_price_by_seller: dict[str, float] = {}
        for offer in offers:
            if not isinstance(offer, dict):
                continue

            seller_id = str(offer.get("sellerId") or "").strip().upper()
            if not seller_id:
                continue

            if not bool(offer.get("isFBA")):
                continue
            if bool(offer.get("isAmazon")) or seller_id in US_AMAZON_SELLER_IDS:
                continue
            if not self._offer_is_active_and_in_stock(offer, now_keepa_minutes):
                continue

            total_price = _extract_offer_total_price_dollars(offer.get("offerCSV"))
            if total_price is None or total_price <= 0:
                continue
            if total_price > ceiling_price:
                continue

            previous = best_price_by_seller.get(seller_id)
            if previous is None or total_price < previous:
                best_price_by_seller[seller_id] = total_price

        comp_count = len(best_price_by_seller)
        debug = (
            f"BB={bb_reference_price:.2f}, delta={allowed_delta:.2f}, "
            f"ceiling={ceiling_price:.2f}, comp_fba_sellers={comp_count}"
        )
        return comp_count, allowed_delta, ceiling_price, debug, None

    @staticmethod
    def _offer_is_active_and_in_stock(offer: dict[str, Any], now_keepa_minutes: int) -> bool:
        if bool(offer.get("isScam")) or bool(offer.get("isPreorder")):
            return False
        if not bool(offer.get("isShippable", True)):
            return False

        stock_series = offer.get("stockCSV")
        if isinstance(stock_series, list) and stock_series:
            latest_stock = _extract_latest_stock(stock_series)
            if latest_stock is not None:
                stock_ts, stock_qty = latest_stock
                if stock_qty <= 0:
                    return False
                if (now_keepa_minutes - stock_ts) > ACTIVE_OFFER_MAX_AGE_MINUTES:
                    return False
                return True

        last_seen = _coerce_int(offer.get("lastSeen"))
        if last_seen is None:
            return False
        return (now_keepa_minutes - last_seen) <= ACTIVE_OFFER_MAX_AGE_MINUTES

    @staticmethod
    def _derive_buy_box_stability(
        buy_box_90d_avg: float | None,
        buy_box_90d_low: float | None,
        offer_count_delta_14d: int | None,
    ) -> str:
        if (
            buy_box_90d_avg is None
            or buy_box_90d_low is None
            or buy_box_90d_avg <= 0
            or offer_count_delta_14d is None
        ):
            return "UNSTABLE"
        if offer_count_delta_14d > 10:
            return "UNSTABLE"
        if (buy_box_90d_low / buy_box_90d_avg) < 0.85:
            return "UNSTABLE"
        return "STABLE"


def _extract_csv_series(product: dict[str, Any], csv_type_index: int) -> list[Any]:
    csv_root = product.get("csv")
    if not isinstance(csv_root, list):
        return []
    if csv_type_index >= len(csv_root):
        return []
    series = csv_root[csv_type_index]
    return series if isinstance(series, list) else []


def _parse_time_value_series(
    raw_series: list[Any],
    step: int,
    value_offset: int,
) -> list[tuple[int, float]]:
    points: list[tuple[int, float]] = []
    if not raw_series:
        return points

    for i in range(0, len(raw_series) - value_offset, step):
        ts = _coerce_int(raw_series[i])
        value = _coerce_float(raw_series[i + value_offset])
        if ts is None or value is None:
            continue
        points.append((ts, value))

    return points


def _parse_keepa_price_with_shipping_series(raw_series: list[Any]) -> list[tuple[int, float]]:
    points: list[tuple[int, float]] = []
    if not raw_series:
        return points

    for i in range(0, len(raw_series) - 2, 3):
        ts = _coerce_int(raw_series[i])
        price_cents = _coerce_float(raw_series[i + 1])
        shipping_cents = _coerce_float(raw_series[i + 2])
        if ts is None or price_cents is None:
            continue
        if price_cents < 0:
            points.append((ts, -1.0))
            continue
        shipping = shipping_cents if (shipping_cents is not None and shipping_cents > 0) else 0.0
        points.append((ts, price_cents + shipping))

    return points


def _extract_offer_total_price_dollars(offer_csv: Any) -> float | None:
    if not isinstance(offer_csv, list) or len(offer_csv) < 3:
        return None

    for i in range(len(offer_csv) - 3, -1, -3):
        price_cents = _coerce_float(offer_csv[i + 1])
        shipping_cents = _coerce_float(offer_csv[i + 2])
        if price_cents is None or price_cents <= 0:
            continue
        shipping = shipping_cents if (shipping_cents is not None and shipping_cents > 0) else 0.0
        return round((price_cents + shipping) / 100.0, 2)
    return None


def _extract_latest_stock(stock_csv: list[Any]) -> tuple[int, int] | None:
    if len(stock_csv) < 2:
        return None
    for i in range(len(stock_csv) - 2, -1, -2):
        ts = _coerce_int(stock_csv[i])
        qty = _coerce_int(stock_csv[i + 1])
        if ts is None or qty is None:
            continue
        return ts, qty
    return None


def _allowed_delta(buy_box_price: float) -> float:
    scaled = _round_half_up(buy_box_price * 0.15)
    return float(min(12, max(3, scaled)))


def _round_half_up(value: float) -> int:
    if value >= 0:
        return int(value + 0.5)
    return int(value - 0.5)


def _value_at_or_before(points: list[tuple[int, float]], ts: int) -> float | None:
    if not points:
        return None
    candidate = points[0][1]
    for point_ts, value in points:
        if point_ts > ts:
            break
        candidate = value
    return candidate


def _latest_non_negative_value(points: list[tuple[int, float]]) -> float | None:
    for _, value in reversed(points):
        if value >= 0:
            return value
    return None


def _extract_stats_array_value(raw: Any, index: int) -> float | None:
    if not isinstance(raw, list) or index >= len(raw):
        return None
    value = _coerce_float(raw[index])
    if value is None or value < 0:
        return None
    return value


def _extract_min_interval_value(raw: Any, index: int) -> float | None:
    if not isinstance(raw, list) or index >= len(raw):
        return None
    entry = raw[index]
    if not isinstance(entry, list) or len(entry) < 2:
        return None
    value = _coerce_float(entry[1])
    if value is None or value < 0:
        return None
    return value


def _cents_to_dollars(value: float | None) -> float | None:
    if value is None or value < 0:
        return None
    return round(value / 100.0, 2)


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def _now_keepa_minutes() -> int:
    return int(time.time() // 60) - KEEPA_EPOCH_OFFSET_MINUTES
