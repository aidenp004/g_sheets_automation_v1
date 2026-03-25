from __future__ import annotations

import json
import time
from typing import Any

import requests

from src.buy_box_range import BuyBoxRangeResult, compute_buy_box_range
from src.models import KeepaMetrics

KEEPA_PRODUCT_URL = "https://api.keepa.com/product"
KEEPA_SELLER_URL = "https://api.keepa.com/seller"
KEEPA_QUERY_URL = "https://api.keepa.com/query"
KEEPA_EPOCH_OFFSET_MINUTES = 21_564_000

CSV_COUNT_NEW = 11
CSV_BUY_BOX_SHIPPING = 18

US_AMAZON_SELLER_IDS = {"ATVPDKIKX0DER"}
CURRENT_RANGE_DAYS = 21
HISTORY_QUERY_DAYS = 30
ACTIVE_OFFER_MAX_AGE_MINUTES = 7 * 24 * 60
MODE_FBA = "FBA"
MODE_FBM = "FBM"
MODE_AMAZON = "AMAZON"
MODE_UNKNOWN = "UNKNOWN"


class KeepaError(RuntimeError):
    pass


class KeepaRateLimitError(KeepaError):
    def __init__(
        self,
        message: str,
        refill_in_ms: int | None = None,
        tokens_left: int | None = None,
    ) -> None:
        super().__init__(message)
        self.refill_in_ms = refill_in_ms
        self.tokens_left = tokens_left


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
        brand = _clean_string(product.get("brand"))
        stats = product.get("stats") if isinstance(product.get("stats"), dict) else {}
        now_keepa_minutes = _now_keepa_minutes()
        buy_box_price_history = self._extract_buy_box_price_history(product, window_days=90)
        buy_box_price_history_raw = self._extract_buy_box_price_history_raw(
            product,
            window_days=HISTORY_QUERY_DAYS,
        )
        buy_box_seller_history = self._extract_buy_box_seller_history(product)
        seller_mode_map = self._extract_current_seller_mode_map(product)
        fba_share_90d, fbm_share_90d = self._extract_buy_box_fulfillment_shares_90d(
            stats=stats,
            buy_box_seller_history=buy_box_seller_history,
            seller_mode_map=seller_mode_map,
        )
        range_result = compute_buy_box_range(
            raw_history=buy_box_price_history_raw,
            now_keepa_minutes=now_keepa_minutes,
            window_days=CURRENT_RANGE_DAYS,
        )
        current_bb_price = self._extract_current_buy_box_price(stats)
        range_result = self._normalize_range_with_current_bb(
            range_result=range_result,
            current_bb_price=current_bb_price,
        )
        bb_reference_price = self._compute_buy_box_reference_price(stats, range_result)
        (
            comp_count,
            comp_fba_count,
            comp_fbm_count,
            comp_weighted_stock_units,
            comp_stock_known_sellers,
            comp_stock_total_sellers,
            comp_delta,
            comp_ceiling,
            comp_debug,
            comp_issue,
        ) = self._compute_competitive_sellers(
            product=product,
            now_keepa_minutes=now_keepa_minutes,
            bb_reference_price=bb_reference_price,
            range_result=range_result,
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
        package_weight_grams = self._extract_package_weight_grams(product)

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
            brand=brand,
            buy_box_fba_share_90d=fba_share_90d,
            buy_box_fbm_share_90d=fbm_share_90d,
            package_weight_grams=package_weight_grams,
            buy_box_range_current=range_result.range_text,
            buy_box_range_21d_low=range_result.low,
            buy_box_range_21d_high=range_result.high,
            buy_box_samples_21d=range_result.sample_count,
            buy_box_total_events_21d=range_result.total_events,
            buy_box_relative_spread_21d=range_result.relative_spread,
            buy_box_range_issue=range_result.reason,
            current_buy_box_price=bb_reference_price,
            competitive_sellers_near_bb=comp_count,
            competitive_fba_sellers_near_bb=comp_fba_count,
            competitive_fbm_sellers_near_bb=comp_fbm_count,
            competitive_weighted_stock_units=comp_weighted_stock_units,
            competitive_stock_known_sellers=comp_stock_known_sellers,
            competitive_stock_total_sellers=comp_stock_total_sellers,
            competitive_allowed_delta=comp_delta,
            competitive_ceiling_price=comp_ceiling,
            competitive_debug=comp_debug,
            competitive_issue=comp_issue,
            buy_box_price_history=buy_box_price_history,
            buy_box_seller_history=buy_box_seller_history,
            missing_fields=missing_fields,
        )

    def get_seller_asins(self, seller_id: str, limit: int = 500) -> list[str]:
        seller_clean = _clean_string(seller_id)
        if not seller_clean:
            raise KeepaError("seller_id is blank.")
        if limit <= 0:
            return []
        limit = min(limit, 500)

        params = {
            "key": self.api_key,
            "domain": self.domain_id,
            "seller": seller_clean,
            "storefront": 1,
            "update": 0,
        }

        response = self._request_json(KEEPA_SELLER_URL, params=params)
        sellers = response.get("sellers")
        if not isinstance(sellers, dict):
            raise KeepaError("Keepa seller response missing 'sellers'.")

        seller_obj = sellers.get(seller_clean)
        if not isinstance(seller_obj, dict):
            if len(sellers) == 1:
                only_value = next(iter(sellers.values()))
                if isinstance(only_value, dict):
                    seller_obj = only_value
        if not isinstance(seller_obj, dict):
            raise KeepaError(f"Keepa seller response missing storefront data for {seller_clean}.")

        asin_list = seller_obj.get("asinList")
        if not isinstance(asin_list, list):
            raise KeepaError("Keepa storefront response missing asinList.")

        deduped: list[str] = []
        seen: set[str] = set()
        for raw in asin_list:
            asin = _clean_string(raw)
            if not asin:
                continue
            asin = asin.upper()
            if asin in seen:
                continue
            seen.add(asin)
            deduped.append(asin)
            if len(deduped) >= limit:
                break

        return deduped

    def get_product_finder_asins(
        self,
        selection: dict[str, Any],
        max_pages: int = 10,
        candidate_limit: int = 500,
    ) -> list[str]:
        if not isinstance(selection, dict) or not selection:
            raise KeepaError("Product Finder selection must be a non-empty object.")
        if max_pages <= 0:
            return []
        if candidate_limit <= 0:
            return []

        per_page = _coerce_int(selection.get("perPage")) or 10000
        per_page = max(1, min(10000, per_page))

        deduped: list[str] = []
        seen: set[str] = set()

        for page in range(max_pages):
            selection_page = dict(selection)
            selection_page["perPage"] = per_page
            selection_page["page"] = page

            params = {
                "key": self.api_key,
                "domain": self.domain_id,
                "selection": json.dumps(selection_page, separators=(",", ":")),
            }
            payload = self._request_json(KEEPA_QUERY_URL, params=params)
            page_asins = _extract_query_asin_list(payload)
            if not page_asins:
                break

            page_new = 0
            for asin_raw in page_asins:
                asin = _clean_string(asin_raw).upper()
                if not _looks_like_asin(asin):
                    continue
                if asin in seen:
                    continue
                seen.add(asin)
                deduped.append(asin)
                page_new += 1
                if len(deduped) >= candidate_limit:
                    return deduped

            if page_new == 0:
                break
            if len(page_asins) < per_page:
                break

        return deduped

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

        payload = self._request_json(KEEPA_PRODUCT_URL, params=params)

        products = payload.get("products")
        if not isinstance(products, list) or not products:
            raise KeepaError("Keepa API returned no products.")

        product = products[0]
        if not isinstance(product, dict):
            raise KeepaError("Keepa API returned invalid product format.")

        return product

    def _request_json(self, url: str, params: dict[str, Any]) -> dict[str, Any]:
        response: requests.Response | None = None
        last_error: str | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self._session.get(
                    url,
                    params=params,
                    timeout=self.timeout_seconds,
                )
            except requests.RequestException as exc:
                last_error = f"Failed to call Keepa API: {exc}"
                if attempt < self.max_retries:
                    time.sleep(attempt * 2)
                    continue
                raise KeepaError(last_error) from exc

            payload: dict[str, Any] | None = None
            try:
                parsed = response.json()
                if isinstance(parsed, dict):
                    payload = parsed
            except ValueError:
                payload = None

            if response.status_code == 429:
                refill_in_ms = _coerce_int(payload.get("refillIn")) if payload else None
                tokens_left = _coerce_int(payload.get("tokensLeft")) if payload else None
                error_message = _extract_keepa_error_message(payload)
                wait_seconds = _rate_limit_wait_seconds(refill_in_ms, attempt)
                last_error = (
                    f"Keepa API rate limited (HTTP 429). "
                    f"refillInMs={refill_in_ms} tokensLeft={tokens_left} "
                    f"message={error_message or 'rate limit'}"
                )
                if attempt < self.max_retries:
                    time.sleep(wait_seconds)
                    continue
                raise KeepaRateLimitError(
                    last_error,
                    refill_in_ms=refill_in_ms,
                    tokens_left=tokens_left,
                )

            if response.status_code in {500, 502, 503, 504} and attempt < self.max_retries:
                last_error = f"Keepa API HTTP {response.status_code}: {response.text[:400]}"
                time.sleep(attempt * 2)
                continue
            break

        if response is None:
            raise KeepaError(last_error or "Keepa API request failed.")

        if response.status_code != 200:
            payload: dict[str, Any] | None = None
            try:
                parsed = response.json()
                if isinstance(parsed, dict):
                    payload = parsed
            except ValueError:
                payload = None
            error_message = _extract_keepa_error_message(payload)
            if error_message:
                raise KeepaError(f"Keepa API HTTP {response.status_code}: {error_message}")
            raise KeepaError(f"Keepa API HTTP {response.status_code}: {response.text[:400]}")

        try:
            payload = response.json()
        except ValueError as exc:
            raise KeepaError("Keepa API returned non-JSON response.") from exc

        if not isinstance(payload, dict):
            raise KeepaError("Keepa API returned invalid JSON payload.")
        if payload.get("error"):
            raise KeepaError(f"Keepa API error: {payload['error']}")
        return payload

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
    def _extract_current_seller_mode_map(product: dict[str, Any]) -> dict[str, str]:
        offers = product.get("offers")
        if not isinstance(offers, list):
            return {}

        mode_map: dict[str, str] = {}
        for offer in offers:
            if not isinstance(offer, dict):
                continue
            seller_id = _clean_string(offer.get("sellerId")).upper()
            if not seller_id:
                continue
            if bool(offer.get("isAmazon")) or seller_id in US_AMAZON_SELLER_IDS:
                mode_map[seller_id] = MODE_AMAZON
                continue
            mode_map[seller_id] = MODE_FBA if bool(offer.get("isFBA")) else MODE_FBM
        return mode_map

    def _extract_buy_box_fulfillment_shares_90d(
        self,
        stats: dict[str, Any],
        buy_box_seller_history: list[tuple[int, str]],
        seller_mode_map: dict[str, str],
    ) -> tuple[float | None, float | None]:
        stats_share = self._extract_fulfillment_shares_from_buy_box_stats(
            stats=stats,
            seller_mode_map=seller_mode_map,
        )
        if stats_share is not None:
            return stats_share

        return self._extract_fulfillment_shares_from_history(
            buy_box_seller_history=buy_box_seller_history,
            seller_mode_map=seller_mode_map,
        )

    def _extract_fulfillment_shares_from_buy_box_stats(
        self,
        stats: dict[str, Any],
        seller_mode_map: dict[str, str],
    ) -> tuple[float, float] | None:
        buy_box_stats = stats.get("buyBoxStats")
        if not isinstance(buy_box_stats, dict):
            return None

        totals = {MODE_FBA: 0.0, MODE_FBM: 0.0, MODE_AMAZON: 0.0, MODE_UNKNOWN: 0.0}
        saw_percentage = False

        for raw_seller_id, summary in buy_box_stats.items():
            if not isinstance(summary, dict):
                continue
            percentage_won = _coerce_float(summary.get("percentageWon"))
            if percentage_won is None or percentage_won < 0:
                continue
            seller_id = _clean_string(raw_seller_id).upper()
            mode = _resolve_seller_mode(
                seller_id=seller_id,
                seller_mode_map=seller_mode_map,
                summary=summary,
            )
            totals[mode] += percentage_won
            saw_percentage = True

        if not saw_percentage:
            return None

        # Unknown historical sellers are treated as FBM to keep the FBA estimate conservative.
        totals[MODE_FBM] += totals[MODE_UNKNOWN]
        total_pct = totals[MODE_FBA] + totals[MODE_FBM] + totals[MODE_AMAZON]
        if total_pct <= 0:
            return None

        fba_share = (totals[MODE_FBA] / total_pct) * 100.0
        fbm_share = (totals[MODE_FBM] / total_pct) * 100.0
        return round(fba_share, 2), round(fbm_share, 2)

    def _extract_fulfillment_shares_from_history(
        self,
        buy_box_seller_history: list[tuple[int, str]],
        seller_mode_map: dict[str, str],
    ) -> tuple[float | None, float | None]:
        history = sorted(buy_box_seller_history, key=lambda item: item[0])
        if len(history) < 2:
            return None, None

        start = _now_keepa_minutes() - (90 * 24 * 60)
        end = _now_keepa_minutes()
        totals = {MODE_FBA: 0, MODE_FBM: 0, MODE_AMAZON: 0, MODE_UNKNOWN: 0}

        current_seller = history[0][1]
        current_time = start

        for ts, seller in history:
            if ts <= start:
                current_seller = seller
                continue
            if ts > end:
                break
            duration = ts - current_time
            if duration > 0:
                mode = _resolve_seller_mode(
                    seller_id=current_seller,
                    seller_mode_map=seller_mode_map,
                    summary=None,
                )
                totals[mode] += duration
            current_time = ts
            current_seller = seller

        if end > current_time:
            tail_duration = end - current_time
            mode = _resolve_seller_mode(
                seller_id=current_seller,
                seller_mode_map=seller_mode_map,
                summary=None,
            )
            totals[mode] += tail_duration

        total_minutes = totals[MODE_FBA] + totals[MODE_FBM] + totals[MODE_AMAZON] + totals[MODE_UNKNOWN]
        if total_minutes <= 0:
            return None, None

        totals[MODE_FBM] += totals[MODE_UNKNOWN]
        fba_share = (totals[MODE_FBA] / total_minutes) * 100.0
        fbm_share = (totals[MODE_FBM] / total_minutes) * 100.0
        return round(fba_share, 2), round(fbm_share, 2)

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
    def _extract_package_weight_grams(product: dict[str, Any]) -> float | None:
        candidates = [
            product.get("packageWeight"),
            product.get("packageWeightG"),
            product.get("packageWeightGram"),
            product.get("packageWeightInGram"),
            product.get("itemPackageWeight"),
            product.get("itemWeight"),
        ]
        for raw in candidates:
            weight = _coerce_float(raw)
            if weight is None or weight <= 0:
                continue
            # Keepa commonly reports package weight in grams for these fields.
            return round(weight, 2)
        return None

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
    def _extract_current_buy_box_price(stats: dict[str, Any]) -> float | None:
        current = stats.get("current")
        if isinstance(current, list) and len(current) > CSV_BUY_BOX_SHIPPING:
            current_cents = _coerce_float(current[CSV_BUY_BOX_SHIPPING])
            if current_cents is not None and current_cents > 0:
                return round(current_cents / 100.0, 2)
        return None

    @staticmethod
    def _normalize_range_with_current_bb(
        range_result: BuyBoxRangeResult, current_bb_price: float | None
    ) -> BuyBoxRangeResult:
        if range_result.range_text is not None:
            return range_result
        if current_bb_price is None or current_bb_price <= 0:
            return range_result
        if range_result.reason and (
            range_result.reason.startswith("insufficient buy box samples")
            or range_result.reason == "buy box suppressed or unavailable in last 21 days"
        ):
            return BuyBoxRangeResult(
                low=current_bb_price,
                high=current_bb_price,
                range_text=f"{current_bb_price:.2f}-{current_bb_price:.2f}",
                sample_count=max(1, range_result.sample_count),
                total_events=range_result.total_events,
                stability="STABLE",
                relative_spread=0.0,
                reason=None,
            )
        return range_result

    @staticmethod
    def _compute_buy_box_reference_price(
        stats: dict[str, Any], range_result: Any
    ) -> float | None:
        current_bb_price = KeepaClient._extract_current_buy_box_price(stats)
        if current_bb_price is not None:
            return current_bb_price

        if range_result.low is not None and range_result.high is not None:
            return round((range_result.low + range_result.high) / 2.0, 2)

        return None

    def _compute_competitive_sellers(
        self,
        product: dict[str, Any],
        now_keepa_minutes: int,
        bb_reference_price: float | None,
        range_result: BuyBoxRangeResult,
    ) -> tuple[
        int | None,
        int | None,
        int | None,
        float | None,
        int | None,
        int | None,
        float | None,
        float | None,
        str | None,
        str | None,
    ]:
        band_low: float | None = None
        band_high: float | None = None
        if range_result.low is not None and range_result.high is not None:
            band_low = min(range_result.low, range_result.high)
            band_high = max(range_result.low, range_result.high)
        elif bb_reference_price is not None and bb_reference_price > 0:
            band_low = bb_reference_price
            band_high = bb_reference_price

        if band_low is None or band_high is None or band_high <= 0:
            return None, None, None, None, None, None, None, None, None, "missing Buy Box reference range"

        offers = product.get("offers")
        if not isinstance(offers, list) or not offers:
            return None, None, None, None, None, None, None, None, None, "Keepa offers unavailable"

        band_width = max(0.0, band_high - band_low)
        allowed_delta = round(max(0.5, band_width * 0.25), 2)
        ceiling_price = round(band_high + allowed_delta, 2)

        seller_entries: dict[str, dict[str, Any]] = {}
        for offer in offers:
            if not isinstance(offer, dict):
                continue

            seller_id = str(offer.get("sellerId") or "").strip().upper()
            if not seller_id:
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

            current = seller_entries.get(seller_id)
            if current is not None and float(current["price"]) <= total_price:
                continue

            seller_entries[seller_id] = {
                "price": total_price,
                "is_fba": bool(offer.get("isFBA")),
                "stock_qty": self._extract_offer_stock_qty(offer, now_keepa_minutes),
            }

        comp_fba_count = sum(1 for entry in seller_entries.values() if bool(entry["is_fba"]))
        comp_fbm_count = sum(1 for entry in seller_entries.values() if not bool(entry["is_fba"]))
        comp_count = comp_fba_count + comp_fbm_count

        weighted_stock_units = 0.0
        known_stock_sellers = 0
        for entry in seller_entries.values():
            price = float(entry["price"])
            stock_qty = entry.get("stock_qty")
            if stock_qty is None:
                continue
            known_stock_sellers += 1
            mode_weight = 1.0 if bool(entry["is_fba"]) else 0.6
            price_weight = _price_proximity_weight(
                offer_price=price,
                band_high=band_high,
                ceiling_price=ceiling_price,
            )
            weighted_stock_units += float(stock_qty) * mode_weight * price_weight

        weighted_stock_value: float | None = None
        if known_stock_sellers > 0:
            weighted_stock_value = round(weighted_stock_units, 2)

        debug = (
            f"band={band_low:.2f}-{band_high:.2f}, ext={allowed_delta:.2f}, "
            f"ceiling={ceiling_price:.2f}, comp_fba={comp_fba_count}, comp_fbm={comp_fbm_count}, "
            f"comp_total={comp_count}, stock_known={known_stock_sellers}/{comp_count}, "
            f"weighted_stock={_fmt_debug_float(weighted_stock_value)}"
        )
        return (
            comp_count,
            comp_fba_count,
            comp_fbm_count,
            weighted_stock_value,
            known_stock_sellers,
            comp_count,
            allowed_delta,
            ceiling_price,
            debug,
            None,
        )

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
    def _extract_offer_stock_qty(offer: dict[str, Any], now_keepa_minutes: int) -> int | None:
        stock_series = offer.get("stockCSV")
        if not isinstance(stock_series, list) or not stock_series:
            return None
        latest_stock = _extract_latest_stock(stock_series)
        if latest_stock is None:
            return None
        stock_ts, stock_qty = latest_stock
        if stock_qty <= 0:
            return None
        if (now_keepa_minutes - stock_ts) > ACTIVE_OFFER_MAX_AGE_MINUTES:
            return None
        return int(stock_qty)

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


def _price_proximity_weight(
    offer_price: float,
    band_high: float,
    ceiling_price: float,
) -> float:
    if ceiling_price <= band_high:
        return 1.0
    if offer_price <= band_high:
        return 1.0
    distance = max(0.0, offer_price - band_high)
    max_distance = max(0.01, ceiling_price - band_high)
    ratio = min(1.0, distance / max_distance)
    return max(0.5, 1.0 - (0.5 * ratio))


def _fmt_debug_float(value: float | None) -> str:
    if value is None:
        return "NA"
    return f"{value:.2f}"


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


def _clean_string(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _resolve_seller_mode(
    seller_id: str,
    seller_mode_map: dict[str, str],
    summary: dict[str, Any] | None,
) -> str:
    seller_upper = _clean_string(seller_id).upper()
    if not seller_upper:
        return MODE_UNKNOWN
    if seller_upper in US_AMAZON_SELLER_IDS:
        return MODE_AMAZON

    if summary is not None:
        is_fba = _coerce_bool(summary.get("isFBA"))
        if is_fba is True:
            return MODE_FBA
        is_fbm = _coerce_bool(summary.get("isFBM"))
        if is_fbm is True:
            return MODE_FBM

    mapped = seller_mode_map.get(seller_upper)
    if mapped in {MODE_FBA, MODE_FBM, MODE_AMAZON}:
        return mapped
    return MODE_UNKNOWN


def _coerce_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    raw = str(value).strip().lower()
    if raw in {"true", "1", "yes", "y"}:
        return True
    if raw in {"false", "0", "no", "n"}:
        return False
    return None


def _looks_like_asin(value: str) -> bool:
    if len(value) != 10:
        return False
    return value.isalnum()


def _extract_keepa_error_message(payload: dict[str, Any] | None) -> str | None:
    if not isinstance(payload, dict):
        return None
    error = payload.get("error")
    if isinstance(error, str) and error.strip():
        return error.strip()
    if isinstance(error, dict):
        message = _clean_string(error.get("message"))
        details = _clean_string(error.get("details"))
        if message and details:
            return f"{message} ({details})"
        if message:
            return message
        if details:
            return details
    return None


def _extract_query_asin_list(payload: dict[str, Any]) -> list[str]:
    raw_list = payload.get("asinList")
    if isinstance(raw_list, list):
        return [str(value) for value in raw_list]

    raw_lists = payload.get("asinLists")
    if isinstance(raw_lists, list):
        flattened: list[str] = []
        for entry in raw_lists:
            if not isinstance(entry, list):
                continue
            flattened.extend(str(value) for value in entry)
        if flattened:
            return flattened

    products = payload.get("products")
    if isinstance(products, list):
        values: list[str] = []
        for product in products:
            if not isinstance(product, dict):
                continue
            asin = _clean_string(product.get("asin"))
            if asin:
                values.append(asin)
        return values

    return []


def _rate_limit_wait_seconds(refill_in_ms: int | None, attempt: int) -> float:
    if refill_in_ms is not None and refill_in_ms >= 0:
        # Add a small safety buffer to avoid hitting the boundary exactly.
        return min(120.0, (refill_in_ms / 1000.0) + 1.0)
    return min(30.0, max(1.0, attempt * 3.0))


def _now_keepa_minutes() -> int:
    return int(time.time() // 60) - KEEPA_EPOCH_OFFSET_MINUTES
