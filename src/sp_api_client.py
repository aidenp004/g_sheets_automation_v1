from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

import requests

LWA_TOKEN_URL = "https://api.amazon.com/auth/o2/token"
NA_ENDPOINT = "https://sellingpartnerapi-na.amazon.com"


class SPAPIError(RuntimeError):
    pass


@dataclass(frozen=True)
class FeesEstimate:
    total_fees: float
    referral_fee: float
    fba_fulfillment_fee: float
    breakdown: dict[str, float] = field(default_factory=dict)


class SPAPIClient:
    def __init__(
        self,
        lwa_client_id: str,
        lwa_client_secret: str,
        refresh_token: str,
        marketplace_id: str = "ATVPDKIKX0DER",
        endpoint: str = NA_ENDPOINT,
        timeout_seconds: int = 20,
        max_retries: int = 3,
    ) -> None:
        self.lwa_client_id = lwa_client_id.strip()
        self.lwa_client_secret = lwa_client_secret.strip()
        self.refresh_token = refresh_token.strip()
        self.marketplace_id = marketplace_id.strip() or "ATVPDKIKX0DER"
        self.endpoint = endpoint.strip().rstrip("/") or NA_ENDPOINT
        self.timeout_seconds = timeout_seconds
        self.max_retries = max(1, max_retries)
        self._session = requests.Session()

        self._access_token: str | None = None
        self._access_token_expires_at: float = 0.0
        self._fee_cache: dict[tuple[str, float], FeesEstimate] = {}

        if not self.lwa_client_id or not self.lwa_client_secret or not self.refresh_token:
            raise ValueError("Missing SP-API LWA credentials.")

    def get_fba_fees_estimate(self, asin: str, price: float) -> FeesEstimate:
        asin_clean = (asin or "").strip().upper()
        if not asin_clean:
            raise SPAPIError("ASIN is blank for SP-API fee estimate.")
        if price <= 0:
            raise SPAPIError("Price must be > 0 for SP-API fee estimate.")

        price_rounded = round(price, 2)
        cache_key = (asin_clean, price_rounded)
        cached = self._fee_cache.get(cache_key)
        if cached is not None:
            return cached

        token = self._get_access_token()
        url = f"{self.endpoint}/products/fees/v0/items/{asin_clean}/feesEstimate"

        payload = {
            "FeesEstimateRequest": {
                "MarketplaceId": self.marketplace_id,
                "IsAmazonFulfilled": True,
                "Identifier": f"{asin_clean}-{int(time.time())}",
                "PriceToEstimateFees": {
                    "ListingPrice": {"CurrencyCode": "USD", "Amount": price_rounded},
                    "Shipping": {"CurrencyCode": "USD", "Amount": 0.0},
                    "Points": {
                        "PointsNumber": 0,
                        "PointsMonetaryValue": {"CurrencyCode": "USD", "Amount": 0.0},
                    },
                },
            }
        }

        response = self._request_json(
            method="POST",
            url=url,
            headers={
                "Authorization": f"Bearer {token}",
                "x-amz-access-token": token,
                "content-type": "application/json",
            },
            json_body=payload,
        )
        fees = _parse_fees_estimate(response)
        if len(self._fee_cache) >= 500:
            self._fee_cache.clear()
        self._fee_cache[cache_key] = fees
        return fees

    def _get_access_token(self) -> str:
        now = time.time()
        if self._access_token and now < (self._access_token_expires_at - 60):
            return self._access_token

        form = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
            "client_id": self.lwa_client_id,
            "client_secret": self.lwa_client_secret,
        }
        payload = self._request_json(
            method="POST",
            url=LWA_TOKEN_URL,
            headers={"content-type": "application/x-www-form-urlencoded"},
            form_body=form,
        )
        access_token = str(payload.get("access_token") or "").strip()
        expires_in = _coerce_int(payload.get("expires_in")) or 3600
        if not access_token:
            raise SPAPIError("Failed to obtain SP-API LWA access token.")

        self._access_token = access_token
        self._access_token_expires_at = now + max(60, expires_in)
        return access_token

    def _request_json(
        self,
        method: str,
        url: str,
        headers: dict[str, str],
        json_body: dict[str, Any] | None = None,
        form_body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        last_error: str | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self._session.request(
                    method=method,
                    url=url,
                    headers=headers,
                    json=json_body,
                    data=form_body,
                    timeout=self.timeout_seconds,
                )
            except requests.RequestException as exc:
                last_error = f"SP-API request failed: {exc}"
                if attempt < self.max_retries:
                    time.sleep(min(10.0, attempt * 2.0))
                    continue
                raise SPAPIError(last_error) from exc

            payload: dict[str, Any] | None = None
            try:
                parsed = response.json()
                if isinstance(parsed, dict):
                    payload = parsed
            except ValueError:
                payload = None

            if response.status_code in {429, 500, 502, 503, 504} and attempt < self.max_retries:
                sleep_seconds = min(15.0, attempt * 2.0)
                time.sleep(sleep_seconds)
                continue

            if response.status_code != 200:
                details = _extract_sp_api_error(payload) or response.text[:400]
                raise SPAPIError(f"SP-API HTTP {response.status_code}: {details}")

            if payload is None:
                raise SPAPIError("SP-API returned non-JSON response.")
            return payload

        raise SPAPIError(last_error or "SP-API request failed.")


def _parse_fees_estimate(payload: dict[str, Any]) -> FeesEstimate:
    result = _extract_fees_result(payload)
    if result is None:
        raise SPAPIError("SP-API fees response missing FeesEstimate result.")

    status = str(result.get("Status") or "").strip().lower()
    if status and status != "success":
        err = _extract_sp_api_error(result) or "fee estimate status not success"
        raise SPAPIError(f"SP-API fee estimate failed: {err}")

    fees_obj = result.get("FeesEstimate")
    if not isinstance(fees_obj, dict):
        raise SPAPIError("SP-API fees response missing FeesEstimate object.")

    breakdown: dict[str, float] = {}
    fee_details = fees_obj.get("FeeDetailList")
    if isinstance(fee_details, list):
        for entry in fee_details:
            if not isinstance(entry, dict):
                continue
            fee_type = str(entry.get("FeeType") or "").strip()
            if not fee_type:
                continue
            amount = _extract_amount(entry.get("FinalFee"))
            if amount is None:
                amount = _extract_amount(entry.get("FeeAmount"))
            if amount is None:
                continue
            breakdown[fee_type] = round(breakdown.get(fee_type, 0.0) + amount, 2)

    total = _extract_amount(fees_obj.get("TotalFeesEstimate"))
    if total is None:
        total = round(sum(breakdown.values()), 2)
    referral = round(
        sum(v for k, v in breakdown.items() if "referral" in k.lower()),
        2,
    )
    fba_fulfillment = round(
        sum(
            v
            for k, v in breakdown.items()
            if ("fulfillment" in k.lower() and "fba" in k.lower())
            or "fbaperunitfulfillmentfee" in k.lower()
        ),
        2,
    )
    return FeesEstimate(
        total_fees=round(total, 2),
        referral_fee=referral,
        fba_fulfillment_fee=fba_fulfillment,
        breakdown=breakdown,
    )


def _extract_fees_result(payload: dict[str, Any]) -> dict[str, Any] | None:
    root: Any = payload.get("payload", payload)
    if isinstance(root, dict):
        if isinstance(root.get("FeesEstimateResult"), dict):
            return root["FeesEstimateResult"]
        if isinstance(root.get("feesEstimateResult"), dict):
            return root["feesEstimateResult"]
        if isinstance(root.get("FeesEstimateResultList"), list) and root["FeesEstimateResultList"]:
            first = root["FeesEstimateResultList"][0]
            if isinstance(first, dict):
                return first
        if isinstance(root.get("feesEstimateResultList"), list) and root["feesEstimateResultList"]:
            first = root["feesEstimateResultList"][0]
            if isinstance(first, dict):
                return first
    if isinstance(root, list) and root:
        first = root[0]
        if isinstance(first, dict):
            return first
    return None


def _extract_amount(value: Any) -> float | None:
    if not isinstance(value, dict):
        return None
    amount = _coerce_float(value.get("Amount"))
    if amount is not None:
        return amount
    amount = _coerce_float(value.get("amount"))
    if amount is not None:
        return amount
    return None


def _extract_sp_api_error(payload: dict[str, Any] | None) -> str | None:
    if not isinstance(payload, dict):
        return None
    errors = payload.get("errors")
    if isinstance(errors, list) and errors:
        first = errors[0]
        if isinstance(first, dict):
            message = str(first.get("message") or "").strip()
            code = str(first.get("code") or "").strip()
            if code and message:
                return f"{code}: {message}"
            if message:
                return message
    error = payload.get("error")
    if isinstance(error, dict):
        message = str(error.get("message") or "").strip()
        if message:
            return message
    if isinstance(error, str):
        error = error.strip()
        if error:
            return error
    return None


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
