"""
CRVS DCI polling helper.

Live OpenCRVS `on-search` HTTP 200 bodies differ from outbound `search` requests:

- ``header.action`` is ``on-search`` (response) vs ``search`` (request).
- ``message`` includes ``correlation_id`` and ``search_response`` (array of blocks).
- Each ``search_response[i]`` may include nested ``pagination`` (``page_number``,
  ``page_size``, ``total_count``) and ``data.reg_records``.
- Civil death notifications often use ``reg_record_type`` ``spdci-extensions-dci:Person``,
  ``reg_type`` ``ns:org:RegistryType:Civil``, with ``death_place`` and top-level
  ``identifier`` on each record.
- A root-level ``signature`` (JWT) may be present; ``split_reg_records_into_payloads``
  uses ``deepcopy`` so each queued payload keeps the same verifiable tree as CRVS
  returned (including ``signature`` when present).
"""

import hashlib
import hmac
import json
import uuid
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any

import requests
from requests import Response

from openg2p_celery_job_models.models import G2PExternalDataProvider

from ..config import Settings
from .helper import HelperInterface

_config = Settings.get_config()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _to_crvs_range_date(value: datetime | str) -> str:
    """CRVS search range expects calendar dates (YYYY-MM-DD), not full timestamps."""
    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt.date().isoformat()

    s = value.strip()
    if len(s) >= 10 and s[4:5] == "-" and s[7:8] == "-":
        return s[:10]
    raise ValueError(f"Cannot parse CRVS range date from: {value!r}")


def _utc_today_range_date() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _fresh_ids() -> tuple[str, str, str]:
    mid = str(uuid.uuid4()).replace("-", "")[:24]
    return mid, mid, mid


def _total_reg_records_in_response(body: dict[str, Any]) -> int:
    message = (body or {}).get("message") or {}
    search_response = message.get("search_response") or []
    if not isinstance(search_response, list):
        return 0
    n = 0
    for item in search_response:
        data = (item or {}).get("data") or {}
        reg_records = data.get("reg_records") or []
        if isinstance(reg_records, list):
            n += len(reg_records)
    return n


def _first_block_pagination(body: dict[str, Any]) -> dict[str, Any]:
    message = (body or {}).get("message") or {}
    search_response = message.get("search_response") or []
    if not search_response or not isinstance(search_response, list):
        return {}
    block = search_response[0] or {}
    pag = block.get("pagination")
    return pag if isinstance(pag, dict) else {}


class CrvsHelper(HelperInterface):
    def __init__(self):
        self.registry_ingest_url = _config.registry_ingest_url
        self.client_id = _config.openg2p_crvs_client_id
        self.client_secret = _config.openg2p_crvs_client_secret
        self.sha_secret = _config.openg2p_crvs_sha_secret
        self.entry_point_start_datetime = _config.entry_point_start_datetime

    def create_polling_request(
        self,
        g2p_external_data_provider: G2PExternalDataProvider,
        *,
        page_number: int = 1,
        page_size: int | None = None,
    ) -> tuple[dict, dict]:
        data_provider = g2p_external_data_provider
        sz = page_size if page_size is not None else data_provider.polling_page_size
        if not sz or sz < 1:
            sz = 10

        current_utc_iso = _utc_now_iso()
        gte_date = (
            _to_crvs_range_date(data_provider.poll_latest_success_datetime)
            if data_provider.poll_latest_success_datetime
            else _to_crvs_range_date(self.entry_point_start_datetime)
        )
        lte_date = _utc_today_range_date()

        reg_event_type = (data_provider.reg_event_type or "death").strip()

        mid, tid, rid = _fresh_ids()

        request_body: dict[str, Any] = {
            "header": {
                "version": "1.0.0",
                "message_id": mid,
                "message_ts": current_utc_iso,
                "action": "search",
                "sender_id": "https://integrating-server.com",
                "sender_uri": "https://{server_url}/on-search",
                "receiver_id": "crvs",
                "total_count": 10,
                "encryption_algorithm": "DH-2048",
            },
            "message": {
                "transaction_id": tid,
                "search_request": [
                    {
                        "reference_id": rid,
                        "timestamp": current_utc_iso,
                        "search_criteria": {
                            "version": "1.0.0",
                            "reg_type": "ns:org:RegistryType:Civil",
                            "reg_event_type": reg_event_type,
                            "query_type": "expression",
                            "query": {
                                "type": "ns:org:QueryType:expression",
                                "value": {
                                    "expression": {
                                        "query": {
                                            "legalStatuses.REGISTERED.acceptedAt": {
                                                "type": "range",
                                                "gte": gte_date,
                                                "lte": lte_date,
                                            }
                                        }
                                    }
                                },
                            },
                            "sort": [
                                {
                                    "attribute_name": "createdAt",
                                    "sort_order": "asc",
                                }
                            ],
                            "pagination": {
                                "page_size": sz,
                                "page_number": page_number,
                            },
                        },
                    }
                ],
            },
        }

        request_header = {
            "Authorization": f"Bearer {self.get_oauth_token(data_provider.polling_base_url)}",
            "Content-Type": "application/json",
        }

        return request_header, request_body

    def send_polling_request(
        self,
        g2p_external_data_provider: G2PExternalDataProvider,
    ) -> list[Response]:
        data_provider = g2p_external_data_provider
        if data_provider.polling_base_url is None:
            raise Exception(
                f"Polling URL is not configured for {data_provider.provider_name} data provider"
            )

        try:
            page_size = data_provider.polling_page_size or 10
            max_pages = max(1, _config.crvs_max_pages_per_poll)
            responses: list[Response] = []
            page_number = 1

            while page_number <= max_pages:
                request_header, request_body = self.create_polling_request(
                    data_provider,
                    page_number=page_number,
                    page_size=page_size,
                )
                response = requests.post(
                    data_provider.polling_base_url.rstrip("/") + "/registry/sync/search",
                    headers=request_header,
                    json=request_body,
                    timeout=_config.crvs_search_http_timeout_seconds,
                )
                response.raise_for_status()
                responses.append(response)

                body = response.json() if response.content else {}
                recorded = _total_reg_records_in_response(body)
                if recorded == 0:
                    break

                pag = _first_block_pagination(body)
                total_count = pag.get("total_count")
                resp_page = pag.get("page_number", page_number)
                resp_size = pag.get("page_size", page_size)

                try:
                    total_count_int = int(total_count) if total_count is not None else None
                except (TypeError, ValueError):
                    total_count_int = None

                if total_count_int is not None and total_count_int > 0:
                    if resp_page * resp_size >= total_count_int:
                        break
                elif recorded < page_size:
                    break

                page_number += 1

            return responses

        except requests.exceptions.RequestException as req_e:
            raise Exception(f"Request error calling polling url: {str(req_e)}") from req_e
        except Exception as e:
            raise Exception(f"Error during polling attempt: {str(e)}") from e

    def enrich_polling_response(self, response_body: dict[str, Any]) -> dict[str, Any]:
        query = self._get_section_query()
        response_body["query"] = query

        return response_body

    def get_oauth_token(self, base_url: str) -> str:
        oauth_token_url = f"{base_url.rstrip('/')}/oauth2/client/token"
        response = requests.post(
            oauth_token_url,
            data={
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "grant_type": "client_credentials",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=_config.crvs_oauth_http_timeout_seconds,
        )
        response.raise_for_status()
        token = response.json().get("access_token")
        if not token:
            raise ValueError("OAuth token response missing access_token")
        return str(token)

    def get_signed_request_body(self, payload: dict[str, Any]) -> dict[str, Any]:
        SHA_SECRET = self.sha_secret

        payload_str = json.dumps(payload, separators=(",", ":"), sort_keys=True)

        signature = hmac.new(
            SHA_SECRET.encode("utf-8"),
            payload_str.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        out = json.loads(payload_str)
        out["signature"] = signature

        return out

    def split_reg_records_into_payloads(
        self, response_body: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """
        Splits ``message.search_response[*].data.reg_records`` into one payload per
        record. Each payload is a deep copy of the full CRVS response body with a
        single-element ``reg_records`` list for the chosen block (preserves root
        ``signature`` and ``on-search`` envelope fields).
        """
        if not response_body:
            return []

        message = response_body.get("message") or {}
        search_response = message.get("search_response") or []

        if not isinstance(search_response, list) or not search_response:
            return []

        payloads: list[dict[str, Any]] = []

        for sr_index, sr_item in enumerate(search_response):
            data = (sr_item or {}).get("data") or {}
            reg_records = data.get("reg_records") or []

            if not isinstance(reg_records, list) or len(reg_records) == 0:
                continue

            for record in reg_records:
                new_body = deepcopy(response_body)

                new_body["message"]["search_response"][sr_index]["data"]["reg_records"] = [
                    record
                ]

                payloads.append(new_body)

        return payloads

    def send_registry_ingest_request(
        self,
        data_model: str,
        request_payload: dict,
        request_headers: dict,
    ) -> Response:
        try:
            response = requests.post(
                f"{self.registry_ingest_url}?data_model={data_model}",
                json=request_payload,
                headers={"Content-Type": "application/json"},
                timeout=30,
            )
            return response

        except requests.exceptions.RequestException as req_e:
            raise Exception(
                f"Network or request error calling registry ingest endpoint: {str(req_e)}"
            ) from req_e
        except Exception as e:
            raise Exception(f"Error occured processing ingest request: {str(e)}") from e

    def get_correlation_id(self, response_body: dict[str, Any]) -> str | None:
        # Partner IngestDataResponse: correlation_id lives under response_body.response_payload
        rb = response_body.get("response_body")
        if isinstance(rb, dict):
            payload = rb.get("response_payload")
            if isinstance(payload, dict):
                cid = payload.get("correlation_id")
                if cid:
                    return str(cid)
        # Legacy / CRVS callback shape
        message = response_body.get("message") or {}
        cid = message.get("correlation_id")
        return str(cid) if cid else None
