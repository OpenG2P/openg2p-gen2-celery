import json

import hmac
import hashlib
import math
import uuid
from copy import deepcopy
from typing import Dict, List, Tuple, Any
from datetime import datetime
import requests
from requests import Response

from openg2p_celery_job_models.models import G2PExternalDataProvider

from ..config import Settings
from .helper import HelperInterface

_config = Settings.get_config()

class CrvsHelper(HelperInterface):
    def __init__(self):
        self.registry_ingest_url = _config.registry_ingest_url
        self.client_id = _config.openg2p_crvs_client_id
        self.client_secret = _config.openg2p_crvs_client_secret
        self.sha_secret = _config.openg2p_crvs_sha_secret
        self.entry_point_start_datetime = _config.entry_point_start_datetime
    
    def create_polling_request(self, data_provider: G2PExternalDataProvider, page_number: int, page_size: int = 10) -> Tuple[Dict, Dict]:
        
        current_utc_iso = datetime.utcnow().isoformat(timespec='seconds') + 'Z'
        gte_datetime = (data_provider.poll_latest_success_datetime.isoformat(timespec='seconds') + 'Z') if data_provider.poll_latest_success_datetime else self.entry_point_start_datetime

        request_body: Dict[str, Any] = {
        "header": {
            "version": "1.0.0",
            "message_id": "123456789020211216223812",
            "message_ts": "2022-12-04T18:01:07+00:00",
            "action": "search",
            "sender_id": "https://integrating-server.com",
            "sender_uri": "https://{server_url}/on-search",
            "receiver_id": "crvs",
            "total_count": 10,
            "encryption_algorithm": "DH-2048"
        },
        "message": {
            "transaction_id": "123456789020211216223812",
            "search_request": [
            {
                
                "reference_id": "123456789020211216223812",
                "timestamp": "2022-12-04T17:20:07-04:00",
                "search_criteria": {
                "version": "1.0.0",
                "reg_type": "ns:org:RegistryType:Civil",
                "reg_event_type": "birth",
                "query_type": "expression",
                "query": {
                    "type": "ns:org:QueryType:expression",
                    "value": {
                    "expression": {
                        "query": {
                        "dateOfEvent": {
                            "gte": gte_datetime,
                            "lte": current_utc_iso
                        }
                        }
                    }
                    }
                },
                "sort": [
                    {
                    "attribute_name": "createdAt",
                    "sort_order": "asc"
                    }
                ],
                "pagination": {
                    "page_size": 5,
                    "page_number": 1
                }
                }
            }
            ]
        }
        }

        request_header = {
            "Authorization": f"Bearer {self.get_oauth_token(data_provider.polling_base_url)}",
            "Content-Type": "application/json"
        }

        return request_header, request_body

    def send_polling_request(
        self, 
        data_provider: G2PExternalDataProvider,
    ) -> List[Response]:

        if data_provider.polling_base_url is None:
            raise Exception(f"Polling URL is not configured for {data_provider.provider_name} data provider")
        
        try:
            request_header, request_body = self.create_polling_request(data_provider, page_number=1, page_size=data_provider.polling_page_size)

            # request_body_signed = self.get_signed_request_body(request_body)
            response = requests.post(
                data_provider.polling_base_url + '/registry/sync/search',
                headers=request_header,
                json=request_body,
                timeout=10,
            )

            return response
            
            # TODO: implement pagination for search in crvs registry

        except requests.exceptions.RequestException as req_e:
            raise Exception(f"Request error calling polling url: {str(req_e)}")
        except Exception as e:
            raise Exception(f"Error during polling attempt: {str(e)}")
    
    def enrich_polling_response(self, response_body: Dict[str, Any]) -> Dict[str, Any]:
        query = self._get_section_query()
        response_body["query"] = query

        return response_body

    def get_oauth_token(self, base_url: str) -> str:
        oauth_token_url = f"{base_url}/oauth2/client/token?client_id={self.client_id}&client_secret={self.client_secret}&grant_type=client_credentials"
        response = requests.post(oauth_token_url)
        response.raise_for_status()
        return response.json().get("access_token")
    
    def get_signed_request_body(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        SHA_SECRET = self.sha_secret

        payload = json.dumps(payload, separators=(",", ":"), sort_keys=True)

        signature = hmac.new(
            SHA_SECRET.encode("utf-8"),
            payload.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()

        payload['signature'] = signature

        return payload
        
    
    def split_reg_records_into_payloads(self, response_body: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Splits response_body.message.search_response[*].data.reg_records into multiple payloads.
        Each output payload will contain exactly 1 reg_record per item.
        Everything else remains identical.
        """

        if not response_body:
            return []

        message = response_body.get("message") or {}
        search_response = message.get("search_response") or []

        if not isinstance(search_response, list) or not search_response:
            # Nothing to split
            return [response_body]

        payloads: List[Dict[str, Any]] = []

        for sr_index, sr_item in enumerate(search_response):
            data = (sr_item or {}).get("data") or {}
            reg_records = data.get("reg_records") or []

            # If no reg_records / not a list, keep as-is
            if not isinstance(reg_records, list) or len(reg_records) == 0:
                payloads.append(response_body)
                continue

            # Split each reg_record into its own payload
            for record in reg_records:
                new_body = deepcopy(response_body)

                new_body["message"]["search_response"][sr_index]["data"]["reg_records"] = [record]

                payloads.append(new_body)

        return payloads

    def send_registry_ingest_request(
        self,
        data_model: str,
        request_payload: Dict,
        request_headers: Dict,
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
            raise Exception(f"Network or request error calling registry ingest endpoint: {str(req_e)}")
        except Exception as e:
            raise Exception(f"Error occured processing ingest request: {str(e)}")

    def get_correlation_id(self, response_body: Dict[str, Any]) -> str:
        message = response_body.get("message") or {}
        return message.get("correlation_id")
