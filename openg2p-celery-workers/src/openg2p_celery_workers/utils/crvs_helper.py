import math
import uuid
import json
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
    
    def create_polling_request(self, page_number: int, page_size: int = 1) -> Tuple[Dict, Dict]:
        
        current_utc_iso = datetime.utcnow().isoformat(timespec='milliseconds') + 'Z'

        request_body: Dict[str, Any] = {
            "header": {
                "version": "1.0.0",
                "message_id": uuid.uuid4().hex,
                "message_ts": current_utc_iso,
                "action": "search",
                "sender_id": "spp.example.org",
                "sender_uri": "https://spp.example.org/{namespace}/callback/on-search",
                "receiver_id": "civilregistry.example.org",
                "is_msg_encrypted": False,
                "meta": {}
            },
            "message": {
                "transaction_id": uuid.uuid4().hex,
                "search_request": [
                    {
                        "reference_id": uuid.uuid4().hex,
                        "timestamp": current_utc_iso,
                        "search_criteria": {
                            "version": "1.0.0",
                            "reg_type": "ns:org:RegistryType:Civil",
                            "reg_record_type": "spdci-extensions-dci:Person",
                            "query_type": "expression",
                            "query": {
                                "type": "ns:org:QueryType:graphql",
                                "value": {
                                    "expression": "GeBirthRecordById {\n  person(UIN: \"1\") {\n    BRN\n    name\n    gender\n    birthDate\n    birthPlace\n    parents\n  }\n}\n"
                                }
                            },
                            "sort": [
                                {
                                    "attribute_name": "poverty_score",
                                    "sort_order": "asc"
                                }
                            ],
                            "pagination": {
                                "page_size": page_size,
                                "page_number": page_number
                            },
                            "consent": {
                                "@context": "https://schema.spdci.org/common/v1/api-schemas/Consent.jsonld",
                                "@type": "Consent",
                                "ts": current_utc_iso,
                                "purpose": {
                                    "text": {
                                        "type": "string"
                                    },
                                    "code": {
                                        "type": "string",
                                        "description": "From a fixed set, documented at refUri"
                                    },
                                    "ref_uri": {
                                        "type": "string",
                                        "format": "uri",
                                        "description": "Uri to provide more info on consent codes"
                                    }
                                }
                            },
                            "authorize": {
                                "@context": "https://schema.spdci.org/common/v1/api-schemas/Authorize.jsonld",
                                "@type": "Authorize",
                                "ts": current_utc_iso,
                                "purpose": {
                                    "text": {
                                        "type": "string"
                                    },
                                    "code": {
                                        "type": "string",
                                        "description": "From a fixed set, documented at refUri"
                                    },
                                    "ref_uri": {
                                        "type": "string",
                                        "format": "uri",
                                        "description": "Uri to provide more info on authorize codes"
                                    }
                                }
                            }
                        },
                        "locale": "en"
                    }
                ]
            }
        }
        request_header = {}

        return request_header, request_body

    def send_polling_request(
        self, 
        data_provider: G2PExternalDataProvider,
    ) -> List[Response]:

        if data_provider.polling_url is None:
            raise Exception(f"Polling URL is not configured for {data_provider.provider_name} data provider")

        responses: List[Response] = []
        
        try:
            _, initial_default_body = self.create_polling_request(data_provider, page_number=1)
            pagination_path = initial_default_body.get("message", {}).get("search_request", [{}])[0].get("search_criteria", {}).get("pagination")
            original_page_size = pagination_path.get("page_size", 1) if pagination_path else 1 # Default to 1 if not found

            request_header_initial, request_body_initial = self.create_polling_request(data_provider, page_number=1, page_size=1)
            # NOTE: sign the `request_body_initial` here
            request_body_initial["signature"] = "some_signature_value" # Placeholder for actual signature
            initial_response = requests.post(
                data_provider.polling_url,
                headers=request_header_initial,
                json=request_body_initial,
                timeout=10,
            )
            initial_response.raise_for_status()
            responses.append(initial_response)

            initial_response_data = initial_response.json()
            total_count = initial_response_data.get("header", {}).get("total_count")
            
            if total_count is None:
                return responses
            
            actual_page_size = original_page_size if original_page_size > 0 else 1 
            
            num_pages = math.ceil(total_count / actual_page_size)
            
            if num_pages <= 1:
                 return responses

            for page_number in range(1, num_pages + 1):
                if page_number == 1 and actual_page_size >= total_count:
                    continue

                request_header_paginated, request_body_paginated = self.create_polling_request(data_provider, page_number=page_number, page_size=actual_page_size)
                # NOTE: sign the `request_body_paginated` here
                request_body_paginated["signature"] = "some_signature_value" # Placeholder for actual signature
                response = requests.post(
                    data_provider.polling_url,
                    headers=request_header_paginated,
                    json=request_body_paginated,
                    timeout=10,
                )
                response.raise_for_status()
                responses.append(response)

            return responses

        except requests.exceptions.RequestException as req_e:
            raise Exception(f"Network or request error calling polling_url: {str(req_e)}")
        except Exception as e:
            raise Exception(f"Error during polling attempt: {str(e)}")
    
    def enrich_polling_response(self, response_body: Dict[str, Any]) -> Dict[str, Any]:
        query = self._get_section_query()
        response_body["query"] = query

        return response_body

    def _get_section_query(self) -> Dict[str, Any]:
        return {
            "type": "ns:org:QueryType:graphql",
            "value": {
                "expression": "GeBirthRecordById {\n  person(UIN: \"1\") {\n    BRN\n    name\n    gender\n    birthDate\n    birthPlace\n    parents\n  }\n}\n"
            }
        }
