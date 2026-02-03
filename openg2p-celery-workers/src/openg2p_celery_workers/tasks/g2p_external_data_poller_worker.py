from copy import deepcopy
from typing import Any, Dict, List
import logging
import uuid
import requests
from requests import Response
from typing import Tuple, Dict, List
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func
from openg2p_celery_job_models.models import (
    StatusEnum, 
    G2PExternalDataProvider,
    G2PExternalDataQueue,
    G2PExternalDataPayload,
)

from ..app import celery_app
from ..config import Settings
from ..engine import Engine
from ..utils import HelperFactory, HelperInterface

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = Engine.get_engine()


@celery_app.task(name="g2p_external_data_poller_worker")
def g2p_external_data_poller_worker(provider_id: str):
    _logger.info(f"Processing g2p_external_data_poller_worker")
    session_maker = sessionmaker(
        bind=_engine, expire_on_commit=False
    )

    with session_maker() as session:
        g2p_external_data_provider: G2PExternalDataProvider | None = None
        
        try:
            g2p_external_data_provider = session.get(G2PExternalDataProvider, provider_id)

            polling_helper: HelperInterface = HelperFactory.get_helper(g2p_external_data_provider.helper_class)
            
            poll_response: Response = polling_helper.send_polling_request(
                g2p_external_data_provider
            )
            poll_response.raise_for_status()
            _logger.info(
                f"Successfully received data item from {g2p_external_data_provider.provider_name} for provider_id {provider_id}."
            )
            response_body, response_headers = _get_response_body_headers(poll_response)

            split_payloads = polling_helper.split_reg_records_into_payloads(response_body)

            # If split produced nothing (edge case), fallback to inserting original once

            for single_payload in split_payloads:
                g2p_external_data_payload = G2PExternalDataPayload(
                    payload_id=str(uuid.uuid4()),
                    payload_json=single_payload,
                    payload_headers=response_headers
                )
                session.add(g2p_external_data_payload)

                g2p_external_data_queue = G2PExternalDataQueue(
                    provider_id=provider_id,
                    payload_id=g2p_external_data_payload.payload_id,
                    process_status=StatusEnum.PENDING.value,
                    created_at=func.now()
                )
                session.add(g2p_external_data_queue)

            g2p_external_data_provider.poll_latest_error_code = None
            g2p_external_data_provider.poll_latest_datetime = func.now()
            g2p_external_data_provider.poll_latest_success_datetime = func.now()

            session.commit()

        except Exception as e:
            _logger.error(
                f"Error during processing g2p_external_data_poller_worker for provider_id {provider_id}: {str(e)}"
            )
            session.rollback()

            g2p_external_data_provider.poll_latest_error_code = str(e)
            g2p_external_data_provider.poll_latest_datetime = func.now()

            session.commit()
            # Raise exception for testing
            raise e

        _logger.info(
            f"Completed processing g2p_external_data_poller_worker for provider_id: {provider_id}"
        )


def _get_response_body_headers(response: requests.Response) -> Tuple[Dict, Dict]:

    response_body: Dict = response.json() if response.content else {}
    response_headers: Dict = dict(response.headers)

    return response_body, response_headers
