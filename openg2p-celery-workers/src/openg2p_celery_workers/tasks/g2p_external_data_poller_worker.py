from typing import Any

import logging
import uuid
import requests
from requests import Response
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
    _logger.info(f"Processing g2p_external_data_poller_worker for provider_id: {provider_id}")
    session_maker = sessionmaker(
        bind=_engine, expire_on_commit=False
    )

    with session_maker() as session:
        g2p_external_data_provider: G2PExternalDataProvider | None = None
        
        try:
            g2p_external_data_provider = session.get(G2PExternalDataProvider, provider_id)

            polling_helper: HelperInterface = HelperFactory.get_helper(g2p_external_data_provider.helper_class)
            
            poll_responses: list[Response] = polling_helper.send_polling_request(
                g2p_external_data_provider
            )
            if not poll_responses:
                _logger.info(
                    f"No poll responses returned for {g2p_external_data_provider.provider_name} "
                    f"(provider_id {provider_id})."
                )

            split_payloads: list[dict[str, Any]] = []
            response_headers: dict[str, Any] = {}

            for poll_response in poll_responses:
                poll_response.raise_for_status()
                response_body, response_headers = _get_response_body_headers(poll_response)
                split_payloads.extend(
                    polling_helper.split_reg_records_into_payloads(response_body)
                )

            _logger.info(
                f"Successfully polled {g2p_external_data_provider.provider_name} for provider_id "
                f"{provider_id}: {len(poll_responses)} page(s), {len(split_payloads)} queued record payload(s)."
            )

            if not split_payloads:
                _logger.info(
                    f"No reg_records split from CRVS response for provider_id {provider_id}; "
                    "advancing poll watermark after successful HTTP poll."
                )

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

            if g2p_external_data_provider is not None:
                g2p_external_data_provider.poll_latest_error_code = str(e)
                g2p_external_data_provider.poll_latest_datetime = func.now()

                session.commit()
            # Raise exception for testing
            raise e

        _logger.info(
            f"Completed processing g2p_external_data_poller_worker for provider_id: {provider_id}"
        )


def _get_response_body_headers(response: requests.Response) -> tuple[dict, dict]:
    response_body: dict = response.json() if response.content else {}
    response_headers: dict = dict(response.headers)

    return response_body, response_headers
