# register-ingester-worker
import logging
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
from openg2p_registry_core.schemas import IngestDataResponse

from ..app import celery_app
from ..config import Settings
from ..engine import Engine
from ..utils import HelperFactory, HelperInterface

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = Engine.get_engine()


@celery_app.task(name="g2p_register_ingest_worker")
def g2p_register_ingest_worker(queue_id: str):
    _logger.info(f"Processing g2p_register_ingest_worker")
    session_maker = sessionmaker(
        bind=_engine, expire_on_commit=False
    )

    with session_maker() as session:
        g2p_external_data_queue: G2PExternalDataQueue | None = None
        try:
            g2p_external_data_queue = session.get(G2PExternalDataQueue, queue_id)
            g2p_external_data_payload = session.get(G2PExternalDataPayload, g2p_external_data_queue.payload_id)
            g2p_external_data_provider = session.get(G2PExternalDataProvider, g2p_external_data_queue.provider_id)

            ingest_helper: HelperInterface = HelperFactory.get_helper(g2p_external_data_provider.helper_class)

            raw_response: Response = ingest_helper.send_registry_ingest_request(
                data_model=g2p_external_data_provider.data_model,
                request_payload=g2p_external_data_payload.payload_json,
                request_headers=g2p_external_data_payload.payload_headers
            )

            if raw_response.status_code == 200:
                ingest_response = raw_response.json()
                _logger.info(
                    f"Registry ingestion request successful for queue_id {g2p_external_data_queue.queue_id} with response : {ingest_response}"
                )

                correlation_id = ingest_helper.get_correlation_id(ingest_response)

                g2p_external_data_queue.ingest_correlation_id = correlation_id
                g2p_external_data_queue.process_number_of_attempts += 1
                g2p_external_data_queue.process_status = StatusEnum.COMPLETED.value
                g2p_external_data_queue.process_latest_datetime = func.now()

            else:
                raise Exception(f"Ingestion failed for queue_id {g2p_external_data_queue.queue_id} with status code: {raw_response.status_code}")           
            
            session.commit()

        except Exception as e:
            _logger.error(
                f"Error during processing g2p_register_ingest_worker for queue_id {g2p_external_data_queue.queue_id}: {str(e)}"
            )
            session.rollback()

            g2p_external_data_queue.process_status = (
                StatusEnum.PENDING.value
                if g2p_external_data_queue.process_number_of_attempts < _config.worker_max_attempts
                else StatusEnum.FAILED.value
            )
            g2p_external_data_queue.process_number_of_attempts += 1
            g2p_external_data_queue.process_latest_error_code = str(e)
            g2p_external_data_queue.process_latest_datetime = func.now()

            session.commit()
            # Raise exception for testing
            raise e

        _logger.info(
            f"Completed processing g2p_register_ingest_worker for queue_id: {g2p_external_data_queue.queue_id}"
        )
