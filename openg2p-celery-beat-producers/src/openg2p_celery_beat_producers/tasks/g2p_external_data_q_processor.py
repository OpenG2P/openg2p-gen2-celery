# openg2p-crvs-dci-search

import logging
from typing import List

from openg2p_celery_job_models.models import G2PExternalDataProvider, G2PExternalDataQueue, StatusEnum
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from ..app import celery_app
from ..config import Settings
from ..engine import Engine
from ..utils import Workers

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = Engine.get_engine()


@celery_app.task(name="g2p_external_data_q_processor_beat_producer")
def g2p_external_data_q_processor_beat_producer():
    _logger.info("Processing g2p_external_data_q_processor_beat_producer")

    session_maker = sessionmaker(bind=_engine, expire_on_commit=False)
    
    with session_maker() as session:
        g2p_external_data_queue: List[G2PExternalDataQueue] = (
            session.execute(
                select(G2PExternalDataQueue)
                .filter(
                    G2PExternalDataQueue.process_status == StatusEnum.PENDING.value
                )
                .limit(_config.no_of_tasks_to_process)
            )
            .scalars()
            .all()
        )
        _logger.info(f"Found {len(g2p_external_data_queue)} active g2p_external_data_queue requests")

        for g2p_external_data_q in g2p_external_data_queue:
            # Get worker name from g2p_external_data_providers
            g2p_external_data_provider: G2PExternalDataProvider = session.get(G2PExternalDataProvider, g2p_external_data_q.provider_id)
            _logger.info(f"Queueing g2p_external_data_queue with queue_id: {g2p_external_data_q.queue_id} for processing")

            g2p_external_data_q.process_status = StatusEnum.PROCESSING.value
            # Send task to appropriate celery worker
            celery_app.send_task(
                g2p_external_data_provider.external_data_q_worker,
                args=(g2p_external_data_q.queue_id,),
                queue=_config.worker_queue,
            )
            _logger.info(
                f"Sent task to {g2p_external_data_provider.external_data_q_worker} for ingestion with queue_id: {g2p_external_data_q.queue_id}"
            )
        session.commit()

    _logger.info("Completed processing pending g2p_external_data_queue requests")
