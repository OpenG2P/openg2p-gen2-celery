-- DDL for celery job models (openg2p-celery-job-models).
-- Mirrors SQLAlchemy ORM; safe to re-run with IF NOT EXISTS.
-- Runtime tables g2p_external_data_queue and g2p_external_data_payloads are not seeded.

CREATE TABLE IF NOT EXISTS g2p_external_data_providers (
    provider_id VARCHAR NOT NULL,
    provider_name VARCHAR NOT NULL,
    polling_base_url VARCHAR NOT NULL,
    polling_page_size INTEGER NOT NULL,
    reg_event_type VARCHAR(32) NOT NULL,
    data_model VARCHAR NOT NULL,
    helper_class VARCHAR NOT NULL,
    external_data_q_worker VARCHAR NOT NULL,
    poll_latest_datetime TIMESTAMP WITHOUT TIME ZONE,
    poll_latest_error_code VARCHAR,
    poll_latest_success_datetime TIMESTAMP WITHOUT TIME ZONE,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE,
    PRIMARY KEY (provider_id)
);

CREATE TABLE IF NOT EXISTS g2p_external_data_queue (
    queue_id VARCHAR NOT NULL,
    provider_id VARCHAR NOT NULL,
    payload_id VARCHAR NOT NULL,
    ingest_correlation_id VARCHAR,
    process_status VARCHAR NOT NULL DEFAULT 'PENDING',
    process_latest_datetime TIMESTAMP WITHOUT TIME ZONE,
    process_latest_error_code VARCHAR,
    process_number_of_attempts INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (queue_id)
);

CREATE TABLE IF NOT EXISTS g2p_external_data_payloads (
    payload_id VARCHAR NOT NULL,
    payload_json JSON,
    payload_headers JSON,
    PRIMARY KEY (payload_id)
);
