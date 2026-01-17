-- insert_external_data_provider.sql

-- If you want DB-side UUID generation:
-- Requires: CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

INSERT INTO g2p_external_data_providers (
    provider_id,
    provider_name,
    polling_base_url,
    polling_page_size,
    data_model,
    helper_class,
    external_data_q_worker,
    poll_latest_datetime,
    poll_latest_error_code,
    poll_latest_success_datetime,
    is_active,
    created_at,
    updated_at
) VALUES (
    gen_random_uuid()::text,
    'Sample Provider',
    'https://dci-crvs-api.vc-demo.opencrvs.dev',
    20,
    'dci',
    'crvs',
    'g2p_register_ingest_worker',
    NULL,
    NULL,
    NULL,
    TRUE,
    NOW(),
    NOW()
);
