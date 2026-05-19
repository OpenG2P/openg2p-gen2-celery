-- Seed: one CRVS civil death provider (data_model CRVSVC). No queue/payload rows.
-- Safe to re-run: fixed provider_id and ON CONFLICT DO NOTHING.

INSERT INTO g2p_external_data_providers (
    provider_id,
    provider_name,
    polling_base_url,
    polling_page_size,
    reg_event_type,
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
    '00000000-0000-4000-8000-00000000c102',
    'CRVS Civil Death',
    'https://dci-crvs-api.vc-demo.opencrvs.dev',
    20,
    'death',
    'DCI',
    'crvs',
    'g2p_register_ingest_worker',
    NULL,
    NULL,
    NULL,
    TRUE,
    NOW(),
    NOW()
)
ON CONFLICT (provider_id) DO NOTHING;
