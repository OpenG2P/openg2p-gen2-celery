# openg2p-gen2-celery
Celery Beats and Workers for OpenG2P (across all modules) - typically for integration with partner ecosystems.

## `db-scripts`

- **`g2p_celery_job_tables.sql`** — DDL for celery job models (`g2p_external_data_providers`, `g2p_external_data_queue`, `g2p_external_data_payloads`).
- **`seed_data.sql`** — optional seed for **`g2p_external_data_providers`** only (single **`death`** row, **`CRVSVC`**). Queue and payloads are not seeded.
