# OpenG2P Celery Workers

## CRVS DCI poller → registry ingest

The **external data poller** task pulls vital events from an OpenCRVS API (DCI-compatible flow), splits each `reg_record` into its own queued payload, and the **register ingest** worker posts each payload to the registry partner API (`POST /partner/ingest_data`).

### Flow

1. **Beat** (`openg2p-celery-beat-producers`) enqueues `g2p_external_data_poller_worker` per active row in `g2p_external_data_providers`.
2. **Poller worker** (`crvs_helper`): OAuth2 **form POST** to `{polling_base_url}/oauth2/client/token`, then one or more **paginated** `POST {polling_base_url}/registry/sync/search` calls until `reg_records` is empty or pagination indicates the last page (capped by `celery_jobs_worker_crvs_max_pages_per_poll`).
3. Each HTTP 200 **on-search** body is split so `message.search_response[*].data.reg_records` becomes **one JSON root per record** (full body deep-copied, including root `signature` when present).
4. Rows are inserted into `g2p_external_data_payloads` / `g2p_external_data_queue` (`PENDING`).
5. After a **successful** poll (all pages HTTP 2xx), `poll_latest_success_datetime` is updated even when there are **zero** records (avoids repolling the same window forever).
6. **Queue beat** dispatches `g2p_register_ingest_worker`, which POSTs to `celery_jobs_worker_registry_ingest_url?data_model=...` (default path ends with **`/partner/ingest_data`**). The registry resolves `correlation_id` from the partner **IngestDataResponse** (`response_body.response_payload.correlation_id`).

### Worker environment (`CELERY_JOBS_WORKER_*`)

| Variable | Purpose |
|----------|---------|
| `CELERY_JOBS_WORKER_DB_*` | PostgreSQL connection for `celery_jobs_db` |
| `CELERY_JOBS_WORKER_CELERY_BROKER_URL` / `CELERY_JOBS_WORKER_CELERY_BACKEND_URL` | Redis (or other) broker/backend |
| `CELERY_JOBS_WORKER_REGISTRY_INGEST_URL` | Base URL for partner ingest, e.g. `https://partner.example.org/partner/ingest_data` |
| `CELERY_JOBS_WORKER_ENTRY_POINT_START_DATETIME` | ISO UTC lower bound when `poll_latest_success_datetime` is null (e.g. `2026-01-01T00:00:00Z`) |
| `CELERY_JOBS_WORKER_OPENG2P_CRVS_CLIENT_ID` / `CELERY_JOBS_WORKER_OPENG2P_CRVS_CLIENT_SECRET` | OAuth2 client credentials |
| `CELERY_JOBS_WORKER_CRVS_MAX_PAGES_PER_POLL` | Max search pages per poller run (default 50; always at least 1 page attempted) |
| `CELERY_JOBS_WORKER_CRVS_OAUTH_HTTP_TIMEOUT_SECONDS` | OAuth HTTP timeout |
| `CELERY_JOBS_WORKER_CRVS_SEARCH_HTTP_TIMEOUT_SECONDS` | Search HTTP timeout |
| `CELERY_JOBS_WORKER_WORKER_MAX_ATTEMPTS` | Ingest retries |

### Beat environment (`CELERY_JOBS_BEAT_*`)

See `openg2p-celery-beat-producers` `config.py`: DB settings, broker URLs, `no_of_tasks_to_process`, and poll / queue processor intervals (`g2p_celery_job_poll_frequency_*`, `g2p_celery_job_data_q_frequency`).

### Provider row (`g2p_external_data_providers`)

| Column | Notes |
|--------|--------|
| `polling_base_url` | CRVS API root (no trailing slash required) |
| `polling_page_size` | `search_criteria.pagination.page_size` |
| `reg_event_type` | `birth` or `death` (search criteria; matches `crvs_script.py`) |
| `data_model` | Registry data-model mnemonic, e.g. **`CRVSVC`** (ingest service uppercases for lookup) |
| `helper_class` | `crvs` → `CrvsHelper` |
| `external_data_q_worker` | e.g. `g2p_register_ingest_worker` |

Database artifacts live under **`openg2p-gen2-celery/db-scripts/`**:

| File | Purpose |
|------|---------|
| `g2p_celery_job_tables.sql` | `CREATE TABLE` for `g2p_external_data_providers`, `g2p_external_data_queue`, and `g2p_external_data_payloads` (aligned with **openg2p-celery-job-models**) |
| `seed_data.sql` | Optional seed for **`g2p_external_data_providers` only** (default: one **`death`** row, **`CRVSVC`**) |

`g2p_external_data_payloads` and `g2p_external_data_queue` are filled at runtime by the poller and ingest pipeline; **do not** seed them manually.

### NSR / registry semantics (Civil + Person)

Death-style CRVS payloads use **`reg_type`** `ns:org:RegistryType:Civil` and **`reg_record_type`** `spdci-extensions-dci:Person`. Play / NSR seeds add semantic pattern **`SP3`** (and register pattern **`REG-SEM-PATTERN-CRVS-PERSON`** where `incoming_model_register_semantic_patterns` is used) so classification matches **`identifier[0].identifier_value`** on the person record. Existing **`Member`** / **`Individual`** rows remain for household-style payloads.
