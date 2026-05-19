from openg2p_fastapi_common.config import Settings as BaseSettings
from pydantic_settings import SettingsConfigDict
from datetime import datetime
from . import __version__


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="celery_jobs_worker_", env_file=".env", extra="allow"
    )
    openapi_title: str = "OpenG2P Celery Jobs Worker"
    openapi_description: str = """
        Celery Worker for OpenG2P Celery Jobs
        ***********************************
        Further details goes here
        ***********************************
        """
    openapi_version: str = __version__

    # Celery Jobs Database
    db_driver: str = "postgresql"
    db_username: str = "postgres"
    db_password: str = "password"
    db_hostname: str = "localhost"
    db_port: int = 5432
    db_dbname: str = "celery_jobs_db"

    # Registry Ingest URL (partner route is POST /partner/ingest_data)
    registry_ingest_url: str = "http://localhost:8000/partner/ingest_data"

    # Celery Configuration
    celery_broker_url: str = "redis://localhost:6379/0"
    celery_backend_url: str = "redis://localhost:6379/0"
    worker_queue: str = "celery_jobs_queue"

    batch_size: int = 2000
    worker_max_attempts: int = 3

    entry_point_start_datetime: str = "2026-01-01"

    # CRVS Config
    openg2p_crvs_client_id: str = "client_id"
    openg2p_crvs_client_secret: str = "secret"
    openg2p_crvs_sha_secret: str = ""

    crvs_max_pages_per_poll: int = 50
    crvs_oauth_http_timeout_seconds: float = 60.0
    crvs_search_http_timeout_seconds: float = 120.0
