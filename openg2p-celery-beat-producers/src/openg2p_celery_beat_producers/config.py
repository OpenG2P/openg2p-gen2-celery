from openg2p_fastapi_common.config import Settings as BaseSettings
from pydantic_settings import SettingsConfigDict

from . import __version__
from datetime import datetime

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="celery_jobs_beat_", env_file=".env", extra="allow"
    )
    openapi_title: str = "OpenG2P Celery Jobs"
    openapi_description: str = """
        Celery Jobs for OpenG2P
        ***********************************
        Further details goes here
        ***********************************
        """
    openapi_version: str = __version__

    # Celery Jobs Database
    db_username: str = "postgres"
    db_password: str = "password"
    db_hostname: str = "localhost"
    db_port: int = 5432
    db_dbname: str = "celery_jobs_db"

    # Celery Configuration
    celery_broker_url: str = "redis://localhost:6379/0"
    celery_backend_url: str = "redis://localhost:6379/0"
    worker_queue: str = "celery_jobs_worker_queue"

    batch_size: int = 2000
    no_of_tasks_to_process: int = 4

    g2p_celery_job_poll_frequency_seconds: int = 0      # Poll frequency seconds component
    g2p_celery_job_poll_frequency_minutes: int = 0      # Poll frequency minutes component
    g2p_celery_job_poll_frequency_hours: int = 6        # Poll frequency hours component
    g2p_celery_job_poll_frequency_days: int = 0         # Poll frequency days component
    g2p_celery_job_poll_frequency_weeks: int = 0        # Poll frequency weeks component
    g2p_celery_job_data_q_frequency: int = 10           # Check if able to push frequency in seconds
