import uuid
import enum
from datetime import datetime
from sqlalchemy import String, Date, Boolean, DateTime, JSON, Integer
from sqlalchemy.orm import Mapped, mapped_column
from openg2p_fastapi_common.models import BaseORMModel


class StatusEnum(enum.Enum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

class G2PExternalDataQueue(BaseORMModel):

    __tablename__ = "g2p_external_data_queue"

    queue_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    provider_id: Mapped[str] = mapped_column(String, nullable=False)
    payload_id: Mapped[str] = mapped_column(String, nullable=False)

    registry_ingest_id: Mapped[str] = mapped_column(String, nullable=True)

    process_status: Mapped[str] = mapped_column(String, default=StatusEnum.PENDING.value, nullable=False)
    process_latest_datetime: Mapped[datetime] = mapped_column(DateTime, nullable=True)
    process_latest_error_code: Mapped[str] = mapped_column(String, nullable=True)
    process_number_of_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.now())
