import uuid
from datetime import datetime
from sqlalchemy import String, Boolean, DateTime, func
from sqlalchemy.orm import Mapped, mapped_column
from openg2p_fastapi_common.models import BaseORMModel


class G2PExternalDataProvider(BaseORMModel):

    __tablename__ = "g2p_external_data_providers"

    provider_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    provider_name: Mapped[str] = mapped_column(String, nullable=False)
    polling_url: Mapped[str] = mapped_column(String, nullable=False)
    data_model: Mapped[str] = mapped_column(String, nullable=False)
    helper_class: Mapped[str] = mapped_column(String, nullable=False)
    external_data_q_worker: Mapped[str] = mapped_column(String, nullable=False)

    poll_latest_datetime: Mapped[datetime] = mapped_column(DateTime, nullable=True)
    poll_latest_error_code: Mapped[str] = mapped_column(String, nullable=True)
    poll_latest_success_datetime: Mapped[datetime] = mapped_column(DateTime, nullable=True)

    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=True, onupdate=func.now())
