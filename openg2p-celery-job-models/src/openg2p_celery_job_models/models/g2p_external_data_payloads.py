import uuid
from sqlalchemy import String, JSON
from sqlalchemy.orm import Mapped, mapped_column
from openg2p_fastapi_common.models import BaseORMModel


class G2PExternalDataPayload(BaseORMModel):

    __tablename__ = "g2p_external_data_payloads"

    payload_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))

    payload_json: Mapped[dict] = mapped_column(JSON, nullable=True)
    payload_headers: Mapped[dict] = mapped_column(JSON, nullable=True)
