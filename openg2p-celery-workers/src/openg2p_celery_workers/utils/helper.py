from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

import requests
from requests import Response

from openg2p_celery_job_models.models import G2PExternalDataProvider

from ..config import Settings

_config = Settings.get_config()


class HelperInterface(ABC):
    """
    Interface for helper classes
    Helper naming convention: <helper_class>Helper
    Example: CrvsHelper, DciHelper, UndpHelper
    """

    @abstractmethod
    def create_polling_request(
        self,
        g2p_external_data_provider: G2PExternalDataProvider,
        *,
        page_number: int = 1,
        page_size: int | None = None,
    ) -> tuple[dict, dict]:
        pass

    @abstractmethod
    def send_polling_request(
        self, g2p_external_data_provider: G2PExternalDataProvider
    ) -> list[Response]:
        pass

    @abstractmethod
    def enrich_polling_response(self, response_body: dict) -> dict:
        pass

    @abstractmethod
    def split_reg_records_into_payloads(
        self, response_body: dict[str, Any]
    ) -> list[dict[str, Any]]:
        pass

    def send_registry_ingest_request(
        self,
        data_model: str,
        request_payload: dict,
        request_headers: dict,
    ) -> Response:
        pass

    def _get_polling_datetime_range(
        self,
        poll_latest_datetime: datetime,
    ) -> tuple[datetime, datetime]:
        start_datetime: datetime = datetime.fromisoformat(
            _config.entry_point_start_datetime
        )
        end_datetime: datetime = datetime.now()

        if poll_latest_datetime is not None:
            start_datetime = poll_latest_datetime

        return start_datetime, end_datetime


class HelperFactory:
    """
    Factory class for helper classes
    """

    @staticmethod
    def get_helper(helper_type: str) -> HelperInterface:
        from .crvs_helper import CrvsHelper

        match helper_type.lower():
            case "crvs":
                return CrvsHelper()
            case _:
                raise NotImplementedError(f"Helper for {helper_type} is not implemented")
