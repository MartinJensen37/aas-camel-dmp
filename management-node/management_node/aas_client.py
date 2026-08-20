"""Minimal AAS REST API client (fetch submodels by id)."""

from __future__ import annotations

import logging
from typing import Any

import requests

logger = logging.getLogger(__name__)


class AasClient:
    """Thin client over the AAS submodel repository REST API."""

    def __init__(self, base_url: str, *, timeout: float = 10.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._session = requests.Session()

    def get_submodel(self, submodel_id: str) -> dict[str, Any]:
        """Fetch a submodel by its (URL-encoded) id from the submodel repository."""
        from urllib.parse import quote

        url = f"{self.base_url}/submodels/{quote(submodel_id, safe='')}"
        resp = self._session.get(url, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def get_submodel_by_short(self, aas_id: str, id_short: str) -> dict[str, Any]:
        """Fetch a submodel of an AAS by its idShort."""
        from urllib.parse import quote

        url = f"{self.base_url}/shells/{quote(aas_id, safe='')}/submodels/{quote(id_short, safe='')}"
        resp = self._session.get(url, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()
