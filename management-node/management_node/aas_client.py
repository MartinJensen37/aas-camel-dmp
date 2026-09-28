"""Minimal AAS REST API client (fetch submodels by id).

The BaSyx AAS 2.0 environment addresses identifiers in URL paths via
UTF8-BASE64-URL encoding (RFC 4648 §5, unpadded) — verified against the
live aas-env (``/v3/api-docs``): ``/submodels/{submodelIdentifier}`` and
``/shells/{aasIdentifier}/submodel-refs``.
"""

from __future__ import annotations

import base64
import logging
from typing import Any

import requests

logger = logging.getLogger(__name__)


def _b64url(identifier: str) -> str:
    """UTF8-BASE64-URL encode an AAS identifier for a URL path (unpadded)."""
    return base64.urlsafe_b64encode(identifier.encode("utf-8")).decode("ascii").rstrip("=")


class AasClient:
    """Thin client over the AAS submodel repository REST API."""

    def __init__(self, base_url: str, *, timeout: float = 10.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._session = requests.Session()

    def get_submodel(self, submodel_id: str) -> dict[str, Any]:
        """Fetch a submodel by its id from the submodel repository."""
        url = f"{self.base_url}/submodels/{_b64url(submodel_id)}"
        resp = self._session.get(url, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def get_submodel_by_short(self, aas_id: str, id_short: str) -> dict[str, Any]:
        """Fetch a submodel of an AAS by its idShort.

        The AAS env exposes no ``/shells/{id}/submodels/{idShort}`` path, so we
        list the AAS submodel refs, find the id matching ``{aas_id}/submodels/
        {id_short}``, and fetch it from the submodel repository.
        """
        refs_url = f"{self.base_url}/shells/{_b64url(aas_id)}/submodel-refs"
        resp = self._session.get(refs_url, timeout=self.timeout)
        resp.raise_for_status()
        expected = f"{aas_id.rstrip('/')}/submodels/{id_short}"
        for ref in resp.json().get("result", []):
            keys = ref.get("keys") or []
            if keys and str(keys[0].get("value", "")).rstrip("/") == expected:
                return self.get_submodel(str(keys[0]["value"]))
        raise LookupError(f"no submodel {id_short!r} on AAS {aas_id} (refs: "
                          f"{[ (k.get('value')) for r in resp.json().get('result', []) for k in (r.get('keys') or [])][:6]})")

    def list_submodel_refs(self, aas_id: str) -> list[dict[str, Any]]:
        """The submodel references of one AAS (``GET /shells/{id}/submodel-refs``)."""
        refs_url = f"{self.base_url}/shells/{_b64url(aas_id)}/submodel-refs"
        resp = self._session.get(refs_url, timeout=self.timeout)
        resp.raise_for_status()
        return list(resp.json().get("result", []))

    def list_shells(self) -> list[dict[str, Any]]:
        """All shell descriptors (``GET /shells``); used to derive the owning
        shell of a managed AIMC instance from the registry itself."""
        resp = self._session.get(f"{self.base_url}/shells", timeout=self.timeout)
        resp.raise_for_status()
        return list(resp.json().get("result", []))
