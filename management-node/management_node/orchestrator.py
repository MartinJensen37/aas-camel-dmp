"""Orchestrator — idempotent reconcile of per-asset DMPs (SPEC §11.2).

Reacts to AIMC events, fetches the AID+AIMC submodels, parses them with the
dedicated (separated) parsers, resolves references, compiles the DMP
descriptor, writes the config and (re)deploys the DMP container.

Reconciliation is idempotent and single-flight per asset: repeated events for
the same asset are coalesced, and replayed Kafka events are harmless.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

from .aas_client import AasClient
from .deploy import DmpDeployer
from .descriptor import DescriptorCompiler
from .kafka_listener import AimcEvent
from .parsers.aid import parse_aid
from .parsers.aimc import parse_aimc
from .resolver import resolve

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AssetRef:
    """How the management node reaches an asset's AID + AIMC submodels."""

    aas_id: str
    aas_id_short: str
    aid_id_short: str = "AssetInterfacesDescription"
    aimc_id_short: str = "AssetInterfacesMappingConfiguration"


class Orchestrator:
    def __init__(
        self,
        *,
        aas_base_url: str,
        config_root: str | Path,
        assets: dict[str, AssetRef],
        image: str = "eclipsebasyx/databridge:1.0.0-SNAPSHOT",
    ) -> None:
        self.aas = AasClient(aas_base_url)
        self.assets = dict(assets)
        self.deployer = DmpDeployer(image=image)
        self.compiler = DescriptorCompiler(aas_base_url=aas_base_url)
        self.config_root = Path(config_root)
        self._inflight: set[str] = set()

    # -- event handling --------------------------------------------------- #
    def handle_event(self, event: AimcEvent) -> None:
        asset = self._find_asset(event)
        if asset is None:
            logger.warning("no managed asset for AIMC event submodel=%s", event.submodel_id)
            return
        if event.deleted:
            logger.info("AIMC deleted for %s — decommissioning DMP", asset.aas_id_short)
            self.deployer.stop(asset.aas_id_short)
            return
        self.reconcile(asset)

    def reconcile(self, asset: AssetRef) -> None:
        """Single-flight, idempotent reconcile for one asset."""
        if asset.aas_id in self._inflight:
            logger.debug("reconcile for %s already in flight", asset.aas_id)
            return
        self._inflight.add(asset.aas_id)
        try:
            self._reconcile_inner(asset)
        except Exception:
            logger.exception("reconcile failed for %s", asset.aas_id_short)
        finally:
            self._inflight.discard(asset.aas_id)

    # -- internals -------------------------------------------------------- #
    def _reconcile_inner(self, asset: AssetRef) -> None:
        logger.info("reconciling DMP for %s (%s)", asset.aas_id_short, asset.aas_id)

        aimc = self.aas.get_submodel_by_short(asset.aas_id, asset.aimc_id_short)
        aid = self.aas.get_submodel_by_short(asset.aas_id, asset.aid_id_short)

        aimc_model = parse_aimc(aimc)
        aid_model = parse_aid(aid)

        resolution = resolve(aimc_model, aid_model)
        if not resolution.all_resolved:
            unresolved = {m.id_short: m.unresolved for m in resolution.mappings if m.unresolved}
            logger.warning("unresolved sources for %s: %s", asset.aas_id_short, unresolved)

        descriptor = self.compiler.compile(resolution)
        if not descriptor.files:
            logger.warning("no DMP config produced for %s; skipping deploy", asset.aas_id_short)
            return

        config_dir = self.config_root / asset.aas_id_short
        descriptor.write_to(config_dir)

        self.deployer.deploy(asset.aas_id_short, str(config_dir))
        logger.info("DMP for %s up to date", asset.aas_id_short)

    def _find_asset(self, event: AimcEvent) -> AssetRef | None:
        # 1) by explicit AAS reference in the event payload (if present)
        aas_id = _aas_id_from_payload(event.payload)
        if aas_id and aas_id in self.assets:
            return self.assets[aas_id]

        # 2) by matching the AIMC submodel id against managed assets
        submodel_id = (event.submodel_id or "").rstrip("/")
        for asset in self.assets.values():
            aimc_id = f"{asset.aas_id}/submodels/{asset.aimc_id_short}"
            if submodel_id == aimc_id or submodel_id.endswith(f"/{asset.aimc_id_short}"):
                return asset
        return None


def _aas_id_from_payload(payload: dict | None) -> str | None:
    if not payload:
        return None
    parent = payload.get("parent")
    if isinstance(parent, dict):
        keys = parent.get("keys") or []
        if keys and keys[0].get("type") == "AssetAdministrationShell":
            return str(keys[0].get("value"))
    ref = payload.get("aasId") or payload.get("aas_id")
    return str(ref) if ref else None
