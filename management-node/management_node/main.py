"""Management node entry point (SPEC §11, ADR-013).

Env vars (kg-bridge conventions, ADR-010):
  KAFKA_BOOTSTRAP          (default kafka:9092)
  KAFKA_TOPIC_PATTERN      (default (aas-events|submodel-events))
  KAFKA_CONSUMER_GROUP     (default dmp-management-node)
  KAFKA_AUTO_OFFSET_RESET  (default earliest)
  AAS_BASE_URL             AAS submodel repository base URL (required)
  DMP_CONFIG_ROOT          where DMP config dirs are written (default ./dmp-config)
  DMP_IMAGE                DMP container image (default eclipsebasyx/databridge:1.0.0-SNAPSHOT)
  MANAGED_ASSETS_FILE      JSON file mapping aas_id -> {aas_id_short, aid_id_short?, aimc_id_short?}
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

from .kafka_listener import AimcEventListener
from .orchestrator import AssetRef, Orchestrator

logger = logging.getLogger("management-node")


def load_assets(path: str) -> dict[str, AssetRef]:
    raw = json.loads(Path(path).read_text())
    assets: dict[str, AssetRef] = {}
    for aas_id, ref in raw.items():
        assets[aas_id] = AssetRef(
            aas_id=aas_id,
            aas_id_short=str(ref.get("aas_id_short", aas_id)),
            aid_id_short=str(ref.get("aid_id_short", "AssetInterfacesDescription")),
            aimc_id_short=str(ref.get("aimc_id_short", "AssetInterfacesMappingConfiguration")),
        )
    return assets


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    aas_base_url = os.getenv("AAS_BASE_URL")
    if not aas_base_url:
        raise SystemExit("AAS_BASE_URL is required (AAS submodel repository base URL)")

    assets_file = os.getenv("MANAGED_ASSETS_FILE")
    if not assets_file:
        raise SystemExit("MANAGED_ASSETS_FILE is required (JSON: aas_id -> asset ref)")
    assets = load_assets(assets_file)

    orchestrator = Orchestrator(
        aas_base_url=aas_base_url,
        config_root=os.getenv("DMP_CONFIG_ROOT", "./dmp-config"),
        assets=assets,
        image=os.getenv("DMP_IMAGE", "eclipsebasyx/databridge:1.0.0-SNAPSHOT"),
    )

    listener = AimcEventListener(
        orchestrator.handle_event,
        bootstrap=os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
        topic_pattern=os.getenv("KAFKA_TOPIC_PATTERN", "(aas-events|submodel-events)"),
        consumer_group=os.getenv("KAFKA_CONSUMER_GROUP", "dmp-management-node"),
        auto_offset_reset=os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest"),
    )

    logger.info("management node started: aas=%s assets=%d", aas_base_url, len(assets))
    listener.run_forever()


if __name__ == "__main__":
    main()
