"""Management node entry point (SPEC §11, ADR-013, ADR-021).

Env vars (kg-bridge conventions, ADR-010):
  KAFKA_BOOTSTRAP          (default kafka:9092)
  KAFKA_TOPIC_PATTERN      (default (aas-events|submodel-events))
  KAFKA_CONSUMER_GROUP     (default dmp-management-node)
  KAFKA_AUTO_OFFSET_RESET  (default earliest)
  AAS_BASE_URL             AAS submodel repository base URL (required)
  DMP_CONFIG_ROOT          where DMP config dirs are written (default ./dmp-config)
  DMP_IMAGE                DMP container image (default eclipsebasyx/databridge:1.0.0-SNAPSHOT)
  MANAGED_ASSETS_FILE      JSON managed set keyed by AIMC submodel instance id:
                           { "<aimc_sm_id>": {"label"?: str, "local_aid_id"?: str} }
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

from .kafka_listener import AimcEventListener
from .orchestrator import ManagedAimc, Orchestrator

logger = logging.getLogger("management-node")


def load_managed_aimcs(path: str) -> dict[str, ManagedAimc]:
    """Loads the managed set keyed by AIMC SM instance id (ADR-021).

    Container names, config dirs and health ports derive from the instance id
    (via :func:`management_node.orchestrator.unit_name`); ownership of the AAS
    and the local AID are derived at reconcile time."""
    raw = json.loads(Path(path).read_text())
    units: dict[str, ManagedAimc] = {}
    for aimc_sm_id, cfg in raw.items():
        cfg = cfg or {}
        units[aimc_sm_id] = ManagedAimc(
            aimc_sm_id=str(aimc_sm_id),
            label=str(cfg["label"]) if cfg.get("label") else None,
            local_aid_id=str(cfg["local_aid_id"]) if cfg.get("local_aid_id") else None,
        )
    return units


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    aas_base_url = os.getenv("AAS_BASE_URL")
    if not aas_base_url:
        raise SystemExit("AAS_BASE_URL is required (AAS submodel repository base URL)")

    managed_file = os.getenv("MANAGED_ASSETS_FILE")
    if not managed_file:
        raise SystemExit(
            "MANAGED_ASSETS_FILE is required "
            "(JSON: AIMC SM instance id -> {label?, local_aid_id?})")
    managed = load_managed_aimcs(managed_file)

    mqtt_broker = None
    if raw := os.getenv("DMP_MQTT_BROKER"):
        host, _, port = raw.partition(":")
        mqtt_broker = (host.strip(), int(port or 1883))

    orchestrator = Orchestrator(
        aas_base_url=aas_base_url,
        config_root=os.getenv("DMP_CONFIG_ROOT", "./dmp-config"),
        managed_aimcs=managed,
        image=os.getenv("DMP_IMAGE", "eclipsebasyx/databridge:1.0.0-SNAPSHOT"),
        mqtt_broker=mqtt_broker,
    )

    listener = AimcEventListener(
        orchestrator.handle_event,
        bootstrap=os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
        topic_pattern=os.getenv("KAFKA_TOPIC_PATTERN", "(aas-events|submodel-events)"),
        consumer_group=os.getenv("KAFKA_CONSUMER_GROUP", "dmp-management-node"),
        auto_offset_reset=os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest"),
    )

    # The reference index is what lets a remote AID change invalidate cross-AAS
    # dependencies. Index synchronously so a warm start is fully indexed before
    # events flow, then keep retrying in the background when the repository was
    # not reachable yet (cold-start ordering).
    if not orchestrator.index_managed_references():
        logger.warning(
            "reference index incomplete at startup; retrying in the background")
        orchestrator.reindex_until_complete(should_stop=lambda: not listener.is_running)

    logger.info("management node started: aas=%s managed AIMC units=%d",
                aas_base_url, len(managed))
    try:
        listener.run_forever()
    finally:
        orchestrator.shutdown()


if __name__ == "__main__":
    main()
