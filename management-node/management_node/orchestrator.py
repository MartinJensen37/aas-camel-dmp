"""Orchestrator — idempotent reconcile of per-DMP units (SPEC §11.2, ADR-021).

The managed set is keyed by the AIMC submodel INSTANCE id (exact anchor-id
attribution — never payload parents, suffix guesses, or idShort conventions):

- an event whose id matches a managed instance triggers its reconcile;
- a delete decommissions exactly that one unit (multi-asset deletes cannot
  collide anymore);
- everything else (AAS ids, label names) is DERIVED: the owning shell is
  looked up from the registry on demand and the local AID is bootstrapped
  from the AIMC's own references.

Reconciliation is idempotent and runs off the Kafka poll thread: each unit
has its own single-thread executor and rapid events coalesce (last-write-
wins), so bursts of updates cause at most one queued reconcile per unit.
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from .aas_client import AasClient
from .deploy import DmpDeployer
from .descriptor import DescriptorCompiler
from .kafka_listener import AimcEvent
from .parsers.aimc import parse_aimc
from .resolver import resolve, reference_values, _operation_source_keys
from .submodel_locator import (
    AID_SEMANTIC_ID, AidDirectory, OwningShellDirectory, referenced_element_type,
)

logger = logging.getLogger(__name__)

#: Bounded queue depth per unit; beyond this, coalesced reconciles are
#: dropped (the newest state always wins, so dropping older ones is safe).
_PER_UNIT_QUEUE_DEPTH = 2


def _broker_setting(value: str | None) -> tuple[str, int] | None:
    """Parse a deployment broker setting without putting it in AID/AIMC."""
    if not value:
        return None
    setting = value.strip()
    if "://" in setting:
        setting = setting.split("://", 1)[1]
    host, separator, port = setting.rpartition(":")
    if not separator:
        return setting, 9092
    try:
        return host, int(port)
    except ValueError as exc:
        raise ValueError(f"invalid AAS event broker {value!r}") from exc


def _cci_operation_output_order(submodel: dict) -> dict[str, list[str]]:
    """The CCI submodel's positional output order per skill: walk the
    ``Skills → Skill → <skill> → operation → outputVariables`` element chain
    (DotAAS v3 JSON shape) and collect the output arguments' idShorts in
    order — the DECLARED positional contract of each operation's output
    arguments (the AID's embedded output DataSchema carries types, not the
    positional order). Best-effort: unparsable shapes yield nothing."""
    order: dict[str, list[str]] = {}
    if not isinstance(submodel, dict):
        return order

    def find_collection(element, id_short):
        for key in ("submodelElements", "value", "statements"):
            children = element.get(key)
            if not isinstance(children, list):
                continue
            for child in children:
                if (isinstance(child, dict)
                        and child.get("modelType") == "SubmodelElementCollection"
                        and child.get("idShort") == id_short):
                    return child
        return None

    skills = find_collection(submodel, "Skills")
    if skills is None:
        return order
    # both authoring shapes occur: the skills hang DIRECTLY under Skills, or
    # behind an intermediate "Skill" collection
    skill_entries = find_collection(skills, "Skill") or skills
    for skill in skill_entries.get("value") or []:
        if not isinstance(skill, dict):
            continue
        name = skill.get("idShort")
        operation = None
        for child in skill.get("value") or []:
            if isinstance(child, dict) and child.get("modelType") == "Operation":
                operation = child
                break
        if operation is None:
            continue
        names = []
        for variable in operation.get("outputVariables") or []:
            value = variable.get("value")
            if isinstance(value, dict):
                id_short = value.get("idShort")
                if id_short:
                    names.append(id_short)
        order[str(name)] = names
    return order


def unit_name(aimc_sm_id: str, hint: str | None = None) -> str:
    """Deterministic per-unit name derived from the AIMC instance id (ADR-021).

    The hash makes container names/config dirs/health ports unique per AIMC
    instance regardless of asset naming collisions; an optional authoring
    ``label`` keeps them readable."""
    digest = hashlib.sha256(aimc_sm_id.encode("utf-8")).hexdigest()[:8]
    stem = re.sub(r"[^A-Za-z0-9_.-]", "", hint or "").strip(".-")
    return f"dmp-{stem}-{digest}" if stem else f"dmp-{digest}"


@dataclass(frozen=True)
class ManagedAimc:
    """One managed DMP unit = one AIMC SM instance (managed-set entry)."""

    aimc_sm_id: str
    #: optional human-readable naming hint (container/dir/port readability)
    label: str | None = None
    #: optional pinned local AID; when absent it is discovered from the AIMC's
    #: own references at bootstrap (majority anchor rule)
    local_aid_id: str | None = None

    @property
    def name(self) -> str:
        return unit_name(self.aimc_sm_id, self.label)


class _UnitWorker:
    """Single-threaded, last-write-wins reconcile queue for one unit."""

    def __init__(self, orchestrator: "Orchestrator", unit: ManagedAimc) -> None:
        self._orchestrator = orchestrator
        self._unit = unit
        self._pending: deque[bool] = deque(maxlen=_PER_UNIT_QUEUE_DEPTH)
        self._lock = threading.Lock()
        self._executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix=f"dmp-reconcile-{unit.name}")
        self._dropped = 0

    def submit(self) -> None:
        with self._lock:
            if len(self._pending) == self._pending.maxlen:
                self._dropped += 1
                # bounded queue: the oldest queued request is displaced by the
                # newer one (deque semantics); the newest state wins anyway.
                logger.warning(
                    "reconcile queue full for %s (%d dropped so far); "
                    "coalescing further", self._unit.name, self._dropped)
            self._pending.append(True)
        self._executor.submit(self._drain)

    def _drain(self) -> None:
        while True:
            with self._lock:
                if not self._pending:
                    return
                self._pending.clear()
            self._orchestrator.reconcile_now(self._unit)

    def flush(self, timeout: float = 30.0) -> None:
        """Blocks until everything submitted before this call has been
        reconciled (single-threaded FIFO executor + barrier no-op)."""
        self._executor.submit(lambda: None).result(timeout=timeout)

    def shutdown(self) -> None:
        self._executor.shutdown(wait=False)


class Orchestrator:
    def __init__(
        self,
        *,
        aas_base_url: str,
        config_root: str | Path,
        managed_aimcs: dict[str, ManagedAimc],
        image: str = "eclipsebasyx/databridge:1.0.0-SNAPSHOT",
        mqtt_broker: tuple[str, int] | None = None,
        affordance_validation: str = "OFF",
    ) -> None:
        self.aas_base_url = aas_base_url
        self.aas = AasClient(aas_base_url)
        # managed set keyed by the exact AIMC SM instance id (ADR-021)
        self.managed: dict[str, ManagedAimc] = {
            u.aimc_sm_id.rstrip("/"): u for u in managed_aimcs.values()}
        # image pull before deploy is opt-out for locally-built images
        # (docker pull of a local-only tag may hit a same-named remote image)
        pull_image = os.getenv("DMP_PULL_IMAGE", "true").strip().lower() \
            not in ("0", "false", "no")
        # deployment topology: host networking with hashed health ports is
        # the per-unit default; DMP_NETWORK=hivemq-network + a fixed
        # DMP_HEALTH_PORT puts the DMP on the AAS network under the
        # {delegation_base} DNS convention so the aas-env's operation
        # delegation resolves it
        self.dmp_network = os.getenv("DMP_NETWORK") or "host"
        self.dmp_health_port = int(os.getenv("DMP_HEALTH_PORT")) \
            if os.getenv("DMP_HEALTH_PORT") else None
        self.dmp_container_name = os.getenv("DMP_CONTAINER_NAME") or None
        self.deployer = DmpDeployer(image=image, pull_image=pull_image,
                                    network=self.dmp_network,
                                    health_port=self.dmp_health_port,
                                    affordance_validation=affordance_validation)
        self.mqtt_broker = mqtt_broker
        eventing_flag = os.getenv("AAS_EVENTING_ENABLED")
        eventing_enabled = eventing_flag is None or eventing_flag.strip().lower() \
            not in ("0", "false", "no", "off")
        self.aas_event_broker = _broker_setting(
            (os.getenv("AAS_EVENT_BROKER") or os.getenv("KAFKA_BOOTSTRAP"))
            if eventing_enabled else None
        )
        self.aas_event_topic = os.getenv("AAS_EVENT_TOPIC", "submodel-events")
        self.aas_event_group = os.getenv("AAS_EVENT_GROUP")
        self.config_root = Path(config_root)
        self._workers: dict[str, _UnitWorker] = {}
        self._workers_lock = threading.Lock()
        # Cross-asset resolution (ADR-019): parsed-AID cache keyed by the
        # referenced submodel instance itself; dependency invalidation.
        self.aid_directory = AidDirectory(self.aas)
        self.shell_directory = OwningShellDirectory(self.aas)
        # referenced submodel id -> set of aimc_sm_ids whose compiled DMP depends on it
        self._dependents: dict[str, set[str]] = {}
        self._dependencies_lock = threading.Lock()
        # unit -> effective local AID id (pinned or bootstrap-discovered)
        self._local_aids: dict[str, str] = {}

    # -- event handling --------------------------------------------------- #
    def handle_event(self, event: AimcEvent) -> None:
        """Kafka-thread entry point: never blocks on reconcile work."""
        aimc_sm_id = event.submodel_id.rstrip("/")
        if not aimc_sm_id:
            logger.warning("event without submodel id; ignoring")
            return
        if event.deleted and aimc_sm_id in self.managed:
            unit = self.managed[aimc_sm_id]
            logger.info("AIMC deleted for unit %s — decommissioning", unit.name)
            container_name = self.dmp_container_name or self._container_name_for(unit)
            self.deployer.stop(unit.name, container_name=container_name)
            self._clear_dependencies(unit.aimc_sm_id)
            return
        with self._dependencies_lock:
            referenced = aimc_sm_id in self._dependents or aimc_sm_id in self._local_aids.values()
        if event.semantic_id == AID_SEMANTIC_ID or (event.deleted and referenced):
            # AID updates and body-less deletes of referenced submodels must
            # invalidate the DMPs that used those exact reference anchors.
            self._handle_dependency_event(event)
            return
        unit = self.managed.get(aimc_sm_id)
        if unit is None:
            # exact-id attribution only (ADR-021): unknown ids are noise from
            # unmanaged instances and are dropped loudly once.
            logger.info("AIMC event for unmanaged submodel %s; ignoring",
                        event.submodel_id)
            return
        self.submit(unit)

    def _handle_dependency_event(self, event: AimcEvent) -> None:
        targets: list[ManagedAimc] = []
        submodel_id = (event.submodel_id or "").rstrip("/")
        seen: set[str] = set()
        # Optional pinned/bootstrap AID, plus all explicit reference users.
        with self._dependencies_lock:
            local_aids = tuple(self._local_aids.items())
            dependent_ids = set(self._dependents.get(submodel_id, set()))
        for aimc_id, local_aid in sorted(local_aids):
            if local_aid == submodel_id and aimc_id in self.managed:
                owner = self.managed[aimc_id]
                targets.append(owner)
                seen.add(owner.aimc_sm_id)
        for aimc_id in sorted(dependent_ids):
            unit = self.managed.get(aimc_id)
            if unit is not None and unit.aimc_sm_id not in seen:
                targets.append(unit)
                seen.add(unit.aimc_sm_id)
        if not targets:
            logger.info("reference event for %s touches no managed unit; ignoring",
                        event.submodel_id)
            return
        # Drop any cached AID parse before resolving the changed reference.
        self.aid_directory.invalidate(submodel_id)
        logger.info("reference change on %s -> reconciling %d unit(s)",
                    event.submodel_id, len(targets))
        for unit in targets:
            self.submit(unit)

    def submit(self, unit: ManagedAimc) -> None:
        """Queue an async reconcile for the unit (coalescing)."""
        worker = self._worker_for(unit)
        worker.submit()

    def reconcile(self, unit: ManagedAimc) -> None:
        """Synchronous reconcile — retained for tests/tools and startup scans."""
        try:
            self._reconcile_inner(unit)
        except Exception:
            logger.exception("reconcile failed for %s", unit.name)

    def index_managed_references(self) -> bool:
        """Rebuild AID dependencies from managed AIMCs before consuming events.

        This is read-only: a management-node restart must not depend on a new
        AIMC event to learn which DMPs a remote AID change affects.

        Returns True when every unit was indexed. A unit whose submodel is not
        fetchable yet (the repository is still starting, or the AIMC has not
        been registered) is logged and skipped; :func:`reindex_until_complete`
        retries the remainder in the background so a node that started too early
        still converges without a restart.
        """
        complete = True
        for unit in self.managed.values():
            try:
                model = self._fetch_parsed_aimc(unit)
                if model is not None:
                    self._record_dependencies(unit, model)
                else:
                    complete = False
            except Exception:
                complete = False
                logger.exception("could not index references for %s", unit.name)
        return complete

    def reindex_until_complete(
        self,
        *,
        interval_s: float = 15.0,
        attempts: int = 40,
        should_stop: Callable[[], bool] = lambda: False,
        sleep: Callable[[float], None] = time.sleep,
    ) -> threading.Thread:
        """Retries the reference index on a daemon thread until it succeeds.

        The index is what lets a remote AID change invalidate cross-AAS
        dependencies, so leaving it half-built is a silent correctness gap
        rather than a cosmetic one. Retrying in the background keeps startup
        fast while guaranteeing convergence.
        """

        def _retry() -> None:
            for attempt in range(1, attempts + 1):
                if should_stop():
                    return
                try:
                    if self.index_managed_references():
                        logger.info("reference index complete after %d attempt(s)", attempt)
                        return
                except Exception:
                    logger.exception("reference reindex attempt %d failed", attempt)
                logger.info(
                    "reference index incomplete; retrying in %.0fs (attempt %d/%d)",
                    interval_s, attempt, attempts)
                # Sleep in slices so shutdown is not held up by the backoff.
                remaining = interval_s
                while remaining > 0 and not should_stop():
                    sleep(min(0.5, remaining))
                    remaining -= 0.5

        thread = threading.Thread(
            target=_retry, name="dmp-reference-reindex", daemon=True)
        thread.start()
        return thread

    def reconcile_now(self, unit: ManagedAimc) -> None:
        """Synchronous reconcile that propagates failures to the worker log."""
        try:
            self._reconcile_inner(unit)
        except Exception:
            logger.exception("reconcile failed for %s", unit.name)

    def shutdown(self) -> None:
        with self._workers_lock:
            for worker in self._workers.values():
                worker.shutdown()
            self._workers.clear()

    def flush(self, timeout: float = 30.0) -> None:
        """Waits until all reconciles submitted so far have finished
        (determinism for tests; bounded wait for shutdown paths)."""
        with self._workers_lock:
            workers = list(self._workers.values())
        for worker in workers:
            worker.flush(timeout)

    # -- internals -------------------------------------------------------- #
    def _worker_for(self, unit: ManagedAimc) -> _UnitWorker:
        with self._workers_lock:
            worker = self._workers.get(unit.aimc_sm_id)
            if worker is None:
                worker = _UnitWorker(self, unit)
                self._workers[unit.aimc_sm_id] = worker
            return worker

    def _fetch_parsed_aimc(self, unit: ManagedAimc):
        raw = self.aas.get_submodel(unit.aimc_sm_id)
        model = parse_aimc(raw)
        if model is None:
            logger.error(
                "AIMC %s did not parse into the shared model (not a BaSyx "
                "Submodel?); skipping reconcile", unit.aimc_sm_id)
        return model

    def _cci_output_order(self, unit: ManagedAimc) -> dict[str, list[str]]:
        """The CCI operation's positional output order per action (the
        awaited UA Call zip's declared contract): the CCI submodel of the
        OWNING shell carries every Skill's operation with its output_variable
        id_shorts in order. Best-effort: a missing/unparsable CCI leaves the
        map empty and the reply falls back to the self-describing readback."""
        order: dict[str, list[str]] = {}
        try:
            owner = self.shell_directory.owner_of(unit.aimc_sm_id)
            if owner is None:
                return order
            for ref in self.aas.list_submodel_refs(owner.id):
                sm_id = ""
                for key in (ref.get("keys") or []) if isinstance(ref, dict) else []:
                    if key.get("type") == "Submodel":
                        sm_id = key.get("value") or ""
                if not sm_id.endswith("/submodels/ControlComponentInstance"):
                    continue
                order = _cci_operation_output_order(self.aas.get_submodel(sm_id))
                break
        except Exception as exc:
            logger.debug("unit %s: CCI output order unavailable (%s)",
                         unit.name, exc)
        return order

    def _discover_local_aid(self, unit: ManagedAimc, aimc_model) -> str | None:
        """Bootstrap AID discovery with NO ownership assumptions (W1): the
        local AID is the referenced anchor that (a) parses+validates as an
        AID (semanticId-gated via the directory) and (b) anchors the most
        interface-kind mappings (>= 50%); ties break deterministically by
        first appearance in source order. An explicit pin overrides.
        Operation-delegation mappings (DMP v3) anchor at their CCI, not an
        AID — they are skipped, they never define the local interface."""
        if unit.local_aid_id:
            pinned = unit.local_aid_id.rstrip("/")
            if self.aid_directory.get(pinned) is not None:
                return pinned
            logger.error("pinned local_aid_id %s of unit %s is not a valid AID; "
                         "refusing to select a different endpoint", pinned, unit.name)
            return None
        counts: dict[str, int] = {}
        order: dict[str, int] = {}
        mappings_total = 0
        for mc in aimc_model.MappingConfigurations.value:
            if _operation_source_keys(mc) is not None:
                continue
            interface_kind_refs = [
                reference_values(src.Source.value)[0]
                for src in mc.Sources.value
                if src.Source is not None and reference_values(src.Source.value)
            ]
            if not interface_kind_refs:
                continue
            mappings_total += 1
            for anchor in dict.fromkeys(interface_kind_refs):
                normalized = anchor.rstrip("/")
                counts[normalized] = counts.get(normalized, 0) + 1
                order.setdefault(normalized, len(order))
        best: tuple[int, int] | None = None
        best_anchor: str | None = None
        for anchor, count in sorted(counts.items(), key=lambda kv: order[kv[0]]):
            if count * 2 < mappings_total:
                continue  # below the >=50% threshold
            if self.aid_directory.get(anchor) is None:
                continue  # must parse+validate as an AID
            rank = (-count, order[anchor])
            if best is None or rank < best:
                best, best_anchor = rank, anchor
        if best_anchor is None:
            logger.info("unit %s: no majority AID anchor; resolving each "
                        "reference through its own submodel instance", unit.name)
            return None
        logger.info("unit %s: local AID bootstrapped to %s (referenced by %d/%d)",
                    unit.name, best_anchor, -best[0], mappings_total)
        return best_anchor

    def _reconcile_inner(self, unit: ManagedAimc) -> None:
        logger.info("reconciling DMP unit %s (%s)", unit.name, unit.aimc_sm_id)

        aimc_model = self._fetch_parsed_aimc(unit)
        if aimc_model is None:
            return

        aid_id = self._discover_local_aid(unit, aimc_model)
        if aid_id is None and unit.local_aid_id:
            self._record_dependencies(unit, aimc_model)
            self.deployer.stop(unit.name, container_name=self.dmp_container_name or
                               self._container_name_for(unit))
            return
        aid_model = self.aid_directory.get(aid_id) if aid_id else None
        if aid_id and aid_model is None:
            self._record_dependencies(unit, aimc_model)
            self.deployer.stop(unit.name, container_name=self.dmp_container_name or
                               self._container_name_for(unit))
            return  # already logged by the directory (semanticId gate / parse)
        with self._dependencies_lock:
            if aid_id:
                self._local_aids[unit.aimc_sm_id] = aid_id
            else:
                self._local_aids.pop(unit.aimc_sm_id, None)

        resolution = resolve(
            aimc_model, aid_model,
            aid_for_anchor=self.aid_directory.get,
            element_type_for_reference=lambda ref: referenced_element_type(self.aas, ref),
            operation_output_order=self._cci_output_order(unit),
        )
        if not resolution.all_resolved:
            unresolved = {m.id_short: m.unresolved for m in resolution.mappings if m.unresolved}
            logger.warning("unresolved endpoints for %s: %s", unit.name, unresolved)

        # The unit's AAS context is DERIVED from the registry: the shell that
        # owns this AIMC supplies legacy macros, operation paths and naming.
        # Direct source/sink references already contain their submodel ids.
        owner = self.shell_directory.owner_of(unit.aimc_sm_id)
        if owner is None:
            logger.warning("no owning shell found for %s; macro-substituted "
                           "references stay unresolved", unit.aimc_sm_id)

        compiler = DescriptorCompiler(
            aas_base_url=self.aas_base_url,
            aas_id=owner.id if owner else "{aas_id}",
            aas_id_short=owner.id_short if owner else "{aas_id_short}",
            unit_name=unit.name,
            mqtt_broker=self.mqtt_broker,
            aas_event_broker=self.aas_event_broker,
            aas_event_topic=self.aas_event_topic,
            aas_event_group=self.aas_event_group,
        )
        descriptor = compiler.compile(resolution)
        self._record_dependencies(unit, aimc_model)
        if not descriptor.files:
            logger.warning("no DMP config produced for %s; stopping stale deployment", unit.name)
            self.deployer.stop(unit.name, container_name=self.dmp_container_name or
                               self._container_name_for(unit))
            return

        config_dir = self.config_root / unit.name
        descriptor.write_to(config_dir)

        # per-unit container name: the {delegation_base} DNS convention
        # (dmp-<aas id_short lowercased>) when the owning shell is known —
        # multi-unit deployments each land under their own AAS's DNS name;
        # the DMP_CONTAINER_NAME override pins ONE unit (single-unit setups)
        if self.dmp_container_name:
            container_name = self.dmp_container_name
        elif owner is not None:
            container_name = f"dmp-{owner.id_short.lower()}"
        else:
            container_name = unit.name
        self.deployer.deploy(unit.name, str(config_dir),
                             container_name=container_name)
        logger.info("DMP for %s up to date", unit.name)

    def _container_name_for(self, unit: ManagedAimc) -> str:
        """The {delegation_base} DNS convention for one unit
        (``dmp-<aas id_short lowercased>``); the derived unit name when the
        owning shell is unknown (the per-unit default naming)."""
        owner = self.shell_directory.owner_of(unit.aimc_sm_id)
        if owner is not None:
            return f"dmp-{owner.id_short.lower()}"
        return unit.name

    def _record_dependencies(self, unit: ManagedAimc, aimc_model) -> None:
        """Reverse index: every referenced submodel id -> this unit's AIMC id,
        so an AID change elsewhere can invalidate the compiled DMP here."""
        local = unit.aimc_sm_id  # macro-less instances need no substitution
        referenced: set[str] = set()
        for mc in aimc_model.MappingConfigurations.value:
            for src in mc.Sources.value:
                keys = reference_values(src.Source.value)
                if keys:
                    referenced.add(_substitute_macros(keys[0], local))
            for snk in mc.Sinks.value:
                keys = reference_values(snk.Sink.value)
                if keys:
                    referenced.add(_substitute_macros(keys[0], local))
            for src in getattr(getattr(mc, "ResponseSources", None), "value", None) or []:
                keys = reference_values(src.Source.value)
                if keys:
                    referenced.add(_substitute_macros(keys[0], local))
            for snk in getattr(getattr(mc, "ResponseSinks", None), "value", None) or []:
                keys = reference_values(snk.Sink.value)
                if keys:
                    referenced.add(_substitute_macros(keys[0], local))
        if unit.local_aid_id:
            referenced.add(unit.local_aid_id.rstrip("/"))
        with self._dependencies_lock:
            if local_aid := self._local_aids.get(unit.aimc_sm_id):
                referenced.add(local_aid)
            self._clear_dependencies_locked(unit.aimc_sm_id)
            for ref in referenced:
                self._dependents.setdefault(ref, set()).add(unit.aimc_sm_id)

    def _clear_dependencies(self, aimc_sm_id: str) -> None:
        with self._dependencies_lock:
            self._clear_dependencies_locked(aimc_sm_id)
            self._local_aids.pop(aimc_sm_id, None)

    def _clear_dependencies_locked(self, aimc_sm_id: str) -> None:
        for ref, dependents in list(self._dependents.items()):
            dependents.discard(aimc_sm_id)
            if not dependents:
                del self._dependents[ref]


def _substitute_macros(value: str, aimc_sm_id: str) -> str:
    """Resolve remaining {aas_id} macros using the parent chain of the AIMC
    instance itself (``.../aas/<idShort>/submodels/<id>``) — references are
    anchored at precise instances, so macros only exist in legacy authorings."""
    if "{aas_id}" not in value and "{aas_id_short}" not in value:
        return value
    parent = aimc_sm_id.rsplit("/submodels/", 1)[0]
    short = parent.rsplit("/", 1)[-1]
    return value.replace("{aas_id}", parent).replace("{aas_id_short}", short)
