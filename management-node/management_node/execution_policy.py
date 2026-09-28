"""Declared-fact delivery classification (SPEC §6.1, protocol-agnostic).

Execution is a FIXED DMP runtime semantic (decision 2026-09-15, replacing the
ADR-020 authorable policy): every mapping evaluates on any source update —
an async arrival or the sync source's declared polling interval — once ALL of
its sources have delivered; sync sources are read at their declared IDTA
polling interval. Nothing about execution is authored on the AIMC, so there
is no policy parsing here anymore. This module keeps the classification
helpers the descriptor and registration tooling share.
"""

from __future__ import annotations

__all__ = [
    "TD_TRANSPORT_SEMANTIC_IDS",
    "NAME_PREFIX_PROTOCOLS",
    "PUSH_PROTOCOLS",
    "OBSERVE_OP_MARKERS",
    "EVENT_AFFORDANCE",
    "ACTION_AFFORDANCE",
    "PROPERTY_AFFORDANCE",
    "protocol_from_interface_name",
    "protocol_from_declared_semantic_ids",
    "classify_delivery",
]

# --------------------------------------------------------------------------- #
# Declared-fact delivery classification (SPEC §6.1, protocol-agnostic)
# --------------------------------------------------------------------------- #
#
# Transport classes derived from W3C-TD supplemental semantic ids on the
# AID interface. Push transports deliver asynchronously by their very
# definition; pull transports are request/response reads. Per-affordance
# declarations (`op` containing observe*/subscribe*, observable=true,
# event-group membership) override toward push; nothing else does.

#: TD transport semantic id -> ("push"|"pull", canonical protocol name)
TD_TRANSPORT_SEMANTIC_IDS = {
    "http://www.w3.org/2011/mqtt": ("push", "mqtt"),
    "https://www.w3.org/2019/wot/hypermedia#sse": ("push", "sse"),
    "https://www.w3.org/2019/wot/hypermedia#websub": ("push", "websub"),
    "http://www.w3.org/2011/http": ("pull", "rest"),
    "https://www.w3.org/2011/http": ("pull", "rest"),
}

#: Canonical names for non-TD protocols declared via interface-name prefixes.
NAME_PREFIX_PROTOCOLS = {
    "mqtt": "mqtt",
    "opcua": "opcua",
    "modbus": "modbus",
    "plc4x": "plc4x",
    "http": "rest",
    "rest": "rest",
    "bacnet": "bacnet",
    "iolink_over_profinet_rest": "rest",
}

PUSH_PROTOCOLS = frozenset({"mqtt"})
#: Operation markers declaring subscription semantics on an affordance form.
OBSERVE_OP_MARKERS = ("observe", "subscribeevent", "subscribe")

EVENT_AFFORDANCE = "event"
ACTION_AFFORDANCE = "action"
PROPERTY_AFFORDANCE = "property"


def protocol_from_interface_name(interface_name: str) -> str:
    """Documented FALLBACK ONLY (ADR-020): derives the protocol from the
    historical ``interface_<prot>[_rest]`` name convention. Authoritative
    detection uses the interface's supplemental TD semantic ids."""
    stem = interface_name.removeprefix("interface_").removeprefix("InterfaceTemplateFor").lower()
    if stem.startswith("iolink_over_profinet"):
        return "rest"
    return NAME_PREFIX_PROTOCOLS.get(stem.split("_")[0], "")


def protocol_from_declared_semantic_ids(semantic_ids) -> str:
    """The canonical protocol declared by the interface's supplemental
    semantic ids (W3C TD transports); '' when none matches."""
    for sid in semantic_ids or []:
        entry = TD_TRANSPORT_SEMANTIC_IDS.get(str(sid).rstrip("/"))
        if entry:
            return entry[1]
    return ""


def classify_delivery(*, affordance_kind: str, protocol: str, op: str = "",
                      observable: bool | None = None) -> str:
    """Async/sync role of one affordance, from DECLARED facts only:

    1. event affordances push;
    2. affordances whose ``op`` declares observe/subscribe semantics push;
    3. properties with ``observable=true`` push;
    4. transport-declared push protocols (MQTT) push everything that is not
       explicitly a read form;
    5. everything else is pulled (polled request/response)."""
    kind = (affordance_kind or "").lower()
    if kind == EVENT_AFFORDANCE:
        return "async"
    lowered = (op or "").lower()
    if any(marker in lowered for marker in OBSERVE_OP_MARKERS):
        return "async"
    if observable is True:
        return "async"
    if protocol in PUSH_PROTOCOLS and kind != ACTION_AFFORDANCE:
        return "async"
    return "sync"
