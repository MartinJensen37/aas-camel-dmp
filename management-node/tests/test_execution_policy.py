"""Declared-fact delivery classification (SPEC §6.1).

The classification helpers live in ``management_node.execution_policy``; the
Property classes they used to parse were REMOVED from the AIMC template
(decision 2026-09-15: execution is a fixed DMP runtime semantic, nothing is
authored on the mapping). This module tests the classification only.
"""

from __future__ import annotations

from management_node.execution_policy import (
    classify_delivery,
    protocol_from_declared_semantic_ids,
    protocol_from_interface_name,
)


def test_protocol_from_declared_semantic_ids():
    assert protocol_from_declared_semantic_ids(
        ["http://www.w3.org/2011/mqtt"]) == "mqtt"
    assert protocol_from_declared_semantic_ids(
        ["http://www.w3.org/2011/http"]) == "rest"
    assert protocol_from_declared_semantic_ids(["urn:unknown"]) == ""
    assert protocol_from_declared_semantic_ids(None) == ""


def test_protocol_from_interface_name_fallback():
    assert protocol_from_interface_name("interface_mqtt_rest") == "mqtt"
    assert protocol_from_interface_name("interface_opcua") == "opcua"
    assert protocol_from_interface_name("InterfaceTemplateForModbus") == "modbus"
    assert protocol_from_interface_name("iolink_over_profinet_mqtt") == "rest"
    assert protocol_from_interface_name("something_else") == ""


def test_event_affordance_is_async():
    assert classify_delivery(affordance_kind="event", protocol="rest") == "async"


def test_observe_op_makes_property_async():
    assert classify_delivery(
        affordance_kind="property", protocol="opcua",
        op="readproperty observeproperty") == "async"


def test_observable_true_is_async():
    assert classify_delivery(
        affordance_kind="property", protocol="opcua", observable=True) == "async"


def test_plain_http_property_is_sync():
    assert classify_delivery(
        affordance_kind="property", protocol="rest") == "sync"


def test_mqtt_is_async_except_actions():
    assert classify_delivery(
        affordance_kind="property", protocol="mqtt") == "async"
    assert classify_delivery(
        affordance_kind="action", protocol="mqtt") == "sync"
