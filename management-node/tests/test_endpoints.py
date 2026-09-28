from __future__ import annotations

import pytest

from management_node.endpoints import parse_endpoint, parse_endpoint_options


def test_endpoint_parser_separates_service_path_and_query_options():
    assert parse_endpoint_options("tcp://host:502/service?unit=2") == (
        "host", 502, "/service", "unit=2")


def test_legacy_endpoint_parser_rejects_silently_dropped_query():
    with pytest.raises(ValueError, match="query options unsupported"):
        parse_endpoint("tcp://host:502?unit=2")


@pytest.mark.parametrize("endpoint", [
    "tcp://host:bad", "tcp://host:502/service#fragment",
    "tcp://user:password@host:502", "tcp://host:502?unit=2&unit=3",
])
def test_endpoint_parser_rejects_malformed_authority_or_ambiguous_options(endpoint):
    with pytest.raises(ValueError):
        parse_endpoint_options(endpoint)


def test_endpoint_parser_supports_ipv6_authority():
    assert parse_endpoint_options("opc.tcp://[::1]:4840/server") == (
        "::1", 4840, "/server", "")
