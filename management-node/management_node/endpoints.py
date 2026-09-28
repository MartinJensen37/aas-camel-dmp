"""Shared endpoint parsing — one implementation for both consumers.

The descriptor compiler and the resolver both decompose AID endpoint bases
(``<scheme>://host[:port][/path]``); this module is the single home for that
grammar so MQTT/OPC UA/HTTP bases are parsed identically everywhere.
"""

from __future__ import annotations

from urllib.parse import parse_qsl, urlsplit

def parse_endpoint(base: str | None) -> tuple[str | None, int | None, str]:
    """Parses an endpoint base into ``(host, port, path)``.

    Unparseable input is returned verbatim as the path component (the caller
    decides whether that is fatal)."""
    if not base:
        return None, None, ""
    host, port, path, options = parse_endpoint_options(base)
    if options:
        raise ValueError(f"endpoint {base!r} has query options unsupported by this connector")
    return host, port, path


def parse_endpoint_options(base: str | None) -> tuple[str | None, int | None, str, str]:
    """Parse an AID base including PLC4X query options without losing them."""
    if not base:
        return None, None, "", ""
    parts = urlsplit(base)
    if not parts.scheme or not parts.netloc or not parts.hostname:
        return None, None, base, ""
    if parts.fragment or parts.username is not None or parts.password is not None:
        raise ValueError(f"endpoint {base!r} has unsupported fragment or credentials")
    try:
        port = parts.port
    except ValueError as exc:
        raise ValueError(f"endpoint {base!r} has an invalid port") from exc
    if port is not None and not 1 <= port <= 65535:
        raise ValueError(f"endpoint {base!r} has an invalid port")
    options = parts.query
    if options:
        pairs = parse_qsl(options, keep_blank_values=True, strict_parsing=True)
        if not pairs or any(not key for key, _ in pairs) or len({key for key, _ in pairs}) != len(pairs):
            raise ValueError(f"endpoint {base!r} has invalid or duplicate query options")
    return parts.hostname, port, parts.path, options
