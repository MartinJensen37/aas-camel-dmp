"""Dedicated submodel parsers.

Kept largely separated on purpose (SPEC §11, ADR-013):
- ``aimc`` parses the Asset Interfaces Mapping Configuration (AIMC).
- ``aid``  parses the Asset Interfaces Description (AID).

Neither parser imports the other; the AIMC parser never mentions AID and vice
versa. Linking AIMC references to AID endpoints is the job of the separate
``management_node.resolver``.
"""
