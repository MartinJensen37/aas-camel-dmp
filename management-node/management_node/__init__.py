"""Management node for aas-camel-dmp (SPEC §11, ADR-013).

The management node reacts to AAS events, parses the asset's AID and AIMC
submodels, compiles a DMP descriptor (routes.json / luatransformer.json /
Lua blobs) and instantiates/updates/stops per-asset DMP containers.

The AID and AIMC parsers are kept largely separated (see ``parsers/``): each
is self-contained and knows nothing about the other; reference resolution and
descriptor compilation are separate steps.
"""

__version__ = "0.1.0"
