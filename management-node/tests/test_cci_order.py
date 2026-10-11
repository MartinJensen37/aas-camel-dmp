"""CCI positional output order — the awaited UA Call zip's contract.

The CCI operation's output_variable id_shorts ARE the positional contract of
the method's output arguments (the AID's embedded output DataSchema carries
types, not the method-signature order). The orchestrator walks the CCI
submodel, the resolver threads the order into the interaction, and the
descriptor emits ``responseOrder`` into the route's triggerData."""

from __future__ import annotations

import json
import pathlib

from management_node.orchestrator import _cci_operation_output_order
from management_node.parsers import aid as aid_parser, aimc as aimc_parser
from management_node.resolver import resolve

DATA_AID = pathlib.Path(__file__).parent / "data" / "syntegon_stoppering_aid.json"
DATA_AIMC = pathlib.Path(__file__).parent / "data" / "syntegon_stoppering_aimc.json"


def _cci_submodel() -> dict:
    """A minimal CCI submodel in the DotAAS v3 JSON shape: skills hang
    DIRECTLY under the Skills collection, each skill carries an Operation."""
    def variable(id_short: str) -> dict:
        return {
            "modelType": "OperationVariable",
            "value": {"modelType": "Property", "idShort": id_short},
        }

    def operation(output_ids: list[str]) -> dict:
        return {
            "modelType": "Operation",
            "idShort": "operation",
            "outputVariables": [{"value": {"modelType": "Property", "idShort": i}}
                                for i in output_ids],
        }

    def skill(name: str, output_ids: list[str]) -> dict:
        return {"modelType": "SubmodelElementCollection", "idShort": name,
                "value": [operation(output_ids)]}

    return {
        "modelType": "Submodel",
        "idShort": "ControlComponentInstance",
        "submodelElements": [{
            "modelType": "SubmodelElementCollection",
            "idShort": "Skills",
            "value": [
                skill("Halt", ["State"]),
                skill("Stoppering", ["State", "Outcome"]),
            ],
        }],
    }


def test_cci_operation_output_order_walks_v3_shape():
    order = _cci_operation_output_order(_cci_submodel())
    assert order == {"Halt": ["State"], "Stoppering": ["State", "Outcome"]}


def test_cci_operation_output_order_tolerates_garbage():
    assert _cci_operation_output_order({}) == {}
    assert _cci_operation_output_order({"Skills": None}) == {}
    assert _cci_operation_output_order(None) == {}


def _models():
    from aas_model.resource_template.asset_interfaces_description import ensure_aid_request_reply_terms

    aid_model = aid_parser.parse_aid(json.loads(DATA_AID.read_text()))
    ensure_aid_request_reply_terms(aid_model)
    aimc_model = aimc_parser.parse_aimc(json.loads(DATA_AIMC.read_text()))
    return aid_model, aimc_model


def test_resolver_threads_response_order_into_interactions():
    aid_model, aimc_model = _models()

    resolution = resolve(aimc_model, aid_model,
                         operation_output_order={"Stoppering": ["State", "Outcome"]})
    by_name = {i.id_short: i for i in resolution.interactions}
    assert by_name["Stoppering"].caller_response_order == ["State", "Outcome"]
    # undeclared actions keep an empty positional contract
    assert by_name["Halt"].caller_response_order == []

    # absent map → empty everywhere
    resolution2 = resolve(aimc_model, aid_model)
    assert all(not i.caller_response_order for i in resolution2.interactions)


def test_descriptor_emits_caller_output_order_not_ua_method_order():
    from management_node.descriptor import DescriptorCompiler

    aid_model, aimc_model = _models()
    resolution = resolve(aimc_model, aid_model,
                         operation_output_order={"Stoppering": ["State", "Outcome"]})

    compiler = DescriptorCompiler(aas_base_url="http://aas-env:8081",
                                  aas_id="https://x", aas_id_short="x",
                                  unit_name="dmp-x")
    descriptor = compiler.compile(resolution)
    routes = json.loads(descriptor.files["routes.json"])
    stoppering = [r for r in routes if r["routeId"].endswith("Stoppering_interaction")]
    assert stoppering, [r["routeId"] for r in routes]
    trigger_data = stoppering[0]["triggerData"]
    assert trigger_data["callerOutputOrder"] == ["State", "Outcome"]
