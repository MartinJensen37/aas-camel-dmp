-- M4 example: MQTT -> OPC UA (SPEC §10.2, command direction)
--
-- AIMC-compliant blob. Input (MQTT payload parsed as JSON, SPEC §10.6):
--   sources["body"] = { "command": "start", "parameter": 42 }
-- Output: canonical OPC UA write object; the OpcUaNormalizer extracts
-- `.value` and writes it as a Milo Variant to the configured node.
--
--   { "nodeId": "ns=2;s=Command", "value": "start" }

function aimc_main(sources)
    local cmd = sources["body"] or {}

    return {
        ["nodeId"] = "ns=2;s=Command",
        ["value"] = cmd["command"]
    }
end
