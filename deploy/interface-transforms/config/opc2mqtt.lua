-- M4 example: OPC UA -> MQTT (SPEC §10.2, state direction)
--
-- AIMC-compliant blob. Input (canonical OPC UA form, SPEC §10.6):
--   sources["body"] = { "nodeId": <id>, "value": <typed>,
--                       "status": <code>, "timestamp": <time> }
-- Output: compact MQTT state payload, published to aas/<asset>/State.
--
-- Protocol-agnostic: this blob only sees JSON.

function aimc_main(sources)
    local src = sources["body"] or {}

    local state = {
        ["value"] = src["value"],
        ["quality"] = src["status"],
        ["ts"] = src["timestamp"]
    }

    return {
        ["State"] = state
    }
end
