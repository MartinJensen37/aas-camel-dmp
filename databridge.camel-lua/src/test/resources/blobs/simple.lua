-- AIMC-compliant transformation blob (contract test fixture)
-- sources: map keyed by SourceId; return: map keyed by SinkId
function aimc_main(sources)
    local a = sources["a"] or 0
    local b = sources["b"] or 0
    return {
        ["sum"] = a + b,
        ["product"] = a * b
    }
end
