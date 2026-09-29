-- AIMC-compliant blob demonstrating the json lib and table/string usage
function aimc_main(sources)
    local raw = sources["body"]
    local decoded = json.decode(raw)
    local doubled = {}
    for k, v in pairs(decoded) do
        doubled[k] = v * 2
    end
    return {
        ["encoded"] = json.encode(doubled),
        ["keys"] = table.concat({ "a", "b" }, ","),
        ["upper"] = string.upper("aimc")
    }
end
