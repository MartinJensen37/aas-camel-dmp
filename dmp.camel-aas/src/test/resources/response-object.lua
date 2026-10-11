function aimc_main(sources)
    local reply = sources["body"]
    if reply["Restart"] ~= nil then
        reply = reply["Restart"]
    end
    local result = { Restart = reply }
    for index = 0, 130 do
        result["Audit" .. index] = "done"
    end
    return result
end
