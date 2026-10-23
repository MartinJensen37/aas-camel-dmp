function aimc_main(sources)
    local result = { Restart = sources["Restart"] }
    for index = 0, 130 do
        result["Audit" .. index] = "done"
    end
    return result
end
