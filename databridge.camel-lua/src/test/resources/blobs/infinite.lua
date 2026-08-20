-- Malicious/infinite-loop blob: must be killed by the CPU timeout hook
function aimc_main(sources)
    local i = 0
    while true do
        i = i + 1
    end
    return { ["i"] = i }
end
