-- Malicious blob attempting to load modules via require
function aimc_main(sources)
    local m = require("os")
    return { ["loaded"] = m ~= nil }
end
