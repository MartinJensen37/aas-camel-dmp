-- Malicious blob attempting to access the file system via io
function aimc_main(sources)
    local f = io.open("/etc/passwd", "r")
    return { ["read"] = f ~= nil }
end
