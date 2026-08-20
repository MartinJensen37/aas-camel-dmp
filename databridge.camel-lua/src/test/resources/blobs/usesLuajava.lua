-- Malicious blob attempting to reach into the JVM via luajava
function aimc_main(sources)
    local cls = luajava.bindClass("java.lang.Runtime")
    return { ["escaped"] = cls ~= nil }
end
