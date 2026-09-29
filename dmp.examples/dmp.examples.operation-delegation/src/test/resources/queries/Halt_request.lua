-- AIMC request-direction blob: REST operation variables -> native command.
-- `sources.Halt` carries the caller's JSON body; the native station expects a
-- flat command payload (the correlation Uuid is injected by the bridge).
function aimc_main(sources)
    local halt = sources["Halt"] or {}
    return {
        Halt = {
            Command = halt["Command"] or "STOP"
        }
    }
end
