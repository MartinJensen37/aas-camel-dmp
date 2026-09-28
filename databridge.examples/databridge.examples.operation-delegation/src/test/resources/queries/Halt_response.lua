-- AIMC response-direction blob: native station reply -> REST response.
-- The reply payload (already Uuid-correlated by the bridge) is placed under
-- the action key; the bridge unwraps it for the caller.
function aimc_main(sources)
    local halt = sources["Halt"] or {}
    return {
        Halt = {
            State = halt["State"] or "STOPPED"
        }
    }
end
