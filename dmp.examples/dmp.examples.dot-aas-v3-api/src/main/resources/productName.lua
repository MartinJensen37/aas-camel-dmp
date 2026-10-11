function aimc_main(sources)
    local product = sources.mqttSource2.Account.Order[1].Product[1]
    return {
        ["ConnectedTestSubmodel/DotAASV3ConformantApiStringProperty"] = product["Product Name"]
    }
end
