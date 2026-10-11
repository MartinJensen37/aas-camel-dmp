function aimc_main(sources)
    local product = sources.mqttSource1.Account.Order[1].Product[1]
    return {
        ["ConnectedTestSubmodel/DotAASV3ConformantApiProperty"] = product.Description.Weight
    }
end
