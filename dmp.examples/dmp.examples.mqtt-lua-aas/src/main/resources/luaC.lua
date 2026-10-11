function aimc_main(sources)
    local productId = sources["property3"].Account.Order[1].Product[1].ProductID
    return { ["ConnectedTestSubmodel/ConnectedPropertyC"] = productId }
end
