function aimc_main(sources)
    local productId = sources["property1"].Account.Order[1].Product[1].ProductID
    return { ["ConnectedSubmodel/ConnectedPropertyB"] = productId }
end
