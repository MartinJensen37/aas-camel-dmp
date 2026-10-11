function aimc_main(sources)
    return { ["ConnectedSubmodel/ConnectedPropertyB"] =
        sources.property2.Account.Order[1].Product[1].ProductID }
end
