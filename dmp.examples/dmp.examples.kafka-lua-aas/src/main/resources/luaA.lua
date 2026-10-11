function aimc_main(sources)
    local total = 0
    local orders = sources["property1"].Account.Order
    for _, order in ipairs(orders) do
        for _, product in ipairs(order.Product) do
            total = total + (tonumber(product.Price) or 0) * (tonumber(product.Quantity) or 0)
        end
    end
    return { ["ConnectedSubmodel/ConnectedPropertyA"] = total }
end
