function aimc_main(sources)
    local products = sources.property1.Account.Order.Product
    if products.Price ~= nil then products = { products } end
    local total = 0
    for _, product in ipairs(products) do
        total = total + (tonumber(product.Price) or 0) * (tonumber(product.Quantity) or 0)
    end
    return { ["ConnectedSubmodel/ConnectedPropertyA"] = total }
end
