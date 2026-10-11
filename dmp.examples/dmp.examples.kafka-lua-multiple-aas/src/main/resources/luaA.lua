function aimc_main(sources)
    local products = {}
    for _, order in ipairs(sources["property1"].Account.Order) do
        for _, product in ipairs(order.Product) do
            products[#products + 1] = product
        end
    end
    return { ["luaA"] = products }
end
