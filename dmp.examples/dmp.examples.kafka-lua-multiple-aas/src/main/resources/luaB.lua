function aimc_main(sources)
    local total = 0
    local products = sources["luaA"]
    if products.Price ~= nil then
        products = { products }
    end
    for _, product in ipairs(products) do
        total = total + (tonumber(product.Price) or 0)
    end
    return { ["ConnectedSubmodel/ConnectedPropertyA"] = total }
end
