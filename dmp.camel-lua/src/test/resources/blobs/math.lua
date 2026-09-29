-- AIMC-compliant transformation blob using the math library
-- (demonstrates the math expressiveness requirement that motivated Lua)
function aimc_main(sources)
    local value_1 = sources["source1"] or 0
    local value_2 = sources["source2"] or 0
    return {
        ["hypot"] = math.sqrt(value_1 * value_1 + value_2 * value_2),
        ["abs"] = math.abs(value_1),
        ["label"] = string.format("(%.1f, %.1f)", value_1, value_2)
    }
end
