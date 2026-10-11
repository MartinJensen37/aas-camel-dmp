local function epochSeconds(timestamp)
    local year, month, day, hour, minute, second = timestamp:match(
        "^(%d%d%d%d)%-(%d%d)%-(%d%d)T(%d%d):(%d%d):(%d%d)")
    year, month, day = tonumber(year), tonumber(month), tonumber(day)
    hour, minute, second = tonumber(hour), tonumber(minute), tonumber(second)
    if not year then error("invalid ISO timestamp: " .. tostring(timestamp)) end
    local fraction = timestamp:match("^%d%d%d%d%-%d%d%-%d%dT%d%d:%d%d:%d%d(%.%d+)")
    local fractionalSeconds = fraction and tonumber("0" .. fraction) or 0
    local timezoneSign, timezoneHour, timezoneMinute = timestamp:match("([+-])(%d%d):?(%d%d)$")
    local offset = 0
    if timezoneSign then
        offset = tonumber(timezoneHour) * 3600 + tonumber(timezoneMinute) * 60
        if timezoneSign == "-" then offset = -offset end
    end
    local adjustedYear = year
    if month <= 2 then adjustedYear = adjustedYear - 1 end
    local era = math.floor(adjustedYear / 400)
    local yearOfEra = adjustedYear - era * 400
    local shiftedMonth = month + (month > 2 and -3 or 9)
    local dayOfYear = math.floor((153 * shiftedMonth + 2) / 5) + day - 1
    local dayOfEra = yearOfEra * 365 + math.floor(yearOfEra / 4)
        - math.floor(yearOfEra / 100) + dayOfYear
    return (era * 146097 + dayOfEra - 719468) * 86400 + hour * 3600
        + minute * 60 + second - offset + fractionalSeconds
end

function aimc_main(sources)
    local processData = sources.mqttSource.Process.ProcessData
    local seconds = math.floor(epochSeconds(processData.EndPorcess) - epochSeconds(processData.StartProcess))
    local duration = math.floor(seconds / 60) .. "M" .. (seconds % 60) .. "S"
    return { ["one/duration"] = duration, ["other/duration"] = duration }
end
