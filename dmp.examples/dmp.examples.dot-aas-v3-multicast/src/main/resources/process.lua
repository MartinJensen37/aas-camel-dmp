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
        offset = (tonumber(timezoneHour) * 3600 + tonumber(timezoneMinute) * 60)
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
    local days = era * 146097 + dayOfEra - 719468
    return days * 86400 + hour * 3600 + minute * 60 + second - offset + fractionalSeconds
end

function aimc_main(sources)
    local message = sources.mqttSource
    local process = message.Process
    local processData = process.ProcessData
    local diff = epochSeconds(processData.EndPorcess) - epochSeconds(processData.StartProcess)
    local wholeSeconds = math.floor(diff)
    local duration = math.floor(wholeSeconds / 60) .. "M" .. (wholeSeconds % 60) .. "S"
    return {
        ["process/processId"] = process.ProcessID,
        ["process/processResult"] = process.Result,
        ["process/processData"] = { ProcessData = processData },
        ["process/processData_raw"] = { mqttSource = message },
        ["process/processDuration"] = duration
    }
end
