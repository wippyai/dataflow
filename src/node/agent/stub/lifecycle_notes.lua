local ctx = require("ctx")

local function handler(payload)
    local phase = tostring(payload and payload.phase)
    local options = ctx.get("options") or {}
    local host = payload and payload.host or {}
    local messages = {}
    for _, role in ipairs(options[phase] or {}) do
        messages[#messages + 1] = {
            role = role,
            content = string.format("note:%s:%s:%s", phase, tostring(host.iteration), role),
        }
    end
    return { messages = messages }
end

return { handler = handler }
