local MEMORIES = { "m1", "m2", "m3", "m4", "m5", "m6" }

local function recall(args)
    local seen = {}
    for _, id in ipairs(args and args.previous_memories or {}) do
        seen[id] = true
    end
    for _, id in ipairs(MEMORIES) do
        if not seen[id] then
            return { memories = { { id = id, content = "recalled " .. id } } }
        end
    end
    return { memories = {} }
end

return { recall = recall }
