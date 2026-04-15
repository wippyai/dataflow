-- Test stub that returns a 100_000-character memory so compaction size cap
-- can be exercised. The content is a repeating filler string.
local function handler(_args)
    local chunk = "oversize-" -- 9 chars
    local pieces = {}
    local total = 0
    while total < 100000 do
        pieces[#pieces + 1] = chunk
        total = total + #chunk
    end
    return { memory = table.concat(pieces) }
end

return { handler = handler }
