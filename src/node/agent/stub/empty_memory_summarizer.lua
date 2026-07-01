-- Test stub that returns an empty memory so checkpoint is treated as a soft failure.
local function handler(_args)
    return { memory = "" }
end

return { handler = handler }
