local function handler(_args)
    return { memory = "a" .. string.rep("\209\143", 5000) }
end

return { handler = handler }
