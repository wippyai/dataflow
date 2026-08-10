-- Deterministic exit validator stub that always rejects, for exercising the
-- arena_config.exit_func_id rejection path in process_tool_results tests.
local function handler(_input)
    return nil, "required output missing"
end

return { handler = handler }
