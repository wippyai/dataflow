local run_context = require("run_context")

local M = {}

function M.handle(args)
    return run_context.get_prompt(args)
end

return M
