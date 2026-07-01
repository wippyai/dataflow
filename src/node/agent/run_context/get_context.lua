local run_context = require("run_context")

local M = {}

function M.handle(args)
    return run_context.get_context(args)
end

return M
