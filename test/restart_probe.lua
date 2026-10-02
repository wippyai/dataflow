local flow = require("flow")
local funcs = require("funcs")

local M = {}

local function hold_runtime()
    local events = process.events()
    while true do
        local result = channel.select({ events:case_receive() })
        if not result.ok then return end
        local event = result.value
        if event and event.kind == process.event.CANCEL then return end
    end
end

function M.create()
    -- Services may start while the dependency bootloader is still migrating.
    -- Use the same canonical readiness check as the ordinary test harness.
    local ready, readiness_err = funcs.call("app:wait_for_boot")
    if readiness_err or not ready then error(readiness_err or "boot is not ready") end
    local template = (flow.template() :: any):func("userspace.dataflow.node.func:test_func")
    local dataflow_id, start_err = (flow.create() :: any)
        :with_input({
            items = {
                { message = "restart slow", delay_ms = 9000 },
                { message = "restart fast 1", delay_ms = 2000 },
                { message = "restart fast 2", delay_ms = 2000 },
                { message = "restart fast 3", delay_ms = 2000 },
            }
        })
        :parallel({
            source_array_key = "items",
            batch_size = 2,
            scheduling = "rolling",
            on_error = "continue",
            template = template,
        })
        :start()
    if start_err or not dataflow_id then error(start_err or "workflow start failed") end
    hold_runtime()
end

function M.observe()
    hold_runtime()
end

return M
