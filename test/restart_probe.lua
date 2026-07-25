local uuid = require("uuid")
local time = require("time")
local client = require("client")
local consts = require("consts")

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
    local dataflow_client, client_err = client.new()
    if client_err or not dataflow_client then error(client_err or "dataflow client unavailable") end

    local node_id = uuid.v7()
    local dataflow_id, create_err = dataflow_client:create_workflow({
        {
            type = consts.COMMAND_TYPES.CREATE_NODE,
            payload = {
                node_id = node_id,
                node_type = "userspace.dataflow.node.func:node",
                status = consts.STATUS.PENDING,
                config = {
                    func_id = "userspace.dataflow.node.func:test_func",
                    data_targets = { { data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT } },
                },
                metadata = { title = "Full runtime restart probe" },
            },
        },
        {
            type = consts.COMMAND_TYPES.CREATE_DATA,
            payload = {
                data_id = uuid.v7(),
                data_type = consts.DATA_TYPE.NODE_INPUT,
                node_id = node_id,
                content = { message = "restart recovered", delay_ms = 8000 },
                content_type = consts.CONTENT_TYPE.JSON,
                key = "default",
            },
        },
    })
    if create_err or not dataflow_id then error(create_err or "workflow creation failed") end

    local started, start_err = dataflow_client:start(dataflow_id)
    if start_err or started ~= dataflow_id then error(start_err or "workflow start failed") end
    hold_runtime()
end

function M.observe()
    hold_runtime()
end

return M
