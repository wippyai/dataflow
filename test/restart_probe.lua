local flow = require("flow")
local funcs = require("funcs")
local uuid = require("uuid")
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

local function create_behavior_workflow()
    local node_id, input_id = uuid.v7(), uuid.v7()
    local c = client.new()
    local id, err = c:create_workflow({
        { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
            node_id = node_id, node_type = "userspace.dataflow.node.agent:node", status = consts.STATUS.PENDING,
            config = {
                agent = "userspace.dataflow.node.agent.stub:recovery_test_agent", show_tool_calls = false,
                data_targets = {{ data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "result", content_type = consts.CONTENT_TYPE.TEXT }},
                active_traits = { "userspace.dataflow.node.agent.stub:behavior_switch_compact_trait" },
                arena = { prompt = "Complete the restart scenario.", max_iterations = 4, tool_calling = "auto",
                    tools = { "userspace.dataflow.node.agent.stub:recovery_tool" } },
            },
        } },
        { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
            data_id = input_id, data_type = consts.DATA_TYPE.WORKFLOW_INPUT,
            content = { scenario_id = "behavior-restart", mode = "behavior_restart", tool_delay_ms = 9000 },
            content_type = consts.CONTENT_TYPE.JSON,
        } },
        { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
            data_id = uuid.v7(), data_type = consts.DATA_TYPE.NODE_INPUT, node_id = node_id, key = input_id,
            content = "", content_type = consts.CONTENT_TYPE.REFERENCE,
        } },
    })
    if not id then error(err or "behavior workflow creation failed") end
    local started, start_err = c:start(id)
    if not started then error(start_err or "behavior workflow start failed") end
end

local function prepare_runtime()
    -- Services may start while the dependency bootloader is still migrating.
    -- Use the same canonical readiness check as the ordinary test harness.
    local ready, readiness_err = funcs.call("app:wait_for_boot")
    if readiness_err or not ready then error(readiness_err or "boot is not ready") end
    -- The standalone restart process bypasses the test runner's 00_setup suite.
    -- Seed the same authority fixture before capturing a workflow identity.
    local authority_ready, authority_err = funcs.call("app.await_authority:setup", { durable_tokens = true })
    if authority_err or not authority_ready then error(authority_err or "authority fixture is not ready") end
end

function M.create_behavior()
    prepare_runtime()
    create_behavior_workflow()
    return hold_runtime()
end

function M.create()
    prepare_runtime()
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
