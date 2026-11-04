local test = require("test")
local client = require("client")
local consts = require("consts")
local funcs = require("funcs")
local time = require("time")
local uuid = require("uuid")

local STUB = "userspace.dataflow.node.agent.stub:"
local TOOL_DELAY_MS = 600

local function run_two_tools()
    local c = client.new()
    local node_id = uuid.v7()
    local input_id = uuid.v7()
    local scenario_id = "parallel-tools-" .. uuid.v7()
    local dataflow_id, create_err = c:create_workflow({
        {
            type = consts.COMMAND_TYPES.CREATE_NODE,
            payload = {
                node_id = node_id,
                node_type = "userspace.dataflow.node.agent:node",
                status = consts.STATUS.PENDING,
                config = {
                    agent = STUB .. "recovery_test_agent",
                    show_tool_calls = false,
                    data_targets = {
                        {
                            data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                            key = "result",
                            content_type = consts.CONTENT_TYPE.TEXT,
                        },
                    },
                    arena = {
                        prompt = "Run both tools.",
                        max_iterations = 4,
                        tool_calling = "auto",
                        tools = { STUB .. "recovery_tool" },
                    },
                },
            },
        },
        {
            type = consts.COMMAND_TYPES.CREATE_DATA,
            payload = {
                data_id = input_id,
                data_type = consts.DATA_TYPE.WORKFLOW_INPUT,
                content = { scenario_id = scenario_id, mode = "two_tools_one_turn", tool_delay_ms = TOOL_DELAY_MS },
                content_type = consts.CONTENT_TYPE.JSON,
            },
        },
        {
            type = consts.COMMAND_TYPES.CREATE_DATA,
            payload = {
                data_id = uuid.v7(),
                data_type = consts.DATA_TYPE.NODE_INPUT,
                node_id = node_id,
                key = input_id,
                content = "",
                content_type = consts.CONTENT_TYPE.REFERENCE,
            },
        },
    })
    test.is_nil(create_err, "create")
    local _, reset_err = funcs.new():call(STUB .. "recovery_metrics_reset", { scenario_id = scenario_id })
    test.is_nil(reset_err, "reset")
    local _, start_err = c:start(dataflow_id)
    test.is_nil(start_err, "start")

    local status
    for _ = 1, 250 do
        status = c:get_status(dataflow_id)
        if status == consts.STATUS.COMPLETED_SUCCESS or status == consts.STATUS.COMPLETED_FAILURE then
            break
        end
        time.sleep("100ms")
    end

    local metrics, metrics_err = funcs.new():call(STUB .. "recovery_metrics_get", { scenario_id = scenario_id })
    test.is_nil(metrics_err, "metrics")
    return status, metrics
end

local function define_tests()
    describe("agent tool calls in one step", function()
        it("runs the tool calls of one model turn concurrently", function()
            local status, metrics = run_two_tools()

            test.eq(status, consts.STATUS.COMPLETED_SUCCESS)
            test.eq(metrics.tool_attempts, 2, "both calls run once")
            test.gt(metrics.step1_started, 0)
            test.gt(metrics.step2_started, 0)
            -- Each call holds its worker for TOOL_DELAY_MS; concurrent calls
            -- start before the other one finishes.
            test.is_true(metrics.step2_started < metrics.step1_finished, "second call starts before the first ends")
            test.is_true(metrics.step1_started < metrics.step2_finished, "first call starts before the second ends")
        end)
    end)
end

return test.run_cases(define_tests)
