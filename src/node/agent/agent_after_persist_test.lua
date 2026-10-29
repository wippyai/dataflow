local test = require("test")
local client = require("client")
local consts = require("consts")
local agent_consts = require("agent_consts")
local data_reader = require("data_reader")
local funcs = require("funcs")
local time = require("time")
local uuid = require("uuid")

local STUB = "userspace.dataflow.node.agent.stub:"

local function run_scenario(traits, mode)
    local c = client.new()
    local node_id = uuid.v7()
    local input_id = uuid.v7()
    local dataflow_id, create_err = c:create_workflow({
        {
            type = consts.COMMAND_TYPES.CREATE_NODE,
            payload = {
                node_id = node_id,
                node_type = "userspace.dataflow.node.agent:node",
                status = consts.STATUS.PENDING,
                config = {
                    agent = STUB .. "recovery_test_agent",
                    active_traits = traits,
                    show_tool_calls = false,
                    data_targets = {
                        {
                            data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                            key = "result",
                            content_type = consts.CONTENT_TYPE.TEXT,
                        },
                    },
                    arena = {
                        prompt = "Run the after_persist scenario.",
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
                content = { scenario_id = "after-persist-" .. uuid.v7(), mode = mode or "single_tool_then_final" },
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
    test.is_nil(create_err)
    local _, reset_err = funcs.new():call(STUB .. "recovery_metrics_reset", { scenario_id = dataflow_id })
    test.is_nil(reset_err)
    local _, start_err = c:start(dataflow_id)
    test.is_nil(start_err)

    local status
    for _ = 1, 250 do
        status = c:get_status(dataflow_id)
        if status == consts.STATUS.COMPLETED_SUCCESS or status == consts.STATUS.COMPLETED_FAILURE then
            break
        end
        time.sleep("100ms")
    end

    local records, records_err = funcs.new():call(STUB .. "persist_probe_records", { scenario_id = dataflow_id })
    test.is_nil(records_err)
    local metrics, metrics_err = funcs.new():call(STUB .. "recovery_metrics_get", { scenario_id = dataflow_id })
    test.is_nil(metrics_err)
    local actions = data_reader.with_dataflow(dataflow_id)
        :with_nodes(node_id)
        :with_data_types(agent_consts.DATA_TYPE.AGENT_ACTION)
        :order_by("created_at", "ASC")
        :all()
    return status, records or {}, metrics, actions or {}
end

local function define_tests()
    describe("agent after_persist", function()
        it("delivers each committed action, readable through run_context, by its stored id", function()
            local status, records, metrics, actions = run_scenario({
                STUB .. "persist_probe_trait",
                STUB .. "lifecycle_test_trait",
            })

            test.eq(status, consts.STATUS.COMPLETED_SUCCESS)
            test.eq(#actions, 2)
            test.eq(#records, 2, "one delivery per stored action")
            for index, record in ipairs(records) do
                test.is_nil(record.history_error)
                test.eq(record.event_id, actions[index].data_id, "event_id is the committed row id")
                test.eq(record.found_role, "assistant", "range ending at event_id includes it")
                test.eq(record.found_key, actions[index].key)
                test.eq(record.iteration, index)
            end
            test.eq(records[1].outcome_state, "continues", "tool step is delivered before tool results")
            test.eq(records[2].outcome_state, "completed")
            test.eq(metrics.lifecycle_after_step, 2, "existing bindings keep their phases")
            test.eq(metrics.lifecycle_before_step, 2)
        end)

        it("reports a failed recall in the outcome and the stored action without stopping the node", function()
            local status, records, _, actions = run_scenario({
                STUB .. "persist_probe_trait",
                STUB .. "failing_recall_trait",
            })

            test.eq(status, consts.STATUS.COMPLETED_SUCCESS)
            test.eq(#actions, 2)
            local reported = 0
            for index, action in ipairs(actions) do
                local stored_error = action.metadata and action.metadata.memory_recall_error
                test.eq(records[index].memory_recall_error, stored_error)
                if stored_error then
                    test.contains(stored_error, "memory store offline")
                    reported = reported + 1
                end
            end
            test.gt(reported, 0, "at least one step attempted recall")
        end)

        it("delivers a stored truncated action", function()
            local status, records, _, actions = run_scenario({ STUB .. "persist_probe_trait" }, "truncated_then_final")

            test.eq(status, consts.STATUS.COMPLETED_SUCCESS)
            test.eq(#actions, 2)
            test.eq(#records, 2)
            test.eq(records[1].event_id, actions[1].data_id)
            test.eq(records[1].found_role, "assistant")
            test.eq(records[1].outcome_state, "continues")
            test.eq(actions[1].metadata.finish_reason, "length")
        end)

        it("keeps running when a non-strict after_persist binding fails", function()
            local status, records, _, actions = run_scenario({ STUB .. "persist_probe_failing_trait" })

            test.eq(status, consts.STATUS.COMPLETED_SUCCESS)
            test.eq(#records, 2)
            test.eq(#actions, 2)
        end)

        it("fails the node on a strict after_persist error and keeps the stored action", function()
            local status, records, _, actions = run_scenario({ STUB .. "persist_probe_strict_failing_trait" })

            test.eq(status, consts.STATUS.COMPLETED_FAILURE)
            test.eq(#records, 1)
            test.eq(#actions, 1)
            test.eq(records[1].event_id, actions[1].data_id)
        end)
    end)
end

return { run_tests = test.run_cases(define_tests) }
