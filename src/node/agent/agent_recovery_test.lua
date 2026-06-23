local test = require("test")
local uuid = require("uuid")
local time = require("time")
local client = require("client")
local consts = require("consts")
local data_reader = require("data_reader")
local funcs = require("funcs")
local node_reader = require("node_reader")
local agent_consts = require("agent_consts")

local function define_tests()
    describe("Agent Recovery Tests", function()
        local c

        before_all(function()
            c = client.new()
            test.not_nil(c, "client created")
        end)

        local function kill_orchestrator(df_id)
            local pid = process.registry.lookup("dataflow." .. df_id)
            if pid then
                process.terminate(pid)
                time.sleep("200ms")
            end
        end

        local function wait_until(predicate, timeout_ms, interval_ms)
            local timeout = timeout_ms or 15000
            local interval = interval_ms or 100
            local attempts = math.ceil(timeout / interval)

            for _ = 1, attempts do
                local ok, value = pcall(predicate)
                if ok and value then
                    return value
                end
                time.sleep(tostring(interval) .. "ms")
            end

            return nil
        end

        local function wait_complete(df_id, timeout_ms)
            return wait_until(function()
                local status = c:get_status(df_id)
                if status == consts.STATUS.COMPLETED_SUCCESS then
                    return true
                end
                if status == consts.STATUS.COMPLETED_FAILURE then
                    return false
                end
                return nil
            end, timeout_ms or 20000, 100)
        end

        local function reset_metrics(scenario_id)
            local _, err = funcs.new():call(
                "userspace.dataflow.node.agent.stub:recovery_metrics_reset",
                { scenario_id = scenario_id }
            )
            test.is_nil(err, "metrics reset")
        end

        local function get_metrics(scenario_id)
            local result, err = funcs.new():call(
                "userspace.dataflow.node.agent.stub:recovery_metrics_get",
                { scenario_id = scenario_id }
            )
            test.is_nil(err, "metrics fetch")
            return result or {}
        end

        local function wait_for_metrics(scenario_id, field_name, minimum_value, timeout_ms)
            return wait_until(function()
                local metrics = get_metrics(scenario_id)
                if (metrics[field_name] or 0) >= minimum_value then
                    return metrics
                end
                return nil
            end, timeout_ms or 12000, 100)
        end

        local function wait_for_action_count(df_id, node_id, minimum_count, timeout_ms)
            return wait_until(function()
                local rows = data_reader.with_dataflow(df_id)
                    :with_nodes(node_id)
                    :with_data_types(agent_consts.DATA_TYPE.AGENT_ACTION)
                    :all()
                if #rows >= minimum_count then
                    return rows
                end
                return nil
            end, timeout_ms or 12000, 100)
        end

        local function create_workflow(opts)
            local node_id = uuid.v7()
            local input_id = uuid.v7()
            local node_input_id = uuid.v7()
            local scenario_id = opts.scenario_id or ("agent-recovery-" .. uuid.v7())

            reset_metrics(scenario_id)

            local commands = {
                {
                    type = consts.COMMAND_TYPES.CREATE_NODE,
                    payload = {
                        node_id = node_id,
                        node_type = "userspace.dataflow.node.agent:node",
                        status = consts.STATUS.PENDING,
                        config = {
                            agent = "userspace.dataflow.node.agent.stub:recovery_test_agent",
                            show_tool_calls = false,
                            recovery_test_hooks = opts.recovery_test_hooks,
                            active_traits = opts.active_traits,
                            arena = {
                                prompt = "Execute the deterministic recovery scenario.",
                                max_iterations = opts.max_iterations or 6,
                                tool_calling = "auto",
                                tools = {
                                    "userspace.dataflow.node.agent.stub:recovery_tool"
                                }
                            },
                            data_targets = {
                                {
                                    data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                    key = "result",
                                    content_type = consts.CONTENT_TYPE.TEXT
                                }
                            }
                        },
                        metadata = {
                            title = "Agent Recovery Test"
                        }
                    }
                },
                {
                    type = consts.COMMAND_TYPES.CREATE_DATA,
                    payload = {
                        data_id = input_id,
                        data_type = consts.DATA_TYPE.WORKFLOW_INPUT,
                        content = {
                            scenario_id = scenario_id,
                            mode = opts.mode,
                            tool_delay_ms = opts.tool_delay_ms or 0
                        },
                        content_type = consts.CONTENT_TYPE.JSON
                    }
                },
                {
                    type = consts.COMMAND_TYPES.CREATE_DATA,
                    payload = {
                        data_id = node_input_id,
                        data_type = consts.DATA_TYPE.NODE_INPUT,
                        node_id = node_id,
                        key = input_id,
                        content = "",
                        content_type = consts.CONTENT_TYPE.REFERENCE
                    }
                }
            }

            local dataflow_id, err = c:create_workflow(commands, {
                metadata = { title = "Agent Recovery Test Workflow" }
            })
            test.is_nil(err, "workflow created")

            return {
                dataflow_id = dataflow_id,
                node_id = node_id,
                scenario_id = scenario_id
            }
        end

        local function get_output_text(df_id)
            local outputs = data_reader.with_dataflow(df_id)
                :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                :all()

            for _, output in ipairs(outputs) do
                if output.key == "result" then
                    return output.content
                end
            end

            return nil
        end

        it("runs agent lifecycle trait handlers and injects activation prompt context", function()
            local scenario_id = "agent-lifecycle-" .. uuid.v7()
            local workflow = create_workflow({
                scenario_id = scenario_id,
                mode = "text_final",
                max_iterations = 2,
                active_traits = {
                    {
                        id = "userspace.dataflow.node.agent.stub:lifecycle_test_trait",
                        context = {
                            scenario_id = scenario_id
                        }
                    }
                }
            })

            c:start(workflow.dataflow_id)

            test.is_true(wait_complete(workflow.dataflow_id, 20000), "workflow completed")
            test.eq(c:get_status(workflow.dataflow_id), consts.STATUS.COMPLETED_SUCCESS, "workflow succeeded")

            local metrics = get_metrics(scenario_id)
            test.eq(metrics.lifecycle_activate, 1, "agent activated once")
            test.eq(metrics.lifecycle_before_step, 1, "before_step ran once")
            test.eq(metrics.lifecycle_after_step, 1, "after_step ran once")
            test.eq(metrics.lifecycle_deactivate, 1, "agent deactivated once")
            test.eq(metrics.lifecycle_prompt_seen, 1, "activation message reached the LLM prompt")
            test.eq(metrics.lifecycle_last_iteration, 1, "lifecycle received dataflow host iteration")
        end)

        it("reuses a persisted tool turn after restart without an extra LLM call", function()
            local workflow = create_workflow({
                mode = "single_tool_then_final",
                tool_delay_ms = 1500
            })

            c:start(workflow.dataflow_id)
            test.not_nil(wait_for_action_count(workflow.dataflow_id, workflow.node_id, 1, 12000), "first action persisted")

            kill_orchestrator(workflow.dataflow_id)
            c:start(workflow.dataflow_id)

            test.is_true(wait_complete(workflow.dataflow_id, 20000), "workflow recovered")
            test.eq(c:get_status(workflow.dataflow_id), consts.STATUS.COMPLETED_SUCCESS, "workflow succeeded")
            test.eq(get_output_text(workflow.dataflow_id), "final:single_tool_then_final:" .. workflow.scenario_id .. ":1")

            local metrics = get_metrics(workflow.scenario_id)
            test.eq(metrics.llm_calls, 2, "recovery reused persisted tool turn")

            local node = (node_reader.with_dataflow(workflow.dataflow_id) :: any)
                :with_nodes(workflow.node_id)
                :one()
            test.not_nil(node, "node exists")
            test.eq(node.metadata.state.current_iteration, 2, "iteration count persisted across restart")
            test.eq(node.metadata.state.tool_calls, 1, "tool call count persisted across restart")
            test.eq(node.metadata.state.total_tokens.total_tokens, 34, "token totals persisted across restart")
        end)

        it("re-executes an interrupted tool idempotently after crash", function()
            local workflow = create_workflow({
                mode = "single_tool_then_final",
                tool_delay_ms = 1500
            })

            c:start(workflow.dataflow_id)
            test.not_nil(wait_for_metrics(workflow.scenario_id, "tool_attempts", 1, 12000), "tool started")

            kill_orchestrator(workflow.dataflow_id)
            c:start(workflow.dataflow_id)

            test.is_true(wait_complete(workflow.dataflow_id, 20000), "workflow recovered")

            local metrics = get_metrics(workflow.scenario_id)
            test.eq(metrics.tool_effects, 1, "tool side effect applied once")
            test.eq(metrics.step1_effect, 1, "first tool call effect recorded once")
            test.gte(metrics.tool_attempts, 1, "tool was attempted")
        end)

        it("replays the LLM when crash happens after response but before durable submit", function()
            local workflow = create_workflow({
                mode = "text_final",
                recovery_test_hooks = {
                    pre_action_submit_delay_ms = 1200
                }
            })

            c:start(workflow.dataflow_id)
            test.not_nil(wait_for_metrics(workflow.scenario_id, "llm_calls", 1, 12000), "first LLM call completed")
            time.sleep("200ms")

            kill_orchestrator(workflow.dataflow_id)
            c:start(workflow.dataflow_id)

            test.is_true(wait_complete(workflow.dataflow_id, 20000), "workflow recovered")
            test.eq(get_output_text(workflow.dataflow_id), "final:text_final:" .. workflow.scenario_id .. ":0")

            local metrics = get_metrics(workflow.scenario_id)
            test.eq(metrics.llm_calls, 2, "LLM was replayed because the response was not durably committed")

            local actions = data_reader.with_dataflow(workflow.dataflow_id)
                :with_nodes(workflow.node_id)
                :with_data_types(agent_consts.DATA_TYPE.AGENT_ACTION)
                :all()
            test.eq(#actions, 1, "only the durable post-restart action exists")
        end)

        it("continues a multi-turn agent conversation after restart between turns", function()
            local workflow = create_workflow({
                mode = "two_tool_turns",
                tool_delay_ms = 100
            })

            c:start(workflow.dataflow_id)
            test.not_nil(wait_for_metrics(workflow.scenario_id, "tool_effects", 1, 12000), "first turn completed")

            kill_orchestrator(workflow.dataflow_id)
            c:start(workflow.dataflow_id)

            test.is_true(wait_complete(workflow.dataflow_id, 20000), "workflow recovered across turns")
            test.eq(get_output_text(workflow.dataflow_id), "final:two_tool_turns:" .. workflow.scenario_id .. ":2")

            local metrics = get_metrics(workflow.scenario_id)
            test.eq(metrics.llm_calls, 3, "exactly one LLM call per turn")
            test.eq(metrics.tool_effects, 2, "both tool turns completed once")

            local node = (node_reader.with_dataflow(workflow.dataflow_id) :: any)
                :with_nodes(workflow.node_id)
                :one()
            test.not_nil(node, "node exists")
            test.eq(node.metadata.state.current_iteration, 3, "all turns persisted")
            test.eq(node.metadata.state.tool_calls, 2, "both tool turns counted durably")
            test.eq(node.metadata.state.total_tokens.total_tokens, 56, "multi-turn token totals persisted")
        end)
    end)
end

return { run_tests = test.run_cases(define_tests) }
