local test = require("test")
local uuid = require("uuid")
local time = require("time")
local client = require("client")
local commit = require("commit")
local consts = require("consts")
local data_reader = require("data_reader")
local node_reader = require("node_reader")
local node_sdk = require("node_sdk")
local agent_node = require("agent_node")
local agent_consts = require("agent_consts")
local helpers = require("recovery_helpers")
local control_handler = require("control_handler")

local STUB = "userspace.dataflow.node.agent.stub:"
local TOOL_DELAY_MS = 600
local FAIL_MESSAGE = "Page returned status 403"
local SOFT_FAIL_MESSAGE = "boom"
local FAST_DELAY_MS = 200
local SLOW_DELAY_MS = 1500
local START_TIMEOUT_MS = 10000

local function create_workflow(input, show_tool_calls)
    local node_id = uuid.v7()
    local input_id = uuid.v7()
    local dataflow_id, create_err = client.new():create_workflow({
        {
            type = consts.COMMAND_TYPES.CREATE_NODE,
            payload = {
                node_id = node_id,
                node_type = "userspace.dataflow.node.agent:node",
                status = consts.STATUS.RUNNING,
                config = {
                    agent = STUB .. "recovery_test_agent",
                    show_tool_calls = show_tool_calls == true,
                    data_targets = { {
                        data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                        key = "result",
                        content_type = consts.CONTENT_TYPE.TEXT,
                    } },
                    arena = {
                        prompt = "Run the tool scenario.",
                        max_iterations = 4,
                        tool_calling = "auto",
                        tools = { STUB .. "recovery_tool", STUB .. "recovery_control_tool" },
                    },
                },
            },
        },
        {
            type = consts.COMMAND_TYPES.CREATE_DATA,
            payload = {
                data_id = input_id,
                data_type = consts.DATA_TYPE.WORKFLOW_INPUT,
                content = input,
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
    helpers.reset_metrics(input.scenario_id)
    return dataflow_id, node_id
end

local function rows(dataflow_id, node_id, kind)
    local result, read_err = data_reader.with_dataflow(dataflow_id):with_nodes(node_id):with_data_types(kind):all()
    test.is_nil(read_err)
    return result or {}
end

local function metric_number(scenario_id, name)
    return tonumber(helpers.get_metric(scenario_id, name, 0)) or 0
end

-- Waits up to `timeout_ms` for a metric to be recorded.
local function wait_for_metric(scenario_id, name, timeout_ms)
    local waited = 0
    while metric_number(scenario_id, name) == 0 and waited < timeout_ms do
        time.sleep("20ms")
        waited = waited + 20
    end
    return metric_number(scenario_id, name) > 0
end

local function call_observations(dataflow_id, node_id)
    local result = {}
    for _, row in ipairs(rows(dataflow_id, node_id, agent_consts.DATA_TYPE.AGENT_OBSERVATION)) do
        if row.metadata and row.metadata.tool_call_id then result[#result + 1] = row end
    end
    return result
end

-- Runs the node in-process with synchronous commits and no orchestrator: a
-- yield commits its rows and continues. `interrupt_after` is checked after
-- every durable commit; once it returns true that commit and every later one
-- raise, leaving exactly the committed rows behind, as a runtime crash does.
-- `refuse_commit` rejects a commit by its commands, as a failed write does.
local function run_node(dataflow_id, node_id, interrupt_after, refuse_commit)
    local row, read_err = node_reader.with_dataflow(dataflow_id):with_nodes(node_id):one()
    test.is_nil(read_err)
    test.not_nil(row)

    local stopped = false
    local saved_new = node_sdk.new
    node_sdk.new = function(run_args)
        local n, err = saved_new(run_args, {
            data_reader = data_reader,
            commit = { submit = function(df_id, op_id, commands)
                if stopped then
                    error("interrupt after commit")
                end
                if refuse_commit and refuse_commit(commands) then
                    return nil, "commit refused"
                end
                local result, commit_err = commit.execute(df_id, op_id, commands, { publish = false })
                if interrupt_after and interrupt_after() then
                    stopped = true
                    error("interrupt after commit")
                end
                return result, commit_err
            end },
            process = {
                listen = function(topic: string, _options: any?): (Channel<any>, any?)
                    return process.listen(topic)
                end,
                send = function(pid: string, topic: string, payload: any): (boolean, any?)
                    return process.send(pid, topic, payload)
                end,
            },
        })
        if n then
            n.yield = function(self)
                return self:submit()
            end
        end
        return n, err
    end
    local ok, result = pcall(agent_node.run, { dataflow_id = dataflow_id, node_id = node_id, node = row })
    node_sdk.new = saved_new
    return ok, result
end

local function define_tests()
    describe("agent tool round recovery", function()
        it("fails the node naming the artifact when its deterministic id cannot be generated", function()
            local dataflow_id, node_id = create_workflow({
                scenario_id = "artifact-id-failure-" .. uuid.v7(), mode = "thinking_rounds", rounds = 1, artifact_at = 1,
            })
            local _, id_error = uuid.v5("invalid-artifact-namespace", "artifact")
            test.not_nil(id_error)
            local saved_artifacts = control_handler.process_artifacts
            control_handler.process_artifacts = function(control, n, iteration, call_id)
                local node_id = n.node_id
                n.node_id = "invalid-artifact-namespace"
                local changes, err = saved_artifacts(control, n, iteration, call_id)
                n.node_id = node_id
                return changes, err
            end
            local ok, result = run_node(dataflow_id, node_id)
            control_handler.process_artifacts = saved_artifacts
            test.is_true(ok, tostring(result))
            test.is_false(result.success, "artifact id failure fails the node")
            test.contains(result.message, "Report")
            test.contains(result.message, tostring(id_error))
            test.eq(#rows(dataflow_id, node_id, consts.DATA_TYPE.ARTIFACT), 0)
            test.eq(#rows(dataflow_id, node_id, consts.DATA_TYPE.WORKFLOW_OUTPUT), 0)
            local failed = node_reader.with_dataflow(dataflow_id):with_nodes(node_id):one()
            test.eq(failed.metadata.error.message, result.message, "the node persists the artifact failure")
        end)

        it("runs a failing tool once and returns its error to the model as that call's result", function()
            local scenario_id = "tool-recovery-fail-" .. uuid.v7()
            local call_id = helpers.call_id(scenario_id, 1)
            local dataflow_id, node_id = create_workflow({
                scenario_id = scenario_id, mode = "failing_tool_then_final", fail_message = FAIL_MESSAGE,
            })

            local ok, result = run_node(dataflow_id, node_id)
            test.is_true(ok, tostring(result))
            test.is_true(result.success, tostring(result.message))

            local metrics = helpers.get_metrics(scenario_id)
            test.eq(metrics.tool_attempts, 1, "the failing call runs once")
            local observations = call_observations(dataflow_id, node_id)
            test.eq(#observations, 1)
            test.eq(observations[1].metadata.tool_call_id, call_id)
            test.is_true(observations[1].metadata.is_error, "the observation is an error")
            test.eq(observations[1].content, FAIL_MESSAGE, "the observation stores the error message")
            test.eq(helpers.get_metric(scenario_id, "result_text:" .. call_id), "Error: " .. FAIL_MESSAGE,
                "the model receives the error as the call result")
        end)

        it("recovers a persisted action with a stored error observation without re-running the tool", function()
            local scenario_id = "tool-recovery-fail-restart-" .. uuid.v7()
            local call_id = helpers.call_id(scenario_id, 1)
            local dataflow_id, node_id = create_workflow({
                scenario_id = scenario_id, mode = "failing_tool_then_final", fail_message = FAIL_MESSAGE,
            })

            local first_ok, first_result = run_node(dataflow_id, node_id, function()
                return #call_observations(dataflow_id, node_id) > 0
            end)
            test.is_false(first_ok, "the first run stops after the observation commit")
            test.contains(tostring(first_result), "interrupt after commit")
            test.eq(helpers.get_metrics(scenario_id).llm_calls, 1, "the model has not seen the result yet")

            local ok, result = run_node(dataflow_id, node_id)
            test.is_true(ok, tostring(result))
            test.is_true(result.success, tostring(result.message))

            local metrics = helpers.get_metrics(scenario_id)
            test.eq(metrics.tool_attempts, 1, "recovery keeps the stored error")
            test.eq(metrics.llm_calls, 2)
            local observations = call_observations(dataflow_id, node_id)
            test.eq(#observations, 1, "recovery adds no observation")
            test.is_true(observations[1].metadata.is_error)
            test.eq(observations[1].content, FAIL_MESSAGE, "the observation stores the error message")
            test.eq(helpers.get_metric(scenario_id, "result_text:" .. call_id), "Error: " .. FAIL_MESSAGE,
                "the model receives the error as the call result")
        end)

        it("runs the unresolved calls of a recovered action concurrently", function()
            local scenario_id = "tool-recovery-parallel-" .. uuid.v7()
            local dataflow_id, node_id = create_workflow({
                scenario_id = scenario_id, mode = "two_tools_one_turn", tool_delay_ms = TOOL_DELAY_MS,
            })

            local first_ok, first_result = run_node(dataflow_id, node_id, function()
                return #rows(dataflow_id, node_id, agent_consts.DATA_TYPE.AGENT_ACTION) > 0
            end)
            test.is_false(first_ok, "the first run stops after the action commit")
            test.contains(tostring(first_result), "interrupt after commit")
            local interrupted = helpers.get_metrics(scenario_id)
            test.eq(interrupted.step1_attempts + interrupted.step2_attempts, 0, "no call ran before the interruption")

            local ok, result = run_node(dataflow_id, node_id)
            test.is_true(ok, tostring(result))
            test.is_true(result.success, tostring(result.message))

            local metrics = helpers.get_metrics(scenario_id)
            test.eq(metrics.step1_attempts, 1, "recovery runs the first call once")
            test.eq(metrics.step2_attempts, 1, "recovery runs the second call once")
            local first_turn_actions = 0
            for _, action in ipairs(rows(dataflow_id, node_id, agent_consts.DATA_TYPE.AGENT_ACTION)) do
                if action.metadata.iteration == 1 then first_turn_actions = first_turn_actions + 1 end
            end
            test.eq(first_turn_actions, 1, "recovery reuses the stored action")
            test.eq(#call_observations(dataflow_id, node_id), 2, "one observation per call")
            test.gt(metrics.step1_started, 0)
            test.gt(metrics.step2_started, 0)
            test.is_true(metrics.step2_started < metrics.step1_finished, "second call starts before the first ends")
            test.is_true(metrics.step1_started < metrics.step2_finished, "first call starts before the second ends")
        end)

        local function exercise_early_commit(show_tool_calls)
            local scenario_id = "tool-recovery-early-" .. uuid.v7()
            local fast_id = helpers.call_id(scenario_id, 1)
            local slow_id = helpers.call_id(scenario_id, 2)
            local dataflow_id, node_id = create_workflow({
                scenario_id = scenario_id, mode = "fast_and_slow_one_turn",
                fast_delay_ms = FAST_DELAY_MS, slow_delay_ms = SLOW_DELAY_MS,
            }, show_tool_calls)

            local slow_started_at_interrupt
            local slow_finished_at_interrupt
            local observed_at_interrupt
            local first_ok = run_node(dataflow_id, node_id, function()
                local observations = call_observations(dataflow_id, node_id)
                for _, observation in ipairs(observations) do
                    if observation.metadata.tool_call_id == fast_id then
                        -- Stop only once the slow call has started, so its re-run is a second attempt.
                        slow_started_at_interrupt = wait_for_metric(scenario_id, "tool_started:" .. slow_id,
                            START_TIMEOUT_MS)
                        slow_finished_at_interrupt = metric_number(scenario_id, "tool_finished:" .. slow_id)
                        observed_at_interrupt = #observations
                        return true
                    end
                end
                return false
            end)
            test.is_false(first_ok, "the first run stops after the fast call's observation commits")
            test.is_true(slow_started_at_interrupt, "the slow call starts before the interruption")
            test.eq(slow_finished_at_interrupt, 0, "the slow call is still running when the fast result commits")
            test.eq(observed_at_interrupt, 1, "only the finished call is observed")

            local ok, result = run_node(dataflow_id, node_id)
            test.is_true(ok, tostring(result))
            test.is_true(result.success, tostring(result.message))

            test.eq(tonumber(helpers.get_metric(scenario_id, "tool_attempts:" .. fast_id, 0)), 1,
                "recovery keeps the committed fast result")
            test.eq(tonumber(helpers.get_metric(scenario_id, "tool_attempts:" .. slow_id, 0)), 2,
                "recovery re-runs the unfinished call with its original id")
            local per_call = {}
            for _, observation in ipairs(call_observations(dataflow_id, node_id)) do
                local call_id = observation.metadata.tool_call_id
                per_call[call_id] = (per_call[call_id] or 0) + 1
            end
            test.eq(per_call[fast_id], 1)
            test.eq(per_call[slow_id], 1)
            return dataflow_id, node_id, fast_id, slow_id
        end

        it("commits a finished call's observation while a slower call of the turn still runs", function()
            exercise_early_commit(false)
        end)

        it("keeps one tool call node per call across the restart", function()
            local dataflow_id, node_id, fast_id, slow_id = exercise_early_commit(true)
            local tool_nodes, read_err = node_reader.with_dataflow(dataflow_id):with_parent_nodes(node_id)
                :with_node_types("tool.call"):all()
            test.is_nil(read_err)
            local per_call = {}
            for _, tool_node in ipairs(tool_nodes or {}) do
                local call_id = tool_node.metadata.tool_call_id
                per_call[call_id] = (per_call[call_id] or 0) + 1
                test.eq(tool_node.status, consts.STATUS.COMPLETED_SUCCESS, "tool call node " .. tostring(call_id))
            end
            test.eq(per_call[fast_id], 1, "the fast call keeps its node")
            test.eq(per_call[slow_id], 1, "the recovered call reuses its node")
        end)

        local function assert_soft_failure_reached_model(scenario_id, dataflow_id, node_id)
            local call_id = helpers.call_id(scenario_id, 1)
            test.eq(helpers.get_metrics(scenario_id).tool_attempts, 1, "the failing call runs once")
            local observations = call_observations(dataflow_id, node_id)
            test.eq(#observations, 1)
            test.is_true(observations[1].metadata.is_error)
            test.eq(observations[1].content_type, consts.CONTENT_TYPE.TEXT)
            test.eq(observations[1].content, SOFT_FAIL_MESSAGE, "the observation stores the error message")
            test.eq(helpers.get_metric(scenario_id, "result_text:" .. call_id), "Error: " .. SOFT_FAIL_MESSAGE,
                "the model receives the error as the call result")
        end

        local function create_soft_failure()
            local scenario_id = "tool-recovery-soft-" .. uuid.v7()
            local dataflow_id, node_id = create_workflow({
                scenario_id = scenario_id, mode = "soft_failing_tool_then_final", soft_fail_message = SOFT_FAIL_MESSAGE,
            })
            return scenario_id, dataflow_id, node_id
        end

        it("returns a failure the tool reports as data to the model as text", function()
            local scenario_id, dataflow_id, node_id = create_soft_failure()
            local ok, result = run_node(dataflow_id, node_id)
            test.is_true(ok, tostring(result))
            test.is_true(result.success, tostring(result.message))
            assert_soft_failure_reached_model(scenario_id, dataflow_id, node_id)
        end)

        it("returns a stored failure the tool reported as data to the model after a restart", function()
            local scenario_id, dataflow_id, node_id = create_soft_failure()
            local first_ok = run_node(dataflow_id, node_id, function()
                return #call_observations(dataflow_id, node_id) > 0
            end)
            test.is_false(first_ok, "the first run stops after the observation commit")
            local ok, result = run_node(dataflow_id, node_id)
            test.is_true(ok, tostring(result))
            test.is_true(result.success, tostring(result.message))
            assert_soft_failure_reached_model(scenario_id, dataflow_id, node_id)
        end)

        it("returns a failure a recovered call reports as data to the model as text", function()
            local scenario_id, dataflow_id, node_id = create_soft_failure()
            local first_ok = run_node(dataflow_id, node_id, function()
                return #rows(dataflow_id, node_id, agent_consts.DATA_TYPE.AGENT_ACTION) > 0
            end)
            test.is_false(first_ok, "the first run stops after the action commit")
            local ok, result = run_node(dataflow_id, node_id)
            test.is_true(ok, tostring(result))
            test.is_true(result.success, tostring(result.message))
            assert_soft_failure_reached_model(scenario_id, dataflow_id, node_id)
        end)

        it("stores one error text on the observation and on the tool call node", function()
            for _, input in ipairs({
                { mode = "failing_tool_then_final", fail_message = FAIL_MESSAGE, expected = FAIL_MESSAGE },
                { mode = "soft_failing_tool_then_final", soft_fail_message = SOFT_FAIL_MESSAGE, expected = SOFT_FAIL_MESSAGE },
            }) do
                input.scenario_id = "tool-recovery-text-" .. uuid.v7()
                local dataflow_id, node_id = create_workflow(input, true)
                local ok, result = run_node(dataflow_id, node_id)
                test.is_true(ok, tostring(result))
                test.is_true(result.success, tostring(result.message))
                test.eq(call_observations(dataflow_id, node_id)[1].content, input.expected)
                local tool_nodes, read_err = node_reader.with_dataflow(dataflow_id):with_parent_nodes(node_id)
                    :with_node_types("tool.call"):all()
                test.is_nil(read_err)
                test.eq(#tool_nodes, 1)
                test.eq(tool_nodes[1].metadata.error_message, input.expected)
            end
        end)

        it("runs a recovered call whose id an earlier turn already used", function()
            local scenario_id = "tool-recovery-reused-" .. uuid.v7()
            local call_id = helpers.call_id(scenario_id, 1)
            local dataflow_id, node_id = create_workflow({ scenario_id = scenario_id, mode = "reused_call_id_then_final" })

            local first_ok = run_node(dataflow_id, node_id, function()
                for _, action in ipairs(rows(dataflow_id, node_id, agent_consts.DATA_TYPE.AGENT_ACTION)) do
                    if action.metadata.iteration == 2 then return true end
                end
                return false
            end)
            test.is_false(first_ok, "the first run stops after the second turn's action commits")
            test.eq(metric_number(scenario_id, "tool_attempts:" .. call_id), 1, "only the first turn's call ran")

            local ok, result = run_node(dataflow_id, node_id)
            test.is_true(ok, tostring(result))
            test.is_true(result.success, tostring(result.message))

            test.eq(metric_number(scenario_id, "tool_attempts:" .. call_id), 2, "the second turn's call runs on recovery")
            test.eq(helpers.get_metrics(scenario_id).llm_calls, 3, "the model answers after both calls have results")
            local per_iteration = {}
            for _, observation in ipairs(call_observations(dataflow_id, node_id)) do
                local iteration = observation.metadata.iteration
                per_iteration[iteration] = (per_iteration[iteration] or 0) + 1
            end
            test.eq(per_iteration[1], 1)
            test.eq(per_iteration[2], 1, "the second turn's call has its own observation")
        end)

        it("fails the round when a finished call's observation cannot be committed", function()
            local scenario_id = "tool-recovery-refused-" .. uuid.v7()
            local fast_id = helpers.call_id(scenario_id, 1)
            local slow_id = helpers.call_id(scenario_id, 2)
            local dataflow_id, node_id = create_workflow({
                scenario_id = scenario_id, mode = "fast_and_slow_one_turn",
                fast_delay_ms = FAST_DELAY_MS, slow_delay_ms = SLOW_DELAY_MS,
            })

            local refused = 0
            local slow_started = false
            local ok, result = run_node(dataflow_id, node_id, nil, function(commands)
                for _, command in ipairs(commands) do
                    local payload = command.payload or {}
                    if command.type == consts.COMMAND_TYPES.CREATE_DATA
                        and payload.data_type == agent_consts.DATA_TYPE.AGENT_OBSERVATION
                        and payload.metadata and payload.metadata.tool_call_id == fast_id
                        and refused == 0 then
                        refused = refused + 1
                        slow_started = wait_for_metric(scenario_id, "tool_started:" .. slow_id, START_TIMEOUT_MS)
                        return true
                    end
                end
                return false
            end)
            test.is_true(ok, tostring(result))
            test.is_false(result.success, "the node fails when a tool result cannot be persisted")
            test.contains(tostring(result.message), "Tool result listener failed for " .. fast_id)
            test.contains(tostring(result.message), "commit refused")
            test.is_true(slow_started, "the slow call runs when the commit is refused")
            -- A cancelled call never reaches its finish; an uncancelled one reaches it within its delay.
            test.is_false(wait_for_metric(scenario_id, "tool_finished:" .. slow_id, SLOW_DELAY_MS + 500),
                "the slower call is cancelled with the round")
            test.eq(helpers.get_metrics(scenario_id).llm_calls, 1, "the model gets no partial round")
        end)
    end)
end

return test.run_cases(define_tests)
