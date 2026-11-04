local test = require("test")
local uuid = require("uuid")
local client = require("client")
local node = require("node")
local consts = require("consts")
local node_reader = require("node_reader")
local data_reader = require("data_reader")
local commit = require("commit")
local delegation_handler = require("delegation_handler")
local agent_node = require("agent_node")
local workflow_state = require("workflow_state")

-- Applies the node's queued commands synchronously (the orchestrator is not running
-- in this test), mirroring the control_handler integration tests.
local function apply(n, dataflow_id)
    if #n._queued_commands == 0 then
        return
    end
    local _, err = commit.execute(dataflow_id, uuid.v7(), n._queued_commands, { publish = false })
    test.is_nil(err, "commands applied: " .. tostring(err))
    n._queued_commands = {}
end

local function setup_parent()
    local c, err = client.new()
    test.is_nil(err, "client created")

    local node_id = uuid.v7()
    local dataflow_id, create_err = c:create_workflow({
        {
            type = consts.COMMAND_TYPES.CREATE_NODE,
            payload = {
                node_id = node_id,
                node_type = "userspace.dataflow.node.agent:node",
                status = consts.STATUS.PENDING,
                config = {}
            }
        }
    })
    test.is_nil(create_err, "workflow created")

    local n, node_err = node.new({ node_id = node_id, dataflow_id = dataflow_id, node = {} })
    test.is_nil(node_err, "node sdk created")
    return n, dataflow_id, node_id
end

local function children_for_tool_call(dataflow_id, parent_node_id, tool_call_id)
    local rows = (node_reader.with_dataflow(dataflow_id) :: any)
        :with_parent_nodes(parent_node_id)
        :all()
    local count = 0
    for _, row in ipairs(rows or {}) do
        if (row.metadata or {}).tool_call_id == tool_call_id then
            count = count + 1
        end
    end
    return count
end

-- Runs one turn's delegation through the node up to the yield that would start
-- the child, committing what the node queued. Returns the child node ids the
-- yield would run.
local function issue_delegation_turn(n, dataflow_id, parent_node_id, iteration, call_id)
    local yields = 0
    local run_nodes
    n.yield = function(_, options)
        yields = yields + 1
        apply(n, dataflow_id)
        if yields == 2 then
            run_nodes = options.run_nodes
            return nil, "stop before the children run"
        end
        return true, nil
    end
    local delegate_calls = { { id = call_id, name = "delegate_researcher", agent_id = "researcher",
        arguments = { task = "investigate" } } }
    local _, _, err = agent_node._test.finalize_iteration(n, {},
        { dataflow_id = dataflow_id, node_id = parent_node_id }, iteration, 10, 0, "auto", nil,
        { result = "", delegate_calls = delegate_calls }, delegate_calls, {}, {}, {}, {})
    test.contains(tostring(err), "stop before the children run")
    return run_nodes
end

local function create_child_row(dataflow_id, parent_node_id, node_type, metadata)
    local child_id = uuid.v7()
    local _, err = commit.execute(dataflow_id, uuid.v7(), { {
        type = consts.COMMAND_TYPES.CREATE_NODE,
        payload = {
            node_id = child_id, node_type = node_type, parent_node_id = parent_node_id,
            status = consts.STATUS.RUNNING, config = {}, metadata = metadata,
        },
    } }, { publish = false })
    test.is_nil(err, "child row created: " .. tostring(err))
    return child_id
end

local function define_tests()
    describe("Delegation Recovery Idempotency", function()
        before_each(function()
            -- avoid depending on a registered agent; create_child_node degrades through
            -- the display resolver helper.
            delegation_handler._agent_ref = {
                resolve = function(agent_id)
                    return { id = agent_id, title = "Custom agent", name = "Custom agent" }
                end
            }
        end)

        after_each(function()
            delegation_handler._agent_ref = nil
        end)

        it("accepts a delegated false result as a successful result", function()
            local n, dataflow_id, parent_node_id = setup_parent()
            local child_id, result_id = uuid.v7(), uuid.v7()
            local _, err = commit.execute(dataflow_id, uuid.v7(), {
                { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                    data_id = result_id, data_type = consts.DATA_TYPE.NODE_RESULT,
                    content = "false", content_type = consts.CONTENT_TYPE.JSON,
                } },
                { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                    data_id = uuid.v7(), data_type = "node.yield.result", node_id = parent_node_id,
                    content = { [child_id] = result_id }, content_type = consts.CONTENT_TYPE.JSON,
                } },
            }, { publish = false })
            test.is_nil(err)
            n.yield = function() return true end
            local results, execute_err = delegation_handler.execute_delegation_batch({ {
                child_id = child_id, agent_info = { title = "Delegate" }, tool_call_id = "scalar",
            } }, n)
            test.is_nil(execute_err)
            test.eq(#results, 1)
            test.is_true(results[1].success, "false is a successful delegated result")
            test.eq(results[1].result, false)
            test.is_nil(results[1].error)
        end)

        it("persists scalar delegation results as text without a reference target", function()
            for _, item in ipairs({ { value = false, text = "false" }, { value = 0, text = "0" },
                { value = "", text = "" }, { text = "nil" } }) do
                local n, dataflow_id, parent_node_id = setup_parent()
                delegation_handler.map_delegation_results_to_conversation({ {
                    success = true, result = item.value,
                    delegation_info = { child_id = uuid.v7(), delegate_tool_name = "delegate", tool_call_id = "scalar" },
                } }, n, 1)
                apply(n, dataflow_id)
                local observation = data_reader.with_dataflow(dataflow_id):with_nodes(parent_node_id)
                    :with_data_types("agent.observation"):one()
                test.not_nil(observation)
                test.eq(observation.content, item.text)
                test.eq(observation.content_type, consts.CONTENT_TYPE.TEXT)
                test.eq(observation.metadata.tool_call_id, "scalar")
                test.is_false(observation.metadata.is_error == true)
            end
        end)

        it("reuses an existing in-flight child on a recovery re-issue", function()
            local n, dataflow_id, parent_node_id = setup_parent()
            local session_context = { dataflow_id = dataflow_id, node_id = parent_node_id }
            local delegation = {
                agent_id = "researcher",
                tool_call_id = "call-abc",
                iteration = 1,
                input_data = { task = "investigate" },
                system_prompt = "Complete the task"
            }

            -- initial delegation: creates the child
            local first = delegation_handler.create_child_node(n, delegation, 1, session_context)
            apply(n, dataflow_id)
            test.eq(children_for_tool_call(dataflow_id, parent_node_id, "call-abc"), 1, "child created once")

            -- recovery re-issues the same unresolved delegation
            local second = delegation_handler.create_child_node(n, delegation, 1, session_context)
            apply(n, dataflow_id)

            test.eq(second.child_id, first.child_id, "recovery reuses the existing child id")
            test.eq(children_for_tool_call(dataflow_id, parent_node_id, "call-abc"), 1,
                "no duplicate child agent created on recovery")
        end)

        it("starts a new child when a later turn reuses a delegate call id", function()
            local n, dataflow_id, parent_node_id = setup_parent()

            local first = issue_delegation_turn(n, dataflow_id, parent_node_id, 1, "call-reused")
            local second = issue_delegation_turn(n, dataflow_id, parent_node_id, 2, "call-reused")

            test.eq(children_for_tool_call(dataflow_id, parent_node_id, "call-reused"), 2,
                "each turn's delegation has its own child")
            test.is_true(first[1] ~= second[1], "the later turn runs its own child")

            local rows = (node_reader.with_dataflow(dataflow_id) :: any):with_nodes(second[1]):all()
            test.eq((rows[1].metadata or {}).iteration, 2, "the child records its turn")
        end)

        it("resumes the child of its own turn when recovery re-issues a reused call id", function()
            local n, dataflow_id, parent_node_id = setup_parent()

            issue_delegation_turn(n, dataflow_id, parent_node_id, 1, "call-reused")
            local live = issue_delegation_turn(n, dataflow_id, parent_node_id, 2, "call-reused")
            local recovered = issue_delegation_turn(n, dataflow_id, parent_node_id, 2, "call-reused")

            test.eq(recovered[1], live[1], "recovery resumes the turn's existing child")
            test.eq(children_for_tool_call(dataflow_id, parent_node_id, "call-reused"), 2,
                "recovery creates no further child")
        end)

        it("resumes a persisted child that has no recorded turn", function()
            local n, dataflow_id, parent_node_id = setup_parent()
            local persisted = create_child_row(dataflow_id, parent_node_id, "userspace.dataflow.node.agent:node",
                { tool_call_id = "call-persisted" })

            local run_nodes = issue_delegation_turn(n, dataflow_id, parent_node_id, 3, "call-persisted")

            test.eq(run_nodes[1], persisted, "recovery resumes the child rather than repeating its work")
            test.eq(children_for_tool_call(dataflow_id, parent_node_id, "call-persisted"), 1)
        end)

        it("prefers the child of the same turn over one with no recorded turn", function()
            local n, dataflow_id, parent_node_id = setup_parent()
            create_child_row(dataflow_id, parent_node_id, "userspace.dataflow.node.agent:node",
                { tool_call_id = "call-both" })
            local same_turn = create_child_row(dataflow_id, parent_node_id, "userspace.dataflow.node.agent:node",
                { tool_call_id = "call-both", iteration = 3 })

            local run_nodes = issue_delegation_turn(n, dataflow_id, parent_node_id, 3, "call-both")

            test.eq(run_nodes[1], same_turn)
        end)

        it("does not mistake the tool call node of a delegating tool for its child", function()
            local n, dataflow_id, parent_node_id = setup_parent()
            local tool_node = create_child_row(dataflow_id, parent_node_id, "tool.call",
                { tool_call_id = "call-tool", iteration = 1 })

            local run_nodes = issue_delegation_turn(n, dataflow_id, parent_node_id, 1, "call-tool")

            test.is_true(run_nodes[1] ~= tool_node, "the delegation runs an agent child")
            local rows = (node_reader.with_dataflow(dataflow_id) :: any):with_nodes(run_nodes[1]):all()
            test.eq(rows[1].type, "userspace.dataflow.node.agent:node")
        end)

        it("creates distinct children for distinct tool_call_ids", function()
            local n, dataflow_id, parent_node_id = setup_parent()
            local session_context = { dataflow_id = dataflow_id, node_id = parent_node_id }

            local a = delegation_handler.create_child_node(n,
                { agent_id = "a", tool_call_id = "call-1", input_data = {} }, 1, session_context)
            local b = delegation_handler.create_child_node(n,
                { agent_id = "b", tool_call_id = "call-2", input_data = {} }, 2, session_context)
            apply(n, dataflow_id)

            test.is_true(a.child_id ~= b.child_id, "distinct delegations get distinct children")
            test.eq(children_for_tool_call(dataflow_id, parent_node_id, "call-1"), 1)
            test.eq(children_for_tool_call(dataflow_id, parent_node_id, "call-2"), 1)
        end)

        it("uses resolved agent display title for child node metadata", function()
            delegation_handler._agent_ref = {
                resolve = function(agent_id)
                    return { id = agent_id, title = "Researcher", name = "Researcher" }
                end
            }

            local n, dataflow_id, parent_node_id = setup_parent()
            local session_context = { dataflow_id = dataflow_id, node_id = parent_node_id }
            delegation_handler.create_child_node(n, {
                agent_id = "user_agent:019eccf3-81ae-7e91-9bf8-750d4a685492",
                tool_call_id = "call-title",
                input_data = {},
            }, 1, session_context)
            apply(n, dataflow_id)

            local rows = (node_reader.with_dataflow(dataflow_id) :: any)
                :with_parent_nodes(parent_node_id)
                :all()
            test.eq(#rows, 1)
            test.eq((rows[1].metadata or {}).title, "Researcher")
        end)

        it("declares a consumed delegation failure on the child and excludes it from failure evidence", function()
            local n, dataflow_id, parent_node_id = setup_parent()
            local session_context = { dataflow_id = dataflow_id, node_id = parent_node_id }
            local delegation = {
                agent_id = "researcher",
                tool_call_id = "call-failed",
                input_data = { task = "investigate" },
                delegate_tool_name = "to_researcher"
            }

            local info = delegation_handler.create_child_node(n, delegation, 1, session_context)
            apply(n, dataflow_id)

            -- Child run ends in failure.
            local _, update_err = commit.execute(dataflow_id, uuid.v7(), {
                {
                    type = consts.COMMAND_TYPES.UPDATE_NODE,
                    payload = {
                        node_id = info.child_id,
                        status = consts.STATUS.COMPLETED_FAILURE
                    }
                }
            }, { publish = false })
            test.is_nil(update_err, "child failure persisted")

            -- Parent consumes the failure as an observation.
            delegation_handler.map_delegation_results_to_conversation({
                {
                    success = false,
                    error = "delegated agent failed",
                    delegation_info = info
                }
            }, n, 1)
            apply(n, dataflow_id)

            local observations = (data_reader.with_dataflow(dataflow_id) :: any)
                :with_nodes(parent_node_id)
                :with_data_types("agent.observation")
                :all() or {}
            local error_observation = nil
            for _, row in ipairs(observations) do
                if (row.metadata or {}).is_error == true then
                    error_observation = row
                end
            end
            test.not_nil(error_observation, "delegation error recorded as parent observation")

            local child_rows = (node_reader.with_dataflow(dataflow_id) :: any)
                :with_nodes(info.child_id)
                :all() or {}
            test.eq(#child_rows, 1)
            test.eq(child_rows[1].status, consts.STATUS.COMPLETED_FAILURE, "child keeps its failure status")
            test.is_true((child_rows[1].metadata or {}).error_observed == true,
                "consumed failure declared on the child")

            local ws = workflow_state.new(dataflow_id) :: any
            local _, load_err = ws:load_state()
            test.is_nil(load_err, "workflow state loaded")
            test.is_nil(ws:get_failed_node_errors(),
                "consumed delegation failure is not workflow failure evidence")
        end)
    end)
end

return test.run_cases(define_tests)
