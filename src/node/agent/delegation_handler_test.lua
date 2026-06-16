local test = require("test")
local uuid = require("uuid")
local client = require("client")
local node = require("node")
local consts = require("consts")
local node_reader = require("node_reader")
local commit = require("commit")
local delegation_handler = require("delegation_handler")

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

local function define_tests()
    describe("Delegation Recovery Idempotency", function()
        before_each(function()
            -- avoid depending on a registered agent; create_child_node degrades to defaults
            delegation_handler._agent_registry = {
                get_by_id = function(_) return nil end,
                get_by_name = function(_) return nil end
            }
        end)

        after_each(function()
            delegation_handler._agent_registry = nil
        end)

        it("reuses an existing in-flight child on a recovery re-issue", function()
            local n, dataflow_id, parent_node_id = setup_parent()
            local session_context = { dataflow_id = dataflow_id, node_id = parent_node_id }
            local delegation = {
                agent_id = "researcher",
                tool_call_id = "call-abc",
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
    end)
end

return test.run_cases(define_tests)
