local test = require("test")
local uuid = require("uuid")
local _json = require("json")

local client = require("client")
local node = require("node")
local consts = require("consts")
local agent_consts = require("agent_consts")
local data_reader = require("data_reader")
local node_reader = require("node_reader")
local commit = require("commit")
local agent_context = require("agent_context")
local control_handler = require("control_handler")

-- Applies the node's queued commands synchronously through the real ops layer
-- (node:submit only enqueues a commit for the orchestrator, which is not running here).
local function apply(n, dataflow_id)
    if #n._queued_commands == 0 then
        return
    end
    local _, err = commit.execute(dataflow_id, uuid.v7(), n._queued_commands, { publish = false })
    test.is_nil(err, "commands applied: " .. tostring(err))
    n._queued_commands = {}
end

-- Creates a real workflow with a single pending node and returns a node SDK bound to it.
local function setup_node()
    local c, err = client.new()
    test.is_nil(err, "client created")

    local node_id = uuid.v7()
    local dataflow_id, create_err = c:create_workflow({
        {
            type = consts.COMMAND_TYPES.CREATE_NODE,
            payload = {
                node_id = node_id,
                node_type = "userspace.dataflow.node.func:node",
                status = consts.STATUS.PENDING,
                config = { func_id = "userspace.dataflow.node.func:test_func" }
            }
        }
    })
    test.is_nil(create_err, "workflow created")

    local n, node_err = node.new({
        node_id = node_id,
        dataflow_id = dataflow_id,
        node = {}
    })
    test.is_nil(node_err, "node sdk created")

    return n, dataflow_id, node_id
end

local function memory_rows(dataflow_id, node_id)
    return data_reader.with_dataflow(dataflow_id)
        :with_nodes(node_id)
        :with_data_types(agent_consts.DATA_TYPE.AGENT_MEMORY)
        :all()
end

local function define_tests()
    describe("Agent Control Handler Integration", function()
        it("persists added memory then clears it by type from the store", function()
            local n, dataflow_id, node_id = setup_node()

            control_handler.process_memory_operations({ memory = { add = {
                { type = "fact", text = "alpha fact" },
                { type = "fact", text = "beta fact" },
                { type = "note", text = "a note" }
            } } }, n, 1)
            apply(n, dataflow_id)

            local rows = memory_rows(dataflow_id, node_id)
            test.eq(#rows, 3, "three memory rows persisted")

            local fact_count = 0
            for _, row in ipairs(rows) do
                if row.discriminator == "fact" then
                    fact_count = fact_count + 1
                end
            end
            test.eq(fact_count, 2, "fact rows carry discriminator")

            control_handler.process_memory_operations({ memory = { clear = "fact" } }, n, 2)
            apply(n, dataflow_id)

            local after = memory_rows(dataflow_id, node_id)
            test.eq(#after, 1, "only non-fact memory remains")
            test.eq(after[1].discriminator, "note", "the note survived the clear")
        end)

        it("deletes a single memory row by data id", function()
            local n, dataflow_id, node_id = setup_node()

            control_handler.process_memory_operations({ memory = { add = {
                { type = "fact", text = "deletable" }
            } } }, n, 1)
            apply(n, dataflow_id)

            local rows = memory_rows(dataflow_id, node_id)
            test.eq(#rows, 1, "one row before delete")

            control_handler.process_memory_operations({ memory = { delete = { rows[1].data_id } } }, n, 2)
            apply(n, dataflow_id)

            test.eq(#memory_rows(dataflow_id, node_id), 0, "row removed by id")
        end)

        it("merges session context into node metadata persisted in the store", function()
            local n, dataflow_id, node_id = setup_node()

            control_handler.process_session_context(
                { context = { session = { set = { first = "1", second = "2" } } } }, n)
            apply(n, dataflow_id)

            control_handler.process_session_context(
                { context = { session = { set = { third = "3" }, delete = { "first" } } } }, n)
            apply(n, dataflow_id)

            local row = node_reader.with_dataflow(dataflow_id):with_nodes(node_id):one()
            test.not_nil(row, "node row read back")
            local sc = (row.metadata or {}).session_context or {}
            test.is_nil(sc.first, "deleted key gone in persisted metadata")
            test.eq(sc.second, "2", "earlier key preserved across submits")
            test.eq(sc.third, "3", "later key merged in")
        end)

        it("merges public metadata into node metadata persisted in the store", function()
            local n, dataflow_id, node_id = setup_node()

            control_handler.process_public_metadata(
                { context = { public_meta = { set = { keep = { id = "keep", type = "ref" } } } } }, n)
            apply(n, dataflow_id)

            control_handler.process_public_metadata(
                { context = { public_meta = { set = { extra = { id = "extra", type = "ref" } } } } }, n)
            apply(n, dataflow_id)

            local row = node_reader.with_dataflow(dataflow_id):with_nodes(node_id):one()
            local pm = (row.metadata or {}).public_meta or {}
            test.eq((pm.keep or {}).id, "keep", "first public_meta preserved")
            test.eq((pm.extra or {}).id, "extra", "second public_meta merged")
        end)

        it("applies a combined directive (context + memory) end to end", function()
            local n, dataflow_id, node_id = setup_node()

            control_handler.process_control_directive({
                result = "ok",
                _control = {
                    context = { session = { set = { phase = "review" } } },
                    memory = { add = { { type = "fact", text = "remembered" } } }
                }
            }, n, 1)
            apply(n, dataflow_id)

            local row = node_reader.with_dataflow(dataflow_id):with_nodes(node_id):one()
            local sc = (row.metadata or {}).session_context or {}
            test.eq(sc.phase, "review", "context persisted")
            test.eq(#memory_rows(dataflow_id, node_id), 1, "memory persisted from same directive")
        end)

        it("persists active trait/tool overlays to node config and recovers them on reload", function()
            local n, dataflow_id, node_id = setup_node()
            local ctx = agent_context.new({})

            -- A control directive sets the declarative overlays and persists them.
            control_handler.apply_control_responses({
                { traits_change = { "researcher" }, tools_change = { "wippy.x:tool" } }
            }, ctx, n)
            apply(n, dataflow_id)

            -- The live context carries the overlays.
            test.eq((ctx.active_traits or {})[1], "researcher", "live context has trait overlay")
            test.eq((ctx.active_tools or {})[1], "wippy.x:tool", "live context has tool overlay")

            -- They are persisted to node config in the store.
            local row = node_reader.with_dataflow(dataflow_id):with_nodes(node_id):one()
            local cfg = row.config or {}
            test.eq((cfg.active_traits or {})[1], "researcher", "traits persisted to node config")
            test.eq((cfg.active_tools or {})[1], "wippy.x:tool", "tools persisted to node config")

            -- Recovery: a fresh context reapplies the persisted config, as node.lua does
            -- at startup on a re-run.
            local recovered = agent_context.new({})
            if cfg.active_traits ~= nil then
                recovered:set_active_traits(cfg.active_traits)
            end
            if cfg.active_tools ~= nil then
                recovered:set_active_tools(cfg.active_tools)
            end
            test.eq((recovered.active_traits or {})[1], "researcher", "trait overlay reapplied on recovery")
            test.eq((recovered.active_tools or {})[1], "wippy.x:tool", "tool overlay reapplied on recovery")
        end)
    end)
end

return test.run_cases(define_tests)
