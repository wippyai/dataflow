local test = require("test")
local uuid = require("uuid")
local _json = require("json")

local node = require("node")
local consts = require("consts")
local agent_consts = require("agent_consts")
local control_handler = require("control_handler")

-- Builds a node_sdk instance backed by mock deps so handler behavior is observed
-- through real SDK methods (metadata merge, queued commands, find_data).
local function make_node(initial_metadata, find_rows)
    local captured = {
        submits = {},
        find_calls = {}
    }

    local reader_chain
    reader_chain = {
        with_dataflow = function(_) return reader_chain end,
        with_nodes = function(_) return reader_chain end,
        with_data_types = function(data_type)
            table.insert(captured.find_calls, { field = "data_type", value = data_type })
            return reader_chain
        end,
        with_data_discriminators = function(disc)
            table.insert(captured.find_calls, { field = "discriminator", value = disc })
            return reader_chain
        end,
        fetch_options = function(_) return reader_chain end,
        all = function() return find_rows or {} end
    }

    local deps = {
        commit = {
            submit = function(dataflow_id, op_id, commands)
                table.insert(captured.submits, { dataflow_id = dataflow_id, op_id = op_id, commands = commands })
                return { commit_id = uuid.v7() }, nil
            end
        },
        data_reader = {
            with_dataflow = function(_) return reader_chain end
        },
        process = {
            send = function(_, _, _) return true end,
            listen = function(_) return { receive = function() return nil end } end
        }
    }

    local n, err = node.new({
        node_id = uuid.v7(),
        dataflow_id = uuid.v7(),
        node = { metadata = initial_metadata or {} }
    }, deps)

    return n, captured, err
end

-- Collects queued commands of a given type from the node's pending command list.
local function queued_of_type(n, command_type)
    local out = {}
    for _, cmd in ipairs(n._queued_commands) do
        if cmd.type == command_type then
            table.insert(out, cmd)
        end
    end
    return out
end

local function define_tests()
    describe("Agent Control Handler", function()
        describe("session context", function()
            it("merges set keys with existing session_context", function()
                local n = make_node({ session_context = { existing = "keep" } })

                local changes = control_handler.process_session_context(
                    { context = { session = { set = { added = "new" } } } }, n)

                local meta = n:metadata()
                test.eq(meta.session_context.existing, "keep", "existing key preserved")
                test.eq(meta.session_context.added, "new", "new key set")
                test.eq(changes.session_context.added.action, "set", "change recorded")
            end)

            it("deletes a key while preserving the rest", function()
                local n = make_node({ session_context = { a = "1", b = "2" } })

                control_handler.process_session_context(
                    { context = { session = { delete = { "a" } } } }, n)

                local meta = n:metadata()
                test.is_nil(meta.session_context.a, "deleted key removed")
                test.eq(meta.session_context.b, "2", "other key preserved")
            end)

            it("applies set and delete together", function()
                local n = make_node({ session_context = { a = "1", b = "2" } })

                control_handler.process_session_context(
                    { context = { session = { set = { c = "3" }, delete = { "a" } } } }, n)

                local meta = n:metadata()
                test.is_nil(meta.session_context.a)
                test.eq(meta.session_context.b, "2")
                test.eq(meta.session_context.c, "3")
            end)

            it("returns empty when no session block present", function()
                local n = make_node({})
                local changes = control_handler.process_session_context({ context = {} }, n)
                test.eq(next(changes), nil, "no changes")
                test.eq(#queued_of_type(n, consts.COMMAND_TYPES.UPDATE_NODE), 0, "no command queued")
            end)
        end)

        describe("public metadata", function()
            it("merges set map with existing public_meta", function()
                local n = make_node({ public_meta = { keep = { id = "keep" } } })

                control_handler.process_public_metadata(
                    { context = { public_meta = { set = { added = { id = "added" } } } } }, n)

                local meta = n:metadata()
                test.eq(meta.public_meta.keep.id, "keep")
                test.eq(meta.public_meta.added.id, "added")
            end)

            it("accepts array-of-items set form keyed by id", function()
                local n = make_node({})

                control_handler.process_public_metadata(
                    { context = { public_meta = { set = { { id = "x", title = "X" } } } } }, n)

                test.eq(n:metadata().public_meta.x.title, "X")
            end)

            it("deletes ids and clears by type", function()
                local n = make_node({ public_meta = {
                    a = { id = "a", type = "doc" },
                    b = { id = "b", type = "doc" },
                    c = { id = "c", type = "note" }
                } })

                control_handler.process_public_metadata(
                    { context = { public_meta = { delete = { "c" }, clear = "doc" } } }, n)

                local meta = n:metadata()
                test.is_nil(meta.public_meta.a, "cleared by type")
                test.is_nil(meta.public_meta.b, "cleared by type")
                test.is_nil(meta.public_meta.c, "deleted by id")
            end)
        end)

        describe("memory", function()
            it("adds memory as AGENT_MEMORY data with discriminator", function()
                local n = make_node({})

                local changes = control_handler.process_memory_operations(
                    { memory = { add = { { type = "fact", text = "the sky is blue" } } } }, n, 3)

                local data_cmds = queued_of_type(n, consts.COMMAND_TYPES.CREATE_DATA)
                test.eq(#data_cmds, 1, "one data command queued")
                test.eq(data_cmds[1].payload.data_type, agent_consts.DATA_TYPE.AGENT_MEMORY)
                test.eq(data_cmds[1].payload.discriminator, "fact", "discriminator set to memory type")
                test.eq(changes[1].action, "add")
            end)

            it("deletes memory by id via DELETE_DATA", function()
                local n = make_node({})

                control_handler.process_memory_operations(
                    { memory = { delete = { "mem-1", "mem-2" } } }, n, 1)

                local del = queued_of_type(n, consts.COMMAND_TYPES.DELETE_DATA)
                test.eq(#del, 2, "two delete commands")
                test.eq(del[1].payload.data_id, "mem-1")
                test.eq(del[2].payload.data_id, "mem-2")
            end)

            it("clears memory by type, deleting matching rows", function()
                local n = make_node({}, {
                    { data_id = "row-1" },
                    { data_id = "row-2" }
                })

                local changes = control_handler.process_memory_operations(
                    { memory = { clear = "fact" } }, n, 1)

                local del = queued_of_type(n, consts.COMMAND_TYPES.DELETE_DATA)
                test.eq(#del, 2, "deletes both matching rows")
                test.eq(changes[1].action, "clear")
                test.eq(changes[1].deleted, 2)
            end)

            it("clears multiple types when given an array", function()
                local n = make_node({}, { { data_id = "r" } })

                local changes = control_handler.process_memory_operations(
                    { memory = { clear = { "fact", "note" } } }, n, 1)

                test.eq(#changes, 2, "one change per type")
                test.eq(#queued_of_type(n, consts.COMMAND_TYPES.DELETE_DATA), 2, "one delete per type row")
            end)
        end)

        describe("config / yield / delegate passthrough", function()
            it("surfaces agent and model changes", function()
                local n = make_node({})
                local _, response = control_handler.process_control_directive(
                    { _control = { config = { agent = "agent:b", model = "model:x" } } }, n, 1)

                test.eq(response.agent_change, "agent:b")
                test.eq(response.model_change, "model:x")
            end)

            it("surfaces trait and tool overlays from config", function()
                local n = make_node({})
                local _, response = control_handler.process_control_directive(
                    { _control = { config = { traits = { "researcher" }, tools = { "wippy.x:tool" } } } }, n, 1)

                test.eq((response.traits_change or {})[1], "researcher")
                test.eq((response.tools_change or {})[1], "wippy.x:tool")
            end)

            it("passes yield through and queues yield commands", function()
                local n = make_node({})
                local _, response = control_handler.process_control_directive(
                    { _control = { yield = {
                        user_context = { run_node_ids = { "child-1" } },
                        commands = { { type = consts.COMMAND_TYPES.CREATE_NODE, payload = { node_id = "child-1" } } }
                    } } }, n, 1)

                test.not_nil(response.yield, "yield surfaced")
                test.eq(#queued_of_type(n, consts.COMMAND_TYPES.CREATE_NODE), 1, "yield command queued")
            end)

            it("passes delegate array through untouched", function()
                local n = make_node({})
                local _, response = control_handler.process_control_directive(
                    { _control = { delegate = { { agent = "agent:c", traits = { "t1" } } } } }, n, 1)

                local delegate = response.delegate or {}
                test.eq((delegate[1] or {}).agent, "agent:c")
            end)
        end)

        describe("process_control_directive", function()
            it("strips _control and records applied changes", function()
                local n = make_node({ session_context = {} })
                local cleaned, response = control_handler.process_control_directive({
                    result = "ok",
                    _control = {
                        context = { session = { set = { k = "v" } } },
                        memory = { add = { { type = "fact", text = "x" } } }
                    }
                }, n, 1)

                test.is_nil(cleaned._control, "_control removed from result")
                test.eq(cleaned.result, "ok", "rest of result preserved")
                test.not_nil(response.changes_applied.session_context, "context change applied")
                test.not_nil(response.changes_applied.memory, "memory change applied")
            end)

            it("returns result unchanged when no _control present", function()
                local n = make_node({})
                local cleaned, response = control_handler.process_control_directive({ result = "plain" }, n, 1)
                test.eq(cleaned.result, "plain")
                test.is_nil(response)
            end)

            it("leaves a non-table tool result untouched", function()
                local n = make_node({})
                local cleaned, response = control_handler.process_control_directive("just a string", n, 1)
                test.eq(cleaned, "just a string")
                test.is_nil(response)
            end)

            it("applies context, memory and config together in one directive", function()
                local n = make_node({ session_context = { keep = "x" } }, { { data_id = "old" } })
                local cleaned, response = control_handler.process_control_directive({
                    result = "done",
                    _control = {
                        context = { session = { set = { added = "y" }, delete = { "keep" } } },
                        memory = { add = { { type = "fact", text = "f" } }, clear = "stale" },
                        config = { agent = "agent:next" }
                    }
                }, n, 5)

                test.is_nil(cleaned._control)
                test.eq(cleaned.result, "done")
                test.not_nil(response.changes_applied.session_context)
                test.not_nil(response.changes_applied.memory)
                test.eq(response.agent_change, "agent:next")
                test.eq(n:metadata().session_context.added, "y")
                test.is_nil(n:metadata().session_context.keep)
            end)
        end)

        describe("session context edge cases", function()
            it("overwrites an existing key on set", function()
                local n = make_node({ session_context = { a = "old" } })
                control_handler.process_session_context(
                    { context = { session = { set = { a = "new" } } } }, n)
                test.eq(n:metadata().session_context.a, "new")
            end)

            it("ignores deleting a missing key without error", function()
                local n = make_node({ session_context = { a = "1" } })
                local changes = control_handler.process_session_context(
                    { context = { session = { delete = { "ghost" } } } }, n)
                test.eq(changes.session_context.ghost.action, "delete")
                test.eq(n:metadata().session_context.a, "1", "real key untouched")
            end)
        end)

        describe("public metadata edge cases", function()
            it("is a no-op when clear matches nothing", function()
                local n = make_node({ public_meta = { a = { id = "a", type = "doc" } } })
                control_handler.process_public_metadata(
                    { context = { public_meta = { clear = "missing" } } }, n)
                test.not_nil(n:metadata().public_meta.a, "nothing removed")
            end)

            it("combines set, delete and clear in one call", function()
                local n = make_node({ public_meta = {
                    old = { id = "old", type = "doc" },
                    drop = { id = "drop", type = "note" }
                } })
                control_handler.process_public_metadata({ context = { public_meta = {
                    set = { fresh = { id = "fresh", type = "note" } },
                    delete = { "drop" },
                    clear = "doc"
                } } }, n)

                local meta = n:metadata().public_meta
                test.is_nil(meta.old, "cleared by type")
                test.is_nil(meta.drop, "deleted by id")
                test.eq(meta.fresh.id, "fresh", "set applied")
            end)
        end)

        describe("memory edge cases", function()
            it("skips memory items missing type or text", function()
                local n = make_node({})
                control_handler.process_memory_operations({ memory = { add = {
                    { type = "ok", text = "valid" },
                    { text = "no type" },
                    { type = "no text" }
                } } }, n, 1)
                test.eq(#queued_of_type(n, consts.COMMAND_TYPES.CREATE_DATA), 1, "only valid item stored")
            end)

            it("reports zero deleted when clear finds nothing", function()
                local n = make_node({}, {})
                local changes = control_handler.process_memory_operations(
                    { memory = { clear = "none" } }, n, 1)
                test.eq(changes[1].deleted, 0)
                test.eq(#queued_of_type(n, consts.COMMAND_TYPES.DELETE_DATA), 0)
            end)

            it("handles add, clear and delete in a single operation", function()
                local n = make_node({}, { { data_id = "to-clear" } })
                local changes = control_handler.process_memory_operations({ memory = {
                    add = { { type = "fact", text = "new" } },
                    clear = "fact",
                    delete = { "explicit-id" }
                } }, n, 2)

                test.eq(#queued_of_type(n, consts.COMMAND_TYPES.CREATE_DATA), 1, "added")
                test.eq(#queued_of_type(n, consts.COMMAND_TYPES.DELETE_DATA), 2, "cleared row + explicit delete")
                test.eq(#changes, 3, "three change entries")
            end)
        end)

        describe("artifacts", function()
            it("creates ARTIFACT data for items with content", function()
                local n = make_node({})
                local changes = control_handler.process_artifacts(
                    { artifacts = { { title = "Note", content = "hello", type = "inline" } } }, n, 1)

                local data_cmds = queued_of_type(n, consts.COMMAND_TYPES.CREATE_DATA)
                test.eq(#data_cmds, 1)
                test.eq(data_cmds[1].payload.data_type, consts.DATA_TYPE.ARTIFACT)
                test.eq(changes[1].title, "Note")
            end)

            it("skips artifacts without content", function()
                local n = make_node({})
                local changes = control_handler.process_artifacts(
                    { artifacts = { { title = "Empty" } } }, n, 1)
                test.eq(#queued_of_type(n, consts.COMMAND_TYPES.CREATE_DATA), 0)
                test.eq(#changes, 0)
            end)
        end)

        describe("apply_control_responses", function()
            local function mock_agent_ctx()
                local calls = { agent = {}, model = {}, traits = {}, tools = {} }
                local ctx = {
                    switch_to_agent = function(_, id, _opts)
                        table.insert(calls.agent, id)
                        return true, nil
                    end,
                    switch_to_model = function(_, m)
                        table.insert(calls.model, m)
                        return true, nil
                    end,
                    set_active_traits = function(_, t)
                        table.insert(calls.traits, t)
                    end,
                    set_active_tools = function(_, t)
                        table.insert(calls.tools, t)
                    end
                }
                return ctx, calls
            end

            it("routes agent and model changes and persists them to node config", function()
                local n = make_node({})
                local ctx, calls = mock_agent_ctx()
                local summary, err = control_handler.apply_control_responses({
                    { agent_change = "agent:b" },
                    { model_change = "model:x" }
                }, ctx, n)

                test.is_nil(err)
                test.is_true(summary.agent_changed)
                test.is_true(summary.model_changed)
                test.eq(calls.agent[1], "agent:b")
                test.eq(calls.model[1], "model:x")

                local cfg = n:config()
                test.eq(cfg.agent, "agent:b", "agent persisted to config for recovery")
                test.eq(cfg.model, "model:x", "model persisted to config for recovery")
            end)

            it("routes trait and tool overlays and persists them to node config", function()
                local n = make_node({})
                local ctx, calls = mock_agent_ctx()
                local summary, err = control_handler.apply_control_responses({
                    { traits_change = { "researcher" } },
                    { tools_change = { "wippy.x:tool" } }
                }, ctx, n)

                test.is_nil(err)
                test.is_true(summary.traits_changed)
                test.is_true(summary.tools_changed)
                test.eq((calls.traits[1] or {})[1], "researcher")
                test.eq((calls.tools[1] or {})[1], "wippy.x:tool")

                local cfg = n:config()
                test.eq((cfg.active_traits or {})[1], "researcher", "traits persisted for recovery")
                test.eq((cfg.active_tools or {})[1], "wippy.x:tool", "tools persisted for recovery")
            end)

            it("applies an agent switch before the trait/tool overlay in one directive", function()
                -- switch_to_agent resets active overlays, so for a combined directive the
                -- agent must switch first and the overlay must land on the new agent.
                local n = make_node({})
                local order = {}
                local ctx = {
                    switch_to_agent = function(_, id)
                        table.insert(order, "agent")
                        return true, nil
                    end,
                    switch_to_model = function(_, _m)
                        table.insert(order, "model")
                        return true, nil
                    end,
                    set_active_traits = function(_, _t)
                        table.insert(order, "traits")
                    end,
                    set_active_tools = function(_, _t)
                        table.insert(order, "tools")
                    end
                }

                control_handler.apply_control_responses({
                    { agent_change = "agent:b", traits_change = { "x" }, tools_change = { "y" } }
                }, ctx, n)

                local agent_idx, traits_idx
                for i, step in ipairs(order) do
                    if step == "agent" then agent_idx = i end
                    if step == "traits" then traits_idx = i end
                end
                test.not_nil(agent_idx, "agent switched")
                test.not_nil(traits_idx, "traits set")
                test.is_true(agent_idx < traits_idx, "agent switch precedes trait overlay")

                local cfg = n:config()
                test.eq(cfg.agent, "agent:b", "new agent persisted")
                test.eq((cfg.active_traits or {})[1], "x", "overlay persisted alongside the new agent")
            end)

            it("clears persisted overlays when only the agent changes", function()
                local n = make_node({})
                local ctx = mock_agent_ctx()

                control_handler.apply_control_responses({ { agent_change = "agent:c" } }, ctx, n)

                local cfg = n:config()
                test.eq(cfg.active_traits, false, "trait overlay cleared on agent switch")
                test.eq(cfg.active_tools, false, "tool overlay cleared on agent switch")
            end)

            it("lets a later agent change clear an overlay set in an earlier response", function()
                local n = make_node({})
                local ctx = mock_agent_ctx()

                control_handler.apply_control_responses({
                    { traits_change = { "x" } },
                    { agent_change = "agent:b" }
                }, ctx, n)

                local cfg = n:config()
                test.eq(cfg.agent, "agent:b")
                test.eq(cfg.active_traits, false, "earlier overlay cleared by the later agent switch")
            end)

            it("persists an empty overlay as an explicit clear, not the agent-switch marker", function()
                local n = make_node({})
                local ctx = mock_agent_ctx()

                control_handler.apply_control_responses({
                    { traits_change = {}, tools_change = {} }
                }, ctx, n)

                local cfg = n:config()
                -- node.lua applies a table overlay (here empty) and skips the `false` marker,
                -- so an empty list must persist as a table to mean "explicit clear".
                test.eq(type(cfg.active_traits), "table")
                test.eq(#cfg.active_traits, 0)
                test.eq(type(cfg.active_tools), "table")
                test.eq(#cfg.active_tools, 0)
            end)

            it("collects errors when a switch fails", function()
                local n = make_node({})
                local ctx = {
                    switch_to_agent = function(_, _id) return nil, "boom" end,
                    switch_to_model = function(_, _m) return true, nil end
                }
                local summary, err = control_handler.apply_control_responses({
                    { agent_change = "agent:bad" }
                }, ctx, n)

                test.not_nil(err, "error surfaced")
                test.is_true(not summary.agent_changed)
                test.is_true(#summary.errors > 0)
            end)
        end)
    end)
end

return test.run_cases(define_tests)
