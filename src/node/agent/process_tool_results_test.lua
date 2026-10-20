local test = require("test")
local agent_node = require("agent_node")
local agent_consts = require("agent_consts")

-- A minimal node_sdk double: records every n:data() call so tests can assert
-- on which observations process_tool_results actually wrote.
local function make_recording_node()
    local recorded = {}
    local n
    n = {
        node_id = "test-node",
        data = function(_self, data_type, content, opts)
            table.insert(recorded, {
                data_type = data_type,
                content = content,
                opts = opts
            })
        end
    }
    return n, recorded
end

local function find_by_tool_call_id(recorded, tool_call_id)
    for _, row in ipairs(recorded) do
        local meta = row.opts and row.opts.metadata or {}
        if meta.tool_call_id == tool_call_id then
            return row
        end
    end
    return nil
end

local function define_tests()
    describe("behavior policy settlement", function()
        it("settles sibling outcomes before policies on a successful finish", function()
            local n, recorded = make_recording_node()
            n.update_metadata = function() end
            n.yield = function() return true end
            local complete, result, err = agent_node._test.finalize_iteration(
                n, {}, {}, 1, 10, 0, "any", "finish",
                { tool_calls = {
                    { id = "finish", name = "finish", arguments = { answer = "done" } },
                    { id = "sibling", name = "read", arguments = {} },
                } }, {}, { sibling = { error = "read failed" } }, {}, {}, { {} }
            )
            test.is_nil(err)
            test.is_true(complete)
            test.eq((result or {}).answer, "done")
            test.not_nil(find_by_tool_call_id(recorded, "sibling"))
        end)

        local function settle(persist_error)
            local n, recorded = make_recording_node()
            local metadata: table = {}
            local events = {}
            local persisted = 0
            n.metadata = function() return metadata end
            n.update_metadata = function(_, changes)
                for key, value in pairs(changes) do metadata[key] = value end
            end
            n.update_config = function() events[#events + 1] = "config" end
            n.yield = function()
                if persist_error then return nil, persist_error end
                persisted = #recorded
                events[#events + 1] = "persist"
                return true
            end
            local agent_ctx = { switch_to_model = function()
                test.eq(persisted, 1, "failed tool outcome is durable before policy applies")
                events[#events + 1] = "switch"
                return true
            end }
            local complete, result, err = agent_node._test.finalize_iteration(
                n, agent_ctx, {}, 1, 10, 0, "auto", nil,
                { tool_calls = { { id = "failed-tool", name = "read", arguments = {} } } },
                {}, { ["failed-tool"] = { error = "read failed" } }, {}, {},
                { { config = { model = "model:recovery" }, context = { session = { set = { escalated = true } } } } }
            )
            return metadata, events, complete, result, err
        end

        it("records every outcome before policy switches and persists completion", function()
            local metadata, events, _, _, err = settle(nil)
            test.is_nil(err)
            test.eq(events[1], "persist")
            test.eq(events[2], "switch")
            test.eq(events[#events], "persist")
            test.is_true(metadata.session_context.escalated)
            test.eq(metadata.behavior_pending, false)
        end)

        it("does not apply policies after a failed observation commit", function()
            local metadata, events, _, _, err = settle("commit failed")
            test.eq(err, "commit failed")
            test.eq(#events, 0)
            test.is_nil(metadata.session_context)
            test.eq(metadata.behavior_pending.controls[1].config.model, "model:recovery")
        end)

        it("keeps a compact request when the host disables checkpointing or lacks a provider", function()
            for _, config in ipairs({ { checkpoint = { enabled = false } }, { checkpoint = {} } }) do
                local n = { metadata = function() return { checkpoint_requested = true } end }
                local marker, err = agent_node._test.maybe_checkpoint_history(n, config, {}, {}, "app:agent", "model", 1)
                test.is_nil(marker)
                test.is_nil(err)
            end
        end)

        it("recovers persisted proposals without re-executing resolved tools", function()
            local metadata: table = { behavior_pending = {
                iteration = 1, controls = {{ context = { session = { set = { recovered = true } } } }},
            } }
            local action = { content = { result = "done", tool_calls = {} },
                metadata = { iteration = 1 }, content_type = "application/json" }
            local selected
            local query = {}
            query.with_nodes = function(self) return self end
            query.with_data_types = function(self, kind) selected = kind; return self end
            query.order_by = function(self) return self end
            query.all = function()
                return selected == agent_consts.DATA_TYPE.AGENT_ACTION and { action } or {}
            end
            local yields = 0
            local n = {
                metadata = function() return metadata end,
                query = function() return query end,
                update_metadata = function(_, changes)
                    for key, value in pairs(changes) do metadata[key] = value end
                end,
                yield = function() yields = yields + 1; return true end,
            }
            local caller = { execute = function() error("resolved tools must not re-execute") end }
            local complete, result, _, err = agent_node._test.recover_persisted_action(
                n, {}, {}, caller, {}, {}, 1, 10, 0, "auto", nil, false, "app:agent", "model:test"
            )
            test.is_nil(err)
            test.is_true(complete)
            test.eq(result, "done")
            test.is_true(metadata.session_context.recovered)
            test.eq(metadata.behavior_pending, false)
            test.eq(yields, 1)
        end)
    end)

    describe("process_tool_results: exit validator rejection", function()
        for _, validation in ipairs({
            {
                name = "exit schema",
                config = {
                    exit_schema = {
                        type = "object",
                        properties = { answer = { type = "string" } },
                        required = { "answer" }
                    }
                }
            },
            {
                name = "exit function",
                config = { exit_func_id = "userspace.dataflow.node.agent.stub:exit_reject_validator" }
            }
        }) do
            it("answers every finish after rejection by the " .. validation.name, function()
                local n, recorded = make_recording_node()
                local calls = {
                    { id = "search_before", name = "kb_search", arguments = {} },
                    { id = "finish_first", name = "finish", arguments = {} },
                    { id = "finish_second", name = "finish", arguments = { answer = "must not win" } },
                    { id = "search_after", name = "kb_search", arguments = {} },
                    { id = "finish_third", name = "finish", arguments = {} }
                }
                local responses, delegations, complete, result = agent_node._test.process_tool_results(
                    n,
                    {
                        search_before = { result = { hits = 1 } },
                        search_after = { error = "search unavailable" }
                    },
                    1, "finish", { tool_calls = calls }, validation.config, {}, {}
                )

                test.eq(complete, false, "a later valid finish cannot override the first rejection")
                test.is_nil(result, "the rejected turn produces no final output")
                test.eq(#responses, 0)
                test.eq(#delegations, 0)
                test.eq(#recorded, #calls, "exactly one observation per call")

                local keys = {}
                for _, call in ipairs(calls) do
                    local matches = 0
                    for _, row in ipairs(recorded) do
                        if row.opts.metadata.tool_call_id == call.id then matches = matches + 1 end
                    end
                    test.eq(matches, 1, "one result for " .. call.id)
                    local observation = find_by_tool_call_id(recorded, call.id)
                    test.not_nil(observation)
                    test.eq(observation.data_type, agent_consts.DATA_TYPE.AGENT_OBSERVATION)
                    test.eq(observation.opts.metadata.tool_name, call.name)
                    if call.name == "finish" then
                        test.eq(observation.opts.metadata.is_error, true)
                        test.eq(observation.opts.metadata.exit_validation, true)
                        test.is_nil(keys[observation.opts.key], "finish observations have distinct keys")
                        keys[observation.opts.key] = true
                    end
                end
                test.contains(find_by_tool_call_id(recorded, "finish_second").content, "once")
                test.contains(find_by_tool_call_id(recorded, "finish_third").content, "once")
                test.eq(find_by_tool_call_id(recorded, "search_after").opts.metadata.is_error, true)
            end)
        end

        it("keeps the first successful finish result and rejects duplicate finishes", function()
            local n, recorded = make_recording_node()
            local _responses, _delegations, complete, result = agent_node._test.process_tool_results(
                n, {}, 1, "finish", {
                    tool_calls = {
                        { id = "finish_first", name = "finish", arguments = { answer = "first" } },
                        { id = "finish_second", name = "finish", arguments = { answer = "second" } },
                        { id = "finish_third", name = "finish", arguments = {} }
                    }
                }, {}, {}, {}
            )
            test.eq(complete, true)
            test.eq((result :: any).answer, "first", "later calls never replace the completion output")
            test.eq(#recorded, 2, "both duplicates are explicitly rejected")
            for _, id in ipairs({ "finish_second", "finish_third" }) do
                local rejection = find_by_tool_call_id(recorded, id)
                test.not_nil(rejection)
                test.eq(rejection.opts.metadata.is_error, true)
                test.contains(rejection.content, "once")
            end
        end)

        it("still records observations for sibling tool calls in the same turn", function()
            local process_tool_results = agent_node._test.process_tool_results
            test.not_nil(process_tool_results, "process_tool_results exported for testing")

            local n, recorded = make_recording_node()

            local agent_result = {
                tool_calls = {
                    { id = "call_finish", name = "finish", arguments = { answer = "done" } },
                    { id = "call_search_1", name = "kb_search", arguments = { query = "a" } },
                    { id = "call_search_2", name = "kb_search", arguments = { query = "b" } },
                }
            }

            -- Only the sibling calls were actually executed by execute_tools; the
            -- exit call never appears in tool_results (matches production: split_exit_tool_calls
            -- routes it away from the executable set).
            local tool_results = {
                call_search_1 = { result = { hits = 1 } },
                call_search_2 = { result = { hits = 2 } },
            }

            local arena_config = {
                exit_func_id = "userspace.dataflow.node.agent.stub:exit_reject_validator"
            }

            local control_responses, control_delegations, task_complete, final_result = process_tool_results(
                n,
                tool_results,
                1,
                "finish",
                agent_result,
                arena_config,
                {},
                {}
            )

            test.eq(task_complete, false, "rejected finish does not complete the task")
            test.is_nil(final_result, "no final result on rejection")
            test.eq(#control_responses, 0, "no control responses for plain tool results")
            test.eq(#control_delegations, 0, "no delegations for plain tool results")

            local rejection = find_by_tool_call_id(recorded, "call_finish")
            test.not_nil(rejection, "rejection observation recorded for the finish call")
            test.eq(rejection.data_type, agent_consts.DATA_TYPE.AGENT_OBSERVATION, "rejection is an observation")
            test.eq((rejection.opts.metadata or {}).is_error, true, "rejection observation flagged as error")
            test.eq((rejection.opts.metadata or {}).exit_validation, true, "rejection observation flagged as exit validation")

            local sibling_1 = find_by_tool_call_id(recorded, "call_search_1")
            test.not_nil(sibling_1, "sibling call_search_1 got a recorded observation")
            test.eq((sibling_1.opts.metadata or {}).tool_name, "kb_search", "sibling observation carries tool name")
            test.eq((sibling_1.opts.metadata or {}).is_error, false, "sibling observation is not an error")

            local sibling_2 = find_by_tool_call_id(recorded, "call_search_2")
            test.not_nil(sibling_2, "sibling call_search_2 got a recorded observation")

            test.eq(#recorded, 3, "exactly one observation per tool_use id: rejection plus both siblings")
        end)

        it("a genuine completion still returns early without touching sibling results", function()
            local process_tool_results = agent_node._test.process_tool_results

            local n, recorded = make_recording_node()

            local agent_result = {
                tool_calls = {
                    { id = "call_finish", name = "finish", arguments = { answer = "done" } },
                    { id = "call_search_1", name = "kb_search", arguments = { query = "a" } },
                }
            }

            local tool_results = {
                call_search_1 = { result = { hits = 1 } },
            }

            -- No exit_func_id: the exit tool call is accepted unconditionally.
            local arena_config = {}

            local _control_responses, _control_delegations, task_complete, final_result = process_tool_results(
                n,
                tool_results,
                1,
                "finish",
                agent_result,
                arena_config,
                {},
                {}
            )

            test.eq(task_complete, true, "unconditional finish completes the task")
            test.eq((final_result :: any).answer, "done", "final result carries the finish arguments")
            test.eq(#recorded, 0, "success path does not record sibling tool observations")
        end)

        it("rejects a finish call that omits a required exit field", function()
            local process_tool_results = agent_node._test.process_tool_results
            local n, recorded = make_recording_node()
            local agent_result = {
                tool_calls = {
                    { id = "call_finish", name = "finish", arguments = {} },
                }
            }

            local _responses, _delegations, task_complete, final_result = process_tool_results(
                n,
                {},
                1,
                "finish",
                agent_result,
                {
                    exit_schema = {
                        type = "object",
                        properties = { answer = { type = "string" } },
                        required = { "answer" }
                    }
                },
                {},
                {}
            )

            test.eq(task_complete, false, "invalid finish does not complete the task")
            test.is_nil(final_result, "invalid finish has no final result")
            local rejection = find_by_tool_call_id(recorded, "call_finish")
            test.not_nil(rejection, "schema rejection is recorded")
            test.contains(rejection.content, "answer", "rejection identifies the missing field")
        end)

        it("accepts a finish call that satisfies the exit schema", function()
            local process_tool_results = agent_node._test.process_tool_results
            local n, recorded = make_recording_node()
            local agent_result = {
                tool_calls = {
                    { id = "call_finish", name = "finish", arguments = { answer = "done" } },
                }
            }

            local _responses, _delegations, task_complete, final_result = process_tool_results(
                n,
                {},
                1,
                "finish",
                agent_result,
                {
                    exit_schema = {
                        type = "object",
                        properties = { answer = { type = "string" } },
                        required = { "answer" }
                    }
                },
                {},
                {}
            )

            test.eq(task_complete, true, "valid finish completes the task")
            test.eq((final_result :: any).answer, "done", "valid finish preserves its result")
            test.eq(#recorded, 0, "valid finish records no rejection")
        end)
    end)
end

return { run_tests = test.run_cases(define_tests) }
