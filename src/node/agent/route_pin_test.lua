local agent_node = require("agent_node")
local test = require("test")
local node_sdk = require("node_sdk")
local agent_context = require("agent_context")
local prompt_builder = require("prompt_builder")
local agent_consts = require("agent_consts")

local function define_tests()
    describe("selection replay before the next step", function()
        for _, scenario in ipairs({
            { name = "agent", changes = { agent = "agent:new" }, drops_pin = true },
            { name = "model", changes = { model = "model:new" }, drops_pin = true },
            { name = "traits", changes = { traits = { "trait:new" } } },
            { name = "tools", changes = { tools = {} } },
            { name = "failed reload", changes = { tools = {} }, fails = true },
        }) do
            it("uses the recovered selection before another step: " .. scenario.name, function()
                local selected, seen_options
                local old_agent = { id = "agent:old", model = "model:old", step = function(_, _, options)
                    selected = "old"; seen_options = options; return nil, "probe complete" end }
                local new_agent = { id = scenario.changes.agent or old_agent.id,
                    model = scenario.changes.model or old_agent.model, step = function(_, _, options)
                        selected = "new"; seen_options = options; return nil, "probe complete" end }
                local active = old_agent
                local config = { agent = old_agent.id, model = old_agent.model,
                    arena = { tool_calling = "auto", max_iterations = 4 } }
                local metadata = { behavior_pending = { iteration = 1, controls = {
                    { config = scenario.changes }
                } } }
                local action = { content = { result = "", tool_calls = {} }, metadata = { iteration = 1 } }
                local query_kind, query_key, feedback
                local query = {}
                query.with_nodes = function(self) return self end
                query.with_data_types = function(self, kind) query_kind = kind; return self end
                query.with_data_keys = function(self, key) query_key = key; return self end
                query.order_by = function(self) return self end
                query.all = function() return query_kind == agent_consts.DATA_TYPE.AGENT_ACTION and { action } or {} end
                query.one = function() return feedback and feedback.key == query_key and feedback or nil end
                local n = { node_id = "node:test", dataflow_id = "df:test",
                    config = function() return config end,
                    inputs = function() return {} end,
                    query = function() return query end,
                    data = function(_, kind, content, options)
                        feedback = { data_type = kind, content = content, key = options.key }
                    end,
                    metadata = function() return metadata end,
                    update_metadata = function(_, update) for k,v in pairs(update) do metadata[k] = v end end,
                    update_config = function(_, update) for k,v in pairs(update) do config[k] = v end end,
                    yield = function() return true end,
                    fail = function(_, _, message) return message end,
                }
                local loaded = false
                local ctx = { current_model = old_agent.model, current_agent_id = old_agent.id,
                    configure_delegate_tools = function() end,
                    load_agent = function()
                        if not loaded then loaded = true; return old_agent end
                        if scenario.fails then return nil, "selected agent unavailable" end
                        active = new_agent
                        return new_agent
                    end,
                    get_current_agent = function() return active end,
                    get_config = function(self) return { current_agent_id = self.current_agent_id, current_model = self.current_model } end,
                    switch_to_agent = function(self)
                        active = new_agent; self.current_agent_id = new_agent.id; self.current_model = new_agent.model; return true end,
                    switch_to_model = function(self)
                        active = new_agent; self.current_model = new_agent.model; return true end,
                    set_active_traits = function(self, traits) self.active_traits = traits; active = nil; return true end,
                    set_active_tools = function(self, tools) self.active_tools = tools; active = nil; return true end,
                }
                local builder = {}
                for _, method in ipairs({ "with_arena_config", "with_initial_input", "with_pending_history" }) do
                    builder[method] = function(self) return self end
                end
                builder.build_prompt = function() return {} end
                local saved_node, saved_ctx, saved_builder = node_sdk.new, agent_context.new, prompt_builder.new
                node_sdk.new = function() return n end
                agent_context.new = function() return ctx end
                prompt_builder.new = function() return builder end
                local pin = { model = "backup", provider_id = "provider:backup" }
                local ok, result = pcall(agent_node.run, {node={metadata={state={
                    current_iteration=1, agent_id=old_agent.id, model=old_agent.model, route_pin=pin,
                }}}})
                node_sdk.new, agent_context.new, prompt_builder.new = saved_node, saved_ctx, saved_builder
                test.is_true(ok, tostring(result))
                if scenario.fails then
                    test.eq(result, "selected agent unavailable")
                    test.is_nil(selected, "a failed reload must never fall back to the old agent")
                else
                    test.eq(result, "probe complete")
                    test.eq(selected, "new", "the first model step uses the replayed selection")
                    test.eq(feedback.key, "1_empty_result")
                    test.eq(feedback.content, agent_consts.FEEDBACK.EMPTY_RESULT)
                    if scenario.drops_pin then test.is_nil(seen_options.route)
                    else test.eq(seen_options.route, pin) end
                end
            end)
        end
    end)

    describe("agent node route pinning", function()
        local carry = agent_node._test.carry_route_pin
        local after_switch = agent_node._test.route_pin_after_switch
        local pin = { model = "backup", provider_id = "p.b", provider_model = "b-1" }

        for _, owner in ipairs({
            { agent_id = "agent:a", model = "class:fast", keep = true },
            { agent_id = "agent:a", model = "class:balanced" },
            { agent_id = "agent:b", model = "class:fast" },
            {},
        }) do
            it("restores a saved pin only for its recovered agent/model: " .. tostring(owner.agent_id)
                .. "/" .. tostring(owner.model), function()
                local original_node, original_context, original_builder = node_sdk.new, agent_context.new, prompt_builder.new
                local seen, progress
                local instance = { id = "agent:a", model = "class:fast", step = function(_, _, options)
                    seen = options
                    return nil, "probe complete"
                end }
                local ctx = {
                    configure_delegate_tools = function() end,
                    get_current_agent = function() return instance end,
                    load_agent = function() return instance end,
                    get_config = function() return { current_agent_id = "agent:a", current_model = "class:fast" } end,
                }
                local query = {}
                for _, method in ipairs({ "with_nodes", "with_data_types", "order_by" }) do
                    query[method] = function(self) return self end
                end
                query.all = function() return {} end
                local n = {
                    dataflow_id = "df-recovered", node_id = "node-recovered",
                    config = function() return { agent = "agent:a", model = "class:fast",
                        arena = { tool_calling = "none", max_iterations = 2 } } end,
                    inputs = function() return {} end,
                    metadata = function() return {} end,
                    query = function() return query end,
                    update_metadata = function(_, metadata) progress = metadata.state end,
                    fail = function(_, _, message) return message end,
                }
                local builder = {}
                for _, method in ipairs({ "with_arena_config", "with_initial_input", "with_pending_history" }) do
                    builder[method] = function(self) return self end
                end
                builder.build_prompt = function() return {} end
                node_sdk.new = function() return n end
                agent_context.new = function() return ctx end
                prompt_builder.new = function() return builder end
                local ok, result = pcall(agent_node.run, { node = { metadata = { state = {
                    agent_id = owner.agent_id, model = owner.model, route_pin = pin,
                } } } })
                node_sdk.new, agent_context.new, prompt_builder.new = original_node, original_context, original_builder
                test.is_true(ok, tostring(result))
                test.eq(result, "probe complete")
                test.not_nil(seen, "the recovered node must reach its next agent step")
                if owner.keep then
                    test.eq(seen.route, pin)
                    test.eq(progress.route_pin, pin)
                else
                    test.is_nil(seen.route)
                    test.is_nil(progress.route_pin)
                end
            end)
        end

        it("pins nothing while the agent reports no route", function()
            test.is_nil(carry(nil, { result = "text" }))
            test.is_nil(carry(nil, nil))
        end)

        it("takes the route the agent pinned after a fallback", function()
            test.eq(carry(nil, { result = "", route_pin = pin }), pin)
        end)

        it("keeps the pin across a step that does not report one", function()
            test.eq(carry(pin, { result = "text" }), pin)
            test.eq(carry(pin, nil), pin)
        end)

        it("follows a new pin reported by a later step", function()
            local other = { model = "third", provider_id = "p.c", provider_model = "c-1" }
            test.eq(carry(pin, { route_pin = other }), other)
        end)

        it("keeps the pin while the run stays on the same agent and model", function()
            test.eq(after_switch(pin, "agent:a", "class:fast", "agent:a", "class:fast"), pin)
        end)

        it("drops the pin when a control directive changes the agent or the model", function()
            test.is_nil(after_switch(pin, "agent:a", "class:fast", "agent:b", "class:fast"))
            test.is_nil(after_switch(pin, "agent:a", "class:fast", "agent:a", "class:balanced"))
        end)

        it("persists the pin in the node state for a resumed run", function()
            local recorded: any = nil
            local n = { update_metadata = function(_self, metadata) recorded = metadata end }
            local tokens = { total_tokens = 10, prompt_tokens = 6, completion_tokens = 4,
                cache_read_tokens = 0, cache_write_tokens = 0, thinking_tokens = 0 }

            agent_node._test.update_node_progress(n, 2, 5, tokens, 1, "Iteration 2/5",
                "agent:a", "class:fast", 0, pin)
            test.eq(recorded.state.route_pin, pin)
            test.eq(recorded.state.current_iteration, 2)

            agent_node._test.update_node_progress(n, 3, 5, tokens, 1, "Iteration 3/5",
                "agent:a", "class:fast", 0, nil)
            test.is_nil(recorded.state.route_pin)
        end)
    end)
end

return test.run_cases(define_tests)
