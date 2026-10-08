local agent_node = require("agent_node")
local test = require("test")

local function define_tests()
    describe("agent node route pinning", function()
        local carry = agent_node._test.carry_route_pin
        local after_switch = agent_node._test.route_pin_after_switch
        local pin = { model = "backup", provider_id = "p.b", provider_model = "b-1" }

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
