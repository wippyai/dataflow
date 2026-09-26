local test = require("test")
local traits = require("traits")
local compiler = require("compiler")

local function define_tests()
    describe("published agent behavior contract", function()
        it("compiles new behaviors alongside legacy lifecycle and checkpoint bindings", function()
            local trait, discovery_err = traits.get_by_id("app:lifecycle_behavior_trait")
            test.is_nil(discovery_err)
            test.not_nil(trait)
            test.not_nil(trait.behaviors)
            test.eq(trait.behaviors[1].id, "durable_context")

            local compiled, compile_err = compiler.compile({
                id = "app:dataflow_behavior_test_agent",
                traits = { "app:lifecycle_behavior_trait", "app:legacy_lifecycle_trait" },
            })
            test.is_nil(compile_err)
            test.not_nil(compiled)
            test.eq(#compiled.bindings.lifecycle, 3)
            test.eq(compiled.bindings.lifecycle[1].phases[1], "activate")
            test.eq(compiled.bindings.lifecycle[2].phases[1], "before_step")
            test.eq(compiled.bindings.lifecycle[1].contract, "wippy.agent:lifecycle")
            test.eq(compiled.bindings.lifecycle[1].binding, "app:lifecycle_test_binding")
            test.eq(compiled.bindings.lifecycle[3].phases[1], "deactivate")
            test.eq(compiled.bindings.lifecycle[3].binding, "app:legacy_lifecycle_test_binding")
            test.eq(#compiled.bindings.checkpoint, 2)
            test.eq(compiled.bindings.checkpoint[1].contract, "wippy.agent:checkpoint")
            test.eq(compiled.bindings.checkpoint[1].binding, "app:checkpoint_test_binding")
            test.eq(compiled.bindings.checkpoint[2].binding, "app:legacy_checkpoint_test_binding")
            test.eq(compiled.agent_options.checkpoint.token_threshold, 1200)
            test.eq(compiled.agent_options.checkpoint.max_memory_chars, 2000)
        end)
    end)
end

return { run_tests = test.run_cases(define_tests) }
