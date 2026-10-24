local test = require("test")
local agent_node = require("agent_node")

local function define_tests()
    describe("Agent Node malformed tool call", function()
        it("rejects a missing tool name at the finalize-iteration boundary", function()
            local complete, result, err = agent_node._test.finalize_iteration(
                {}, {}, {}, 1, 5, 0, "auto", nil,
                {
                    tool_calls = {
                        {
                            id = "missing-name",
                            arguments = {},
                            arguments_error = "unexpected end of JSON input"
                        }
                    }
                },
                {}, {}, {}, {}, {}
            )

            test.is_nil(complete, "malformed metadata cannot complete an iteration")
            test.is_nil(result, "malformed metadata cannot produce a result")
            test.not_nil(err, "malformed metadata has a named error")
            test.eq(err.code, "MALFORMED_TOOL_CALL", "the boundary returns the public malformed-call code")
            test.eq(err.message, "Malformed tool call at index 1: name is required",
                "the boundary names the absent field")
        end)
    end)
end

return { run_tests = test.run_cases(define_tests) }
