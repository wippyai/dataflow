local test = require("test")
local agent_node = require("agent_node")
local agent_consts = require("agent_consts")

local resolve = agent_node._test.validate_and_resolve_config

local EXIT_SCHEMA = {
    type = "object",
    properties = { answer = { type = "string" } },
    required = { "answer" }
}

local function define_tests()
    describe("Agent Node tool_calling resolution", function()
        it("resolves a missing mode to auto when no exit_schema is declared", function()
            local config, err = resolve({ arena = { prompt = "Work." } })

            test.is_nil(err)
            test.eq(config.arena.tool_calling, agent_consts.TOOL_CALLING.AUTO)
        end)

        it("resolves a missing mode to auto when an exit_schema is declared", function()
            local config, err = resolve({ arena = { prompt = "Work.", exit_schema = EXIT_SCHEMA } })

            test.is_nil(err)
            test.eq(config.arena.tool_calling, agent_consts.TOOL_CALLING.AUTO)
        end)

        it("keeps an explicit mode", function()
            local config, err = resolve({
                arena = { prompt = "Work.", tool_calling = agent_consts.TOOL_CALLING.ANY, exit_schema = EXIT_SCHEMA }
            })

            test.is_nil(err)
            test.eq(config.arena.tool_calling, agent_consts.TOOL_CALLING.ANY)
        end)

        it("rejects any mode without an exit_schema", function()
            local config, err = resolve({ arena = { prompt = "Work.", tool_calling = agent_consts.TOOL_CALLING.ANY } })

            test.is_nil(config)
            test.eq(err, "any mode requires exit_schema to be defined")
        end)

        it("rejects none mode with an exit_schema", function()
            local config, err = resolve({
                arena = { prompt = "Work.", tool_calling = agent_consts.TOOL_CALLING.NONE, exit_schema = EXIT_SCHEMA }
            })

            test.is_nil(config)
            test.eq(err, "none mode cannot have exit_schema")
        end)
    end)
end

return test.run_cases(define_tests)
