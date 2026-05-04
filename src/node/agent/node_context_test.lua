local agent_node = require("agent_node")
local agent_consts = require("agent_consts")
local test = require("test")

local function define_tests()
    describe("agent node context inheritance", function()
        it("passes node session and input context under agent_context config.context", function()
            local build = agent_node._test.build_agent_context_config
            local cfg = build(
                {
                    enable_cache = false,
                    delegate_tools = {
                        enabled = agent_consts.DELEGATE_DEFAULTS.GENERATE_TOOL_SCHEMAS,
                        description_suffix = agent_consts.DELEGATE_DEFAULTS.DESCRIPTION_SUFFIX,
                        default_schema = agent_consts.DELEGATE_DEFAULTS.SCHEMA,
                    },
                },
                {
                    dataflow_id = "df-1",
                    node_id = "node-1",
                    overlay_branch = "branch-from-arena",
                    task_id = "task-from-arena",
                    shared = "arena",
                },
                {
                    task_id = "task-from-input",
                    shared = "input",
                    input_only = "visible",
                }
            )

            test.eq(cfg.enable_cache, false)
            test.eq(cfg.delegate_tools.enabled, true)

            test.eq(cfg.context.dataflow_id, "df-1")
            test.eq(cfg.context.node_id, "node-1")
            test.eq(cfg.context.overlay_branch, "branch-from-arena")
            test.eq(cfg.context.task_id, "task-from-input")
            test.eq(cfg.context.shared, "input")
            test.eq(cfg.context.input_only, "visible")
        end)
    end)
end

return { run_tests = test.run_cases(define_tests) }
