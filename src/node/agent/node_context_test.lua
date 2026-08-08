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

    describe("agent node reserved inputs", function()
        local process = agent_node._test.process_multiple_inputs

        it("treats a nil-content reserved input as absent", function()
            -- An input_transform field whose expression resolves to nil produces
            -- an entry with nil content; the reserved carriers read it as "not
            -- provided", never as a malformed value.
            local input_context, agent_id_override, model_override, input_data, err = process({
                context = { content = nil, metadata = {} },
                model = { content = nil, metadata = {} },
                agent_id = { content = nil, metadata = {} },
                lead = { content = { name = "Jane" }, metadata = {} },
            })
            test.is_nil(err)
            test.is_nil(input_context)
            test.is_nil(agent_id_override)
            test.is_nil(model_override)
            test.is_true(input_data:find('<input key="lead">', 1, true) ~= nil)
        end)

        it("renders no input tag for a nil-content input", function()
            local _, _, _, input_data, err = process({
                empty = { content = nil, metadata = {} },
                brief = { content = "text", metadata = {} },
            })
            test.is_nil(err)
            test.is_true(input_data:find('<input key="brief">', 1, true) ~= nil)
            test.is_true(input_data:find('<input key="empty">', 1, true) == nil)
        end)

        it("still merges a table context and applies string overrides", function()
            local input_context, agent_id_override, model_override, input_data, err = process({
                context = { content = { kb_ids = { "kb-1" } }, metadata = {} },
                model = { content = "class:fast", metadata = {} },
                agent_id = { content = "ns:researcher", metadata = {} },
                lead = { content = { name = "Jane" }, metadata = {} },
            })
            test.is_nil(err)
            test.not_nil(input_context)
            test.eq((input_context :: any).kb_ids[1], "kb-1")
            test.eq(model_override, "class:fast")
            test.eq(agent_id_override, "ns:researcher")
            test.is_true(input_data:find('<input key="context">', 1, true) == nil)
        end)
    end)
end

return { run_tests = test.run_cases(define_tests) }
