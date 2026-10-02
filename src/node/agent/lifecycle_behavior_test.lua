local test = require("test")
local traits = require("traits")
local compiler = require("compiler")
local client = require("client")
local consts = require("consts")
local funcs = require("funcs")
local time = require("time")
local uuid = require("uuid")

local function define_tests()
    describe("agent behavior contract", function()
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

        it("transitions on trait overlays while keeping unchanged turns active", function()
            local c = client.new()
            local scenario_id = "lifecycle-overlay-" .. uuid.v7()
            local node_id = uuid.v7()
            local input_id = uuid.v7()
            local lifecycle_trait = "userspace.dataflow.node.agent.stub:lifecycle_test_trait"
            local overlay_compiled, overlay_err = compiler.compile({
                id = "app:lifecycle_overlay_test_agent",
                traits = { lifecycle_trait },
            })
            test.is_nil(overlay_err)
            test.eq(#overlay_compiled.bindings.lifecycle, 1)

            local commands = {
                {
                    type = consts.COMMAND_TYPES.CREATE_NODE,
                    payload = {
                        node_id = node_id,
                        node_type = "userspace.dataflow.node.agent:node",
                        status = consts.STATUS.PENDING,
                        config = {
                            agent = "userspace.dataflow.node.agent.stub:recovery_test_agent",
                            active_traits = { lifecycle_trait },
                            show_tool_calls = false,
                            data_targets = {
                                {
                                    data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                    key = "result",
                                    content_type = consts.CONTENT_TYPE.TEXT,
                                },
                            },
                            arena = {
                                prompt = "Run the lifecycle overlay scenario.",
                                max_iterations = 6,
                                tool_calling = "auto",
                                tools = {
                                    "userspace.dataflow.node.agent.stub:recovery_control_tool",
                                    "userspace.dataflow.node.agent.stub:recovery_tool",
                                },
                            },
                        },
                    },
                },
                {
                    type = consts.COMMAND_TYPES.CREATE_DATA,
                    payload = {
                        data_id = input_id,
                        data_type = consts.DATA_TYPE.WORKFLOW_INPUT,
                        content = { scenario_id = scenario_id, mode = "lifecycle_overlay" },
                        content_type = consts.CONTENT_TYPE.JSON,
                    },
                },
                {
                    type = consts.COMMAND_TYPES.CREATE_DATA,
                    payload = {
                        data_id = uuid.v7(),
                        data_type = consts.DATA_TYPE.NODE_INPUT,
                        node_id = node_id,
                        key = input_id,
                        content = "",
                        content_type = consts.CONTENT_TYPE.REFERENCE,
                    },
                },
            }

            local dataflow_id, create_err = c:create_workflow(commands)
            test.is_nil(create_err)
            test.not_nil(dataflow_id)
            local _, reset_err = funcs.new():call(
                "userspace.dataflow.node.agent.stub:recovery_metrics_reset",
                { scenario_id = dataflow_id }
            )
            test.is_nil(reset_err)
            local _, start_err = c:start(dataflow_id)
            test.is_nil(start_err)

            local completed = false
            for _ = 1, 250 do
                local status = c:get_status(dataflow_id)
                if status == consts.STATUS.COMPLETED_SUCCESS then
                    completed = true
                    break
                end
                if status == consts.STATUS.COMPLETED_FAILURE then
                    break
                end
                time.sleep("100ms")
            end
            test.is_true(completed, "overlay workflow completed")

            local metrics, metrics_err = funcs.new():call(
                "userspace.dataflow.node.agent.stub:recovery_metrics_get",
                { scenario_id = dataflow_id }
            )
            test.is_nil(metrics_err)
            test.eq(metrics.lifecycle_activate, 2, "initial and restored trait activate")
            test.eq(metrics.lifecycle_deactivate, 2, "overlay removal and terminal exit deactivate")
            test.eq(metrics.lifecycle_switch_deactivate, 1, "old trait deactivates on overlay switch")
            test.eq(metrics.lifecycle_switch_deactivate_refs_present, 0,
                "switch deactivation receives no new activation refs")
            test.eq(metrics.lifecycle_before_step, 3, "trait active for turns one, three, and four")
            test.eq(metrics.lifecycle_after_step, 3, "trait active for turns one, three, and four")
            local prompt_metrics, prompt_metrics_err = funcs.new():call(
                "userspace.dataflow.node.agent.stub:recovery_metrics_get",
                { scenario_id = scenario_id }
            )
            test.is_nil(prompt_metrics_err)
            test.eq(prompt_metrics.lifecycle_prompt_seen, 2, "activation context reaches both resumed prompts")
        end)
    end)
end

return { run_tests = test.run_cases(define_tests) }
