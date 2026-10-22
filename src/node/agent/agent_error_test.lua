local test = require("test")
local agent_node = require("agent_node")
local node_sdk = require("node_sdk")
local agent_context = require("agent_context")
local prompt_builder = require("prompt_builder")
local agent_consts = require("agent_consts")
local json = require("json")

local function define_tests()
    describe("Agent failure causes", function()
        for _, case in ipairs({
            { kind = errors.PERMISSION_DENIED, retryable = false, error_type = "payment_required" },
            { kind = errors.RATE_LIMITED, retryable = true, error_type = "rate_limit_exceeded" },
            { kind = errors.NOT_FOUND, retryable = false, error_type = "model_error" },
        }) do
            it("serializes " .. case.error_type .. " without changing the node code or message", function()
                local cause = errors.new({ kind = case.kind, retryable = case.retryable,
                    message = "Model request failed", details = { error_type = case.error_type,
                        provider = "test_provider", model = "test_model", nested = { retained = true } } })
                local instance = { id = "test_agent", model = "test_model",
                    step = function() return nil, cause end }
                local config = { agent = instance.id, model = instance.model,
                    arena = { tool_calling = "auto", max_iterations = 1 } }
                local metadata = {}
                local query = {}
                query.with_nodes = function(self) return self end
                query.with_data_types = function(self) return self end
                query.order_by = function(self) return self end
                query.all = function() return {} end
                local n = { node_id = "test_node", dataflow_id = "test_workflow",
                    config = function() return config end,
                    inputs = function() return {} end,
                    query = function() return query end,
                    metadata = function() return metadata end,
                    update_metadata = function(_, update) for k, v in pairs(update) do metadata[k] = v end end,
                    fail = function(_, payload, message)
                        return { success = false, error = payload, message = message }
                    end }
                local ctx = { current_model = instance.model, current_agent_id = instance.id,
                    configure_delegate_tools = function() end,
                    load_agent = function() return instance end,
                    get_current_agent = function() return instance end,
                    get_config = function() return { current_agent_id = instance.id, current_model = instance.model } end }
                local builder = {}
                for _, method in ipairs({ "with_arena_config", "with_initial_input", "with_pending_history" }) do
                    builder[method] = function(self) return self end
                end
                builder.build_prompt = function() return {} end
                local saved_node, saved_ctx, saved_builder = node_sdk.new, agent_context.new, prompt_builder.new
                node_sdk.new = function() return n end
                agent_context.new = function() return ctx end
                prompt_builder.new = function() return builder end
                local ok, result = pcall(agent_node.run, {})
                node_sdk.new, agent_context.new, prompt_builder.new = saved_node, saved_ctx, saved_builder
                test.is_true(ok, tostring(result))

                local encoded, encode_err = json.encode(result)
                test.is_nil(encode_err)
                local stored = json.decode(encoded)
                test.is_false(stored.success)
                test.eq(stored.error.code, agent_consts.ERROR.AGENT_EXEC_FAILED)
                test.eq(stored.error.message, "Model request failed")
                test.eq(stored.message, "Model request failed")
                test.not_nil(stored.error.cause)
                test.eq(stored.error.cause.kind, case.kind)
                test.eq(stored.error.cause.message, "Model request failed")
                test.eq(stored.error.cause.retryable, case.retryable)
                test.eq(stored.error.cause.details.error_type, case.error_type)
                test.eq(stored.error.cause.details.provider, "test_provider")
                test.eq(stored.error.cause.details.model, "test_model")
                test.is_true(stored.error.cause.details.nested.retained)
            end)
        end
    end)
end

return test.run_cases(define_tests)
