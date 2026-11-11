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
            { kind = errors.PERMISSION_DENIED, retryable = false, error_type = "payment_required", details = true },
            { kind = errors.RATE_LIMITED, retryable = true, error_type = "rate_limit_exceeded" },
            { kind = errors.NOT_FOUND, retryable = false, error_type = "model_error" },
            { kind = errors.INVALID, retryable = false, error_type = "old_agent", old = true },
            { kind = errors.INTERNAL, retryable = false, error_type = "structured_details", old = true, details = true },
            { kind = errors.RATE_LIMITED, retryable = true, error_type = "write_failure", details = true, write_error = true },
        }) do
            it("serializes " .. case.error_type .. " without changing the node code or message", function()
                local native_details = { code = "request_rejected", error_type = case.error_type,
                    provider = "test_provider", model = "test_model", nested = { retained = true } }
                if case.old then native_details = nil end
                local cause = errors.new({ kind = case.kind, retryable = case.retryable,
                    message = "Model request failed", details = native_details })
                local details = case.details and { code = "request_rejected", tokens = {
                    prompt_tokens = 17, completion_tokens = 3, total_tokens = 20,
                    cache_read_tokens = 2, prompt = "private prompt", nested = { secret = "credential" }
                }, finish_reason = "error", prompt = "private prompt", secret = "credential" } or nil
                local writes, submitted = {}, {}
                local instance = { id = "test_agent", model = "test_model",
                    step = function() return nil, cause, details end }
                local config = { agent = instance.id, model = instance.model,
                    arena = { tool_calling = "auto", max_iterations = 1 } }
                local metadata = {}
                local query = {}
                query.with_nodes = function(self) return self end
                query.with_data_types = function(self) return self end
                query.order_by = function(self) return self end
                query.all = function() return {} end
                local n = node_sdk.new({ node_id = "test_node", dataflow_id = "test_workflow",
                    node = { config = config, metadata = metadata } }, {
                    commit = {
                        execute = function(_, _, commands)
                            writes[#writes + 1] = commands[1]
                            if case.write_error then return nil, "record write refused" end
                            return {}
                        end,
                        submit = function(_, _, commands)
                            for _, command in ipairs(commands) do
                                submitted[#submitted + 1] = command
                                if command.payload.data_type == agent_consts.DATA_TYPE.AGENT_ERROR and case.write_error then
                                    return nil, "record write refused"
                                end
                            end
                            return {}
                        end
                    },
                    data_reader = { with_dataflow = function() return query end },
                    process = { listen = function() return {} end, send = function() return true end }
                } :: any) :: any
                local records = {}
                local original_data = n.persist_data
                n.persist_data = function(self, kind, value, options)
                    if kind == agent_consts.DATA_TYPE.AGENT_ERROR then records[#records + 1] = {kind=kind, value=value, options=options} end
                    return original_data(self, kind, value, options)
                end
                n.inputs = function() return {} end
                local ctx = { current_model = instance.model, current_agent_id = instance.id,
                    configure_delegate_tools = function() end,
                    load_agent = function() return instance end,
                    get_current_agent = function() return instance end,
                    get_config = function() return { current_agent_id = instance.id, current_model = instance.model } end }
                local builder = {}
                for _, method in ipairs({ "with_arena_config", "with_initial_input", "with_pending_history" }) do
                    builder[method] = function(self) return self end
                end
                builder.build_prompt = function() return { get_messages = function() return { { role = "user" } } end } end
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
                test.eq(#records, 1)
                test.eq(records[1].kind, agent_consts.DATA_TYPE.AGENT_ERROR)
                local meta = records[1].options.metadata
                test.eq(meta.model, "test_model"); test.eq(meta.iteration, 1)
                test.eq(meta.error_code, agent_consts.ERROR.AGENT_EXEC_FAILED)
                test.eq(meta.error_kind, case.kind)
                test.eq(meta.retryable, case.retryable)
                if case.old and not case.details then test.is_nil(meta.code) else test.eq(meta.code, "request_rejected") end
                if case.details then
                    test.eq(meta.tokens.prompt_tokens, 17)
                    test.eq(meta.tokens.completion_tokens, 3)
                    test.eq(meta.tokens.total_tokens, 20)
                    test.eq(meta.tokens.cache_read_tokens, 2)
                    test.is_nil(meta.tokens.prompt)
                    test.is_nil(meta.tokens.nested)
                    test.eq(meta.finish_reason, "error")
                else
                    test.is_nil(meta.tokens)
                    test.is_nil(meta.finish_reason)
                end
                test.is_nil(meta.prompt)
                test.is_nil(meta.secret)
                test.is_true(meta.latency_ms >= 0)
                test.eq(records[1].value.message, "Model request failed")
                test.is_false(stored.success)
                test.eq(stored.error.code, agent_consts.ERROR.AGENT_EXEC_FAILED)
                test.eq(stored.error.message, "Model request failed")
                test.eq(stored.message, "Model request failed")
                test.not_nil(stored.error.cause)
                test.eq(stored.error.cause.kind, case.kind)
                test.eq(stored.error.cause.message, "Model request failed")
                test.eq(stored.error.cause.retryable, case.retryable)
                if case.old then
                    test.is_nil(stored.error.cause.details)
                else
                    test.eq(stored.error.cause.details.code, "request_rejected")
                    test.eq(stored.error.cause.details.error_type, case.error_type)
                    test.eq(stored.error.cause.details.provider, "test_provider")
                    test.eq(stored.error.cause.details.model, "test_model")
                    test.is_true(stored.error.cause.details.nested.retained)
                end
                test.eq(#writes, 1)
                test.eq(writes[1].payload.data_type, agent_consts.DATA_TYPE.AGENT_ERROR)
                for _, command in ipairs(submitted) do
                    test.is_true(command.payload.data_type ~= agent_consts.DATA_TYPE.AGENT_ERROR, "failure telemetry is independent of final commands")
                end
                test.eq(#n:created_data_ids(), case.write_error and 1 or 2)
            end)
        end
    end)
end

return test.run_cases(define_tests)
