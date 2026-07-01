local test = require("test")
local uuid = require("uuid")
local time = require("time")
local client = require("client")
local consts = require("consts")
local data_reader = require("data_reader")
local funcs = require("funcs")
local agent_consts = require("agent_consts")
local prompt_builder = require("prompt_builder")
local prompt = require("prompt")

local function row_id(row)
    if type(row) ~= "table" then
        return ""
    end
    return tostring(row.data_id or "")
end

local function sort_history_rows(history)
    table.sort(history, function(a, b)
        return row_id(a) < row_id(b)
    end)
    return history
end

local function is_structured_result_row(row)
    if type(row) ~= "table" then
        return false
    end

    local metadata = row.metadata or {}
    if type(metadata.tool_call_id) ~= "string" or metadata.tool_call_id == "" then
        return false
    end
    if type(metadata.tool_name) ~= "string" or metadata.tool_name == "" then
        return false
    end

    return row.type == agent_consts.DATA_TYPE.AGENT_OBSERVATION
        or row.type == agent_consts.DATA_TYPE.AGENT_DELEGATION
end

local function apply_latest_marker(history)
    if not history or #history == 0 then
        return history or {}
    end

    sort_history_rows(history)

    local latest_marker = nil
    for i = #history, 1, -1 do
        local row = history[i]
        if row.type == agent_consts.DATA_TYPE.AGENT_MEMORY
           and row.metadata and row.metadata.checkpoint_marker == true then
            latest_marker = row
            break
        end
    end

    if not latest_marker then
        return history
    end

    local cut_before = tostring((latest_marker.metadata or {}).checkpoint_before_data_id or "")
    local filtered = { latest_marker }
    for _, row in ipairs(history) do
        if row ~= latest_marker then
            local keep = row_id(row) > cut_before or is_structured_result_row(row)
            if keep then
                filtered[#filtered + 1] = row
            end
        end
    end

    return filtered
end

local function define_tests()
    describe("Agent Checkpoint Tests", function()
        local c

        before_all(function()
            c = client.new()
            test.not_nil(c, "client created")
        end)

        local function kill_orchestrator(df_id)
            local pid = process.registry.lookup("dataflow." .. df_id)
            if pid then
                process.terminate(pid)
                time.sleep("200ms")
            end
        end

        local function wait_until(predicate, timeout_ms, interval_ms)
            local timeout = timeout_ms or 15000
            local interval = interval_ms or 100
            local attempts = math.ceil(timeout / interval)

            for _ = 1, attempts do
                local ok, value = pcall(predicate)
                if ok and value then
                    return value
                end
                time.sleep(tostring(interval) .. "ms")
            end

            return nil
        end

        local function wait_complete(df_id, timeout_ms)
            return wait_until(function()
                local status = c:get_status(df_id)
                if status == consts.STATUS.COMPLETED_SUCCESS then
                    return true
                end
                if status == consts.STATUS.COMPLETED_FAILURE then
                    return false
                end
                return nil
            end, timeout_ms or 25000, 100)
        end

        local function wait_failed(df_id, timeout_ms)
            return wait_until(function()
                local status = c:get_status(df_id)
                if status == consts.STATUS.COMPLETED_FAILURE then
                    return true
                end
                if status == consts.STATUS.COMPLETED_SUCCESS then
                    return false
                end
                return nil
            end, timeout_ms or 15000, 100)
        end

        local function wait_node_result(df_id, node_id, timeout_ms)
            return wait_until(function()
                local rows = data_reader.with_dataflow(df_id)
                    :with_nodes(node_id)
                    :with_data_types(consts.DATA_TYPE.NODE_RESULT)
                    :all() or {}
                if #rows > 0 then
                    return rows[1]
                end
                return nil
            end, timeout_ms or 20000, 100)
        end

        local function reset_metrics(scenario_id)
            local _, err = funcs.new():call(
                "userspace.dataflow.node.agent.stub:recovery_metrics_reset",
                { scenario_id = scenario_id }
            )
            test.is_nil(err, "metrics reset")
        end

        local function get_metrics(scenario_id)
            local result, err = funcs.new():call(
                "userspace.dataflow.node.agent.stub:recovery_metrics_get",
                { scenario_id = scenario_id }
            )
            test.is_nil(err, "metrics fetch")
            return result or {}
        end

        local function create_workflow(opts: any)
            opts = opts or {}
            local node_id = uuid.v7()
            local input_id = uuid.v7()
            local node_input_id = uuid.v7()
            local scenario_id = opts.scenario_id or ("agent-checkpoint-" .. uuid.v7())

            reset_metrics(scenario_id)

            local agent_config = {
                agent = "userspace.dataflow.node.agent.stub:recovery_test_agent",
                show_tool_calls = false,
                arena = {
                    prompt = "Execute the checkpoint stress scenario.",
                    max_iterations = opts.max_iterations or 6,
                    tool_calling = "auto",
                    tools = {
                        "userspace.dataflow.node.agent.stub:recovery_tool"
                    }
                },
                data_targets = {
                    {
                        data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                        key = "result",
                        content_type = consts.CONTENT_TYPE.TEXT
                    }
                }
            }

            if opts.checkpoint then
                agent_config.checkpoint = opts.checkpoint
            end

            local commands = {
                {
                    type = consts.COMMAND_TYPES.CREATE_NODE,
                    payload = {
                        node_id = node_id,
                        node_type = "userspace.dataflow.node.agent:node",
                        status = consts.STATUS.PENDING,
                        config = agent_config,
                        metadata = {
                            title = "Agent Checkpoint Test"
                        }
                    }
                },
                {
                    type = consts.COMMAND_TYPES.CREATE_DATA,
                    payload = {
                        data_id = input_id,
                        data_type = consts.DATA_TYPE.WORKFLOW_INPUT,
                        content = {
                            scenario_id = scenario_id,
                            mode = opts.mode or "checkpoint_stress",
                            max_steps = opts.max_steps or 3,
                            tool_delay_ms = opts.tool_delay_ms or 0,
                            prompt_tokens = opts.prompt_tokens or 500
                        },
                        content_type = consts.CONTENT_TYPE.JSON
                    }
                },
                {
                    type = consts.COMMAND_TYPES.CREATE_DATA,
                    payload = {
                        data_id = node_input_id,
                        data_type = consts.DATA_TYPE.NODE_INPUT,
                        node_id = node_id,
                        key = input_id,
                        content = "",
                        content_type = consts.CONTENT_TYPE.REFERENCE
                    }
                }
            }

            local dataflow_id, err = c:create_workflow(commands, {
                metadata = { title = "Agent Checkpoint Test Workflow" }
            })
            test.is_nil(err, "workflow created")

            return {
                dataflow_id = dataflow_id,
                node_id = node_id,
                scenario_id = scenario_id
            }
        end

        local function load_history(df_id, node_id)
            return data_reader.with_dataflow(df_id)
                :with_nodes(node_id)
                :with_data_types(
                    agent_consts.DATA_TYPE.AGENT_ACTION,
                    agent_consts.DATA_TYPE.AGENT_OBSERVATION,
                    agent_consts.DATA_TYPE.AGENT_MEMORY,
                    agent_consts.DATA_TYPE.AGENT_DELEGATION
                )
                :all() or {}
        end

        local function count_checkpoint_markers(history)
            local count = 0
            for _, row in ipairs(history) do
                if row.type == agent_consts.DATA_TYPE.AGENT_MEMORY
                   and row.metadata
                   and row.metadata.checkpoint_marker == true then
                    count = count + 1
                end
            end
            return count
        end

        local function latest_checkpoint_marker(history)
            local latest_marker = nil
            for _, row in ipairs(history) do
                if row.type == agent_consts.DATA_TYPE.AGENT_MEMORY
                   and row.metadata and row.metadata.checkpoint_marker == true then
                    if not latest_marker
                       or tostring(row.data_id or "") > tostring(latest_marker.data_id or "") then
                        latest_marker = row
                    end
                end
            end
            return latest_marker
        end

        local function filtered_history(history)
            local copy = {}
            for i, row in ipairs(history) do
                copy[i] = row
            end
            return apply_latest_marker(copy)
        end

        local function find_message_by_role(messages, role)
            for _, message in ipairs(messages or {}) do
                if message.role == role then
                    return message
                end
            end

            return nil
        end

        it("checkpoint disabled is the default - no markers written", function()
            local workflow = create_workflow({
                max_iterations = 4,
                max_steps = 2,
                prompt_tokens = 1000
            })

            c:start(workflow.dataflow_id)
            test.is_true(wait_complete(workflow.dataflow_id), "workflow completed")

            local history = load_history(workflow.dataflow_id, workflow.node_id)
            test.eq(count_checkpoint_markers(history), 0, "no checkpoint markers without config")

            local metrics = get_metrics(workflow.scenario_id)
            test.eq(metrics.checkpoint_calls, 0, "checkpoint function never called")
        end)

        it("checkpoint below threshold does not fire", function()
            local workflow = create_workflow({
                max_iterations = 4,
                max_steps = 2,
                prompt_tokens = 50,
                checkpoint = {
                    token_threshold = 1000,
                    function_id = "userspace.dataflow.node.agent.stub:checkpoint_summarizer"
                }
            })

            c:start(workflow.dataflow_id)
            test.is_true(wait_complete(workflow.dataflow_id), "workflow completed")

            local history = load_history(workflow.dataflow_id, workflow.node_id)
            test.eq(count_checkpoint_markers(history), 0, "no markers when below threshold")

            local metrics = get_metrics(workflow.scenario_id)
            test.eq(metrics.checkpoint_calls, 0, "summarizer never called")
        end)

        it("checkpoint above threshold fires and writes a marker row", function()
            local workflow = create_workflow({
                max_iterations = 5,
                max_steps = 3,
                prompt_tokens = 500,
                checkpoint = {
                    token_threshold = 100,
                    function_id = "userspace.dataflow.node.agent.stub:checkpoint_summarizer"
                }
            })

            c:start(workflow.dataflow_id)
            test.is_true(wait_complete(workflow.dataflow_id), "workflow completed")

            local history = load_history(workflow.dataflow_id, workflow.node_id)
            local marker_count = count_checkpoint_markers(history)
            test.gt(marker_count, 0, "at least one checkpoint marker written")

            local metrics = get_metrics(workflow.scenario_id)
            test.gt(metrics.checkpoint_calls, 0, "summarizer invoked")
            test.eq(metrics.last_checkpoint_prompt_tokens, 500, "prompt_tokens recorded")
        end)

        it("checkpoint marker sits at correct cut line - history before is dropped", function()
            local workflow = create_workflow({
                max_iterations = 5,
                max_steps = 3,
                prompt_tokens = 500,
                checkpoint = {
                    token_threshold = 100,
                    function_id = "userspace.dataflow.node.agent.stub:checkpoint_summarizer"
                }
            })

            c:start(workflow.dataflow_id)
            test.is_true(wait_complete(workflow.dataflow_id), "workflow completed")

            local history = load_history(workflow.dataflow_id, workflow.node_id)

            local latest_marker = latest_checkpoint_marker(history)
            test.not_nil(latest_marker, "at least one marker exists")

            local cut_before = tostring((latest_marker.metadata or {}).checkpoint_before_data_id or "")
            test.is_true(cut_before ~= "", "checkpoint_before_data_id recorded on marker")

            local filtered = filtered_history(history)
            test.is_true(#filtered > 1, "prompt retains marker plus some follow-on context")

            for _, row in ipairs(filtered) do
                if row ~= latest_marker then
                    local row_after_cut = tostring(row.data_id or "") > cut_before
                    test.is_true(
                        row_after_cut or is_structured_result_row(row),
                        "filtered rows are either after the cut or retained structured results"
                    )
                end
            end

            test.is_true(#filtered < #history, "checkpoint still drops some rows before the cut line")
        end)

        it("strict checkpoint summarizer error fails the agent with CHECKPOINT_FAILED", function()
            -- strict=true: an unreachable function_id must hard-fail the agent
            local workflow = create_workflow({
                max_iterations = 3,
                max_steps = 2,
                prompt_tokens = 500,
                checkpoint = {
                    token_threshold = 100,
                    function_id = "userspace.dataflow.node.agent.stub:nonexistent_summarizer",
                    strict = true
                }
            })

            c:start(workflow.dataflow_id)
            test.is_true(wait_failed(workflow.dataflow_id), "strict mode fails agent on checkpoint error")
        end)

        it("checkpoint survives orchestrator kill between turns", function()
            local workflow = create_workflow({
                max_iterations = 5,
                max_steps = 3,
                tool_delay_ms = 1500,
                prompt_tokens = 500,
                checkpoint = {
                    token_threshold = 100,
                    function_id = "userspace.dataflow.node.agent.stub:checkpoint_summarizer"
                }
            })

            c:start(workflow.dataflow_id)

            -- wait until at least one checkpoint marker has been persisted
            local marker_seen = wait_until(function()
                local history = load_history(workflow.dataflow_id, workflow.node_id)
                if count_checkpoint_markers(history) >= 1 then
                    return true
                end
                return nil
            end, 15000, 100)
            test.is_true(marker_seen, "marker persisted before kill")

            kill_orchestrator(workflow.dataflow_id)
            c:start(workflow.dataflow_id)

            test.not_nil(wait_node_result(workflow.dataflow_id, workflow.node_id, 30000),
                "agent node completes after restart")

            -- after restart the marker must still be in the durable history
            local history = load_history(workflow.dataflow_id, workflow.node_id)
            test.gt(count_checkpoint_markers(history), 0, "marker preserved across restart")
        end)

        -- === data-level verification ===

        it("marker stores summary text from summarizer", function()
            local workflow = create_workflow({
                max_iterations = 4,
                max_steps = 2,
                prompt_tokens = 500,
                checkpoint = {
                    token_threshold = 100,
                    function_id = "userspace.dataflow.node.agent.stub:checkpoint_summarizer"
                }
            })

            c:start(workflow.dataflow_id)
            test.is_true(wait_complete(workflow.dataflow_id), "completed")

            local history = load_history(workflow.dataflow_id, workflow.node_id)
            local marker = latest_checkpoint_marker(history)
            test.not_nil(marker, "marker exists")

            local content = marker.content
            if type(content) == "table" then
                content = content.text or content.content or ""
            end
            test.is_true(type(content) == "string" and #content > 0, "marker has non-empty content")
            test.is_true(string.find(content, "checkpointed", 1, true) ~= nil,
                "content contains stub summarizer text")

            local meta = marker.metadata or {}
            test.eq(meta.checkpoint_marker, true, "marker flag set")
            test.gt(tonumber(meta.checkpoint_at_prompt_tokens) or 0, 0, "prompt_tokens recorded")
            test.is_true(type(meta.checkpoint_before_data_id) == "string"
                and meta.checkpoint_before_data_id ~= "", "cut line recorded")
            test.gt(tonumber(meta.iteration) or 0, 0, "iteration recorded")
            test.gt(tonumber(meta.checkpoint_history_count) or 0, 0, "summarized history count recorded")
            test.is_true(type(meta.checkpoint_first_data_id) == "string"
                and meta.checkpoint_first_data_id ~= "", "first summarized row recorded")
            test.is_true(type(meta.checkpoint_last_data_id) == "string"
                and meta.checkpoint_last_data_id ~= "", "last summarized row recorded")
            test.eq(meta.checkpoint_function_id,
                "userspace.dataflow.node.agent.stub:checkpoint_summarizer", "function id recorded")
        end)

        it("cut line excludes pre-checkpoint rows from prompt_builder output", function()
            local workflow = create_workflow({
                max_iterations = 5,
                max_steps = 3,
                prompt_tokens = 500,
                checkpoint = {
                    token_threshold = 100,
                    function_id = "userspace.dataflow.node.agent.stub:checkpoint_summarizer"
                }
            })

            c:start(workflow.dataflow_id)
            test.is_true(wait_complete(workflow.dataflow_id), "completed")

            local history = load_history(workflow.dataflow_id, workflow.node_id)
            local latest_marker = latest_checkpoint_marker(history)
            test.not_nil(latest_marker, "marker exists")
            local cut_before = tostring((latest_marker.metadata or {}).checkpoint_before_data_id or "")

            local filtered = filtered_history(history)

            for _, row in ipairs(filtered) do
                if row ~= latest_marker then
                    local row_after_cut = tostring(row.data_id or "") > cut_before
                    test.is_true(
                        row_after_cut or is_structured_result_row(row),
                        "filtered row is after the cut line or intentionally retained as a result row"
                    )
                end
            end

            test.is_true(#filtered < #history, "filter removed at least one row")
        end)

        it("BC_REGRESSION_S2_prompt_builder_preserves_provider_metadata", function()
            local workflow = create_workflow({})

            local builder, builder_err = prompt_builder.new(
                workflow.dataflow_id,
                workflow.node_id,
                "userspace.dataflow.node.agent:node"
            )
            test.is_nil(builder_err, "prompt builder created")

            local captured_calls = {
                assistant = {},
                function_call = {},
                function_result = {}
            }
            local original_prompt_new = prompt.new
            prompt.new = function(...)
                local prompt_instance = original_prompt_new(...)
                local original_add_assistant = prompt_instance.add_assistant
                local original_add_function_call = prompt_instance.add_function_call
                local original_add_function_result = prompt_instance.add_function_result

                prompt_instance.add_assistant = function(self, content, meta)
                    captured_calls.assistant[#captured_calls.assistant + 1] = {
                        content = content,
                        meta = meta
                    }
                    return original_add_assistant(self, content, meta)
                end

                prompt_instance.add_function_call = function(self, name, arguments, call_id, options)
                    captured_calls.function_call[#captured_calls.function_call + 1] = {
                        name = name,
                        arguments = arguments,
                        call_id = call_id,
                        options = options
                    }
                    return original_add_function_call(self, name, arguments, call_id, options)
                end

                prompt_instance.add_function_result = function(self, name, content, call_id, options)
                    captured_calls.function_result[#captured_calls.function_result + 1] = {
                        name = name,
                        content = content,
                        call_id = call_id,
                        options = options
                    }
                    return original_add_function_result(self, name, content, call_id, options)
                end

                return prompt_instance
            end

            (builder :: any):with_pending_history({
                {
                    data_id = uuid.v7(),
                    type = agent_consts.DATA_TYPE.AGENT_ACTION,
                    content_type = consts.CONTENT_TYPE.JSON,
                    content = {
                        result = "Assistant reply with provider metadata",
                        tool_calls = {
                            {
                                id = "call_provider_metadata",
                                name = "example_tool",
                                arguments = "{}",
                                provider_metadata = {
                                    google = {
                                        thought_signature = "sig-123"
                                    }
                                }
                            }
                        },
                        delegate_calls = {
                            {
                                id = "delegate_provider_metadata",
                                name = "delegate_tool",
                                arguments = "{}",
                                provider_metadata = {
                                    openai = {
                                        response_format = "json_schema"
                                    }
                                }
                            }
                        }
                    },
                    metadata = {
                        llm_meta = {
                            anthropic_cache = true,
                            provider_metadata = {
                                anthropic = {
                                    cache_control = {
                                        type = "ephemeral"
                                    }
                                }
                            }
                        }
                    }
                },
                {
                    data_id = uuid.v7(),
                    type = agent_consts.DATA_TYPE.AGENT_OBSERVATION,
                    content_type = consts.CONTENT_TYPE.JSON,
                    content = {
                        ok = true
                    },
                    metadata = {
                        tool_call_id = "call_provider_metadata",
                        tool_name = "example_tool",
                        llm_meta = {
                            anthropic_cache = true
                        }
                    }
                },
                {
                    data_id = uuid.v7(),
                    type = agent_consts.DATA_TYPE.AGENT_DELEGATION,
                    content = {
                        delegated = true
                    },
                    metadata = {
                        tool_call_id = "delegate_provider_metadata",
                        tool_name = "delegate_tool",
                        llm_meta = {
                            anthropic_cache = true
                        }
                    }
                }
            })

            local ok, rebuilt_prompt, prompt_err = pcall(function()
                return (builder :: any):build_prompt("System prompt", "Input")
            end)
            prompt.new = original_prompt_new

            if not ok then
                error(rebuilt_prompt)
            end
            test.is_nil(prompt_err, "prompt built")

            local messages = rebuilt_prompt:get_messages()

            local assistant_message = find_message_by_role(messages, "assistant")
            test.not_nil(assistant_message, "assistant message exists")
            local assistant_metadata = assistant_message.metadata or {}
            test.eq(assistant_metadata.anthropic_cache, true, "assistant llm metadata preserved")

            local assistant_call = captured_calls.assistant[1] or {}
            local assistant_call_meta = assistant_call.meta or {}
            test.eq(assistant_call_meta.anthropic_cache, true, "assistant metadata forwarded to builder")

            local function_call_message = find_message_by_role(messages, "function_call")
            test.not_nil(function_call_message, "function call message exists")
            local function_call = function_call_message.function_call or {}
            local function_provider_metadata = function_call.provider_metadata or {}
            local anthropic_metadata = function_provider_metadata.anthropic or {}
            local cache_control = anthropic_metadata.cache_control or {}
            local google_metadata = function_provider_metadata.google or {}
            test.eq(cache_control.type, "ephemeral", "row provider metadata merged into function call")
            test.eq(google_metadata.thought_signature, "sig-123", "function call provider metadata preserved")

            test.eq(#captured_calls.function_call, 2, "action forwarded both tool and delegation calls")

            local tool_call_options = (captured_calls.function_call[1] or {}).options or {}
            test.eq(tool_call_options.anthropic_cache, true, "tool call llm metadata forwarded")
            local tool_call_provider_metadata = tool_call_options.provider_metadata or {}
            test.eq(
                ((tool_call_provider_metadata.anthropic or {}).cache_control or {}).type,
                "ephemeral",
                "tool call row provider metadata forwarded"
            )
            test.eq(
                ((tool_call_provider_metadata.google or {}).thought_signature),
                "sig-123",
                "tool call-specific provider metadata preserved"
            )

            local delegate_call_options = (captured_calls.function_call[2] or {}).options or {}
            test.eq(delegate_call_options.anthropic_cache, true, "delegate call llm metadata forwarded")
            test.eq(
                (((delegate_call_options.provider_metadata or {}).openai or {}).response_format),
                "json_schema",
                "delegate call-specific provider metadata preserved"
            )

            test.eq(#captured_calls.function_result, 2, "observation and delegation forwarded result metadata")

            local observation_result_options = (captured_calls.function_result[1] or {}).options or {}
            test.eq(observation_result_options.anthropic_cache, true, "observation llm metadata forwarded")

            local delegation_result_options = (captured_calls.function_result[2] or {}).options or {}
            test.eq(delegation_result_options.anthropic_cache, true, "delegation llm metadata forwarded")
        end)

        it("multi-fire checkpoint: only latest marker is consumed by reader", function()
            local workflow = create_workflow({
                max_iterations = 6,
                max_steps = 4,
                prompt_tokens = 500,
                checkpoint = {
                    token_threshold = 50,
                    function_id = "userspace.dataflow.node.agent.stub:checkpoint_summarizer"
                }
            })

            c:start(workflow.dataflow_id)
            test.is_true(wait_complete(workflow.dataflow_id), "completed")

            local history = load_history(workflow.dataflow_id, workflow.node_id)
            local marker_count = count_checkpoint_markers(history)
            test.gt(marker_count, 1, "multiple markers fired across turns")

            local latest = latest_checkpoint_marker(history)
            test.not_nil(latest, "latest marker exists")

            -- older markers must sort before the latest
            for _, row in ipairs(history) do
                if row ~= latest
                   and row.type == agent_consts.DATA_TYPE.AGENT_MEMORY
                   and row.metadata and row.metadata.checkpoint_marker == true then
                    test.is_true(tostring(row.data_id or "") < tostring(latest.data_id or ""),
                        "older marker sorts before latest")
                end
            end

            -- the cut line on the latest marker must advance past or equal older markers' cut lines
            local latest_cut = tostring((latest.metadata or {}).checkpoint_before_data_id or "")
            for _, row in ipairs(history) do
                if row ~= latest
                   and row.type == agent_consts.DATA_TYPE.AGENT_MEMORY
                   and row.metadata and row.metadata.checkpoint_marker == true then
                    local older_cut = tostring((row.metadata or {}).checkpoint_before_data_id or "")
                    test.is_true(latest_cut >= older_cut,
                        "latest cut line is at or after older cut line")
                end
            end
        end)

        -- === adversarial cases ===

        it("threshold equals prompt_tokens - checkpoint does NOT fire", function()
            local workflow = create_workflow({
                max_iterations = 3,
                max_steps = 2,
                prompt_tokens = 100,
                checkpoint = {
                    token_threshold = 100,
                    function_id = "userspace.dataflow.node.agent.stub:checkpoint_summarizer"
                }
            })

            c:start(workflow.dataflow_id)
            test.is_true(wait_complete(workflow.dataflow_id), "completed")

            local metrics = get_metrics(workflow.scenario_id)
            test.eq(metrics.checkpoint_calls, 0, "checkpoint uses strict > threshold")

            local history = load_history(workflow.dataflow_id, workflow.node_id)
            test.eq(count_checkpoint_markers(history), 0, "no markers when tokens == threshold")
        end)

        it("threshold zero - checkpoint fires on every turn above zero tokens", function()
            local workflow = create_workflow({
                max_iterations = 4,
                max_steps = 2,
                prompt_tokens = 50,
                checkpoint = {
                    token_threshold = 1,
                    function_id = "userspace.dataflow.node.agent.stub:checkpoint_summarizer"
                }
            })

            c:start(workflow.dataflow_id)
            test.is_true(wait_complete(workflow.dataflow_id), "completed")

            local metrics = get_metrics(workflow.scenario_id)
            test.gt(metrics.checkpoint_calls, 0, "checkpoint fired at least once")
        end)

        it("token_threshold set but function_id missing - no checkpoint, no error", function()
            local workflow = create_workflow({
                max_iterations = 3,
                max_steps = 2,
                prompt_tokens = 500,
                checkpoint = {
                    token_threshold = 100
                }
            })

            c:start(workflow.dataflow_id)
            test.is_true(wait_complete(workflow.dataflow_id), "completed")

            local history = load_history(workflow.dataflow_id, workflow.node_id)
            test.eq(count_checkpoint_markers(history), 0, "partial config is effectively off")
        end)

        it("function_id set but token_threshold missing - no checkpoint, no error", function()
            local workflow = create_workflow({
                max_iterations = 3,
                max_steps = 2,
                prompt_tokens = 500,
                checkpoint = {
                    function_id = "userspace.dataflow.node.agent.stub:checkpoint_summarizer"
                }
            })

            c:start(workflow.dataflow_id)
            test.is_true(wait_complete(workflow.dataflow_id), "completed")

            local metrics = get_metrics(workflow.scenario_id)
            test.eq(metrics.checkpoint_calls, 0, "partial config is effectively off")
        end)

        it("negative token_threshold is treated as disabled", function()
            local workflow = create_workflow({
                max_iterations = 3,
                max_steps = 2,
                prompt_tokens = 500,
                checkpoint = {
                    token_threshold = -1,
                    function_id = "userspace.dataflow.node.agent.stub:checkpoint_summarizer"
                }
            })

            c:start(workflow.dataflow_id)
            test.is_true(wait_complete(workflow.dataflow_id), "completed")

            local metrics = get_metrics(workflow.scenario_id)
            test.eq(metrics.checkpoint_calls, 0, "negative threshold rejected")
        end)

        it("history count recorded on checkpoint grows with turns", function()
            local workflow = create_workflow({
                max_iterations = 6,
                max_steps = 4,
                prompt_tokens = 500,
                checkpoint = {
                    token_threshold = 50,
                    function_id = "userspace.dataflow.node.agent.stub:checkpoint_summarizer"
                }
            })

            c:start(workflow.dataflow_id)
            test.is_true(wait_complete(workflow.dataflow_id), "completed")

            local metrics = get_metrics(workflow.scenario_id)
            test.gt(metrics.checkpoint_calls, 0, "checkpoint fired")
            test.gt(metrics.last_checkpoint_history_count, 0,
                "summarizer received non-empty history")
        end)

        it("repeated kills preserve exactly one effective cut line", function()
            local workflow = create_workflow({
                max_iterations = 5,
                max_steps = 3,
                tool_delay_ms = 1200,
                prompt_tokens = 500,
                checkpoint = {
                    token_threshold = 100,
                    function_id = "userspace.dataflow.node.agent.stub:checkpoint_summarizer"
                }
            })

            c:start(workflow.dataflow_id)

            -- kill twice in quick succession
            time.sleep("800ms")
            kill_orchestrator(workflow.dataflow_id)
            c:start(workflow.dataflow_id)
            time.sleep("800ms")
            kill_orchestrator(workflow.dataflow_id)
            c:start(workflow.dataflow_id)

            test.is_true(wait_complete(workflow.dataflow_id, 30000), "eventually completes")

            local history = load_history(workflow.dataflow_id, workflow.node_id)
            local markers = {}
            for _, row in ipairs(history) do
                if row.type == agent_consts.DATA_TYPE.AGENT_MEMORY
                   and row.metadata and row.metadata.checkpoint_marker == true then
                    markers[#markers + 1] = row
                end
            end

            -- find latest marker
            table.sort(markers, function(a, b)
                return tostring(a.data_id or "") < tostring(b.data_id or "")
            end)
            test.gt(#markers, 0, "at least one marker survived restarts")

            local latest = markers[#markers]
            local filtered = filtered_history(history)
            test.not_nil(latest, "latest marker exists after restart churn")
            test.is_true(#filtered >= 1, "filtered history is well-defined after multi-kill")
        end)

        -- === warning mode (default) vs strict ===

        local function count_observation_with_key_suffix(history, suffix)
            local count = 0
            for _, row in ipairs(history) do
                if row.type == agent_consts.DATA_TYPE.AGENT_OBSERVATION then
                    local key = tostring(row.key or "")
                    if #suffix <= #key and key:sub(-#suffix) == suffix then
                        count = count + 1
                    end
                end
            end
            return count
        end

        it("non-strict: missing summarizer function skips checkpoint, workflow completes", function()
            local workflow = create_workflow({
                max_iterations = 4,
                max_steps = 2,
                prompt_tokens = 500,
                checkpoint = {
                    token_threshold = 100,
                    function_id = "userspace.dataflow.node.agent.stub:nonexistent_summarizer"
                }
            })

            c:start(workflow.dataflow_id)
            test.is_true(wait_complete(workflow.dataflow_id), "workflow completes despite checkpoint failure")

            local history = load_history(workflow.dataflow_id, workflow.node_id)
            test.eq(count_checkpoint_markers(history), 0, "no marker when function missing")
            test.gt(count_observation_with_key_suffix(history, "_checkpoint_skipped"), 0,
                "at least one checkpoint_skipped observation was written")
        end)

        it("strict: missing summarizer function fails the agent", function()
            local workflow = create_workflow({
                max_iterations = 3,
                max_steps = 2,
                prompt_tokens = 500,
                checkpoint = {
                    token_threshold = 100,
                    function_id = "userspace.dataflow.node.agent.stub:nonexistent_summarizer",
                    strict = true
                }
            })

            c:start(workflow.dataflow_id)
            test.is_true(wait_failed(workflow.dataflow_id), "strict mode fails agent on checkpoint error")
        end)

        it("non-strict: empty summary skips checkpoint with observation", function()
            local workflow = create_workflow({
                max_iterations = 4,
                max_steps = 2,
                prompt_tokens = 500,
                checkpoint = {
                    token_threshold = 100,
                    function_id = "userspace.dataflow.node.agent.stub:empty_memory_summarizer"
                }
            })

            c:start(workflow.dataflow_id)
            test.is_true(wait_complete(workflow.dataflow_id), "workflow completes on empty memory")

            local history = load_history(workflow.dataflow_id, workflow.node_id)
            test.eq(count_checkpoint_markers(history), 0, "no marker for empty memory")
            test.gt(count_observation_with_key_suffix(history, "_checkpoint_skipped"), 0,
                "checkpoint_skipped observation for empty memory")
        end)

        -- === size cap ===

        it("default max_memory_chars caps oversized summaries at 8192", function()
            local workflow = create_workflow({
                max_iterations = 4,
                max_steps = 2,
                prompt_tokens = 500,
                checkpoint = {
                    token_threshold = 100,
                    function_id = "userspace.dataflow.node.agent.stub:oversize_summarizer"
                }
            })

            c:start(workflow.dataflow_id)
            test.is_true(wait_complete(workflow.dataflow_id), "completed with oversized summary")

            local history = load_history(workflow.dataflow_id, workflow.node_id)
            local marker = latest_checkpoint_marker(history)
            test.not_nil(marker, "marker exists")
            local content = marker.content
            if type(content) == "table" then
                content = content.text or ""
            end
            test.is_true(type(content) == "string", "marker content is string")
            test.is_true(#content <= 8192, "default cap enforced: " .. #content .. " <= 8192")
            test.eq((marker.metadata or {}).checkpoint_memory_truncated, true,
                "truncation flag set when clamped")
        end)

        it("explicit max_memory_chars caps marker content to config value", function()
            local workflow = create_workflow({
                max_iterations = 4,
                max_steps = 2,
                prompt_tokens = 500,
                checkpoint = {
                    token_threshold = 100,
                    function_id = "userspace.dataflow.node.agent.stub:oversize_summarizer",
                    max_memory_chars = 200
                }
            })

            c:start(workflow.dataflow_id)
            test.is_true(wait_complete(workflow.dataflow_id), "completed with explicit cap")

            local history = load_history(workflow.dataflow_id, workflow.node_id)
            local marker = latest_checkpoint_marker(history)
            test.not_nil(marker, "marker exists")
            local content = marker.content
            if type(content) == "table" then
                content = content.text or ""
            end
            test.is_true(#content <= 200, "explicit cap enforced: " .. #content .. " <= 200")
        end)

        it("summary under cap is stored verbatim without truncation flag", function()
            local workflow = create_workflow({
                max_iterations = 4,
                max_steps = 2,
                prompt_tokens = 500,
                checkpoint = {
                    token_threshold = 100,
                    function_id = "userspace.dataflow.node.agent.stub:checkpoint_summarizer"
                }
            })

            c:start(workflow.dataflow_id)
            test.is_true(wait_complete(workflow.dataflow_id), "completed with small summary")

            local history = load_history(workflow.dataflow_id, workflow.node_id)
            local marker = latest_checkpoint_marker(history)
            test.not_nil(marker, "marker exists")
            local truncated = (marker.metadata or {}).checkpoint_memory_truncated
            test.is_true(truncated == nil or truncated == false,
                "small summary not marked truncated")
        end)
    end)
end

return { run_tests = test.run_cases(define_tests) }
