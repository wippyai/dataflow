local json = require("json")
local time = require("time")
local uuid = require("uuid")
local node_sdk = require("node_sdk")
local agent_context = require("agent_context")
local tool_caller = require("tool_caller")
local prompt_builder = require("prompt_builder")
local control_handler = require("control_handler")
local delegation_handler = require("delegation_handler")
local agent_consts = require("agent_consts")
local consts = require("consts")
local tools = require("tools")
local registry = require("registry")
local funcs = require("funcs")

type ToolCall = {
    id: string,
    name: string,
    arguments: table?,
    registry_id: string?,
}

local function merge_contexts(base_context: any, input_context: any): {[string]: any}
    local merged: {[string]: any} = {}
    if base_context then
        for k, v in pairs(base_context) do
            if type(k) == "string" then
                merged[k] = v
            end
        end
    end
    if input_context then
        for k, v in pairs(input_context) do
            if type(k) == "string" then
                merged[k] = v
            end
        end
    end
    return merged
end

type DelegateToolsConfig = {
    enabled: boolean,
    description_suffix: string,
    default_schema: {
        type: string,
        properties: {[string]: any}?,
        required: any?,
    },
}

type AgentContextConfig = {
    enable_cache: boolean?,
    context: {[string]: any}?,
    delegate_tools: DelegateToolsConfig?,
    memory_contract: any?,
    context_merger: any?,
}

local function build_agent_context_config(
    base_context: {[string]: any},
    session_context: {[string]: any},
    input_context: {[string]: any}?
): AgentContextConfig
    local agent_ctx_config: AgentContextConfig = {
        enable_cache = base_context.enable_cache,
        delegate_tools = base_context.delegate_tools,
        memory_contract = base_context.memory_contract,
        context_merger = base_context.context_merger,
        context = merge_contexts(session_context, input_context),
    }
    return agent_ctx_config
end

local function format_token_count(count)
    if count >= 1000 then
        return string.format("%.1fK", count / 1000)
    else
        return tostring(count)
    end
end

local function new_total_tokens(saved_tokens)
    return {
        total_tokens = type(saved_tokens) == "table" and (saved_tokens.total_tokens or 0) or 0,
        prompt_tokens = type(saved_tokens) == "table" and (saved_tokens.prompt_tokens or 0) or 0,
        completion_tokens = type(saved_tokens) == "table" and (saved_tokens.completion_tokens or 0) or 0,
        cache_read_tokens = type(saved_tokens) == "table" and (saved_tokens.cache_read_tokens or 0) or 0,
        cache_write_tokens = type(saved_tokens) == "table" and (saved_tokens.cache_write_tokens or 0) or 0,
        thinking_tokens = type(saved_tokens) == "table" and (saved_tokens.thinking_tokens or 0) or 0
    }
end

local function decode_json_content(content: any, content_type: any)
    if type(content) ~= "string" then
        return content
    end

    if content_type ~= consts.CONTENT_TYPE.JSON and content_type ~= consts.CONTENT_TYPE.TEXT then
        local decoded_any, decoded_any_err = json.decode(content)
        if not decoded_any_err then
            return decoded_any
        end
        return content
    end

    local decoded, decode_err = json.decode(content)
    if decode_err then
        return content
    end

    return decoded
end

local function compaction_row_id(row)
    if type(row) ~= "table" then
        return ""
    end
    return tostring(row.data_id or "")
end

local function sort_compaction_rows(history_rows)
    table.sort(history_rows, function(a, b)
        return compaction_row_id(a) < compaction_row_id(b)
    end)
    return history_rows
end

local function find_latest_compaction_marker(history_rows)
    for i = #history_rows, 1, -1 do
        local item = history_rows[i]
        if item.type == agent_consts.DATA_TYPE.AGENT_MEMORY
           and item.metadata
           and item.metadata.compaction_marker == true then
            return item
        end
    end

    return nil
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

local function last_compaction_data_id(history_rows)
    local latest = ""
    for _, row in ipairs(history_rows or {}) do
        local rid = compaction_row_id(row)
        if rid > latest then
            latest = rid
        end
    end
    return latest
end

local function compactable_history_rows(history_rows)
    local compactable = {}
    for _, row in ipairs(history_rows or {}) do
        if not is_structured_result_row(row) then
            compactable[#compactable + 1] = row
        end
    end
    return compactable
end

local function build_status_message(iteration, max_iterations, total_tokens, tool_calls_count, is_final, task_complete)
    local status_parts = {}

    if is_final then
        if task_complete then
            table.insert(status_parts, string.format("Completed %d/%d", iteration, max_iterations))
        else
            table.insert(status_parts, string.format("Max iterations %d/%d", iteration, max_iterations))
        end
    else
        if iteration == 0 then
            table.insert(status_parts, "Starting agent")
        else
            table.insert(status_parts, string.format("Iteration %d/%d", iteration, max_iterations))
        end
    end

    local details = {}

    if total_tokens.prompt_tokens and total_tokens.prompt_tokens > 0 then
        table.insert(details, "in: " .. format_token_count(total_tokens.prompt_tokens))
    end

    local completion_total = (total_tokens.completion_tokens or 0) + (total_tokens.thinking_tokens or 0)
    if completion_total > 0 then
        table.insert(details, "out: " .. format_token_count(completion_total))
    end

    if tool_calls_count > 0 then
        table.insert(details, "T: " .. tool_calls_count)
    end

    if #details > 0 then
        table.insert(status_parts, table.concat(details, ", "))
    end

    return table.concat(status_parts, " - ")
end

local function process_multiple_inputs(inputs)
    local input_context = nil
    if inputs.context then
        local context_content = inputs.context.content
        if type(context_content) ~= "table" then
            return nil, nil, nil, nil, "context must be a table/object"
        end
        input_context = context_content
    end

    local agent_id_override = nil
    if inputs.agent_id then
        local agent_id_content = inputs.agent_id.content
        if type(agent_id_content) ~= "string" or agent_id_content == "" then
            return nil, nil, nil, nil, "agent_id must be a non-empty string"
        end
        agent_id_override = agent_id_content
    end

    local model_override = nil
    if inputs.model then
        local model_content = inputs.model.content
        if type(model_content) ~= "string" or model_content == "" then
            return nil, nil, nil, nil, "model must be a non-empty string"
        end
        model_override = model_content
    end

    local parts = {}
    for key, input in pairs(inputs) do
        if key ~= "context" and key ~= "agent_id" and key ~= "model" then
            local content = input.content
            if type(content) == "table" then
                content = json.encode(content)
            else
                content = tostring(content)
            end
            table.insert(parts, string.format('<input key="%s">\n%s\n</input>', key, content))
        end
    end

    if #parts == 0 then
        return input_context, agent_id_override, model_override, "", nil
    end

    return input_context, agent_id_override, model_override, table.concat(parts, "\n\n"), nil
end

local function validate_and_resolve_config(config)
    if not config then
        return nil, agent_consts.ERROR_MSG.INVALID_CONFIG
    end

    if not config.arena then
        return nil, "Arena configuration is required"
    end

    local tool_calling = config.arena.tool_calling or agent_consts.DEFAULTS.TOOL_CALLING
    local has_exit_schema = config.arena.exit_schema ~= nil

    if tool_calling == agent_consts.TOOL_CALLING.AUTO and has_exit_schema then
        config.arena.tool_calling = tool_calling
    end

    if tool_calling == agent_consts.TOOL_CALLING.ANY and not has_exit_schema then
        return nil, "any mode requires exit_schema to be defined"
    end

    if tool_calling == agent_consts.TOOL_CALLING.NONE and has_exit_schema then
        return nil, "none mode cannot have exit_schema"
    end

    return config, nil
end

local function setup_exit_tool(agent_ctx, arena_config)
    local exit_tool_name = nil
    local should_add_exit_tool = (arena_config.tool_calling == agent_consts.TOOL_CALLING.ANY) or
        (arena_config.tool_calling == agent_consts.TOOL_CALLING.AUTO and arena_config.exit_schema)

    if should_add_exit_tool then
        exit_tool_name = "finish"

        local exit_schema = arena_config.exit_schema or {
            type = "object",
            properties = {
                answer = {
                    type = "string",
                    description = "Your final answer to complete the task"
                }
            },
            required = { "answer" }
        }

        agent_ctx:add_tools({
            {
                id = exit_tool_name,
                name = exit_tool_name,
                description = "Call this tool when you have completed the task and want to provide your final answer",
                schema = exit_schema
            }
        })
    end

    if arena_config.tools and #arena_config.tools > 0 then
        agent_ctx:add_tools(arena_config.tools)
    end

    return exit_tool_name
end

local function accumulate_tokens(total_tokens, new_tokens)
    if not new_tokens then
        return total_tokens
    end

    total_tokens.total_tokens = (total_tokens.total_tokens or 0) + (new_tokens.total_tokens or 0)
    total_tokens.prompt_tokens = (total_tokens.prompt_tokens or 0) + (new_tokens.prompt_tokens or 0)
    total_tokens.completion_tokens = (total_tokens.completion_tokens or 0) + (new_tokens.completion_tokens or 0)
    total_tokens.cache_read_tokens = (total_tokens.cache_read_tokens or 0) + (new_tokens.cache_read_tokens or 0)
    total_tokens.cache_write_tokens = (total_tokens.cache_write_tokens or 0) + (new_tokens.cache_write_tokens or 0)
    total_tokens.thinking_tokens = (total_tokens.thinking_tokens or 0) + (new_tokens.thinking_tokens or 0)

    return total_tokens
end

local function update_node_progress(n, iteration, max_iterations, total_tokens, tool_calls_count, status_message,
                                    agent_id, model_name)
    local state_info = {
        current_iteration = iteration,
        max_iterations = max_iterations,
        agent_id = agent_id,
        model = model_name,
        total_tokens = total_tokens,
        tool_calls = tool_calls_count
    }

    n:update_metadata({
        status_message = status_message,
        state = state_info
    })
end

local function store_agent_action(n, agent_result, iteration, agent_id, model_name, exit_tool_name, control_metadata)
    local action_content = {
        result = agent_result.result,
        tool_calls = agent_result.tool_calls,
        delegate_calls = agent_result.delegate_calls
    }

    local is_exit_action = false
    if exit_tool_name and agent_result.tool_calls then
        for _, tool_call in ipairs(agent_result.tool_calls) do
            if tool_call.name == exit_tool_name then
                is_exit_action = true
                break
            end
        end
    end

    local action_key = is_exit_action and (iteration .. "_final") or (iteration .. "_action")

    local metadata = {
        iteration = iteration,
        agent_id = agent_id,
        model = model_name,
        tokens = agent_result.tokens,
        finish_reason = agent_result.finish_reason,
        llm_meta = agent_result.metadata or {},
    }

    if control_metadata and next(control_metadata) then
        metadata._control = control_metadata
    end

    n:data(agent_consts.DATA_TYPE.AGENT_ACTION, action_content, {
        key = action_key,
        content_type = consts.CONTENT_TYPE.JSON,
        node_id = n.node_id,
        metadata = metadata
    })
end

local function store_memory_recall(n, agent_result, iteration)
    if not agent_result.memory_prompt then
        return
    end

    n:data(agent_consts.DATA_TYPE.AGENT_MEMORY, agent_result.memory_prompt.content, {
        key = iteration .. "_memory",
        content_type = consts.CONTENT_TYPE.TEXT,
        node_id = n.node_id,
        metadata = {
            iteration = iteration,
            memory_ids = agent_result.memory_prompt.metadata and agent_result.memory_prompt.metadata.memory_ids,
            llm_meta = agent_result.memory_prompt.metadata or {}
        }
    })
end

local function extract_compaction_scenario_id(history_rows)
    for i = #history_rows, 1, -1 do
        local row = history_rows[i]
        local metadata = row.metadata or {}
        if type(metadata.scenario_id) == "string" and metadata.scenario_id ~= "" then
            return metadata.scenario_id
        end

        local content = decode_json_content(row.content, row.content_type)
        if type(content) == "table" then
            if type(content.scenario_id) == "string" and content.scenario_id ~= "" then
                return content.scenario_id
            end

            for _, tool_call in ipairs(content.tool_calls or {}) do
                local arguments = tool_call.arguments or {}
                if type(arguments.scenario_id) == "string" and arguments.scenario_id ~= "" then
                    return arguments.scenario_id
                end
            end
        end
    end

    return nil
end

local function stringify_history_content(row)
    local content = decode_json_content(row.content, row.content_type)
    if type(content) == "string" then
        return content
    end
    if type(content) == "table" then
        local encoded, encode_err = json.encode(content)
        if not encode_err then
            return encoded
        end
    end
    if content == nil then
        return ""
    end
    return tostring(content)
end

-- Conversation compaction runs at the start of the next turn, after the prior
-- turn's observations have already been yielded and persisted. The marker is
-- queued locally, exposed to prompt_builder immediately via an in-memory
-- overlay, and then flushed atomically with the next turn's submit/yield.
-- Clamp a string to at most max_chars, appending a truncation suffix when
-- the original is longer. Returns the input unchanged if max_chars <= 0 or
-- the string already fits.
local function clamp_memory_text(text, max_chars)
    if type(text) ~= "string" or max_chars == nil or max_chars <= 0 then
        return text
    end
    local cap = math.floor(max_chars)
    if #text <= cap then
        return text
    end
    local suffix = " ... [truncated]"
    if cap <= #suffix then
        return text:sub(1, cap)
    end
    return text:sub(1, cap - #suffix) .. suffix
end

-- Record a compaction-skipped observation row so non-strict mode leaves an
-- audit trail but doesn't kill the turn.
local function record_compaction_skip(n, iteration, reason)
    n:data(agent_consts.DATA_TYPE.AGENT_OBSERVATION, tostring(reason or "compaction skipped"), {
        key = tostring(iteration or 0) .. "_compaction_skipped",
        content_type = consts.CONTENT_TYPE.TEXT,
        node_id = n.node_id,
        metadata = {
            iteration = iteration,
            compaction_skipped = true,
            compaction_error = tostring(reason or "")
        }
    })
end

local function maybe_compact_history(n, config, session_context)
    local compact_cfg = config and config.compact
    if type(compact_cfg) ~= "table" then
        return nil, nil
    end

    local threshold = tonumber(compact_cfg.token_threshold)
    local func_id = compact_cfg.function_id
    if not threshold or threshold <= 0 or type(func_id) ~= "string" or func_id == "" then
        return nil, nil
    end

    local history_rows = (n:query() :: any)
        :with_nodes(n.node_id)
        :with_data_types(
            agent_consts.DATA_TYPE.AGENT_ACTION,
            agent_consts.DATA_TYPE.AGENT_OBSERVATION,
            agent_consts.DATA_TYPE.AGENT_MEMORY,
            agent_consts.DATA_TYPE.AGENT_DELEGATION
        )
        :all() or {}

    if #history_rows == 0 then
        return nil, nil
    end

    sort_compaction_rows(history_rows)

    local latest_action = nil
    for i = #history_rows, 1, -1 do
        local row = history_rows[i]
        if row.type == agent_consts.DATA_TYPE.AGENT_ACTION then
            latest_action = row
            break
        end
    end

    if not latest_action then
        return nil, nil
    end

    local action_metadata = latest_action.metadata or {}
    local turn_tokens = action_metadata.tokens or {}
    local prompt_tokens = tonumber(turn_tokens.prompt_tokens) or 0
    if prompt_tokens <= threshold then
        return nil, nil
    end

    local cut_before = last_compaction_data_id(history_rows)
    if cut_before == "" then
        return nil, nil
    end

    local latest_marker = find_latest_compaction_marker(history_rows)
    if latest_marker
       and tostring((latest_marker.metadata or {}).compacted_before_data_id or "") == cut_before then
        return nil, nil
    end

    local compacted_rows = compactable_history_rows(history_rows)
    if #compacted_rows == 0 then
        return nil, nil
    end

    local retained_result_count = 0
    for _, row in ipairs(history_rows) do
        if is_structured_result_row(row) then
            retained_result_count = retained_result_count + 1
        end
    end

    -- Build a serialization-safe projection of history for the summarizer.
    -- The function only receives rows that will actually be compacted; structured
    -- call results remain in the prompt to preserve tool/result continuity.
    local history_payload = {}
    for i, row in ipairs(compacted_rows) do
        history_payload[i] = {
            data_id = tostring(row.data_id or ""),
            type = tostring(row.type or ""),
            content = stringify_history_content(row),
            content_type = tostring(row.content_type or ""),
            metadata = row.metadata
        }
    end

    local compaction_iteration = tonumber(action_metadata.iteration) or 0
    local scenario_id = extract_compaction_scenario_id(history_rows)

    local summary_result, call_err = funcs.new()
        :with_context(session_context or {})
        :call(func_id, {
            dataflow_id = n.dataflow_id,
            node_id = n.node_id,
            scenario_id = scenario_id,
            iteration = compaction_iteration,
            prompt_tokens = prompt_tokens,
            history_count = #history_payload,
            retained_result_count = retained_result_count,
            compacted_before_data_id = cut_before,
            history = history_payload
        })

    if call_err then
        return nil, "compaction function " .. func_id .. " failed: " .. tostring(call_err)
    end

    local memory_text = nil
    if type(summary_result) == "table" then
        memory_text = summary_result.memory or summary_result.summary
    elseif type(summary_result) == "string" then
        memory_text = summary_result
    end

    if type(memory_text) ~= "string" or memory_text == "" then
        return nil, "compaction function " .. func_id .. " returned empty memory"
    end

    -- Hard cap on stored memory to prevent hostile/buggy summarizers from
    -- bloating every future prompt. Default 8192 chars; configurable.
    local max_memory_chars = tonumber(compact_cfg.max_memory_chars) or 8192
    local original_memory_len = #memory_text
    memory_text = clamp_memory_text(memory_text, max_memory_chars)
    local memory_truncated = #memory_text < original_memory_len

    local marker_metadata = {
        iteration = compaction_iteration,
        compaction_marker = true,
        compacted_at_prompt_tokens = prompt_tokens,
        compacted_before_data_id = cut_before,
        compacted_history_count = #history_payload,
        compacted_first_data_id = tostring((compacted_rows[1] or {}).data_id or ""),
        compacted_last_data_id = tostring((compacted_rows[#compacted_rows] or {}).data_id or ""),
        compacted_retained_result_count = retained_result_count,
        compacted_source_action_data_id = tostring(latest_action.data_id or ""),
        compaction_function_id = func_id,
        compaction_memory_truncated = memory_truncated
    }
    local marker_data_id = uuid.v7()
    local marker_key_prefix = compaction_iteration > 0 and tostring(compaction_iteration) or cut_before

    n:data(agent_consts.DATA_TYPE.AGENT_MEMORY, memory_text, {
        data_id = marker_data_id,
        key = marker_key_prefix .. "_compaction",
        content_type = consts.CONTENT_TYPE.TEXT,
        node_id = n.node_id,
        metadata = marker_metadata
    })

    return {
        data_id = marker_data_id,
        type = agent_consts.DATA_TYPE.AGENT_MEMORY,
        content = memory_text,
        content_type = consts.CONTENT_TYPE.TEXT,
        metadata = marker_metadata
    }, nil
end

local function load_latest_agent_action(n)
    local actions = n:query()
        :with_nodes(n.node_id)
        :with_data_types(agent_consts.DATA_TYPE.AGENT_ACTION)
        :order_by("created_at", "DESC")
        :all()

    if actions and #actions > 0 then
        return actions[1]
    end

    return nil
end

local function load_observed_tool_call_ids(n)
    local observed_ids = {}
    local observations = n:query()
        :with_nodes(n.node_id)
        :with_data_types(agent_consts.DATA_TYPE.AGENT_OBSERVATION)
        :all()

    for _, observation in ipairs(observations or {}) do
        local metadata = observation.metadata or {}
        if metadata.tool_call_id then
            observed_ids[metadata.tool_call_id] = true
        end
    end

    return observed_ids
end

local function load_action_payload(action_row)
    if not action_row then
        return nil
    end

    local content = decode_json_content(action_row.content, action_row.content_type)
    if type(content) ~= "table" then
        content = {
            result = content
        }
    end

    return {
        content = content,
        metadata = action_row.metadata or {},
        row = action_row
    }
end

local function collect_unresolved_calls(action_payload, observed_tool_call_ids)
    local unresolved_tool_calls = {}
    local unresolved_delegate_calls = {}

    for _, tool_call in ipairs(action_payload.content.tool_calls or {}) do
        if tool_call.id and not observed_tool_call_ids[tool_call.id] then
            table.insert(unresolved_tool_calls, tool_call)
        end
    end

    for _, delegate_call in ipairs(action_payload.content.delegate_calls or {}) do
        if delegate_call.id and not observed_tool_call_ids[delegate_call.id] then
            table.insert(unresolved_delegate_calls, delegate_call)
        end
    end

    return unresolved_tool_calls, unresolved_delegate_calls
end

local function queue_iteration_warning(n, iteration, max_iterations)
    local remaining_iterations = (max_iterations :: number) - iteration
    if remaining_iterations == 2 then
        local warning_msg = string.format(agent_consts.FEEDBACK.ITERATIONS_WARNING, remaining_iterations)
        n:data(agent_consts.DATA_TYPE.AGENT_OBSERVATION, warning_msg, {
            key = iteration .. "_iterations_warning",
            content_type = consts.CONTENT_TYPE.TEXT,
            node_id = n.node_id,
            metadata = {
                iteration = iteration,
                remaining_iterations = remaining_iterations
            }
        })
    elseif remaining_iterations == 1 then
        local warning_msg = agent_consts.FEEDBACK.FINAL_ITERATION
        n:data(agent_consts.DATA_TYPE.AGENT_OBSERVATION, warning_msg, {
            key = iteration .. "_final_warning",
            content_type = consts.CONTENT_TYPE.TEXT,
            node_id = n.node_id,
            metadata = {
                iteration = iteration,
                remaining_iterations = remaining_iterations
            }
        })
    elseif remaining_iterations == 0 then
        local warning_msg = agent_consts.FEEDBACK.CRITICAL_FINAL
        n:data(agent_consts.DATA_TYPE.AGENT_OBSERVATION, warning_msg, {
            key = iteration .. "_critical_warning",
            content_type = consts.CONTENT_TYPE.TEXT,
            node_id = n.node_id,
            metadata = {
                iteration = iteration,
                remaining_iterations = remaining_iterations
            }
        })
    end
end

local function append_control_delegations(delegate_calls: any, control_delegations)
    if not control_delegations or #control_delegations == 0 then
        return
    end

    for _, control_del in ipairs(control_delegations) do
        local delegation = control_del.delegation
        local tool_call = control_del.tool_call

        local delegate_call = {
            agent_id = delegation.agent_id,
            arguments = delegation.input_data,
            context = delegation.context,
            name = "delegate_" .. delegation.agent_id,
            id = tool_call.id,
            system_prompt = delegation.system_prompt,
            max_iterations = delegation.max_iterations,
            tool_calling = delegation.tool_calling,
            traits = delegation.traits,
            tools = delegation.tools,
            exit_schema = delegation.exit_schema
        }
        table.insert(delegate_calls, delegate_call)
    end
end

local function run_control_response_commands(control_responses, agent_ctx, n, iteration)
    local _changes_summary, changes_err = control_handler.apply_control_responses(control_responses, agent_ctx, n)
    if changes_err then
        return changes_err
    end

    local created_node_ids = {}
    for _, response in ipairs(control_responses) do
        if response.changes_applied and response.changes_applied.commands and response.changes_applied.created_nodes then
            for _, node_id in ipairs(response.changes_applied.created_nodes) do
                table.insert(created_node_ids, node_id)
            end
        end
    end

    if #created_node_ids == 0 then
        return nil
    end

    local yield_result, yield_err = n:yield({ run_nodes = created_node_ids })
    if yield_result then
        local reader = n:query()
            :with_nodes(created_node_ids)
            :with_data_types(consts.DATA_TYPE.NODE_OUTPUT)
            :fetch_options({ replace_references = true })

        local output_data = reader:all()

        if output_data and #output_data > 0 then
            local output_content = output_data[1].content
            if #output_data > 1 then
                local all_outputs = {}
                for _, output in ipairs(output_data) do
                    table.insert(all_outputs, output.content)
                end
                output_content = all_outputs
            end

            n:data(agent_consts.DATA_TYPE.AGENT_OBSERVATION, output_content, {
                key = iteration .. "_commands_output",
                content_type = type(output_content) == "table" and consts.CONTENT_TYPE.JSON or
                    consts.CONTENT_TYPE.TEXT,
                node_id = n.node_id,
                metadata = {
                    iteration = iteration,
                    created_nodes = created_node_ids
                }
            })
        end
    elseif yield_err then
        n:data(agent_consts.DATA_TYPE.AGENT_OBSERVATION, "Command execution failed: " .. yield_err, {
            key = iteration .. "_commands_error",
            content_type = consts.CONTENT_TYPE.TEXT,
            node_id = n.node_id,
            metadata = {
                iteration = iteration,
                is_error = true
            }
        })
    end

    return nil
end

local function get_tool_title_by_registry_id(registry_id, tool_name)
    if not registry_id then
        return tool_name
    end

    local tool_schema = tools.get_tool_schema(registry_id)
    if tool_schema and tool_schema.title then
        return tool_schema.title
    end

    return tool_name
end

local function create_tool_viz_nodes(n, tool_calls: { ToolCall }?, iteration, show_tool_calls, exit_tool_name)
    local tool_call_to_node_id = {}

    if show_tool_calls == false or not tool_calls or #tool_calls == 0 then
        return tool_call_to_node_id
    end

    for _, tool_call in ipairs(tool_calls) do
        if exit_tool_name and tool_call.name == exit_tool_name then
            goto continue
        end

        local viz_node_id = uuid.v7()
        tool_call_to_node_id[tool_call.id] = viz_node_id

        local input_size = 0
        if tool_call.arguments then
            local args_json = json.encode(tool_call.arguments)
            input_size = string.len(args_json)
        end

        local tool_title = get_tool_title_by_registry_id(tool_call.registry_id, tool_call.name)

        local metadata = {
            tool_name = tool_call.name,
            tool_call_id = tool_call.id,
            iteration = iteration,
            title = tool_title,
            input_size_bytes = input_size
        }

        if tool_call.registry_id then
            metadata.registry_id = tool_call.registry_id
        end

        n:command({
            type = consts.COMMAND_TYPES.CREATE_NODE,
            payload = {
                node_id = viz_node_id,
                node_type = "tool.call",
                parent_node_id = n.node_id,
                status = consts.STATUS.RUNNING,
                config = {},
                metadata = metadata
            }
        })

        ::continue::
    end

    return tool_call_to_node_id
end

local function update_tool_viz_nodes(n, tool_results, tool_call_to_node_id)
    if not tool_results or not tool_call_to_node_id then
        return
    end

    for call_id, result_data in pairs(tool_results) do
        local viz_node_id = tool_call_to_node_id[call_id]
        if viz_node_id then
            local tool_result = result_data.result
            local tool_error = result_data.error

            local output_size = 0
            local output_content = tool_result or tool_error
            if output_content then
                local output_json = type(output_content) == "table" and json.encode(output_content) or
                    tostring(output_content)
                output_size = string.len(output_json)
            end

            local final_status = tool_error and consts.STATUS.COMPLETED_FAILURE or consts.STATUS.COMPLETED_SUCCESS

            n:command({
                type = consts.COMMAND_TYPES.UPDATE_NODE,
                payload = {
                    node_id = viz_node_id,
                    status = final_status,
                    metadata = {
                        has_error = tool_error ~= nil,
                        error_message = tool_error,
                        output_size_bytes = output_size
                    }
                }
            })
        end
    end
end

local function execute_tools(agent_result: { tool_calls: { ToolCall }? }, caller, session_context)
    if not agent_result.tool_calls or #agent_result.tool_calls == 0 then
        return {}
    end

    local validated_tools, validate_err = caller:validate(agent_result.tool_calls)
    if validate_err then
        return {}
    end

    local tool_results = caller:execute(session_context or {}, validated_tools)
    return tool_results or {}
end

local function process_tool_results(n, tool_results, iteration, exit_tool_name, agent_result: any, arena_config,
                                    session_context)
    local control_responses = {}
    local control_delegations = {}
    local task_complete = false
    local final_result = nil
    local skip_call = false

    if exit_tool_name and agent_result.tool_calls then
        for _, original_tool_call in ipairs(agent_result.tool_calls) do
            if original_tool_call.name == exit_tool_name then
                local exit_arguments = original_tool_call.arguments

                if arena_config.exit_func_id then
                    local validated_result, validation_err = funcs.new()
                        :with_context(session_context)
                        :call(arena_config.exit_func_id :: string, exit_arguments)

                    if validation_err then
                        n:data(agent_consts.DATA_TYPE.AGENT_OBSERVATION, validation_err, {
                            key = iteration .. "_exit_validation_failed",
                            content_type = consts.CONTENT_TYPE.TEXT,
                            node_id = n.node_id,
                            metadata = {
                                iteration = iteration,
                                is_error = true,
                                tool_call_id = original_tool_call.id,
                                tool_name = original_tool_call.name,
                                exit_validation = true
                            }
                        })
                        task_complete = false
                        skip_call = true
                    else
                        task_complete = true
                        final_result = validated_result
                    end
                else
                    task_complete = true
                    final_result = exit_arguments or { success = false, error = "Exit tool called without arguments" }
                end
                break
            end
        end
    end

    if task_complete or skip_call then
        return control_responses, control_delegations, task_complete, final_result
    end

    if agent_result.tool_calls then
        for _, tool_call in ipairs(agent_result.tool_calls) do
            local call_id = tool_call.id
            local result_data = tool_results[call_id]

            if result_data then
                local tool_result = result_data.result
                local tool_error = result_data.error

                local cleaned_result, control_response = control_handler.process_control_directive(
                    tool_result, n, iteration
                )
                if control_response then
                    table.insert(control_responses, control_response)

                    if control_response.delegate then
                        for _, delegation in ipairs(control_response.delegate) do
                            table.insert(control_delegations, {
                                delegation = delegation,
                                tool_call = tool_call,
                                control_response = control_response
                            })
                        end
                    end
                end

                if not (control_response and control_response.delegate) then
                    local obs_content = cleaned_result or tool_error
                    if obs_content == nil then
                        obs_content = "nil"
                    end

                    local tool_key = iteration .. "_" .. tool_call.name

                    n:data(agent_consts.DATA_TYPE.AGENT_OBSERVATION, obs_content, {
                        key = tool_key,
                        content_type = type(obs_content) == "table" and consts.CONTENT_TYPE.JSON or
                            consts.CONTENT_TYPE.TEXT,
                        node_id = n.node_id,
                        metadata = {
                            iteration = iteration,
                            tool_call_id = call_id,
                            tool_name = tool_call.name,
                            is_error = tool_error ~= nil
                        }
                    })
                end
            end
        end
    end

    return control_responses, control_delegations, task_complete, final_result
end

local function tools_were_attempted(agent_result)
    if agent_result.tool_calls and #agent_result.tool_calls > 0 then
        return true
    end

    if agent_result.delegate_calls and #agent_result.delegate_calls > 0 then
        return true
    end

    return false
end

local function check_completion(tool_calling, agent_result: any, iteration, min_iterations, exit_tool_name, n)
    local task_complete = false
    local final_result = nil

    if iteration < min_iterations then
        return task_complete, final_result
    end

    if tool_calling == agent_consts.TOOL_CALLING.NONE then
        if agent_result.result and agent_result.result ~= "" then
            task_complete = true
            final_result = agent_result.result
        end
    elseif tool_calling == agent_consts.TOOL_CALLING.AUTO then
        if not tools_were_attempted(agent_result) then
            if agent_result.result and agent_result.result ~= nil then
                task_complete = true
                final_result = agent_result.result
            else
                local feedback = agent_consts.FEEDBACK.NO_TOOLS_CALLED
                n:data(agent_consts.DATA_TYPE.AGENT_OBSERVATION, feedback, {
                    key = iteration .. "_no_tools_called",
                    content_type = consts.CONTENT_TYPE.TEXT,
                    node_id = n.node_id,
                    metadata = {
                        iteration = iteration
                    }
                })
            end
        end
    elseif tool_calling == agent_consts.TOOL_CALLING.ANY then
        if not tools_were_attempted(agent_result) then
            local feedback = agent_consts.FEEDBACK.NO_TOOLS_CALLED
            if exit_tool_name then
                feedback = feedback .. " " .. string.format(agent_consts.FEEDBACK.EXIT_AVAILABLE, exit_tool_name)
            end
            n:data(agent_consts.DATA_TYPE.AGENT_OBSERVATION, feedback, {
                key = iteration .. "_no_tools_called",
                content_type = consts.CONTENT_TYPE.TEXT,
                node_id = n.node_id,
                metadata = {
                    iteration = iteration
                }
            })
        end
    end

    return task_complete, final_result
end

local function finalize_iteration(n, agent_ctx, session_context, iteration, max_iterations, min_iterations, tool_calling,
                                  exit_tool_name, agent_result: any, delegate_calls: any, tool_results, arena_config)
    local control_responses, control_delegations, task_complete, final_result = process_tool_results(
        n,
        tool_results,
        iteration,
        exit_tool_name,
        agent_result,
        arena_config,
        session_context
    )

    append_control_delegations(delegate_calls, control_delegations)
    queue_iteration_warning(n, iteration, max_iterations)
    n:yield()

    local has_delegations = #delegate_calls > 0

    if has_delegations then
        if #control_responses > 0 then
            local _changes_summary, changes_err = control_handler.apply_control_responses(control_responses, agent_ctx, n)
            if changes_err then
                return nil, nil, changes_err
            end
        end

        local delegation_infos = delegation_handler.create_delegation_batch(
            { delegate_calls = delegate_calls },
            n,
            session_context
        )

        local delegation_results, delegation_err = delegation_handler.execute_delegation_batch(delegation_infos, n)
        if delegation_err then
            return nil, nil, delegation_err
        end

        delegation_handler.map_delegation_results_to_conversation(delegation_results, n, iteration)
        n:yield()
    elseif #control_responses > 0 then
        local changes_err = run_control_response_commands(control_responses, agent_ctx, n, iteration)
        if changes_err then
            return nil, nil, changes_err
        end
    end

    if not task_complete and not has_delegations then
        task_complete, final_result = check_completion(tool_calling, agent_result, iteration, min_iterations,
            exit_tool_name, n)
    end

    return task_complete, final_result, nil
end

local function recover_persisted_action(n, agent_ctx, caller, session_context, config, iteration, max_iterations,
                                        min_iterations, tool_calling, exit_tool_name, show_tool_calls)
    local latest_action_row = load_latest_agent_action(n)
    if not latest_action_row then
        return false, nil, nil, nil
    end

    local action_payload = load_action_payload(latest_action_row)
    if not action_payload then
        return false, nil, nil, nil
    end

    local action_iteration = (action_payload.metadata and action_payload.metadata.iteration) or iteration or 0
    local finish_reason = action_payload.metadata and action_payload.metadata.finish_reason
    if finish_reason == "length" then
        return false, nil, action_iteration, nil
    end

    local observed_tool_call_ids = load_observed_tool_call_ids(n)
    local unresolved_tool_calls, unresolved_delegate_calls = collect_unresolved_calls(action_payload, observed_tool_call_ids)

    local has_unresolved_work = (#unresolved_tool_calls > 0) or (#unresolved_delegate_calls > 0)
    if not has_unresolved_work then
        local action_has_calls = #(action_payload.content.tool_calls or {}) > 0 or #(action_payload.content.delegate_calls or {}) > 0
        if not action_has_calls and action_payload.content.result ~= nil and action_payload.content.result ~= "" then
            return true, action_payload.content.result, action_iteration, nil
        end
        return false, nil, action_iteration, nil
    end

    local executable_tool_calls = {} :: { ToolCall }
    for _, tool_call in ipairs(unresolved_tool_calls) do
        if not exit_tool_name or tool_call.name ~= exit_tool_name then
            table.insert(executable_tool_calls, tool_call)
        end
    end

    local tool_call_to_node_id = create_tool_viz_nodes(n, executable_tool_calls, action_iteration, show_tool_calls,
        exit_tool_name)
    local tool_results = execute_tools({ tool_calls = executable_tool_calls }, caller, session_context)

    if show_tool_calls then
        update_tool_viz_nodes(n, tool_results, tool_call_to_node_id)
    end

    local recovered_agent_result = {
        result = action_payload.content.result,
        tool_calls = unresolved_tool_calls,
        delegate_calls = unresolved_delegate_calls
    }

    local delegate_calls = {} :: { any }
    for _, delegate_call in ipairs(unresolved_delegate_calls) do
        table.insert(delegate_calls, delegate_call)
    end

    local task_complete, final_result, finalize_err = finalize_iteration(
        n,
        agent_ctx,
        session_context,
        action_iteration,
        max_iterations,
        min_iterations,
        tool_calling,
        exit_tool_name,
        recovered_agent_result,
        delegate_calls,
        tool_results,
        config.arena
    )

    if finalize_err then
        return false, nil, action_iteration, finalize_err
    end

    return task_complete or false, final_result, action_iteration, nil
end

local function get_delegation_data_id(n)
    local reader = n:query()
        :with_nodes(n.node_id)
        :with_data_types(agent_consts.DATA_TYPE.AGENT_DELEGATION)

    local delegation_data = reader:all()
    if delegation_data and #delegation_data > 0 then
        return delegation_data[#delegation_data].data_id
    end

    return nil
end

local function safe_inputs(n)
    local ok, inputs_or_err, inputs_err = pcall(function()
        return n:inputs()
    end)

    if not ok then
        return nil, tostring(inputs_or_err)
    end

    if inputs_err then
        return nil, tostring(inputs_err)
    end

    return inputs_or_err, nil
end

local function run(args)
    local n, err = node_sdk.new(args)
    if err then
        error(err)
    end

    local config = n:config()
    local validated_config, config_err = validate_and_resolve_config(config)
    if config_err then
        return n:fail({
            code = agent_consts.ERROR.INVALID_CONFIG,
            message = config_err
        }, config_err)
    end

    local inputs, inputs_err = safe_inputs(n)
    if inputs_err then
        return n:fail({
            code = agent_consts.ERROR.INPUT_VALIDATION_FAILED,
            message = inputs_err
        }, inputs_err)
    end

    local input_context, agent_id_override, model_override, input_data, input_err = process_multiple_inputs(inputs)
    if input_err then
        return n:fail({
            code = agent_consts.ERROR.INPUT_VALIDATION_FAILED,
            message = input_err
        }, input_err)
    end

    if agent_id_override then
        n:update_config({ agent = agent_id_override })

        local entry = registry.get(agent_id_override)
        if entry and entry.meta and (entry.meta.title or entry.meta.name) then
            local title = entry.meta.title or entry.meta.name
            n:update_metadata({ title = title })
        else
            n:update_metadata({ title = "Agent: " .. agent_id_override })
        end
    end

    model_override = model_override or config.model or config.arena.model

    local arena_context = config.arena.context or {}
    local session_context = {
        dataflow_id = n.dataflow_id,
        node_id = n.node_id,
    }
    for k, v in pairs(arena_context) do
        session_context[k] = v
    end

    local base_context = {
        enable_cache = false,
        delegate_tools = {
            enabled = agent_consts.DELEGATE_DEFAULTS.GENERATE_TOOL_SCHEMAS,
            description_suffix = agent_consts.DELEGATE_DEFAULTS.DESCRIPTION_SUFFIX,
            default_schema = agent_consts.DELEGATE_DEFAULTS.SCHEMA
        }
    }

    -- agent_context.new reads `config.context` as its compile-time base_context.
    -- Traits' build_funcs use ctx.get(), so dataflow/node ids, arena context, and
    -- per-input context must live under `.context`; operational knobs stay flat.
    local agent_ctx_config = build_agent_context_config(base_context, session_context, input_context)
    local agent_ctx = agent_context.new({
        enable_cache = agent_ctx_config.enable_cache,
        context = agent_ctx_config.context,
    })
    if agent_ctx_config.delegate_tools then
        agent_ctx:configure_delegate_tools({
            enabled = agent_ctx_config.delegate_tools.enabled,
            description_suffix = agent_ctx_config.delegate_tools.description_suffix,
            default_schema = agent_ctx_config.delegate_tools.default_schema,
        })
    end
    if agent_ctx_config.memory_contract then
        agent_ctx:set_memory_contract(agent_ctx_config.memory_contract)
    end
    if agent_ctx_config.context_merger then
        agent_ctx:set_context_merger(agent_ctx_config.context_merger)
    end

    local exit_tool_name = setup_exit_tool(agent_ctx, config.arena)

    local agent_to_load = agent_id_override or config.agent

    if not agent_to_load or agent_to_load == "" then
        return n:fail({
            code = agent_consts.ERROR.AGENT_LOAD_FAILED,
            message = "Agent ID not specified in config or inputs"
        }, "Agent ID not specified in config or inputs")
    end

    local load_options = model_override and { model = model_override } or nil
    local agent_instance, agent_err = agent_ctx:load_agent(agent_to_load, load_options)
    if not agent_instance then
        return n:fail({
            code = agent_consts.ERROR.AGENT_LOAD_FAILED,
            message = string.format(agent_consts.ERROR_MSG.AGENT_LOAD_FAILED, agent_err or "unknown error")
        }, string.format(agent_consts.ERROR_MSG.AGENT_LOAD_FAILED, agent_err or "unknown error"))
    end

    local agent_config = agent_ctx:get_config()
    local agent_id = agent_config.current_agent_id or
        (type(agent_to_load) == "table" and agent_to_load.id or agent_to_load)
    local model_name = agent_config.current_model or "unknown"

    local builder, builder_err = prompt_builder.new(n.dataflow_id, n.node_id, n.path)
    if builder_err then
        return n:fail({
            code = agent_consts.ERROR.PROMPT_BUILD_FAILED,
            message = builder_err
        }, builder_err)
    end
    if not builder then
        return n:fail({
            code = agent_consts.ERROR.PROMPT_BUILD_FAILED,
            message = "Failed to initialize prompt builder"
        }, "Failed to initialize prompt builder")
    end

    builder:with_arena_config(config.arena):with_initial_input(input_data)
    local caller = tool_caller.new()

    local saved_state = ((args.node or {}).metadata or {}).state or {}
    local iteration = saved_state.current_iteration or 0
    local max_iterations = config.arena.max_iterations or agent_consts.DEFAULTS.MAX_ITERATIONS
    local min_iterations = config.arena.min_iterations or agent_consts.DEFAULTS.MIN_ITERATIONS
    local tool_calling = config.arena.tool_calling
    local show_tool_calls = config.show_tool_calls ~= false
    local task_complete = false
    local final_result = nil

    local total_tokens = new_total_tokens(saved_state.total_tokens)
    local tool_calls_count = saved_state.tool_calls or 0
    local pending_compaction_history = {}

    local initial_status = build_status_message(iteration, max_iterations, total_tokens, tool_calls_count, false, false)
    update_node_progress(n, iteration, max_iterations, total_tokens, tool_calls_count, initial_status, agent_id,
        model_name)

    local recovered_complete, recovered_result, recovered_iteration, recovery_err = recover_persisted_action(
        n,
        agent_ctx,
        caller,
        session_context,
        config,
        iteration,
        max_iterations,
        min_iterations,
        tool_calling,
        exit_tool_name,
        show_tool_calls
    )
    if recovery_err then
        return n:fail({
            code = agent_consts.ERROR.AGENT_EXEC_FAILED,
            message = recovery_err
        }, recovery_err)
    end
    if recovered_iteration and recovered_iteration > iteration then
        iteration = recovered_iteration
    end
    if recovered_complete then
        task_complete = true
        final_result = recovered_result
    end

    while iteration < max_iterations and not task_complete do
        iteration = iteration + 1

        local pending_compaction_marker, compact_err = maybe_compact_history(n, config, session_context)
        if compact_err then
            local strict_mode = config.compact and config.compact.strict == true
            if strict_mode then
                return n:fail({
                    code = agent_consts.ERROR.COMPACTION_FAILED,
                    message = compact_err
                }, compact_err)
            end
            -- warning mode: log audit observation and continue the turn
            record_compaction_skip(n, iteration, compact_err)
            pending_compaction_history = {}
        elseif pending_compaction_marker then
            pending_compaction_history = { pending_compaction_marker }
        else
            pending_compaction_history = {}
        end
        builder:with_pending_history(pending_compaction_history)

        local prompt, prompt_err = builder:build_prompt(config.arena.prompt)
        if prompt_err then
            return n:fail({
                code = agent_consts.ERROR.PROMPT_BUILD_FAILED,
                message = prompt_err
            }, prompt_err)
        end

        local step_options = { tool_call = tool_calling, context = session_context }
        local agent_result, step_err = agent_instance:step(prompt, step_options)
        if step_err then
            return n:fail({
                code = agent_consts.ERROR.AGENT_EXEC_FAILED,
                message = step_err
            }, step_err)
        end

        if agent_result.truncated then
            total_tokens = accumulate_tokens(total_tokens, agent_result.tokens)

            local status_msg = build_status_message(iteration, max_iterations, total_tokens, tool_calls_count, false, false)
            update_node_progress(n, iteration, max_iterations, total_tokens, tool_calls_count, status_msg, agent_id, model_name)

            store_agent_action(n, agent_result, iteration, agent_id, model_name, exit_tool_name, {})

            n:data(agent_consts.DATA_TYPE.AGENT_OBSERVATION, agent_consts.FEEDBACK.OUTPUT_TRUNCATED, {
                key = iteration .. "_truncation_warning",
                content_type = consts.CONTENT_TYPE.TEXT,
                node_id = n.node_id,
                metadata = {
                    iteration = iteration,
                    truncated = true
                }
            })

            local _yield_result, yield_err = n:yield()
            if yield_err then
                return n:fail({
                    code = agent_consts.ERROR.AGENT_EXEC_FAILED,
                    message = "Failed to persist truncated agent turn: " .. tostring(yield_err)
                }, "Failed to persist truncated agent turn: " .. tostring(yield_err))
            end
            pending_compaction_history = {}
            builder:with_pending_history(pending_compaction_history)
            goto continue_loop
        end

        local regular_tool_calls = (agent_result.tool_calls or {}) :: { ToolCall }
        local delegate_calls = (agent_result.delegate_calls or {}) :: { any }

        for _, tool_call in ipairs(regular_tool_calls) do
            if not exit_tool_name or tool_call.name ~= exit_tool_name then
                tool_calls_count = tool_calls_count + 1
            end
        end

        total_tokens = accumulate_tokens(total_tokens, agent_result.tokens)

        local status_msg = build_status_message(iteration, max_iterations, total_tokens, tool_calls_count, false, false)
        update_node_progress(n, iteration, max_iterations, total_tokens, tool_calls_count, status_msg, agent_id,
            model_name)

        store_memory_recall(n, agent_result, iteration)
        store_agent_action(n, agent_result, iteration, agent_id, model_name, exit_tool_name, {})

        local recovery_hooks = config.recovery_test_hooks or {}
        if recovery_hooks.pre_action_submit_delay_ms and recovery_hooks.pre_action_submit_delay_ms > 0 then
            time.sleep(tostring(recovery_hooks.pre_action_submit_delay_ms) .. "ms")
        end

        local submit_ok, submit_err = n:submit()
        if not submit_ok then
            return n:fail({
                code = agent_consts.ERROR.AGENT_EXEC_FAILED,
                message = "Failed to persist agent turn: " .. tostring(submit_err)
            }, "Failed to persist agent turn: " .. tostring(submit_err))
        end
        pending_compaction_history = {}
        builder:with_pending_history(pending_compaction_history)

        local tool_call_to_node_id = create_tool_viz_nodes(n, regular_tool_calls, iteration, show_tool_calls,
            exit_tool_name)

        local tool_results = execute_tools({ tool_calls = regular_tool_calls }, caller, session_context)

        if show_tool_calls then
            update_tool_viz_nodes(n, tool_results, tool_call_to_node_id)
        end

        local finalized_complete, finalized_result, finalize_err = finalize_iteration(
            n,
            agent_ctx,
            session_context,
            iteration,
            max_iterations,
            min_iterations,
            tool_calling,
            exit_tool_name,
            {
                result = agent_result.result,
                tool_calls = regular_tool_calls,
                delegate_calls = delegate_calls
            },
            delegate_calls,
            tool_results,
            config.arena
        )
        if finalize_err then
            return n:fail({
                code = agent_consts.ERROR.STEP_FUNCTION_FAILED,
                message = finalize_err :: string
            }, finalize_err :: string)
        end

        if finalized_complete then
            task_complete = true
            final_result = finalized_result
        end

        ::continue_loop::
    end

    if not task_complete and iteration >= max_iterations then
        local final_status = build_status_message(iteration, max_iterations, total_tokens, tool_calls_count, true, false)
        update_node_progress(n, iteration, max_iterations, total_tokens, tool_calls_count, final_status, agent_id,
            model_name)

        return n:fail({
            code = agent_consts.ERROR.AGENT_EXEC_FAILED,
            message = "Maximum iterations reached without completion"
        }, "Maximum iterations reached")
    end

    local final_status = build_status_message(iteration, max_iterations, total_tokens, tool_calls_count, true,
        task_complete)
    update_node_progress(n, iteration, max_iterations, total_tokens, tool_calls_count, final_status, agent_id, model_name)

    local output_content = final_result or { success = false, error = "No result produced" }
    local success = true
    local final_error = nil
    if type(output_content) == "table" then
        success = output_content.success ~= false
        final_error = output_content.error
    end
    local message = success and agent_consts.STATUS.COMPLETED_SUCCESS or
        (type(final_error) == "string" and (agent_consts.STATUS.COMPLETED_ERROR .. final_error) or
            "Agent execution failed")

    local delegation_data_id = get_delegation_data_id(n)
    if delegation_data_id then
        n:update_metadata({
            delegation_output_data_id = delegation_data_id
        })
    end

    output_content = final_result or { success = false, error = "No result produced" }
    return n:complete(output_content, message)
end

return {
    run = run,
    _test = {
        build_agent_context_config = build_agent_context_config,
    }
}
