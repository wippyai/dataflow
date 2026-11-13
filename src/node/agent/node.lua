local json = require("json")
local time = require("time")
local logger = require("logger"):named("dataflow.agent")
local retry = require("retry")
local uuid = require("uuid")
local env = require("env")
local agent_ref = require("agent_ref")
local node_sdk = require("node_sdk")
local agent_context = require("agent_context")
local tool_caller = require("tool_caller")
local lifecycle_runtime = require("lifecycle_runtime")
local lifecycle_controller = require("lifecycle_controller")
local checkpoint_runtime = require("checkpoint_runtime")
local behavior_controls = require("behavior_controls")
local prompt_builder = require("prompt_builder")
local control_handler = require("control_handler")
local delegation_handler = require("delegation_handler")
local child_output = require("child_output")
local agent_consts = require("agent_consts")
local consts = require("consts")
local tools = require("tools")
local funcs = require("funcs")
local node_reader = require("node_reader")
local turn_context = require("turn_context")

type ToolCall = {
    id: string,
    name: string,
    arguments: string | table?,
    raw_arguments: string?,
    arguments_error: string?,
    registry_id: string?,
}

type ToolWrapperOutcome = {
    state: string,
    reason: string,
}

type ToolWrapperHostRef = {
    kind: string,
    dataflow_id: string?,
    node_id: string?,
    iteration: number?,
}

type ToolWrapperAgentRef = {
    id: string?,
    model: string?,
}

type ToolWrapperExecutionContext = {
    host: ToolWrapperHostRef,
    agent: ToolWrapperAgentRef,
    run_context: table?,
    outcome: ToolWrapperOutcome?,
}

type LifecyclePayload = {
    phase: string,
    host: table,
    agent: ToolWrapperAgentRef?,
    reason: string?,
    outcome: ToolWrapperOutcome?,
    refs: table?,
    run_context: table?,
    options: table?,
}

local function error_message(error_value: any, fallback: string)
    if type(error_value) == "string" then
        return error_value
    end
    if type(error_value) == "userdata" then
        return (error_value :: error):message()
    end
    if type(error_value) == "table" then
        local error_table = error_value :: any
        if type(error_table.message) == "string" then
            return error_table.message
        end
        if type(error_table.status) == "string" then
            return error_table.status
        end
        if error_table.error ~= nil then
            return error_message(error_table.error, fallback)
        end
        if type(error_table.code) == "string" then
            return error_table.code
        end
        return fallback
    end
    if error_value == nil then
        return fallback
    end
    return tostring(error_value)
end

local function string_or_nil(value: any): string?
    if type(value) == "string" and value ~= "" then
        return value
    end
    return nil
end

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

local function resolve_agent_checkpoint_config(config: any, agent_instance: any): table?
    local agent_options = agent_instance and agent_instance.agent_options
    local agent_checkpoint = type(agent_options) == "table" and agent_options.checkpoint or nil
    local config_checkpoint = config and config.checkpoint

    return checkpoint_runtime.resolve_options(agent_checkpoint, config_checkpoint)
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

local RUN_CONTEXT_CONTRACT = "wippy.agent:run_context"
local DEFAULT_RUN_CONTEXT_BINDING = "userspace.dataflow.node.agent.run_context:binding"
local DEFAULT_CHECKPOINT_FUNCTION_ENV = "userspace.dataflow.env:checkpoint_function_id"
local TURN_CONTEXT_KEY = "turn_context"
local CONVERSATION_INPUT_KEY = "conversation_input"

local OUTCOME = {
    CONTINUES = "continues",
    COMPLETED = "completed",
    FAILED = "failed",
}

local REASON = {
    AGENT_LOADED = "agent_loaded",
    AGENT_STEP = "agent_step",
    AGENT_SWITCH = "agent_switch",
    NO_TOOLS_REQUIRED = "no_tools_required",
    TOOL_RESULTS_RECORDED = "tool_results_recorded",
    CONTEXT_LIMIT_REACHED = "context_limit_reached",
    MAX_ITERATIONS_REACHED = "max_iterations_reached",
    UNPRODUCTIVE_STEPS = "unproductive_steps",
    HOST_FAILED = "host_failed",
    DATAFLOW_FINISHED = "dataflow_finished",
}

local function context_with_agent_run(base_context: any, n: any, agent_id: any, model_name: any, iteration: any, binding: any): table
    local next_context = {}
    if type(base_context) == "table" then
        for k, v in pairs(base_context) do
            next_context[k] = v
        end
    end
    -- Match the run-context API: settled session controls overlay the arena
    -- defaults for each new call, including after recovery. Never mutate a
    -- snapshot already handed to a tool or summarizer.
    local metadata = n:metadata() or {}
    if type(metadata.session_context) == "table" then
        for k, v in pairs(metadata.session_context) do next_context[k] = v end
    end

    local host = {
        kind = "dataflow",
        dataflow_id = n.dataflow_id,
        node_id = n.node_id,
        iteration = tonumber(iteration) or 0
    }
    local agent_info = {
        id = agent_id,
        model = model_name
    }

    next_context.agent_run = {
        host = host,
        agent = agent_info,
        run_context = {
            contract = RUN_CONTEXT_CONTRACT,
            binding = binding or DEFAULT_RUN_CONTEXT_BINDING,
            host = host,
            agent = agent_info
        }
    }

    return next_context
end

local function lifecycle_host_ref(n: any, iteration: any): table
    return {
        kind = "dataflow",
        dataflow_id = n.dataflow_id,
        node_id = n.node_id,
        iteration = tonumber(iteration) or 0
    }
end

local function lifecycle_agent_ref(agent_id: any, model_name: any): ToolWrapperAgentRef
    return {
        id = string_or_nil(agent_id),
        model = string_or_nil(model_name)
    }
end

local function lifecycle_run_context_ref(host: table, agent_ref: ToolWrapperAgentRef, binding: any): table
    local binding_id = string_or_nil(binding) or DEFAULT_RUN_CONTEXT_BINDING
    return {
        contract = RUN_CONTEXT_CONTRACT,
        binding = binding_id,
        host = host,
        agent = agent_ref
    }
end

local function apply_agent_lifecycle(agent_instance: any, phase: string, n: any, agent_id: any, model_name: any,
                                     iteration: any, run_context_binding: any, opts: table?): (table?, string?)
    if not agent_instance or type(agent_instance.bindings) ~= "table" then
        return { applied = 0, skipped = 0 }, nil
    end

    local host = lifecycle_host_ref(n, iteration)
    local agent_ref = lifecycle_agent_ref(agent_id, model_name)
    local payload = {
        phase = phase,
        host = host,
        agent = agent_ref,
        reason = opts and opts.reason,
        outcome = opts and opts.outcome,
        refs = opts and opts.refs,
        run_context = lifecycle_run_context_ref(host, agent_ref, run_context_binding),
    } :: LifecyclePayload

    return lifecycle_runtime.apply(agent_instance.bindings, payload :: any)
end

-- A note a step sends after the history. It is stored before the step output,
-- so every later request replays it where this one sent it.
type StepNote = {
    role: string,
    content: string,
    metadata: table,
    lifecycle_phase: string?,
}

local NOTE_ROLES = { system = true, developer = true }

local function positional_metadata(source: any): table
    local metadata = {}
    if type(source) == "table" then
        for key, value in pairs(source) do metadata[key] = value end
    end
    metadata.preserve_position = true
    return metadata
end

-- Lifecycle messages are host notes: a user or assistant role would put words
-- in either party's mouth, so only developer and system are accepted.
local function lifecycle_notes(result: table?, phase: string, agent_id: any): ({StepNote}?, string?)
    local notes: {StepNote} = {}
    if type(result) ~= "table" or type(result.messages) ~= "table" then
        return notes, nil
    end
    for _, message in ipairs(result.messages) do
        if type(message) == "table" then
            local content = message.content or message.text or message.data
            if type(content) == "string" and content ~= "" then
                local role = message.role or message.type or "developer"
                if not NOTE_ROLES[role] then
                    return nil, string.format("Lifecycle %s message for agent %s has role '%s'; "
                        .. "lifecycle messages must be developer or system notes",
                        phase, tostring(agent_id), tostring(role))
                end
                table.insert(notes, { role = role, content = content,
                    metadata = positional_metadata(message.metadata), lifecycle_phase = phase })
            end
        end
    end
    return notes, nil
end

-- Context notes are prompt messages the agent sent after the conversation.
local function context_notes(result: any): ({StepNote}?, string?)
    local notes: {StepNote} = {}
    if type(result) ~= "table" or type(result.context_notes) ~= "table" then
        return notes, nil
    end
    for index, message in ipairs(result.context_notes) do
        local role = type(message) == "table" and message.role or nil
        local content = type(message) == "table" and message.content or nil
        if type(content) == "table" and #content == 1 and type(content[1]) == "table"
            and content[1].type == "text" then
            content = content[1].text
        end
        if not NOTE_ROLES[role] or type(content) ~= "string" then
            return nil, "Context note " .. tostring(index) .. " is not a single-text developer or system message"
        end
        table.insert(notes, { role = role, content = content, metadata = positional_metadata(message.metadata) })
    end
    return notes, nil
end

local function has_conversation(prompt: any): boolean
    for _, message in ipairs(prompt:get_messages()) do
        if message.role ~= "cache_marker"
            and (message.role ~= "system" or (message.metadata or {}).preserve_position == true) then
            return true
        end
    end
    return false
end

local function append_step_notes(prompt: any, notes: {StepNote})
    for _, note in ipairs(notes) do
        if note.role == "system" then
            prompt:add_system(note.content, note.metadata)
        else
            prompt:add_developer(note.content, note.metadata)
        end
    end
end

local function step_outcome(agent_result: any): table
    if agent_result and agent_result.truncated then
        return {
            state = OUTCOME.CONTINUES,
            reason = REASON.CONTEXT_LIMIT_REACHED
        }
    end

    local has_tools = agent_result and (
        (type(agent_result.tool_calls) == "table" and #agent_result.tool_calls > 0) or
        (type(agent_result.delegate_calls) == "table" and #agent_result.delegate_calls > 0)
    )
    if has_tools then
        return {
            state = OUTCOME.CONTINUES,
            reason = REASON.TOOL_RESULTS_RECORDED
        }
    end

    return {
        state = OUTCOME.COMPLETED,
        reason = REASON.NO_TOOLS_REQUIRED
    }
end

local function lifecycle_outcome_from_agent_result(agent_result: any): table
    local outcome = step_outcome(agent_result)
    if agent_result and agent_result.memory_recall_error then
        outcome.memory_recall_error = agent_result.memory_recall_error
    end
    return outcome
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
    -- An input entry with nil content is an input that was not provided — an
    -- input_transform field whose expression resolved to nil produces exactly
    -- this shape. It never counts as a malformed value and never renders a tag.
    local input_context = nil
    if inputs.context and inputs.context.content ~= nil then
        local context_content = inputs.context.content
        if type(context_content) ~= "table" then
            return nil, nil, nil, nil, nil, "context must be a table/object"
        end
        input_context = context_content
    end

    local agent_id_override = nil
    if inputs.agent_id and inputs.agent_id.content ~= nil then
        local agent_id_content = inputs.agent_id.content
        if type(agent_id_content) ~= "string" or agent_id_content == "" then
            return nil, nil, nil, nil, nil, "agent_id must be a non-empty string"
        end
        agent_id_override = agent_id_content
    end

    local model_override = nil
    if inputs.model and inputs.model.content ~= nil then
        local model_content = inputs.model.content
        if type(model_content) ~= "string" or model_content == "" then
            return nil, nil, nil, nil, nil, "model must be a non-empty string"
        end
        model_override = model_content
    end

    local max_iterations_override = nil
    if inputs.max_iterations and inputs.max_iterations.content ~= nil then
        local max_iterations_content = tonumber(inputs.max_iterations.content)
        if not max_iterations_content or max_iterations_content < 1
            or max_iterations_content ~= math.floor(max_iterations_content) then
            return nil, nil, nil, nil, nil, "max_iterations must be a positive integer"
        end
        max_iterations_override = max_iterations_content
    end

    local keys = {}
    for key, input in pairs(inputs) do
        if prompt_builder.is_conversation_input(key) and input.content ~= nil then
            table.insert(keys, key)
        end
    end
    table.sort(keys)

    local parts = {}
    for _, key in ipairs(keys) do
        local content = inputs[key].content
        if type(content) == "table" then
            content = json.encode(content)
        else
            content = tostring(content)
        end
        table.insert(parts, string.format('<input key="%s">\n%s\n</input>', key, content))
    end

    if #parts == 0 then
        return input_context, agent_id_override, model_override, max_iterations_override, "", nil
    end

    return input_context, agent_id_override, model_override, max_iterations_override, table.concat(parts, "\n\n"), nil
end

local function validate_and_resolve_config(config)
    if not config then
        return nil, agent_consts.ERROR_MSG.INVALID_CONFIG
    end

    if not config.arena then
        return nil, "Arena configuration is required"
    end

    -- The resolved mode is written back so every consumer reads one value.
    local tool_calling = config.arena.tool_calling or agent_consts.DEFAULTS.TOOL_CALLING
    local has_exit_schema = config.arena.exit_schema ~= nil
    config.arena.tool_calling = tool_calling

    if tool_calling == agent_consts.TOOL_CALLING.ANY and not has_exit_schema then
        return nil, "any mode requires exit_schema to be defined"
    end

    if tool_calling == agent_consts.TOOL_CALLING.NONE and has_exit_schema then
        return nil, "none mode cannot have exit_schema"
    end

    if config.arena.max_unproductive_steps ~= nil then
        local bound = tonumber(config.arena.max_unproductive_steps)
        if not bound or bound < 1 or bound ~= math.floor(bound) then
            return nil, "max_unproductive_steps must be a positive integer"
        end
        config.arena.max_unproductive_steps = bound
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

local function carry_route_pin(current: any, agent_result: any): any?
    if type(agent_result) == "table" and agent_result.route_pin ~= nil then
        return agent_result.route_pin
    end
    return current
end

local function route_pin_after_switch(pin: any, previous_agent_id: any, previous_model: any,
                                      agent_id: any, model_name: any): any?
    if agent_id ~= previous_agent_id or model_name ~= previous_model then
        return nil
    end
    return pin
end

local function update_node_progress(n, iteration, max_iterations, total_tokens, tool_calls_count, status_message,
                                    agent_id, model_name, unproductive_steps, route_pin)
    local state_info = {
        current_iteration = iteration,
        max_iterations = max_iterations,
        agent_id = agent_id,
        model = model_name,
        total_tokens = total_tokens,
        tool_calls = tool_calls_count,
        unproductive_steps = unproductive_steps,
        route_pin = route_pin
    }

    n:update_metadata({
        status_message = status_message,
        state = state_info
    })
end

local function store_agent_action(n, agent_result, iteration, agent_id, model_name, exit_tool_name, control_metadata, latency_ms)
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

    local llm_meta = {}
    for key, value in pairs(agent_result.metadata or {}) do llm_meta[key] = value end
    llm_meta.prefix_fingerprint = agent_result.prefix_fingerprint

    local metadata = {
        iteration = iteration,
        agent_id = agent_id,
        model = model_name,
        tokens = agent_result.tokens,
        finish_reason = agent_result.finish_reason,
        latency_ms = latency_ms,
        llm_meta = llm_meta,
        memory_recall_error = agent_result.memory_recall_error,
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
    return action_key
end

-- The committed row is authoritative: persistence deduplication can keep an
-- earlier row's data_id instead of the candidate generated for this command.
local function persisted_action_id(n, action_key): (string?, string?)
    local row = n:query()
        :with_nodes(n.node_id)
        :with_data_types(agent_consts.DATA_TYPE.AGENT_ACTION)
        :with_data_keys(action_key)
        :one()
    if not row or type(row.data_id) ~= "string" or row.data_id == "" then
        return nil, "Persisted agent action not found: " .. tostring(action_key)
    end
    return row.data_id, nil
end

local function store_step_notes(n, notes: {StepNote}, iteration)
    for index, note in ipairs(notes) do
        n:data(agent_consts.DATA_TYPE.AGENT_MEMORY, note.content, {
            key = iteration .. "_note_" .. index,
            content_type = consts.CONTENT_TYPE.TEXT,
            node_id = n.node_id,
            metadata = {
                iteration = iteration,
                role = note.role,
                lifecycle_phase = note.lifecycle_phase,
                llm_meta = note.metadata
            }
        })
    end
end

-- The stored row decides: a start before the first commit renders the note, and
-- every start after it replays the minute the first request sent.
local function store_turn_context(n, agent_instance: any): any
    local note, render_err = turn_context.render(agent_instance.agent_options, time.now())
    if render_err then
        return "Failed to render turn context: " .. tostring(render_err)
    end
    if not note then
        return nil
    end
    local stored = n:query()
        :with_nodes(n.node_id)
        :with_data_types(agent_consts.DATA_TYPE.AGENT_MEMORY)
        :with_data_keys(TURN_CONTEXT_KEY)
        :one()
    if stored then
        return nil
    end
    n:data(agent_consts.DATA_TYPE.AGENT_MEMORY, note, {
        key = TURN_CONTEXT_KEY,
        content_type = consts.CONTENT_TYPE.TEXT,
        node_id = n.node_id,
        metadata = {
            turn_context = true,
            role = "system",
            llm_meta = { preserve_position = true }
        }
    })
    local _, yield_err = n:yield()
    return yield_err
end

local function load_pinned_input(n)
    local stored = n:query()
        :with_nodes(n.node_id)
        :with_data_types(agent_consts.DATA_TYPE.AGENT_INPUT)
        :all()
    return stored and stored[1] or nil
end

-- The fold is the first user message of every request, so the first start pins it.
-- It commits with the node's first commit; a start before that sent nothing that
-- an output depends on and folds again.
local function pin_conversation_input(n, pinned_input, folded: string): string
    if pinned_input then
        return pinned_input.content
    end
    n:data(agent_consts.DATA_TYPE.AGENT_INPUT, folded, {
        key = CONVERSATION_INPUT_KEY,
        content_type = consts.CONTENT_TYPE.TEXT,
        node_id = n.node_id,
    })
    return folded
end

local function extract_checkpoint_scenario_id(history_rows)
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

-- Conversation checkpointing runs at the start of the next turn, after the prior
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
    local _, over_cap = prompt_builder.text_prefix(text, cap)
    if not over_cap then
        return text
    end
    local suffix = " ... [truncated]"
    if cap <= #suffix then
        return (prompt_builder.text_prefix(text, cap))
    end
    return (prompt_builder.text_prefix(text, cap - #suffix)) .. suffix
end

-- Record a checkpoint-skipped observation row so non-strict mode leaves an
-- audit trail but doesn't kill the turn.
local function record_checkpoint_skip(n, iteration, reason)
    n:data(agent_consts.DATA_TYPE.AGENT_OBSERVATION, error_message(reason, "checkpoint skipped"), {
        key = tostring(iteration or 0) .. "_checkpoint_skipped",
        content_type = consts.CONTENT_TYPE.TEXT,
        node_id = n.node_id,
        metadata = {
            iteration = iteration,
            checkpoint_skipped = true,
            checkpoint_error = error_message(reason, "")
        }
    })
end

local function has_checkpoint_bindings(agent_instance: any): boolean
    local bindings = agent_instance and agent_instance.bindings
    local checkpoint_bindings = type(bindings) == "table" and bindings.checkpoint or nil
    return type(checkpoint_bindings) == "table" and #checkpoint_bindings > 0
end

local function default_checkpoint_function_id(): string?
    local function_id, _ = env.get(DEFAULT_CHECKPOINT_FUNCTION_ENV)
    if type(function_id) == "string" and function_id ~= "" then
        return function_id
    end
    return nil
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

-- A call of the newest action with no result may still get one: a finish whose node
-- has not completed runs again on recovery. An older unanswered call cannot, so it is
-- summarized with its round.
local function latest_action_is_open(action_row, rows)
    local payload = load_action_payload(action_row)
    local metadata = payload and payload.metadata or {}
    if not payload or metadata.finish_reason == "length" then
        return false
    end
    local observed = {}
    for _, row in ipairs(rows) do
        local row_metadata = row.metadata or {}
        if row_metadata.tool_call_id and row_metadata.iteration == metadata.iteration then
            observed[row_metadata.tool_call_id] = true
        end
    end
    local tools, delegates = collect_unresolved_calls(payload, observed)
    return #tools > 0 or #delegates > 0
end

local function maybe_checkpoint_history(n, config, session_context, agent_instance, agent_id, model_name, iteration,
                                        completing)
    local checkpoint_cfg = config and config.checkpoint
    if type(checkpoint_cfg) ~= "table" then
        return nil, nil
    end
    if checkpoint_cfg.enabled == false then
        return nil, nil
    end

    local threshold = tonumber(checkpoint_cfg.token_threshold)
    local compact_requested = (n:metadata() or {}).checkpoint_requested == true
    local func_id = checkpoint_cfg.function_id
    if type(func_id) ~= "string" or func_id == "" then
        func_id = default_checkpoint_function_id()
    end
    local has_binding = has_checkpoint_bindings(agent_instance)
    if (not compact_requested and (not threshold or threshold <= 0)) or
        (not has_binding and (type(func_id) ~= "string" or func_id == "")) then
        return nil, nil
    end

    local history_rows = (n:query() :: any)
        :fetch_options({ replace_references = true })
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

    prompt_builder.sort_history_rows(history_rows)

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
    -- context_tokens is the full prompt size (uncached input plus cache reads
    -- and writes); prompt_tokens alone counts only the uncached input.
    local context_tokens = tonumber(turn_tokens.context_tokens) or 0
    local prompt_tokens = tonumber(turn_tokens.prompt_tokens) or 0
    if not compact_requested and context_tokens <= threshold then
        return nil, nil
    end

    local source_rows = prompt_builder.checkpoint_source_rows(history_rows)

    -- The result needs its call in the request. The request flag waits for the round
    -- to close, unless this is the node's last commit and no request follows.
    if not completing and latest_action_is_open(latest_action, source_rows) then
        return nil, nil
    end

    -- A summary with nothing after it has nothing to replace.
    local latest_marker = prompt_builder.find_latest_checkpoint_marker(history_rows)
    if #source_rows == (latest_marker and 1 or 0) then
        if compact_requested then n:update_metadata({ checkpoint_requested = false }) end
        return nil, nil
    end

    -- A failed summary of these same rows would fail again. New rows bring a new try.
    local attempted_rows = prompt_builder.row_id(source_rows[#source_rows])
    if n._failed_checkpoint_rows == attempted_rows then
        return nil, nil
    end
    local function failed(message, strict)
        n._failed_checkpoint_rows = attempted_rows
        return nil, message, strict
    end

    local cut_before = prompt_builder.row_id(history_rows[#history_rows])

    -- A serialization-safe projection of the rows the summary replaces, calls and
    -- results together.
    local history_payload = {}
    for i, row in ipairs(source_rows) do
        history_payload[i] = {
            data_id = prompt_builder.row_id(row),
            type = tostring(row.type or ""),
            content = stringify_history_content(row),
            content_type = tostring(row.content_type or ""),
            metadata = row.metadata
        }
    end

    local checkpoint_iteration = tonumber(action_metadata.iteration) or 0
    local scenario_id = extract_checkpoint_scenario_id(history_rows)

    local summary_result = nil
    local checkpoint_source = nil
    local binding_failed = false

    if has_binding then
        local host = {
            kind = "dataflow",
            dataflow_id = n.dataflow_id,
            node_id = n.node_id,
            iteration = tonumber(iteration) or checkpoint_iteration
        }
        local agent = {
            id = agent_id,
            model = model_name
        }
        local runtime_result, runtime_err = checkpoint_runtime.create(agent_instance.bindings, {
            host = host,
            agent = agent,
            reason = compact_requested and "compaction_requested" or "token_threshold_exceeded",
            selector = {
                mode = "since_checkpoint"
            },
            run_context = {
                contract = RUN_CONTEXT_CONTRACT,
                binding = config.run_context_binding or DEFAULT_RUN_CONTEXT_BINDING,
                host = host,
                agent = agent
            },
            refs = {
                dataflow_id = n.dataflow_id,
                node_id = n.node_id,
                scenario_id = scenario_id,
                iteration = checkpoint_iteration,
                prompt_tokens = prompt_tokens,
                context_tokens = context_tokens,
                history_count = #history_payload,
                checkpoint_before_data_id = cut_before
            },
            history = history_payload,
            options = checkpoint_cfg,
        })
        if runtime_err then
            return failed("checkpoint binding failed: " .. error_message(runtime_err, "unknown"), true)
        end
        if runtime_result and runtime_result.result then
            summary_result = runtime_result.result
            checkpoint_source = "binding"
        elseif runtime_result and runtime_result.errors and #runtime_result.errors > 0 then
            binding_failed = true
        end
    end

    if not summary_result then
        if type(func_id) ~= "string" or func_id == "" then
            if binding_failed then
                -- Strict binding failures returned above. A non-strict failure
                -- remains skippable unless the host requires strict checkpointing.
                return failed("checkpoint binding failed and no fallback function is configured")
            end
            return nil, nil
        end

        local call_err
        local executor = funcs.new()
        summary_result, call_err = executor
            :with_context(session_context or {})
            :call(func_id, {
                dataflow_id = n.dataflow_id,
                node_id = n.node_id,
                scenario_id = scenario_id,
                iteration = checkpoint_iteration,
                prompt_tokens = prompt_tokens,
                context_tokens = context_tokens,
                history_count = #history_payload,
                checkpoint_before_data_id = cut_before,
                history = history_payload,
                options = checkpoint_cfg,
            })

        if call_err then
            return failed("checkpoint function " .. func_id .. " failed: " .. error_message(call_err, "unknown"))
        end
        checkpoint_source = "function"
    end

    local memory_text = nil
    if type(summary_result) == "table" then
        memory_text = summary_result.memory or summary_result.summary
    elseif type(summary_result) == "string" then
        memory_text = summary_result
    end

    if type(memory_text) ~= "string" or not memory_text:match("%S") then
        return failed("checkpoint provider returned empty memory")
    end

    -- Hard cap on stored memory to prevent hostile/buggy summarizers from
    -- bloating every future prompt. Default 8192 chars; configurable.
    local max_memory_chars = tonumber(checkpoint_cfg.max_memory_chars) or agent_consts.DEFAULTS.MAX_MEMORY_CHARS
    local original_memory_len = #memory_text
    memory_text = clamp_memory_text(memory_text, max_memory_chars)
    local memory_truncated = #memory_text < original_memory_len

    local marker_metadata = {
        iteration = checkpoint_iteration,
        checkpoint_marker = true,
        checkpoint_reason = compact_requested and "compaction_requested" or "token_threshold_exceeded",
        checkpoint_at_prompt_tokens = prompt_tokens,
        checkpoint_at_context_tokens = context_tokens,
        checkpoint_before_data_id = cut_before,
        checkpoint_history_count = #history_payload,
        checkpoint_first_data_id = prompt_builder.row_id(source_rows[1]),
        checkpoint_last_data_id = prompt_builder.row_id(source_rows[#source_rows]),
        checkpoint_source_action_data_id = tostring(latest_action.data_id or ""),
        checkpoint_function_id = func_id,
        checkpoint_source = checkpoint_source,
        checkpoint_memory_truncated = memory_truncated
    }
    local marker_data_id = uuid.v7()
    local marker_key_prefix = checkpoint_iteration > 0 and tostring(checkpoint_iteration) or cut_before

    n:data(agent_consts.DATA_TYPE.AGENT_MEMORY, memory_text, {
        data_id = marker_data_id,
        key = marker_key_prefix .. "_checkpoint",
        content_type = consts.CONTENT_TYPE.TEXT,
        node_id = n.node_id,
        metadata = marker_metadata
    })
    if compact_requested then n:update_metadata({ checkpoint_requested = false }) end

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

-- Call ids are unique within one turn only, so a call is resolved by an
-- observation of its own iteration.
local function load_observed_tool_call_ids(n, iteration)
    local observed_ids = {}
    local observations = n:query()
        :with_nodes(n.node_id)
        :with_data_types(agent_consts.DATA_TYPE.AGENT_OBSERVATION)
        :all()

    for _, observation in ipairs(observations or {}) do
        local metadata = observation.metadata or {}
        if metadata.tool_call_id and metadata.iteration == iteration then
            observed_ids[metadata.tool_call_id] = true
        end
    end

    return observed_ids
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

local function collect_control_outputs(n, child_node_ids, yield_results)
    local output_data, output_err = child_output.collect_outputs(n, child_node_ids, yield_results)
    -- This callback runs after every recorded child has a terminal result.
    n:update_metadata({ agent_pending = false })
    return output_data, output_err
end

local function resume_control_commands(n, child_node_ids, iteration)
    local output_data, output_err = child_output.resume_children(
        n,
        child_node_ids,
        collect_control_outputs
    )
    if output_err then return output_err end

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
            content_type = type(output_content) == "table" and consts.CONTENT_TYPE.JSON or consts.CONTENT_TYPE.TEXT,
            node_id = n.node_id,
            metadata = {
                iteration = iteration,
                created_nodes = child_node_ids
            }
        })
    end

    local _, flush_err = n:yield()
    return flush_err
end

local function apply_control_response_commands(control_responses, agent_ctx, n, iteration)
    local created_node_ids = {}
    local pending = (n:metadata() or {}).agent_pending
    if type(pending) == "table" then
        for _, node_id in ipairs(pending.child_node_ids or {}) do
            table.insert(created_node_ids, node_id)
        end
    end
    for _, response in ipairs(control_responses) do
        if response.changes_applied and response.changes_applied.commands and response.changes_applied.created_nodes then
            for _, node_id in ipairs(response.changes_applied.created_nodes) do
                table.insert(created_node_ids, node_id)
            end
        end
    end

    -- Persist the exact child set in the same commit that creates it, ahead of any
    -- yield directive. A restarted Agent must resume this DAG and collect its output,
    -- not ask the model to compose a replacement.
    if #created_node_ids > 0 then
        n:update_metadata({
            agent_pending = {
                iteration = iteration,
                child_node_ids = created_node_ids
            }
        })
    end

    local _changes_summary, changes_err = control_handler.apply_control_responses(control_responses, agent_ctx, n)
    return changes_err
end

local function resume_pending_control_commands(n, iteration)
    local pending = (n:metadata() or {}).agent_pending
    if type(pending) ~= "table" or type(pending.child_node_ids) ~= "table"
        or #pending.child_node_ids == 0 then return nil end

    iteration = tonumber(pending.iteration) or iteration
    local resume_err = resume_control_commands(n, pending.child_node_ids, iteration)
    if resume_err then
        n:data(agent_consts.DATA_TYPE.AGENT_OBSERVATION,
            "Command execution failed: " .. error_message(resume_err, "unknown"), {
            key = iteration .. "_commands_error",
            content_type = consts.CONTENT_TYPE.TEXT,
            node_id = n.node_id,
            metadata = { iteration = iteration, is_error = true }
        })
    end
    return resume_err
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

-- The tool call nodes already committed for one round, by call id.
local function load_tool_viz_nodes(n, iteration): ({[string]: string}?, string?)
    local rows, read_err = (node_reader.with_dataflow(n.dataflow_id) :: any)
        :with_parent_nodes(n.node_id)
        :with_node_types("tool.call")
        :all()
    if read_err then
        return nil, "Failed to load tool call nodes: " .. error_message(read_err, "unknown")
    end
    local node_ids: {[string]: string} = {}
    for _, row in ipairs(rows or {}) do
        local metadata = row.metadata or {}
        if metadata.iteration == iteration and type(metadata.tool_call_id) == "string" then
            node_ids[metadata.tool_call_id] = row.node_id
        end
    end
    return node_ids, nil
end

local function tool_error_text(tool_error: any): string
    return error_message(tool_error, "Tool call failed")
end

-- A call listed in `committed_node_ids` keeps its existing node;
-- every other call gets a new node.
local function create_tool_viz_nodes(n, tool_calls: { ToolCall }?, iteration, show_tool_calls, exit_tool_name,
                                     committed_node_ids: {[string]: string}?)
    local tool_call_to_node_id = {}

    if show_tool_calls == false or not tool_calls or #tool_calls == 0 then
        return tool_call_to_node_id
    end

    for _, tool_call in ipairs(tool_calls) do
        if exit_tool_name and tool_call.name == exit_tool_name then
            goto continue
        end

        local committed_node_id = committed_node_ids and committed_node_ids[tool_call.id]
        if committed_node_id then
            tool_call_to_node_id[tool_call.id] = committed_node_id
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

local function update_tool_viz_nodes(n, tool_results, tool_call_to_node_id, persisted_call_ids)
    if not tool_results or not tool_call_to_node_id then
        return
    end

    for call_id, result_data in pairs(tool_results) do
        local viz_node_id = tool_call_to_node_id[call_id]
        if viz_node_id and not (persisted_call_ids and persisted_call_ids[call_id]) then
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
                        error_message = tool_error ~= nil and tool_error_text(tool_error) or nil,
                        output_size_bytes = output_size
                    }
                }
            })
        end
    end
end

local function cancel_unfinished_tool_viz_nodes(n, finished_call_ids, tool_call_to_node_id)
    for call_id, viz_node_id in pairs(tool_call_to_node_id) do
        if not finished_call_ids[call_id] then
            n:command({
                type = consts.COMMAND_TYPES.UPDATE_NODE,
                payload = { node_id = viz_node_id, status = consts.STATUS.CANCELLED }
            })
        end
    end
end

-- Provider responses are external input. Validate call metadata before any
-- phase uses it for routing, persistence, invocation, or observation keys.
-- In particular, do not turn a malformed call into an observation with an
-- invented name: the node must fail through its configured error targets.
local function validate_tool_calls(tool_calls: any): (boolean, string?)
    if tool_calls == nil then return true, nil end
    if type(tool_calls) ~= "table" then
        return false, "Malformed tool calls: expected an array"
    end

    local count = #tool_calls
    for key in pairs(tool_calls) do
        if type(key) ~= "number" or key < 1 or key % 1 ~= 0 or key > count then
            return false, "Malformed tool calls: expected an array"
        end
    end

    for index = 1, count do
        local tool_call = tool_calls[index]
        if type(tool_call) ~= "table" then
            return false, "Malformed tool call at index " .. index .. ": expected an object"
        end
        if type(tool_call.id) ~= "string" or tool_call.id:match("%S") == nil then
            return false, "Malformed tool call at index " .. index .. ": id is required"
        end
        if type(tool_call.name) ~= "string" or tool_call.name:match("%S") == nil then
            return false, "Malformed tool call at index " .. index .. ": name is required"
        end
    end

    return true, nil
end

local function split_exit_tool_calls(tool_calls: {ToolCall}?, exit_tool_name: string?): ({ToolCall}, {ToolCall})
    local executable_tool_calls = {} :: { ToolCall }
    local exit_tool_calls = {} :: { ToolCall }

    for _, tool_call in ipairs(tool_calls or {}) do
        if exit_tool_name and tool_call.name == exit_tool_name then
            table.insert(exit_tool_calls, tool_call)
        else
            table.insert(executable_tool_calls, tool_call)
        end
    end

    return executable_tool_calls, exit_tool_calls
end

local function prepare_tools(agent_result: { tool_calls: { ToolCall }? }, caller): (any, {ToolCall}, string?)
    local validated_tools, validate_err = caller:validate(agent_result.tool_calls)
    if validate_err then
        return {}, {}, validate_err
    end

    local effective_tool_calls: {ToolCall} = agent_result.tool_calls or {}
    if type(caller.get_last_tool_calls) == "function" then
        effective_tool_calls = (caller:get_last_tool_calls() or {}) :: {ToolCall}
    end

    return validated_tools, effective_tool_calls, nil
end

local function execute_tools(caller, session_context, validated_tools, result_listener)
    caller:set_result_listener(result_listener)
    local tool_results, execute_err = caller:execute(session_context or {}, validated_tools)
    caller:set_result_listener(nil)
    if execute_err then
        return nil, "Failed to persist tool result: " .. error_message(execute_err, "unknown")
    end
    return tool_results or {}, nil
end

local function configure_tool_wrappers(caller, agent_instance, n, agent_id, model_name, iteration, outcome: ToolWrapperOutcome?,
                                      run_context_binding: string?)
    local host = {
        kind = "dataflow",
        dataflow_id = string_or_nil(n.dataflow_id),
        node_id = string_or_nil(n.node_id),
        iteration = tonumber(iteration) or 0
    } :: ToolWrapperHostRef
    local agent_ref = {
        id = string_or_nil(agent_id),
        model = string_or_nil(model_name)
    } :: ToolWrapperAgentRef
    local binding_id = string_or_nil(run_context_binding) or DEFAULT_RUN_CONTEXT_BINDING

    local wrapper_context: ToolWrapperExecutionContext = {
        host = {
            kind = host.kind,
            dataflow_id = host.dataflow_id,
            node_id = host.node_id,
            iteration = host.iteration
        },
        agent = agent_ref,
        run_context = {
            contract = RUN_CONTEXT_CONTRACT,
            binding = binding_id,
            host = host,
            agent = agent_ref
        },
        outcome = outcome
    }

    if type(caller.set_tool_wrappers) == "function" then
        caller:set_tool_wrappers(agent_instance and agent_instance.tool_wrappers or {})
    end
    if type(caller.set_wrapper_context) == "function" then
        caller:set_wrapper_context(wrapper_context)
    end
end

local function exit_schema_violation(schema: any, arguments: any): string?
    if type(schema) ~= "table" then return nil end
    if type(arguments) ~= "table" then
        return "the finish call carried " .. type(arguments)
            .. " arguments where the exit schema requires an object"
    end

    local missing = {} :: { string }
    local required = (schema :: any).required
    if type(required) == "table" then
        for _, key in ipairs(required :: { any }) do
            if type(key) == "string" and (arguments :: any)[key] == nil then
                table.insert(missing, key :: string)
            end
        end
    end
    if #missing > 0 then
        return "the finish call is missing required fields: " .. table.concat(missing, ", ")
    end

    local properties = (schema :: any).properties
    if next(arguments :: any) == nil and type(properties) == "table" and next(properties :: any) ~= nil then
        return "the finish call carried no arguments where the exit schema declares properties"
    end
    return nil
end

-- A call whose arguments failed to parse reports that failure as its result.
local function call_result(tool_call, result_data)
    if tool_call.arguments_error then
        return { error = "Failed to parse arguments: " .. tool_call.arguments_error }
    end
    return result_data
end

local function record_tool_observation(n, iteration, tool_call, content, tool_error, tool_call_to_node_id)
    local call_id = tool_call.id
    local obs_content = content
    if tool_error ~= nil then
        obs_content = tool_error_text(tool_error)
    elseif type(obs_content) ~= "table" then
        obs_content = tostring(obs_content)
    end

    n:data(agent_consts.DATA_TYPE.AGENT_OBSERVATION, obs_content, {
        key = iteration .. "_" .. tool_call.name,
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

    local viz_node_id = tool_error ~= nil and tool_call_to_node_id and tool_call_to_node_id[call_id] or nil
    if viz_node_id then
        -- Declared alongside the observation just recorded; both flush in one
        -- durable commit. A consumed child failure is not workflow-terminal
        -- evidence; the consuming parent's own outcome is.
        n:command({
            type = consts.COMMAND_TYPES.UPDATE_NODE,
            payload = {
                node_id = viz_node_id,
                metadata = { error_observed = true }
            }
        })
    end
end

-- Calls in `persisted_call_ids` already committed their observation as they
-- finished; the round records the remaining calls in model order.
local function process_tool_results(n, tool_results, iteration, exit_tool_name, agent_result: any, arena_config,
                                    session_context, tool_call_to_node_id, settle_completed_round: boolean?,
                                    persisted_call_ids: {[string]: boolean}?)
    local control_responses = {}
    local control_delegations = {}
    local task_complete = false
    local final_result = nil

    if exit_tool_name and agent_result.tool_calls then
        local finish_seen = false
        for _, original_tool_call in ipairs(agent_result.tool_calls) do
            if original_tool_call.name == exit_tool_name then
                if original_tool_call.arguments_error then
                    finish_seen = true
                    goto next_exit_call
                end
                -- Only the first finish decides completion. Every later finish
                -- still needs a result, or a rejected turn cannot be sent again.
                if finish_seen then
                    n:data(agent_consts.DATA_TYPE.AGENT_OBSERVATION,
                        "Call " .. exit_tool_name .. " once with all required outputs; additional calls in this turn are ignored.", {
                            key = iteration .. "_exit_duplicate_" .. original_tool_call.id,
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
                    goto next_exit_call
                end
                finish_seen = true
                local exit_arguments = original_tool_call.arguments

                if arena_config.exit_func_id then
                    local executor = funcs.new()
                    local validated_result, validation_err = executor
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
                    else
                        task_complete = true
                        final_result = validated_result
                    end
                else
                    local violation = exit_schema_violation(arena_config.exit_schema, exit_arguments)
                    if violation then
                        n:data(agent_consts.DATA_TYPE.AGENT_OBSERVATION, violation, {
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
                    else
                        task_complete = true
                        final_result = exit_arguments
                    end
                end
            end
            ::next_exit_call::
        end
    end

    if agent_result.tool_calls then
        for _, tool_call in ipairs(agent_result.tool_calls) do
            local call_id = tool_call.id
            if persisted_call_ids and persisted_call_ids[call_id] then
                goto next_tool_result
            end
            local result_data = call_result(tool_call, tool_results[call_id])
            if not tool_call.arguments_error and task_complete and not settle_completed_round then
                goto next_tool_result
            end

            if result_data then
                local tool_result = result_data.result
                local tool_error = result_data.error

                local cleaned_result, control_response, control_err = control_handler.process_control_directive(
                    tool_result, n, iteration, call_id
                )
                if control_err then return nil, nil, false, nil, control_err end
                if control_response then
                    control_response.tool_call_id = call_id
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
                    record_tool_observation(n, iteration, tool_call, cleaned_result, tool_error, tool_call_to_node_id)
                end
            end
            ::next_tool_result::
        end
    end

    return control_responses, control_delegations, task_complete, final_result
end

-- Commits each call's observation as soon as the call finishes, so a restart
-- re-runs only the calls still in flight. A result carrying a control
-- directive stays with the after-batch pass, which applies directives in model
-- order. In a round with an exit call a valid finish drops sibling
-- observations, so only argument failures, which that rule keeps, commit early.
-- Returns the listener, committed call ids, and calls with queued visualization results.
local function tool_result_persister(n, iteration, tool_calls: {ToolCall}, has_exit_call: boolean,
                                     tool_call_to_node_id, show_tool_calls)
    local calls_by_id = {}
    for _, tool_call in ipairs(tool_calls) do
        calls_by_id[tool_call.id] = tool_call
    end
    local persisted: {[string]: boolean} = {}
    local finished_viz_calls: {[string]: boolean} = {}

    local function listener(call_id, result)
        local tool_call = calls_by_id[call_id]
        if not tool_call or (has_exit_call and not tool_call.arguments_error) then
            return true, nil
        end
        local result_data = call_result(tool_call, result)
        if type(result_data.result) == "table" and result_data.result._control then
            return true, nil
        end
        if show_tool_calls then
            update_tool_viz_nodes(n, { [call_id] = result }, tool_call_to_node_id)
            finished_viz_calls[call_id] = true
        end
        record_tool_observation(n, iteration, tool_call, result_data.result, result_data.error, tool_call_to_node_id)
        local pending = (n:metadata() or {}).memory_pending
        if type(pending) == "table" and pending.iteration == iteration and (pending.by_call or {})[call_id] then
            local by_call = {}
            for pending_call_id, items in pairs(pending.by_call) do
                if pending_call_id ~= call_id then by_call[pending_call_id] = items end
            end
            n:update_metadata({ memory_pending = { iteration = iteration, by_call = by_call } })
        end
        local submitted, submit_err = n:submit()
        if not submitted then
            return nil, error_message(submit_err, "unknown")
        end
        persisted[call_id] = true
        return true, nil
    end

    return listener, persisted, finished_viz_calls
end

local function tools_were_attempted(agent_result, exit_tool_name: string?)
    for _, tool_call in ipairs(agent_result.tool_calls or {}) do
        if not exit_tool_name or tool_call.name ~= exit_tool_name then return true end
    end

    if agent_result.delegate_calls and #agent_result.delegate_calls > 0 then
        return true
    end

    return false
end

-- A result completes the task only when it carries content: text needs a
-- non-whitespace character, a structured table needs at least one field, and
-- nil/false are the model declining to answer. Field-level validation of a
-- structured result belongs to the exit schema.
local function has_usable_result(result)
    if result == nil or result == false then
        return false
    end
    if type(result) == "string" then
        return result:match("%S") ~= nil
    end
    if type(result) == "table" then
        return next(result) ~= nil
    end
    return true
end

local function completion_decision(tool_calling, agent_result: any, iteration, min_iterations, exit_tool_name)
    -- A turn that gets no feedback leaves the next request ending on the assistant.
    if iteration < min_iterations then
        if tools_were_attempted(agent_result) then
            return false, nil, nil, nil
        end
        return false, nil, agent_consts.FEEDBACK.CONTINUE_WORKING, "_continue_working"
    end

    if tool_calling == agent_consts.TOOL_CALLING.NONE then
        if has_usable_result(agent_result.result) then
            return true, agent_result.result, nil, nil
        end
        -- Empty text stores no assistant message, so only other unusable text needs this.
        if agent_result.result ~= nil and agent_result.result ~= "" then
            return false, nil, agent_consts.FEEDBACK.EMPTY_RESULT, "_empty_result"
        end
    elseif tool_calling == agent_consts.TOOL_CALLING.AUTO then
        if not tools_were_attempted(agent_result) then
            if has_usable_result(agent_result.result) then
                return true, agent_result.result, nil, nil
            end
            if agent_result.result ~= nil then
                return false, nil, agent_consts.FEEDBACK.EMPTY_RESULT, "_empty_result"
            end
            return false, nil, agent_consts.FEEDBACK.NO_TOOLS_CALLED, "_no_tools_called"
        end
    elseif tool_calling == agent_consts.TOOL_CALLING.ANY then
        if not tools_were_attempted(agent_result) then
            local feedback = agent_consts.FEEDBACK.NO_TOOLS_CALLED
            if exit_tool_name then
                feedback = feedback .. " " .. string.format(agent_consts.FEEDBACK.EXIT_AVAILABLE, exit_tool_name)
            end
            return false, nil, feedback, "_no_tools_called"
        end
    end

    return false, nil, nil, nil
end

local function record_completion_feedback(n, iteration, feedback, key_suffix)
    n:data(agent_consts.DATA_TYPE.AGENT_OBSERVATION, feedback, {
        key = iteration .. key_suffix,
        content_type = consts.CONTENT_TYPE.TEXT,
        node_id = n.node_id,
        metadata = { iteration = iteration }
    })
end

-- Returns (task_complete, final_result, feedback_recorded). feedback_recorded is
-- true when an observation asking the model for a real answer was queued.
local function check_completion(tool_calling, agent_result: any, iteration, min_iterations, exit_tool_name, n)
    local task_complete, final_result, feedback, key_suffix = completion_decision(tool_calling, agent_result,
        iteration, min_iterations, exit_tool_name)
    if feedback then record_completion_feedback(n, iteration, feedback, key_suffix) end
    return task_complete, final_result, feedback ~= nil
end

-- Exit calls settle the task only after validation. An accepted exit completes
-- before the stall check; a rejected exit-only turn consumes the allowance.
local function is_unproductive_turn(tool_calling, agent_result: any, iteration, min_iterations, exit_tool_name: string?)
    if iteration < min_iterations or tools_were_attempted(agent_result, exit_tool_name) then
        return false
    end
    if tools_were_attempted(agent_result) then return true end

    if tool_calling == agent_consts.TOOL_CALLING.NONE or tool_calling == agent_consts.TOOL_CALLING.AUTO then
        return not has_usable_result(agent_result.result)
    end

    return tool_calling == agent_consts.TOOL_CALLING.ANY
end

local function action_call_ids(tool_calls): {string}
    local ids = {}
    for _, tool_call in ipairs(tool_calls or {}) do table.insert(ids, tool_call.id) end
    return ids
end

-- The round's memory additions in the action's call order, and the same additions by
-- call id. A call that ran again replaces whatever it added on its earlier run.
local function round_memory_additions(n, iteration, agent_result: any, control_responses): ({any}, {[string]: any})
    local by_call = {}
    local pending = (n:metadata() or {}).memory_pending
    if type(pending) == "table" and pending.iteration == iteration then
        for call_id, items in pairs(pending.by_call or {}) do by_call[call_id] = items end
    end
    for _, tool_call in ipairs(agent_result.tool_calls or {}) do by_call[tool_call.id] = nil end
    for _, response in ipairs(control_responses) do
        local items = control_handler.memory_additions(response)
        if #items > 0 then by_call[response.tool_call_id] = items end
    end

    local additions = {}
    local function take(call_id)
        for _, item in ipairs(by_call[call_id] or {}) do table.insert(additions, item) end
    end
    local listed = {}
    for _, call_id in ipairs(agent_result.action_call_ids or {}) do
        listed[call_id] = true
        take(call_id)
    end
    for _, response in ipairs(control_responses) do
        if not listed[response.tool_call_id] then take(response.tool_call_id) end
    end
    return additions, by_call
end

local function finalize_iteration(n, agent_ctx, session_context, iteration, max_iterations, min_iterations, tool_calling,
                                  exit_tool_name, agent_result: any, delegate_calls: any, tool_results, arena_config,
                                  tool_call_to_node_id, proposed_controls, persisted_call_ids: {[string]: boolean}?)
    local calls_valid, calls_err = validate_tool_calls(agent_result and agent_result.tool_calls)
    if not calls_valid then
        return nil, nil, {
            code = agent_consts.ERROR.MALFORMED_TOOL_CALL,
            message = calls_err
        }
    end
    local prepared, prepare_err = behavior_controls.prepare(proposed_controls)
    if not prepared then return nil, nil, prepare_err end
    local control_responses, control_delegations, task_complete, final_result, results_err = process_tool_results(
        n,
        tool_results,
        iteration,
        exit_tool_name,
        agent_result,
        arena_config,
        session_context,
        tool_call_to_node_id,
        #prepared > 0,
        persisted_call_ids
    )
    if results_err then return nil, nil, results_err end

    append_control_delegations(delegate_calls, control_delegations)
    local has_delegations = #delegate_calls > 0

    -- Host text follows every result of the round; between a call and its result it
    -- makes the request invalid. Without a delegation every result is written here. A
    -- delegate's result comes later, so the additions wait in the node metadata.
    local additions, pending_by_call = round_memory_additions(n, iteration, agent_result, control_responses)
    local had_pending = type((n:metadata() or {}).memory_pending) == "table"
    if has_delegations then
        if #additions > 0 then
            n:update_metadata({ memory_pending = { iteration = iteration, by_call = pending_by_call } })
        elseif had_pending then
            n:update_metadata({ memory_pending = false })
        end
    else
        control_handler.queue_memory_additions(additions, n, iteration)
        queue_iteration_warning(n, iteration, max_iterations)
        n:update_metadata({ round_settled = iteration })
        if had_pending then n:update_metadata({ memory_pending = false }) end
    end
    if #prepared > 0 then n:update_metadata({ behavior_pending = { iteration = iteration, controls = prepared } }) end

    -- Everything above commits before anything waits, together with the results and
    -- any switch: the stored results no longer hold the directive, so recovery could
    -- not apply it again.
    if #control_responses > 0 then
        local changes_err = apply_control_response_commands(control_responses, agent_ctx, n, iteration)
        if changes_err then
            return nil, nil, changes_err
        end
    end

    local _, persist_err = n:yield()
    if persist_err then return nil, nil, persist_err end

    if has_delegations then
        local delegation_infos = delegation_handler.create_delegation_batch(
            { delegate_calls = delegate_calls },
            n,
            session_context,
            iteration
        )

        local delegation_results, delegation_err = delegation_handler.execute_delegation_batch(delegation_infos, n)
        if delegation_err then
            return nil, nil, delegation_err
        end

        delegation_handler.map_delegation_results_to_conversation(delegation_results, n, iteration)
        control_handler.queue_memory_additions(additions, n, iteration)
        queue_iteration_warning(n, iteration, max_iterations)
        n:update_metadata({ round_settled = iteration })
        if #additions > 0 then n:update_metadata({ memory_pending = false }) end
        local _, delegation_persist_err = n:yield()
        if delegation_persist_err then return nil, nil, delegation_persist_err end
    end

    -- Command output is host text, so it follows every call's result, including delegates.
    local commands_err = resume_pending_control_commands(n, iteration)
    if commands_err then return nil, nil, commands_err end

    if #prepared > 0 then
        local applied, control_err = control_handler.apply_behavior_controls(prepared, agent_ctx, n, iteration)
        if not applied then return nil, nil, control_err end
        n:update_metadata({ behavior_pending = false })
        local _, control_persist_err = n:yield()
        if control_persist_err then return nil, nil, control_persist_err end
    end

    if not task_complete and not has_delegations then
        local feedback_recorded
        task_complete, final_result, feedback_recorded = check_completion(tool_calling, agent_result, iteration,
            min_iterations, exit_tool_name, n)
        -- The feedback check_completion records has to be applied before the
        -- next prompt is built from persisted history, or the model only sees
        -- it one turn late. yield waits for the orchestrator to apply the commit;
        -- a bare submit only queues it.
        if feedback_recorded then
            local _, yield_err = n:yield()
            if yield_err then
                return nil, nil, yield_err
            end
        end
    end

    return task_complete, final_result, nil
end

-- A kill after the last per-call commit and before finalize's commit leaves a resolved
-- round with no iteration warning and no after_execute proposals. Replay is at-least-once,
-- as for every wrapper; a run stored without round_settled gets it once on resume.
-- A recovered call in the shape the caller validated it to, so after_execute sees what it
-- sees live. The context and provider data are those stored with the call.
local function validated_call(tool_call: any): table
    local validated = {
        call_id = tool_call.id,
        name = tool_call.name,
        args = tool_call.arguments,
        registry_id = tool_call.registry_id,
        context = tool_call.context,
        provider_metadata = tool_call.provider_metadata,
        meta = {},
        valid = false,
    }
    if tool_call.arguments_error then
        validated.error = "Failed to parse arguments: " .. tool_call.arguments_error
        return validated
    end
    local schema, schema_err = tools.get_tool_schema(tool_call.registry_id)
    if schema_err then
        validated.error = "Failed to get tool schema: " .. tostring(schema_err)
        return validated
    end
    validated.meta = schema and schema.meta or {}
    validated.valid = true
    return validated
end

local function settle_recovered_round(n, caller, agent_instance, action_payload, iteration, max_iterations,
                                      proposed_controls, agent_id, model_name, run_context_binding, exit_tool_name)
    local tool_calls = split_exit_tool_calls(action_payload.content.tool_calls, exit_tool_name)
    local calls_by_id = {}
    for _, tool_call in ipairs(tool_calls) do calls_by_id[tool_call.id] = tool_call end
    local tool_results = {}
    local observations = n:query()
        :with_nodes(n.node_id)
        :with_data_types(agent_consts.DATA_TYPE.AGENT_OBSERVATION)
        :all()
    for _, row in ipairs(observations or {}) do
        local metadata = row.metadata or {}
        local tool_call = calls_by_id[metadata.tool_call_id]
        if tool_call and metadata.iteration == iteration then
            local content = row.content
            if row.content_type == consts.CONTENT_TYPE.JSON then
                content = decode_json_content(row.content, row.content_type)
            end
            tool_results[tool_call.id] = {
                result = not metadata.is_error and content or nil,
                error = metadata.is_error and content or nil,
                tool_call = validated_call(tool_call),
            }
        end
    end
    configure_tool_wrappers(caller, agent_instance, n, agent_id, model_name, iteration, nil, run_context_binding)
    caller:apply_tool_wrappers(tool_caller.PHASE.AFTER_EXECUTE, {
        tool_calls = tool_calls,
        tool_results = tool_results,
    })
    local controls = {}
    for _, control in ipairs(proposed_controls) do controls[#controls + 1] = control end
    for _, control in ipairs(caller:get_wrapper_controls()) do controls[#controls + 1] = control end
    local prepared, prepare_err = behavior_controls.prepare(controls)
    if not prepared then return nil, prepare_err end

    local additions = round_memory_additions(n, iteration, {
        action_call_ids = action_call_ids(action_payload.content.tool_calls),
    }, {})
    control_handler.queue_memory_additions(additions, n, iteration)
    if type((n:metadata() or {}).memory_pending) == "table" then
        n:update_metadata({ memory_pending = false })
    end
    local stored_warning = n:query()
        :with_nodes(n.node_id)
        :with_data_types(agent_consts.DATA_TYPE.AGENT_OBSERVATION)
        :with_data_keys(iteration .. "_iterations_warning", iteration .. "_final_warning")
        :one()
    if not stored_warning then
        queue_iteration_warning(n, iteration, max_iterations)
    end
    n:update_metadata({ round_settled = iteration })
    if #prepared > 0 then
        n:update_metadata({ behavior_pending = { iteration = iteration, controls = prepared } })
    end
    local _, persist_err = n:yield()
    if persist_err then return nil, persist_err end
    return prepared, nil
end

local function recover_persisted_action(n, agent_ctx, agent_instance, caller, session_context, config, iteration, max_iterations,
                                        min_iterations, tool_calling, exit_tool_name, show_tool_calls, agent_id, model_name)
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
        local warning = n:query()
            :with_nodes(n.node_id)
            :with_data_types(agent_consts.DATA_TYPE.AGENT_OBSERVATION)
            :with_data_keys(action_iteration .. "_truncation_warning")
            :one()
        if warning then return false, nil, action_iteration, nil end
    end

    local calls_valid, calls_err = validate_tool_calls(action_payload.content.tool_calls)
    if not calls_valid then
        return false, nil, action_iteration, {
            code = agent_consts.ERROR.MALFORMED_TOOL_CALL,
            message = calls_err
        }
    end

    local observed_tool_call_ids = load_observed_tool_call_ids(n, action_iteration)
    local unresolved_tool_calls, unresolved_delegate_calls = collect_unresolved_calls(action_payload, observed_tool_call_ids)

    local has_unresolved_work = (#unresolved_tool_calls > 0) or (#unresolved_delegate_calls > 0)
    local behavior_pending = (n:metadata() or {}).behavior_pending
    local proposed_controls = type(behavior_pending) == "table" and
        behavior_pending.iteration == action_iteration and behavior_pending.controls or {}
    if not has_unresolved_work then
        local resume_err = resume_pending_control_commands(n, action_iteration)
        if resume_err then
            return false, nil, action_iteration,
                "Failed to resume control commands: " .. error_message(resume_err, "unknown")
        end
        if tools_were_attempted(action_payload.content) and (n:metadata() or {}).round_settled ~= action_iteration then
            local settle_err
            proposed_controls, settle_err = settle_recovered_round(n, caller, agent_instance, action_payload,
                action_iteration, max_iterations, proposed_controls, agent_id, model_name,
                string_or_nil(config.run_context_binding), exit_tool_name)
            if settle_err then return false, nil, action_iteration, settle_err end
        end
        if #proposed_controls > 0 then
            local applied, control_err = control_handler.apply_behavior_controls(proposed_controls, agent_ctx, n, action_iteration)
            if not applied then return false, nil, action_iteration, control_err end
            n:update_metadata({ behavior_pending = false })
            local _, persist_err = n:yield()
            if persist_err then return false, nil, action_iteration, persist_err end
        end
        if not tools_were_attempted(action_payload.content) then
            local complete, result, feedback, key_suffix = completion_decision(tool_calling, action_payload.content,
                action_iteration, min_iterations, exit_tool_name)
            if feedback then
                local observation = n:query()
                    :with_nodes(n.node_id)
                    :with_data_types(agent_consts.DATA_TYPE.AGENT_OBSERVATION)
                    :with_data_keys(action_iteration .. key_suffix)
                    :one()
                if not observation then
                    record_completion_feedback(n, action_iteration, feedback, key_suffix)
                    local _, persist_err = n:yield()
                    if persist_err then return false, nil, action_iteration, persist_err end
                end
            end
            return complete, result, action_iteration, nil
        end
        return false, nil, action_iteration, nil
    end

    local executable_tool_calls, exit_tool_calls = split_exit_tool_calls(unresolved_tool_calls :: {ToolCall}, exit_tool_name)

    configure_tool_wrappers(caller, agent_instance, n, agent_id, model_name, action_iteration, nil, string_or_nil(config.run_context_binding))
    local validated_tools, effective_tool_calls, validate_err = prepare_tools({ tool_calls = executable_tool_calls }, caller)
    if validate_err then
        return false, nil, action_iteration, "Tool validation failed: " .. error_message(validate_err, "unknown")
    end

    local committed_node_ids = nil
    if show_tool_calls then
        local load_err
        committed_node_ids, load_err = load_tool_viz_nodes(n, action_iteration)
        if load_err then
            return false, nil, action_iteration, load_err
        end
    end
    local tool_call_to_node_id = create_tool_viz_nodes(n, effective_tool_calls, action_iteration, show_tool_calls,
        exit_tool_name, committed_node_ids)
    local result_listener, persisted_call_ids, finished_viz_calls = tool_result_persister(n, action_iteration, effective_tool_calls,
        #exit_tool_calls > 0, tool_call_to_node_id, show_tool_calls)
    local tool_results, execute_err = execute_tools(caller, session_context, validated_tools, result_listener)
    if execute_err then
        cancel_unfinished_tool_viz_nodes(n, finished_viz_calls, tool_call_to_node_id)
        return false, nil, action_iteration, execute_err
    end
    local combined_controls = {}
    for _, control in ipairs(proposed_controls) do combined_controls[#combined_controls + 1] = control end
    for _, control in ipairs(caller:get_wrapper_controls()) do combined_controls[#combined_controls + 1] = control end

    if show_tool_calls then
        update_tool_viz_nodes(n, tool_results, tool_call_to_node_id, persisted_call_ids)
    end

    local recovered_tool_calls = {} :: { ToolCall }
    for _, tool_call in ipairs(exit_tool_calls) do
        table.insert(recovered_tool_calls, tool_call)
    end
    for _, tool_call in ipairs(effective_tool_calls) do
        table.insert(recovered_tool_calls, tool_call)
    end

    local recovered_agent_result = {
        result = action_payload.content.result,
        tool_calls = recovered_tool_calls,
        delegate_calls = unresolved_delegate_calls,
        action_call_ids = action_call_ids(action_payload.content.tool_calls)
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
        config.arena,
        tool_call_to_node_id,
        combined_controls,
        persisted_call_ids
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
        return nil, inputs_or_err
    end

    if inputs_err then
        return nil, inputs_err
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
        if type(inputs_err) == "table" then
            return n:fail(inputs_err, error_message(inputs_err, "Failed to load inputs"))
        end
        return n:fail({
            code = agent_consts.ERROR.INPUT_VALIDATION_FAILED,
            message = inputs_err
        }, inputs_err)
    end

    local input_context, agent_id_override, model_override, max_iterations_override, input_data, input_err =
        process_multiple_inputs(inputs)
    if input_err then
        return n:fail({
            code = agent_consts.ERROR.INPUT_VALIDATION_FAILED,
            message = input_err
        }, input_err)
    end

    local saved_state = ((args.node or {}).metadata or {}).state or {}

    -- Inputs choose the agent and model until the node's first commit, which stores
    -- that choice and the input pin; after it the config holds the selection, including
    -- any switch. A run stored before the pin stores the model it was using now, unless
    -- its config has moved on.
    local pinned_input = load_pinned_input(n)
    if saved_state.agent_id == nil then
        if agent_id_override then
            n:update_config({ agent = agent_id_override })

            n:update_metadata({ title = agent_ref.resolve(agent_id_override).title })
        end
        if model_override then
            n:update_config({ model = model_override })
        end
    else
        agent_id_override = nil
        if pinned_input then
            model_override = config.model
        else
            -- The state is older than a switch the config already holds.
            local config_agent = config.agent
            if type(config_agent) == "table" then
                config_agent = config_agent.id or config_agent.name
            end
            if saved_state.agent_id ~= config_agent or (not model_override and config.model) then
                model_override = config.model
            else
                model_override = saved_state.model
                if model_override then
                    n:update_config({ model = model_override })
                end
            end
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

    -- Reapply persisted active trait/tool overlays so they survive a re-run/recovery.
    -- A prior _control directive stored these in node config (active_traits/active_tools);
    -- agent/model recover through agent_to_load/model_override above.
    -- Only a list overlay is applied; `false` is the cleared marker written on an agent
    -- switch (config can't drop a key), meaning "use the new agent's own traits/tools".
    if type(config.active_traits) == "table" then
        agent_ctx:set_active_traits(config.active_traits)
    end
    if type(config.active_tools) == "table" then
        agent_ctx:set_active_tools(config.active_tools)
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

    -- An empty fold cannot be a user turn; the arena prompt is the node's instruction.
    local first_message = input_data
    if first_message == "" and type(config.arena.prompt) == "string" and config.arena.prompt:match("%S") then
        first_message = config.arena.prompt
    end
    builder:with_arena_config(config.arena):with_initial_input(pin_conversation_input(n, pinned_input, first_message))
    -- A resumed stored run has sent this input already: pin it before anything else
    -- happens on this resume, so a later input cannot change it.
    if not pinned_input and saved_state.agent_id ~= nil then
        local _, pin_err = n:yield()
        if pin_err then
            return n:fail({
                code = agent_consts.ERROR.AGENT_EXEC_FAILED,
                message = error_message(pin_err, "Failed to store the conversation input")
            }, error_message(pin_err, "Failed to store the conversation input"))
        end
    end
    local caller = tool_caller.new()
    -- The tool calls of one model turn run concurrently, as in the session host.
    caller:set_strategy(tool_caller.STRATEGY.PARALLEL)

    local iteration = saved_state.current_iteration or 0
    local max_iterations = max_iterations_override or config.arena.max_iterations or agent_consts.DEFAULTS.MAX_ITERATIONS
    local min_iterations = config.arena.min_iterations or agent_consts.DEFAULTS.MIN_ITERATIONS
    local max_unproductive_steps = config.arena.max_unproductive_steps or agent_consts.DEFAULTS.MAX_UNPRODUCTIVE_STEPS
    local tool_calling = config.arena.tool_calling
    local show_tool_calls = config.show_tool_calls ~= false
    local task_complete = false
    local final_result = nil

    local total_tokens = new_total_tokens(saved_state.total_tokens)
    local tool_calls_count = saved_state.tool_calls or 0
    -- The run of consecutive unproductive turns is node state: a resumed node
    -- continues the run it was in rather than starting a fresh allowance.
    local unproductive_steps = saved_state.unproductive_steps or 0
    local route_pin = route_pin_after_switch(saved_state.route_pin, saved_state.agent_id, saved_state.model,
        agent_id, model_name)
    local pending_checkpoint_history = {}
    local lifecycle_state = {
        active_agent_id = nil,
        active_model = nil,
        active_agent = nil,
    }

    local function dispatch_lifecycle(active_agent: any, phase: string, payload: any): (table?, string?)
        return apply_agent_lifecycle(
            active_agent,
            phase,
            n,
            payload.agent_id,
            payload.model_name,
            payload.iteration,
            config.run_context_binding,
            payload.options :: table?
        )
    end

    local function deactivate_active_agent(reason: string?, outcome: table?, active_iteration: number?)
        if not lifecycle_state.active_agent_id then
            return nil
        end

        local _, lifecycle_err = lifecycle_controller.deactivate(lifecycle_state, {
            dispatch = dispatch_lifecycle,
            payload = function(_phase: string, descriptor: table): table
                return {
                    agent_id = descriptor.id,
                    model_name = descriptor.model,
                    iteration = active_iteration or iteration,
                    options = {
                        reason = reason or REASON.DATAFLOW_FINISHED,
                        outcome = outcome or {
                            state = OUTCOME.COMPLETED,
                            reason = reason or REASON.DATAFLOW_FINISHED
                        }
                    }
                }
            end
        })
        return lifecycle_err
    end

    local function activate_current_agent(active_iteration: number, refs: table?): (table?, string?)
        local result, lifecycle_err = lifecycle_controller.activate(lifecycle_state, {
            id = agent_id,
            model = model_name,
            agent = agent_instance,
            -- A _control trait overlay can change lifecycle bindings without
            -- changing the agent ID or model. The controller snapshots this
            -- value so a recompiled but unchanged overlay does not reactivate.
            variant = agent_ctx.active_traits,
        }, {
            dispatch = dispatch_lifecycle,
            payload = function(phase: string, descriptor: table): table
                local switching = phase == lifecycle_runtime.PHASE.DEACTIVATE
                local lifecycle_reason = switching and REASON.AGENT_SWITCH or REASON.AGENT_LOADED
                local options = {
                    reason = lifecycle_reason,
                    outcome = {
                        state = OUTCOME.CONTINUES,
                        reason = lifecycle_reason
                    }
                }
                if not switching then
                    options.refs = refs
                end
                return {
                    agent_id = descriptor.id,
                    model_name = descriptor.model,
                    iteration = active_iteration,
                    options = options
                }
            end
        })
        return (result and result.activation) :: table?, lifecycle_err
    end

    local function fail_with_lifecycle(payload: table, message: any, reason: string?, active_iteration: number?)
        local lifecycle_err = deactivate_active_agent(reason or REASON.HOST_FAILED, {
            state = OUTCOME.FAILED,
            reason = reason or REASON.HOST_FAILED
        }, active_iteration)
        if lifecycle_err and type(payload) == "table" then
            payload.lifecycle_error = lifecycle_err
        end
        return n:fail(payload, message)
    end

    -- Dispatches after_persist for the committed action row. Returns a failure
    -- payload and message when the row cannot be resolved or a strict binding fails.
    local function apply_after_persist(action_key: string, agent_result: any, active_iteration: number): (table?, string?)
        local event_id, lookup_err = persisted_action_id(n, action_key)
        if not event_id then
            return { code = agent_consts.ERROR.AGENT_EXEC_FAILED, message = lookup_err }, lookup_err
        end
        local _, persist_err = apply_agent_lifecycle(
            agent_instance,
            lifecycle_runtime.PHASE.AFTER_PERSIST,
            n,
            agent_id,
            model_name,
            active_iteration,
            config.run_context_binding,
            {
                reason = REASON.AGENT_STEP,
                refs = {
                    iteration = active_iteration,
                    event_id = event_id
                },
                outcome = lifecycle_outcome_from_agent_result(agent_result)
            }
        )
        if persist_err then
            return { code = agent_consts.ERROR.AGENT_EXEC_FAILED, message = persist_err }, persist_err
        end
        return nil, nil
    end

    local function fail_unproductive()
        local stalled_status = build_status_message(iteration, max_iterations, total_tokens, tool_calls_count, true,
            false)
        update_node_progress(n, iteration, max_iterations, total_tokens, tool_calls_count, stalled_status, agent_id,
            model_name, unproductive_steps, route_pin)

        local stalled_message = string.format(agent_consts.ERROR_MSG.UNPRODUCTIVE_STEPS, unproductive_steps)
        return fail_with_lifecycle({
            code = agent_consts.ERROR.UNPRODUCTIVE_STEPS,
            message = stalled_message
        }, stalled_message, REASON.UNPRODUCTIVE_STEPS, iteration)
    end

    -- Reconcile the committed selection before admitting checkpoint providers,
    -- lifecycle hooks or another model step, including after proposal replay.
    local function refresh_current_agent()
        local refreshed = agent_ctx:get_current_agent()
        local current_config = n:config()
        if not refreshed then
            local load_err
            local reload_model = current_config.model or agent_ctx.current_model or model_name
            refreshed, load_err = agent_ctx:load_agent(current_config.agent or agent_id,
                reload_model and { model = reload_model } or nil)
            if not refreshed then return nil, load_err or "Failed to reload selected agent" end
        end
        local previous_agent_id, previous_model = agent_id, model_name
        local refreshed_config = agent_ctx:get_config()
        agent_instance = refreshed
        agent_id = refreshed_config.current_agent_id or agent_id
        model_name = refreshed_config.current_model or model_name
        route_pin = route_pin_after_switch(route_pin, previous_agent_id, previous_model, agent_id, model_name)
        config = current_config
        return true
    end

    local function flush_requested_checkpoint(completing)
        if (n:metadata() or {}).checkpoint_requested ~= true then return true end
        -- A proposal may switch agent/model/overlays before requesting compact.
        -- Resolve the committed target here, as recovery does at node startup,
        -- rather than compacting with the stale agent that produced the round.
        local refreshed, refresh_err = refresh_current_agent()
        if not refreshed then return nil, refresh_err end
        local options = resolve_agent_checkpoint_config(config, agent_instance)
        local marker, checkpoint_err, strict_err = maybe_checkpoint_history(n, {
            checkpoint = options, run_context_binding = config.run_context_binding,
        }, context_with_agent_run(session_context, n, agent_id, model_name, iteration,
            config.run_context_binding), agent_instance, agent_id, model_name, iteration, completing)
        if checkpoint_err then
            -- No request follows a completing node, so strict has nothing left to protect.
            if not completing and (strict_err or (options and options.strict == true)) then
                return nil, checkpoint_err
            end
            record_checkpoint_skip(n, iteration, checkpoint_err)
        end
        -- A completing node commits the marker with its completion, so no restart can run
        -- the finish again against a cut that holds its call.
        if not completing and (marker or checkpoint_err or (n:metadata() or {}).checkpoint_requested ~= true) then
            local _, persist_err = n:yield()
            if persist_err then return nil, persist_err end
        end
        return true
    end

    local initial_status = build_status_message(iteration, max_iterations, total_tokens, tool_calls_count, false, false)
    update_node_progress(n, iteration, max_iterations, total_tokens, tool_calls_count, initial_status, agent_id,
        model_name, unproductive_steps, route_pin)

    local recovered_complete, recovered_result, recovered_iteration, recovery_err = recover_persisted_action(
        n,
        agent_ctx,
        agent_instance,
        caller,
        context_with_agent_run(session_context, n, agent_id, model_name, iteration, config.run_context_binding),
        config,
        iteration,
        max_iterations,
        min_iterations,
        tool_calling,
        exit_tool_name,
        show_tool_calls,
        agent_id,
        model_name
    )
    if recovery_err then
        if type(recovery_err) == "table" then
            return fail_with_lifecycle(recovery_err, error_message(recovery_err, "Agent recovery failed"),
                REASON.HOST_FAILED, iteration)
        end
        return fail_with_lifecycle({
            code = agent_consts.ERROR.AGENT_EXEC_FAILED,
            message = recovery_err
        }, recovery_err, REASON.HOST_FAILED, iteration)
    end
    if recovered_iteration and recovered_iteration > iteration then
        iteration = recovered_iteration
    end
    local recovered_agent_ok, recovered_agent_err = refresh_current_agent()
    if not recovered_agent_ok then
        return fail_with_lifecycle({ code = agent_consts.ERROR.AGENT_LOAD_FAILED, message = recovered_agent_err },
            recovered_agent_err, REASON.HOST_FAILED, iteration)
    end
    local recovered_checkpoint_ok, recovered_checkpoint_err = flush_requested_checkpoint(recovered_complete == true)
    if not recovered_checkpoint_ok then
        local checkpoint_message = recovered_checkpoint_err or "Requested checkpoint failed"
        return fail_with_lifecycle({ code = agent_consts.ERROR.CHECKPOINT_FAILED, message = checkpoint_message },
            checkpoint_message, REASON.HOST_FAILED, iteration)
    end
    if recovered_complete then
        task_complete = true
        final_result = recovered_result
    end

    -- A node that persisted the turn exhausting its allowance and restarted
    -- before failing resolves the same way it would have without the restart.
    if not task_complete and unproductive_steps >= max_unproductive_steps then
        return fail_unproductive()
    end

    -- A run past its first iteration already sent its input, with or without a note.
    if iteration == 0 and not task_complete then
        local turn_context_err = store_turn_context(n, agent_instance)
        if turn_context_err then
            local turn_context_message = error_message(turn_context_err, "Failed to store turn context")
            return fail_with_lifecycle({
                code = agent_consts.ERROR.AGENT_EXEC_FAILED,
                message = turn_context_message
            }, turn_context_message, REASON.HOST_FAILED, iteration)
        end
    end

    while iteration < max_iterations and not task_complete do
        iteration = iteration + 1
        local run_session_context = context_with_agent_run(session_context, n, agent_id, model_name, iteration,
            config.run_context_binding)

        local effective_checkpoint = resolve_agent_checkpoint_config(config, agent_instance)
        local pending_checkpoint_marker, checkpoint_err, strict_checkpoint_err = maybe_checkpoint_history(n, {
            checkpoint = effective_checkpoint,
            run_context_binding = config.run_context_binding
        }, run_session_context, agent_instance, agent_id, model_name, iteration)
        if checkpoint_err then
            local strict_mode = strict_checkpoint_err == true or (effective_checkpoint and effective_checkpoint.strict == true)
            if strict_mode then
                return fail_with_lifecycle({
                    code = agent_consts.ERROR.CHECKPOINT_FAILED,
                    message = checkpoint_err
                }, checkpoint_err, REASON.HOST_FAILED, iteration)
            end
            -- warning mode: log audit observation and continue the turn
            record_checkpoint_skip(n, iteration, checkpoint_err)
            pending_checkpoint_history = {}
        elseif pending_checkpoint_marker then
            pending_checkpoint_history = { pending_checkpoint_marker }
        else
            pending_checkpoint_history = {}
        end
        builder:with_pending_history(pending_checkpoint_history)

        local prompt, prompt_err = builder:build_prompt(config.arena.prompt)
        if prompt_err then
            return fail_with_lifecycle({
                code = agent_consts.ERROR.PROMPT_BUILD_FAILED,
                message = prompt_err
            }, prompt_err, REASON.HOST_FAILED, iteration)
        end

        local activate_result, activate_err = activate_current_agent(iteration, {
            iteration = iteration
        })
        if activate_err then
            return fail_with_lifecycle({
                code = agent_consts.ERROR.AGENT_EXEC_FAILED,
                message = activate_err
            }, activate_err, REASON.HOST_FAILED, iteration)
        end
        local step_notes, activate_notes_err = lifecycle_notes(activate_result, lifecycle_runtime.PHASE.ACTIVATE, agent_id)
        if not step_notes then
            return fail_with_lifecycle({
                code = agent_consts.ERROR.AGENT_EXEC_FAILED,
                message = activate_notes_err
            }, activate_notes_err, REASON.HOST_FAILED, iteration)
        end

        local before_result, before_err = apply_agent_lifecycle(
            agent_instance,
            lifecycle_runtime.PHASE.BEFORE_STEP,
            n,
            agent_id,
            model_name,
            iteration,
            config.run_context_binding,
            {
                reason = REASON.AGENT_STEP,
                refs = {
                    iteration = iteration
                },
                outcome = {
                    state = OUTCOME.CONTINUES,
                    reason = REASON.AGENT_STEP
                }
            }
        )
        if before_err then
            return fail_with_lifecycle({
                code = agent_consts.ERROR.AGENT_EXEC_FAILED,
                message = before_err
            }, before_err, REASON.HOST_FAILED, iteration)
        end
        local before_notes, before_notes_err = lifecycle_notes(before_result, lifecycle_runtime.PHASE.BEFORE_STEP, agent_id)
        if not before_notes then
            return fail_with_lifecycle({
                code = agent_consts.ERROR.AGENT_EXEC_FAILED,
                message = before_notes_err
            }, before_notes_err, REASON.HOST_FAILED, iteration)
        end
        for _, note in ipairs(before_notes) do table.insert(step_notes, note) end
        append_step_notes(prompt, step_notes)

        -- The system text is not a turn, and Claude and Bedrock reject an empty one.
        if not has_conversation(prompt) then
            local nothing_message = "Agent node has nothing to send: no conversation input, note or history"
            return fail_with_lifecycle({
                code = agent_consts.ERROR.INPUT_VALIDATION_FAILED,
                message = nothing_message
            }, nothing_message, REASON.HOST_FAILED, iteration)
        end

        local step_options = {
            tool_call = tool_calling,
            context = run_session_context,
            drop_stale_thinking = true
        }
        -- "any" is enforced here: the arena answers a text-only turn with feedback
        -- and completes only through the finish tool. A model that cannot be
        -- forced may therefore be asked with "auto" (see the llm model_profile).
        if tool_calling == agent_consts.TOOL_CALLING.ANY then
            step_options.tool_call_fallback = "auto"
        end
        if route_pin ~= nil then
            step_options.route = route_pin
        end
        local step_started = time.now():unix_nano()
        local agent_result, step_err, step_details = agent_instance:step(prompt, step_options)
        local step_latency_ms = math.floor((time.now():unix_nano() - step_started) / 1000000)
        local memory_recall_error = type(agent_result) == "table" and agent_result.memory_recall_error or nil
        if step_err then
            local cause = retry.extract_error(step_err)
            local details = type(step_details) == "table" and step_details or {}
            local native_details = type(cause.details) == "table" and cause.details or {}
            local outcome = type(agent_result) == "table" and agent_result or details
            local tokens = nil
            if type(outcome.tokens) == "table" then
                tokens = {}
                for key, value in pairs(outcome.tokens) do
                    if type(key) == "string" and type(value) == "number" and value == value
                        and value ~= math.huge and value ~= -math.huge then
                        tokens[key] = value
                    end
                end
            end
            local code = details.code or native_details.code
            local _, record_err = n:persist_data(agent_consts.DATA_TYPE.AGENT_ERROR, {message = tostring(step_err)}, {
                key = iteration .. "_error", content_type = consts.CONTENT_TYPE.JSON, node_id = n.node_id,
                metadata = {iteration = iteration, agent_id = agent_id, model = model_name,
                    tokens = tokens,
                    finish_reason = type(outcome.finish_reason) == "string" and outcome.finish_reason or nil,
                    latency_ms = step_latency_ms, error_code = agent_consts.ERROR.AGENT_EXEC_FAILED,
                    error_kind = cause.kind, retryable = cause.retryable,
                    code = (type(code) == "string" or type(code) == "number") and code or nil}
            })
            if record_err then
                logger:error("Failed to persist agent step failure", {error = tostring(record_err)})
            end
            return fail_with_lifecycle({
                code = agent_consts.ERROR.AGENT_EXEC_FAILED,
                message = step_err,
                memory_recall_error = memory_recall_error
            }, step_err, REASON.HOST_FAILED, iteration)
        end

        local calls_valid, calls_err = validate_tool_calls(agent_result and agent_result.tool_calls)
        if not calls_valid then
            return fail_with_lifecycle({
                code = agent_consts.ERROR.MALFORMED_TOOL_CALL,
                message = calls_err
            }, calls_err, REASON.HOST_FAILED, iteration)
        end
        route_pin = carry_route_pin(route_pin, agent_result)

        local sent_notes, sent_notes_err = context_notes(agent_result)
        if not sent_notes then
            return fail_with_lifecycle({
                code = agent_consts.ERROR.AGENT_EXEC_FAILED,
                message = sent_notes_err
            }, sent_notes_err, REASON.HOST_FAILED, iteration)
        end
        for _, note in ipairs(sent_notes) do table.insert(step_notes, note) end

        local after_result, after_err = apply_agent_lifecycle(
            agent_instance,
            lifecycle_runtime.PHASE.AFTER_STEP,
            n,
            agent_id,
            model_name,
            iteration,
            config.run_context_binding,
            {
                reason = REASON.AGENT_STEP,
                refs = {
                    iteration = iteration
                },
                outcome = lifecycle_outcome_from_agent_result(agent_result)
            }
        )
        if after_err then
            return fail_with_lifecycle({
                code = agent_consts.ERROR.AGENT_EXEC_FAILED,
                message = after_err
            }, after_err, REASON.HOST_FAILED, iteration)
        end

        if agent_result.truncated then
            total_tokens = accumulate_tokens(total_tokens, agent_result.tokens)
            -- A truncated turn overran the token budget: the model spoke, so the
            -- consecutive unproductive run ends here.
            unproductive_steps = 0

            local status_msg = build_status_message(iteration, max_iterations, total_tokens, tool_calls_count, false, false)
            update_node_progress(n, iteration, max_iterations, total_tokens, tool_calls_count, status_msg, agent_id,
                model_name, unproductive_steps, route_pin)

            store_step_notes(n, step_notes, iteration)
            local truncated_action_key = store_agent_action(n, agent_result, iteration, agent_id, model_name,
                exit_tool_name, {}, step_latency_ms)

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
                if type(yield_err) == "table" then
                    return fail_with_lifecycle(yield_err, error_message(yield_err, "Failed to persist truncated agent turn"),
                        REASON.HOST_FAILED, iteration)
                end
                local yield_message = "Failed to persist truncated agent turn: " .. error_message(yield_err, "unknown")
                return fail_with_lifecycle({
                    code = agent_consts.ERROR.AGENT_EXEC_FAILED,
                    message = yield_message
                }, yield_message, REASON.HOST_FAILED, iteration)
            end
            if lifecycle_runtime.subscribed(agent_instance.bindings, lifecycle_runtime.PHASE.AFTER_PERSIST) then
                local persist_payload, persist_message = apply_after_persist(truncated_action_key, agent_result, iteration)
                if persist_payload then
                    return fail_with_lifecycle(persist_payload, persist_message or "after_persist failed",
                        REASON.HOST_FAILED, iteration)
                end
            end
            pending_checkpoint_history = {}
            builder:with_pending_history(pending_checkpoint_history)
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

        if is_unproductive_turn(tool_calling, agent_result, iteration, min_iterations, exit_tool_name) then
            unproductive_steps = unproductive_steps + 1
        else
            unproductive_steps = 0
        end

        local status_msg = build_status_message(iteration, max_iterations, total_tokens, tool_calls_count, false, false)
        update_node_progress(n, iteration, max_iterations, total_tokens, tool_calls_count, status_msg, agent_id,
            model_name, unproductive_steps, route_pin)

        store_step_notes(n, step_notes, iteration)
        local action_key = store_agent_action(n, agent_result, iteration, agent_id, model_name, exit_tool_name, {}, step_latency_ms)
        local proposed_controls = {}
        for _, summary in ipairs({ before_result or {}, after_result or {} }) do
            for _, control in ipairs(summary.controls or {}) do proposed_controls[#proposed_controls + 1] = control end
        end
        if #proposed_controls > 0 then
            n:update_metadata({ behavior_pending = { iteration = iteration, controls = proposed_controls } })
        end

        local recovery_hooks = config.recovery_test_hooks or {}
        if recovery_hooks.pre_action_submit_delay_ms and recovery_hooks.pre_action_submit_delay_ms > 0 then
            time.sleep(tostring(recovery_hooks.pre_action_submit_delay_ms) .. "ms")
        end

        -- after_persist needs the turn applied and readable, which only an
        -- acknowledged yield guarantees; a bare submit only queues the commit.
        local persist_subscribed = lifecycle_runtime.subscribed(agent_instance.bindings,
            lifecycle_runtime.PHASE.AFTER_PERSIST)
        local submit_ok, submit_err
        if persist_subscribed then
            local _, yield_err = n:yield()
            submit_ok, submit_err = yield_err == nil, yield_err
        else
            submit_ok, submit_err = n:submit()
        end
        if not submit_ok then
            if type(submit_err) == "table" then
                return fail_with_lifecycle(submit_err, error_message(submit_err, "Failed to persist agent turn"),
                    REASON.HOST_FAILED, iteration)
            end
            local submit_message = "Failed to persist agent turn: " .. error_message(submit_err, "unknown")
            return fail_with_lifecycle({
                code = agent_consts.ERROR.AGENT_EXEC_FAILED,
                message = submit_message
            }, submit_message, REASON.HOST_FAILED, iteration)
        end
        if persist_subscribed then
            local persist_payload, persist_message = apply_after_persist(action_key, agent_result, iteration)
            if persist_payload then
                return fail_with_lifecycle(persist_payload, persist_message or "after_persist failed",
                    REASON.HOST_FAILED, iteration)
            end
        end
        pending_checkpoint_history = {}
        builder:with_pending_history(pending_checkpoint_history)

        local executable_tool_calls, exit_tool_calls = split_exit_tool_calls(regular_tool_calls, exit_tool_name)

        configure_tool_wrappers(caller, agent_instance, n, agent_id, model_name, iteration, nil, string_or_nil(config.run_context_binding))
        local validated_tools, effective_tool_calls, validate_err = prepare_tools({ tool_calls = executable_tool_calls }, caller)
        if validate_err then
            if type(validate_err) == "table" then
                return fail_with_lifecycle(validate_err, error_message(validate_err, "Tool validation failed"),
                    REASON.HOST_FAILED, iteration)
            end
            local validation_message = "Tool validation failed: " .. error_message(validate_err, "unknown")
            return fail_with_lifecycle({
                code = agent_consts.ERROR.AGENT_EXEC_FAILED,
                message = validation_message
            }, validation_message, REASON.HOST_FAILED, iteration)
        end

        local tool_call_to_node_id = create_tool_viz_nodes(n, effective_tool_calls, iteration, show_tool_calls,
            exit_tool_name)
        local result_listener, persisted_call_ids, finished_viz_calls = tool_result_persister(n, iteration, effective_tool_calls,
            #exit_tool_calls > 0, tool_call_to_node_id, show_tool_calls)
        local tool_results, execute_err = execute_tools(caller, run_session_context, validated_tools, result_listener)
        if execute_err then
            cancel_unfinished_tool_viz_nodes(n, finished_viz_calls, tool_call_to_node_id)
            return fail_with_lifecycle({
                code = agent_consts.ERROR.AGENT_EXEC_FAILED,
                message = execute_err
            }, execute_err, REASON.HOST_FAILED, iteration)
        end
        for _, control in ipairs(caller:get_wrapper_controls()) do proposed_controls[#proposed_controls + 1] = control end

        if show_tool_calls then
            update_tool_viz_nodes(n, tool_results, tool_call_to_node_id, persisted_call_ids)
        end

        local finalized_tool_calls = {} :: { ToolCall }
        for _, tool_call in ipairs(exit_tool_calls) do
            table.insert(finalized_tool_calls, tool_call)
        end
        for _, tool_call in ipairs(effective_tool_calls) do
            table.insert(finalized_tool_calls, tool_call)
        end

        local finalized_complete, finalized_result, finalize_err = finalize_iteration(
            n,
            agent_ctx,
            run_session_context,
            iteration,
            max_iterations,
            min_iterations,
            tool_calling,
            exit_tool_name,
            {
                result = agent_result.result,
                tool_calls = finalized_tool_calls,
                delegate_calls = delegate_calls,
                action_call_ids = action_call_ids(regular_tool_calls)
            },
            delegate_calls,
            tool_results,
            config.arena,
            tool_call_to_node_id,
            proposed_controls,
            persisted_call_ids
        )
        if finalize_err then
            if type(finalize_err) == "table" then
                return fail_with_lifecycle(finalize_err, error_message(finalize_err, "Agent execution failed"),
                    REASON.HOST_FAILED, iteration)
            end
            return fail_with_lifecycle({
                code = agent_consts.ERROR.STEP_FUNCTION_FAILED,
                message = finalize_err
            }, finalize_err, REASON.HOST_FAILED, iteration)
        end

        if finalized_complete then
            task_complete = true
            final_result = finalized_result
        end
        local checkpoint_ok, requested_checkpoint_err = flush_requested_checkpoint(task_complete)
        if not checkpoint_ok then
            local checkpoint_message = requested_checkpoint_err or "Requested checkpoint failed"
            return fail_with_lifecycle({ code = agent_consts.ERROR.CHECKPOINT_FAILED, message = checkpoint_message },
                checkpoint_message, REASON.HOST_FAILED, iteration)
        end

        -- The unproductive turn has been answered with feedback above; once the
        -- model has stayed unproductive for the whole allowance the node fails
        -- with a named reason instead of spending the rest of the iteration
        -- budget on identical turns.
        if not task_complete and unproductive_steps >= max_unproductive_steps then
            return fail_unproductive()
        end

        if not task_complete then
            local refreshed, refresh_err = refresh_current_agent()
            if not refreshed then
                return fail_with_lifecycle({ code = agent_consts.ERROR.AGENT_LOAD_FAILED, message = refresh_err },
                    refresh_err, REASON.HOST_FAILED, iteration)
            end
        end

        ::continue_loop::
    end

    if not task_complete and iteration >= max_iterations then
        local final_status = build_status_message(iteration, max_iterations, total_tokens, tool_calls_count, true, false)
        update_node_progress(n, iteration, max_iterations, total_tokens, tool_calls_count, final_status, agent_id,
            model_name, unproductive_steps, route_pin)

        return fail_with_lifecycle({
            code = agent_consts.ERROR.AGENT_EXEC_FAILED,
            message = "Maximum iterations reached without completion"
        }, "Maximum iterations reached", REASON.MAX_ITERATIONS_REACHED, iteration)
    end

    local final_status = build_status_message(iteration, max_iterations, total_tokens, tool_calls_count, true,
        task_complete)
    update_node_progress(n, iteration, max_iterations, total_tokens, tool_calls_count, final_status, agent_id,
        model_name, unproductive_steps, route_pin)

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
    local lifecycle_err = deactivate_active_agent(REASON.DATAFLOW_FINISHED, {
        state = success and OUTCOME.COMPLETED or OUTCOME.FAILED,
        reason = success and REASON.NO_TOOLS_REQUIRED or REASON.HOST_FAILED
    }, iteration)
    if lifecycle_err then
        return n:fail({
            code = agent_consts.ERROR.AGENT_EXEC_FAILED,
            message = lifecycle_err
        }, lifecycle_err)
    end

    return (n:complete(output_content, message))
end

return {
    run = run,
    _test = {
        validate_and_resolve_config = validate_and_resolve_config,
        build_agent_context_config = build_agent_context_config,
        context_with_agent_run = context_with_agent_run,
        check_completion = check_completion,
        is_unproductive_turn = is_unproductive_turn,
        process_multiple_inputs = process_multiple_inputs,
        process_tool_results = process_tool_results,
        validate_tool_calls = validate_tool_calls,
        finalize_iteration = finalize_iteration,
        maybe_checkpoint_history = maybe_checkpoint_history,
        recover_persisted_action = recover_persisted_action,
        carry_route_pin = carry_route_pin,
        route_pin_after_switch = route_pin_after_switch,
        update_node_progress = update_node_progress,
    }
}
