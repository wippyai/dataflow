local prompt_lib = require("prompt")
local data_reader = require("data_reader")
local json = require("json")
local agent_consts = require("agent_consts")
local consts = require("consts")

local prompt_builder = {}
local mt = { __index = prompt_builder }

-- A row's own id and write position: a resolved reference row carries its target's
-- data_id, so the row it was stored as is in reference_row_id.
function prompt_builder.row_id(row)
    if type(row) ~= "table" then
        return ""
    end
    return tostring(row.reference_row_id or row.data_id or "")
end

-- UUID v7 order is write order, regardless of created_at resolution across backends.
function prompt_builder.sort_history_rows(history_rows)
    table.sort(history_rows, function(a, b)
        return prompt_builder.row_id(a) < prompt_builder.row_id(b)
    end)
    return history_rows
end

function prompt_builder.find_latest_checkpoint_marker(history_rows)
    for i = #history_rows, 1, -1 do
        local item = history_rows[i]
        if item.type == agent_consts.DATA_TYPE.AGENT_MEMORY
           and item.metadata
           and item.metadata.checkpoint_marker == true then
            return item
        end
    end

    return nil
end

-- The turn context note follows the initial input, which every request sends,
-- so it stays with the input when a checkpoint drops the rows around it.
function prompt_builder.is_turn_context_row(row)
    return type(row) == "table"
        and row.type == agent_consts.DATA_TYPE.AGENT_MEMORY
        and type(row.metadata) == "table"
        and row.metadata.turn_context == true
end

-- An audit row records what the host did; the model never received it.
function prompt_builder.is_audit_row(row)
    return type(row) == "table"
        and row.type == agent_consts.DATA_TYPE.AGENT_OBSERVATION
        and type(row.metadata) == "table"
        and row.metadata.checkpoint_skipped == true
end

-- The first max_chars characters of a text, each whole, and whether any were left out.
-- Counts the bytes that start a character; the runtime has no utf8 library. The walk
-- ends within max_chars * 4 bytes, the most that many characters can take, so its cost
-- follows the cap and bytes that start no character cannot pass it.
function prompt_builder.text_prefix(text, max_chars)
    local size = #text
    if size <= max_chars then
        return text, false
    end
    local window = math.min(size, max_chars * 4)
    local chars = 0
    for position = 1, window do
        local byte = string.byte(text, position)
        if byte < 128 or byte > 191 then
            chars = chars + 1
            if chars > max_chars then
                return text:sub(1, position - 1), true
            end
        end
    end
    if size > window then
        return text:sub(1, window), true
    end
    return text, false
end

-- Every request sends the pinned input and the note after it, whatever a checkpoint
-- cut around them.
function prompt_builder.is_kept_by_checkpoint(row)
    return prompt_builder.is_turn_context_row(row)
        or (type(row) == "table" and row.type == agent_consts.DATA_TYPE.AGENT_INPUT)
end

-- The checkpoint summary replaces every row up to the cut, so a request carries the
-- input, the turn note, the marker and the rows after the cut. No checkpoint runs
-- while an action has an unresolved call, so a cut never separates a call from its
-- result. The rows arrive sorted; the result is in request order.
function prompt_builder.apply_latest_marker(history_rows)
    if not history_rows or #history_rows == 0 then
        return history_rows or {}
    end

    local latest_marker = prompt_builder.find_latest_checkpoint_marker(history_rows)
    if not latest_marker then
        return history_rows
    end

    local cut_before = tostring((latest_marker.metadata or {}).checkpoint_before_data_id or "")
    local filtered = {}
    for _, item in ipairs(history_rows) do
        if prompt_builder.is_kept_by_checkpoint(item) then
            filtered[#filtered + 1] = item
        end
    end
    filtered[#filtered + 1] = latest_marker
    for _, item in ipairs(history_rows) do
        if item ~= latest_marker and not prompt_builder.is_kept_by_checkpoint(item)
            and prompt_builder.row_id(item) > cut_before then
            filtered[#filtered + 1] = item
        end
    end

    return filtered
end

-- The rows a new summary replaces: what requests carry since the latest cut, the
-- earlier summary included so it rolls forward. The input and the turn note stay in
-- every request, and an audit row was never in one.
function prompt_builder.checkpoint_source_rows(history_rows)
    local source_rows = {}
    for _, row in ipairs(prompt_builder.apply_latest_marker(history_rows)) do
        if not prompt_builder.is_kept_by_checkpoint(row) and not prompt_builder.is_audit_row(row) then
            source_rows[#source_rows + 1] = row
        end
    end
    return source_rows
end

local function clone_table(tbl)
    if type(tbl) ~= "table" then
        return nil
    end

    local copy = {}
    for key, value in pairs(tbl) do
        copy[key] = value
    end
    return copy
end

local function merge_provider_metadata(base_provider_metadata, override_provider_metadata)
    if type(base_provider_metadata) ~= "table" then
        return clone_table(override_provider_metadata)
    end

    if type(override_provider_metadata) ~= "table" then
        return clone_table(base_provider_metadata)
    end

    local merged_provider_metadata = clone_table(base_provider_metadata)
    for key, value in pairs(override_provider_metadata) do
        merged_provider_metadata[key] = value
    end

    return merged_provider_metadata
end

local function extract_message_meta(metadata, provider_metadata_override)
    if type(metadata) ~= "table" then
        if type(provider_metadata_override) == "table" then
            return {
                provider_metadata = clone_table(provider_metadata_override)
            }
        end
        return nil
    end

    local llm_meta = metadata.llm_meta
    local message_meta = nil
    if type(llm_meta) == "table" then
        message_meta = clone_table(llm_meta)
    end

    local resolved_provider_metadata = nil
    if type(message_meta) == "table" and type(message_meta.provider_metadata) == "table" then
        resolved_provider_metadata = clone_table(message_meta.provider_metadata)
    elseif type(metadata.provider_metadata) == "table" then
        resolved_provider_metadata = clone_table(metadata.provider_metadata)
    end

    if type(provider_metadata_override) == "table" then
        resolved_provider_metadata = merge_provider_metadata(
            resolved_provider_metadata,
            provider_metadata_override
        )
    end

    if type(message_meta) == "table" then
        if type(resolved_provider_metadata) == "table" then
            message_meta.provider_metadata = resolved_provider_metadata
        end
        return message_meta
    end

    if type(resolved_provider_metadata) == "table" then
        return {
            provider_metadata = resolved_provider_metadata
        }
    end

    return nil
end

-- A developer row keeps its own message: the builder merges adjacent messages of
-- one role, which would rewrite an earlier one when a turn stores no assistant row.
local function positional_meta(metadata)
    local message_meta = extract_message_meta(metadata) or {}
    message_meta.preserve_position = true
    return message_meta
end

function prompt_builder.new(dataflow_id, node_id, node_path)
    if not dataflow_id then
        return nil, "dataflow_id is required"
    end
    if not node_id then
        return nil, "node_id is required"
    end
    if not node_path then
        return nil, "node_path is required"
    end

    local self = setmetatable({}, mt)
    self.dataflow_id = dataflow_id
    self.node_id = node_id
    self.node_path = node_path
    self._arena_config = nil
    self._initial_input = nil
    self._pending_history = {}
    self._history_override = nil

    return self, nil
end

function prompt_builder:with_arena_config(arena_config)
    self._arena_config = arena_config
    return self
end

function prompt_builder:with_initial_input(initial_input)
    self._initial_input = initial_input
    return self
end

function prompt_builder:with_pending_history(pending_history)
    self._pending_history = pending_history or {}
    return self
end

function prompt_builder:with_history(history)
    self._history_override = history or {}
    return self
end

function prompt_builder.is_conversation_input(key)
    return key ~= "context" and key ~= "agent_id" and key ~= "model" and key ~= "max_iterations"
end

function prompt_builder:_parse_json_content(content, content_type)
    if content_type == "application/json" and type(content) == "string" then
        local parsed, err = json.decode(content)
        if err then
            return content, nil
        end
        return parsed, nil
    end
    return content, nil
end

function prompt_builder:_load_conversation_history()
    if self._history_override then
        local history = {}
        local function append_rows(rows)
            for _, row in ipairs(rows or {}) do
                history[#history + 1] = row
            end
        end
        append_rows(self._history_override)
        append_rows(self._pending_history)
        return history, nil
    end

    local reader = data_reader.with_dataflow(self.dataflow_id)
        :fetch_options({ replace_references = true })
        :with_nodes(self.node_id)
        :with_data_types(
            agent_consts.DATA_TYPE.AGENT_ACTION,
            agent_consts.DATA_TYPE.AGENT_OBSERVATION,
            agent_consts.DATA_TYPE.AGENT_MEMORY,
            agent_consts.DATA_TYPE.AGENT_DELEGATION
        )

    local history_items, err = reader:all()
    if err then
        return nil, "Failed to load conversation history: " .. err
    end

    local merged_history = {}
    local seen_data_ids = {}

    local function append_rows(rows)
        for _, row in ipairs(rows or {}) do
            local row_data_id = prompt_builder.row_id(row)
            if row_data_id == "" or not seen_data_ids[row_data_id] then
                merged_history[#merged_history + 1] = row
                if row_data_id ~= "" then
                    seen_data_ids[row_data_id] = true
                end
            end
        end
    end

    append_rows(history_items)
    append_rows(self._pending_history)

    if #merged_history == 0 then
        return merged_history, nil
    end

    -- The latest checkpoint marker records the cut line as
    -- metadata.checkpoint_before_data_id.
    prompt_builder.sort_history_rows(merged_history)
    local filtered = prompt_builder.apply_latest_marker(merged_history)
    return filtered, nil
end

function prompt_builder:_format_action(action_item, builder)
    local content, err = self:_parse_json_content(action_item.content, action_item.content_type)
    if err then
        return "Failed to parse action content: " .. err
    end

    local metadata = action_item.metadata or {}
    local text_content = ""
    local tool_calls = {}
    local delegate_calls = {}

    if type(content) == "table" then
        text_content = content.result or ""
        tool_calls = content.tool_calls or {}
        delegate_calls = content.delegate_calls or {}
    else
        text_content = content or ""
    end

    local has_tool_calls = #tool_calls > 0
    local has_delegate_calls = #delegate_calls > 0
    local has_text_content = text_content and text_content ~= ""

    -- Always add assistant message if there are tool calls, delegate calls, or text content
    -- This ensures tool calls have a proper assistant message to attach to
    if has_tool_calls or has_delegate_calls or has_text_content then
        local message_meta = extract_message_meta(metadata)
        builder:add_assistant(text_content or "", message_meta)
    end

    -- Process regular tool calls
    if has_tool_calls then
        for _, tool_call in ipairs(tool_calls) do
            local call_id = tool_call.id
            if not call_id then
                return "Tool call missing ID in action"
            end
            local call_options = extract_message_meta(metadata, tool_call.provider_metadata)
            builder:add_function_call(tool_call.name, tool_call.arguments, call_id, call_options)
        end
    end

    -- Process delegate calls
    if has_delegate_calls then
        for _, delegate_call in ipairs(delegate_calls) do
            local call_id = delegate_call.id
            if not call_id then
                return "Delegate call missing ID in action"
            end
            local call_options = extract_message_meta(metadata, delegate_call.provider_metadata)
            builder:add_function_call(delegate_call.name, delegate_call.arguments, call_id, call_options)
        end
    end

    return nil
end

-- The text a table row sends: an array of command outputs has no other form.
local function content_text(content)
    if type(content) ~= "table" then
        return tostring(content)
    end
    local encoded, encode_err = json.encode(content)
    return encode_err and "[Failed to encode JSON result]" or encoded
end

function prompt_builder:_format_observation(obs_item, builder)
    local content, err = self:_parse_json_content(obs_item.content, obs_item.content_type)
    if err then
        return "Failed to parse observation content: " .. err
    end

    local metadata = obs_item.metadata or {}
    local tool_call_id = metadata.tool_call_id
    local tool_name = metadata.tool_name

    if tool_call_id and tool_name then
        local result_content
        if metadata.is_error then
            result_content = "Error: " .. content_text(content)
        elseif content == nil then
            result_content = "nil"
        else
            result_content = content_text(content)
        end

        local result_options = extract_message_meta(metadata)
        builder:add_function_result(tool_name, result_content, tool_call_id, result_options)
    else
        local feedback_content = content_text(content)
        if feedback_content and feedback_content ~= "" then
            builder:add_developer(feedback_content, positional_meta(metadata))
        end
    end

    return nil
end

function prompt_builder:_format_memory(memory_item, builder)
    local content = memory_item.content
    if not content or content == "" then
        return nil
    end

    local metadata = memory_item.metadata or {}
    local message_meta = positional_meta(metadata)
    if metadata.role == "system" then
        builder:add_system(content, message_meta)
    else
        builder:add_developer(content, message_meta)
    end
    return nil
end

function prompt_builder:_format_delegation(delegation_item, builder)
    local content = delegation_item.content
    if not content then
        return nil
    end

    local metadata = delegation_item.metadata or {}
    local tool_call_id = metadata.tool_call_id
    local tool_name = metadata.tool_name

    if tool_call_id and tool_name then
        local result_content
        if type(content) == "table" then
            local json_str, json_err = json.encode(content)
            if json_err then
                result_content = "[Failed to encode delegation result]"
            else
                result_content = json_str
            end
        else
            result_content = tostring(content)
        end

        local result_options = extract_message_meta(metadata)
        builder:add_function_result(tool_name, result_content, tool_call_id, result_options)
    else
        local delegation_text = "Delegation result: " .. tostring(content)
        builder:add_developer(delegation_text, positional_meta(metadata))
    end

    return nil
end

function prompt_builder:_format_input(content, builder)
    if content == nil or content == "" then return nil end
    local text = content
    if type(content) == "table" then
        local encoded, err = json.encode(content)
        if err then return "Failed to encode input: " .. tostring(err) end
        text = encoded
    end
    builder:add_user(tostring(text))
    return nil
end

function prompt_builder:format_history_item(item, builder)
    if prompt_builder.is_audit_row(item) then
        return nil
    end
    if item.type == agent_consts.DATA_TYPE.AGENT_INPUT then
        return self:_format_input(item.content, builder)
    elseif item.type == consts.DATA_TYPE.NODE_INPUT then
        local content, err = self:_parse_json_content(item.content, item.content_type)
        if err then return err end
        return self:_format_input(content, builder)
    elseif item.type == agent_consts.DATA_TYPE.AGENT_ACTION then
        return self:_format_action(item, builder)
    elseif item.type == agent_consts.DATA_TYPE.AGENT_OBSERVATION then
        return self:_format_observation(item, builder)
    elseif item.type == agent_consts.DATA_TYPE.AGENT_MEMORY then
        return self:_format_memory(item, builder)
    elseif item.type == agent_consts.DATA_TYPE.AGENT_DELEGATION then
        return self:_format_delegation(item, builder)
    end
    return nil
end

function prompt_builder:build_prompt(system_prompt, initial_input)
    local input_content = initial_input or self._initial_input

    local history_items, err = self:_load_conversation_history()
    if err then
        return nil, err
    end

    local builder = prompt_lib.new()

    if type(system_prompt) == "string" then
        builder:add_system(system_prompt)
        builder:add_cache_marker("system_complete")
    end

    if self._arena_config then
        local tool_calling = self._arena_config.tool_calling

        if tool_calling == "none" then
            builder:add_system("Respond with text only, do not call any tools.")
        elseif tool_calling == "auto" then
            builder:add_system(
            "Use appropriate tools when needed to advance the task. You may respond with text only if no tools are required.")
        elseif tool_calling == "any" then
            builder:add_system(
            "You must use tools to complete tasks. Use the finish tool when you have completed the task.")
        end
    end

    if input_content then
        local input_text
        if type(input_content) == "table" then
            local json_str, json_err = json.encode(input_content)
            if json_err then
                input_text = "[Complex input data]"
            else
                input_text = json_str
            end
        else
            input_text = tostring(input_content)
        end
        if input_text ~= "" then
            builder:add_user(input_text)
        end
    end
    builder:add_cache_marker("user_complete")

    for i, item in ipairs(history_items) do
        local process_err = self:format_history_item(item, builder)

        if process_err then
            return nil, process_err
        end
    end

    -- A ROLLING BREAKPOINT ON THE HISTORY TAIL.
    --
    -- The prompt is rebuilt from the stored actions and observations on every step, and
    -- only the system prompt and the initial input were marked. Every tool call and
    -- result was therefore sent uncached on every step. Marking the end of the history
    -- lets a supported provider reuse the unchanged prefix on the next step. The new
    -- suffix is a cache write; changed or expired prefixes can still miss.
    --
    -- Provider mappers deduplicate and cap breakpoints while reserving a slot for the
    -- latest eligible history boundary. Providers without explicit caching ignore markers.
    if #history_items > 0 then
        builder:add_cache_marker("history_tail")
    end

    return builder, nil
end

return prompt_builder
