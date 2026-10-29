local json = require("json")
local data_reader = require("data_reader")
local node_reader = require("node_reader")
local prompt_builder = require("prompt_builder")
local prompt = require("prompt")
local agent_consts = require("agent_consts")
local consts = require("consts")

local M = {}

type Selector = {
    mode: string?,
    last: any?,
    from_id: string?,
    to_id: string?,
    checkpoint_id: string?,
    max_chars: any?,
}

local HISTORY_TYPES = {
    consts.DATA_TYPE.NODE_INPUT,
    agent_consts.DATA_TYPE.AGENT_ACTION,
    agent_consts.DATA_TYPE.AGENT_OBSERVATION,
    agent_consts.DATA_TYPE.AGENT_MEMORY,
    agent_consts.DATA_TYPE.AGENT_DELEGATION
}

local function host_ids(args)
    local host = type(args) == "table" and args.host or nil
    local dataflow_id = host and host.dataflow_id or (type(args) == "table" and args.dataflow_id)
    local node_id = host and host.node_id or (type(args) == "table" and args.node_id)

    if type(dataflow_id) ~= "string" or dataflow_id == "" then
        return nil, nil, "host.dataflow_id is required"
    end
    if type(node_id) ~= "string" or node_id == "" then
        return nil, nil, "host.node_id is required"
    end

    return dataflow_id, node_id, nil
end

local function row_id(row)
    if type(row) ~= "table" then
        return ""
    end
    return tostring(row.data_id or "")
end

local function sort_rows(rows)
    table.sort(rows, function(a, b)
        return row_id(a) < row_id(b)
    end)
    return rows
end

local function find_latest_checkpoint_marker(rows)
    for i = #rows, 1, -1 do
        local row = rows[i]
        if row.type == agent_consts.DATA_TYPE.AGENT_MEMORY
           and type(row.metadata) == "table"
           and row.metadata.checkpoint_marker == true then
            return row
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

local function apply_latest_marker(rows)
    if #rows == 0 then
        return rows
    end

    sort_rows(rows)
    local latest_marker = find_latest_checkpoint_marker(rows)
    if not latest_marker then
        return rows
    end

    local cut_before = tostring((latest_marker.metadata or {}).checkpoint_before_data_id or "")
    local filtered = { latest_marker }
    for _, row in ipairs(rows) do
        if row ~= latest_marker then
            local keep = row_id(row) > cut_before or is_structured_result_row(row)
            if keep then
                filtered[#filtered + 1] = row
            end
        end
    end

    return sort_rows(filtered)
end

local function selector(args): Selector
    local raw = type(args) == "table" and args.selector or nil
    if type(raw) ~= "table" then
        return { mode = "since_checkpoint" } :: Selector
    end
    return raw :: Selector
end

local function copy_rows(rows)
    local copied = {}
    for _, row in ipairs(rows or {}) do
        copied[#copied + 1] = row
    end
    return copied
end

local function load_history_rows(dataflow_id, node_id)
    local ok, rows_or_err = pcall(function()
        return data_reader.with_dataflow(dataflow_id)
            :fetch_options({ replace_references = false })
            :with_nodes(node_id)
            :with_data_types(HISTORY_TYPES)
            :all()
    end)
    if not ok then
        return nil, tostring(rows_or_err)
    end
    local rows = {}
    for _, row in ipairs(rows_or_err or {}) do
        local input_key = row.discriminator or row.key or "default"
        if row.content_type == consts.CONTENT_TYPE.REFERENCE then
            row.metadata = row.ref_metadata or {}
            row.content = row.ref_content
            row.content_type = row.ref_content_type
            input_key = row.discriminator or row.ref_key or row.key or "default"
        end
        if row.type ~= consts.DATA_TYPE.NODE_INPUT or prompt_builder.is_conversation_input(input_key) then
            rows[#rows + 1] = row
        end
    end
    return sort_rows(rows), nil
end

local function slice_rows(rows, sel: Selector)
    local mode = sel.mode or "since_checkpoint"
    local source = copy_rows(rows)

    if mode == "since_checkpoint" then
        return apply_latest_marker(source)
    end

    if mode == "checkpoint" then
        if type(sel.checkpoint_id) ~= "string" or sel.checkpoint_id == "" then
            return nil, "selector.checkpoint_id is required for checkpoint"
        end
        mode = "since_id"
        sel = {
            mode = "since_id",
            from_id = sel.checkpoint_id,
            max_chars = sel.max_chars
        } :: Selector
    end

    if mode == "window" then
        local count: number? = 20
        if sel.last ~= nil then
            count = tonumber(sel.last)
        end
        if not count or count <= 0 then
            return nil, "selector.last must be positive for window"
        end
        local count_value = count :: number
        local first = math.max(1, #source - count_value + 1)
        local out = {}
        for i = first, #source do
            out[#out + 1] = source[i]
        end
        return out
    end

    if mode == "since_id" then
        if type(sel.from_id) ~= "string" or sel.from_id == "" then
            return nil, "selector.from_id is required for since_id"
        end
        local out = {}
        for _, row in ipairs(source) do
            if row_id(row) > sel.from_id then
                out[#out + 1] = row
            end
        end
        return out
    end

    if mode == "range" then
        local out = {}
        for _, row in ipairs(source) do
            local id = row_id(row)
            local after_from = type(sel.from_id) ~= "string" or sel.from_id == "" or id > sel.from_id
            if after_from then
                out[#out + 1] = row
            end
            if type(sel.to_id) == "string" and sel.to_id ~= "" and id == sel.to_id then
                break
            end
        end
        return out
    end

    if mode == "all" then
        return source
    end

    return nil, "unknown selector mode: " .. tostring(mode)
end

local function decode_json(content)
    if type(content) ~= "string" then return content end
    local decoded, err = json.decode(content)
    if err then return content end
    return decoded
end

local function normalize_event(row, formatter, calls)
    local builder = prompt.new()
    local err = formatter:format_history_item(row, builder)
    if err then return nil, err end
    local texts = {}
    local role
    local metadata = {}
    for key, value in pairs(row.metadata or {}) do metadata[key] = value end
    for _, message in ipairs(builder:get_messages()) do
        if message.role ~= prompt.ROLE.FUNCTION_CALL and message.role ~= prompt.ROLE.CACHE_MARKER then
            role = message.role == prompt.ROLE.FUNCTION_RESULT and "function" or message.role
            for _, part in ipairs(message.content or {}) do
                if part.text then texts[#texts + 1] = part.text end
            end
            if role == "function" then
                metadata.function_name = message.name
                metadata.status = metadata.is_error and "error" or "success"
                metadata.call_id = message.function_call_id
            end
        end
    end
    local text = table.concat(texts, "\n")
    local content = text
    if role == "function" then
        local call = calls[metadata.call_id]
        if not call then return nil, "Function result has no stored call: " .. tostring(metadata.call_id) end
        content = decode_json(call.arguments)
        if type(call.arguments) == "string" then
            text = call.arguments
        else
            local encoded, encode_err = json.encode(call.arguments)
            if encode_err then return nil, encode_err end
            text = encoded
        end
        metadata.registry_id = call.registry_id
        metadata.result = row.content
        if row.content_type == consts.CONTENT_TYPE.JSON then
            metadata.result = decode_json(row.content)
        end
    end
    local author = metadata.author
    if type(author) ~= "table" then
        author = { kind = "unknown" }
    end
    return {
        id = row.data_id,
        role = role,
        author = author,
        content = content,
        raw_content = text,
        content_type = row.content_type,
        key = row.key,
        discriminator = row.discriminator,
        metadata = metadata,
        created_at = row.created_at
    }, nil
end

local function content_length(event)
    local content = event.raw_content or event.content or ""
    if type(content) ~= "string" then
        local encoded = json.encode(content)
        content = encoded or tostring(content)
    end
    local length = #content
    -- Normalized tool events put arguments in content and the stored result in
    -- metadata. Both belong to the bounded conversation, including get_prompt.
    local result = event.role == "function" and event.metadata and event.metadata.result
    if result ~= nil and event.role == "function" then
        if type(result) ~= "string" then
            local encoded = json.encode(result)
            result = encoded or tostring(result)
        end
        length = length + #result
    end
    return length
end

local function clamp_rows(rows, events, max_chars)
    local limit = tonumber(max_chars)
    if not limit or limit <= 0 then
        return rows, events, false
    end

    local kept_rows = {}
    local kept_events = {}
    local total = 0
    for i, event in ipairs(events) do
        local next_total = total + content_length(event)
        if next_total > limit then
            return kept_rows, kept_events, true
        end
        total = next_total
        kept_rows[#kept_rows + 1] = rows[i]
        kept_events[#kept_events + 1] = event
    end
    return kept_rows, kept_events, false
end

local function history_rows(args)
    local dataflow_id, node_id, id_err = host_ids(args)
    if id_err then
        return nil, nil, nil, id_err
    end

    local all_rows, load_err = load_history_rows(dataflow_id, node_id)
    if load_err then
        return nil, nil, nil, load_err
    end

    local calls = {}
    for _, row in ipairs(all_rows) do
        if row.type == agent_consts.DATA_TYPE.AGENT_ACTION then
            local action = row.content_type == consts.CONTENT_TYPE.JSON and decode_json(row.content) or row.content
            if type(action) == "table" then
                for _, list in ipairs({ action.tool_calls or {}, action.delegate_calls or {} }) do
                    for _, call in ipairs(list) do calls[call.id] = call end
                end
            end
        end
    end
    local sel = selector(args)
    local rows, slice_err = slice_rows(all_rows, sel)
    if slice_err then
        return nil, nil, nil, slice_err
    end

    local events = {}
    local formatter, formatter_err = prompt_builder.new(dataflow_id, node_id, { node_id })
    if formatter_err then return nil, nil, nil, formatter_err end
    for _, row in ipairs(rows or {}) do
        local event, event_err = normalize_event(row, formatter, calls)
        if event_err then return nil, nil, nil, event_err end
        events[#events + 1] = event
    end

    local truncated
    rows, events, truncated = clamp_rows(rows or {}, events, sel.max_chars)
    return rows, events, sel, nil, truncated
end

local function node_row(dataflow_id, node_id)
    local reader, reader_err = node_reader.with_dataflow(dataflow_id)
    if not reader then
        return nil, reader_err
    end
    local row, row_err = reader:with_nodes(node_id):one()
    if row_err then
        return nil, row_err
    end
    return row, nil
end

local function merge_into(target, source)
    if type(source) ~= "table" then
        return target
    end
    for key, value in pairs(source) do
        if type(key) == "string" then
            target[key] = value
        end
    end
    return target
end

function M.get_context(args)
    local dataflow_id, node_id, id_err = host_ids(args)
    if id_err then
        return nil, id_err
    end

    local row, row_err = node_row(dataflow_id, node_id)
    if row_err then
        return nil, row_err
    end
    if not row then
        return nil, "node not found"
    end

    local config = row.config or {}
    local metadata = row.metadata or {}
    local context = {
        dataflow_id = dataflow_id,
        node_id = node_id
    }
    merge_into(context, ((config.arena or {}).context))
    merge_into(context, metadata.session_context)

    return {
        context = context,
        host = type(args) == "table" and args.host or nil,
        agent = type(args) == "table" and args.agent or nil,
        config = config,
        metadata = metadata
    }, nil
end

function M.get_history(args)
    local rows, events, sel, err, truncated = history_rows(args)
    if err then
        return nil, err
    end
    rows = rows or {}
    events = events or {}

    local first = events[1]
    local last = events[#events]
    return {
        events = events,
        range = {
            from_id = first and first.id or nil,
            to_id = last and last.id or nil,
            checkpoint_id = sel and sel.checkpoint_id or nil
        },
        truncated = truncated == true,
        count = #rows
    }, nil
end

local function prompt_to_text(messages)
    local parts = {}
    for _, msg in ipairs(messages or {}) do
        local role = tostring(msg.role or "message")
        local text = ""
        if type(msg.content) == "table" then
            for _, item in ipairs(msg.content) do
                if type(item) == "table" and item.text then
                    text = text .. tostring(item.text)
                elseif type(item) == "string" then
                    text = text .. item
                end
            end
        elseif msg.content ~= nil then
            text = tostring(msg.content)
        end
        parts[#parts + 1] = string.format("[%s] %s", role, text)
    end
    return table.concat(parts, "\n")
end

function M.get_prompt(args)
    local dataflow_id, node_id, id_err = host_ids(args)
    if id_err then
        return nil, id_err
    end

    local rows, _, sel, hist_err, truncated = history_rows(args)
    if hist_err then
        return nil, hist_err
    end

    local row, row_err = node_row(dataflow_id, node_id)
    if row_err then
        return nil, row_err
    end
    if not row then
        return nil, "node not found"
    end

    local config = row.config or {}
    local arena = config.arena or {}
    local builder, builder_err = prompt_builder.new(dataflow_id, node_id, { node_id })
    if not builder then
        return nil, builder_err or "failed to create prompt builder"
    end

    builder:with_arena_config(arena):with_history(rows or {})
    local built, build_err = builder:build_prompt(args and args.prompt or arena.prompt, args and args.initial_input)
    if build_err then
        return nil, build_err
    end

    local messages = built:get_messages()
    local format = args and args.format or "messages"
    local first = rows and rows[1] or nil
    local last = rows and rows[#rows] or nil
    local result = {
        range = {
            from_id = first and first.data_id or nil,
            to_id = last and last.data_id or nil,
            checkpoint_id = sel and sel.checkpoint_id or nil
        },
        truncated = truncated == true
    }
    if format == "text" or format == "both" then
        result.text = prompt_to_text(messages)
    end
    if format ~= "text" then
        result.messages = messages
    end

    return result, nil
end

return M
