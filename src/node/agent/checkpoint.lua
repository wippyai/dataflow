local json = require("json")
local prompt = require("prompt")
local llm = require("llm")

local CONFIG = {
    model = "class:fast",
    temperature = 0.2,
    max_tokens = 2000,
    max_row_chars = 2000,
}

local function positive_number(value, fallback)
    local n = tonumber(value)
    if not n or n <= 0 then
        return fallback
    end
    return n
end

local function config_from_args(args)
    local options = type(args) == "table" and type(args.options) == "table" and args.options or {}
    return {
        model = type(options.model) == "string" and options.model ~= "" and options.model or CONFIG.model,
        temperature = tonumber(options.temperature) or CONFIG.temperature,
        max_tokens = positive_number(options.max_tokens, CONFIG.max_tokens),
        max_row_chars = positive_number(options.max_row_chars, CONFIG.max_row_chars),
    }
end

local function row_content(row, max_chars)
    local content = row and row.content
    if type(content) == "table" then
        local encoded, encode_err = json.encode(content)
        content = encode_err and tostring(content) or encoded
    elseif content == nil then
        content = ""
    else
        content = tostring(content)
    end

    if max_chars and max_chars > 0 and #content > max_chars then
        content = content:sub(1, max_chars) .. "..."
    end

    return content
end

local function history_text(history, max_row_chars)
    local parts = {}
    for index, row in ipairs(history or {}) do
        local meta = type(row.metadata) == "table" and row.metadata or {}
        parts[#parts + 1] = string.format(
            "[%d] %s %s\n%s",
            index,
            tostring(row.type or ""),
            tostring(row.data_id or ""),
            row_content(row, max_row_chars)
        )
        if meta.tool_name or meta.iteration then
            parts[#parts + 1] = "metadata: " .. json.encode({
                iteration = meta.iteration,
                tool_name = meta.tool_name,
                status = meta.status,
                finish_reason = meta.finish_reason,
            })
        end
    end
    return table.concat(parts, "\n\n")
end

local function handler(args)
    if type(args) ~= "table" then
        return nil, "checkpoint args are required"
    end
    if type(args.history) ~= "table" or #args.history == 0 then
        return nil, "history is required"
    end

    local cfg = config_from_args(args)
    local p = prompt.new()
    p:add_system(
        "Create a concise operational checkpoint for a dataflow agent. Preserve task state, decisions, tool effects, blockers, and next actions. Return only the checkpoint text."
    )
    p:add_user(string.format(
        "dataflow_id=%s\nnode_id=%s\niteration=%s\nprompt_tokens=%s\nhistory_count=%s\n\n%s",
        tostring(args.dataflow_id or ""),
        tostring(args.node_id or ""),
        tostring(args.iteration or ""),
        tostring(args.prompt_tokens or ""),
        tostring(args.history_count or #args.history),
        history_text(args.history, cfg.max_row_chars)
    ))

    local response, err = llm.generate(p, {
        model = cfg.model,
        temperature = cfg.temperature,
        max_tokens = cfg.max_tokens,
    })
    if err or not response or type(response.result) ~= "string" or response.result == "" then
        return nil, "failed to generate checkpoint: " .. tostring(err or "empty result")
    end

    return {
        memory = response.result,
        summary = response.result,
        tokens = response.tokens or {},
    }
end

return {
    handler = handler,
    config_from_args = config_from_args,
    history_text = history_text,
}
