local json = require("json")
local prompt = require("prompt")
local llm = require("llm")

local CONFIG = {
    model = "class:fast",
    temperature = 0.2,
    max_row_chars = 2000,
    -- Above this, providers advise streaming, and this call does not stream.
    max_tokens_ceiling = 16000,
}

local function positive_number(value: any, fallback: any): any
    local n = tonumber(value)
    if not n or n <= 0 then
        return fallback
    end
    return n
end

local function config_from_args(args)
    local options = type(args) == "table" and type(args.options) == "table" and args.options or {}
    local model = type(options.model) == "string" and options.model ~= "" and options.model or CONFIG.model
    local max_memory_chars = positive_number(options.max_memory_chars, agent_consts.DEFAULTS.MAX_MEMORY_CHARS)
    -- llm.resolve_model names the first card of a class route; a later card with a
    -- lower limit is not counted. A model that cannot resolve fails in generate.
    local card = llm.resolve_model(model)
    local output_limit = positive_number(type(card) == "table" and card.output_tokens, nil)
    local window = positive_number(type(card) == "table" and card.max_tokens, nil)
    local max_tokens = positive_number(options.max_tokens, nil)
    if not max_tokens then
        -- Admission reserves all of it against the window; the rest is for the prompt.
        max_tokens = math.min(max_memory_chars * 2, CONFIG.max_tokens_ceiling)
        if window then max_tokens = math.min(max_tokens, math.floor(window / 8)) end
    end
    if output_limit then max_tokens = math.min(max_tokens, output_limit) end
    return {
        model = model,
        temperature = tonumber(options.temperature) or CONFIG.temperature,
        max_tokens = max_tokens,
        -- Thinking and dense text share the cap.
        summary_budget = math.floor(math.min(max_memory_chars, max_tokens * 3 / 4)),
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

    if max_chars and max_chars > 0 then
        local kept, clipped = prompt_builder.text_prefix(content, max_chars)
        content = clipped and (kept .. "...") or kept
    end

    return content
end

local function history_text(history, max_row_chars)
    local parts = {}
    for index, row in ipairs(history or {}) do
        local meta = type(row.metadata) == "table" and row.metadata or {}
        -- The earlier summary is the only record of the rows it replaced.
        local row_limit = meta.checkpoint_marker ~= true and max_row_chars or nil
        parts[#parts + 1] = string.format(
            "[%d] %s %s\n%s",
            index,
            tostring(row.type or ""),
            tostring(row.data_id or ""),
            row_content(row, row_limit)
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

local SUMMARY_INSTRUCTION = "Create a concise operational checkpoint for a dataflow agent. "
    .. "Preserve task state, decisions, tool effects, blockers, and next actions. Return only the checkpoint text. "
    .. "The result must fit in %d characters: anything past that is cut off."
local MERGE_INSTRUCTION = " The history contains the previous checkpoint. Merge it with the new events: keep everything "
    .. "in it that still holds, mark completed items done, and update progress and blockers. The events it covers are "
    .. "gone, so anything you drop is lost. Condense older detail first and keep the newest state."

local function has_previous_checkpoint(history)
    for _, row in ipairs(history) do
        if type(row.metadata) == "table" and row.metadata.checkpoint_marker == true then
            return true
        end
    end
    return false
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
    p:add_system(string.format(SUMMARY_INSTRUCTION, cfg.summary_budget)
        .. (has_previous_checkpoint(args.history) and MERGE_INSTRUCTION or ""))
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
    if err or not response or type(response.result) ~= "string" or not response.result:match("%S") then
        return nil, "failed to generate checkpoint: " .. tostring(err or "empty result")
    end
    if response.finish_reason == output.FINISH_REASON.LENGTH then
        return nil, "failed to generate checkpoint: the summary reached max_tokens before it finished"
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
