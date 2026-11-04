local json = require("json")
local prompt = require("prompt")
local llm = require("llm")

local CONFIG = {
    model = "class:fast",
    temperature = 0.2,
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
    local max_memory_chars = positive_number(options.max_memory_chars, agent_consts.DEFAULTS.MAX_MEMORY_CHARS)
    return {
        model = type(options.model) == "string" and options.model ~= "" and options.model or CONFIG.model,
        temperature = tonumber(options.temperature) or CONFIG.temperature,
        -- Only a cap: a summary at the budget fits it even at one token per character.
        max_tokens = positive_number(options.max_tokens, max_memory_chars * 2),
        max_memory_chars = max_memory_chars,
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

-- What the output can hold at two tokens per character. The route lowers max_tokens to
-- the model's output limit, so the stated budget has to follow that too.
local function summary_budget(cfg)
    local card = llm.resolve_model(cfg.model)
    local model_limit = type(card) == "table" and tonumber(card.output_tokens) or nil
    local tokens = cfg.max_tokens
    if model_limit and model_limit > 0 and model_limit < tokens then
        tokens = model_limit
    end
    return math.floor(math.min(cfg.max_memory_chars, tokens / 2))
end

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
    p:add_system(string.format(SUMMARY_INSTRUCTION, summary_budget(cfg))
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
    if err or not response or type(response.result) ~= "string" or response.result == "" then
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
