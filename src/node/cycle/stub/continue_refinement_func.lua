local function run(context)
    if not context then
        return false
    end

    local state = context.state or {}
    local input = context.input or {}

    local target_quality = state.target_quality or input.target_quality or 0.8
    local current_quality = state.quality_score or 0

    return current_quality < target_quality
end

return { run = run }
