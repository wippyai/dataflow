-- Validate template function for cycle template chain tests
local function run(data)
    -- Extract cycle context
    local input_data = data.input
    local state = data.state or {}
    local iteration = data.iteration or 1

    -- Get current value and target threshold
    local current_value = state.current_value or (input_data and input_data.start_value) or 10
    local target_threshold = (input_data and input_data.target_threshold) or 100

    -- Check if target is reached
    local target_reached = current_value >= target_threshold

    -- Return validation result
    return {
        state = state, -- Preserve state
        result = {
            processed_by_chain = true,
            final_value = current_value,
            target_threshold = target_threshold,
            target_reached = target_reached,
            iterations_completed = iteration,
            validated_by = "validate_template_func"
        },
        continue = not target_reached -- Stop when target is reached
    }
end

return { run = run }