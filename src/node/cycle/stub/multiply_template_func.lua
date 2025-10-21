-- Multiply template function for cycle template chain tests
local function run(data)
    -- Extract cycle context
    local input_data = data.input
    local state = data.state or {}
    local iteration = data.iteration or 1

    -- Get current value from state or input
    local current_value = state.current_value or (input_data and input_data.start_value) or 10

    -- Multiply by 2 each iteration
    local multiplied_value = current_value * 2

    -- Return updated state and result
    return {
        state = {
            current_value = multiplied_value
        },
        result = {
            multiplied_value = multiplied_value,
            iteration = iteration,
            processed_by = "multiply_template_func"
        },
        continue = true -- Always continue for template chain
    }
end

return { run = run }