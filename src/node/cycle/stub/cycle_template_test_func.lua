-- Cycle template test function for template detection tests
local function run(data)
    -- Extract cycle context
    local input_data = data.input
    local state = data.state or {}
    local iteration = data.iteration or 1

    -- Get initial and target values
    local initial_value = (input_data and input_data.initial_value) or 1
    local target = (input_data and input_data.target) or 5
    local increment = (input_data and input_data.increment) or 1

    -- Get current value from state or use initial
    local current_value = state.current_value or initial_value

    -- Increment the value
    local new_value = current_value + increment

    -- Check if target is reached
    local target_reached = new_value >= target

    -- Return result
    return {
        state = {
            current_value = new_value
        },
        result = {
            current_value = new_value,
            target = target,
            target_reached = target_reached,
            template_processed = true,
            iteration = iteration,
            processed_by = "cycle_template_test_func"
        },
        continue = not target_reached -- Stop when target is reached
    }
end

return { run = run }