-- State persistence template function for cycle state persistence tests
local function run(data)
    -- Extract cycle context
    local input_data = data.input
    local state = data.state or {}
    local iteration = data.iteration or 1

    -- Get parameters from input
    local initial_count = (input_data and input_data.initial_count) or 0
    local increment = (input_data and input_data.increment) or 5
    local target = (input_data and input_data.target) or 25

    -- Get current state
    local count = state.count or initial_count
    local history = state.history or {}

    -- Increment count
    local new_count = count + increment

    -- Add to history
    local new_history = {}
    for i, item in ipairs(history) do
        new_history[i] = item
    end
    table.insert(new_history, {
        iteration = iteration,
        count = new_count,
        increment = increment
    })

    -- Check if target is reached
    local target_reached = new_count >= target

    -- Return result
    return {
        state = {
            count = new_count,
            history = new_history
        },
        result = {
            final_count = new_count,
            total_iterations = iteration,
            iteration_history = new_history,
            state_persisted = true,
            target_reached = target_reached,
            processed_by = "state_persistence_template_func"
        },
        continue = not target_reached -- Stop when target is reached
    }
end

return { run = run }