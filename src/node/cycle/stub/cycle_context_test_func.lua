-- Cycle context test function for template context tests
local function run(data)
    -- Extract cycle context
    local input_data = data.input
    local state = data.state or {}
    local last_result = data.last_result
    local iteration = data.iteration or 1

    -- Store original input in state on first iteration
    if iteration == 1 and input_data then
        state.original_input = input_data
    end

    -- Get multiplier from original input (stored in state)
    local original_input = state.original_input
    local multiplier = (original_input and original_input.multiplier) or 2
    local message = (original_input and original_input.message) or "default message"

    -- Get accumulator from state
    local accumulator = state.accumulator or 0
    local iteration_count = state.iteration_count or 0

    -- Process this iteration
    local new_accumulator = accumulator + (iteration * multiplier)
    local new_iteration_count = iteration_count + 1

    -- Check if we should stop (after 3 iterations for context test)
    local should_continue = new_iteration_count < 3

    -- Return result
    return {
        state = {
            accumulator = new_accumulator,
            iteration_count = new_iteration_count,
            original_input = state.original_input
        },
        result = {
            received_input = original_input,
            received_state = state,
            received_iteration = iteration,
            received_last_result = last_result,
            final_accumulator = new_accumulator,
            iteration_count = new_iteration_count,
            processed_by = "cycle_context_test_func"
        },
        continue = should_continue
    }
end

return { run = run }