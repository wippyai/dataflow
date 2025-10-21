-- Failing template function for cycle error handling tests
local function run(data)
    -- Extract cycle context
    local input_data = data.input
    local state = data.state or {}
    local iteration = data.iteration or 1

    -- Initialize state on first iteration
    if iteration == 1 then
        if input_data then
            state.should_fail = input_data.should_fail
            state.failure_iteration = input_data.failure_iteration or 2
        end
        state.iteration_count = 0
    end

    -- Get failure parameters from state
    local should_fail = state.should_fail
    local failure_iteration = state.failure_iteration or 2

    -- Check if this iteration should fail
    if should_fail and iteration >= failure_iteration then
        error("Template function failed on iteration " .. iteration .. " as requested")
    end

    -- Update iteration count in state
    local new_iteration_count = state.iteration_count + 1

    -- Return successful result for non-failing iterations
    return {
        state = {
            iteration_count = new_iteration_count,
            should_fail = state.should_fail,
            failure_iteration = state.failure_iteration
        },
        result = {
            iteration = iteration,
            iteration_count = new_iteration_count,
            should_fail = should_fail,
            failure_iteration = failure_iteration,
            processed_by = "failing_template_func"
        },
        continue = new_iteration_count < 5 -- Continue for up to 5 iterations
    }
end

return { run = run }