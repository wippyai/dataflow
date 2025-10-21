local uuid = require("uuid")
local consts = require("consts")
local ctx = require("ctx")

local function run(context)
    if not context then
        return nil, "context is required"
    end

    local input = context.input
    local state = context.state or {}
    local last_result = context.last_result
    local iteration = context.iteration or 1
    local parent_node_id = ctx.get("node_id")

    -- Initialize state on first iteration
    if iteration == 1 then
        if not input then
            return nil, "input is required on first iteration"
        end

        state = {
            refinement_count = 0,
            quality_score = state.quality_score or 0.3,
            target_quality = input.target_quality or 0.8,
            current_text = input.initial_text or "Default text to refine",
            refinement_history = {}
        }
        print("[REFINE] Initial state - quality:", state.quality_score, "target:", state.target_quality)
    end

    -- Update state based on last result from child node
    if last_result and type(last_result) == "table" then
        print("[REFINE] Processing last_result, quality_score:", last_result.quality_score)

        if last_result.refined_text then
            state.current_text = last_result.refined_text
        end

        if last_result.quality_score then
            state.quality_score = last_result.quality_score
            print("[REFINE] Updated quality to:", state.quality_score)
        end

        state.refinement_count = state.refinement_count + 1

        table.insert(state.refinement_history, {
            iteration = iteration,
            quality = state.quality_score,
            text_length = string.len(state.current_text)
        })
    else
        print("[REFINE] No valid last_result, current quality:", state.quality_score)
    end

    -- Special case: infinite loop test
    if input and input.infinite_loop then
        print("[REFINE] Infinite loop test mode")
        return {
            state = state,
            continue = true,
            result = "infinite_loop_iteration_" .. iteration
        }
    end

    -- Check if refinement is complete
    print("[REFINE] Checking termination - current:", state.quality_score, ">=", state.target_quality, "?")
    if state.quality_score >= state.target_quality then
        print("[REFINE] Target reached! Terminating after", state.refinement_count, "refinements")
        return {
            state = state,
            continue = false,
            result = {
                refined_text = state.current_text,
                refinement_complete = true,
                final_quality = state.quality_score,
                total_refinements = state.refinement_count,
                refinement_history = state.refinement_history
            }
        }
    end

    -- Create child node to perform refinement work
    local child_node_id = uuid.v7()
    local child_input_data_id = uuid.v7()

    local refinement_input = {
        text_to_refine = state.current_text,
        current_quality = state.quality_score,
        target_quality = state.target_quality,
        iteration = iteration,
        refinement_type = (iteration % 2 == 1) and "enhance" or "polish"
    }

    print("[REFINE] Creating child for iteration", iteration, "with quality", state.quality_score)

    local commands = {
        {
            type = consts.COMMAND_TYPES.CREATE_NODE,
            payload = {
                node_id = child_node_id,
                node_type = "userspace.dataflow.node.func:node",
                status = consts.STATUS.PENDING,
                parent_node_id = parent_node_id,
                config = {
                    -- FIXED: Use correct namespace for inner_refine_test_func
                    func_id = "userspace.dataflow.node.cycle.stub:inner_refine_test_func",
                    data_targets = {
                        {
                            data_type = consts.DATA_TYPE.NODE_OUTPUT,
                            key = "refined_output",
                            content_type = consts.CONTENT_TYPE.JSON
                        }
                    }
                },
                metadata = {
                    title = "Refinement Step " .. iteration,
                    refinement_step = iteration,
                    created_by_cycle = true
                }
            }
        },
        {
            type = consts.COMMAND_TYPES.CREATE_DATA,
            payload = {
                data_id = child_input_data_id,
                data_type = consts.DATA_TYPE.NODE_INPUT,
                content = refinement_input,
                content_type = consts.CONTENT_TYPE.JSON,
                node_id = child_node_id,
                key = "default"
            }
        }
    }

    return {
        state = state,
        continue = true,
        result = {
            message = "Refinement step " .. iteration .. " initiated",
            current_quality = state.quality_score,
            target_quality = state.target_quality,
            progress = state.quality_score / state.target_quality
        },
        _control = {
            commands = commands
        }
    }
end

return { run = run }