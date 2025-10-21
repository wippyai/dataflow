-- Inner Refinement Test Function (FIXED)
-- Processes individual refinement steps within cycle iterations

local json = require("json")

local function run(input_data)
    if not input_data then
        return nil, "input data is required"
    end

    local text_to_refine = input_data.text_to_refine or "Default text"
    local current_quality = input_data.current_quality or 0.3
    local target_quality = input_data.target_quality or 0.8
    local iteration = input_data.iteration or 1
    local refinement_type = input_data.refinement_type or "enhance"

    -- Simulate different types of refinement
    local refined_text = text_to_refine
    local quality_improvement = 0.1

    if refinement_type == "enhance" then
        -- Enhance: add descriptive content
        refined_text = text_to_refine .. " [enhanced with more detail and clarity]"
        quality_improvement = 0.15
    elseif refinement_type == "polish" then
        -- Polish: improve existing content
        refined_text = "Polished: " .. text_to_refine .. " [with improved structure and flow]"
        quality_improvement = 0.1
    else
        -- Generic refinement
        refined_text = "Refined: " .. text_to_refine
        quality_improvement = 0.08
    end

    -- FIXED: Better convergence that still reaches target
    local base_improvement = quality_improvement
    local remaining = target_quality - current_quality

    -- Use more aggressive convergence that ensures we reach the target
    local convergence_factor
    if remaining > 0.3 then
        convergence_factor = 1.0  -- Full improvement when far from target
    elseif remaining > 0.1 then
        convergence_factor = 0.8  -- Still good improvement
    else
        -- When close to target, ensure we can still reach it
        convergence_factor = math.max(0.4, remaining / base_improvement)
    end

    local actual_improvement = base_improvement * convergence_factor
    local new_quality = math.min(target_quality + 0.02, current_quality + actual_improvement)

    -- Add some processing metadata
    local processing_stats = {
        original_length = string.len(text_to_refine),
        refined_length = string.len(refined_text),
        quality_improvement = new_quality - current_quality,
        refinement_type = refinement_type,
        iteration = iteration
    }

    local result = {
        refined_text = refined_text,
        quality_score = new_quality,
        processing_stats = processing_stats,
        refinement_complete = new_quality >= target_quality,
        processed_by = "inner_refine_test_func",
        timestamp = os.time(),
        convergence_rate = convergence_factor
    }

    return result
end

return { run = run }