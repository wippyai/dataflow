local parallel = {}

type FuncExecutor = {
    with_context: (self: FuncExecutor, context: {[string]: any}) -> FuncExecutor,
    call: (self: FuncExecutor, func_id: string, data: any) -> (any, string?),
}

type ExtractorFn = (parallel_result: any) -> any

parallel._deps = {
    node = require("node"),
    template_graph = require("template_graph"),
    iterator = require("iterator"),
    funcs = require("funcs"),
    uuid = require("uuid"),
    consts = require("consts")
}

parallel.DEFAULTS = {
    BATCH_SIZE = 1,
    ITERATION_INPUT_KEY = "default",
    ON_ERROR = "continue",
    FILTER = "all",
    UNWRAP = false
}

parallel.ON_ERROR_STRATEGIES = {
    FAIL_FAST = "fail_fast",
    CONTINUE = "continue"
}

parallel.FILTER_STRATEGIES = {
    ALL = "all",
    SUCCESSES = "successes",
    FAILURES = "failures"
}

parallel.EXTRACTORS = {
    SUCCESSES = "successes",
    FAILURES = "failures"
}

parallel.ERRORS = {
    MISSING_SOURCE_ARRAY_KEY = "MISSING_SOURCE_ARRAY_KEY",
    NO_INPUT_DATA = "NO_INPUT_DATA",
    NO_TEMPLATES = "NO_TEMPLATES",
    INVALID_INPUT_STRUCTURE = "INVALID_INPUT_STRUCTURE",
    ITERATION_FAILED = "ITERATION_FAILED",
    TEMPLATE_DISCOVERY_FAILED = "TEMPLATE_DISCOVERY_FAILED",
    INVALID_ON_ERROR_STRATEGY = "INVALID_ON_ERROR_STRATEGY",
    INVALID_FILTER_STRATEGY = "INVALID_FILTER_STRATEGY",
    INVALID_BATCH_SIZE = "INVALID_BATCH_SIZE",
    INVALID_PIPELINE_STEP = "INVALID_PIPELINE_STEP",
    INVALID_EXTRACTOR = "INVALID_EXTRACTOR"
}

parallel.extractors = ({
    [parallel.EXTRACTORS.SUCCESSES] = function(parallel_result)
        local results = {}
        for _, entry in ipairs(parallel_result.successes or {}) do
            table.insert(results, entry.result)
        end
        return results
    end,
    [parallel.EXTRACTORS.FAILURES] = function(parallel_result)
        local failures = {}
        for _, entry in ipairs(parallel_result.failures or {}) do
            table.insert(failures, entry)
        end
        return failures
    end
}) :: {[string]: ExtractorFn}

local function validate_on_error_strategy(strategy)
    return strategy == parallel.ON_ERROR_STRATEGIES.FAIL_FAST or
        strategy == parallel.ON_ERROR_STRATEGIES.CONTINUE
end

local function validate_filter_strategy(strategy)
    return strategy == parallel.FILTER_STRATEGIES.ALL or
        strategy == parallel.FILTER_STRATEGIES.SUCCESSES or
        strategy == parallel.FILTER_STRATEGIES.FAILURES
end

local function validate_batch_size(size)
    return type(size) == "number" and size > 0 and size <= 1000
end

local function merge_context(base, additions)
    local merged = {}

    if type(base) == "table" then
        for k, v in pairs(base) do
            merged[k] = v
        end
    end

    if type(additions) == "table" then
        for k, v in pairs(additions) do
            merged[k] = v
        end
    end

    return merged
end

local function call_func(func_id: string, data: any, context: {[string]: any}?)
    local executor = parallel._deps.funcs.new() :: FuncExecutor
    if context ~= nil then
        executor = executor:with_context(context)
    end

    return executor:call(func_id, data)
end

local function safe_inputs(n)
    local ok, inputs_or_err, inputs_err = pcall(function()
        return n:inputs()
    end)

    if not ok then
        return nil, tostring(inputs_or_err)
    end

    if inputs_err then
        return nil, tostring(inputs_err)
    end

    return inputs_or_err, nil
end

local function normalize_input_payload(inputs)
    if not inputs or next(inputs) == nil then
        return nil
    end

    if inputs.default and inputs.default.content ~= nil then
        return inputs.default.content
    end

    if inputs[""] and inputs[""].content ~= nil then
        return inputs[""].content
    end

    local count = 0
    local first_input = nil
    for _, input in pairs(inputs) do
        count = count + 1
        if not first_input then
            first_input = input
        end
    end

    if count == 1 and first_input then
        return first_input.content
    end

    local mapped = {}
    for key, input in pairs(inputs) do
        mapped[key] = input.content
    end

    return mapped
end

function parallel.validate_item_pipeline_step(step)
    if type(step) ~= "table" then
        return false, "item step must be a table"
    end

    if step.context ~= nil and type(step.context) ~= "table" then
        return false, "context must be a table"
    end

    if step.type ~= "map" and step.type ~= "filter" then
        return false, "item step type must be 'map' or 'filter'"
    end

    if type(step.func_id) ~= "string" or step.func_id == "" then
        return false, "item step func_id is required"
    end

    return true, nil
end

function parallel.validate_reduction_pipeline_step(step, _shape)
    if type(step) ~= "table" then
        return false, "reduction step must be a table"
    end

    if step.context ~= nil and type(step.context) ~= "table" then
        return false, "context must be a table"
    end

    if step.type == "map" or step.type == "filter" or step.type == "aggregate" then
        if type(step.func_id) ~= "string" or step.func_id == "" then
            return false, "reduction step func_id is required"
        end
        return true, nil
    end

    if step.type == "group" then
        if type(step.key_func_id) ~= "string" or step.key_func_id == "" then
            return false, "group step key_func_id is required"
        end
        return true, nil
    end

    return false, "invalid reduction step type"
end

function parallel.validate_reduction_pipeline_flow(extractor_name, steps)
    if extractor_name ~= nil and parallel.extractors[extractor_name] == nil then
        return false, "invalid extractor"
    end

    if steps == nil then
        return true, nil
    end

    if type(steps) ~= "table" then
        return false, "reduction_steps must be a table"
    end

    local shape = "array"
    for _, step in ipairs(steps) do
        local valid, err = parallel.validate_reduction_pipeline_step(step, shape)
        if not valid then
            return false, err
        end

        if step.type == "group" then
            shape = "grouped"
        elseif step.type == "aggregate" then
            shape = "scalar"
        end
    end

    return true, nil
end

function parallel.execute_item_pipeline_step(step, data)
    local valid, err = parallel.validate_item_pipeline_step(step)
    if not valid then
        return nil, err
    end

    local func_id = step.func_id :: string
    local step_context = step.context :: {[string]: any}?

    if step.type == "map" then
        return call_func(func_id, data, step_context)
    end

    local keep, call_err = call_func(func_id, data, step_context)
    if call_err then
        return nil, call_err
    end

    return keep == true, nil
end

function parallel.execute_reduction_pipeline_step(step, data)
    local valid, err = parallel.validate_reduction_pipeline_step(step, "array")
    if not valid then
        return nil, err
    end

    if step.type == "aggregate" then
        local func_id = step.func_id :: string
        local step_context = step.context :: {[string]: any}?
        return call_func(func_id, data, step_context)
    end

    if type(data) ~= "table" then
        return nil, "reduction step requires array input"
    end

    if step.type == "map" then
        local func_id = step.func_id :: string
        local mapped = {}
        for index, item in ipairs(data) do
            local ctx = nil
            if step.context ~= nil then
                ctx = merge_context(step.context, {
                    current_item = item,
                    item_index = index
                })
            end

            local mapped_item, map_err = call_func(func_id, item, ctx)
            if map_err then
                return nil, map_err
            end
            table.insert(mapped, mapped_item)
        end
        return mapped, nil
    end

    if step.type == "filter" then
        local func_id = step.func_id :: string
        local filtered = {}
        for index, item in ipairs(data) do
            local ctx = nil
            if step.context ~= nil then
                ctx = merge_context(step.context, {
                    current_item = item,
                    item_index = index
                })
            end

            local keep, filter_err = call_func(func_id, item, ctx)
            if filter_err then
                return nil, filter_err
            end
            if keep == true then
                table.insert(filtered, item)
            end
        end
        return filtered, nil
    end

    local grouped = {}
    local key_func_id = step.key_func_id :: string
    for index, item in ipairs(data) do
        local ctx = nil
        if step.context ~= nil then
            ctx = merge_context(step.context, {
                current_item = item,
                item_index = index
            })
        end

        local key, key_err = call_func(key_func_id, item, ctx)
        if key_err then
            return nil, key_err
        end

        local key_str = tostring(key)
        if grouped[key_str] == nil then
            grouped[key_str] = {}
        end
        table.insert(grouped[key_str], item)
    end

    return grouped, nil
end

local function gather_run_nodes(iterations)
    local run_nodes = {}

    for _, iteration in ipairs(iterations) do
        if type(iteration.root_nodes) == "table" and #iteration.root_nodes > 0 then
            for _, node_id in ipairs(iteration.root_nodes) do
                table.insert(run_nodes, node_id)
            end
        elseif type(iteration.uuid_mapping) == "table" then
            for _, node_id in pairs(iteration.uuid_mapping) do
                table.insert(run_nodes, node_id)
            end
        end
    end

    return run_nodes
end

local function fail_iteration(n, message)
    return n:fail({
        code = parallel.ERRORS.ITERATION_FAILED,
        message = message
    }, message)
end

local function add_failure(parallel_result, iteration, err_message)
    parallel_result.failure_count = parallel_result.failure_count + 1
    parallel_result.failures[parallel_result.failure_count] = {
        iteration = iteration.iteration or iteration.iteration_index,
        item = iteration.input_item,
        error = err_message
    }
end

local function add_success(parallel_result, iteration, result)
    parallel_result.success_count = parallel_result.success_count + 1
    parallel_result.successes[parallel_result.success_count] = {
        iteration = iteration.iteration or iteration.iteration_index,
        item = iteration.input_item,
        result = result
    }
end

local function maybe_materialize_passthrough_inputs(n, inputs, passthrough_keys)
    if type(passthrough_keys) ~= "table" or #passthrough_keys == 0 then
        return {}, nil
    end

    local passthrough_inputs = {}

    for _, key in ipairs(passthrough_keys) do
        local input = inputs[key]
        if input then
            local data_id = input.data_id

            if not data_id and input.content ~= nil then
                data_id = parallel._deps.uuid.v7()
                n:data(parallel._deps.consts.DATA_TYPE.NODE_INPUT, input.content, {
                    data_id = data_id,
                    discriminator = key,
                    node_id = n.node_id,
                    content_type = type(input.content) == "table" and
                        parallel._deps.consts.CONTENT_TYPE.JSON or
                        parallel._deps.consts.CONTENT_TYPE.TEXT
                })
            end

            if data_id then
                passthrough_inputs[key] = {
                    node_id = n.node_id,
                    data_id = data_id
                }
            end
        end
    end

    if next(passthrough_inputs) ~= nil and type(n.submit) == "function" then
        local submit_ok, submit_err = n:submit()
        if not submit_ok then
            return nil, submit_err or "failed to submit passthrough inputs"
        end
    end

    return passthrough_inputs, nil
end

local function process_reduction_pipeline(config: any, parallel_result, on_error)
    local reduction_extract = config.reduction_extract
    local reduction_steps = config.reduction_steps

    local current = parallel_result

    if reduction_extract ~= nil then
        local extractor = parallel.extractors[reduction_extract]
        if extractor == nil then
            return nil, "invalid extractor"
        end
        current = extractor(parallel_result)
    end

    if reduction_steps and #reduction_steps > 0 then
        for _, step in ipairs(reduction_steps) do
            local next_value, step_err = parallel.execute_reduction_pipeline_step(step, current)
            if step_err then
                if on_error == parallel.ON_ERROR_STRATEGIES.FAIL_FAST then
                    return nil, "Reduction pipeline failed: " .. step_err
                end
                return parallel_result, nil
            end
            current = next_value
        end
    end

    return current, nil
end

local function run(args)
    local n, err = parallel._deps.node.new(args)
    if err then
        error(err)
    end

    local config = n:config() or {}

    local source_array_key = config.source_array_key
    if type(source_array_key) ~= "string" or source_array_key == "" then
        return n:fail({
            code = parallel.ERRORS.MISSING_SOURCE_ARRAY_KEY,
            message = "source_array_key is required in parallel configuration"
        }, "Missing source_array_key in config")
    end

    local on_error = config.on_error or parallel.DEFAULTS.ON_ERROR
    if not validate_on_error_strategy(on_error) then
        return n:fail({
            code = parallel.ERRORS.INVALID_ON_ERROR_STRATEGY,
            message = "Invalid on_error: " .. tostring(on_error)
        }, "Invalid on_error strategy")
    end

    local filter = config.filter or parallel.DEFAULTS.FILTER
    if not validate_filter_strategy(filter) then
        return n:fail({
            code = parallel.ERRORS.INVALID_FILTER_STRATEGY,
            message = "Invalid filter: " .. tostring(filter)
        }, "Invalid filter strategy")
    end

    local unwrap = config.unwrap
    if unwrap == nil then
        unwrap = parallel.DEFAULTS.UNWRAP
    end

    local batch_size = config.batch_size or parallel.DEFAULTS.BATCH_SIZE
    if not validate_batch_size(batch_size) then
        return n:fail({
            code = parallel.ERRORS.INVALID_BATCH_SIZE,
            message = "batch_size must be a positive number <= 1000"
        }, "Invalid batch size")
    end

    if config.item_steps ~= nil then
        if type(config.item_steps) ~= "table" then
            return n:fail({
                code = parallel.ERRORS.INVALID_PIPELINE_STEP,
                message = "item_steps must be a table"
            }, "Invalid item pipeline")
        end

        for _, step in ipairs(config.item_steps) do
            local valid, step_err = parallel.validate_item_pipeline_step(step)
            if not valid then
                return n:fail({
                    code = parallel.ERRORS.INVALID_PIPELINE_STEP,
                    message = step_err
                }, "Invalid item pipeline")
            end
        end
    end

    if config.reduction_extract ~= nil and parallel.extractors[config.reduction_extract] == nil then
        return n:fail({
            code = parallel.ERRORS.INVALID_EXTRACTOR,
            message = "Invalid reduction extractor: " .. tostring(config.reduction_extract)
        }, "Invalid reduction extractor")
    end

    local reduction_valid, reduction_err = parallel.validate_reduction_pipeline_flow(
        config.reduction_extract,
        config.reduction_steps
    )
    if not reduction_valid then
        local code = parallel.ERRORS.INVALID_PIPELINE_STEP
        if string.find(tostring(reduction_err), "extractor", 1, true) then
            code = parallel.ERRORS.INVALID_EXTRACTOR
        end

        return n:fail({
            code = code,
            message = reduction_err
        }, "Invalid reduction pipeline")
    end

    local inputs, inputs_err = safe_inputs(n)
    if inputs_err then
        return n:fail({
            code = "INPUT_VALIDATION_FAILED",
            message = inputs_err
        }, inputs_err)
    end

    local input_payload = normalize_input_payload(inputs)
    if input_payload == nil then
        return n:fail({
            code = parallel.ERRORS.NO_INPUT_DATA,
            message = "No input data provided for parallel node"
        }, "Parallel node requires input data")
    end

    if type(input_payload) ~= "table" then
        return n:fail({
            code = parallel.ERRORS.INVALID_INPUT_STRUCTURE,
            message = "Input data must be an object with source array"
        }, "Invalid input structure for parallel")
    end

    local items = input_payload[source_array_key]
    if items == nil then
        return n:fail({
            code = parallel.ERRORS.INVALID_INPUT_STRUCTURE,
            message = "Input data must contain '" .. source_array_key .. "' field with array"
        }, "Invalid input structure for parallel")
    end

    if type(items) ~= "table" or #items == 0 then
        return n:fail({
            code = parallel.ERRORS.INVALID_INPUT_STRUCTURE,
            message = "Field '" .. source_array_key .. "' must be a non-empty array"
        }, "Invalid input structure for parallel")
    end

    local passthrough_keys = {}
    if type(config.passthrough_keys) == "table" then
        for _, key in ipairs(config.passthrough_keys) do
            if type(key) == "string" then
                table.insert(passthrough_keys, key)
            end
        end
    end

    local passthrough_inputs, passthrough_err = maybe_materialize_passthrough_inputs(
        n,
        inputs,
        passthrough_keys
    )
    if passthrough_err then
        return fail_iteration(n, "Failed to materialize passthrough inputs: " .. tostring(passthrough_err))
    end

    local template_graph, template_err = parallel._deps.template_graph.build_for_node(n)
    if template_err then
        return n:fail({
            code = parallel.ERRORS.TEMPLATE_DISCOVERY_FAILED,
            message = template_err
        }, "Failed to discover template nodes")
    end

    if template_graph:is_empty() then
        return n:fail({
            code = parallel.ERRORS.NO_TEMPLATES,
            message = "No template nodes found. Parallel requires child nodes with status='template'"
        }, "No template nodes found")
    end

    local parallel_result = {
        successes = {},
        failures = {},
        success_count = 0,
        failure_count = 0,
        total_iterations = #items
    }

    local iteration_input_key = config.iteration_input_key or parallel.DEFAULTS.ITERATION_INPUT_KEY

    for batch_start = 1, #items, batch_size do
        local batch_end = math.min(batch_start + batch_size - 1, #items)

        local iterations, create_err = parallel._deps.iterator.create_batch(
            n,
            template_graph,
            items,
            batch_start,
            batch_end,
            iteration_input_key,
            passthrough_inputs
        )

        if create_err then
            if on_error == parallel.ON_ERROR_STRATEGIES.FAIL_FAST then
                return fail_iteration(n, "Iteration creation failed: " .. tostring(create_err))
            end

            for i = batch_start, batch_end do
                add_failure(parallel_result, {
                    iteration = i,
                    input_item = items[i]
                }, "Iteration creation failed: " .. tostring(create_err))
            end
            goto continue_batch
        end

        local run_nodes = gather_run_nodes(iterations)
        if #run_nodes > 0 then
            local _, yield_err = n:yield({ run_nodes = run_nodes })
            if yield_err then
                if on_error == parallel.ON_ERROR_STRATEGIES.FAIL_FAST then
                    return fail_iteration(n, "Yield failed: " .. tostring(yield_err))
                end

                for _, iteration in ipairs(iterations) do
                    add_failure(parallel_result, iteration, "Yield failed: " .. tostring(yield_err))
                end
                goto continue_batch
            end
        end

        for _, iteration in ipairs(iterations) do
            local iteration_result, collect_err = parallel._deps.iterator.collect_results(n, iteration)
            if collect_err then
                if on_error == parallel.ON_ERROR_STRATEGIES.FAIL_FAST then
                    return fail_iteration(n, "Iteration failed: " .. tostring(collect_err))
                end
                add_failure(parallel_result, iteration, tostring(collect_err))
                goto continue_iteration
            end

            local processed_result = iteration_result
            if config.item_steps and #config.item_steps > 0 then
                local filtered_out = false

                for _, step in ipairs(config.item_steps) do
                    local step_result, step_err = parallel.execute_item_pipeline_step(step, processed_result)
                    if step_err then
                        local pipeline_err = "Item pipeline failed: " .. tostring(step_err)
                        if on_error == parallel.ON_ERROR_STRATEGIES.FAIL_FAST then
                            return fail_iteration(n, pipeline_err)
                        end

                        add_failure(parallel_result, iteration, pipeline_err)
                        goto continue_iteration
                    end

                    if step.type == "filter" then
                        if not step_result then
                            filtered_out = true
                            break
                        end
                    else
                        processed_result = step_result
                    end
                end

                if filtered_out then
                    goto continue_iteration
                end
            end

            add_success(parallel_result, iteration, processed_result)

            ::continue_iteration::
        end

        ::continue_batch::
    end

    local final_output = parallel_result

    local has_reduction = config.reduction_extract ~= nil or (config.reduction_steps and #config.reduction_steps > 0)
    if has_reduction then
        local reduced_output, reduction_err = process_reduction_pipeline(config, parallel_result, on_error)
        if reduction_err then
            return fail_iteration(n, reduction_err)
        end
        final_output = reduced_output
    else
        local filtered = {}
        if filter == parallel.FILTER_STRATEGIES.SUCCESSES then
            for _, entry in ipairs(parallel_result.successes or {}) do
                if unwrap then
                    table.insert(filtered, entry.result)
                else
                    table.insert(filtered, entry)
                end
            end
        elseif filter == parallel.FILTER_STRATEGIES.FAILURES then
            for _, entry in ipairs(parallel_result.failures or {}) do
                table.insert(filtered, entry)
            end
        else
            for _, entry in ipairs(parallel_result.successes or {}) do
                if unwrap then
                    table.insert(filtered, entry.result)
                else
                    table.insert(filtered, entry)
                end
            end
            for _, entry in ipairs(parallel_result.failures or {}) do
                if unwrap then
                    table.insert(filtered, { error = entry.error })
                else
                    table.insert(filtered, entry)
                end
            end
        end
        final_output = filtered
    end

    return n:complete(final_output, "Parallel processing completed successfully")
end

parallel.run = run
return parallel
