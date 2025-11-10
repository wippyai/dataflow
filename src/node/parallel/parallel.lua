local parallel = {}

parallel._deps = {
    node = require("node"),
    template_graph = require("template_graph"),
    iterator = require("iterator"),
    uuid = require("uuid"),
    consts = require("consts"),
    data_reader = require("data_reader"),
    json = require("json")
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
    PASSTHROUGH_MATERIALIZATION_FAILED = "PASSTHROUGH_MATERIALIZATION_FAILED"
}

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

local function process_batch(n, template_graph, items, batch_start, batch_end, iteration_input_key, passthrough_inputs)
    local iterations, create_err = parallel._deps.iterator.create_batch(
        n, template_graph, items, batch_start, batch_end, iteration_input_key, passthrough_inputs
    )

    if create_err then
        return nil, create_err
    end

    local all_template_nodes = {}
    for _, iteration in ipairs(iterations) do
        for _, node_id in pairs(iteration.uuid_mapping) do
            table.insert(all_template_nodes, node_id)
        end
    end

    local yield_results, yield_err = n:yield({ run_nodes = all_template_nodes })
    if yield_err then
        return nil, "Yield failed: " .. yield_err
    end

    local batch_failure_count = 0
    for _, iteration in ipairs(iterations) do
        local success, err = parallel._deps.iterator.collect_results(n, iteration)
        if not success then
            batch_failure_count = batch_failure_count + 1
        end
    end

    return batch_failure_count, nil
end

local function run(args)
    local uuid = parallel._deps.uuid
    local consts = parallel._deps.consts

    local n, err = parallel._deps.node.new(args)
    if err then
        error(err)
    end

    local config = n:config()

    local source_array_key = config.source_array_key
    if not source_array_key or source_array_key == "" then
        return n:fail({
            code = parallel.ERRORS.MISSING_SOURCE_ARRAY_KEY,
            message = "source_array_key is required in parallel configuration"
        }, "Missing source_array_key in config")
    end

    local iteration_input_key = config.iteration_input_key or parallel.DEFAULTS.ITERATION_INPUT_KEY
    local on_error = config.on_error or parallel.DEFAULTS.ON_ERROR
    local filter = config.filter or parallel.DEFAULTS.FILTER
    local unwrap = config.unwrap
    if unwrap == nil then
        unwrap = parallel.DEFAULTS.UNWRAP
    end
    local batch_size = config.batch_size or parallel.DEFAULTS.BATCH_SIZE
    local passthrough_keys = config.passthrough_keys or {}

    if not validate_on_error_strategy(on_error) then
        return n:fail({
            code = parallel.ERRORS.INVALID_ON_ERROR_STRATEGY,
            message = "Invalid on_error: " .. tostring(on_error) ..
                ". Valid options: " .. parallel.ON_ERROR_STRATEGIES.FAIL_FAST ..
                ", " .. parallel.ON_ERROR_STRATEGIES.CONTINUE
        }, "Invalid on_error strategy")
    end

    if not validate_filter_strategy(filter) then
        return n:fail({
            code = parallel.ERRORS.INVALID_FILTER_STRATEGY,
            message = "Invalid filter: " .. tostring(filter) ..
                ". Valid options: " .. parallel.FILTER_STRATEGIES.ALL ..
                ", " .. parallel.FILTER_STRATEGIES.SUCCESSES ..
                ", " .. parallel.FILTER_STRATEGIES.FAILURES
        }, "Invalid filter strategy")
    end

    if not validate_batch_size(batch_size) then
        return n:fail({
            code = parallel.ERRORS.INVALID_BATCH_SIZE,
            message = "batch_size must be a positive number <= 1000, got: " .. tostring(batch_size)
        }, "Invalid batch size")
    end

    local inputs, inputs_err = n:inputs()
    if inputs_err then
        return n:fail({
            code = "INPUT_VALIDATION_FAILED",
            message = inputs_err
        }, inputs_err)
    end

    local input_data = nil

    if inputs.default then
        input_data = inputs.default.content
    else
        input_data = {}
        for key, i_data in pairs(inputs) do
            input_data[key] = i_data.content
        end
    end

    if input_data == nil then
        return n:fail({
            code = parallel.ERRORS.NO_INPUT_DATA,
            message = "No input data provided for parallel node"
        }, "Parallel node requires input data")
    end

    local items_to_process = nil
    if type(input_data) == "table" and input_data[source_array_key] then
        items_to_process = input_data[source_array_key]
    else
        return n:fail({
            code = parallel.ERRORS.INVALID_INPUT_STRUCTURE,
            message = "Input data must contain '" .. source_array_key .. "' field with array"
        }, "Invalid input structure for parallel")
    end

    if type(items_to_process) ~= "table" or #items_to_process == 0 then
        return n:fail({
            code = parallel.ERRORS.INVALID_INPUT_STRUCTURE,
            message = "Field '" .. source_array_key .. "' must be a non-empty array"
        }, "Invalid input structure for parallel")
    end

    local passthrough_inputs = {}
    if type(passthrough_keys) == "table" and #passthrough_keys > 0 then
        for _, key in ipairs(passthrough_keys) do
            local input = inputs[key]
            if input then
                local ref_target

                if input.data_id then
                    if input.content_type == "dataflow/reference" then
                        ref_target = {
                            node_id = input.content.node_id,
                            data_id = input.content.data_id
                        }
                    else
                        ref_target = {
                            node_id = n.node_id,
                            data_id = input.data_id
                        }
                    end
                else
                    local materialized_id = uuid.v7()
                    n:data(consts.DATA_TYPE.NODE_INPUT, input.content, {
                        data_id = materialized_id,
                        discriminator = key,
                        node_id = n.node_id,
                        content_type = type(input.content) == "table" and
                            consts.CONTENT_TYPE.JSON or
                            consts.CONTENT_TYPE.TEXT
                    })

                    ref_target = {
                        node_id = n.node_id,
                        data_id = materialized_id
                    }
                end

                passthrough_inputs[key] = ref_target
            end
        end

        local submit_success, submit_err = n:submit()
        if not submit_success then
            return n:fail({
                code = parallel.ERRORS.PASSTHROUGH_MATERIALIZATION_FAILED,
                message = "Failed to materialize passthrough inputs: " .. (submit_err or "unknown")
            }, "Failed to materialize passthrough data")
        end
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

    local total_iterations = #items_to_process

    for batch_start = 1, total_iterations, batch_size do
        local batch_end = math.min(batch_start + batch_size - 1, total_iterations)

        local batch_failure_count, batch_err = process_batch(
            n, template_graph, items_to_process, batch_start, batch_end,
            iteration_input_key, passthrough_inputs
        )

        if batch_err then
            return n:fail({
                code = parallel.ERRORS.ITERATION_FAILED,
                message = "Batch processing failed: " .. batch_err
            }, "Parallel batch processing failed")
        end

        local submit_success, submit_err = n:submit()
        if not submit_success then
            return n:fail({
                code = parallel.ERRORS.ITERATION_FAILED,
                message = "Failed to submit batch results: " .. (submit_err or "unknown")
            }, "Failed to submit batch")
        end

        if batch_failure_count > 0 and on_error == parallel.ON_ERROR_STRATEGIES.FAIL_FAST then
            local all_data = parallel._deps.data_reader.with_dataflow(n.dataflow_id)
                :with_nodes(n.node_id)
                :with_data_types({consts.DATA_TYPE.ITERATION_RESULT, consts.DATA_TYPE.ITERATION_ERROR})
                :order_by("discriminator", "ASC")
                :all()

            for _, data in ipairs(all_data) do
                if data.content_type == consts.CONTENT_TYPE.JSON and type(data.content) == "string" then
                    local parsed, parse_err = parallel._deps.json.decode(data.content)
                    if not parse_err then
                        data.content = parsed
                    end
                end
            end

            local partial_results = table.create(#all_data, 0)
            for i, data in ipairs(all_data) do
                local is_error = data.type == consts.DATA_TYPE.ITERATION_ERROR

                if unwrap then
                    partial_results[i] = data.content
                else
                    if is_error then
                        partial_results[i] = {
                            iteration = data.metadata.iteration,
                            error = data.content
                        }
                    else
                        partial_results[i] = {
                            iteration = data.metadata.iteration,
                            result = data.content
                        }
                    end
                end
            end

            return n:fail({
                code = parallel.ERRORS.ITERATION_FAILED,
                message = "Iteration failed",
                partial_results = partial_results
            }, "Parallel failed due to iteration failure")
        end
    end

    local all_data = parallel._deps.data_reader.with_dataflow(n.dataflow_id)
        :with_nodes(n.node_id)
        :with_data_types({consts.DATA_TYPE.ITERATION_RESULT, consts.DATA_TYPE.ITERATION_ERROR})
        :order_by("discriminator", "ASC")
        :all()

    for _, data in ipairs(all_data) do
        if data.content_type == consts.CONTENT_TYPE.JSON and type(data.content) == "string" then
            local parsed, parse_err = parallel._deps.json.decode(data.content)
            if not parse_err then
                data.content = parsed
            end
        end
    end

    local results = table.create(#all_data, 0)
    local errors = table.create(#all_data, 0)
    local result_count = 0
    local error_count = 0

    for _, data in ipairs(all_data) do
        if data.type == consts.DATA_TYPE.ITERATION_ERROR then
            error_count = error_count + 1
            errors[error_count] = data
        else
            result_count = result_count + 1
            results[result_count] = data
        end
    end

    local final_output
    if filter == parallel.FILTER_STRATEGIES.SUCCESSES then
        final_output = table.create(result_count, 0)
        for i = 1, result_count do
            if unwrap then
                final_output[i] = results[i].content
            else
                final_output[i] = {
                    iteration = results[i].metadata.iteration,
                    result = results[i].content
                }
            end
        end
    elseif filter == parallel.FILTER_STRATEGIES.FAILURES then
        final_output = table.create(error_count, 0)
        for i = 1, error_count do
            if unwrap then
                final_output[i] = errors[i].content
            else
                final_output[i] = {
                    iteration = errors[i].metadata.iteration,
                    error = errors[i].content
                }
            end
        end
    else
        final_output = table.create(#all_data, 0)
        local output_idx = 1

        for i = 1, result_count do
            if unwrap then
                final_output[output_idx] = results[i].content
            else
                final_output[output_idx] = {
                    iteration = results[i].metadata.iteration,
                    result = results[i].content
                }
            end
            output_idx = output_idx + 1
        end

        for i = 1, error_count do
            if unwrap then
                final_output[output_idx] = errors[i].content
            else
                final_output[output_idx] = {
                    iteration = errors[i].metadata.iteration,
                    error = errors[i].content
                }
            end
            output_idx = output_idx + 1
        end
    end

    return n:complete(final_output, "Parallel processing completed successfully")
end

parallel.run = run
return parallel
