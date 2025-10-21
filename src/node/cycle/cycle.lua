local json = require("json")
local uuid = require("uuid")
local expr = require("expr")

local cycle = {}

cycle.ERROR = table.freeze({
    MISSING_FUNC_ID = "MISSING_FUNC_ID",
    NO_INPUT_DATA = "NO_INPUT_DATA",
    FUNCTION_CANCELED = "FUNCTION_CANCELED",
    FUNCTION_EXECUTION_FAILED = "FUNCTION_EXECUTION_FAILED",
    MAX_ITERATIONS_EXCEEDED = "MAX_ITERATIONS_EXCEEDED",
    TEMPLATE_DISCOVERY_FAILED = "TEMPLATE_DISCOVERY_FAILED",
    NO_TEMPLATES = "NO_TEMPLATES",
    TEMPLATE_EXECUTION_FAILED = "TEMPLATE_EXECUTION_FAILED"
})

cycle._deps = {
    node = require("node"),
    funcs = require("funcs"),
    consts = require("consts"),
    template_graph = require("template_graph"),
    data_reader = require("data_reader")
}

cycle.DEFAULTS = table.freeze({
    MAX_ITERATIONS = 100,
    INITIAL_STATE = {}
})

cycle.CYCLE_STATE_DATA_TYPE = "cycle.state"
cycle.CYCLE_FUNCTION_RESULT_DATA_TYPE = "cycle.function_result"

local function build_cycle_context(base_context, dataflow_id, node_id, path, iteration_number)
    local execution_context = {}

    if base_context then
        for k, v in pairs(base_context) do
            execution_context[k] = v
        end
    end

    execution_context.dataflow_id = dataflow_id
    execution_context.node_id = node_id
    execution_context.path = path or {}
    execution_context.iteration = iteration_number

    return execution_context
end

local function persist_state(n, state, iteration_number)
    n:data(cycle.CYCLE_STATE_DATA_TYPE, state, {
        node_id = n.node_id,
        key = "cycle_state",
        metadata = {
            iteration = iteration_number
        }
    })
end

local function persist_function_result(n, result, iteration_number)
    n:data(cycle.CYCLE_FUNCTION_RESULT_DATA_TYPE, result, {
        node_id = n.node_id,
        key = "function_result_" .. iteration_number,
        metadata = {
            iteration = iteration_number,
            timestamp = os.time()
        }
    })
end

local function load_persisted_state(n, initial_state)
    local state_data, query_err = n:query()
        :with_data_types(cycle.CYCLE_STATE_DATA_TYPE)
        :with_nodes({ n.node_id })
        :with_data_keys("cycle_state")
        :fetch_options({ replace_references = true })
        :all()

    if query_err then
        return initial_state, 1
    end

    if not state_data or #state_data == 0 then
        return initial_state, 1
    end

    local latest_state = nil
    local max_iteration = 0

    for _, data in ipairs(state_data) do
        local iteration = (data.metadata and data.metadata.iteration) or 0
        if iteration > max_iteration then
            max_iteration = iteration
            latest_state = data
        end
    end

    if not latest_state then
        return initial_state, 1
    end

    return latest_state.content or initial_state, max_iteration + 1
end

local function execute_function_iteration(executor, func_id, context, events_channel)
    local command = executor:async(func_id, context)
    local response_channel = command:response()

    local result = channel.select({
        response_channel:case_receive(),
        events_channel:case_receive()
    })

    if result.channel == events_channel then
        local event = result.value
        if event.kind == process.event.CANCEL then
            command:cancel()
            return nil, cycle.ERROR.FUNCTION_CANCELED, "Function execution was canceled by system event"
        end
    end

    if command:is_canceled() then
        return nil, cycle.ERROR.FUNCTION_CANCELED, "Function execution was canceled"
    end

    local payload, result_err = command:result()
    if result_err then
        return nil, cycle.ERROR.FUNCTION_EXECUTION_FAILED, "Function execution failed: " .. result_err
    end

    return payload:data(), nil, nil
end

local function remap_template_config(config, uuid_mapping)
    if not config then
        return {}
    end

    local remapped = {}
    for k, v in pairs(config) do
        remapped[k] = v
    end

    if config.data_targets then
        remapped.data_targets = {}
        for _, target in ipairs(config.data_targets) do
            local remapped_target = {}
            for tk, tv in pairs(target) do
                remapped_target[tk] = tv
            end

            if target.node_id and uuid_mapping[target.node_id] then
                remapped_target.node_id = uuid_mapping[target.node_id]
            end

            table.insert(remapped.data_targets, remapped_target)
        end
    end

    if config.error_targets then
        remapped.error_targets = {}
        for _, target in ipairs(config.error_targets) do
            local remapped_target = {}
            for tk, tv in pairs(target) do
                remapped_target[tk] = tv
            end

            if target.node_id and uuid_mapping[target.node_id] then
                remapped_target.node_id = uuid_mapping[target.node_id]
            end

            table.insert(remapped.error_targets, remapped_target)
        end
    end

    return remapped
end

local function parse_content(content, content_type)
    if (content_type == cycle._deps.consts.CONTENT_TYPE.JSON or content_type == "application/json")
        and type(content) == "string" then
        local parsed, err = json.decode(content)
        if not err then
            return parsed
        end
    end
    return content
end

local function collect_template_outputs(n, uuid_mapping, template_graph)
    local iteration_node_ids = {}
    for _, actual_node_id in pairs(uuid_mapping) do
        table.insert(iteration_node_ids, actual_node_id)
    end

    local reader, reader_err = cycle._deps.data_reader.with_dataflow(n.dataflow_id)
    if reader_err then
        return nil, "Failed to create data reader: " .. reader_err
    end

    local output_data, query_err = reader
        :with_nodes(iteration_node_ids)
        :with_data_types(cycle._deps.consts.DATA_TYPE.NODE_OUTPUT)
        :fetch_options({ replace_references = true })
        :all()

    if query_err then
        return nil, "Failed to query output data: " .. query_err
    end

    if #output_data == 0 then
        return nil, "No output data found for template execution"
    end

    local results = {}
    for _, output in ipairs(output_data) do
        local parsed_content = parse_content(output.content, output.content_type)
        table.insert(results, {
            key = output.key,
            content = parsed_content,
            node_id = output.node_id,
            discriminator = output.discriminator
        })
    end

    if #results == 1 then
        return results[1].content, nil
    else
        return results, nil
    end
end

local function execute_template_iteration(n, template_graph, current_state, last_result, iteration_number, original_input)
    local uuid_mapping = {}

    local template_ids = {}
    for template_id, _ in pairs(template_graph.nodes) do
        table.insert(template_ids, template_id)
    end
    table.sort(template_ids)

    for _, template_id in ipairs(template_ids) do
        uuid_mapping[template_id] = uuid.v7()
    end

    for _, template_id in ipairs(template_ids) do
        local template = template_graph.nodes[template_id]
        local actual_node_id = uuid_mapping[template_id]

        local remapped_config = remap_template_config(template.config, uuid_mapping)

        local merged_metadata = {}
        if template.metadata then
            for k, v in pairs(template.metadata) do
                merged_metadata[k] = v
            end
        end

        merged_metadata.iteration = iteration_number
        merged_metadata.template_source = template_id
        merged_metadata.cycle_iteration = true

        if merged_metadata.title then
            merged_metadata.title = merged_metadata.title .. " (Cycle #" .. iteration_number .. ")"
        end

        n:command({
            type = cycle._deps.consts.COMMAND_TYPES.CREATE_NODE,
            payload = {
                node_id = actual_node_id,
                node_type = template.type,
                parent_node_id = n.node_id,
                status = cycle._deps.consts.STATUS.PENDING,
                config = remapped_config,
                metadata = merged_metadata
            }
        })
    end

    local cycle_context = {
        input = (iteration_number == 1) and original_input or nil,
        state = current_state,
        last_result = last_result,
        iteration = iteration_number
    }

    local template_roots = template_graph:get_roots()
    for _, root_template_id in ipairs(template_roots) do
        local actual_node_id = uuid_mapping[root_template_id]
        n:data(cycle._deps.consts.DATA_TYPE.NODE_INPUT, cycle_context, {
            node_id = actual_node_id,
            key = "default"
        })
    end

    local all_nodes = {}
    for _, node_id in pairs(uuid_mapping) do
        table.insert(all_nodes, node_id)
    end

    local yield_result, yield_err = n:yield({ run_nodes = all_nodes })
    if yield_err then
        return nil, "Template execution failed: " .. yield_err
    end

    local outputs, collect_err = collect_template_outputs(n, uuid_mapping, template_graph)
    if collect_err then
        return nil, collect_err
    end

    return outputs, nil
end

local function process_control_commands(n, control_commands, iteration_number)
    if not control_commands or type(control_commands) ~= "table" or #control_commands == 0 then
        return nil, nil
    end

    local created_node_ids = {}

    for i, cmd in ipairs(control_commands) do
        if cmd.type and cmd.payload then
            if cmd.type == cycle._deps.consts.COMMAND_TYPES.CREATE_NODE then
                if not cmd.payload.metadata then
                    cmd.payload.metadata = {}
                end
                cmd.payload.metadata.iteration = iteration_number
            end

            n:command(cmd)

            if cmd.type == cycle._deps.consts.COMMAND_TYPES.CREATE_NODE and cmd.payload.node_id then
                table.insert(created_node_ids, cmd.payload.node_id)
            end
        end
    end

    if #created_node_ids > 0 then
        local yield_result, yield_err = n:yield({ run_nodes = created_node_ids })
        if yield_err then
            return nil, "Control command execution failed: " .. yield_err
        end

        local output_data, collect_err = n:query()
            :with_nodes(created_node_ids)
            :with_data_types(cycle._deps.consts.DATA_TYPE.NODE_OUTPUT)
            :fetch_options({ replace_references = true })
            :all()

        if collect_err then
            return nil, "Failed to collect child outputs: " .. collect_err
        end

        if output_data and #output_data > 0 then
            if #output_data == 1 then
                local content = output_data[1].content

                if output_data[1].content_type == "application/json" or
                    output_data[1].content_type == cycle._deps.consts.CONTENT_TYPE.JSON then
                    if type(content) == "string" then
                        local parsed, parse_err = json.decode(content)
                        if not parse_err and parsed then
                            return parsed, nil
                        end
                    end
                end

                return content, nil
            else
                local results = {}
                for _, output in ipairs(output_data) do
                    local content = output.content

                    if output.content_type == "application/json" or
                        output.content_type == cycle._deps.consts.CONTENT_TYPE.JSON then
                        if type(content) == "string" then
                            local parsed, parse_err = json.decode(content)
                            if not parse_err and parsed then
                                content = parsed
                            end
                        end
                    end

                    table.insert(results, content)
                end
                return results, nil
            end
        end
    end

    return nil, nil
end

local function update_node_metadata(n, metadata_updates)
    if not metadata_updates or type(metadata_updates) ~= "table" then
        return
    end

    n:update_metadata(metadata_updates)
end

local function run(args)
    local n, err = cycle._deps.node.new(args)
    if err then
        error(err)
    end

    local config = n:config()

    local func_id = config.func_id
    local use_template = false
    local template_graph = nil

    if func_id then
        use_template = false
    else
        template_graph, template_err = cycle._deps.template_graph.build_for_node(n)
        if template_err then
            return n:fail({
                code = cycle.ERROR.TEMPLATE_DISCOVERY_FAILED,
                message = "Template discovery failed: " .. template_err
            }, "Failed to discover template nodes")
        end

        if template_graph:is_empty() then
            return n:fail({
                code = cycle.ERROR.MISSING_FUNC_ID,
                message = "Cycle requires either func_id or template nodes"
            }, "No execution target specified")
        end

        use_template = true
    end

    local max_iterations = config.max_iterations or cycle.DEFAULTS.MAX_ITERATIONS
    local initial_state = config.initial_state or cycle.DEFAULTS.INITIAL_STATE

    local inputs, inputs_err = n:inputs()
    if inputs_err then
        return n:fail({
            code = "INPUT_VALIDATION_FAILED",
            message = inputs_err
        }, inputs_err)
    end

    local original_input = nil

    if next(inputs) == nil then
        return n:fail({
            code = cycle.ERROR.NO_INPUT_DATA,
            message = "No input data provided for cycle node"
        }, "Cycle node requires input data")
    elseif inputs.default then
        original_input = inputs.default.content
    elseif inputs[""] then
        original_input = inputs[""].content
    else
        local input_count = 0
        for _ in pairs(inputs) do
            input_count = input_count + 1
        end

        if input_count == 1 then
            for _, input in pairs(inputs) do
                original_input = input.content
                break
            end
        else
            original_input = {}
            for key, input in pairs(inputs) do
                original_input[key] = input.content
            end
        end
    end

    local current_state, start_iteration = load_persisted_state(n, initial_state)
    local last_result = nil

    local executor = nil
    local base_context = config.context
    local events_channel = process.events()

    if not use_template then
        executor = cycle._deps.funcs.new()
    end

    for iteration_number = start_iteration, max_iterations do
        local iteration_result, iter_err, iter_err_detail

        if use_template then
            iteration_result, iter_err = execute_template_iteration(
                n, template_graph, current_state, last_result,
                iteration_number, original_input
            )
            iter_err_detail = iter_err
        else
            local execution_context = build_cycle_context(
                base_context,
                n.dataflow_id,
                n.node_id,
                n.path,
                iteration_number
            )

            executor = executor:with_context(execution_context)

            local function_context = {
                input = (iteration_number == 1) and original_input or nil,
                state = current_state,
                last_result = last_result,
                iteration = iteration_number
            }

            iteration_result, iter_err, iter_err_detail = execute_function_iteration(
                executor, func_id, function_context, events_channel
            )

            if iteration_result then
                persist_function_result(n, iteration_result, iteration_number)
            end
        end

        if iter_err then
            local error_code = iter_err
            local error_message = iter_err_detail or iter_err

            return n:fail({
                code = error_code,
                message = error_message
            }, "Execution failed in iteration " .. iteration_number .. ": " .. error_message)
        end

        local should_continue = false
        local new_state = current_state
        local current_result = nil
        local control_commands = nil
        local metadata_updates = nil

        if type(iteration_result) == "table" then
            new_state = iteration_result.state or current_state
            current_result = iteration_result.result

            if iteration_result.continue ~= nil then
                should_continue = iteration_result.continue
            else
                should_continue = (new_state ~= current_state)
            end

            if iteration_result._control and iteration_result._control.commands then
                control_commands = iteration_result._control.commands
            end

            metadata_updates = iteration_result._metadata
        else
            current_result = iteration_result
            should_continue = false
        end

        current_state = new_state

        update_node_metadata(n, metadata_updates)
        persist_state(n, current_state, iteration_number)

        if control_commands then
            local child_result, cmd_err = process_control_commands(n, control_commands, iteration_number)
            if cmd_err then
                return n:fail({
                    code = cycle.ERROR.FUNCTION_EXECUTION_FAILED,
                    message = cmd_err
                }, "Failed to execute control commands in iteration " .. iteration_number)
            end

            if type(child_result) == "table" then
                new_state = child_result.state or new_state
                current_result = child_result.result
                if child_result.continue ~= nil then
                    should_continue = child_result.continue
                end
                current_state = new_state
            end

            last_result = current_result
        else
            last_result = current_result
        end

        if not should_continue then
            local final_result = control_commands and last_result or current_result
            return n:complete(final_result, "Cycle completed after " .. iteration_number .. " iterations")
        end

        if iteration_number >= max_iterations then
            local final_result = control_commands and last_result or current_result
            return n:complete(final_result, "Cycle completed at maximum iterations (" .. max_iterations .. ")")
        end
    end

    return n:fail({
        code = cycle.ERROR.MAX_ITERATIONS_EXCEEDED,
        message = "Maximum iterations (" .. max_iterations .. ") exceeded"
    }, "Cycle exceeded maximum iterations limit")
end

cycle.run = run
return cycle