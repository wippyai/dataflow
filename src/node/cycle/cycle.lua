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
    TEMPLATE_EXECUTION_FAILED = "TEMPLATE_EXECUTION_FAILED",
    INVALID_CONTINUATION_CONFIG = "INVALID_CONTINUATION_CONFIG",
    CONTINUATION_EVALUATION_FAILED = "CONTINUATION_EVALUATION_FAILED"
})

cycle._deps = {
    node = require("node"),
    funcs = require("funcs"),
    consts = require("consts"),
    template_graph = require("template_graph"),
    data_reader = require("data_reader"),
    node_reader = require("node_reader"),
    child_output = require("child_output")
}

cycle.DEFAULTS = table.freeze({
    MAX_ITERATIONS = 100,
    INITIAL_STATE = {}
})

cycle.CYCLE_STATE_DATA_TYPE = "cycle.state"
cycle.CYCLE_FUNCTION_RESULT_DATA_TYPE = "cycle.function_result"

local query_node_outputs
local parse_output_rows

local function create_array(size)
    return table.create(size or 0, 0)
end

local function decode_json_content(content: any)
    if type(content) ~= "string" then
        return content
    end

    local decoded, decode_err = json.decode(content)
    if not decode_err then
        return decoded
    end

    return content
end

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

local function persist_state(n, state, iteration_number, last_result)
    n:data(cycle.CYCLE_STATE_DATA_TYPE, {
        _cycle_persisted = true,
        state = state,
        last_result = last_result
    }, {
        node_id = n.node_id,
        key = "cycle_state",
        metadata = {
            iteration = iteration_number,
            last_result = last_result
        }
    })
end

local function unpack_persisted_state(content: any, metadata: any, initial_state: any)
    local decoded_content = decode_json_content(content)
    local state = decoded_content
    local last_result = metadata and metadata.last_result

    if type(last_result) == "string" then
        local decoded_last_result = decode_json_content(last_result)
        if type(decoded_last_result) == "table" then
            last_result = decoded_last_result
        end
    end

    if type(decoded_content) == "table" and decoded_content._cycle_persisted == true then
        state = decoded_content.state
        if decoded_content.last_result ~= nil then
            last_result = decoded_content.last_result
        end
    end

    return state or initial_state, last_result
end

local function is_legacy_persisted_state(content: any)
    local decoded_content = decode_json_content(content)
    return type(decoded_content) == "table" and (decoded_content :: any)._cycle_persisted == nil
end

local function extract_iteration_result(result_row: any)
    if not result_row then
        return nil
    end

    local decoded_content = decode_json_content(result_row.content :: any)
    if type(decoded_content) == "table" then
        return decoded_content.result
    end

    return decoded_content
end

local function load_iteration_function_result(n, iteration_number)
    local ok, rows_or_err = pcall(function()
        return (n:query() :: any)
            :with_data_types(cycle.CYCLE_FUNCTION_RESULT_DATA_TYPE)
            :with_nodes({ n.node_id })
            :with_data_keys("function_result_" .. iteration_number)
            :fetch_options({ replace_references = true })
            :all()
    end)

    if not ok or not rows_or_err or #rows_or_err == 0 then
        return nil
    end

    return extract_iteration_result(rows_or_err[#rows_or_err])
end

local function load_iteration_child_result(n, iteration_number)
    local reader, reader_err = cycle._deps.node_reader.with_dataflow(n.dataflow_id)
    if reader_err then
        return nil
    end

    local child_nodes, nodes_err = reader
        :with_parent_nodes(n.node_id)
        :fetch_options({ config = false })
        :all()

    if nodes_err or not child_nodes or #child_nodes == 0 then
        return nil
    end

    local child_node_ids = {}
    for _, child in ipairs(child_nodes) do
        local child_iteration = tonumber(child.metadata and child.metadata.iteration)
        if child_iteration == iteration_number then
            table.insert(child_node_ids, child.node_id)
        end
    end

    if #child_node_ids == 0 then
        return nil
    end

    local output_data, query_err = query_node_outputs(n, child_node_ids)
    if query_err or not output_data or #output_data == 0 then
        return nil
    end

    return parse_output_rows(output_data)
end

local function rebuild_legacy_last_result(n, iteration_number)
    local function_result = load_iteration_function_result(n, iteration_number)
    if function_result ~= nil then
        return function_result
    end

    return load_iteration_child_result(n, iteration_number)
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
        return initial_state, 1, nil
    end

    if not state_data or #state_data == 0 then
        return initial_state, 1, nil
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
        return initial_state, 1, nil
    end

    local content, last_result = unpack_persisted_state(
        latest_state.content :: any,
        latest_state.metadata :: any,
        initial_state
    )

    if is_legacy_persisted_state(latest_state.content :: any) then
        return content, max_iteration, rebuild_legacy_last_result(n, max_iteration)
    end

    return content, max_iteration + 1, last_result
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
        remapped.data_targets = create_array(#config.data_targets)
        for index, target in ipairs(config.data_targets) do
            local remapped_target = {}
            for tk, tv in pairs(target) do
                remapped_target[tk] = tv
            end

            if target.node_id and uuid_mapping[target.node_id] then
                remapped_target.node_id = uuid_mapping[target.node_id]
            end

            remapped.data_targets[index] = remapped_target
        end
    end

    if config.error_targets then
        remapped.error_targets = create_array(#config.error_targets)
        for index, target in ipairs(config.error_targets) do
            local remapped_target = {}
            for tk, tv in pairs(target) do
                remapped_target[tk] = tv
            end

            if target.node_id and uuid_mapping[target.node_id] then
                remapped_target.node_id = uuid_mapping[target.node_id]
            end

            remapped.error_targets[index] = remapped_target
        end
    end

    return remapped
end

local function parse_content(content: any, content_type: any)
    if content_type == cycle._deps.consts.CONTENT_TYPE.JSON or content_type == "application/json" then
        return decode_json_content(content)
    end

    return content
end

query_node_outputs = function(n, node_ids: {any})
    return cycle._deps.child_output.query_node_outputs(n, node_ids)
end

local function collect_node_ids(uuid_mapping)
    local count = 0
    for _ in pairs(uuid_mapping) do
        count = count + 1
    end

    local node_ids = create_array(count)
    local index = 0
    for _, actual_node_id in pairs(uuid_mapping) do
        index = index + 1
        node_ids[index] = actual_node_id
    end

    return node_ids
end

local function collect_parsed_outputs(output_data)
    local results = create_array(#output_data)
    for index, output in ipairs(output_data) do
        results[index] = {
            key = output.key,
            content = parse_content(output.content :: string, output.content_type :: string),
            node_id = output.node_id,
            discriminator = output.discriminator
        }
    end

    return results
end

parse_output_rows = function(output_data)
    local results = collect_parsed_outputs(output_data)
    if #results == 1 then
        return results[1].content
    end

    return results
end

local function collect_outputs(n, node_ids, yield_result)
    return cycle._deps.child_output.collect_outputs(n, node_ids, yield_result)
end

local function collect_error_message(err)
    if type(err) == "table" then
        return tostring(err.message or err.status or err.code or "Child workflow failed")
    end

    return tostring(err)
end

local function collect_template_outputs(n, uuid_mapping, yield_result)
    local iteration_node_ids = collect_node_ids(uuid_mapping)

    local output_data, query_err = collect_outputs(n, iteration_node_ids, yield_result)
    if query_err then
        return nil, query_err
    end

    if not output_data or #output_data == 0 then
        return nil, "No output data found for template execution"
    end

    return parse_output_rows(output_data), nil
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

    local template_roots = (template_graph :: any):get_roots()
    for _, root_template_id in ipairs(template_roots) do
        local actual_node_id = uuid_mapping[root_template_id]
        n:data(cycle._deps.consts.DATA_TYPE.NODE_INPUT, cycle_context, {
            node_id = actual_node_id,
            key = "default"
        })
    end

    local all_nodes = collect_node_ids(uuid_mapping)

    local yield_result, yield_err = n:yield({ run_nodes = all_nodes })
    if yield_err then
        return nil, "Template execution failed: " .. collect_error_message(yield_err)
    end

    local outputs, collect_err = collect_template_outputs(n, uuid_mapping, yield_result)
    if collect_err then
        return nil, "Template execution failed: " .. collect_error_message(collect_err)
    end

    return outputs, nil
end

-- Collects the (durable) outputs of the given child nodes into a single result
-- (one content, or an array of contents). Used both on the normal path and when
-- resuming an interrupted iteration on recovery.
local function collect_children_result(n, node_ids: {any}, yield_result)
    local output_data, collect_err = collect_outputs(n, node_ids, yield_result)
    if collect_err then
        return nil, "Failed to collect child outputs: " .. collect_error_message(collect_err)
    end

    if output_data and #output_data > 0 then
        local parsed_outputs = collect_parsed_outputs(output_data)
        if #parsed_outputs == 1 then
            return parsed_outputs[1].content, nil
        end

        local contents = create_array(#parsed_outputs)
        for index, output in ipairs(parsed_outputs) do
            contents[index] = output.content
        end
        return contents, nil
    end

    return nil, nil
end

-- Applies a child result over the iteration's working values. A child may return an
-- envelope ({state,result,continue}) or a bare value; mirrors the inline logic used
-- on both the normal and recovery-resume paths.
local function apply_child_result(child_result, current_state, current_result, should_continue, has_explicit_continue)
    if child_result == nil then
        return current_result, current_state, should_continue, has_explicit_continue
    end

    if type(child_result) == "table" then
        local child_result_table = child_result :: any
        local has_envelope = child_result_table.state ~= nil or
            child_result_table.result ~= nil or
            child_result_table.continue ~= nil

        if has_envelope then
            current_state = child_result_table.state or current_state
            if child_result_table.result ~= nil then
                current_result = child_result_table.result
            else
                current_result = child_result_table
            end
            if child_result_table.continue ~= nil then
                should_continue = child_result_table.continue and true or false
                has_explicit_continue = true
            end
        else
            current_result = child_result_table
        end
    else
        current_result = child_result
    end

    return current_result, current_state, should_continue, has_explicit_continue
end

local function process_control_commands(n, control_commands, iteration_number, checkpoint)
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
        -- Checkpoint the in-flight iteration BEFORE yielding (committed by the yield's
        -- submit). On recovery this lets the cycle resume the SAME iteration -- collecting
        -- these existing children -- instead of re-running the user function and creating
        -- duplicate child nodes. Stored in node metadata (single in-place field).
        n:update_metadata({
            cycle_pending = {
                iteration = iteration_number,
                child_node_ids = created_node_ids,
                state = (checkpoint or {}).state,
                should_continue = (checkpoint or {}).should_continue,
                has_explicit_continue = (checkpoint or {}).has_explicit_continue
            }
        })

        local yield_result, yield_err = n:yield({ run_nodes = created_node_ids })
        if yield_err then
            return nil, "Control command execution failed: " .. (yield_err :: string)
        end

        return collect_children_result(n, created_node_ids, yield_result)
    end

    return nil, nil
end

local function resume_children(n, child_node_ids: {any}?)
    return cycle._deps.child_output.resume_children(n, child_node_ids, collect_children_result)
end

local function update_node_metadata(n, metadata_updates)
    if not metadata_updates or type(metadata_updates) ~= "table" then
        return
    end

    n:update_metadata(metadata_updates)
end

local function evaluate_continue_condition(continue_condition, context)
    local should_continue, condition_err = expr.eval(continue_condition :: string, context)
    if condition_err then
        return nil, "Failed to evaluate continue_condition: " .. tostring(condition_err)
    end

    return should_continue and true or false, nil
end

local function evaluate_continue_function(continue_executor, continue_func_id, execution_context, continue_context, events_channel)
    local scoped_executor = continue_executor:with_context(execution_context)
    local continue_result, continue_err_code, continue_err_detail = execute_function_iteration(
        scoped_executor,
        continue_func_id,
        continue_context,
        events_channel
    )

    if continue_err_code then
        return nil, continue_err_detail or continue_err_code
    end

    if type(continue_result) == "table" and continue_result.continue ~= nil then
        return continue_result.continue and true or false, nil
    end

    return continue_result and true or false, nil
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

local function run(args)
    local n, err = cycle._deps.node.new(args) :: any
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
        local template_err
        template_graph, template_err = cycle._deps.template_graph.build_for_node(n)
        if template_err then
            return n:fail({
                code = cycle.ERROR.TEMPLATE_DISCOVERY_FAILED,
                message = "Template discovery failed: " .. (template_err :: string)
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

    local continue_condition = config.continue_condition
    local continue_func_id = config.continue_func_id

    if continue_condition and continue_func_id then
        local validation_message = "Only one continuation method allowed"
        return n:fail({
            code = cycle.ERROR.INVALID_CONTINUATION_CONFIG,
            message = validation_message
        }, validation_message)
    end

    local max_iterations = config.max_iterations or cycle.DEFAULTS.MAX_ITERATIONS
    local initial_state = config.initial_state or cycle.DEFAULTS.INITIAL_STATE

    local inputs, inputs_err = safe_inputs(n)
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

    local current_state, start_iteration, loaded_last_result = load_persisted_state(n, initial_state)
    local last_result = loaded_last_result

    local executor = nil
    local continue_executor = nil
    local base_context = config.context
    local events_channel = process.events()

    if not use_template then
        executor = cycle._deps.funcs.new() :: any
    end

    if continue_func_id then
        continue_executor = cycle._deps.funcs.new() :: any
    end

    -- On recovery, an iteration that yielded for children but never persisted its
    -- completed state is resumed (collecting those children) rather than re-run.
    type CyclePending = {
        iteration: number,
        child_node_ids: {any},
        state: any,
        should_continue: boolean?,
        has_explicit_continue: boolean?,
    }
    local resume_pending: CyclePending? = nil
    local persisted_pending = (n:metadata() or {}).cycle_pending :: CyclePending?
    if type(persisted_pending) == "table" and persisted_pending.iteration == start_iteration then
        resume_pending = persisted_pending
    end

    for iteration_number = start_iteration, max_iterations do
        local should_continue = false
        local has_explicit_continue = false
        local current_result = nil
        local control_commands = nil

        if resume_pending and iteration_number == resume_pending.iteration then
            -- Resume the interrupted iteration: collect its existing children's outputs
            -- without re-running the user function (so no duplicate child nodes are made).
            current_state = resume_pending.state or current_state
            should_continue = resume_pending.should_continue and true or false
            has_explicit_continue = resume_pending.has_explicit_continue and true or false
            control_commands = resume_pending.child_node_ids

            local child_result, cmd_err = resume_children(n, resume_pending.child_node_ids)
            if cmd_err then
                return n:fail({
                    code = cycle.ERROR.FUNCTION_EXECUTION_FAILED,
                    message = cmd_err
                }, "Failed to resume control commands in iteration " .. iteration_number)
            end

            current_result, current_state, should_continue, has_explicit_continue =
                apply_child_result(child_result, current_state, current_result, should_continue,
                    has_explicit_continue)
            last_result = current_result
            resume_pending = nil
        else
            local iteration_result, iter_err, iter_err_detail

            if use_template then
                iteration_result, iter_err = execute_template_iteration(
                    n, template_graph :: any, current_state, last_result,
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

            local new_state = current_state
            local metadata_updates = nil

            if type(iteration_result) == "table" then
                local iter_table = iteration_result :: any
                new_state = iter_table.state or current_state
                current_result = iter_table.result

                if iter_table.continue ~= nil then
                    should_continue = iter_table.continue
                    has_explicit_continue = true
                else
                    should_continue = (new_state ~= current_state)
                end

                if iter_table._control and iter_table._control.commands then
                    control_commands = iter_table._control.commands
                end

                metadata_updates = iter_table._metadata
            else
                current_result = iteration_result
                should_continue = false
            end

            current_state = new_state

            update_node_metadata(n, metadata_updates)

            if control_commands then
                local child_result, cmd_err = process_control_commands(n, control_commands, iteration_number, {
                    state = current_state,
                    should_continue = should_continue,
                    has_explicit_continue = has_explicit_continue
                })
                if cmd_err then
                    return n:fail({
                        code = cycle.ERROR.FUNCTION_EXECUTION_FAILED,
                        message = cmd_err
                    }, "Failed to execute control commands in iteration " .. iteration_number)
                end

                current_result, current_state, should_continue, has_explicit_continue =
                    apply_child_result(child_result, current_state, current_result, should_continue,
                        has_explicit_continue)
                last_result = current_result
            else
                last_result = current_result
            end
        end

        -- persist iteration state AFTER children have contributed their results.
        -- persisting before children runs would let a mid-iteration crash burn the
        -- iteration count without ever applying the child's output on restart.
        -- also persist last_result so recovery can replay the user function with
        -- the prior child's output and not lose its contribution.
        persist_state(n, current_state, iteration_number, last_result)
        n:submit()

        local continuation_context = {
            input = original_input,
            state = current_state,
            result = current_result,
            last_result = last_result,
            iteration = iteration_number
        }

        if not has_explicit_continue then
            if continue_condition then
                local evaluated_continue, continue_err = evaluate_continue_condition(continue_condition, continuation_context)
                if continue_err then
                    return n:fail({
                        code = cycle.ERROR.CONTINUATION_EVALUATION_FAILED,
                        message = continue_err
                    }, continue_err)
                end
                should_continue = evaluated_continue
            elseif continue_func_id then
                local continue_context = build_cycle_context(
                    base_context,
                    n.dataflow_id,
                    n.node_id,
                    n.path,
                    iteration_number
                )

                local evaluated_continue, continue_err = evaluate_continue_function(
                    continue_executor :: any,
                    continue_func_id,
                    continue_context,
                    continuation_context,
                    events_channel
                )
                if continue_err then
                    return n:fail({
                        code = cycle.ERROR.CONTINUATION_EVALUATION_FAILED,
                        message = "Continue function failed: " .. continue_err
                    }, "Continue function failed: " .. continue_err)
                end
                should_continue = evaluated_continue
            end
        end

        if should_continue and iteration_number >= max_iterations then
            local max_iterations_message = "Maximum iterations (" .. max_iterations .. ") exceeded"
            return n:fail({
                code = cycle.ERROR.MAX_ITERATIONS_EXCEEDED,
                message = max_iterations_message
            }, max_iterations_message)
        end

        if not should_continue then
            local final_result = control_commands and last_result or current_result
            return n:complete(final_result, "Cycle completed after " .. iteration_number .. " iterations")
        end
    end

    return n:fail({
        code = cycle.ERROR.MAX_ITERATIONS_EXCEEDED,
        message = "Maximum iterations (" .. max_iterations .. ") exceeded"
    }, "Cycle exceeded maximum iterations limit")
end

cycle.run = run
cycle._load_persisted_state = load_persisted_state
return cycle
