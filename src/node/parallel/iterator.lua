local uuid = require("uuid")
local json = require("json")
local time = require("time")
local consts = require("consts")

local default_deps = {
    data_reader = require("data_reader")
}

local iterator = {}

local function build_iteration_discriminator(iteration_index, attempt_id)
    local padded_iteration = string.format("%06d", iteration_index)
    if type(attempt_id) == "string" and attempt_id ~= "" then
        return "iteration." .. padded_iteration .. "." .. attempt_id
    end
    return "iteration." .. padded_iteration
end

function iterator.remap_template_config(config, uuid_mapping)
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

function iterator.redirect_terminals_to_parent(config, parent_node_id, iteration_index, source_node_id, attempt_id)
    local iteration_discriminator = build_iteration_discriminator(iteration_index, attempt_id)
    local new_data_targets = {}
    for _, target in ipairs(config.data_targets or {}) do
        if not target.node_id and target.data_type == consts.DATA_TYPE.NODE_OUTPUT then
            table.insert(new_data_targets, {
                data_type = consts.DATA_TYPE.ITERATION_RESULT,
                node_id = parent_node_id,
                discriminator = iteration_discriminator,
                content_type = target.content_type,
                metadata = {
                    source_node_id = source_node_id,
                    iteration = iteration_index,
                    attempt_id = attempt_id
                }
            })
        else
            table.insert(new_data_targets, target)
        end
    end

    local new_error_targets = {}
    for _, target in ipairs(config.error_targets or {}) do
        if not target.node_id and target.data_type == consts.DATA_TYPE.NODE_OUTPUT then
            table.insert(new_error_targets, {
                data_type = consts.DATA_TYPE.ITERATION_ERROR,
                node_id = parent_node_id,
                discriminator = iteration_discriminator,
                key = target.key or "error",
                content_type = target.content_type,
                metadata = {
                    source_node_id = source_node_id,
                    iteration = iteration_index,
                    attempt_id = attempt_id
                }
            })
        else
            table.insert(new_error_targets, target)
        end
    end

    config.data_targets = new_data_targets
    config.error_targets = new_error_targets
    return config
end

function iterator.create_iteration(parent_node, template_graph, input_item, iteration_index, iteration_input_key,
                                   passthrough_inputs, options, deps)
    if deps == nil and options ~= nil and options.data_reader ~= nil then
        deps = options
        options = nil
    end

    options = options or {}
    deps = deps or default_deps
    local attempt_id = options.attempt_id

    local uuid_mapping = {}

    local template_ids = {}
    for template_id, _ in pairs(template_graph.nodes) do
        table.insert(template_ids, template_id)
    end
    table.sort(template_ids)

    for _, template_id in ipairs(template_ids) do
        uuid_mapping[template_id] = uuid.v7()
    end

    local child_path = {}
    for _, ancestor_id in ipairs(parent_node.path or {}) do
        table.insert(child_path, ancestor_id)
    end
    table.insert(child_path, parent_node.node_id)

    local root_nodes = {}
    local template_roots = template_graph:get_roots()

    for _, template_id in ipairs(template_ids) do
        local template = template_graph.nodes[template_id]
        local actual_node_id = uuid_mapping[template_id]

        local remapped_config = iterator.remap_template_config(template.config, uuid_mapping)

        remapped_config = iterator.redirect_terminals_to_parent(
            remapped_config,
            parent_node.node_id,
            iteration_index,
            actual_node_id,
            attempt_id
        )

        local merged_metadata = {}

        if template.metadata then
            for k, v in pairs(template.metadata) do
                merged_metadata[k] = v
            end
        end

        merged_metadata.iteration = iteration_index
        merged_metadata.template_source = template_id

        parent_node:command({
            type = consts.COMMAND_TYPES.CREATE_NODE,
            payload = {
                node_id = actual_node_id,
                node_type = template.type,
                parent_node_id = parent_node.node_id,
                status = consts.STATUS.PENDING,
                config = remapped_config,
                metadata = merged_metadata
            }
        })

        local is_root = false
        for _, root_template_id in ipairs(template_roots) do
            if template_id == root_template_id then
                is_root = true
                break
            end
        end

        if is_root then
            table.insert(root_nodes, actual_node_id)

            parent_node:data(consts.DATA_TYPE.NODE_INPUT, input_item, {
                node_id = actual_node_id,
                discriminator = iteration_input_key
            })

            if passthrough_inputs then
                for input_key, ref_target in pairs(passthrough_inputs) do
                    parent_node:data(consts.DATA_TYPE.NODE_INPUT, {
                        node_id = ref_target.node_id,
                        data_id = ref_target.data_id
                    }, {
                        node_id = actual_node_id,
                        key = ref_target.data_id,
                        discriminator = input_key,
                        content_type = consts.CONTENT_TYPE.REFERENCE
                    })
                end
            end
        end
    end

    return {
        iteration = iteration_index,
        input_item = input_item,
        uuid_mapping = uuid_mapping,
        root_nodes = root_nodes,
        child_path = child_path,
        attempt_id = attempt_id
    }
end

function iterator.create_batch(parent_node, template_graph, items, batch_start, batch_end, iteration_input_key,
                               passthrough_inputs, options, deps)
    if deps == nil and options ~= nil and options.data_reader ~= nil then
        deps = options
        options = nil
    end

    options = options or {}
    deps = deps or default_deps

    if not parent_node or not template_graph or not items then
        return nil, "Missing required parameters"
    end

    if batch_start < 1 or batch_end > #items or batch_start > batch_end then
        return nil, "Invalid batch range"
    end

    local iterations = {}
    local selected_iterations = options.iteration_indices
    local indices = {}

    if type(selected_iterations) == "table" and #selected_iterations > 0 then
        for _, iteration_index in ipairs(selected_iterations) do
            if type(iteration_index) ~= "number" or iteration_index < batch_start or
               iteration_index > batch_end or iteration_index ~= math.floor(iteration_index) then
                return nil, "Invalid batch range"
            end
            table.insert(indices, iteration_index)
        end
        table.sort(indices)
    else
        for i = batch_start, batch_end do
            table.insert(indices, i)
        end
    end

    for _, i in ipairs(indices) do
        local iteration_info = iterator.create_iteration(
            parent_node, template_graph, items[i], i, iteration_input_key, passthrough_inputs, options, deps
        )
        table.insert(iterations, iteration_info)
    end

    return iterations, nil
end

function iterator.collect_results(parent_node, iteration_info, deps)
    deps = deps or default_deps

    local iteration_node_ids = {}
    for _, actual_node_id in pairs(iteration_info.uuid_mapping) do
        table.insert(iteration_node_ids, actual_node_id)
    end

    local reader, reader_err = deps.data_reader.with_dataflow(parent_node.dataflow_id)
    if not reader then
        return nil, "Failed to create data reader: " .. tostring(reader_err or "unknown")
    end

    local output_data = {}
    for attempt = 1, 15 do
        output_data = {}

        local parent_query_err = nil
        if parent_node.node_id and iteration_info.iteration then
            local iteration_discriminator = build_iteration_discriminator(
                iteration_info.iteration :: number,
                iteration_info.attempt_id
            )
            local parent_rows, err_parent = (reader :: any)
                :with_nodes(parent_node.node_id)
                :with_data_types({
                    consts.DATA_TYPE.ITERATION_RESULT,
                    consts.DATA_TYPE.ITERATION_ERROR
                })
                :with_data_discriminators(iteration_discriminator)
                :fetch_options({ replace_references = true })
                :all()
            if err_parent then
                parent_query_err = err_parent
            elseif parent_rows then
                for _, row in ipairs(parent_rows) do
                    table.insert(output_data, row)
                end
            end
        end

        if #output_data == 0 and #iteration_node_ids > 0 then
            local child_rows, child_query_err = (reader :: any)
                :with_nodes(iteration_node_ids)
                :with_data_types({
                    consts.DATA_TYPE.ITERATION_RESULT,
                    consts.DATA_TYPE.ITERATION_ERROR,
                    consts.DATA_TYPE.NODE_OUTPUT,
                    consts.DATA_TYPE.NODE_RESULT
                })
                :fetch_options({ replace_references = true })
                :all()
            if child_query_err then
                return nil, "Failed to query output data: " .. tostring(child_query_err)
            end

            if child_rows then
                for _, row in ipairs(child_rows) do
                    table.insert(output_data, row)
                end
            end
        end

        if #output_data > 0 then
            break
        end

        if parent_query_err then
            return nil, "Failed to query output data: " .. tostring(parent_query_err)
        end

        if attempt < 15 then
            time.sleep("10ms")
        end
    end

    if #output_data == 0 then
        return nil, "No output data found for iteration"
    end

    local parsed_outputs = {}
    local errors: {any} = {}

    for _, output in ipairs(output_data) do
        local parsed_output = {}
        for k, v in pairs(output) do
            parsed_output[k] = v
        end

        if parsed_output.content_type == consts.CONTENT_TYPE.JSON and type(parsed_output.content) == "string" then
            local parsed_content, parse_err = json.decode(parsed_output.content)
            if not parse_err then
                parsed_output.content = parsed_content
            end
        end

        local output_type = parsed_output.type or parsed_output.data_type
        local discriminator = parsed_output.discriminator or ""

        if output_type == consts.DATA_TYPE.ITERATION_ERROR or
            (output_type == consts.DATA_TYPE.NODE_RESULT and discriminator == "result.error") or
            discriminator == "error" or
            string.find(discriminator, "error", 1, true) ~= nil then
            table.insert(errors, parsed_output.content)
        elseif output_type ~= consts.DATA_TYPE.NODE_RESULT then
            table.insert(parsed_outputs, parsed_output)
        end
    end

    if #errors > 0 then
        local error_messages = {}
        for _, error_content in ipairs(errors) do
            if type(error_content) == "table" then
                local error_table = error_content :: {[string]: any}
                error_content = error_table.error or error_table.message or tostring(error_content)
            end
            table.insert(error_messages, tostring(error_content))
        end

        return nil, table.concat(error_messages, "; ")
    end

    if #parsed_outputs == 1 then
        return parsed_outputs[1].content, nil
    end

    return parsed_outputs, nil
end

return iterator
