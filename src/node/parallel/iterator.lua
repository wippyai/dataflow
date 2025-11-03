local uuid = require("uuid")
local json = require("json")
local consts = require("consts")

local default_deps = {
    data_reader = require("data_reader")
}

local iterator = {}

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

function iterator.redirect_terminals_to_parent(config, parent_node_id, iteration_index, source_node_id)
    local padded_iteration = string.format("%06d", iteration_index)

    local new_data_targets = {}
    for _, target in ipairs(config.data_targets or {}) do
        if not target.node_id and target.data_type == consts.DATA_TYPE.NODE_OUTPUT then
            table.insert(new_data_targets, {
                data_type = consts.DATA_TYPE.ITERATION_RESULT,
                node_id = parent_node_id,
                discriminator = "iteration." .. padded_iteration,
                content_type = target.content_type,
                metadata = {
                    source_node_id = source_node_id,
                    iteration = iteration_index
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
                discriminator = "iteration." .. padded_iteration,
                key = target.key or "error",
                content_type = target.content_type,
                metadata = {
                    source_node_id = source_node_id,
                    iteration = iteration_index
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
                                   passthrough_inputs, deps)
    deps = deps or default_deps

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
            actual_node_id
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
        child_path = child_path
    }
end

function iterator.create_batch(parent_node, template_graph, items, batch_start, batch_end, iteration_input_key,
                               passthrough_inputs, deps)
    deps = deps or default_deps

    if not parent_node or not template_graph or not items then
        return nil, "Missing required parameters"
    end

    if batch_start < 1 or batch_end > #items or batch_start > batch_end then
        return nil, "Invalid batch range"
    end

    local iterations = {}

    for i = batch_start, batch_end do
        local iteration_info = iterator.create_iteration(
            parent_node, template_graph, items[i], i, iteration_input_key, passthrough_inputs, deps
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
    if reader_err then
        return nil, "Failed to create data reader: " .. reader_err
    end

    local errors = reader
        :with_nodes(iteration_node_ids)
        :with_data_types(consts.DATA_TYPE.ITERATION_ERROR)
        :all()

    if #errors > 0 then
        local error_messages = {}
        for _, error_data in ipairs(errors) do
            local error_content = error_data.content
            if type(error_content) == "table" then
                error_content = error_content.error or error_content.message or tostring(error_content)
            end
            table.insert(error_messages, tostring(error_content))
        end

        return nil, table.concat(error_messages, "; ")
    end

    return true, nil
end

return iterator
