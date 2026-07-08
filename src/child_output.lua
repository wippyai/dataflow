local child_output = {}

child_output._deps = {
    consts = require("consts")
}

function child_output.query_node_outputs(n, node_ids)
    if type(node_ids) ~= "table" or #node_ids == 0 then
        return {}, nil
    end

    local ok, output_data_or_err = pcall(function()
        return (n:query() :: any)
            :with_nodes(node_ids)
            :with_data_types(child_output._deps.consts.DATA_TYPE.NODE_OUTPUT)
            :fetch_options({ replace_references = true })
            :all()
    end)

    if not ok then
        return nil, "Failed to query output data: " .. tostring(output_data_or_err)
    end

    return output_data_or_err, nil
end

function child_output.resume_children(n, child_node_ids, collect_children_result)
    if type(child_node_ids) ~= "table" or #child_node_ids == 0 then
        return nil, nil
    end

    local existing, existing_err = collect_children_result(n, child_node_ids)
    if existing_err then
        return nil, existing_err
    end
    if existing ~= nil then
        return existing, nil
    end

    local _yield_result, yield_err = n:yield({ run_nodes = child_node_ids })
    if yield_err then
        return nil, "Failed to resume children: " .. tostring(yield_err)
    end

    return collect_children_result(n, child_node_ids)
end

return child_output
