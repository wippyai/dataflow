local json = require("json")

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

local function decode_content(content)
    if type(content) ~= "string" then
        return content
    end

    local decoded, decode_err = json.decode(content)
    if not decode_err then
        return decoded
    end
    return content
end

local function child_failure_message(content)
    if type(content.error) == "table" then
        return tostring(content.error.message or content.error.status or content.message or "Child workflow failed")
    end

    return tostring(content.error or content.message or "Child workflow failed")
end

function child_output.outputs_from_yield_results(n, yield_results)
    if type(yield_results) ~= "table" or next(yield_results) == nil then
        return nil, nil
    end

    local result_ids = {}
    local expected_result_ids = {}
    for _, result_id in pairs(yield_results) do
        if type(result_id) == "string" and result_id ~= "" then
            table.insert(result_ids, result_id)
            expected_result_ids[result_id] = true
        end
    end
    if #result_ids == 0 then
        return nil, nil
    end

    local ok, result_rows_or_err = pcall(function()
        return (n:query() :: any)
            :with_data(result_ids)
            :with_data_types(child_output._deps.consts.DATA_TYPE.NODE_RESULT)
            :fetch_options({ replace_references = true })
            :all()
    end)
    if not ok then
        return nil, "Failed to query child result data: " .. tostring(result_rows_or_err)
    end

    local output_ids = {}
    local fallback_node_ids = {}
    local seen_output_ids = {}
    local seen_fallback_node_ids = {}
    local seen_result_ids = {}
    for _, row in ipairs(result_rows_or_err or {}) do
        if row.data_id then
            seen_result_ids[row.data_id] = true
        end
        local content = decode_content(row.content)
        if type(content) == "table" then
            if content.success == false then
                return nil, {
                    code = "CHILD_WORKFLOW_FAILED",
                    message = child_failure_message(content),
                    status = "Child workflow failed"
                }
            end

            if content.data_ids ~= nil and type(content.data_ids) ~= "table" then
                return nil, "Child result data_ids must be an array"
            end

            for _, data_id in ipairs(content.data_ids or {}) do
                if type(data_id) == "string" and data_id ~= "" and not seen_output_ids[data_id] then
                    table.insert(output_ids, data_id)
                    seen_output_ids[data_id] = true
                end
            end
        elseif row.node_id and not seen_fallback_node_ids[row.node_id] then
            table.insert(fallback_node_ids, row.node_id)
            seen_fallback_node_ids[row.node_id] = true
        end
    end

    for _, result_id in ipairs(result_ids) do
        if not seen_result_ids[result_id] and expected_result_ids[result_id] then
            return nil, "Child result data not found: " .. result_id
        end
    end

    if #output_ids == 0 then
        return child_output.query_node_outputs(n, fallback_node_ids)
    end

    local output_ok, output_rows_or_err = pcall(function()
        return (n:query() :: any)
            :with_data(output_ids)
            :with_data_types(child_output._deps.consts.DATA_TYPE.NODE_OUTPUT)
            :fetch_options({ replace_references = true })
            :all()
    end)
    if not output_ok then
        return nil, "Failed to query child output data: " .. tostring(output_rows_or_err)
    end

    return output_rows_or_err, nil
end

function child_output.collect_outputs(n, node_ids, yield_results)
    local has_yield_results = type(yield_results) == "table" and next(yield_results) ~= nil
    local output_data, collect_err = child_output.outputs_from_yield_results(n, yield_results)
    if collect_err then
        return nil, collect_err
    end

    if has_yield_results then
        return output_data or {}, nil
    end

    if output_data and #output_data > 0 then
        return output_data, nil
    end

    return child_output.query_node_outputs(n, node_ids)
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

    local yield_result, yield_err = n:yield({ run_nodes = child_node_ids })
    if yield_err then
        return nil, "Failed to resume children: " .. tostring(yield_err)
    end

    return collect_children_result(n, child_node_ids, yield_result)
end

return child_output
