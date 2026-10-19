local M = { builder = {} }

function M.get()
    return {
        release = function() end,
        type = function() return "sqlite", nil end,
    }, nil
end

function M.builder.select(...)
    local query = {}
    function query:from() return self end
    function query:where() return self end
    function query:order_by() return self end
    function query:run_with() return self end
    function query:query()
        return {
            { type = "invalid_json_config_node", config = '{"invalid":json}', metadata = '{"invalid":metadata}' },
            { type = "empty_string_config_node", config = "", metadata = "" },
        }, nil
    end
    return query
end

return M
