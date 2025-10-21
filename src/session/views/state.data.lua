local function handler(context)
    return {
        dataflow_id = context.params.id
    }
end

return {
    handler = handler
}