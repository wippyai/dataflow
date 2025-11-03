local json = require("json")

local state = {}

state._deps = {
    node = require("node"),
    consts = require("consts")
}

local function collect_inputs_as_array(n, ignored_set)
    local input_data = n:query()
        :with_nodes(n.node_id)
        :with_data_types(state._deps.consts.DATA_TYPE.NODE_INPUT)
        :order_by("created_at", "ASC")
        :all()

    local result = {}
    for _, input in ipairs(input_data) do
        if not ignored_set[input.discriminator] then
            if input.content_type == state._deps.consts.CONTENT_TYPE.JSON then
                local decoded, decode_err = json.decode(input.content)
                if decode_err == nil then
                    input.content = decoded
                end
            end

            table.insert(result, input.content)
        end
    end

    return result
end

local function run(args)
    local n, err = state._deps.node.new(args)
    if err then
        error(err)
    end

    local config = n:config()
    local output_mode = config.output_mode or "object"
    local ignored_keys = config.ignored_keys or {}

    local ignored_set = {}
    for _, key in ipairs(ignored_keys) do
        ignored_set[key] = true
    end

    local inputs, inputs_err = n:inputs()
    if inputs_err then
        return n:fail({
            code = "INPUT_VALIDATION_FAILED",
            message = inputs_err
        }, inputs_err)
    end

    if next(inputs) == nil then
        return n:fail("No input data provided", "State node requires input data")
    end

    if output_mode == "array" then
        local array_result = collect_inputs_as_array(n, ignored_set)
        return n:complete(array_result, "State collection completed")
    end

    local collected = {}
    for key, input in pairs(inputs) do
        if key ~= "" and not ignored_set[key] then
            collected[key] = input.content
        end
    end

    if next(collected) == nil then
        for _, input in pairs(inputs) do
            return n:complete(input.content, "State collection completed")
        end
    end

    if collected.default then
        local has_other_keys = false
        for key in pairs(collected) do
            if key ~= "default" then
                has_other_keys = true
                break
            end
        end

        if not has_other_keys then
            return n:complete(collected.default, "State collection completed")
        end
    end

    return n:complete(collected, "State collection completed")
end

state.run = run
return state