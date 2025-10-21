local state = {}

state._deps = {
    node = require("node")
}

local function run(args)
    local n, err = state._deps.node.new(args)
    if err then
        error(err)
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

    local collected = {}
    for key, input in pairs(inputs) do
        if key ~= "" then
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