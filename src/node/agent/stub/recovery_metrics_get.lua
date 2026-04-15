local helpers = require("helpers")

local function handler(input)
    if type(input) ~= "table" or not input.scenario_id then
        return nil, "scenario_id is required"
    end

    return helpers.get_metrics(tostring(input.scenario_id))
end

return { handler = handler }
