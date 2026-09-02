local helpers = require("helpers")

-- Test tool that persists its content as a control artifact, the same channel
-- the Artifacts capability uses, so agent-node tests can drive the
-- "artifact created, then an empty final turn" sequence deterministically.
local function handler(input)
    if type(input) ~= "table" or not input.scenario_id then
        return nil, "scenario_id is required"
    end
    if type(input.content) ~= "string" or input.content == "" then
        return nil, "content is required"
    end

    helpers.bump_metric(tostring(input.scenario_id), "tool_attempts", 1)

    return {
        ok = true,
        _control = {
            artifacts = {
                {
                    title = input.title or "Report",
                    content = input.content,
                    content_type = "text/markdown",
                    type = "inline"
                }
            }
        }
    }
end

return { handler = handler }
