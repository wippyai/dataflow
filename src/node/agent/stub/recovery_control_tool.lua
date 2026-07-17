local uuid = require("uuid")
local ctx = require("ctx")
local consts = require("consts")

local function handler(input)
    if type(input) ~= "table" or not input.scenario_id then
        return nil, "scenario_id is required"
    end

    local child_node_id = uuid.v7()
    return {
        accepted = true,
        _control = {
            commands = {
                {
                    type = consts.COMMAND_TYPES.CREATE_NODE,
                    payload = {
                        node_id = child_node_id,
                        node_type = "userspace.dataflow.node.signal:node",
                        parent_node_id = tostring(ctx.get("node_id") or ""),
                        status = consts.STATUS.PENDING,
                        config = {
                            signal_id = "agent-control-" .. tostring(input.scenario_id),
                            data_targets = {
                                {
                                    data_type = consts.DATA_TYPE.NODE_OUTPUT,
                                    node_id = child_node_id,
                                    discriminator = "result",
                                    content_type = consts.CONTENT_TYPE.JSON,
                                }
                            }
                        },
                        metadata = {
                            title = "Agent recovery control child",
                            recovery_scenario_id = tostring(input.scenario_id),
                        }
                    }
                },
                {
                    type = consts.COMMAND_TYPES.CREATE_DATA,
                    payload = {
                        data_id = uuid.v7(),
                        data_type = consts.DATA_TYPE.NODE_INPUT,
                        node_id = child_node_id,
                        key = "default",
                        content = {
                            signal_id = "agent-control-" .. tostring(input.scenario_id)
                        },
                        content_type = consts.CONTENT_TYPE.JSON,
                    }
                }
            }
        }
    }
end

return { handler = handler }
