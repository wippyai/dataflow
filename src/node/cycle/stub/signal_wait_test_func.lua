local uuid = require("uuid")
local ctx = require("ctx")
local consts = require("consts")

local function run(context)
    local input = context.input or {}
    local state = context.state or {}
    local last_result = context.last_result
    local iteration = context.iteration or 1
    local parent_node_id = ctx.get("node_id")

    if iteration == 1 then
        state = {
            signal_prefix = input.signal_prefix or ("cycle-signal-" .. uuid.v7()),
            required_iterations = input.required_iterations or 2,
            received = {}
        }
    end

    if last_result ~= nil then
        table.insert(state.received, last_result)
    end

    if #state.received >= (state.required_iterations or 2) then
        return {
            state = state,
            continue = false,
            result = {
                received = state.received,
                total = #state.received
            }
        }
    end

    local next_signal_index = #state.received + 1
    local signal_id = state.signal_prefix .. "-" .. tostring(next_signal_index)
    local signal_node_id = uuid.v7()

    return {
        state = state,
        continue = true,
        result = {
            waiting_for = signal_id,
            received = #state.received
        },
        _control = {
            commands = {
                {
                    type = consts.COMMAND_TYPES.CREATE_NODE,
                    payload = {
                        node_id = signal_node_id,
                        node_type = "userspace.dataflow.node.signal:node",
                        status = consts.STATUS.PENDING,
                        parent_node_id = parent_node_id,
                        config = {
                            signal_id = signal_id,
                            data_targets = {
                                {
                                    data_type = consts.DATA_TYPE.NODE_OUTPUT,
                                    key = "signal_result",
                                    content_type = consts.CONTENT_TYPE.JSON
                                }
                            }
                        },
                        metadata = {
                            title = "Cycle Signal Iteration " .. tostring(iteration),
                            iteration = iteration
                        }
                    }
                },
                {
                    type = consts.COMMAND_TYPES.CREATE_DATA,
                    payload = {
                        data_id = uuid.v7(),
                        data_type = consts.DATA_TYPE.NODE_INPUT,
                        node_id = signal_node_id,
                        key = "default",
                        content = {
                            signal_id = signal_id,
                            iteration = iteration
                        },
                        content_type = consts.CONTENT_TYPE.JSON
                    }
                }
            }
        }
    }
end

return { run = run }
