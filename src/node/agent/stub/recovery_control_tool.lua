local uuid = require("uuid")
local ctx = require("ctx")
local consts = require("consts")
local helpers = require("helpers")

local function handler(input)
    if type(input) ~= "table" or not input.scenario_id then
        return nil, "scenario_id is required"
    end

    if type(input.overlay_traits) == "table" then
        return {
            accepted = true,
            _control = { config = { traits = input.overlay_traits } }
        }
    end

    if type(input.switch_agent) == "string" then
        return {
            accepted = true,
            _control = { config = { agent = input.switch_agent } }
        }
    end

    if type(input.memory_add) == "string" then
        return {
            accepted = true,
            _control = { memory = { add = { { type = "fact", text = input.memory_add } } } }
        }
    end

    if type(input.yield_child_ms) == "number" then
        local child = uuid.v7()
        return {
            accepted = true,
            _control = {
                yield = {
                    user_context = { run_node_ids = { child } },
                    commands = {
                        { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                            node_id = child, node_type = "userspace.dataflow.node.func:node",
                            parent_node_id = tostring(ctx.get("node_id") or ""), status = consts.STATUS.PENDING,
                            config = { func_id = "userspace.dataflow.node.func:test_func", data_targets = { {
                                data_type = consts.DATA_TYPE.NODE_OUTPUT, node_id = child, discriminator = "result",
                                content_type = consts.CONTENT_TYPE.JSON } } },
                        } },
                        { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                            data_id = uuid.v7(), data_type = consts.DATA_TYPE.NODE_INPUT, node_id = child, key = "default",
                            content = { message = "slow", delay_ms = input.yield_child_ms },
                            content_type = consts.CONTENT_TYPE.JSON } },
                    },
                },
            },
        }
    end

    if type(input.memory_and_delegate) == "string" then
        return {
            _control = {
                memory = { add = { { type = "fact", text = input.memory_and_delegate } } },
                delegate = { {
                    agent_id = input.delegate_to,
                    input_data = input.delegate_input,
                    tool_calling = "auto",
                } }
            }
        }
    end

    -- A call that delegates on its first run and answers plainly when recovery runs it again.
    if input.delegate_once
        and helpers.bump_metric(input.scenario_id, "delegate_once:" .. tostring(input.step), 1) > 1 then
        return { accepted = true, replayed = true }
    end

    if type(input.commands_memory) == "string" then
        local child = uuid.v7()
        return {
            accepted = true,
            _control = {
                memory = { add = { { type = "fact", text = input.commands_memory } } },
                commands = {
                    { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = child, node_type = "userspace.dataflow.node.func:node",
                        parent_node_id = tostring(ctx.get("node_id") or ""), status = consts.STATUS.PENDING,
                        config = { func_id = "userspace.dataflow.node.func:test_func", data_targets = { {
                            data_type = consts.DATA_TYPE.NODE_OUTPUT, node_id = child, discriminator = "result",
                            content_type = consts.CONTENT_TYPE.JSON } } },
                    } },
                    { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                        data_id = uuid.v7(), data_type = consts.DATA_TYPE.NODE_INPUT, node_id = child, key = "default",
                        content = { message = "slow", delay_ms = tonumber(input.child_delay_ms) or 6000 },
                        content_type = consts.CONTENT_TYPE.JSON } },
                },
            },
        }
    end

    if type(input.delegate_to) == "string" then
        return {
            _control = {
                delegate = { {
                    agent_id = input.delegate_to,
                    input_data = input.delegate_input or { scenario_id = input.scenario_id .. "-child", mode = "text_final" },
                    tool_calling = "auto",
                } }
            }
        }
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
