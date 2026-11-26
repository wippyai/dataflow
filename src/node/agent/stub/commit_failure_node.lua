local agent_node = require("agent_node")
local node_sdk = require("node_sdk")
local consts = require("consts")
local agent_consts = require("agent_consts")

local function run(args)
    local saved_new = node_sdk.new
    local refused = false
    node_sdk.new = function(run_args)
        local n, err = saved_new(run_args)
        if n then
            local submit = n._deps.commit.submit
            n._deps.commit = { submit = function(dataflow_id, op_id, commands)
                for _, command in ipairs(commands) do
                    local payload = command.payload or {}
                    if not refused and command.type == consts.COMMAND_TYPES.CREATE_DATA
                        and payload.data_type == agent_consts.DATA_TYPE.AGENT_OBSERVATION
                        and (payload.metadata or {}).tool_call_id == n:config().refused_call_id then
                        refused = true
                        return nil, "commit refused"
                    end
                end
                return submit(dataflow_id, op_id, commands)
            end }
        end
        return n, err
    end
    local ok, result = pcall(agent_node.run, args)
    node_sdk.new = saved_new
    if not ok then error(result) end
    return result
end

return { run = run }
