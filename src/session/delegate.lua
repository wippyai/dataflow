local uuid = require("uuid")
local client = require("client")
local consts = require("consts")
local agent_ref = require("agent_ref")
local ctx = require("ctx")

local function is_empty_table(value)
    if type(value) ~= "table" then
        return false
    end
    return next(value) == nil
end

local function failure_message(output)
    if type(output) == "string" and output ~= "" then
        return output
    end
    if type(output) == "table" then
        if type(output.error) == "string" and output.error ~= "" then
            return output.error
        end
        if type(output.message) == "string" and output.message ~= "" then
            return output.message
        end
    end
    return "Delegated workflow failed"
end

local function handle(args)
    -- Validate required arguments
    if not args.message then
        return nil, "message is required for delegation"
    end

    -- Get target agent_id from context
    local target_agent_id, agent_err = ctx.get("to_agent_id")
    if not target_agent_id then
        return nil, "to_agent_id not found in context"
    end
    if type(target_agent_id) ~= "string" or target_agent_id == "" then
        return nil, "to_agent_id must be a non-empty string"
    end
    local target_agent = target_agent_id :: string

    -- Get all session context
    local session_context, ctx_err = ctx.all()
    if ctx_err then
        session_context = {}
    end

    local agent_title = agent_ref.resolve(target_agent).title

    -- Create dataflow client
    local c, client_err = client.new()
    if client_err then
        return nil, "Failed to create dataflow client: " .. client_err
    end

    local node_id = uuid.v7()
    local input_data_id = uuid.v7()
    local node_input_id = uuid.v7()

    -- Create workflow with agent node
    local workflow_commands = {
        {
            type = consts.COMMAND_TYPES.CREATE_NODE,
            payload = {
                node_id = node_id,
                node_type = "userspace.dataflow.node.agent:node",
                status = consts.STATUS.PENDING,
                config = {
                    agent = target_agent,
                    arena = {
                        prompt = "You are executing user request.", -- this prompt reseved for system context passing
                        max_iterations = session_context.max_iterations or 64,
                        min_iterations = 1,
                        tool_calling = "auto",
                        context = session_context
                    },
                    data_targets = {
                        { data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, content_type = consts.CONTENT_TYPE.JSON }
                    }
                },
                metadata = {
                    title = agent_title,
                    delegation_from = session_context.from_agent_id or "unknown",
                }
            }
        },
        {
            type = consts.COMMAND_TYPES.CREATE_DATA,
            payload = {
                data_id = input_data_id,
                data_type = consts.DATA_TYPE.WORKFLOW_INPUT,
                content = args.message,
                content_type = consts.CONTENT_TYPE.TEXT
            }
        },
        {
            type = consts.COMMAND_TYPES.CREATE_DATA,
            payload = {
                data_id = node_input_id,
                data_type = consts.DATA_TYPE.NODE_INPUT,
                key = input_data_id,
                node_id = node_id,
                content_type = consts.CONTENT_TYPE.REFERENCE,
                content = ""
            }
        }
    }

    -- Create and execute workflow
    local dataflow_id, create_err = (c :: any):create_workflow(workflow_commands, {
        metadata = {
            title = agent_title,
            delegation_type = "session_delegation",
            target_agent = target_agent,
            message = args.message,
            created_by = "userspace.dataflow.session:delegate"
        }
    })

    if create_err then
        return nil, "Failed to create delegation workflow: " .. create_err
    end

    -- Delegation is a synchronous caller, so execute the Dataflow directly.
    -- Async callers use client:start and observe through their own event hook.
    local execute_result, execute_err = (c :: any):execute(dataflow_id, {
        init_func_id = "userspace.dataflow.session:artifact"
    })

    if execute_err then
        return nil, "Failed to execute delegation workflow: " .. tostring(execute_err)
    end

    if execute_result and execute_result.pending then
        return {
            dataflow_id = dataflow_id,
            pending = true,
        }
    end

    local output = execute_result and execute_result.data

    if execute_result and not execute_result.success then
        return nil, failure_message(output)
    end

    if output == nil or is_empty_table(output) then
        return nil, "Delegated workflow completed without output"
    end

    return output
end

return { handle = handle }
