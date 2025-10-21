local json = require("json")
local uuid = require("uuid")
local client = require("client")
local consts = require("consts")
local agent_registry = require("agent_registry")
local ctx = require("ctx")

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

    -- Get all session context
    local session_context, ctx_err = ctx.all()
    if ctx_err then
        session_context = {}
    end

    -- Get agent title from registry lookup
    local agent_spec, lookup_err = agent_registry.get_by_id(target_agent_id)
    if not agent_spec then
        -- Try by name if ID lookup failed
        agent_spec, lookup_err = agent_registry.get_by_name(target_agent_id)
    end

    local agent_title = "Delegated Agent"
    if agent_spec and agent_spec.title then
        agent_title = agent_spec.title
    elseif agent_spec and agent_spec.name then
        agent_title = agent_spec.name
    else
        agent_title = target_agent_id
    end

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
                    agent = target_agent_id,
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
    local dataflow_id, create_err = c:create_workflow(workflow_commands, {
        metadata = {
            title = agent_title,
            delegation_type = "session_delegation",
            target_agent = target_agent_id,
            message = args.message,
            created_by = "userspace.dataflow.session:delegate"
        }
    })

    if create_err then
        return nil, "Failed to create delegation workflow: " .. create_err
    end

    -- Execute workflow
    local result, exec_err = c:execute(dataflow_id, {
        init_func_id = "userspace.dataflow.session:artifact"
    })

    if exec_err then
        return nil, "Failed to execute delegation workflow: " .. exec_err
    end

    -- Return delegation result directly
    return result
end

return { handle = handle }