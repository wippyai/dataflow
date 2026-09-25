local uuid = require("uuid")
local time = require("time")
local consts = require("dataflow_consts")

-- Get default dependencies (lazy loaded)
local function get_default_deps()
    return {
        dataflow_repo = require("dataflow_repo"),
        activation_repo = require("activation_repo"),
        commit = require("commit"),
        data_reader = require("data_reader"),
        process = process,
        funcs = require("funcs"),
        security = require("security"),
        execution_frame = require("execution_frame")
    }
end

local client = {}
local methods: any = {}
local mt = { __index = methods }

local TERMINAL_STATUS = {
    [consts.STATUS.COMPLETED_SUCCESS] = true,
    [consts.STATUS.COMPLETED_FAILURE] = true,
    [consts.STATUS.CANCELLED] = true,
    [consts.STATUS.TERMINATED] = true
}

local function has_actor_context(value)
    return (type(value) == "string" and value ~= "") or
        (type(value) == "table" and next(value) ~= nil)
end

function methods:_capture_identity()
    local ok, result, capture_err = pcall(function()
        return self._deps.execution_frame.capture()
    end)
    if not ok then
        return nil, "execution identity capture failed: " .. tostring(result)
    end
    if capture_err then
        return nil, "execution identity capture failed: " .. tostring(capture_err)
    end
    if type(result) ~= "table" then
        return nil, "execution identity capture returned invalid result"
    end
    if type(result.actor_id) ~= "string" or result.actor_id == "" then
        return nil, "execution identity capture returned no actor"
    end
    if result.actor_id ~= self._actor_id then
        return nil, "execution identity capture actor mismatch"
    end
    if type(result.actor_context) ~= "string" or result.actor_context == "" then
        return nil, "execution identity capture returned no context"
    end
    return {
        actor_id = result.actor_id,
        actor_context = result.actor_context,
    }, nil
end

-- Constructor
function client.new(deps)
    deps = deps or get_default_deps()

    -- Get current security actor
    local actor = deps.security.actor()

    if not actor then
        return nil, "No current security actor available"
    end

    local scope = deps.security.scope()
    if not scope then
        return nil, "No current security scope available"
    end

    -- Get actor ID
    local actor_id = actor:id()

    -- Validate actor ID is not empty
    if not actor_id or actor_id == "" then
        return nil, "Actor ID cannot be empty"
    end

    local instance = {
        _actor = actor,
        _actor_id = actor_id,
        _scope = scope,
        _deps = deps
    }

    return setmetatable(instance, mt) :: any, nil
end

function methods:_owned_workflow(dataflow_id)
    if not dataflow_id or dataflow_id == "" then
        return nil, "Workflow ID is required"
    end

    local workflow, err = self._deps.dataflow_repo.get_by_user(dataflow_id, self._actor_id)
    if err or type(workflow) ~= "table" then
        return nil, "failed to load owned workflow: " .. tostring(err or "workflow not found")
    end
    if workflow.actor_id ~= self._actor_id then
        return nil, "workflow actor differs from current actor"
    end

    return workflow, nil
end

function methods:_ensure_workflow_context(workflow)
    if type(workflow) ~= "table" then return nil, "workflow is required" end
    if has_actor_context(workflow.actor_context) then
        return workflow, nil
    end
    local identity, identity_err = self:_capture_identity()
    if identity_err then return nil, identity_err end
    if type(self._deps.dataflow_repo.capture_context_if_empty) ~= "function" then
        return nil, "workflow execution context upgrade is unavailable"
    end
    local persisted, persist_err = self._deps.dataflow_repo.capture_context_if_empty(
        workflow.dataflow_id, self._actor_id, identity.actor_context)
    if persist_err or type(persisted) ~= "table" then
        return nil, "failed to persist workflow execution context: " ..
            tostring(persist_err or "invalid persistence result")
    end
    if persisted.actor_id ~= self._actor_id then
        return nil, "persisted workflow actor differs from current actor"
    end
    if not has_actor_context(persisted.actor_context) then
        return nil, "persisted workflow execution context is empty"
    end
    return persisted, nil
end

function methods:_actor_for_workflow(dataflow_id)
    local workflow, err = self:_owned_workflow(dataflow_id)
    if not workflow then
        return nil, err
    end
    if TERMINAL_STATUS[workflow.status] then return self._actor, nil end
    workflow, err = self:_ensure_workflow_context(workflow)
    if not workflow then return nil, err end
    return self._actor, nil
end

-- Create workflow with optional commands and options
function methods:create_workflow(commands, options)
    commands = commands or {}
    options = options or {}

    local dataflow_id = options.dataflow_id or uuid.v7()
    local workflow_type = options.type or "workflow"
    local metadata = options.metadata or {}

    -- Preserve the legacy completion-hook reference across orchestrator lives.
    -- Delivery is best-effort; new flows should model completion as an explicit
    -- terminal node in the graph instead of relying on this compatibility hook.
    if type(options.on_complete) == "string" and options.on_complete ~= "" then
        metadata.on_complete = options.on_complete
    end

    -- Create workflow command
    local identity_row, identity_err = self:_capture_identity()
    if identity_err then return nil, identity_err end

    local workflow_command = {
        type = consts.COMMAND_TYPES.CREATE_WORKFLOW,
        payload = {
            dataflow_id = dataflow_id,
            type = workflow_type,
            actor_id = self._actor_id,
            actor_context = identity_row.actor_context,
            metadata = metadata
        }
    }

    -- Combine workflow command with additional commands
    local all_commands = { workflow_command }
    for _, cmd in ipairs(commands) do
        table.insert(all_commands, cmd)
    end

    -- Execute commands
    local result, err = self._deps.commit.execute(dataflow_id, uuid.v7(), all_commands)
    if err then
        return nil, err
    end

    return dataflow_id, nil
end

-- Execute workflow synchronously
function methods:execute(dataflow_id, options)
    options = options or {}
    local fetch_output = options.fetch_output
    if fetch_output == nil then
        fetch_output = true
    end

    if not dataflow_id or dataflow_id == "" then
        return nil, "Dataflow ID is required"
    end

    -- Prepare orchestrator arguments
    local orchestrator_args = {
        dataflow_id = dataflow_id
    }

    if options.init_func_id then
        orchestrator_args.init_func_id = options.init_func_id
    end

    if options.on_complete then
        orchestrator_args.on_complete = options.on_complete
    end

    -- Execute via funcs
    local executor = self._deps.funcs.new()
    local actor, actor_err = self:_actor_for_workflow(dataflow_id)
    if actor_err then
        return nil, "Failed to execute workflow: " .. tostring(actor_err)
    end
    if actor then
        executor = executor:with_actor(actor)
    end
    executor = executor:with_scope(self._scope)

    -- Fence the synchronous attempt only after ownership validation. Do not
    -- notify the overseer before this direct call can register its orchestrator.
    local activation, activation_err = self._deps.commit.request_activation(
        dataflow_id,
        orchestrator_args,
        { notify = false }
    )
    if activation_err then
        return nil, "Failed to activate workflow: " .. tostring(activation_err)
    end
    if type(activation) ~= "table" then
        return nil, "Failed to activate workflow: invalid activation result"
    end
    if activation.terminal == true then
        return nil, "Failed to activate workflow in terminal state: " .. tostring(activation.status)
    end
    local activation_generation = tonumber(activation.generation)
    if not activation_generation or activation_generation < 1 or activation_generation % 1 ~= 0 then
        return nil, "Failed to activate workflow: invalid activation generation"
    end
    orchestrator_args.activation_generation = activation_generation

    local call_ok, orch_result, err = pcall(function()
        return executor:call(consts.ORCHESTRATOR, orchestrator_args)
    end)

    if not call_ok then
        pcall(self._deps.commit.notify_activation, dataflow_id, activation_generation)
        return nil, "Failed to execute workflow: " .. tostring(orch_result)
    end

    if err then
        pcall(self._deps.commit.notify_activation, dataflow_id, activation_generation)
        return nil, "Failed to execute workflow: " .. err
    end

    if not orch_result then
        pcall(self._deps.commit.notify_activation, dataflow_id, activation_generation)
        return nil, "No result returned from orchestrator"
    end

    -- Build consistent result format
    local result = {
        success = orch_result.success,
        dataflow_id = orch_result.dataflow_id or dataflow_id,
        data = nil,
        error = orch_result.error,
        pending = orch_result.pending == true,
        passivated = orch_result.passivated == true,
    }

    -- Handle workflow failure: return both result AND error so callers can
    -- use either pattern: `if err then` or `if not result.success then`.
    if not orch_result.success then
        local err_message = result.error or "Workflow failed"
        result.error = err_message
        return result, errors.new({
            message = err_message,
            kind = "WorkflowFailed",
            details = {
                dataflow_id = result.dataflow_id,
                success = false
            }
        })
    end


    if result.pending then
        return result, nil
    end

    -- Handle successful workflow - fetch outputs if requested
    if fetch_output then
        local outputs, output_err = self:output(dataflow_id)
        if output_err then
            return nil, "Failed to fetch workflow outputs: " .. output_err
        end
        result.data = outputs
    end

    return result, nil
end

-- Get workflow output data as key=>value pairs
function methods:output(dataflow_id)
    if not dataflow_id or dataflow_id == "" then
        return nil, "Dataflow ID is required"
    end

    if not self._deps.data_reader then
        return nil, "Data reader dependency not available"
    end

    -- Fetch all workflow outputs with error handling
    local output_data, output_err = self._deps.data_reader.with_dataflow(dataflow_id)
        :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
        :fetch_options({ replace_references = true })
        :all()

    if output_err then
        return nil, "Failed to fetch workflow outputs: " .. tostring(output_err)
    end

    if not output_data or #output_data == 0 then
        return {}, nil -- Return empty table if no outputs
    end

    local outputs = {}
    local root_output = nil

    for _, data in ipairs(output_data) do
        local key = data.key or ""
        local content = data.content

        -- Parse JSON content if it's a string
        if type(content) == "string" and data.content_type == consts.CONTENT_TYPE.JSON then
            local json = require("json")
            local decoded, decode_err = json.decode(content)
            if not decode_err then
                content = decoded
            end
        end

        if key == "" then
            -- Root output - store separately
            root_output = content
        else
            -- Named output
            outputs[key] = content
        end
    end

    -- If we have a root output and no named outputs, return the root content directly
    if root_output and next(outputs) == nil then
        return root_output, nil
    end

    -- If we have a root output and named outputs, include root as special key
    if root_output then
        outputs[""] = root_output
    end

    return outputs, nil
end

-- Start workflow asynchronously
function methods:start(dataflow_id, options)
    options = options or {}

    if not dataflow_id or dataflow_id == "" then
        return nil, "Dataflow ID is required"
    end
    local workflow, ownership_err = self:_owned_workflow(dataflow_id)
    if not workflow then return nil, "Failed to authorize workflow activation: " .. tostring(ownership_err) end
    if TERMINAL_STATUS[workflow.status] then
        return nil, "Failed to activate workflow in terminal state: " .. tostring(workflow.status)
    end
    workflow, ownership_err = self:_ensure_workflow_context(workflow)
    if not workflow then return nil, "Failed to prepare workflow activation: " .. tostring(ownership_err) end

    -- Prepare orchestrator arguments
    local orchestrator_args = {
        dataflow_id = dataflow_id
    }

    if options.init_func_id then
        orchestrator_args.init_func_id = options.init_func_id
    end

    if options.on_complete then
        orchestrator_args.on_complete = options.on_complete
    end

    local activation, activation_err = self._deps.commit.request_activation(dataflow_id, orchestrator_args)
    if activation_err then
        return nil, "Failed to activate workflow: " .. tostring(activation_err)
    end
    if type(activation) ~= "table" then
        return nil, "Failed to activate workflow: invalid activation result"
    end
    if activation.terminal == true then
        return nil, "Failed to activate workflow in terminal state: " .. tostring(activation.status)
    end

    return dataflow_id, nil
end

require("client_admission")(methods)
require("client_lifecycle")(methods)

return client :: any
