local uuid = require("uuid")
local time = require("time")
local security = require("security")
local funcs = require("funcs")
local contract = require("contract")
local consts = require("dataflow_consts")

local SYSTEM_ACTOR_ID = "system.dataflow"
local EXECUTION_IDENTITY_CONTRACT = "userspace.dataflow:execution_identity"

-- Get default dependencies (lazy loaded)
local function get_default_deps()
    return {
        dataflow_repo = require("dataflow_repo"),
        commit = require("commit"),
        data_reader = require("data_reader"),
        process = process,
        funcs = require("funcs"),
        security = require("security"),
        contract = require("contract")
    }
end

local client = {}
local methods = {}
local mt = { __index = methods }

local TERMINAL_STATUS = {
    [consts.STATUS.COMPLETED_SUCCESS] = true,
    [consts.STATUS.COMPLETED_FAILURE] = true,
    [consts.STATUS.CANCELLED] = true,
    [consts.STATUS.TERMINATED] = true
}

local function current_scope(deps)
    if deps.security and type(deps.security.scope) == "function" then
        return deps.security.scope()
    end
    return nil
end

function methods:_identity_contract()
    if self._deps.identity_contract ~= nil then
        return self._deps.identity_contract
    end

    local contract_mod = self._deps.contract or contract
    local def, get_err = contract_mod.get(EXECUTION_IDENTITY_CONTRACT)
    if get_err or not def then
        return nil
    end

    local actor = self._actor
    local scope = current_scope(self._deps)
    if not actor or not scope then
        return nil
    end

    if type(def.with_actor) ~= "function" then
        return nil
    end

    local scoped_def, actor_err = def:with_actor(actor)
    if actor_err or not scoped_def or type(scoped_def.with_scope) ~= "function" then
        return nil
    end

    local bound_def, scope_err = scoped_def:with_scope(scope)
    if scope_err or not bound_def or type(bound_def.open) ~= "function" then
        return nil
    end

    local opened, open_err = bound_def:open()
    if open_err or not opened then
        return nil
    end
    return opened
end

function methods:_capture_identity()
    local provider = self:_identity_contract()
    if not provider or type(provider.capture) ~= "function" then
        return nil, nil
    end

    local ok, result = pcall(function()
        return provider:capture({ reason = "dataflow" })
    end)
    if not ok then
        return nil, "execution identity capture failed: " .. tostring(result)
    end
    if type(result) ~= "table" then
        return nil, "execution identity capture returned invalid result"
    end
    if result.success == false then
        return nil, "execution identity capture failed: " .. tostring(result.error)
    end
    if type(result.actor_id) == "string" and result.actor_id ~= "" then
        if result.actor_id ~= self._actor_id then
            return nil, "execution identity capture actor mismatch"
        end
        return {
            actor_id = result.actor_id,
            actor_context = result.actor_context,
        }, nil
    end
    return nil, nil
end

-- Constructor
function client.new(deps)
    deps = deps or get_default_deps()

    -- Get current security actor
    local actor = deps.security.actor()

    -- In non-authenticated contexts (tests/system workflows), synthesize a stable actor.
    if not actor then
        actor = deps.security.new_actor(SYSTEM_ACTOR_ID, {
            kind = "system",
            source = "userspace.dataflow:client"
        })
    end

    -- Validate security actor exists
    if not actor then
        return nil, "No current security actor available"
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
        _deps = deps
    }

    return setmetatable(instance, mt) :: any, nil
end

function methods:_workflow_for_spawn(dataflow_id)
    if not dataflow_id or dataflow_id == "" then
        return nil, "Workflow ID is required"
    end

    local workflow, err = self._deps.dataflow_repo.get(dataflow_id)
    if err or type(workflow) ~= "table" then
        return nil, "failed to load workflow identity: " .. tostring(err or "workflow not found")
    end

    local actor_id = workflow.actor_id
    if type(actor_id) ~= "string" or actor_id == "" then
        return nil, "workflow is missing actor_id"
    end

    return workflow, nil
end

function methods:_actor_for_workflow(dataflow_id)
    local workflow, err = self:_workflow_for_spawn(dataflow_id)
    if not workflow then
        return nil, err
    end
    if workflow.actor_id == self._actor_id then
        return self._actor
    end
    return nil, "workflow actor differs from current actor and no execution identity contract is used for direct calls"
end

function methods:_spawn_orchestrator(dataflow_id, args)
    local workflow, workflow_err = self:_workflow_for_spawn(dataflow_id)
    if not workflow then
        return nil, workflow_err
    end

    if workflow.actor_id == self._actor_id then
        local actor = self._actor
        if self._deps.process.with_context and actor then
            local spawner = self._deps.process.with_context({}):with_actor(actor)
            local scope = current_scope(self._deps)
            if scope then spawner = spawner:with_scope(scope) end
            local pid, spawn_err = spawner:spawn(consts.ORCHESTRATOR, consts.HOST_ID, args)
            if pid and type(self._deps.process.send) == "function" then
                local registered, register_err = self._deps.process.send("dataflow.wakes", "dataflow.orchestrator.started", {
                    dataflow_id = dataflow_id, pid = pid,
                })
                if not registered then
                    if type(self._deps.process.terminate) == "function" then self._deps.process.terminate(pid) end
                    return nil, "orchestrator supervision registration failed: " .. tostring(register_err)
                end
            end
            return pid, spawn_err
        end
        local pid, spawn_err = self._deps.process.spawn(consts.ORCHESTRATOR, consts.HOST_ID, args)
        if pid and type(self._deps.process.send) == "function" then
            local registered, register_err = self._deps.process.send("dataflow.wakes", "dataflow.orchestrator.started", {
                dataflow_id = dataflow_id, pid = pid,
            })
            if not registered then
                if type(self._deps.process.terminate) == "function" then self._deps.process.terminate(pid) end
                return nil, "orchestrator supervision registration failed: " .. tostring(register_err)
            end
        end
        return pid, spawn_err
    end

    local provider = self:_identity_contract()
    if not provider or type(provider.spawn_orchestrator) ~= "function" then
        return nil, "execution identity contract is not bound; cannot revive workflow " .. tostring(dataflow_id)
    end

    local ok, result = pcall(function()
        return provider:spawn_orchestrator({
            dataflow_id = dataflow_id,
            process_id = consts.ORCHESTRATOR,
            host_id = consts.HOST_ID,
            args = args or {},
        })
    end)
    if not ok then
        return nil, "execution identity spawn failed: " .. tostring(result)
    end
    if type(result) ~= "table" or result.success == false then
        return nil, "execution identity spawn failed: " .. tostring(result and result.error or "invalid result")
    end
    if result.pid and type(self._deps.process.send) == "function" then
        local registered, register_err = self._deps.process.send("dataflow.wakes", "dataflow.orchestrator.started", {
            dataflow_id = dataflow_id, pid = result.pid,
        })
        if not registered then
            if type(self._deps.process.terminate) == "function" then self._deps.process.terminate(result.pid) end
            return nil, "orchestrator supervision registration failed: " .. tostring(register_err)
        end
    end
    return result.pid, nil
end

-- Create workflow with optional commands and options
function methods:create_workflow(commands, options)
    commands = commands or {}
    options = options or {}

    local dataflow_id = options.dataflow_id or uuid.v7()
    local workflow_type = options.type or "workflow"
    local metadata = options.metadata or {}

    -- Persist the optional completion hook durably in metadata so a respawned
    -- orchestrator (boot recovery, due wake, or late signal) still fires it.
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
            actor_context = identity_row and identity_row.actor_context or nil,
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
    local orch_result, err = executor:call(consts.ORCHESTRATOR, orchestrator_args)

    if err then
        return nil, "Failed to execute workflow: " .. err
    end

    if not orch_result then
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

    -- Spawn orchestrator process
    local pid, spawn_err = self:_spawn_orchestrator(dataflow_id, orchestrator_args)
    if not pid then
        return nil, "Failed to spawn workflow process: " .. tostring(spawn_err)
    end

    return dataflow_id, nil
end

-- Cancel workflow
function methods:cancel(dataflow_id, timeout)
    if not dataflow_id or dataflow_id == "" then
        return false, "Workflow ID is required"
    end

    timeout = timeout or "30s"

    -- Verify workflow exists and user has access
    local workflow, err = self._deps.dataflow_repo.get_by_user(dataflow_id, self._actor_id)
    if err then
        return false, err
    end

    if not workflow then
        return false, "Workflow not found"
    end

    -- Check if workflow can be cancelled
    local cancellable_states = {
        [consts.STATUS.PENDING] = true,
        [consts.STATUS.RUNNING] = true,
        [consts.STATUS.WAITING] = true
    }

    if not cancellable_states[workflow.status] then
        return false, "Workflow cannot be cancelled in current state: " .. workflow.status
    end

    -- Find workflow process
    local process_name = "dataflow." .. dataflow_id
    local pid = self._deps.process.registry.lookup(process_name)
    if not pid then
        -- No live orchestrator (parked run): degrade to a direct terminal status
        -- update, the same path terminate uses. Without this a parked run is
        -- uncancellable — process.cancel needs a live pid.
        local update_commands = {
            {
                type = consts.COMMAND_TYPES.UPDATE_WORKFLOW,
                payload = {
                    status = consts.STATUS.CANCELLED,
                    metadata = {
                        cancelled_at = time.now():format(time.RFC3339),
                        cancelled_by = self._actor_id
                    }
                }
            }
        }

        local _result, update_err = self._deps.commit.execute(dataflow_id, uuid.v7(), update_commands)
        if update_err then
            return false, "Failed to cancel workflow: " .. update_err
        end

        return true, nil, {
            dataflow_id = dataflow_id,
            process_cancelled = false,
            status_updated = true,
            message = "Workflow cancelled without a live process"
        }
    end

    -- Send cancel signal
    local success, cancel_err = self._deps.process.cancel(pid, timeout)
    if not success then
        return false, "Failed to send cancel signal: " .. (cancel_err or "unknown error")
    end

    return true, nil, {
        dataflow_id = dataflow_id,
        timeout = timeout,
        process_cancelled = true,
        status_updated = false,
        message = "Cancel signal sent to workflow process"
    }
end

-- Terminate workflow
function methods:terminate(dataflow_id)
    if not dataflow_id or dataflow_id == "" then
        return false, "Workflow ID is required"
    end

    -- Verify workflow exists and user has access
    local workflow, err = self._deps.dataflow_repo.get_by_user(dataflow_id, self._actor_id)
    if err then
        return false, err
    end

    if not workflow then
        return false, "Workflow not found"
    end

    -- Check if workflow is already finished
    local finished_states = {
        [consts.STATUS.COMPLETED_SUCCESS] = true,
        [consts.STATUS.COMPLETED_FAILURE] = true,
        [consts.STATUS.CANCELLED] = true,
        [consts.STATUS.TERMINATED] = true
    }

    if finished_states[workflow.status] then
        return false, "Workflow already finished with status: " .. workflow.status
    end

    local info = {
        dataflow_id = dataflow_id,
        process_terminated = false,
        status_updated = false
    }

    -- Find and terminate workflow process
    local process_name = "dataflow." .. dataflow_id
    local pid = self._deps.process.registry.lookup(process_name)
    if pid then
        local terminate_success, terminate_err = self._deps.process.terminate(pid)
        if terminate_success then
            info.process_terminated = true
        else
            info.terminate_error = terminate_err
        end
    end

    -- Update workflow status
    local update_commands = {
        {
            type = consts.COMMAND_TYPES.UPDATE_WORKFLOW,
            payload = {
                status = consts.STATUS.TERMINATED,
                metadata = {
                    terminated_at = time.now():format(time.RFC3339),
                    terminated_by = self._actor_id
                }
            }
        }
    }

    local result, update_err = self._deps.commit.execute(dataflow_id, uuid.v7(), update_commands)
    if update_err then
        return false, "Failed to update workflow status: " .. update_err, info
    end

    info.status_updated = true
    return true, nil, info
end

-- Get workflow status
function methods:get_status(dataflow_id)
    if not dataflow_id or dataflow_id == "" then
        return nil, "Workflow ID is required"
    end

    -- Get workflow with actor verification
    local workflow, err = self._deps.dataflow_repo.get_by_user(dataflow_id, self._actor_id)
    if err then
        return nil, err
    end

    if not workflow then
        return nil, "Workflow not found"
    end

    return workflow.status, nil
end

-- Send a signal to a waiting signal node in a workflow.
-- If the orchestrator is dead, respawns it to process the signal from the outbox.
function methods:signal(dataflow_id, signal_id, data)
    if not dataflow_id or dataflow_id == "" then
        return nil, "Workflow ID is required"
    end
    if not signal_id or signal_id == "" then
        return nil, "Signal ID is required"
    end

    -- Refuse to signal a missing or terminal dataflow: a late signal must not
    -- resurrect a finished run. Access control is the caller's concern (the HTTP
    -- endpoint authorizes via get_by_user); internal callers already run under
    -- the workflow's frozen identity.
    local workflow, get_err = self._deps.dataflow_repo.get(dataflow_id)
    if get_err or not workflow then
        return nil, errors.new({
            message = "Cannot signal workflow: " .. (get_err or "not found"),
            kind = "WorkflowNotFound",
            details = { dataflow_id = dataflow_id }
        })
    end
    if TERMINAL_STATUS[workflow.status] then
        return nil, errors.new({
            message = "Cannot signal workflow in terminal state: " .. tostring(workflow.status),
            kind = "WorkflowTerminal",
            details = { dataflow_id = dataflow_id, status = workflow.status }
        })
    end

    -- 1. Write signal commit to outbox (durable, survives crashes)
    local op_id = uuid.v7()
    local result, err = self._deps.commit.submit(dataflow_id, op_id, {
        {
            type = consts.COMMAND_TYPES.CREATE_DATA,
            payload = {
                data_id = uuid.v7(),
                data_type = consts.DATA_TYPE.NODE_SIGNAL,
                content = data or {},
                content_type = consts.CONTENT_TYPE.JSON,
                key = signal_id,
            }
        }
    })

    if err then
        return nil, "Failed to send signal: " .. tostring(err)
    end

    -- NODE_SIGNAL atomically creates an immediate targeted wake row. Nudge the
    -- single wake process for latency; durability does not depend on this send.
    if type(self._deps.process.send) == "function" then
        self._deps.process.send("dataflow.wakes", "dataflow.wake.changed", { dataflow_id = dataflow_id })
    end

    -- 2. Check if orchestrator is alive
    local pid = self._deps.process.registry.lookup("dataflow." .. dataflow_id)
    if not pid then
        -- Orchestrator is dead — respawn it.
        -- It will load state from DB + pending commits (including our signal).
        -- If another caller also respawns, the duplicate orchestrator detects
        -- the name conflict on registry.register and exits gracefully.
        local _pid, spawn_err = self:_spawn_orchestrator(dataflow_id, {
            dataflow_id = dataflow_id
        })
        if spawn_err then
            return nil, "Failed to respawn workflow process: " .. tostring(spawn_err)
        end
    end

    return result, nil
end

-- Ensure a live orchestrator exists for a dataflow. Returns the existing pid when
-- one is registered, otherwise respawns the orchestrator under the workflow's frozen
-- identity. The orchestrator's registry single-instance guard makes a concurrent
-- double-spawn safe, and its terminal-status guard makes reviving a finished run a
-- no-op. Used by signal delivery and one-shot boot recovery.
function methods:revive(dataflow_id)
    if not dataflow_id or dataflow_id == "" then
        return nil, "Workflow ID is required"
    end

    local pid = self._deps.process.registry.lookup("dataflow." .. dataflow_id)
    if pid then
        return pid, nil, { spawned = false }
    end

    local spawned_pid, spawn_err = self:_spawn_orchestrator(dataflow_id, {
        dataflow_id = dataflow_id
    })
    if spawn_err then
        return nil, spawn_err
    end
    return spawned_pid, nil, { spawned = true }
end

return client
