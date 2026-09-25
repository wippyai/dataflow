local uuid = require("uuid")
local time = require("time")
local consts = require("dataflow_consts")

return function(methods)
    local TERMINAL_STATUS = {
        [consts.STATUS.COMPLETED_SUCCESS] = true,
        [consts.STATUS.COMPLETED_FAILURE] = true,
        [consts.STATUS.CANCELLED] = true,
        [consts.STATUS.TERMINATED] = true,
    }

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

    -- Persist the business outcome before touching the runtime process. A
    -- process CANCEL is also used during application shutdown and therefore
    -- cannot itself carry cancellation semantics.
    local _result, update_err = self._deps.commit.execute(dataflow_id, uuid.v7(), {
        {
            type = consts.COMMAND_TYPES.UPDATE_WORKFLOW,
            payload = {
                status = consts.STATUS.CANCELLED,
                metadata = {
                    cancelled_at = time.now():format(time.RFC3339),
                    cancelled_by = self._actor_id,
                },
            },
        },
    })
    if update_err then return false, "Failed to cancel workflow: " .. update_err end

    local pid = self._deps.process.registry.lookup("dataflow." .. dataflow_id)
    local process_cancelled = false
    local cancel_error = nil
    if pid then
        local success, cancel_err = self._deps.process.cancel(pid, timeout)
        process_cancelled = success == true
        cancel_error = success and nil or tostring(cancel_err or "unknown error")
    end

    return true, nil, {
        dataflow_id = dataflow_id,
        timeout = timeout,
        process_cancelled = process_cancelled,
        status_updated = true,
        cancel_error = cancel_error,
        message = pid and "Workflow cancelled; runtime stop requested" or
            "Workflow cancelled without a live process",
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

    -- Persist the terminal state first so an EXIT can never race the overseer
    -- into projecting an operational failure over an administrative outcome.
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

    local pid = self._deps.process.registry.lookup("dataflow." .. dataflow_id)
    if pid then
        local terminate_success, terminate_err = self._deps.process.terminate(pid)
        if terminate_success then
            info.process_terminated = true
        else
            info.terminate_error = terminate_err
        end
    end
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

-- Send a signal to a waiting signal node. commit.submit atomically persists the
-- signal activation and owns the post-commit overseer notification.
function methods:signal(dataflow_id, signal_id, data)
    if not dataflow_id or dataflow_id == "" then
        return nil, "Workflow ID is required"
    end
    if not signal_id or signal_id == "" then
        return nil, "Signal ID is required"
    end

    local workflow, get_err = self:_owned_workflow(dataflow_id)
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
    workflow, get_err = self:_ensure_workflow_context(workflow)
    if not workflow then return nil, "Cannot signal workflow: " .. tostring(get_err) end

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

    return result, nil
end

-- Ensure the desired activation exists and notify the overseer. A registered
-- orchestrator PID remains observable for compatibility; a newly accepted
-- activation is explicitly pending and is never represented as a PID.
function methods:revive(dataflow_id)
    if not dataflow_id or dataflow_id == "" then
        return nil, "Workflow ID is required"
    end
    local workflow, ownership_err = self:_owned_workflow(dataflow_id)
    if not workflow then return nil, "Failed to authorize workflow revival: " .. tostring(ownership_err) end
    if TERMINAL_STATUS[workflow.status] then
        return nil, nil, {
            accepted = false,
            pending = false,
            terminal = true,
            status = workflow.status,
            spawned = false,
        }
    end

    local pid = self._deps.process.registry.lookup("dataflow." .. dataflow_id)
    if pid then
        return pid, nil, {
            accepted = true,
            pending = false,
            existing = true,
            spawned = false,
        }
    end

    workflow, ownership_err = self:_ensure_workflow_context(workflow)
    if not workflow then return nil, "Failed to prepare workflow revival: " .. tostring(ownership_err) end

    local activation, activation_err = self._deps.commit.request_activation(dataflow_id, {
        dataflow_id = dataflow_id
    })
    if activation_err then
        return nil, activation_err
    end
    if type(activation) ~= "table" then
        return nil, "invalid activation result"
    end
    if activation.terminal == true then
        return nil, nil, {
            accepted = false,
            pending = false,
            terminal = true,
            status = activation.status,
            spawned = false,
        }
    end
    return nil, nil, {
        accepted = true,
        pending = true,
        dataflow_id = dataflow_id,
        generation = activation.generation,
        spawned = false,
    }
end

end
