local time = require("time")
local consts = require("dataflow_consts")

return function(methods)
    local function typed(kind, message: any)
        return errors.new({ kind = kind, message = tostring(message) })
    end
    local function owner_error(err)
        local message = tostring(err or "workflow not found")
        if message:find("access denied", 1, true) or
            message:find("actor differs", 1, true) then
            return typed(errors.PERMISSION_DENIED, message)
        end
        return typed(errors.NOT_FOUND, message)
    end
    local function available_error(err)
        if type(err) == "userdata" then return err end
        return typed(errors.UNAVAILABLE, err or "operation unavailable")
    end
    local TERMINAL_STATUS = {
        [consts.STATUS.COMPLETED_SUCCESS] = true,
        [consts.STATUS.COMPLETED_FAILURE] = true,
        [consts.STATUS.CANCELLED] = true,
        [consts.STATUS.TERMINATED] = true,
    }

-- Admission calls use one durable key for the lifetime of a workflow run.
-- The repository serializes them with ordinary activation and terminal writes.
function methods:ensure_activation(dataflow_id, admission_key)
    if type(dataflow_id) ~= "string" or dataflow_id == "" then
        return nil, typed(errors.INVALID, "dataflow_id is required")
    end
    if type(admission_key) ~= "string" or admission_key == "" then
        return nil, typed(errors.INVALID, "admission_key is required")
    end
    local current, current_err = self._deps.activation_repo.get_activation_evidence(
        dataflow_id, admission_key)
    if current_err then return nil, current_err end
    if current.state == "absent" then return current, nil end
    local workflow, ownership_err = self:_owned_workflow(dataflow_id)
    if not workflow then return nil, owner_error(ownership_err) end
    if not TERMINAL_STATUS[workflow.status] then
        workflow, ownership_err = self:_ensure_workflow_context(workflow)
        if not workflow then return nil, available_error(ownership_err) end
    end
    local evidence, err = self._deps.activation_repo.ensure_activation(
        dataflow_id, admission_key, time.now():format(time.RFC3339NANO))
    if err then return nil, err end
    if evidence.state == "activated" or evidence.state == "running" then
        local _, notify_err = self._deps.commit.notify_activation(dataflow_id, evidence.generation)
        if notify_err then return nil, available_error(notify_err) end
    end
    return evidence, nil
end

function methods:get_activation_evidence(dataflow_id, admission_key)
    if type(dataflow_id) ~= "string" or dataflow_id == "" then
        return nil, typed(errors.INVALID, "dataflow_id is required")
    end
    if type(admission_key) ~= "string" or admission_key == "" then
        return nil, typed(errors.INVALID, "admission_key is required")
    end
    local evidence, err = self._deps.activation_repo.get_activation_evidence(
        dataflow_id, admission_key)
    if err or evidence.state == "absent" then return evidence, err end
    local workflow, ownership_err = self:_owned_workflow(dataflow_id)
    if not workflow then return nil, owner_error(ownership_err) end
    return evidence, nil
end

function methods:ack_terminal(dataflow_id, admission_key, generation)
    if type(dataflow_id) ~= "string" or dataflow_id == "" then
        return nil, typed(errors.INVALID, "dataflow_id is required")
    end
    if type(admission_key) ~= "string" or admission_key == "" then
        return nil, typed(errors.INVALID, "admission_key is required")
    end
    if type(generation) ~= "number" or generation < 1 or generation % 1 ~= 0 then
        return nil, typed(errors.INVALID, "generation must be a positive integer")
    end
    local workflow, ownership_err = self:_owned_workflow(dataflow_id)
    if not workflow then return nil, owner_error(ownership_err) end
    return self._deps.activation_repo.ack_terminal(
        dataflow_id, admission_key, generation, time.now():format(time.RFC3339NANO))
end

end
