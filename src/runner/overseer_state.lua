-- Pure ownership state for the Dataflow overseer.
--
-- Durable activation is the source of truth. This state only tracks which PID
-- owns an activation generation inside the current runtime. A missing owner is
-- spawned while acquiring a newly observed generation (including boot). Once a
-- generation has been observed, losing its owner is a terminal failure; it is
-- never restarted in the same runtime.
local M = {}

type OwnershipRecord = {
    dataflow_id: string,
    generation: number,
    phase: string,
    pid: string?,
    claim_required: boolean,
    claim_from_epoch: string?,
    candidate_pid: string?,
}

type State = {
    by_dataflow: { [string]: OwnershipRecord },
    by_pid: { [string]: string },
}

type IdentityInput = {
    dataflow_id: string,
    generation: number,
}

type ActivationInput = {
    dataflow_id: string,
    generation: number,
    desired_active: boolean,
    status: string?,
    terminal: boolean?,
    owner_epoch: string?,
    runtime_epoch: string,
}

type OwnerObservationInput = {
    dataflow_id: string,
    generation: number,
    registered_pid: string?,
    message: string?,
}

type ClaimObservationInput = {
    dataflow_id: string,
    generation: number,
    claimed: boolean,
}

type SpawnObservationInput = {
    dataflow_id: string,
    generation: number,
    registered_pid: string?,
    spawn_pid: string?,
    error: string?,
}

type MonitorObservationInput = {
    dataflow_id: string,
    generation: number,
    pid: string,
    monitor_ok: boolean,
    registered_pid: string?,
    error: string?,
}

type ExitInput = {
    pid: string,
    generation: number?,
    desired_active: boolean,
    status: string?,
    terminal: boolean?,
    message: string?,
}

type Decision = {
    kind: string,
    reason: string,
    dataflow_id: string?,
    generation: number?,
    current_generation: number?,
    observed_generation: number?,
    pid: string?,
    phase: string?,
    message: string?,
    observed_epoch: string?,
}

type DecisionDetails = {
    dataflow_id: string?,
    generation: number?,
    current_generation: number?,
    observed_generation: number?,
    pid: string?,
    phase: string?,
    message: string?,
    observed_epoch: string?,
}

M.ACTION = {
    NONE = "none",
    INSPECT_OWNER = "inspect_owner",
    CLAIM = "claim",
    REFRESH = "refresh",
    SPAWN = "spawn",
    MONITOR = "monitor",
    STOP = "stop",
    FAIL = "fail",
}

local TERMINAL_STATUS = {
    completed = true,
    failed = true,
    cancelled = true,
    terminated = true,
}

local function copy_record(record: OwnershipRecord): OwnershipRecord
    return {
        dataflow_id = record.dataflow_id,
        generation = record.generation,
        phase = record.phase,
        pid = record.pid,
        claim_required = record.claim_required,
        claim_from_epoch = record.claim_from_epoch,
        candidate_pid = record.candidate_pid,
    }
end

local function get_record(state: State, dataflow_id: string): OwnershipRecord?
    return state.by_dataflow[dataflow_id]
end

local function none(reason: string, details: DecisionDetails?): Decision
    details = details or {}
    return {
        kind = "none",
        reason = reason,
        dataflow_id = details.dataflow_id,
        generation = details.generation,
        current_generation = details.current_generation,
        observed_generation = details.observed_generation,
        pid = details.pid,
        phase = details.phase,
        message = details.message,
        observed_epoch = details.observed_epoch,
    }
end

local function identity(input: IdentityInput): IdentityInput
    local dataflow_id = input.dataflow_id
    local generation = input.generation
    assert(dataflow_id ~= "", "dataflow_id is required")
    assert(generation >= 1 and generation % 1 == 0, "generation must be a positive integer")
    return { dataflow_id = dataflow_id, generation = generation }
end

local function is_terminal(input: { status: string?, terminal: boolean? }): boolean
    return input.terminal == true or
        TERMINAL_STATUS[string.lower(tostring(input.status or ""))] == true
end

local function unbind(state: State, record: OwnershipRecord?)
    if record and record.pid ~= nil then
        state.by_pid[tostring(record.pid)] = nil
        record.pid = nil
    end
end

local function remove(state: State, dataflow_id: string)
    local record = get_record(state, dataflow_id)
    if record then unbind(state, record) end
    state.by_dataflow[dataflow_id] = nil
end

local function current(state: State, dataflow_id: string, generation: number): (OwnershipRecord?, Decision)
    local record = get_record(state, dataflow_id)
    if not record then return nil, none("unknown_activation") end
    if record.generation ~= generation then
        return nil, none("stale_generation", {
            current_generation = record.generation,
            observed_generation = generation,
        })
    end
    return record, none("current_generation")
end

local function fail(record: OwnershipRecord, reason: string, message: string?): Decision
    record.phase = "failure_requested"
    return {
        kind = M.ACTION.FAIL,
        reason = reason,
        message = tostring(message or reason),
        dataflow_id = record.dataflow_id,
        generation = record.generation,
    }
end

local function bind(state: State, record: OwnershipRecord, pid: string)
    unbind(state, record)
    local key = tostring(pid)
    local displaced_id = state.by_pid[key]
    if displaced_id then
        local other = state.by_dataflow[displaced_id]
        if other then
            other.pid = nil
            other.phase = "verification_requested"
        end
    end
    record.pid = key
    record.phase = "monitored"
    state.by_pid[key] = record.dataflow_id
end

function M.new(): State
    local records: { [string]: OwnershipRecord } = {}
    local pids: { [string]: string } = {}
    return { by_dataflow = records, by_pid = pids }
end

function M.on_activation(state: State, input: ActivationInput): (State, Decision, string?)
    local id = identity(input)
    local next_state: State = state
    local record = get_record(next_state, id.dataflow_id)

    if record and id.generation < record.generation then
        return next_state, none("stale_activation", {
            current_generation = record.generation,
            observed_generation = id.generation,
        }), nil
    end

    if is_terminal(input) or input.desired_active == false then
        local stopped_pid = nil
        if record and id.generation >= record.generation then
            stopped_pid = record.pid
            remove(next_state, id.dataflow_id)
        end
        if stopped_pid then
            return next_state, {
                kind = "stop",
                reason = is_terminal(input) and "terminal_owner_stop" or "inactive_owner_stop",
                dataflow_id = id.dataflow_id,
                generation = id.generation,
                pid = stopped_pid,
            }, nil
        end
        return next_state, none(is_terminal(input) and "terminal" or "inactive"), nil
    end
    assert(input.desired_active == true, "desired_active must be a boolean")

    if record and id.generation == record.generation then
        if record.phase == "monitored" then
            record.phase = "verification_requested"
            return next_state, {
                kind = M.ACTION.INSPECT_OWNER,
                reason = "verify_active_owner",
                dataflow_id = id.dataflow_id,
                generation = id.generation,
            }, nil
        end
        if record.phase == "failure_requested" then
            return next_state, fail(record, "retry_failure_persistence",
                "active generation previously lost its runtime owner"), nil
        end
        return next_state, none("activation_in_flight", { phase = record.phase }), nil
    end

    if record and id.generation > record.generation then
        -- A live orchestrator may own a sequence of activation generations
        -- (for example, one signal after another). Process monitoring belongs
        -- to that stable owner, not to an individual generation; retain the
        -- monitor while the durable generation fence advances.
        if record.pid ~= nil then
            record.generation = id.generation
            record.phase = "acquisition_requested"
            record.claim_required = input.owner_epoch ~= input.runtime_epoch
            record.claim_from_epoch = input.owner_epoch
            record.candidate_pid = nil
            return next_state, {
                kind = M.ACTION.INSPECT_OWNER,
                reason = "advance_monitored_owner",
                dataflow_id = id.dataflow_id,
                generation = id.generation,
            }, nil
        end

        -- Once an observed owner is lost, a later activation cannot turn that
        -- same-runtime failure into a restart. Fence the newest generation so
        -- failure persistence wins even if a signal raced the EXIT event.
        if record.phase == "failure_requested" or record.phase == "verification_requested" then
            record.generation = id.generation
            return next_state, fail(record :: OwnershipRecord, "runtime_owner_lost",
                "active orchestrator disappeared during runtime"), nil
        end
    end

    remove(next_state, id.dataflow_id)
    next_state.by_dataflow[id.dataflow_id] = {
        dataflow_id = id.dataflow_id,
        generation = id.generation,
        phase = "acquisition_requested",
        claim_required = input.owner_epoch ~= input.runtime_epoch,
        claim_from_epoch = input.owner_epoch,
    }
    return next_state, {
        kind = M.ACTION.INSPECT_OWNER,
        reason = "acquire_activation",
        dataflow_id = id.dataflow_id,
        generation = id.generation,
    }, nil
end

function M.on_owner_observation(state: State, input: OwnerObservationInput): (State, Decision, string?)
    local id = identity(input)
    local next_state: State = state
    local record, stale = current(next_state, id.dataflow_id, id.generation)
    if not record then return next_state, stale, nil end

    local pid = input.registered_pid
    if pid ~= nil and tostring(pid) ~= "" then
        if record.pid ~= nil and tostring(record.pid) == tostring(pid) and
            record.claim_required == false then
            record.phase = "monitored"
            return next_state, none("existing_owner_verified", {
                dataflow_id = id.dataflow_id,
                generation = id.generation,
                pid = tostring(pid),
            }), nil
        end
        if record.phase == "acquisition_requested" and record.claim_required then
            record.phase = "claim_requested"
            record.candidate_pid = tostring(pid)
            local claim_reason = "new_owner_adoption_claim"
            local observed_epoch: string? = record.claim_from_epoch
            if observed_epoch ~= nil then claim_reason = "reboot_owner_adoption_claim" end
            return next_state, {
                kind = M.ACTION.CLAIM,
                reason = claim_reason,
                dataflow_id = id.dataflow_id,
                generation = id.generation,
                observed_epoch = observed_epoch,
            }, nil
        end
        record.phase = "monitor_requested"
        return next_state, {
            kind = M.ACTION.MONITOR,
            reason = "registered_owner",
            dataflow_id = id.dataflow_id,
            generation = id.generation,
            pid = tostring(pid),
        }, nil
    end

    if record.pid ~= nil then
        unbind(next_state, record)
        return next_state, fail(record, "runtime_owner_lost",
            input.message or "active orchestrator disappeared during runtime"), nil
    end

    if record.phase == "acquisition_requested" and record.claim_required then
        record.phase = "claim_requested"
        local claim_reason = "new_activation_claim"
        local observed_epoch: string? = record.claim_from_epoch
        if observed_epoch ~= nil then claim_reason = "reboot_recovery_claim" end
        return next_state, {
            kind = M.ACTION.CLAIM,
            reason = claim_reason,
            dataflow_id = id.dataflow_id,
            generation = id.generation,
            observed_epoch = observed_epoch,
        }, nil
    end
    if record.phase == "acquisition_requested" then
        return next_state, fail(record, "same_runtime_owner_missing",
            "active orchestrator disappeared in the current runtime epoch"), nil
    end
    return next_state, fail(record, "runtime_owner_lost",
        input.message or "active orchestrator disappeared during runtime"), nil
end

function M.on_claim_observation(state: State, input: ClaimObservationInput): (State, Decision, string?)
    local id = identity(input)
    local next_state: State = state
    local record, stale = current(next_state, id.dataflow_id, id.generation)
    if not record then return next_state, stale, nil end
    if input.claimed == true then
        record.claim_required = false
        if record.candidate_pid then
            if record.pid ~= nil and tostring(record.pid) == tostring(record.candidate_pid) then
                record.phase = "monitored"
                return next_state, none("existing_owner_claimed", {
                    dataflow_id = id.dataflow_id,
                    generation = id.generation,
                    pid = record.pid,
                }), nil
            end
            record.phase = "monitor_requested"
            return next_state, {
                kind = M.ACTION.MONITOR,
                reason = "activation_owner_epoch_claimed",
                dataflow_id = id.dataflow_id,
                generation = id.generation,
                pid = record.candidate_pid,
            }, nil
        end
        record.phase = "spawn_requested"
        return next_state, {
            kind = M.ACTION.SPAWN,
            reason = "activation_epoch_claimed",
            dataflow_id = id.dataflow_id,
            generation = id.generation,
        }, nil
    end
    remove(next_state, id.dataflow_id)
    return next_state, {
        kind = M.ACTION.REFRESH,
        reason = "activation_epoch_claim_lost",
        dataflow_id = id.dataflow_id,
        generation = id.generation,
    }, nil
end

function M.on_spawn_observation(state: State, input: SpawnObservationInput): (State, Decision, string?)
    local id = identity(input)
    local next_state: State = state
    local record, stale = current(next_state, id.dataflow_id, id.generation)
    if not record then return next_state, stale, nil end

    local registered_pid = input.registered_pid
    if registered_pid ~= nil and tostring(registered_pid) == "" then registered_pid = nil end
    local spawn_pid = input.spawn_pid
    if spawn_pid ~= nil and tostring(spawn_pid) == "" then spawn_pid = nil end
    local candidate = registered_pid or spawn_pid
    if candidate then
        record.phase = "monitor_requested"
        return next_state, {
            kind = M.ACTION.MONITOR,
            reason = registered_pid and "canonical_owner_after_spawn" or "spawned_owner",
            dataflow_id = id.dataflow_id,
            generation = id.generation,
            pid = tostring(candidate),
        }, nil
    end
    return next_state, fail(record, "orchestrator_spawn_failed",
        input.error or "orchestrator spawn returned no owner"), nil
end

function M.on_monitor_observation(state: State, input: MonitorObservationInput): (State, Decision, string?)
    local id = identity(input)
    local next_state: State = state
    local record, stale = current(next_state, id.dataflow_id, id.generation)
    if not record then return next_state, stale, nil end

    local pid = input.pid
    if input.monitor_ok == true and pid ~= nil and tostring(pid) ~= "" then
        bind(next_state, record, pid)
        return next_state, none("owner_monitored", { pid = tostring(pid) }), nil
    end
    local registered_pid = input.registered_pid
    if registered_pid and tostring(registered_pid) ~= "" and
        tostring(registered_pid) ~= tostring(pid or "") then
        record.phase = "monitor_requested"
        return next_state, {
            kind = M.ACTION.MONITOR,
            reason = "owner_changed_during_monitor",
            dataflow_id = id.dataflow_id,
            generation = id.generation,
            pid = tostring(registered_pid),
        }, nil
    end
    return next_state, fail(record, "orchestrator_monitor_failed",
        input.error or "canonical orchestrator could not be monitored"), nil
end

function M.on_exit(state: State, input: ExitInput): (State, Decision, string?)
    local pid = input.pid
    assert(pid ~= "", "pid is required")
    local next_state: State = state
    local dataflow_id = next_state.by_pid[pid]
    if not dataflow_id then return next_state, none("stale_exit"), nil end
    local record = next_state.by_dataflow[dataflow_id]
    if not record then
        next_state.by_pid[pid] = nil
        return next_state, none("stale_exit_owner"), nil
    end
    if input.generation and tonumber(input.generation) ~= record.generation then
        return next_state, none("stale_exit_generation"), nil
    end
    if tostring(record.pid or "") ~= pid then
        next_state.by_pid[pid] = nil
        return next_state, none("stale_exit_owner"), nil
    end
    unbind(next_state, record)
    if is_terminal(input) or input.desired_active == false then
        remove(next_state, record.dataflow_id)
        return next_state, none(is_terminal(input) and "terminal_exit" or "inactive_exit"), nil
    end
    assert(input.desired_active == true, "desired_active must be a boolean")
    record.phase = "verification_requested"
    return next_state, {
        kind = M.ACTION.INSPECT_OWNER,
        reason = "verify_after_exit",
        message = input.message,
        dataflow_id = record.dataflow_id,
        generation = record.generation,
    }, nil
end

function M.on_failed(state: State, input: IdentityInput): (State, Decision, string?)
    local id = identity(input)
    local next_state: State = state
    local record = next_state.by_dataflow[id.dataflow_id]
    if record and record.generation == id.generation then remove(next_state, id.dataflow_id) end
    return next_state, none("failure_persisted"), nil
end

function M.owner_for_dataflow(state: State, dataflow_id: string): OwnershipRecord?
    local record = get_record(state, dataflow_id)
    return record and copy_record(record) or nil
end

function M.owner_for_pid(state: State, pid: string): { dataflow_id: string, generation: number }?
    local dataflow_id = state.by_pid[pid]
    if not dataflow_id then return nil end
    local record = state.by_dataflow[dataflow_id]
    if not record then return nil end
    return { dataflow_id = dataflow_id, generation = record.generation }
end

return M
