-- Pure ownership state for the Dataflow overseer.
--
-- This module does not inspect the process registry, spawn, monitor, sleep, or
-- read durable state. Callers provide those observations and execute the
-- returned decisions. Keeping those boundaries explicit makes lifecycle races
-- deterministic and independently testable.
local M = {}

M.ACTION = {
    NONE = "none",
    INSPECT_OWNER = "inspect_owner",
    SPAWN = "spawn",
    MONITOR = "monitor",
    RESTART = "restart",
}

local DEFAULT_RETRY_BASE_MS = 250
local DEFAULT_RETRY_MAX_MS = 30000

local TERMINAL_STATUS = {
    completed = true,
    failed = true,
    cancelled = true,
    terminated = true,
}

local function copy_record(record: any): any
    local out = {}
    for key, value in pairs(record or {}) do out[key] = value end
    return out
end

local function copy_state(state: any): any
    local out = {
        retry_base_ms = state.retry_base_ms,
        retry_max_ms = state.retry_max_ms,
        by_dataflow = {},
        by_pid = {},
    }
    for id, record in pairs(state.by_dataflow or {}) do
        out.by_dataflow[id] = copy_record(record)
    end
    for pid, owner in pairs(state.by_pid or {}) do
        out.by_pid[pid] = copy_record(owner)
    end
    return out
end

local function none(reason: string, extra: any?): any
    local decision = { kind = M.ACTION.NONE, reason = reason }
    for key, value in pairs(extra or {}) do decision[key] = value end
    return decision
end

local function required_identity(input: any): (any?, string?)
    if type(input) ~= "table" then return nil, "input must be a table" end
    local dataflow_id = tostring(input.dataflow_id or "")
    if dataflow_id == "" then return nil, "dataflow_id is required" end
    local generation = tonumber(input.generation)
    if generation == nil or generation < 1 or generation % 1 ~= 0 then
        return nil, "generation must be a positive integer"
    end
    return { dataflow_id = dataflow_id, generation = generation }, nil
end

local function is_terminal(input: any): boolean
    if input.terminal == true then return true end
    return TERMINAL_STATUS[string.lower(tostring(input.status or ""))] == true
end

local function unbind_pid(state: any, record: any)
    if record and record.pid ~= nil then
        state.by_pid[tostring(record.pid)] = nil
        record.pid = nil
    end
end

local function remove_owner(state: any, dataflow_id: any)
    local record = state.by_dataflow[dataflow_id]
    if record then unbind_pid(state, record) end
    state.by_dataflow[dataflow_id] = nil
end

local function current_record(state: any, dataflow_id: any, generation: any): (any?, any?)
    local record = state.by_dataflow[dataflow_id]
    if not record then
        return nil, none("unknown_activation")
    end
    if record.generation ~= generation then
        return nil, none("stale_generation", {
            current_generation = record.generation,
            observed_generation = generation,
        })
    end
    return record, nil
end

local function retry_delay(state: any, failures: number): number
    local delay = tonumber(state.retry_base_ms) or DEFAULT_RETRY_BASE_MS
    local maximum = tonumber(state.retry_max_ms) or DEFAULT_RETRY_MAX_MS
    for _ = 1, failures do
        if delay >= maximum then return maximum end
        delay = math.min(delay * 2, maximum)
    end
    return delay
end

local function schedule_restart(state: any, record: any, reason: string): any
    local failures = tonumber(record.failure_count) or 0
    local delay = retry_delay(state, failures)
    record.failure_count = failures + 1
    record.phase = "restart_scheduled"
    return {
        kind = M.ACTION.RESTART,
        reason = reason,
        dataflow_id = record.dataflow_id,
        generation = record.generation,
        delay_ms = delay,
        attempt = record.failure_count,
    }
end

local function bind_pid(state: any, record: any, pid: any)
    unbind_pid(state, record)
    local pid_key = tostring(pid)
    local displaced = state.by_pid[pid_key]
    if displaced then
        local other = state.by_dataflow[displaced.dataflow_id]
        if other and other.generation == displaced.generation and tostring(other.pid or "") == pid_key then
            other.pid = nil
            other.phase = "owner_unknown"
        end
    end
    record.pid = pid_key
    record.phase = "monitored"
    state.by_pid[pid_key] = {
        dataflow_id = record.dataflow_id,
        generation = record.generation,
    }
end

function M.new(options: any?): any
    options = options or {}
    local base = tonumber(options.retry_base_ms) or DEFAULT_RETRY_BASE_MS
    local maximum = tonumber(options.retry_max_ms) or DEFAULT_RETRY_MAX_MS
    if base < 1 then base = DEFAULT_RETRY_BASE_MS end
    if maximum < base then maximum = base end
    return {
        retry_base_ms = base,
        retry_max_ms = maximum,
        by_dataflow = {},
        by_pid = {},
    }
end

-- Apply a durable activation notification. Notifications never prove whether a
-- process exists; a new active generation therefore asks the IO shell to inspect
-- the canonical process name. Repeated notifications for the same generation
-- are intentionally no-ops in every in-flight phase.
function M.on_activation(state: any, input: any): (any?, any?, string?)
    local identity, err = required_identity(input)
    if err then return nil, nil, err end
    local dataflow_id = identity.dataflow_id
    local generation = identity.generation
    local next_state: any = copy_state(state)
    local current = next_state.by_dataflow[dataflow_id]

    if current and generation < current.generation then
        return next_state, none("stale_activation", {
            current_generation = current.generation,
            observed_generation = generation,
        }), nil
    end

    if is_terminal(input) or input.desired_active == false then
        if current and generation >= current.generation then remove_owner(next_state, dataflow_id) end
        return next_state, none(is_terminal(input) and "terminal" or "inactive"), nil
    end

    if input.desired_active ~= true then
        return nil, nil, "desired_active must be a boolean"
    end

    if current and generation == current.generation then
        return next_state, none("duplicate_notification", {
            phase = current.phase,
            pid = current.pid,
        }), nil
    end

    remove_owner(next_state, dataflow_id)
    next_state.by_dataflow[dataflow_id] = {
        dataflow_id = dataflow_id,
        generation = generation,
        desired_active = true,
        phase = "owner_inspection_requested",
        failure_count = 0,
    }
    return next_state, {
        kind = M.ACTION.INSPECT_OWNER,
        reason = "active_generation",
        dataflow_id = dataflow_id,
        generation = generation,
    }, nil
end

-- Resolve a canonical process-name lookup. A present owner is monitored; only a
-- confirmed absence permits a spawn request.
function M.on_owner_observation(state: any, input: any): (any?, any?, string?)
    local identity, err = required_identity(input)
    if err then return nil, nil, err end
    local dataflow_id = identity.dataflow_id
    local generation = identity.generation
    local next_state: any = copy_state(state)
    local record, stale = current_record(next_state, dataflow_id, generation)
    if not record then return next_state, stale, nil end

    local after_exit = record.phase == "exit_owner_inspection_requested"
    local pid = input.registered_pid
    if pid ~= nil and tostring(pid) ~= "" then
        record.phase = "monitor_requested"
        return next_state, {
            kind = M.ACTION.MONITOR,
            reason = "registered_owner",
            dataflow_id = dataflow_id,
            generation = generation,
            pid = tostring(pid),
        }, nil
    end

    if after_exit then
        return next_state, schedule_restart(next_state, record, "active_exit_owner_absent"), nil
    end

    record.phase = "spawn_requested"
    return next_state, {
        kind = M.ACTION.SPAWN,
        reason = "owner_absent",
        dataflow_id = dataflow_id,
        generation = generation,
    }, nil
end

-- Converge an attempted spawn, including ambiguous outcomes. The registered
-- canonical owner wins over the returned spawn PID. If either PID may exist it
-- is monitored before another spawn is considered.
function M.on_spawn_observation(state: any, input: any): (any?, any?, string?)
    local identity, err = required_identity(input)
    if err then return nil, nil, err end
    local dataflow_id = identity.dataflow_id
    local generation = identity.generation
    local next_state: any = copy_state(state)
    local record, stale = current_record(next_state, dataflow_id, generation)
    if not record then return next_state, stale, nil end

    local registered_pid = input.registered_pid
    if registered_pid ~= nil and tostring(registered_pid) == "" then registered_pid = nil end
    local spawn_pid = input.spawn_pid
    if spawn_pid ~= nil and tostring(spawn_pid) == "" then spawn_pid = nil end
    local candidate = registered_pid or spawn_pid

    if input.monitor_ok == true and candidate ~= nil then
        bind_pid(next_state, record, candidate)
        return next_state, none("owner_monitored", { pid = tostring(candidate) }), nil
    end

    if candidate ~= nil then
        record.phase = "monitor_requested"
        return next_state, {
            kind = M.ACTION.MONITOR,
            reason = registered_pid and "canonical_owner_after_spawn" or "spawn_owner_unconfirmed",
            dataflow_id = dataflow_id,
            generation = generation,
            pid = tostring(candidate),
        }, nil
    end

    return next_state, schedule_restart(next_state, record, "spawn_unresolved"), nil
end

function M.on_monitor_observation(state: any, input: any): (any?, any?, string?)
    local identity, err = required_identity(input)
    if err then return nil, nil, err end
    local dataflow_id = identity.dataflow_id
    local generation = identity.generation
    local next_state: any = copy_state(state)
    local record, stale = current_record(next_state, dataflow_id, generation)
    if not record then return next_state, stale, nil end

    local pid = input.pid
    if input.monitor_ok == true and pid ~= nil and tostring(pid) ~= "" then
        bind_pid(next_state, record, pid)
        return next_state, none("owner_monitored", { pid = tostring(pid) }), nil
    end

    local registered_pid = input.registered_pid
    if registered_pid ~= nil and tostring(registered_pid) ~= "" and tostring(registered_pid) ~= tostring(pid or "") then
        record.phase = "monitor_requested"
        return next_state, {
            kind = M.ACTION.MONITOR,
            reason = "owner_changed_during_monitor",
            dataflow_id = dataflow_id,
            generation = generation,
            pid = tostring(registered_pid),
        }, nil
    end

    return next_state, schedule_restart(next_state, record, "monitor_unresolved"), nil
end

-- EXIT decisions require the caller's durable lifecycle observation. A PID and
-- generation must still own the dataflow or the event is stale. Only durable
-- inactive/terminal states suppress recovery: a clean process result can still
-- be a duplicate-name loser or an early failure while durable intent is active.
function M.on_exit(state: any, input: any): (any?, any?, string?)
    if type(input) ~= "table" then return nil, nil, "input must be a table" end
    local pid = tostring(input.pid or "")
    if pid == "" then return nil, nil, "pid is required" end

    local next_state: any = copy_state(state)
    local owner = next_state.by_pid[pid]
    if not owner then return next_state, none("stale_exit"), nil end

    local observed_generation = tonumber(input.generation)
    if observed_generation ~= nil and observed_generation ~= owner.generation then
        return next_state, none("stale_exit_generation", {
            current_generation = owner.generation,
            observed_generation = observed_generation,
        }), nil
    end

    local record = next_state.by_dataflow[owner.dataflow_id]
    if not record or record.generation ~= owner.generation or tostring(record.pid or "") ~= pid then
        next_state.by_pid[pid] = nil
        return next_state, none("stale_exit_owner"), nil
    end

    unbind_pid(next_state, record)
    if is_terminal(input) or input.desired_active == false then
        remove_owner(next_state, record.dataflow_id)
        local reason = "inactive_exit"
        if is_terminal(input) then reason = "terminal_exit" end
        return next_state, none(reason), nil
    end
    if input.desired_active ~= true then
        return nil, nil, "desired_active must be a boolean"
    end

    record.phase = "exit_owner_inspection_requested"
    return next_state, {
        kind = M.ACTION.INSPECT_OWNER,
        reason = input.clean == true and "clean_exit_while_active" or "unexpected_exit",
        dataflow_id = record.dataflow_id,
        generation = record.generation,
    }, nil
end

-- Called by the IO shell when a returned restart delay has elapsed. Re-inspect
-- the canonical owner before attempting another spawn.
function M.on_restart_due(state: any, input: any): (any?, any?, string?)
    local identity, err = required_identity(input)
    if err then return nil, nil, err end
    local dataflow_id = identity.dataflow_id
    local generation = identity.generation
    local next_state: any = copy_state(state)
    local record, stale = current_record(next_state, dataflow_id, generation)
    if not record then return next_state, stale, nil end
    if record.phase ~= "restart_scheduled" then
        return next_state, none("restart_not_scheduled", { phase = record.phase }), nil
    end
    record.phase = "owner_inspection_requested"
    return next_state, {
        kind = M.ACTION.INSPECT_OWNER,
        reason = "restart_due",
        dataflow_id = dataflow_id,
        generation = generation,
    }, nil
end

-- Durable progress may explicitly reset the consecutive-failure backoff. Merely
-- spawning or monitoring a PID does not, otherwise crash loops stay at the
-- minimum delay forever.
function M.mark_stable(state: any, input: any): (any?, any?, string?)
    local identity, err = required_identity(input)
    if err then return nil, nil, err end
    local dataflow_id = identity.dataflow_id
    local generation = identity.generation
    local next_state: any = copy_state(state)
    local record, stale = current_record(next_state, dataflow_id, generation)
    if not record then return next_state, stale, nil end
    record.failure_count = 0
    return next_state, none("failure_backoff_reset"), nil
end

function M.owner_for_dataflow(state: any, dataflow_id: string): any
    local record = state.by_dataflow[tostring(dataflow_id or "")]
    return record and copy_record(record) or nil
end

function M.owner_for_pid(state: any, pid: any): any
    local owner = state.by_pid[tostring(pid or "")]
    return owner and copy_record(owner) or nil
end

return M
