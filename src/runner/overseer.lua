local time = require("time")
local logger = require("logger"):named("dataflow.overseer")
local activation_repo = require("activation_repo")
local commit = require("commit")
local consts = require("consts")
local dataflow_repo = require("dataflow_repo")
local execution_frame = require("execution_frame")
local overseer_state = require("overseer_state")
local sql = require("sql")

local M = {
    activation_repo = activation_repo,
    commit = commit,
    consts = consts,
    dataflow_repo = dataflow_repo,
    execution_frame = execution_frame,
    overseer_state = overseer_state,
    process = process,
    channel = channel,
    sql = sql,
    time = time,
}

local NAME = "dataflow.overseer"
local TOPIC = "dataflow.activation.changed"
local SAFETY_INTERVAL = "30s"
local SCAN_LIMIT = 100

type OwnershipState = {
    by_pid: { [string]: string },
    by_dataflow: { [string]: string },
}

-- Spawns made for one observed ownership state that never led to an admission,
-- and when the next one may be made.
type StartAttempts = {
    fence: string,
    count: number,
    retry_at: number,
}

type Runtime = {
    ownership: OwnershipState,
    starts: { [string]: StartAttempts },
    woken: { [string]: number },
    bootstrapped: boolean,
    epoch: string?,
}

type Observation = {
    activation: any,
    status: string?,
    registered_pid: string?,
}

type Workflow = {
    dataflow_id: string?,
    actor_id: string?,
    actor_context: any,
    status: string?,
}

type WakeRow = {
    dataflow_id: string,
    wake_key: string,
    wake_at: string,
}

type ReconcileOptions = {
    message: string?,
}

local function schema_not_ready(err: any): boolean
    local message = string.lower(tostring(err or ""))
    local missing = message:find("no such table", 1, true) ~= nil or
        message:find("no such column", 1, true) ~= nil or
        message:find("does not exist", 1, true) ~= nil
    return missing and (message:find("dataflow_wakes", 1, true) ~= nil or
        message:find("dataflow_activations", 1, true) ~= nil or
        message:find("activation_generation", 1, true) ~= nil or
        message:find("dataflows", 1, true) ~= nil)
end

local function duration_until(value: string): (number?, string?)
    if value == "" then return nil, "deadline is missing" end
    local deadline, err = M.time.parse(M.time.RFC3339NANO, value)
    if err then deadline, err = M.time.parse(M.time.RFC3339, value) end
    if err then return nil, "invalid deadline: " .. value end
    local now = M.time.now()
    if now:after(deadline) or now:equal(deadline) then return 0, nil end
    return deadline:sub(now):nanoseconds(), nil
end

local function now_value(): string
    return M.time.now():format(M.time.RFC3339NANO)
end

local function with_tx(fn: (any) -> (any?, string?)): (any?, string?)
    local db, db_err = M.sql.get(tostring(M.consts.APP_DB))
    if db_err then return nil, tostring(db_err) end
    local tx, begin_err = db:begin()
    if begin_err then
        db:release()
        return nil, tostring(begin_err)
    end
    local ok, result, operation_err = pcall(fn, tx)
    if not ok or operation_err then
        tx:rollback()
        db:release()
        return nil, not ok and tostring(result) or operation_err
    end
    local committed, commit_err = tx:commit()
    if not committed or commit_err then
        tx:rollback()
        db:release()
        return nil, tostring(commit_err or "transaction did not commit")
    end
    db:release()
    return result, nil
end

M.with_tx = with_tx

-- Keep the replaceable test seam while preserving the callback's two-result
-- contract for strict callers. Fields read through M are intentionally dynamic.
local function call_with_tx(fn: (any) -> (any?, string?)): (any?, string?)
    return M.with_tx(fn)
end

local function log_flow(message: string, dataflow_id: string, err: any)
    logger:warn(message, {
        dataflow_id = dataflow_id,
        error = tostring(err or ""),
    })
end

local function is_not_found(err: any): boolean
    if err == nil then return false end
    local kind_ok, kind = pcall(function() return err:kind() end)
    if kind_ok and tostring(kind) == "NotFound" then return true end
    return string.lower(tostring(err)):find("not found", 1, true) ~= nil
end

local function is_already_monitoring(value: any): boolean
    return string.lower(tostring(value or "")):find(
        "already monitoring", 1, true) ~= nil
end

local function lookup_owner(dataflow_id: string): (string?, string?)
    local ok, pid, lookup_err = pcall(M.process.registry.lookup, "dataflow." .. dataflow_id)
    if not ok then return nil, tostring(pid) end
    if pid == nil and lookup_err ~= nil then
        if is_not_found(lookup_err) then return nil, nil end
        return nil, tostring(lookup_err)
    end
    return pid and tostring(pid) or nil, nil
end

local function clone_launch_args(value: { [string]: any }?): { [string]: any }
    local result: { [string]: any } = {}
    for key, item in pairs(value or {}) do result[key] = item end
    return result
end

function M.new_runtime(epoch: string?): Runtime
    return {
        ownership = M.overseer_state.new() :: OwnershipState,
        starts = {},
        woken = {},
        bootstrapped = false,
        epoch = epoch,
    }
end

local function load_runtime_epoch(): (string?, string?)
    local value, err = env.get(M.consts.RUNTIME_EPOCH_ENV)
    -- The service can start before the migrations-ready bootloader. A missing
    -- value is expected readiness state, not an operational failure.
    if err then
        if is_not_found(err) then return nil, nil end
        return nil, tostring(err)
    end
    if value == nil or tostring(value) == "" then return nil, nil end
    return tostring(value), nil
end

M.load_runtime_epoch = load_runtime_epoch

-- A live owner absorbs newer requests. Waking it once for each durable
-- generation it has not been woken for makes it reload pending work, so no
-- request depends on a message whose sender may have died after committing.
local function wake_owner(runtime: Runtime, dataflow_id: string, pid: string, generation: number?)
    if not generation then return end
    local woken = runtime.woken[pid]
    if woken and woken >= generation then return end
    local ok, sent, send_err = pcall(M.process.send, pid, M.consts.MESSAGE_TOPIC.WAKE, {
        dataflow_id = dataflow_id,
        generation = generation,
    })
    if not ok or not sent then
        log_flow("owner wake delivery failed", dataflow_id, not ok and sent or send_err)
        return
    end
    runtime.woken[pid] = generation
end

local function failure_message(event: any): string
    local result = event and event.result or nil
    if result and result.error ~= nil then return tostring(result.error) end
    local value = result and result.value or nil
    if type(value) == "table" then
        if value.error ~= nil then return tostring(value.error) end
        if value.message ~= nil then return tostring(value.message) end
    end
    return "active orchestrator exited before reaching a durable terminal or waiting state"
end

local MAX_RECONCILE_PASSES = 4
-- Spawns allowed for one observed ownership state. An orchestrator that exits
-- before admitting itself leaves that state unchanged; the next spawn waits
-- for start_retry_delay, so a transient outage can pass before the last one.
local MAX_STARTS = 5
local START_RETRY_BASE_NS = 1000000000

M.MAX_STARTS = MAX_STARTS

function M.start_retry_delay(attempt: number): number
    return START_RETRY_BASE_NS * math.floor(2 ^ (attempt - 1))
end

function M.clock(): number
    return M.time.now():unix_nano()
end

-- Read the activation and the canonical name together under the workflow lock,
-- so an admission or release in flight finishes before the name is checked.
local function observe(dataflow_id: string): (Observation?, string?)
    local observed, observe_err = call_with_tx(function(tx)
        local locked, locked_err = M.activation_repo.read_locked_tx(tx, dataflow_id)
        if locked_err then return nil, tostring(locked_err) end
        local pid, lookup_err = lookup_owner(dataflow_id)
        if lookup_err then return nil, "canonical owner lookup failed: " .. lookup_err end
        return {
            activation = locked and locked.activation or nil,
            status = locked and locked.status or nil,
            registered_pid = pid,
        }, nil
    end)
    if observe_err then return nil, tostring(observe_err) end
    return observed :: Observation, nil
end

local function stop_owner(pid: string): (boolean?, string?)
    local cancel_ok, cancelled, cancel_err = pcall(M.process.cancel, pid, "5s")
    if cancel_ok and cancelled == true then return true, nil end
    local cancel_failure = cancel_ok and cancel_err or cancelled
    if is_not_found(cancel_failure) then return true, nil end
    local terminate_ok, terminated, terminate_err = pcall(M.process.terminate, pid)
    local terminate_failure = terminate_ok and terminate_err or terminated
    if (not terminate_ok or terminated ~= true) and not is_not_found(terminate_failure) then
        return nil, "failed to stop orchestrator: " .. tostring(
            terminate_err or terminated or cancel_err or cancelled)
    end
    return true, nil
end

local function monitor_owner(runtime: Runtime, dataflow_id: string, pid: string, generation: number?): boolean
    local ok, monitored, monitor_err = pcall(M.process.monitor, pid)
    local monitor_ok = ok and (monitored == true or
        is_already_monitoring(monitored) or is_already_monitoring(monitor_err))
    if not monitor_ok then return false end
    M.overseer_state.track(runtime.ownership, dataflow_id, pid)
    wake_owner(runtime, dataflow_id, pid, generation)
    return true
end

local function fence_key(fence: any): string
    return table.concat({
        tostring(fence.token or ""), tostring(fence.phase or ""), tostring(fence.generation or ""),
    }, "|")
end

-- The spawns made for the observed ownership state; a changed state starts over.
local function start_budget(runtime: Runtime, dataflow_id: string, fence: any): StartAttempts
    local key = fence_key(fence)
    local attempts = runtime.starts[dataflow_id]
    if attempts and attempts.fence == key then return attempts end
    local fresh: StartAttempts = { fence = key, count = 0, retry_at = 0 }
    runtime.starts[dataflow_id] = fresh
    return fresh
end

local function fail(dataflow_id: string, fence: any, reason: string, message: string): (any?, string?)
    return M.commit.fail_activation(dataflow_id, fence, {
        source = "dataflow.overseer",
        reason = reason,
        message = message,
        failed_at = now_value(),
    })
end

-- Spawn the canonical orchestrator for the observed request. Returns the
-- spawned pid, or a name conflict marker, or the reason spawning is impossible.
local function spawn_owner(
    runtime: Runtime,
    dataflow_id: string,
    activation: any,
    generation: number
): (string?, boolean, string?)
    local raw_workflow, workflow_err = M.dataflow_repo.get(dataflow_id)
    if workflow_err or not raw_workflow then
        return nil, false, "durable spawn state unavailable: " .. tostring(workflow_err or "missing row")
    end
    local workflow = raw_workflow :: Workflow
    local actor, scope, frame_err = M.execution_frame.reconstruct(workflow.actor_id, workflow.actor_context)
    if frame_err or not actor or not scope then
        return nil, false, "execution frame reconstruction failed: " .. tostring(
            frame_err or "missing actor or scope")
    end
    local launch_args: { [string]: any }? = nil
    if type(activation.launch_args) == "table" then
        launch_args = activation.launch_args :: { [string]: any }
    end
    local args = clone_launch_args(launch_args)
    args.dataflow_id = dataflow_id
    args.activation_generation = generation
    local spawn_ok, spawn_pid, spawn_err = pcall(function()
        return M.process.with_context({})
            :with_name("dataflow." .. dataflow_id)
            :with_actor(actor)
            :with_scope(scope)
            :spawn_monitored(tostring(M.consts.ORCHESTRATOR), tostring(M.consts.HOST_ID), args)
    end)
    if spawn_ok and spawn_pid then return tostring(spawn_pid), false, nil end
    local failure = spawn_ok and spawn_err or spawn_pid
    local registered_pid = select(1, lookup_owner(dataflow_id))
    if registered_pid then return nil, true, nil end
    return nil, false, "orchestrator spawn failed: " .. tostring(failure or "spawn returned no PID")
end

-- Converge one dataflow on its durable ownership record. Every pass starts
-- from a fresh locked observation; nothing decided earlier is needed.
function M.reconcile(runtime: Runtime, dataflow_id: string, options: ReconcileOptions?): (boolean?, string?)
    if not runtime.epoch then return nil, "runtime epoch is unavailable" end
    local message = options and options.message or "active orchestrator disappeared during runtime"
    for _ = 1, MAX_RECONCILE_PASSES do
        local observed, observe_err = observe(dataflow_id)
        if observe_err or not observed then return nil, tostring(observe_err) end
        local activation = observed.activation
        if not activation then
            M.overseer_state.forget_dataflow(runtime.ownership, dataflow_id)
            return true, nil
        end
        local decision: any = M.overseer_state.decide({
            dataflow_id = dataflow_id,
            status = observed.status,
            desired_active = activation.desired_active == true,
            generation = tonumber(activation.generation),
            owner_token = activation.owner_token and tostring(activation.owner_token) or nil,
            owner_phase = activation.owner_phase and tostring(activation.owner_phase) or nil,
            owner_epoch = activation.owner_epoch and tostring(activation.owner_epoch) or nil,
            registered_pid = observed.registered_pid,
            runtime_epoch = runtime.epoch,
        })
        if decision.kind ~= M.overseer_state.ACTION.SPAWN and
            decision.kind ~= M.overseer_state.ACTION.MONITOR then
            runtime.starts[dataflow_id] = nil
        end

        if decision.kind == M.overseer_state.ACTION.NONE then
            return true, nil
        elseif decision.kind == M.overseer_state.ACTION.STOP then
            return stop_owner(tostring(decision.pid))
        elseif decision.kind == M.overseer_state.ACTION.MONITOR then
            if monitor_owner(runtime, dataflow_id, tostring(decision.pid), tonumber(decision.generation)) then
                return true, nil
            end
        elseif decision.kind == M.overseer_state.ACTION.FAIL then
            local failed, fail_err = fail(dataflow_id, decision.fence, tostring(decision.reason), message)
            if fail_err then return nil, tostring(fail_err) end
            if failed and (failed.completed == true or failed.terminal == true) then return true, nil end
        elseif decision.kind == M.overseer_state.ACTION.SPAWN then
            local attempts = start_budget(runtime, dataflow_id, decision.fence)
            if attempts.count >= MAX_STARTS then
                -- The budget stays exhausted until the failure is durable and the
                -- observed state changes.
                local failed, fail_err = fail(dataflow_id, decision.fence, "orchestrator_start_failed", message)
                if fail_err then return nil, tostring(fail_err) end
                if failed and (failed.completed == true or failed.terminal == true) then return true, nil end
            elseif M.clock() < attempts.retry_at then
                return true, nil
            else
                local pid, conflict, spawn_err = spawn_owner(
                    runtime, dataflow_id, activation, tonumber(decision.generation) or 1)
                if pid then
                    attempts.count = attempts.count + 1
                    attempts.retry_at = M.clock() + M.start_retry_delay(attempts.count)
                    M.overseer_state.track(runtime.ownership, dataflow_id, pid)
                    -- A new orchestrator loads everything up to its generation.
                    runtime.woken[pid] = tonumber(decision.generation)
                    return true, nil
                end
                if not conflict then
                    local failed, fail_err = fail(dataflow_id, decision.fence,
                        "orchestrator_spawn_failed", tostring(spawn_err))
                    if fail_err then return nil, tostring(fail_err) end
                    if failed and (failed.completed == true or failed.terminal == true) then return true, nil end
                end
            end
        else
            return nil, "unknown overseer decision " .. tostring(decision.kind)
        end
    end
    return nil, "ownership of " .. dataflow_id .. " did not settle"
end

local function pending_due(now: string, limit: number): ({ WakeRow }?, string?)
    local db, db_err = M.sql.get(tostring(M.consts.APP_DB))
    if db_err then return nil, tostring(db_err) end
    local db_type, type_err = db:type()
    if type_err then db:release(); return nil, tostring(type_err) end
    local placeholder = "?"
    if db_type == M.sql.type.POSTGRES or db_type == "postgres" then placeholder = "$1" end
    local query = [[
        SELECT dataflow_id, wake_key, wake_at FROM dataflow_wakes
        WHERE activation_generation IS NULL AND wake_at <= ]] .. placeholder .. [[
        ORDER BY wake_at ASC, dataflow_id ASC, wake_key ASC LIMIT ]] .. tostring(limit)
    local rows, query_err = db:query(query, { now })
    db:release()
    if query_err then return nil, tostring(query_err) end
    return ((rows or {}) :: any) :: { WakeRow }, nil
end

M.pending_due = pending_due

function M.promote_due(runtime: Runtime): (number?, string?)
    local now = now_value()
    local rows, due_err = M.pending_due(now, SCAN_LIMIT)
    if due_err then return nil, due_err end
    local promoted = 0
    for _, row in ipairs(rows or {}) do
        local activation, activation_err = call_with_tx(function(tx)
            local result, err = M.activation_repo.activate_due_tx(
                tx, tostring(row.dataflow_id), tostring(row.wake_key), now)
            return result, err and tostring(err) or nil
        end)
        if activation_err then
            if schema_not_ready(activation_err) then return nil, activation_err end
            log_flow("due wake promotion failed", tostring(row.dataflow_id), activation_err)
        elseif activation and activation.promoted then
            promoted = promoted + 1
            local ok, reconcile_err = M.reconcile(runtime, tostring(row.dataflow_id))
            if not ok then
                log_flow("promoted activation reconciliation failed",
                    tostring(row.dataflow_id), reconcile_err)
            end
        end
    end
    return promoted, nil
end

function M.reconcile_all(runtime: Runtime): (number?, string?)
    local active, list_err = M.activation_repo.list_active()
    if list_err then return nil, tostring(list_err) end
    local seen: { [string]: boolean } = {}
    local active_count = 0
    for _, activation in ipairs(active or {}) do
        active_count = active_count + 1
        local id = tostring(activation.dataflow_id)
        seen[id] = true
        local ok, reconcile_err = M.reconcile(runtime, id)
        if not ok then log_flow("active activation reconciliation failed", id, reconcile_err) end
    end
    -- A tracked dataflow whose activation is no longer active is reconciled too:
    -- the holder of a terminal one is stopped.
    for _, id in ipairs(M.overseer_state.tracked(runtime.ownership)) do
        if not seen[id] then
            local ok, reconcile_err = M.reconcile(runtime, id)
            if not ok then log_flow("inactive activation reconciliation failed", id, reconcile_err) end
        end
    end
    return active_count, nil
end

function M.bootstrap(runtime: Runtime): (number?, string?)
    if not runtime.epoch then
        local epoch, epoch_err = M.load_runtime_epoch()
        if epoch_err then return nil, epoch_err end
        if not epoch then return nil, "dataflow runtime epoch is not ready" end
        runtime.epoch = epoch
    end
    local _, due_err = M.promote_due(runtime)
    if due_err then return nil, due_err end
    local count, reconcile_err = M.reconcile_all(runtime)
    if reconcile_err then return nil, reconcile_err end
    runtime.bootstrapped = true
    return count, nil
end

function M.handle_activation_hint(runtime: Runtime, payload: any): (boolean?, string?)
    if type(payload) ~= "table" or type(payload.dataflow_id) ~= "string" or
        payload.dataflow_id == "" then
        return nil, "activation hint identity is invalid"
    end
    return M.reconcile(runtime, payload.dataflow_id)
end

function M.safety_reconcile(runtime: Runtime): (number?, string?)
    local _, due_err = M.promote_due(runtime)
    if due_err then return nil, due_err end
    return M.reconcile_all(runtime)
end

function M.handle_exit(runtime: Runtime, event: any): (boolean?, string?)
    local pid = event and event.from and tostring(event.from) or nil
    if not pid then return true, nil end
    runtime.woken[pid] = nil
    local dataflow_id = M.overseer_state.forget_pid(runtime.ownership, pid)
    if not dataflow_id then return true, nil end
    return M.reconcile(runtime, dataflow_id, { message = failure_message(event) })
end

-- The wake deadline as RFC 3339 text at the precision it is stored with. The
-- PostgreSQL driver renders TIMESTAMPTZ values without fractional seconds, so
-- the deadline is formatted in SQL; SQLite stores the text as written.
function M.wake_at_column(db_type: any): string
    if db_type == M.sql.type.POSTGRES or db_type == "postgres" then
        return [[to_char(dataflow_wakes.wake_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')]]
    end
    return "dataflow_wakes.wake_at"
end

-- The nearest pending wake, or the nearest one after a given time.
function M.next_pending_wake(after: string?): (any?, string?)
    local db, db_err = M.sql.get(tostring(M.consts.APP_DB))
    if db_err then return nil, tostring(db_err) end
    local db_type, type_err = db:type()
    if type_err then db:release(); return nil, tostring(type_err) end
    local bound = ""
    local params: { any } = {}
    if after ~= nil then
        bound = " AND dataflow_wakes.wake_at > ?"
        if db_type == M.sql.type.POSTGRES or db_type == "postgres" then
            bound = " AND dataflow_wakes.wake_at > $1"
        end
        params = { after }
    end
    local rows, query_err = db:query(
        "SELECT dataflow_id, wake_key, " .. M.wake_at_column(db_type) .. [[ AS wake_at
        FROM dataflow_wakes
        WHERE activation_generation IS NULL]] .. bound .. [[
        ORDER BY dataflow_wakes.wake_at ASC, dataflow_id ASC, wake_key ASC LIMIT 1
    ]], params)
    db:release()
    if query_err then return nil, tostring(query_err) end
    return rows and rows[1] or nil, nil
end

function M.notify(payload: any?): (boolean?, string?)
    return M.process.send(NAME, TOPIC, payload or {})
end

local function reconcile_or_log(runtime: Runtime, operation: (Runtime) -> (any?, string?))
    local ok, err = operation(runtime)
    if not ok and err then
        local waiting_for_epoch = tostring(err):find(
            "dataflow runtime epoch is not ready", 1, true) ~= nil
        if schema_not_ready(err) or waiting_for_epoch then
            runtime.bootstrapped = false
            logger:debug("overseer waiting for boot readiness", { error = tostring(err) })
        else
            logger:warn("overseer reconciliation failed", { error = tostring(err) })
        end
    end
end

-- Promote the wakes that are already due, then arm a timer for the nearest wake
-- that is not. A due wake that cannot be promoted never hides a later one; it is
-- retried on the next event or safety pass.
function M.wake_timer(runtime: Runtime): any?
    local nearest, nearest_err = M.next_pending_wake()
    if nearest_err then
        if not schema_not_ready(nearest_err) then
            logger:warn("could not inspect nearest dataflow wake", { error = tostring(nearest_err) })
        end
        return nil
    end
    if not nearest then return nil end
    local wait_ns = select(1, duration_until(tostring(nearest.wake_at)))
    if wait_ns ~= nil and wait_ns > 0 then return M.time.after(wait_ns) end
    reconcile_or_log(runtime, runtime.bootstrapped and M.promote_due or M.bootstrap)
    local upcoming, upcoming_err = M.next_pending_wake(now_value())
    if upcoming_err or not upcoming then return nil end
    local upcoming_ns = select(1, duration_until(tostring(upcoming.wake_at)))
    if upcoming_ns == nil or upcoming_ns <= 0 then return nil end
    return M.time.after(upcoming_ns)
end

-- Spawn again for every dataflow whose start retry is due, then arm a timer for
-- the nearest one that is not.
function M.retry_starts(runtime: Runtime): (boolean?, string?)
    local now = M.clock()
    local due: { string } = {}
    for dataflow_id, attempts in pairs(runtime.starts) do
        if attempts.count > 0 and attempts.count < MAX_STARTS and attempts.retry_at <= now then
            table.insert(due, dataflow_id)
        end
    end
    for _, dataflow_id in ipairs(due) do
        local ok, reconcile_err = M.reconcile(runtime, dataflow_id)
        if not ok then log_flow("orchestrator start retry failed", dataflow_id, reconcile_err) end
    end
    return true, nil
end

local function start_retry_timer(runtime: Runtime): any?
    local now = M.clock()
    local nearest: number? = nil
    for _, attempts in pairs(runtime.starts) do
        if attempts.count > 0 and attempts.count < MAX_STARTS and attempts.retry_at > now and
            (nearest == nil or attempts.retry_at < nearest) then
            nearest = attempts.retry_at
        end
    end
    if nearest == nil then return nil end
    return M.time.after(nearest - now)
end

function M.run(_args: any)
    local registered, register_err = M.process.registry.register(NAME)
    if not registered then error("overseer registration failed: " .. tostring(register_err)) end

    local runtime = M.new_runtime()
    reconcile_or_log(runtime, M.bootstrap)
    local inbox = M.process.inbox()
    local events = M.process.events()

    while true do
        M.retry_starts(runtime)
        local wake_timer = M.wake_timer(runtime)
        local retry_timer = start_retry_timer(runtime)

        local safety_timer = M.time.after(SAFETY_INTERVAL)
        local cases = { inbox:case_receive(), events:case_receive(), safety_timer:case_receive() }
        if wake_timer then table.insert(cases, wake_timer:case_receive()) end
        if retry_timer then table.insert(cases, retry_timer:case_receive()) end

        local result = M.channel.select(cases)
        if not result.ok then break end
        if result.channel == events then
            local event = result.value
            if event.kind == M.process.event.CANCEL then break end
            if event.kind == M.process.event.EXIT or event.kind == M.process.event.LINK_DOWN then
                local ok, exit_err = M.handle_exit(runtime, event)
                if not ok then log_flow("orchestrator EXIT reconciliation failed",
                    tostring(event.from or ""), exit_err) end
            end
        elseif result.channel == wake_timer then
            reconcile_or_log(runtime, runtime.bootstrapped and M.promote_due or M.bootstrap)
        elseif result.channel == retry_timer then
            M.retry_starts(runtime)
        elseif result.channel == inbox then
            local message = result.value
            local topic = message and tostring(message:topic()) or ""
            local payload = message and message:payload():data() or nil
            if topic == TOPIC and type(payload) == "table" and
                type(payload.runtime_epoch) == "string" and payload.runtime_epoch ~= "" then
                runtime.epoch = payload.runtime_epoch
            end
            if not runtime.bootstrapped then
                reconcile_or_log(runtime, M.bootstrap)
            elseif topic == TOPIC and type(payload) == "table" and payload.dataflow_id then
                local function targeted(current: Runtime)
                    return M.handle_activation_hint(current, payload)
                end
                reconcile_or_log(runtime, targeted)
            elseif topic == TOPIC then
                reconcile_or_log(runtime, M.promote_due)
            end
        else
            reconcile_or_log(runtime, runtime.bootstrapped and M.safety_reconcile or M.bootstrap)
        end
    end
    return { status = "shutdown" }
end

M.NAME = NAME
M.TOPIC = TOPIC
M.duration_until = duration_until
M.schema_not_ready = schema_not_ready
return M
