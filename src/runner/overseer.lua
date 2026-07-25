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
local RUNTIME_EPOCH_ENV = "userspace.dataflow.env:runtime_epoch"

type OwnerReference = {
    dataflow_id: string,
    generation: number,
}

type OwnershipRecord = {
    dataflow_id: string,
    generation: number,
    phase: string,
    pid: string?,
    claim_required: boolean,
    claim_from_epoch: string?,
    candidate_pid: string?,
}

type OwnershipState = {
    by_dataflow: { [string]: OwnershipRecord },
    by_pid: { [string]: string },
}

type Decision = {
    kind: string,
    reason: string,
    dataflow_id: string?,
    generation: number?,
    pid: string?,
    message: string?,
    observed_epoch: string?,
}

type Runtime = {
    ownership: OwnershipState,
    nudges: { [string]: Nudge },
    known: { [string]: boolean },
    bootstrapped: boolean,
    epoch: string?,
}

type Nudge = {
    dataflow_id: string,
    generation: number,
    wake_key: string?,
    wake_at: string?,
}

type Activation = {
    dataflow_id: string?,
    generation: number?,
    desired_active: boolean?,
    owner_epoch: string?,
    launch_args: table?,
    promoted: boolean?,
    requested_at: string?,
    updated_at: string?,
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

local TERMINAL_STATUS = {
    [consts.STATUS.COMPLETED_SUCCESS] = true,
    [consts.STATUS.COMPLETED_FAILURE] = true,
    [consts.STATUS.CANCELLED] = true,
    [consts.STATUS.TERMINATED] = true,
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
    local db, db_err = M.sql.get(M.consts.APP_DB)
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

local function is_terminal(status: string?): boolean
    return TERMINAL_STATUS[tostring(status or "")] == true
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

local function apply_transition(
    runtime: Runtime,
    next_state: OwnershipState?,
    decision: Decision?,
    err: any
): (Decision?, string?)
    if err then return nil, tostring(err) end
    if not next_state or not decision then return nil, "overseer transition returned no result" end
    runtime.ownership = next_state :: OwnershipState
    return decision :: Decision, nil
end

local function clone_launch_args(value: { [string]: any }?): { [string]: any }
    local result: { [string]: any } = {}
    for key, item in pairs(value or {}) do result[key] = item end
    return result
end

function M.new_runtime(epoch: string?): Runtime
    return {
        ownership = M.overseer_state.new() :: OwnershipState,
        nudges = {},
        known = {},
        bootstrapped = false,
        epoch = epoch,
    }
end

local function load_runtime_epoch(): (string?, string?)
    local value, err = env.get(RUNTIME_EPOCH_ENV)
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

local function deliver_nudge(runtime: Runtime, dataflow_id: string, pid: string): (boolean?, string?)
    local nudge = runtime.nudges[dataflow_id]
    if not nudge then return true, nil end
    local ok, sent, send_err = pcall(M.process.send, pid, M.consts.MESSAGE_TOPIC.WAKE, nudge)
    if not ok or not sent then return nil, not ok and tostring(sent) or tostring(send_err) end
    runtime.nudges[dataflow_id] = nil
    return true, nil
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

function M.drive_decision(runtime: Runtime, initial: Decision?): (boolean?, string?)
    local decision = initial
    for _ = 1, 10 do
        if not decision or decision.kind == M.overseer_state.ACTION.NONE then
            if decision and decision.reason == "owner_monitored" and decision.pid then
                local owner = M.overseer_state.owner_for_pid(
                    runtime.ownership, decision.pid) :: OwnerReference?
                if owner then
                    local delivered, delivery_err = deliver_nudge(
                        runtime, owner.dataflow_id, decision.pid)
                    if not delivered then
                        log_flow("owner nudge delivery failed", owner.dataflow_id, delivery_err)
                    end
                end
            end
            return true, nil
        end

        local dataflow_id = decision.dataflow_id
        local generation = decision.generation
        if not dataflow_id or not generation then return nil, "decision identity is missing" end

        if decision.kind == M.overseer_state.ACTION.INSPECT_OWNER then
            local pid, lookup_err = lookup_owner(dataflow_id)
            if lookup_err then return nil, "canonical owner lookup failed: " .. lookup_err end
            decision = select(1, apply_transition(runtime,
                M.overseer_state.on_owner_observation(runtime.ownership, {
                    dataflow_id = dataflow_id,
                    generation = generation,
                    registered_pid = pid,
                    message = decision.message,
                })))

        elseif decision.kind == M.overseer_state.ACTION.CLAIM then
            if not runtime.epoch then return nil, "runtime epoch is unavailable" end
            local claimed, claim_err = M.with_tx(function(tx)
                return M.activation_repo.claim_epoch_tx(
                    tx, dataflow_id, generation, decision.observed_epoch,
                    runtime.epoch, now_value())
            end)
            if claim_err then return nil, tostring(claim_err) end
            decision = select(1, apply_transition(runtime,
                M.overseer_state.on_claim_observation(runtime.ownership, {
                    dataflow_id = dataflow_id,
                    generation = generation,
                    claimed = claimed ~= nil and claimed.claimed == true,
                })))

        elseif decision.kind == M.overseer_state.ACTION.REFRESH then
            local current, current_err = M.activation_repo.get(dataflow_id)
            local workflow, workflow_err = M.dataflow_repo.get(dataflow_id)
            if current_err or workflow_err then
                return nil, tostring(current_err or workflow_err)
            end
            if not current then return true, nil end
            return M.reconcile_activation(runtime, current, workflow)

        elseif decision.kind == M.overseer_state.ACTION.MONITOR then
            if not decision.pid then return nil, "monitor decision has no PID" end
            local ok, monitored, monitor_err = pcall(M.process.monitor, decision.pid)
            local monitor_ok = ok and (monitored == true or
                is_already_monitoring(monitored) or is_already_monitoring(monitor_err))
            local registered_pid = nil
            if not monitor_ok then registered_pid = select(1, lookup_owner(dataflow_id)) end
            decision = select(1, apply_transition(runtime,
                M.overseer_state.on_monitor_observation(runtime.ownership, {
                    dataflow_id = dataflow_id,
                    generation = generation,
                    pid = decision.pid,
                    monitor_ok = monitor_ok,
                    registered_pid = registered_pid,
                    error = not ok and tostring(monitored) or tostring(monitor_err or "monitor failed"),
                })))

        elseif decision.kind == M.overseer_state.ACTION.STOP then
            if not decision.pid then return nil, "stop decision has no PID" end
            local registered_pid, lookup_err = lookup_owner(dataflow_id)
            if lookup_err then return nil, "terminal owner lookup failed: " .. lookup_err end
            if not registered_pid then
                decision = nil
                goto continue_decision
            end
            local target_pid = registered_pid
            local cancel_ok, cancelled, cancel_err = pcall(M.process.cancel, target_pid, "5s")
            if not cancel_ok or cancelled ~= true then
                local cancel_failure = cancel_ok and cancel_err or cancelled
                if is_not_found(cancel_failure) then
                    decision = nil
                else
                    local terminate_ok, terminated, terminate_err = pcall(
                        M.process.terminate, target_pid)
                    local terminate_failure = terminate_ok and terminate_err or terminated
                    if (not terminate_ok or terminated ~= true) and
                        not is_not_found(terminate_failure) then
                        return nil, "failed to stop terminal orchestrator: " .. tostring(
                            terminate_err or terminated or cancel_err or cancelled)
                    end
                    decision = nil
                end
            else
                decision = nil
            end

        elseif decision.kind == M.overseer_state.ACTION.SPAWN then
            local activation, activation_err = M.activation_repo.get(dataflow_id)
            local workflow, workflow_err = M.dataflow_repo.get(dataflow_id)
            if activation_err or workflow_err or not activation or not workflow then
                decision = select(1, apply_transition(runtime,
                    M.overseer_state.on_spawn_observation(runtime.ownership, {
                        dataflow_id = dataflow_id,
                        generation = generation,
                        error = "durable spawn state unavailable: " .. tostring(
                            activation_err or workflow_err or "missing row"),
                    })))
            elseif activation.desired_active ~= true or
                tonumber(activation.generation) ~= generation or is_terminal(workflow.status) then
                decision = select(1, apply_transition(runtime,
                    M.overseer_state.on_activation(runtime.ownership, {
                        dataflow_id = dataflow_id,
                        generation = tonumber(activation.generation) or generation,
                        desired_active = activation.desired_active == true,
                        status = tostring(workflow.status),
                        owner_epoch = activation.owner_epoch and
                            tostring(activation.owner_epoch) or nil,
                        runtime_epoch = runtime.epoch,
                    })))
            else
                local actor, scope, frame_err = M.execution_frame.reconstruct(
                    workflow.actor_id, workflow.actor_context)
                if frame_err or not actor or not scope then
                    decision = select(1, apply_transition(runtime,
                        M.overseer_state.on_spawn_observation(runtime.ownership, {
                            dataflow_id = dataflow_id,
                            generation = generation,
                            error = "execution frame reconstruction failed: " .. tostring(
                                frame_err or "missing actor or scope"),
                        })))
                else
                    local args = clone_launch_args(activation.launch_args)
                    args.dataflow_id = dataflow_id
                    args.activation_generation = generation
                    local spawn_ok, spawn_pid, spawn_err = pcall(function()
                        return M.process.with_context({})
                            :with_name("dataflow." .. dataflow_id)
                            :with_actor(actor)
                            :with_scope(scope)
                            :spawn_monitored(M.consts.ORCHESTRATOR, M.consts.HOST_ID, args)
                    end)
                    if not spawn_ok then
                        spawn_err = tostring(spawn_pid)
                        spawn_pid = nil
                    end
                    local registered_pid = select(1, lookup_owner(dataflow_id))
                    decision = select(1, apply_transition(runtime,
                        M.overseer_state.on_spawn_observation(runtime.ownership, {
                            dataflow_id = dataflow_id,
                            generation = generation,
                            spawn_pid = spawn_pid and tostring(spawn_pid) or nil,
                            registered_pid = registered_pid,
                            error = spawn_pid and nil or tostring(spawn_err or "spawn returned no PID"),
                        })))
                end
            end

        elseif decision.kind == M.overseer_state.ACTION.FAIL then
            local failure, failure_err = M.commit.fail_activation(dataflow_id, generation, {
                source = "dataflow.overseer",
                reason = decision.reason,
                message = decision.message,
                failed_at = now_value(),
            })
            if failure_err then return nil, failure_err end
            decision = select(1, apply_transition(runtime,
                M.overseer_state.on_failed(runtime.ownership, {
                    dataflow_id = dataflow_id,
                    generation = generation,
                })))
            if failure and failure.completed ~= true then
                local current, current_err = M.activation_repo.get(dataflow_id)
                local workflow, workflow_err = M.dataflow_repo.get(dataflow_id)
                if current_err or workflow_err then
                    return nil, tostring(current_err or workflow_err)
                end
                if current then
                    local reconciled, reconcile_err = M.reconcile_activation(runtime, current, workflow)
                    if not reconciled then return nil, reconcile_err end
                end
            end
        else
            return nil, "unknown overseer decision " .. tostring(decision.kind)
        end
        ::continue_decision::
    end
    return nil, "overseer decision chain exceeded safety bound"
end

function M.reconcile_activation(
    runtime: Runtime,
    raw_activation: any,
    raw_workflow: any?,
    nudge: Nudge?
): (boolean?, string?)
    if type(raw_activation) ~= "table" then return nil, "activation must be a table" end
    local normalized_launch_args: table? = nil
    if type(raw_activation.launch_args) == "table" then
        normalized_launch_args = raw_activation.launch_args :: table
    end
    local activation: Activation = {
        dataflow_id = raw_activation.dataflow_id and
            tostring(raw_activation.dataflow_id) or nil,
        generation = tonumber(raw_activation.generation),
        desired_active = raw_activation.desired_active == true,
        owner_epoch = raw_activation.owner_epoch and tostring(raw_activation.owner_epoch) or nil,
        launch_args = normalized_launch_args,
        promoted = raw_activation.promoted == true,
        requested_at = raw_activation.requested_at and
            tostring(raw_activation.requested_at) or nil,
        updated_at = raw_activation.updated_at and tostring(raw_activation.updated_at) or nil,
    }
    local workflow: Workflow? = nil
    if type(raw_workflow) == "table" then
        workflow = {
            dataflow_id = raw_workflow.dataflow_id and tostring(raw_workflow.dataflow_id) or nil,
            actor_id = raw_workflow.actor_id and tostring(raw_workflow.actor_id) or nil,
            actor_context = raw_workflow.actor_context,
            status = raw_workflow.status and tostring(raw_workflow.status) or nil,
        }
    end
    local dataflow_id = tostring(activation.dataflow_id or "")
    local generation = tonumber(activation.generation)
    if dataflow_id == "" or not generation then return nil, "activation identity is invalid" end
    if not runtime.epoch then return nil, "runtime epoch is unavailable" end
    runtime.known[dataflow_id] = true

    local current = M.overseer_state.owner_for_dataflow(runtime.ownership, dataflow_id)
    if activation.desired_active == true and (not current or generation > current.generation) then
        runtime.nudges[dataflow_id] = nudge or {
            dataflow_id = dataflow_id,
            generation = generation,
        }
    elseif activation.desired_active ~= true or (workflow and is_terminal(workflow.status)) then
        runtime.nudges[dataflow_id] = nil
    end

    local next_state, next_decision, transition_err = M.overseer_state.on_activation(
        runtime.ownership, {
        dataflow_id = dataflow_id,
        generation = generation,
        desired_active = activation.desired_active == true,
        status = workflow and tostring(workflow.status) or nil,
        owner_epoch = activation.owner_epoch and tostring(activation.owner_epoch) or nil,
        runtime_epoch = runtime.epoch,
    })
    local decision, state_err = apply_transition(
        runtime, next_state, next_decision, transition_err)
    if state_err then return nil, state_err end
    return M.drive_decision(runtime, decision)
end

local function pending_due(now: string, limit: number): ({ WakeRow }?, string?)
    local db, db_err = M.sql.get(M.consts.APP_DB)
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
        local activation, activation_err = M.with_tx(function(tx)
            return M.activation_repo.activate_due_tx(
                tx, tostring(row.dataflow_id), tostring(row.wake_key), now)
        end)
        if activation_err then
            if schema_not_ready(activation_err) then return nil, activation_err end
            log_flow("due wake promotion failed", tostring(row.dataflow_id), activation_err)
        elseif activation and activation.promoted then
            promoted = promoted + 1
            local promoted_generation = tonumber(activation.generation)
            if not promoted_generation then
                log_flow("promoted activation has invalid generation",
                    tostring(row.dataflow_id), "generation is missing")
                goto continue_due
            end
            local ok, reconcile_err = M.reconcile_activation(
                runtime, activation :: Activation, nil, {
                dataflow_id = tostring(row.dataflow_id),
                generation = promoted_generation,
                wake_key = tostring(row.wake_key),
                wake_at = row.wake_at and tostring(row.wake_at) or nil,
            })
            if not ok then
                log_flow("promoted activation reconciliation failed",
                    tostring(row.dataflow_id), reconcile_err)
            end
        end
        ::continue_due::
    end
    return promoted, nil
end

function M.reconcile_all(runtime: Runtime): (number?, string?)
    local active, list_err = M.activation_repo.list_active()
    if list_err then return nil, tostring(list_err) end
    local active_ids: { [string]: boolean } = {}
    local active_count = 0
    for _, activation in ipairs(active or {}) do
        active_count = active_count + 1
        local id = tostring(activation.dataflow_id)
        active_ids[id] = true
        local ok, reconcile_err = M.reconcile_activation(runtime, activation)
        if not ok then log_flow("active activation reconciliation failed", id, reconcile_err) end
    end

    local inactive_ids = {}
    for dataflow_id in pairs(runtime.known) do
        if not active_ids[dataflow_id] then table.insert(inactive_ids, dataflow_id) end
    end
    for _, dataflow_id in ipairs(inactive_ids) do
        local owner = M.overseer_state.owner_for_dataflow(runtime.ownership, dataflow_id)
        if owner then
            local activation, activation_err = M.activation_repo.get(dataflow_id)
            local workflow, workflow_err = M.dataflow_repo.get(dataflow_id)
            if activation_err or workflow_err then
                log_flow("inactive activation reconciliation failed", dataflow_id,
                    activation_err or workflow_err)
            else
                local snapshot = activation or {
                    dataflow_id = dataflow_id,
                    generation = owner.generation,
                    desired_active = false,
                }
                local ok, reconcile_err = M.reconcile_activation(runtime, snapshot, workflow)
                if not ok then log_flow("inactive state application failed", dataflow_id, reconcile_err) end
            end
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
    local activation, activation_err = M.activation_repo.get(payload.dataflow_id)
    if activation_err then return nil, tostring(activation_err) end
    if not activation then return nil, "activation row is missing" end
    local workflow, workflow_err = M.dataflow_repo.get(payload.dataflow_id)
    if workflow_err then return nil, tostring(workflow_err) end
    return M.reconcile_activation(runtime, activation, workflow)
end

function M.safety_reconcile(runtime: Runtime): (number?, string?)
    local _, due_err = M.promote_due(runtime)
    if due_err then return nil, due_err end
    return M.reconcile_all(runtime)
end

function M.handle_exit(runtime: Runtime, event: any): (boolean?, string?)
    local pid = event and event.from and tostring(event.from) or nil
    local owner = pid and M.overseer_state.owner_for_pid(runtime.ownership, pid) or nil
    if not owner or not pid then return true, nil end

    local activation, activation_err = M.activation_repo.get(owner.dataflow_id)
    local workflow, workflow_err = M.dataflow_repo.get(owner.dataflow_id)
    if activation_err or workflow_err then return nil, tostring(activation_err or workflow_err) end

    local generation = activation and tonumber(activation.generation) or nil
    if generation and generation ~= owner.generation then
        local next_state, next_decision, transition_err = M.overseer_state.on_exit(
            runtime.ownership, {
            pid = pid,
            generation = owner.generation,
            desired_active = false,
            status = workflow and tostring(workflow.status) or nil,
        })
        local _, remove_err = apply_transition(
            runtime, next_state, next_decision, transition_err)
        if remove_err then return nil, remove_err end
        return M.reconcile_activation(runtime, activation, workflow)
    end

    local desired_active = activation ~= nil and activation.desired_active == true and
        workflow ~= nil and not is_terminal(workflow.status)
    local next_state, next_decision, transition_err = M.overseer_state.on_exit(
        runtime.ownership, {
        pid = pid,
        generation = owner.generation,
        desired_active = desired_active,
        status = workflow and tostring(workflow.status) or nil,
        message = failure_message(event),
    })
    local decision, state_err = apply_transition(
        runtime, next_state, next_decision, transition_err)
    if state_err then return nil, state_err end
    return M.drive_decision(runtime, decision)
end

function M.next_pending_wake(): (any?, string?)
    local db, db_err = M.sql.get(M.consts.APP_DB)
    if db_err then return nil, tostring(db_err) end
    local rows, query_err = db:query([[
        SELECT dataflow_id, wake_key, wake_at FROM dataflow_wakes
        WHERE activation_generation IS NULL
        ORDER BY wake_at ASC, dataflow_id ASC, wake_key ASC LIMIT 1
    ]])
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

function M.run(_args: any)
    local registered, register_err = M.process.registry.register(NAME)
    if not registered then error("overseer registration failed: " .. tostring(register_err)) end

    local runtime = M.new_runtime()
    reconcile_or_log(runtime, M.bootstrap)
    local inbox = M.process.inbox()
    local events = M.process.events()

    while true do
        local safety_timer = M.time.after(SAFETY_INTERVAL)
        local wake_timer = nil
        local cases = { inbox:case_receive(), events:case_receive(), safety_timer:case_receive() }

        local wake, wake_err = M.next_pending_wake()
        if not wake_err and wake then
            local wait_ns = select(1, duration_until(tostring(wake.wake_at)))
            if wait_ns ~= nil then
                wake_timer = M.time.after(wait_ns)
                table.insert(cases, wake_timer:case_receive())
            end
        elseif wake_err and not schema_not_ready(wake_err) then
            logger:warn("could not inspect nearest dataflow wake", { error = tostring(wake_err) })
        end

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
