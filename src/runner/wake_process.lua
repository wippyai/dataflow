local time = require("time")
local logger = require("logger"):named("dataflow.overseer")

local M = {
    activation_repo = require("activation_repo"),
    dataflow_repo = require("dataflow_repo"),
    overseer_state = require("overseer_state"),
    consts = require("consts"),
    sql = require("sql"),
    process = process,
    channel = channel,
    time = time,
}

local NAME = "dataflow.overseer"
local COMPAT_NAME = "dataflow.wakes"
local TOPIC = "dataflow.activation.changed"
local COMPAT_TOPIC = "dataflow.wake.changed"
local SAFETY_INTERVAL = "30s"
local SCAN_LIMIT = 100

local TERMINAL_STATUS = {
    completed = true,
    failed = true,
    cancelled = true,
    terminated = true,
}

local function schema_not_ready(err)
    local message = string.lower(tostring(err or ""))
    local missing = message:find("no such table", 1, true) ~= nil or
        message:find("no such column", 1, true) ~= nil or
        message:find("does not exist", 1, true) ~= nil
    return missing and (message:find("dataflow_wakes", 1, true) ~= nil or
        message:find("dataflow_activations", 1, true) ~= nil or
        message:find("activation_generation", 1, true) ~= nil or
        message:find("dataflows", 1, true) ~= nil)
end

local function duration_until(value)
    if type(value) ~= "string" or value == "" then return nil, "deadline is missing" end
    local deadline, err = M.time.parse(M.time.RFC3339NANO, value)
    if err then deadline, err = M.time.parse(M.time.RFC3339, value) end
    if err then return nil, "invalid deadline: " .. tostring(value) end
    local now = M.time.now()
    if now:after(deadline) or now:equal(deadline) then return 0, nil end
    return deadline:sub(now):nanoseconds(), nil
end

local function now_value()
    return M.time.now():format(M.time.RFC3339NANO)
end

local function with_tx(fn)
    local db, db_err = M.sql.get(M.consts.APP_DB)
    if db_err then return nil, db_err end
    local tx, begin_err = db:begin()
    if begin_err then
        db:release()
        return nil, begin_err
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
        return nil, commit_err or "transaction did not commit"
    end
    db:release()
    return result, nil
end

M.with_tx = with_tx

local function is_terminal(status)
    return TERMINAL_STATUS[string.lower(tostring(status or ""))] == true
end

local function has_frozen_context(value)
    if type(value) == "string" then return value:match("%S") ~= nil end
    if type(value) == "table" then return next(value) ~= nil end
    return false
end

local function log_flow(_level: string, message: string, dataflow_id: any, err: any)
    logger:warn(message, {
        dataflow_id = tostring(dataflow_id or ""),
        error = tostring(err or ""),
    })
end

local function lookup_owner(dataflow_id)
    local ok, pid, lookup_err = pcall(M.process.registry.lookup, "dataflow." .. tostring(dataflow_id))
    if not ok then return nil, tostring(pid) end
    if pid == nil and lookup_err ~= nil then
        local kind_ok, kind = pcall(function() return lookup_err:kind() end)
        if kind_ok and tostring(kind) == "NotFound" then return nil, nil end
        return nil, tostring(lookup_err)
    end
    return pid, nil
end

local function transition(runtime: any, operation: any, input: any)
    local next_state, decision, err = operation(runtime.ownership, input)
    if err then return nil, err end
    runtime.ownership = next_state
    return decision, nil
end

local function schedule_failed_observation(runtime: any, decision: any, reason: string)
    log_flow("warn", reason, decision.dataflow_id, reason)
    return transition(runtime, M.overseer_state.on_spawn_observation, {
        dataflow_id = decision.dataflow_id,
        generation = decision.generation,
    })
end

local function clone_launch_args(value)
    local result = {}
    for key, item in pairs(type(value) == "table" and value or {}) do result[key] = item end
    return result
end

function M.new_runtime(options: any?)
    return {
        ownership = M.overseer_state.new(options or {}),
        retries = {},
        nudges = {},
        known = {},
        boot_recovered = false,
    }
end

local function deliver_nudge(runtime: any, dataflow_id: string, pid: any)
    local nudge = runtime.nudges[dataflow_id]
    if not nudge or not pid then return true, nil end
    local ok, sent, send_err = pcall(M.process.send, pid, M.consts.MESSAGE_TOPIC.WAKE, nudge)
    if not ok or not sent then return nil, not ok and tostring(sent) or send_err end
    runtime.nudges[dataflow_id] = nil
    return true, nil
end

function M.drive_decision(runtime: any, initial: any)
    local decision: any = initial
    for _ = 1, 8 do
        if not decision or decision.kind == M.overseer_state.ACTION.NONE then
            if decision and decision.reason == "owner_monitored" then
                local owner = decision.pid and M.overseer_state.owner_for_pid(runtime.ownership, decision.pid) or nil
                if owner then
                    runtime.retries[owner.dataflow_id] = nil
                    local delivered, delivery_err = deliver_nudge(runtime, tostring(owner.dataflow_id), decision.pid)
                    if not delivered then
                        log_flow("warn", "owner nudge delivery failed", owner.dataflow_id, delivery_err)
                    end
                end
            end
            return true, nil
        end

        local dataflow_id = tostring(decision.dataflow_id)
        local generation = tonumber(decision.generation)
        if decision.kind == M.overseer_state.ACTION.RESTART then
            runtime.retries[dataflow_id] = {
                generation = generation,
                due_at = M.time.now():add((tonumber(decision.delay_ms) or 1) * M.time.MILLISECOND)
                    :format(M.time.RFC3339NANO),
            }
            return true, nil
        end

        if decision.kind == M.overseer_state.ACTION.INSPECT_OWNER then
            local pid, lookup_err = lookup_owner(dataflow_id)
            if lookup_err then
                decision = select(1, schedule_failed_observation(runtime, decision,
                    "canonical owner lookup failed: " .. tostring(lookup_err)))
            else
                decision = select(1, transition(runtime, M.overseer_state.on_owner_observation, {
                    dataflow_id = dataflow_id,
                    generation = generation,
                    registered_pid = pid,
                }))
            end
        elseif decision.kind == M.overseer_state.ACTION.MONITOR then
            local ok, monitored, monitor_err = pcall(M.process.monitor, decision.pid)
            local registered_pid = nil
            if not ok or not monitored then registered_pid = select(1, lookup_owner(dataflow_id)) end
            decision = select(1, transition(runtime, M.overseer_state.on_monitor_observation, {
                dataflow_id = dataflow_id,
                generation = generation,
                pid = decision.pid,
                monitor_ok = ok and monitored == true,
                registered_pid = registered_pid,
            }))
            if not ok or not monitored then
                log_flow("warn", "could not monitor canonical owner", dataflow_id, monitor_err or monitored)
            end
        elseif decision.kind == M.overseer_state.ACTION.SPAWN then
            local activation, activation_err = M.activation_repo.get(dataflow_id)
            local workflow, workflow_err = M.dataflow_repo.get(dataflow_id)
            if activation_err or workflow_err or not activation or not workflow then
                decision = select(1, schedule_failed_observation(runtime, decision,
                    "durable spawn state unavailable: " .. tostring(activation_err or workflow_err or "missing row")))
            elseif activation.desired_active ~= true or tonumber(activation.generation) ~= generation or
                is_terminal(workflow.status) then
                decision = select(1, transition(runtime, M.overseer_state.on_activation, {
                    dataflow_id = dataflow_id,
                    generation = tonumber(activation.generation) or generation,
                    desired_active = activation.desired_active == true,
                    status = workflow.status,
                }))
            else
                local args = clone_launch_args(activation.launch_args)
                args.dataflow_id = dataflow_id
                args.activation_generation = generation
                local spawn_ok, spawn_pid, spawn_err = pcall(function()
                    return M.process.with_context({})
                        :with_name("dataflow." .. dataflow_id)
                        :spawn_monitored(M.consts.ORCHESTRATOR, M.consts.HOST_ID, args)
                end)
                if not spawn_ok then
                    spawn_err = tostring(spawn_pid)
                    spawn_pid = nil
                end
                local registered_pid = select(1, lookup_owner(dataflow_id))
                decision = select(1, transition(runtime, M.overseer_state.on_spawn_observation, {
                    dataflow_id = dataflow_id,
                    generation = generation,
                    spawn_pid = spawn_pid,
                    registered_pid = registered_pid,
                    -- A named spawn may return an existing owner. The host's
                    -- spawn-or-signal shortcut does not install our monitor,
                    -- so every returned/canonical PID is observed explicitly.
                    monitor_ok = false,
                }))
                if not spawn_pid then
                    log_flow("warn", "orchestrator spawn failed", dataflow_id, spawn_err)
                end
            end
        else
            return nil, "unknown overseer decision " .. tostring(decision.kind)
        end
    end
    return nil, "overseer decision chain exceeded safety bound"
end

function M.reconcile_activation(runtime: any, activation: any, workflow: any?, nudge: any?)
    if type(activation) ~= "table" then return nil, "activation is required" end
    local dataflow_id = tostring(activation.dataflow_id or "")
    local generation = tonumber(activation.generation)
    if dataflow_id == "" or not generation then return nil, "activation identity is invalid" end
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
    local decision, state_err = transition(runtime, M.overseer_state.on_activation, {
        dataflow_id = dataflow_id,
        generation = generation,
        desired_active = activation.desired_active == true,
        status = workflow and workflow.status or nil,
    })
    if state_err then return nil, state_err end
    if decision and decision.kind == M.overseer_state.ACTION.NONE and current and current.pid then
        local delivered, delivery_err = deliver_nudge(runtime, dataflow_id, current.pid)
        if not delivered then log_flow("warn", "owner nudge delivery failed", dataflow_id, delivery_err) end
    end
    return M.drive_decision(runtime, decision)
end

function M.recover_legacy_running(runtime)
    -- This compatibility scan runs once. It must not be capped: pending rows
    -- sort in the same legacy result set and could otherwise starve RUNNING
    -- rows beyond the cap forever.
    local rows, list_err = M.dataflow_repo.list_non_terminal()
    if list_err then return nil, list_err end
    local recovered = 0
    for _, workflow in ipairs(rows or {}) do
        if workflow.status == M.consts.STATUS.RUNNING and has_frozen_context(workflow.actor_context) then
            local activation, activation_err = M.with_tx(function(tx)
                return M.activation_repo.ensure_running_recovery_tx(
                    tx, tostring(workflow.dataflow_id), now_value())
            end)
            if activation_err then
                if schema_not_ready(activation_err) then return nil, activation_err end
                log_flow("warn", "legacy running recovery failed", workflow.dataflow_id, activation_err)
            elseif activation and activation.recovered then
                recovered = recovered + 1
            end
        end
    end
    runtime.boot_recovered = true
    return recovered, nil
end

local function pending_due(now, limit)
    local db, db_err = M.sql.get(M.consts.APP_DB)
    if db_err then return nil, db_err end
    local db_type, type_err = db:type()
    if type_err then
        db:release()
        return nil, type_err
    end
    local placeholder = "?"
    if db_type == M.sql.type.POSTGRES or db_type == "postgres" then placeholder = "$1" end
    local query = [[
        SELECT dataflow_id, wake_key, wake_at FROM dataflow_wakes
        WHERE activation_generation IS NULL AND wake_at <= ]] .. placeholder .. [[
        ORDER BY wake_at ASC, dataflow_id ASC, wake_key ASC LIMIT ]] .. tostring(limit)
    local rows, query_err = db:query(query, { now })
    db:release()
    if query_err then return nil, query_err end
    return rows or {}, nil
end

M.pending_due = pending_due

function M.promote_due(runtime)
    local now = now_value()
    -- Filter promoted rows before LIMIT. Otherwise the first batch of durable
    -- (but not yet consumed) wakes can permanently starve every later wake.
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
            log_flow("warn", "due wake promotion failed", row.dataflow_id, activation_err)
        elseif activation and activation.promoted then
            promoted = promoted + 1
            local ok, reconcile_err = M.reconcile_activation(runtime, activation, nil, {
                dataflow_id = tostring(row.dataflow_id),
                generation = tonumber(activation.generation),
                wake_key = tostring(row.wake_key),
                wake_at = row.wake_at and tostring(row.wake_at) or nil,
            })
            if not ok then log_flow("warn", "promoted activation reconcile failed", row.dataflow_id, reconcile_err) end
        end
    end
    return promoted, nil
end

function M.reconcile_all(runtime)
    local active, list_err = M.activation_repo.list_active()
    if list_err then return nil, list_err end
    local active_ids = {}
    for _, activation in ipairs(active or {}) do
        active_ids[tostring(activation.dataflow_id)] = true
        local ok, reconcile_err = M.reconcile_activation(runtime, activation)
        if not ok then
            log_flow("warn", "active activation reconcile failed", activation.dataflow_id, reconcile_err)
        end
    end
    for dataflow_id in pairs(runtime.known) do
        if not active_ids[dataflow_id] then
            local owner = M.overseer_state.owner_for_dataflow(runtime.ownership, tostring(dataflow_id))
            if owner then
                local activation, activation_err = M.activation_repo.get(dataflow_id)
                local workflow, workflow_err = M.dataflow_repo.get(dataflow_id)
                if activation_err or workflow_err then
                    log_flow("warn", "inactive activation reconcile failed", dataflow_id,
                        activation_err or workflow_err)
                else
                    local snapshot = activation or {
                        dataflow_id = dataflow_id,
                        generation = owner.generation,
                        desired_active = false,
                    }
                    if workflow and is_terminal(workflow.status) then snapshot.desired_active = false end
                    local ok, reconcile_err = M.reconcile_activation(runtime, snapshot, workflow)
                    if not ok then log_flow("warn", "inactive state apply failed", dataflow_id, reconcile_err) end
                end
            end
        end
    end
    return #active, nil
end

function M.bootstrap(runtime)
    if not runtime.boot_recovered then
        local _, recovery_err = M.recover_legacy_running(runtime)
        if recovery_err then return nil, recovery_err end
    end
    local _, due_err = M.promote_due(runtime)
    if due_err then return nil, due_err end
    return M.reconcile_all(runtime)
end

function M.handle_activation_hint(runtime, payload)
    if type(payload) ~= "table" or type(payload.dataflow_id) ~= "string" or
        payload.dataflow_id == "" or not tonumber(payload.generation) then
        return nil, "activation hint identity is invalid"
    end
    local activation, activation_err = M.activation_repo.get(payload.dataflow_id)
    if activation_err then return nil, activation_err end
    if not activation then return nil, "activation row is missing" end
    -- The decision driver reloads workflow identity immediately before spawn.
    -- Keeping that failure inside the ownership machine gives it bounded
    -- per-flow backoff instead of turning a targeted hint into a service error.
    return M.reconcile_activation(runtime, activation)
end

function M.handle_notification(runtime, payload)
    if type(payload) == "table" and type(payload.dataflow_id) == "string" and
        payload.dataflow_id ~= "" and tonumber(payload.generation) then
        return M.handle_activation_hint(runtime, payload)
    end
    -- Legacy wake notifications carry no identity. The durable wake index is
    -- already sufficient to promote only the due rows and reset the timer.
    return M.promote_due(runtime)
end

function M.safety_reconcile(runtime)
    local _, due_err = M.promote_due(runtime)
    if due_err then return nil, due_err end
    return M.reconcile_all(runtime)
end

function M.handle_exit(runtime, event)
    local pid = event and event.from
    local owner = pid and M.overseer_state.owner_for_pid(runtime.ownership, pid) or nil
    if not owner then return true, nil end
    local activation, activation_err = M.activation_repo.get(owner.dataflow_id)
    local workflow, workflow_err = M.dataflow_repo.get(owner.dataflow_id)
    if activation_err then log_flow("warn", "activation unavailable during EXIT", owner.dataflow_id, activation_err) end
    if workflow_err then log_flow("warn", "workflow unavailable during EXIT", owner.dataflow_id, workflow_err) end
    local desired_active = true
    if activation and activation.desired_active == false then desired_active = false end
    if workflow and is_terminal(workflow.status) then desired_active = false end
    local clean = event.kind == M.process.event.EXIT and
        (not event.result or not event.result.error) and
        not (event.result and type(event.result.value) == "table" and event.result.value.success == false)
    local decision, state_err = transition(runtime, M.overseer_state.on_exit, {
        pid = pid,
        generation = owner.generation,
        desired_active = desired_active,
        status = workflow and workflow.status or nil,
        clean = clean,
    })
    if state_err then return nil, state_err end
    local ok, drive_err = M.drive_decision(runtime, decision)
    if not ok then return nil, drive_err end
    if activation and tonumber(activation.generation) ~= tonumber(owner.generation) then
        return M.reconcile_activation(runtime, activation, workflow)
    end
    return true, nil
end

function M.run_due_retries(runtime)
    local due = {}
    for dataflow_id, retry in pairs(runtime.retries) do
        local wait_ns, wait_err = duration_until(tostring(retry.due_at))
        if wait_err then
            runtime.retries[dataflow_id] = nil
            log_flow("warn", "invalid retry deadline", dataflow_id, wait_err)
        elseif wait_ns == 0 then
            table.insert(due, { dataflow_id = dataflow_id, generation = retry.generation })
        end
    end
    for _, retry in ipairs(due) do
        runtime.retries[retry.dataflow_id] = nil
        local decision, state_err = transition(runtime, M.overseer_state.on_restart_due, retry)
        if state_err then
            log_flow("warn", "restart transition failed", retry.dataflow_id, state_err)
        else
            local ok, drive_err = M.drive_decision(runtime, decision)
            if not ok then log_flow("warn", "restart attempt failed", retry.dataflow_id, drive_err) end
        end
    end
    return #due, nil
end

function M.next_retry_wait(runtime)
    local nearest = nil
    for _, retry in pairs(runtime.retries) do
        local wait_ns, wait_err = duration_until(tostring(retry.due_at))
        if not wait_err and (nearest == nil or wait_ns < nearest) then nearest = wait_ns end
    end
    return nearest
end

function M.next_pending_wake()
    local db, db_err = M.sql.get(M.consts.APP_DB)
    if db_err then return nil, db_err end
    local rows, query_err = db:query([[
        SELECT dataflow_id, wake_key, wake_at FROM dataflow_wakes
        WHERE activation_generation IS NULL
        ORDER BY wake_at ASC, dataflow_id ASC, wake_key ASC LIMIT 1
    ]])
    db:release()
    if query_err then return nil, query_err end
    return rows and rows[1] or nil, nil
end

function M.notify()
    -- Preserve the legacy name/topic used by existing wake writers. Both names
    -- resolve to this process and every notification is only a reconciliation hint.
    return M.process.send(COMPAT_NAME, COMPAT_TOPIC, {})
end

local function reconcile_or_log(runtime, operation)
    local ok, err = operation(runtime)
    if not ok and err then
        if schema_not_ready(err) then
            runtime.boot_recovered = false
            logger:debug("overseer waiting for dataflow migrations", { error = tostring(err) })
        else
            logger:warn("overseer reconciliation failed", { error = tostring(err) })
        end
    end
end

function M.run(_args)
    local registered, register_err = M.process.registry.register(NAME)
    if not registered then error("overseer registration failed: " .. tostring(register_err)) end
    local compatible, compat_err = M.process.registry.register(COMPAT_NAME)
    if not compatible then error("wake compatibility registration failed: " .. tostring(compat_err)) end

    local runtime = M.new_runtime()
    reconcile_or_log(runtime, M.bootstrap)
    local inbox = M.process.inbox()
    local events = M.process.events()

    while true do
        local safety_timer = M.time.after(SAFETY_INTERVAL)
        local wake_timer = nil
        local retry_timer = nil
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

        local retry_wait = M.next_retry_wait(runtime)
        if retry_wait ~= nil then
            retry_timer = M.time.after(retry_wait)
            table.insert(cases, retry_timer:case_receive())
        end

        local result = M.channel.select(cases)
        if not result.ok then break end
        if result.channel == events then
            local event = result.value
            if event.kind == M.process.event.CANCEL then break end
            if event.kind == M.process.event.EXIT or event.kind == M.process.event.LINK_DOWN then
                local ok, exit_err = M.handle_exit(runtime, event)
                if not ok then log_flow("warn", "EXIT reconciliation failed", event.from, exit_err) end
            end
        elseif result.channel == retry_timer then
            M.run_due_retries(runtime)
        elseif result.channel == wake_timer then
            reconcile_or_log(runtime, runtime.boot_recovered and M.promote_due or M.bootstrap)
        elseif result.channel == inbox then
            local message = result.value
            local topic = message and tostring(message:topic()) or ""
            local payload = message and message:payload():data() or nil
            if not runtime.boot_recovered then
                -- Migration readiness is delivered through the legacy wake
                -- alias. Once any durable hint arrives, retry the complete
                -- one-time boot boundary rather than waiting for safety scan.
                reconcile_or_log(runtime, M.bootstrap)
            elseif topic == TOPIC then
                local function targeted(current) return M.handle_activation_hint(current, payload) end
                reconcile_or_log(runtime, targeted)
            elseif topic == COMPAT_TOPIC then
                reconcile_or_log(runtime, M.promote_due)
            end
        else
            reconcile_or_log(runtime, runtime.boot_recovered and M.safety_reconcile or M.bootstrap)
        end
    end
    return { status = "shutdown" }
end

M.NAME = NAME
M.COMPAT_NAME = COMPAT_NAME
M.TOPIC = TOPIC
M.COMPAT_TOPIC = COMPAT_TOPIC
M.duration_until = duration_until
M.schema_not_ready = schema_not_ready
return M
