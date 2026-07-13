local time = require("time")
local logger = require("logger"):named("dataflow.wakes")

local M = {
    wake_repo = require("wake_repo"),
    boot_reconcile = require("boot_reconcile"),
    client = require("client"),
    dataflow_repo = require("dataflow_repo"),
    process = process,
    channel = channel,
}

local NAME = "dataflow.wakes"
local TOPIC = "dataflow.wake.changed"
local ORCHESTRATOR_STARTED_TOPIC = "dataflow.orchestrator.started"
local FALLBACK_REQUERY_NS = 60 * 1000000000

local function ensure_monitored(dataflow_id, candidate_pid, monitored)
    local pid = candidate_pid
    for _ = 1, 2 do
        if pid and monitored[tostring(pid)] == tostring(dataflow_id) then return true end
        if pid then
            local ok = M.process.monitor(tostring(pid))
            if ok then
                monitored[tostring(pid)] = tostring(dataflow_id)
                return true
            end
        end
        pid = M.process.registry.lookup("dataflow." .. tostring(dataflow_id))
        if not pid then
            local row, row_err = M.dataflow_repo.get(dataflow_id)
            if row_err then return nil, row_err end
            local status = row and row.status
            if status ~= "pending" and status ~= "running" then return true end
            local c, client_err = M.client.new()
            if client_err then return nil, client_err end
            local revived, revive_err = c:revive(dataflow_id)
            if revive_err then return nil, revive_err end
            pid = revived
        end
    end
    return nil, "orchestrator could not be monitored"
end

local function handle_exit(monitored, event)
    local exited_pid = tostring(event.from)
    local dataflow_id = monitored[exited_pid]
    monitored[exited_pid] = nil
    if not dataflow_id then return true end

    local row, row_err = M.dataflow_repo.get(dataflow_id)
    if row_err then return nil, "supervision status read failed: " .. tostring(row_err) end
    local status = row and row.status
    if status ~= "pending" and status ~= "running" then return true end

    local c, client_err = M.client.new()
    if client_err then return nil, "supervision client failed: " .. tostring(client_err) end
    local revived_pid, revive_err = c:revive(dataflow_id)
    if revive_err then return nil, "active dataflow supervision failed: " .. tostring(revive_err) end
    local ok, monitor_err = ensure_monitored(dataflow_id, revived_pid, monitored)
    if not ok then return nil, "revived dataflow monitor failed: " .. tostring(monitor_err) end
    return true
end

function M.notify()
    M.process.send(NAME, TOPIC, {})
end

local function duration_until(value)
    if type(value) ~= "string" or value == "" then return nil end
    local deadline, err = time.parse(time.RFC3339NANO, value)
    if err then deadline, err = time.parse(time.RFC3339, value) end
    if err then return nil end
    local now = time.now()
    if now:after(deadline) or now:equal(deadline) then return 0 end
    return deadline:sub(now):nanoseconds()
end

function M.run_due()
    local rows, due_err = M.wake_repo.due(time.now():format(time.RFC3339NANO), 100)
    if due_err then return nil, due_err end
    if #rows == 0 then return 0, nil end
    local c, client_err = M.client.new()
    if client_err then return nil, client_err end
    local revived = 0
    for _, row in ipairs(rows) do
        local live = M.process.registry.lookup("dataflow." .. row.dataflow_id)
        local revive_err = nil
        if live then
            local sent = M.process.send("dataflow." .. row.dataflow_id, "dataflow.wake", {
                wake_key = tostring(row.wake_key),
                wake_at = tostring(row.wake_at),
            })
            if not sent then revive_err = "failed to notify live orchestrator" end
        else
            local _, err = c:revive(row.dataflow_id)
            revive_err = err
        end
        if not revive_err then
            -- The revived orchestrator clears or advances the wake only after
            -- it has applied the canonical SIGNAL_TIMEOUT transition. Keeping
            -- the row until then makes a crash between spawn and commit safe.
            revived = revived + 1
        else
            logger:warn("due dataflow wake failed", { dataflow_id = row.dataflow_id, error = tostring(revive_err) })
        end
    end
    return revived, nil
end

function M.run(_args)
    M.process.registry.register(NAME)
    local inbox = M.process.inbox()
    local events = M.process.events()
    local monitored = {}
    local _, boot_err = M.boot_reconcile.reconcile_once(function(dataflow_id, pid)
        local ok, monitor_err = ensure_monitored(dataflow_id, pid, monitored)
        if not ok then error("boot monitor failed: " .. tostring(monitor_err)) end
    end)
    if boot_err then error("boot recovery failed: " .. tostring(boot_err)) end
    while true do
        local revived, due_err = M.run_due()
        if due_err then error("due wake query failed: " .. tostring(due_err)) end

        local next_row, next_err = M.wake_repo.next()
        if next_err then error("next wake query failed: " .. tostring(next_err)) end
        local wait_ns = next_row and duration_until(tostring(next_row.wake_at)) or nil
        if (tonumber(revived) or 0) > 0 and (wait_ns == nil or wait_ns < 1000000000) then
            -- A due row remains until its orchestrator durably advances it.
            -- Avoid a hot loop while preserving targeted retry if that process dies.
            wait_ns = 1000000000
        end
        if wait_ns == nil or wait_ns > FALLBACK_REQUERY_NS then wait_ns = FALLBACK_REQUERY_NS end
        local cases = { inbox:case_receive(), events:case_receive() }
        local timer = wait_ns and time.after(wait_ns) or nil
        if timer then table.insert(cases, timer:case_receive()) end
        local result = M.channel.select(cases)
        if not result.ok then break end
        if result.channel == inbox then
            local message = result.value
            if message:topic() == ORCHESTRATOR_STARTED_TOPIC then
                local payload = message:payload():data()
                if type(payload) == "table" and payload.pid and payload.dataflow_id then
                    local ok, monitor_err = ensure_monitored(payload.dataflow_id, payload.pid, monitored)
                    if not ok then error("orchestrator monitor failed: " .. tostring(monitor_err)) end
                end
            end
        elseif result.channel == events then
            if result.value.kind == M.process.event.CANCEL then break end
            if result.value.kind == M.process.event.EXIT then
                local handled, handle_err = handle_exit(monitored, result.value)
                if not handled then error(handle_err) end
            end
        end
        -- Inbox notifications and the nearest timer both re-evaluate the indexed head.
    end
    return { status = "shutdown" }
end

M.NAME = NAME
M.TOPIC = TOPIC
M.ORCHESTRATOR_STARTED_TOPIC = ORCHESTRATOR_STARTED_TOPIC
M.duration_until = duration_until
M.ensure_monitored = ensure_monitored
M.handle_exit = handle_exit
return M
