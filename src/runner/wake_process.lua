local time = require("time")
local logger = require("logger"):named("dataflow.wakes")

local M = {
    wake_repo = require("wake_repo"),
    client = require("client"),
    process = process,
    channel = channel,
}

local NAME = "dataflow.wakes"
local TOPIC = "dataflow.wake.changed"

-- The service and the migration bootloader both depend on the same database
-- service, so a brand-new database can be reachable a moment before migration
-- 07 creates the queue. That state is readiness, not a service failure. The
-- actor waits for the post-migration bootloader notification. Existing
-- databases still query immediately on restart.
local function schema_not_ready(err)
    local message = string.lower(tostring(err or ""))
    return message:find("no such table: dataflow_wakes", 1, true) ~= nil or
        (message:find("dataflow_wakes", 1, true) ~= nil and
            message:find("does not exist", 1, true) ~= nil)
end

function M.notify()
    -- The durable wake row is authoritative. This message only makes the
    -- service recalculate its exact timer after that row changes.
    M.process.send(NAME, TOPIC, {})
end

local function duration_until(value)
    if type(value) ~= "string" or value == "" then
        return nil, "wake deadline is missing"
    end
    local deadline, err = time.parse(time.RFC3339NANO, value)
    if err then deadline, err = time.parse(time.RFC3339, value) end
    if err then return nil, "invalid wake deadline: " .. tostring(value) end
    local now = time.now()
    if now:after(deadline) or now:equal(deadline) then return 0, nil end
    return deadline:sub(now):nanoseconds(), nil
end

local function monitor_delivery(pid, dataflow_id, monitored)
    local key = tostring(pid)
    if monitored[key] then return true, nil end
    local ok, monitor_err = M.process.monitor(pid)
    if not ok then
        return nil, "could not monitor exact wake delivery for " .. tostring(dataflow_id) ..
            ": " .. tostring(monitor_err)
    end
    monitored[key] = tostring(dataflow_id)
    return true, nil
end

local function clear_delivery_monitors(monitored)
    for pid in pairs(monitored) do
        if type(M.process.unmonitor) == "function" then M.process.unmonitor(pid) end
        monitored[pid] = nil
    end
end

function M.run_due(monitored)
    monitored = monitored or {}
    local rows, due_err = M.wake_repo.due(time.now():format(time.RFC3339NANO), 100)
    if due_err then return nil, due_err end
    if #rows == 0 then return 0, nil, monitored end

    local c, client_err = M.client.new()
    if client_err then return nil, client_err end
    for _, row in ipairs(rows) do
        local live = M.process.registry.lookup("dataflow." .. row.dataflow_id)
        local wake_err = nil
        if live then
            local monitored_ok, monitor_err = monitor_delivery(live, row.dataflow_id, monitored)
            if not monitored_ok then
                wake_err = monitor_err
            else
                local sent, send_err = M.process.send("dataflow." .. row.dataflow_id, "dataflow.wake", {
                    wake_key = tostring(row.wake_key),
                    wake_at = tostring(row.wake_at),
                })
                if not sent then wake_err = send_err or "failed to notify live orchestrator" end
            end
        else
            local revived_pid, revive_err = c:revive(row.dataflow_id)
            if revive_err then
                wake_err = revive_err
            else
                local monitored_ok, monitor_err = monitor_delivery(
                    tostring(revived_pid),
                    tostring(row.dataflow_id),
                    monitored
                )
                if not monitored_ok then wake_err = monitor_err end
            end
        end
        if wake_err then
            return nil, "due dataflow wake failed for " .. tostring(row.dataflow_id) .. ": " .. tostring(wake_err)
        end
    end
    return #rows, nil, monitored
end

function M.run(_args)
    local registered, register_err = M.process.registry.register(NAME)
    if not registered then error("wake service registration failed: " .. tostring(register_err)) end

    local inbox = M.process.inbox()
    local events = M.process.events()
    local monitored = {}
    while true do
        local revived, due_err = M.run_due(monitored)
        local ready = true
        if due_err then
            if schema_not_ready(due_err) then
                revived = 0
                ready = false
            else
                error("due wake query failed: " .. tostring(due_err))
            end
        end

        local cases = { inbox:case_receive(), events:case_receive() }
        if ready and revived == 0 then
            local next_row, next_err = M.wake_repo.next()
            if next_err then error("next wake query failed: " .. tostring(next_err)) end
            if next_row then
                local wait_ns, wait_err = duration_until(tostring(next_row.wake_at))
                if wait_err then error(wait_err) end
                table.insert(cases, time.after(wait_ns):case_receive())
            end
        end

        -- After a due revival, wait for the orchestrator's durable transition
        -- notification. With no due row, wait only for a row-change message or
        -- the exact indexed deadline. There is no periodic requery path.
        local result = M.channel.select(cases)
        if not result.ok then break end
        if result.channel == inbox then
            -- Wake consumption notifies only after the durable transition
            -- commits. Release exact-child monitors before recalculating the
            -- indexed head; a still-due row is monitored again immediately.
            clear_delivery_monitors(monitored)
        elseif result.channel == events then
            if result.value.kind == M.process.event.CANCEL then break end
            if result.value.kind == M.process.event.EXIT then
                monitored[tostring(result.value.from)] = nil
            end
        end
    end
    return { status = "shutdown" }
end

M.NAME = NAME
M.TOPIC = TOPIC
M.duration_until = duration_until
M.monitor_delivery = monitor_delivery
M.clear_delivery_monitors = clear_delivery_monitors
M.schema_not_ready = schema_not_ready
return M
