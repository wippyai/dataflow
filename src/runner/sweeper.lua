local time = require("time")
local env = require("env")
local logger = require("logger"):named("dataflow.sweeper")

-- Revival sweeper: a supervised periodic service that respawns orchestrators for
-- dataflows left in a non-terminal state (pending/running) with no live
-- `dataflow.{id}` process. This covers crash-during-park and any commit whose
-- process.send was dropped because the orchestrator was offline. The criterion is
-- dead-process + non-terminal — NOT merely a pending commit, because a parked run
-- whose yield commit already applied has no pending commits yet is still dormant.
local sweeper = {
    process = process,
    dataflow_repo = require("dataflow_repo"),
    client = require("client"),
    env = env
}

local INTERVAL_ENV = "userspace.dataflow.env:revival_interval_seconds"
local DEFAULT_INTERVAL_SECONDS = 30
local MIN_INTERVAL_SECONDS = 5
local MAX_INTERVAL_SECONDS = 3600
local MAX_SCAN = 500

---Resolve the sweep interval from env, clamped to a sane range.
---@return number seconds
function sweeper.resolve_interval_seconds(): number
    local raw: any = nil
    if sweeper.env then
        raw = sweeper.env.get(INTERVAL_ENV)
    end

    local seconds = tonumber(raw) or DEFAULT_INTERVAL_SECONDS
    seconds = math.floor(seconds)

    if seconds < MIN_INTERVAL_SECONDS then
        seconds = MIN_INTERVAL_SECONDS
    elseif seconds > MAX_INTERVAL_SECONDS then
        seconds = MAX_INTERVAL_SECONDS
    end

    return seconds
end

---Scan non-terminal dataflows and respawn any whose orchestrator is dead.
---Reviving a run that is actually terminal is safe: the orchestrator's
---terminal-status guard bails on startup. A concurrent double-spawn is safe too:
---the registry single-instance guard makes the loser exit gracefully.
---@return number revived Count of orchestrators respawned this sweep
function sweeper.sweep_once(): number
    local active, err = sweeper.dataflow_repo.list_non_terminal(MAX_SCAN)
    if err then
        logger:warn("revival sweep: failed to list dataflows", { error = tostring(err) })
        return 0
    end

    if not active or #active == 0 then
        return 0
    end

    local workflow_client, client_err = sweeper.client.new()
    if not workflow_client then
        logger:warn("revival sweep: failed to build client", { error = tostring(client_err) })
        return 0
    end

    local revived = 0
    for _, df in ipairs(active) do
        local dataflow_id = df.dataflow_id
        if type(dataflow_id) == "string" and dataflow_id ~= "" then
            local pid = sweeper.process.registry.lookup("dataflow." .. dataflow_id)
            if not pid then
                local _, revive_err = (workflow_client :: any):revive(dataflow_id)
                if revive_err then
                    logger:warn("revival sweep: failed to respawn orchestrator", {
                        dataflow_id = dataflow_id,
                        error = tostring(revive_err)
                    })
                else
                    revived = revived + 1
                end
            end
        end
    end

    if revived > 0 then
        logger:info("revival sweep respawned dead orchestrators", { count = revived })
    end

    return revived
end

---Supervised service loop: sweep on every tick until cancelled.
---@param _args table Process arguments (unused)
---@return table result
local function run(_args: any): any
    local interval_seconds = sweeper.resolve_interval_seconds()
    logger:info("revival sweeper starting", { interval_seconds = interval_seconds })

    local ticker = time.ticker(tostring(interval_seconds) .. "s")
    local inbox = sweeper.process.inbox()
    local events = sweeper.process.events()

    while true do
        local result = channel.select({
            inbox:case_receive(),
            events:case_receive(),
            ticker:channel():case_receive()
        })

        if not result.ok then
            break
        end

        if result.channel == events then
            local event = result.value
            if event.kind == sweeper.process.event.CANCEL then
                break
            end
        elseif result.channel == ticker:channel() then
            sweeper.sweep_once()
        end
        -- inbox messages are drained and ignored; the sweeper is timer-driven
    end

    ticker:stop()
    logger:info("revival sweeper shutting down")
    return { status = "shutdown" }
end

sweeper.run = run
return sweeper
