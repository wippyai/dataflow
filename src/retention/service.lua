local env = require("env")
local sql = require("sql")
local time = require("time")
local logger = require("logger"):named("dataflow.retention")
local sweeper = require("sweeper")

local M = {}

local function read(name)
    local value, err = env.get("userspace.dataflow.env:" .. name)
    if err then return nil, tostring(err) end
    return value
end

function M.run()
    local events = process.events()
    -- Wait for migrations on first startup; later failures retry on the tick.
    local delay = "30s"
    while true do
        local tick = time.after(delay)
        local selected = channel.select({ events:case_receive(), tick:case_receive() })
        if selected.channel == events then
            if selected.value.kind == process.event.CANCEL then return end
        else
            local days, days_err = read("diagnostic_retention_days")
            local flow_days, flow_days_err = read("flow_retention_days")
            local batch, batch_err = read("retention_batch_size")
            local interval, interval_err = read("retention_interval_seconds")
            local db_id, db_err = read("retention_db")
            local config, config_err = sweeper.validate({ days = days, flow_days = flow_days, batch_size = batch })
            local seconds = tonumber(interval)
            if days_err or flow_days_err or batch_err or interval_err or db_err or config_err or
                not seconds or seconds < 60 or seconds > 86400 or seconds % 1 ~= 0 then
                logger:error("Invalid dataflow retention configuration", {
                    error = days_err or flow_days_err or batch_err or interval_err or db_err or config_err or "interval must be 60..86400 seconds" })
                delay = "60s"
            else
                delay = tostring(seconds) .. "s"
                if config.days > 0 or config.flow_days > 0 then
                    local db, open_err = sql.get(tostring(db_id))
                    if open_err then
                        logger:error("Retention database unavailable", { error = tostring(open_err) })
                    else
                        local cancelled = false
                        local function yield_batch()
                            local ready = time.after("1ns")
                            local choice = channel.select({ events:case_receive(), ready:case_receive() })
                            if choice.channel == events and choice.value.kind == process.event.CANCEL then
                                cancelled = true
                                return false
                            end
                            return true
                        end
                        local stats, sweep_err = sweeper.run(db, config, yield_batch)
                        db:release()
                        if cancelled then return end
                        if sweep_err then
                            logger:error("Dataflow retention pass failed", { error = tostring(sweep_err) })
                        elseif stats.data + stats.commits + stats.flows + stats.nodes + stats.activations + stats.wakes > 0 then
                            logger:info("Dataflow retention pass completed", stats)
                        end
                    end
                end
            end
        end
    end
end

return M
