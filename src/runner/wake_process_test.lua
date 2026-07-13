local test = require("test")
local wake_process = require("wake_process")

local function run_tests()
    test.describe("Targeted dataflow wake lifecycle", function()
        test.it("revives only due wake rows and leaves durable advancement to the orchestrator", function()
            local revived = {}
            wake_process.wake_repo = {
                due = function(_now: string, limit: number)
                    test.eq(limit, 100)
                    return { {
                        dataflow_id = "df-due",
                        wake_key = "yield:one",
                        wake_at = "2026-07-12T20:00:00Z",
                    } }, nil
                end,
            }
            wake_process.process = {
                registry = { lookup = function() return nil end },
                monitor = function() return true, nil end,
            }
            wake_process.client = {
                new = function()
                    return {
                        revive = function(_self: any, id: string)
                            table.insert(revived, id)
                            return "pid", nil
                        end,
                    }, nil
                end,
            }

            local count, err = wake_process.run_due()
            test.is_nil(err)
            test.eq(count, 1)
            test.eq(revived[1], "df-due")
        end)

        test.it("monitors the exact revived process until durable wake consumption", function()
            local monitored = {}
            wake_process.wake_repo = {
                due = function()
                    return { { dataflow_id = "df-exact", wake_key = "yield:one", wake_at = "2026-07-12T20:00:00Z" } }, nil
                end,
            }
            wake_process.process = {
                registry = { lookup = function() return nil end },
                monitor = function(pid)
                    table.insert(monitored, pid)
                    return true, nil
                end,
            }
            wake_process.client = {
                new = function()
                    return { revive = function() return "pid-exact", nil end }, nil
                end,
            }

            local count, err, deliveries = wake_process.run_due({})
            test.is_nil(err)
            test.eq(count, 1)
            test.eq(monitored[1], "pid-exact")
            test.eq(deliveries["pid-exact"], "df-exact")
        end)

        test.it("fails the service cycle when exact revival fails", function()
            wake_process.wake_repo = {
                due = function()
                    return { { dataflow_id = "df-broken", wake_key = "yield:one", wake_at = "2026-07-12T20:00:00Z" } }, nil
                end,
            }
            wake_process.process = {
                registry = { lookup = function() return nil end },
                monitor = function() return true, nil end,
            }
            wake_process.client = {
                new = function()
                    return { revive = function() return nil, "spawn denied" end }, nil
                end,
            }

            local count, err = wake_process.run_due()
            test.is_nil(count)
            test.contains(err, "df-broken")
            test.contains(err, "spawn denied")
        end)

        test.it("fails immediately when an exact delivery cannot be monitored", function()
            wake_process.wake_repo = {
                due = function()
                    return { { dataflow_id = "df-gone", wake_key = "yield:one", wake_at = "2026-07-12T20:00:00Z" } }, nil
                end,
            }
            wake_process.process = {
                registry = { lookup = function() return nil end },
                monitor = function() return nil, "pid not registered" end,
            }
            wake_process.client = {
                new = function()
                    return { revive = function() return "pid-gone", nil end }, nil
                end,
            }

            local count, err = wake_process.run_due({})
            test.is_nil(count)
            test.contains(err, "df-gone")
            test.contains(err, "pid not registered")
        end)

        test.it("rejects malformed durable deadlines instead of polling", function()
            local wait_ns, err = wake_process.duration_until("not-a-deadline")
            test.is_nil(wait_ns)
            test.contains(err, "invalid wake deadline")
        end)

        test.it("treats a not-yet-migrated wake table as startup readiness", function()
            test.is_true(wake_process.schema_not_ready("no such table: dataflow_wakes"))
            test.is_true(wake_process.schema_not_ready('relation "dataflow_wakes" does not exist'))
            test.is_false(wake_process.schema_not_ready("database connection lost"))
        end)
    end)
end

return { run_tests = run_tests }
