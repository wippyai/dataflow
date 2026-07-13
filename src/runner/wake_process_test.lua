local test = require("test")
local wake_process = require("wake_process")
local boot_reconcile = require("boot_reconcile")

local function run_tests()
    test.describe("Targeted dataflow wake lifecycle", function()
        test.it("queries only due wake rows and leaves deletion to the orchestrator", function()
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

        test.it("boot reconciliation scans once and revives only missing processes", function()
            local listed = 0
            local revived = {}
            boot_reconcile.dataflow_repo = {
                list_non_terminal = function()
                    listed = listed + 1
                    return { { dataflow_id = "live" }, { dataflow_id = "dead" } }, nil
                end,
            }
            boot_reconcile.process = {
                registry = { lookup = function(name: string) return name == "dataflow.live" and "pid-live" or nil end },
            }
            boot_reconcile.client = {
                new = function()
                    return {
                        revive = function(_self: any, id: string)
                            table.insert(revived, id)
                            return "pid-" .. id, nil
                        end,
                    }, nil
                end,
            }
            local live = {}
            local count, err = boot_reconcile.reconcile_once(function(id, pid)
                live[id] = pid
            end)
            test.is_nil(err)
            test.eq(count, 1)
            test.eq(listed, 1)
            test.eq(revived[1], "dead")
            test.eq(live.live, "pid-live")
        end)

        test.it("reattaches monitoring when a pid dies between lookup and monitor", function()
            local monitor_calls = 0
            local revived = 0
            local monitored = {}
            wake_process.process = {
                monitor = function(pid)
                    monitor_calls = monitor_calls + 1
                    if pid == "dead-pid" then return nil, "pid gone" end
                    return true, nil
                end,
                registry = { lookup = function() return nil end },
            }
            wake_process.dataflow_repo = { get = function() return { status = "running" }, nil end }
            wake_process.client = { new = function()
                return { revive = function()
                    revived = revived + 1
                    return "replacement-pid", nil
                end }, nil
            end }

            local ok, err = wake_process.ensure_monitored("df-race", "dead-pid", monitored)
            test.is_true(ok)
            test.is_nil(err)
            test.eq(revived, 1)
            test.eq(monitor_calls, 2)
            test.eq(monitored["replacement-pid"], "df-race")
        end)

        test.it("EXIT revives active runs but never parked runs", function()
            local status = "running"
            local revived = 0
            local monitored = { ["pid-active"] = "df-active", ["pid-waiting"] = "df-waiting" }
            wake_process.process = {
                monitor = function() return true, nil end,
                registry = { lookup = function() return nil end },
            }
            wake_process.dataflow_repo = { get = function()
                return { status = status }, nil
            end }
            wake_process.client = { new = function()
                return { revive = function()
                    revived = revived + 1
                    return "pid-revived", nil
                end }, nil
            end }

            test.is_true(select(1, wake_process.handle_exit(monitored, { from = "pid-active" })))
            test.eq(revived, 1)
            status = "waiting"
            test.is_true(select(1, wake_process.handle_exit(monitored, { from = "pid-waiting" })))
            test.eq(revived, 1)
        end)
    end)
end

return { run_tests = run_tests }
