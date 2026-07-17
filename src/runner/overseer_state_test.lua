local test = require("test")
local overseer = require("overseer_state")

local function activate(state: any, id: string, generation: number): any
    local next_state, decision, err = overseer.on_activation(state, {
        dataflow_id = id,
        generation = generation,
        desired_active = true,
        status = "running",
    })
    test.is_nil(err)
    test.eq(decision.kind, overseer.ACTION.INSPECT_OWNER)
    return next_state
end

local function track(state: any, id: string, generation: number, pid: string): any
    local next_state, decision, err = overseer.on_monitor_observation(state, {
        dataflow_id = id,
        generation = generation,
        pid = pid,
        monitor_ok = true,
    })
    test.is_nil(err)
    test.eq(decision.kind, overseer.ACTION.NONE)
    return next_state
end

local function run_tests()
    test.describe("Pure Dataflow overseer ownership state", function()
        test.it("tracks one PID under its dataflow and activation generation", function()
            local state = activate(overseer.new(), "df-one", 7)
            local observed, decision = overseer.on_owner_observation(state, {
                dataflow_id = "df-one",
                generation = 7,
                registered_pid = "pid-one",
            })
            test.eq(decision.kind, overseer.ACTION.MONITOR)
            test.eq(decision.pid, "pid-one")

            state = track(observed, "df-one", 7, "pid-one")
            local owner = test.not_nil(overseer.owner_for_dataflow(state, "df-one")) :: any
            test.eq(owner.pid, "pid-one")
            test.eq(owner.generation, 7)
            local reverse = test.not_nil(overseer.owner_for_pid(state, "pid-one")) :: any
            test.eq(reverse.dataflow_id, "df-one")
            test.eq(reverse.generation, 7)
        end)

        test.it("collapses duplicate activation notifications in pending and monitored phases", function()
            local state = activate(overseer.new(), "df-duplicate", 2)
            local duplicate, decision = overseer.on_activation(state, {
                dataflow_id = "df-duplicate",
                generation = 2,
                desired_active = true,
            })
            test.eq(decision.kind, overseer.ACTION.NONE)
            test.eq(decision.reason, "duplicate_notification")

            duplicate = track(duplicate, "df-duplicate", 2, "pid-duplicate")
            local after_track, tracked_decision = overseer.on_activation(duplicate, {
                dataflow_id = "df-duplicate",
                generation = 2,
                desired_active = true,
            })
            test.eq(tracked_decision.kind, overseer.ACTION.NONE)
            test.eq(tracked_decision.pid, "pid-duplicate")
            test.eq((test.not_nil(overseer.owner_for_dataflow(after_track, "df-duplicate")) :: any).pid, "pid-duplicate")
        end)

        test.it("rejects stale activation generations and stale EXIT events", function()
            local state = track(activate(overseer.new(), "df-current", 4), "df-current", 4, "pid-current")
            local after_notice, notice = overseer.on_activation(state, {
                dataflow_id = "df-current",
                generation = 3,
                desired_active = true,
            })
            test.eq(notice.reason, "stale_activation")

            local after_exit, exit_decision = overseer.on_exit(after_notice, {
                pid = "pid-current",
                generation = 3,
                desired_active = true,
            })
            test.eq(exit_decision.kind, overseer.ACTION.NONE)
            test.eq(exit_decision.reason, "stale_exit_generation")
            test.eq((test.not_nil(overseer.owner_for_dataflow(after_exit, "df-current")) :: any).pid, "pid-current")

            local unknown, unknown_decision = overseer.on_exit(after_exit, {
                pid = "pid-old",
                generation = 4,
                desired_active = true,
            })
            test.eq(unknown_decision.reason, "stale_exit")
            test.eq((test.not_nil(overseer.owner_for_dataflow(unknown, "df-current")) :: any).pid, "pid-current")
        end)

        test.it("never restarts durably inactive or terminal exits", function()
            local cases = {
                { desired_active = false, want = "inactive_exit" },
                { desired_active = true, terminal = true, want = "terminal_exit" },
                { desired_active = true, status = "cancelled", want = "terminal_exit" },
            }
            for index, case in ipairs(cases) do
                local id = "df-stop-" .. tostring(index)
                local pid = "pid-stop-" .. tostring(index)
                local state = track(activate(overseer.new(), id, 1), id, 1, pid)
                case.pid = pid
                case.generation = 1
                local next_state, decision, err = overseer.on_exit(state, case)
                test.is_nil(err)
                test.eq(decision.kind, overseer.ACTION.NONE)
                test.eq(decision.reason, case.want)
                test.is_nil(overseer.owner_for_dataflow(next_state, id))
                test.is_nil(overseer.owner_for_pid(next_state, pid))
            end
        end)

        test.it("reconciles a clean active exit because clean is not durable passivation", function()
            local state = track(activate(overseer.new({ retry_base_ms = 40 }), "df-clean", 2), "df-clean", 2, "pid-clean")
            local exited, inspect, err = overseer.on_exit(state, {
                pid = "pid-clean",
                generation = 2,
                desired_active = true,
                clean = true,
            })
            test.is_nil(err)
            test.eq(inspect.kind, overseer.ACTION.INSPECT_OWNER)
            test.eq(inspect.reason, "clean_exit_while_active")

            local replacement, monitor = overseer.on_owner_observation(exited, {
                dataflow_id = "df-clean",
                generation = 2,
                registered_pid = "pid-replacement",
            })
            test.eq(monitor.kind, overseer.ACTION.MONITOR)
            test.eq(monitor.pid, "pid-replacement")

            local no_owner_state = track(activate(overseer.new({ retry_base_ms = 40 }), "df-clean-missing", 2), "df-clean-missing", 2, "pid-gone")
            local no_owner_exit = overseer.on_exit(no_owner_state, {
                pid = "pid-gone", generation = 2, desired_active = true, clean = true,
            })
            local scheduled, restart = overseer.on_owner_observation(no_owner_exit, {
                dataflow_id = "df-clean-missing", generation = 2,
            })
            test.eq(restart.kind, overseer.ACTION.RESTART)
            test.eq(restart.delay_ms, 40)
            test.eq((test.not_nil(overseer.owner_for_dataflow(scheduled, "df-clean-missing")) :: any).phase, "restart_scheduled")
            test.eq((test.not_nil(overseer.owner_for_dataflow(replacement, "df-clean")) :: any).phase, "monitor_requested")
        end)

        test.it("restarts unexpected active exits with bounded exponential backoff", function()
            local state = overseer.new({ retry_base_ms = 100, retry_max_ms = 400 })
            state = track(activate(state, "df-crash", 9), "df-crash", 9, "pid-1")

            local expected = { 100, 200, 400, 400 }
            for attempt, delay in ipairs(expected) do
                local exited, inspect, err = overseer.on_exit(state, {
                    pid = "pid-" .. tostring(attempt),
                    generation = 9,
                    desired_active = true,
                    clean = false,
                })
                test.is_nil(err)
                test.eq(inspect.kind, overseer.ACTION.INSPECT_OWNER)
                local crashed, decision = overseer.on_owner_observation(exited, {
                    dataflow_id = "df-crash", generation = 9,
                })
                test.eq(decision.kind, overseer.ACTION.RESTART)
                test.eq(decision.delay_ms, delay)
                test.eq(decision.attempt, attempt)

                local due, inspect = overseer.on_restart_due(crashed, {
                    dataflow_id = "df-crash",
                    generation = 9,
                })
                test.eq(inspect.kind, overseer.ACTION.INSPECT_OWNER)
                state = track(due, "df-crash", 9, "pid-" .. tostring(attempt + 1))
            end
        end)

        test.it("does not reset crash backoff until durable progress marks the owner stable", function()
            local state = track(activate(overseer.new({ retry_base_ms = 10 }), "df-stable", 1), "df-stable", 1, "pid-a")
            local exited = overseer.on_exit(state, {
                pid = "pid-a", generation = 1, desired_active = true,
            })
            local crashed = overseer.on_owner_observation(exited, {
                dataflow_id = "df-stable", generation = 1,
            })
            local due = overseer.on_restart_due(crashed, { dataflow_id = "df-stable", generation = 1 })
            state = track(due, "df-stable", 1, "pid-b")

            local stable, stable_decision = overseer.mark_stable(state, {
                dataflow_id = "df-stable", generation = 1,
            })
            test.eq(stable_decision.reason, "failure_backoff_reset")
            local exited_again = overseer.on_exit(stable, {
                pid = "pid-b", generation = 1, desired_active = true,
            })
            local _, decision = overseer.on_owner_observation(exited_again, {
                dataflow_id = "df-stable", generation = 1,
            })
            test.eq(decision.delay_ms, 10)
            test.eq(decision.attempt, 1)
        end)

        test.it("converges ambiguous spawn outcomes on a canonical registered owner", function()
            local state = activate(overseer.new(), "df-ambiguous", 3)
            local after_spawn, decision, err = overseer.on_spawn_observation(state, {
                dataflow_id = "df-ambiguous",
                generation = 3,
                spawn_error = "timeout",
                registered_pid = "pid-canonical",
            })
            test.is_nil(err)
            test.eq(decision.kind, overseer.ACTION.MONITOR)
            test.eq(decision.pid, "pid-canonical")
            test.eq(decision.reason, "canonical_owner_after_spawn")

            local tracked, settled = overseer.on_monitor_observation(after_spawn, {
                dataflow_id = "df-ambiguous",
                generation = 3,
                pid = "pid-canonical",
                monitor_ok = true,
            })
            test.eq(settled.kind, overseer.ACTION.NONE)
            test.eq((test.not_nil(overseer.owner_for_dataflow(tracked, "df-ambiguous")) :: any).pid, "pid-canonical")
        end)

        test.it("monitors a returned spawn PID before considering another spawn", function()
            local state = activate(overseer.new(), "df-spawned", 1)
            local next_state, decision = overseer.on_spawn_observation(state, {
                dataflow_id = "df-spawned",
                generation = 1,
                spawn_pid = "pid-spawned",
            })
            test.eq(decision.kind, overseer.ACTION.MONITOR)
            test.eq(decision.pid, "pid-spawned")
            test.eq((test.not_nil(overseer.owner_for_dataflow(next_state, "df-spawned")) :: any).phase, "monitor_requested")
        end)

        test.it("follows a replacement canonical owner after monitor ambiguity", function()
            local state = activate(overseer.new(), "df-replaced", 5)
            local next_state, decision = overseer.on_monitor_observation(state, {
                dataflow_id = "df-replaced",
                generation = 5,
                pid = "pid-gone",
                monitor_ok = false,
                registered_pid = "pid-replacement",
            })
            test.eq(decision.kind, overseer.ACTION.MONITOR)
            test.eq(decision.pid, "pid-replacement")
            test.eq(decision.reason, "owner_changed_during_monitor")
            test.eq((test.not_nil(overseer.owner_for_dataflow(next_state, "df-replaced")) :: any).phase, "monitor_requested")
        end)

        test.it("backs off when spawn and monitor observations prove no owner", function()
            local state = activate(overseer.new({ retry_base_ms = 25 }), "df-missing", 1)
            local after_spawn, spawn_decision = overseer.on_spawn_observation(state, {
                dataflow_id = "df-missing",
                generation = 1,
                spawn_error = "host unavailable",
            })
            test.eq(spawn_decision.kind, overseer.ACTION.RESTART)
            test.eq(spawn_decision.delay_ms, 25)

            local due = overseer.on_restart_due(after_spawn, { dataflow_id = "df-missing", generation = 1 })
            local after_monitor, monitor_decision = overseer.on_monitor_observation(due, {
                dataflow_id = "df-missing",
                generation = 1,
                pid = "pid-gone",
                monitor_ok = false,
            })
            test.eq(monitor_decision.kind, overseer.ACTION.RESTART)
            test.eq(monitor_decision.delay_ms, 50)
            test.eq((test.not_nil(overseer.owner_for_dataflow(after_monitor, "df-missing")) :: any).phase, "restart_scheduled")
        end)
    end)
end

return { run_tests = test.run_cases(run_tests) }
