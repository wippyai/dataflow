local test = require("test")
local overseer: any = require("overseer_state")
local on_activation: any = overseer.on_activation
local on_owner_observation: any = overseer.on_owner_observation
local on_claim_observation: any = overseer.on_claim_observation
local on_spawn_observation: any = overseer.on_spawn_observation
local on_monitor_observation: any = overseer.on_monitor_observation
local on_exit: any = overseer.on_exit
local on_failed: any = overseer.on_failed
local CURRENT_EPOCH = "runtime-current"

local function required(value: any): any
    return test.not_nil(value) :: any
end

local function activate(state, id: string, generation: number)
    local next_state, decision, err = on_activation(state, {
        dataflow_id = id,
        generation = generation,
        desired_active = true,
        status = "running",
        runtime_epoch = CURRENT_EPOCH,
    })
    test.is_nil(err)
    decision = required(decision)
    test.eq(decision.kind, overseer.ACTION.INSPECT_OWNER)
    return next_state
end

local function acquire(state, id: string, generation: number, pid: string)
    local inspected, claim = on_owner_observation(state, {
        dataflow_id = id,
        generation = generation,
    })
    claim = required(claim)
    test.eq(claim.kind, overseer.ACTION.CLAIM)
    local claimed, spawn = on_claim_observation(inspected, {
        dataflow_id = id,
        generation = generation,
        claimed = true,
    })
    spawn = required(spawn)
    test.eq(spawn.kind, overseer.ACTION.SPAWN)
    local spawned, monitor = on_spawn_observation(claimed, {
        dataflow_id = id,
        generation = generation,
        spawn_pid = pid,
    })
    monitor = required(monitor)
    test.eq(monitor.kind, overseer.ACTION.MONITOR)
    local tracked, settled = on_monitor_observation(spawned, {
        dataflow_id = id,
        generation = generation,
        pid = pid,
        monitor_ok = true,
    })
    settled = required(settled)
    test.eq(settled.kind, overseer.ACTION.NONE)
    test.eq(settled.reason, "owner_monitored")
    return tracked
end

local function run_tests()
    test.describe("Pure Dataflow overseer ownership state", function()
        test.it("acquires a boot activation exactly once and indexes its canonical owner", function()
            local state = acquire(activate(overseer.new(), "df-boot", 1), "df-boot", 1, "pid-boot")
            local owner = test.not_nil(overseer.owner_for_dataflow(state, "df-boot"))
            test.eq(owner.pid, "pid-boot")
            test.eq(owner.generation, 1)
            local reverse = test.not_nil(overseer.owner_for_pid(state, "pid-boot"))
            test.eq(reverse.dataflow_id, "df-boot")
            test.eq(reverse.generation, 1)
        end)

        test.it("verifies a monitored duplicate notification without requesting another spawn", function()
            local state = acquire(activate(overseer.new(), "df-live", 2), "df-live", 2, "pid-live")
            local verifying, inspect = on_activation(state, {
                dataflow_id = "df-live", generation = 2, desired_active = true,
                owner_epoch = CURRENT_EPOCH, runtime_epoch = CURRENT_EPOCH,
            })
            inspect = required(inspect)
            test.eq(inspect.kind, overseer.ACTION.INSPECT_OWNER)
            test.eq(inspect.reason, "verify_active_owner")
            local tracked, verified = on_owner_observation(verifying, {
                dataflow_id = "df-live", generation = 2, registered_pid = "pid-live",
            })
            verified = required(verified)
            test.eq(verified.kind, overseer.ACTION.NONE)
            test.eq(verified.reason, "existing_owner_verified")
            test.eq((test.not_nil(overseer.owner_for_dataflow(tracked, "df-live"))).pid, "pid-live")
        end)

        test.it("turns a missing runtime owner into failure and never into restart", function()
            local state = acquire(activate(overseer.new(), "df-fail", 3), "df-fail", 3, "pid-fail")
            local exited, inspect = on_exit(state, {
                pid = "pid-fail",
                generation = 3,
                desired_active = true,
                message = "host process crashed",
            })
            inspect = required(inspect)
            test.eq(inspect.kind, overseer.ACTION.INSPECT_OWNER)
            local failing, failure = on_owner_observation(exited, {
                dataflow_id = "df-fail",
                generation = 3,
                message = tostring(inspect.message),
            })
            failure = required(failure)
            test.eq(failure.kind, overseer.ACTION.FAIL)
            test.eq(failure.reason, "runtime_owner_lost")
            test.eq(failure.message, "host process crashed")
            test.eq((test.not_nil(overseer.owner_for_dataflow(failing, "df-fail"))).phase,
                "failure_requested")
        end)

        test.it("adopts a canonical race winner instead of failing or duplicating it", function()
            local state = acquire(activate(overseer.new(), "df-race", 4), "df-race", 4, "pid-loser")
            local exited, inspect = on_exit(state, {
                pid = "pid-loser", generation = 4, desired_active = true,
            })
            inspect = required(inspect)
            local next_state, monitor = on_owner_observation(exited, {
                dataflow_id = "df-race", generation = 4, registered_pid = "pid-winner",
            })
            monitor = required(monitor)
            test.eq(inspect.kind, overseer.ACTION.INSPECT_OWNER)
            test.eq(monitor.kind, overseer.ACTION.MONITOR)
            test.eq(monitor.pid, "pid-winner")
            test.eq((test.not_nil(overseer.owner_for_dataflow(next_state, "df-race"))).phase,
                "monitor_requested")
        end)

        test.it("claims a newly registered synchronous owner before adopting it", function()
            local state = activate(overseer.new(), "df-direct", 1)
            local claiming, claim = on_owner_observation(state, {
                dataflow_id = "df-direct",
                generation = 1,
                registered_pid = "pid-direct",
            })
            claim = required(claim)
            test.eq(claim.kind, overseer.ACTION.CLAIM)
            test.eq(claim.reason, "new_owner_adoption_claim")
            test.is_nil(claim.observed_epoch)

            local monitoring, monitor = on_claim_observation(claiming, {
                dataflow_id = "df-direct",
                generation = 1,
                claimed = true,
            })
            monitor = required(monitor)
            test.eq(monitor.kind, overseer.ACTION.MONITOR)
            test.eq(monitor.reason, "activation_owner_epoch_claimed")
            test.eq(monitor.pid, "pid-direct")
            test.eq((test.not_nil(overseer.owner_for_dataflow(
                monitoring, "df-direct"))).phase, "monitor_requested")
        end)

        test.it("distinguishes full-runtime recovery from an overseer-only restart", function()
            local reboot_state, reboot_inspect = on_activation(overseer.new(), {
                dataflow_id = "df-reboot",
                generation = 4,
                desired_active = true,
                owner_epoch = "runtime-before",
                runtime_epoch = CURRENT_EPOCH,
            })
            test.eq(required(reboot_inspect).kind, overseer.ACTION.INSPECT_OWNER)
            local _, reboot_claim = on_owner_observation(reboot_state, {
                dataflow_id = "df-reboot", generation = 4,
            })
            reboot_claim = required(reboot_claim)
            test.eq(reboot_claim.kind, overseer.ACTION.CLAIM)
            test.eq(reboot_claim.reason, "reboot_recovery_claim")
            test.eq(reboot_claim.observed_epoch, "runtime-before")

            local same_state, same_inspect = on_activation(overseer.new(), {
                dataflow_id = "df-service-restart",
                generation = 5,
                desired_active = true,
                owner_epoch = CURRENT_EPOCH,
                runtime_epoch = CURRENT_EPOCH,
            })
            test.eq(required(same_inspect).kind, overseer.ACTION.INSPECT_OWNER)
            local _, failure = on_owner_observation(same_state, {
                dataflow_id = "df-service-restart", generation = 5,
            })
            failure = required(failure)
            test.eq(failure.kind, overseer.ACTION.FAIL)
            test.eq(failure.reason, "same_runtime_owner_missing")
        end)

        test.it("stops a live owner after a durable terminal transition", function()
            local state = acquire(activate(overseer.new(), "df-cancel", 5), "df-cancel", 5, "pid-cancel")
            local stopped, decision = on_activation(state, {
                dataflow_id = "df-cancel",
                generation = 5,
                desired_active = false,
                status = "cancelled",
                owner_epoch = CURRENT_EPOCH,
                runtime_epoch = CURRENT_EPOCH,
            })
            decision = required(decision)
            test.eq(decision.kind, overseer.ACTION.STOP)
            test.eq(decision.pid, "pid-cancel")
            test.is_nil(overseer.owner_for_dataflow(stopped, "df-cancel"))
            test.is_nil(overseer.owner_for_pid(stopped, "pid-cancel"))
        end)

        test.it("never acquires inactive or terminal activations", function()
            local inactive, inactive_decision = on_activation(overseer.new(), {
                dataflow_id = "df-waiting", generation = 1, desired_active = false, status = "waiting",
                runtime_epoch = CURRENT_EPOCH,
            })
            inactive_decision = required(inactive_decision)
            test.eq(inactive_decision.kind, overseer.ACTION.NONE)
            test.eq(inactive_decision.reason, "inactive")
            test.is_nil(overseer.owner_for_dataflow(inactive, "df-waiting"))

            local terminal, terminal_decision = on_activation(overseer.new(), {
                dataflow_id = "df-done", generation = 1, desired_active = true, status = "failed",
                runtime_epoch = CURRENT_EPOCH,
            })
            terminal_decision = required(terminal_decision)
            test.eq(terminal_decision.kind, overseer.ACTION.NONE)
            test.eq(terminal_decision.reason, "terminal")
            test.is_nil(overseer.owner_for_dataflow(terminal, "df-done"))
        end)

        test.it("generation fences stale notifications and stale EXIT events", function()
            local state = acquire(activate(overseer.new(), "df-current", 7), "df-current", 7, "pid-current")
            local unchanged, stale = on_activation(state, {
                dataflow_id = "df-current", generation = 6, desired_active = true,
                owner_epoch = CURRENT_EPOCH, runtime_epoch = CURRENT_EPOCH,
            })
            stale = required(stale)
            test.eq(stale.reason, "stale_activation")
            local after_exit, stale_exit = on_exit(unchanged, {
                pid = "pid-current", generation = 6, desired_active = true,
            })
            stale_exit = required(stale_exit)
            test.eq(stale_exit.reason, "stale_exit_generation")
            test.eq((test.not_nil(overseer.owner_for_dataflow(after_exit, "df-current"))).pid,
                "pid-current")
        end)

        test.it("advances a live owner's generation without re-monitoring its process", function()
            local state = acquire(activate(overseer.new(), "df-sequential", 1),
                "df-sequential", 1, "pid-sequential")
            local advancing, inspect = on_activation(state, {
                dataflow_id = "df-sequential", generation = 2, desired_active = true,
                runtime_epoch = CURRENT_EPOCH,
            })
            inspect = required(inspect)
            test.eq(inspect.kind, overseer.ACTION.INSPECT_OWNER)
            test.eq(inspect.reason, "advance_monitored_owner")
            test.eq((test.not_nil(overseer.owner_for_pid(
                advancing, "pid-sequential"))).generation, 2)

            local claiming, claim = on_owner_observation(advancing, {
                dataflow_id = "df-sequential", generation = 2,
                registered_pid = "pid-sequential",
            })
            claim = required(claim)
            test.eq(claim.kind, overseer.ACTION.CLAIM)
            local tracked, settled = on_claim_observation(claiming, {
                dataflow_id = "df-sequential", generation = 2, claimed = true,
            })
            settled = required(settled)
            test.eq(settled.kind, overseer.ACTION.NONE)
            test.eq(settled.reason, "existing_owner_claimed")
            local owner = test.not_nil(overseer.owner_for_dataflow(tracked, "df-sequential"))
            test.eq(owner.pid, "pid-sequential")
            test.eq(owner.generation, 2)
            test.eq(owner.phase, "monitored")
        end)

        test.it("does not let a racing activation resurrect a lost runtime owner", function()
            local state = acquire(activate(overseer.new(), "df-racing-loss", 4),
                "df-racing-loss", 4, "pid-racing-loss")
            local exited = select(1, on_exit(state, {
                pid = "pid-racing-loss", generation = 4, desired_active = true,
                message = "killed",
            }))
            local failing, failure = on_activation(exited, {
                dataflow_id = "df-racing-loss", generation = 5, desired_active = true,
                runtime_epoch = CURRENT_EPOCH,
            })
            failure = required(failure)
            test.eq(failure.kind, overseer.ACTION.FAIL)
            test.eq(failure.generation, 5)
            test.eq(failure.reason, "runtime_owner_lost")
            local owner = test.not_nil(overseer.owner_for_dataflow(failing, "df-racing-loss"))
            test.eq(owner.generation, 5)
            test.eq(owner.phase, "failure_requested")
        end)

        test.it("fails spawn and monitor acquisition errors without a retry action", function()
            local state = activate(overseer.new(), "df-spawn-error", 1)
            local inspected = select(1, on_owner_observation(state, {
                dataflow_id = "df-spawn-error", generation = 1,
            }))
            local _, spawn_failure = on_spawn_observation(inspected, {
                dataflow_id = "df-spawn-error", generation = 1, error = "host unavailable",
            })
            spawn_failure = required(spawn_failure)
            test.eq(spawn_failure.kind, overseer.ACTION.FAIL)
            test.eq(spawn_failure.reason, "orchestrator_spawn_failed")

            state = activate(overseer.new(), "df-monitor-error", 1)
            local _, monitor_failure = on_monitor_observation(state, {
                dataflow_id = "df-monitor-error",
                generation = 1,
                pid = "pid-missing",
                monitor_ok = false,
                error = "not found",
            })
            monitor_failure = required(monitor_failure)
            test.eq(monitor_failure.kind, overseer.ACTION.FAIL)
            test.eq(monitor_failure.reason, "orchestrator_monitor_failed")
        end)

        test.it("retries only failure persistence and removes state after it commits", function()
            local state = activate(overseer.new(), "df-persist", 8)
            local inspected = select(1, on_owner_observation(state, {
                dataflow_id = "df-persist", generation = 8,
            }))
            local failing = select(1, on_spawn_observation(inspected, {
                dataflow_id = "df-persist", generation = 8, error = "spawn failed",
            }))
            local repeated, retry = on_activation(failing, {
                dataflow_id = "df-persist", generation = 8, desired_active = true,
                owner_epoch = CURRENT_EPOCH, runtime_epoch = CURRENT_EPOCH,
            })
            retry = required(retry)
            test.eq(retry.kind, overseer.ACTION.FAIL)
            test.eq(retry.reason, "retry_failure_persistence")
            local cleared, done = on_failed(repeated, {
                dataflow_id = "df-persist", generation = 8,
            })
            done = required(done)
            test.eq(done.reason, "failure_persisted")
            test.is_nil(overseer.owner_for_dataflow(cleared, "df-persist"))
        end)
    end)
end

return { run_tests = test.run_cases(run_tests) }
