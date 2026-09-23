local test = require("test")
local uuid = require("uuid")
local overseer: any = require("overseer")
local CURRENT_EPOCH = "runtime-current"

local function activation(id: string, generation: number, launch_args: any?): any
    return {
        dataflow_id = id,
        generation = generation,
        desired_active = true,
        launch_args = launch_args or {},
    }
end

local function workflow(id: string, status: string?): any
    return {
        dataflow_id = id,
        actor_id = "actor:" .. id,
        actor_context = "frame:" .. id,
        status = status or overseer.consts.STATUS.RUNNING,
    }
end

local function process_error(kind: string, message: string)
    return setmetatable({ kind = function() return kind end }, {
        __tostring = function() return message end,
    })
end

local function captures()
    return {
        owners = {},
        lookups = {},
        monitors = {},
        spawns = {},
        sends = {},
        cancels = {},
        terminates = {},
        failures = {},
        reconstructions = {},
        locked_reads = {},
    }
end

local function process_mock(captured)
    local mock = {
        event = { EXIT = "EXIT", LINK_DOWN = "LINK_DOWN", CANCEL = "CANCEL" },
        registry = {
            lookup = function(name)
                table.insert(captured.lookups, name)
                local pid = captured.owners[name]
                if pid then return pid, nil end
                return nil, process_error("NotFound", "name not registered")
            end,
        },
        monitor = function(pid)
            table.insert(captured.monitors, tostring(pid))
            return true, nil
        end,
        send = function(pid, topic, payload)
            table.insert(captured.sends, { pid = tostring(pid), topic = topic, payload = payload })
            return true, nil
        end,
        cancel = function(pid, timeout)
            table.insert(captured.cancels, { pid = tostring(pid), timeout = timeout })
            return true, nil
        end,
        terminate = function(pid)
            table.insert(captured.terminates, tostring(pid))
            return true, nil
        end,
    }
    mock.with_context = function(context)
        local spawner: any = { context = context }
        function spawner:with_name(name)
            self.name = name
            return self
        end
        function spawner:with_actor(actor)
            self.actor = actor
            return self
        end
        function spawner:with_scope(scope)
            self.scope = scope
            return self
        end
        function spawner:spawn_monitored(source, host, args)
            if captured.owners[self.name] then return nil, "name already registered" end
            local pid = "pid-" .. tostring(args.dataflow_id) .. "-" .. tostring(#captured.spawns + 1)
            captured.owners[self.name] = pid
            table.insert(captured.spawns, {
                source = source,
                host = host,
                args = args,
                context = self.context,
                name = self.name,
                actor = self.actor,
                scope = self.scope,
                pid = pid,
            })
            return pid, nil
        end
        return spawner
    end
    return mock
end

-- The overseer never writes ownership: these helpers play the orchestrator.
local function admit(row: any, token: string, pid: string, runtime_epoch: string?)
    row.owner_token = token
    row.owner_pid = pid
    row.owner_epoch = runtime_epoch or CURRENT_EPOCH
    row.owner_phase = "running"
end

local function release(row: any)
    row.owner_phase = "released"
    row.desired_active = false
end

local function advance(row: any)
    row.generation = row.generation + 1
    row.desired_active = true
end

local function fence_matches(row: any, fence: any): boolean
    if row.owner_token ~= fence.token or row.owner_phase ~= fence.phase then return false end
    return fence.generation == nil or row.generation == fence.generation
end

local function run_tests()
    test.describe("Dataflow overseer IO", function()
        local originals
        local observed
        local activations: { [string]: any } = {}
        local workflows: { [string]: any } = {}
        local locked_read_errors: { string } = {}

        test.before_each(function()
            originals = {
                activation_repo = overseer.activation_repo,
                commit = overseer.commit,
                dataflow_repo = overseer.dataflow_repo,
                execution_frame = overseer.execution_frame,
                process = overseer.process,
                sql = overseer.sql,
                with_tx = overseer.with_tx,
                pending_due = overseer.pending_due,
            }
            observed = captures()
            activations = {} :: { [string]: any }
            workflows = {} :: { [string]: any }
            locked_read_errors = {} :: { string }
            overseer.process = process_mock(observed)
            overseer.execution_frame = {
                reconstruct = function(actor_id, actor_context)
                    table.insert(observed.reconstructions, {
                        actor_id = actor_id,
                        actor_context = actor_context,
                    })
                    return "restored:" .. actor_id, "scope:" .. actor_id, nil
                end,
            }
            overseer.activation_repo = {
                read_locked_tx = function(_tx, id)
                    table.insert(observed.locked_reads, id)
                    local read_err = table.remove(locked_read_errors, 1)
                    if read_err then return nil, read_err end
                    local status = workflows[id] and workflows[id].status or nil
                    return { activation = activations[id], status = status }, nil
                end,
                list_active = function()
                    local rows = {}
                    for _, row in pairs(activations) do
                        if row.desired_active then table.insert(rows, row) end
                    end
                    return rows, nil
                end,
            }
            overseer.dataflow_repo = {
                get = function(id)
                    if workflows[id] then return workflows[id], nil end
                    return nil, "Workflow not found"
                end,
            }
            overseer.commit = {
                fail_activation = function(id, fence, failure)
                    local row = activations[id]
                    local completed = row ~= nil and row.desired_active == true and fence_matches(row, fence)
                    table.insert(observed.failures, {
                        dataflow_id = id,
                        generation = row and row.generation or nil,
                        fence = fence,
                        failure = failure,
                        completed = completed,
                    })
                    if not completed then
                        return { completed = false, current_generation = row and row.generation }, nil
                    end
                    row.desired_active = false
                    row.owner_phase = row.owner_token and "released" or row.owner_phase
                    workflows[id].status = overseer.consts.STATUS.COMPLETED_FAILURE
                    return { completed = true, current_generation = row.generation }, nil
                end,
            }
            overseer.with_tx = function(fn) return fn({}) end
            overseer.pending_due = function() return {}, nil end
        end)

        test.after_each(function()
            for key, value in pairs(originals) do overseer[key] = value end
        end)

        local function completed_failures(): { any }
            local done = {}
            for _, failure in ipairs(observed.failures) do
                if failure.completed then table.insert(done, failure) end
            end
            return done
        end

        test.it("recovers each durable boot activation once under its frozen actor and scope", function()
            activations.boot = activation("boot", 3, { init_func_id = "app:init" })
            workflows.boot = workflow("boot")
            local runtime = overseer.new_runtime(CURRENT_EPOCH)
            local count, err = overseer.bootstrap(runtime)
            test.is_nil(err)
            test.eq(count, 1)
            test.is_true(runtime.bootstrapped)
            test.eq(#observed.spawns, 1)
            local spawn = observed.spawns[1]
            test.eq(spawn.name, "dataflow.boot")
            test.eq(spawn.source, overseer.consts.ORCHESTRATOR)
            test.eq(spawn.host, overseer.consts.HOST_ID)
            test.eq(spawn.args.activation_generation, 3)
            test.eq(spawn.args.init_func_id, "app:init")
            test.eq(spawn.actor, "restored:actor:boot")
            test.eq(spawn.scope, "scope:actor:boot")

            local second, second_err = overseer.safety_reconcile(runtime)
            test.is_nil(second_err)
            test.eq(second, 1)
            test.eq(#observed.spawns, 1)
        end)

        test.it("adopts an existing canonical owner without reconstructing or spawning", function()
            activations.live = activation("live", 1)
            admit(activations.live, "t-live", "pid-existing")
            workflows.live = workflow("live")
            observed.owners["dataflow.live"] = "pid-existing"
            local ok, err = overseer.reconcile(overseer.new_runtime(CURRENT_EPOCH), "live")
            test.is_nil(err)
            test.is_true(ok)
            test.eq(#observed.spawns, 0)
            test.eq(#observed.reconstructions, 0)
            test.eq(observed.monitors[1], "pid-existing")
        end)

        test.it("accepts the idempotent monitor result for an already monitored owner", function()
            activations.monitored = activation("monitored", 1)
            workflows.monitored = workflow("monitored")
            observed.owners["dataflow.monitored"] = "pid-monitored"
            overseer.process.monitor = function(pid)
                table.insert(observed.monitors, tostring(pid))
                return nil, "already monitoring pid"
            end
            local runtime = overseer.new_runtime(CURRENT_EPOCH)
            local ok, err = overseer.reconcile(runtime, "monitored")
            test.is_nil(err)
            test.is_true(ok)
            test.eq(#observed.spawns, 0)
            test.eq(#observed.failures, 0)
            test.eq(runtime.ownership.by_dataflow["monitored"], "pid-monitored")
        end)

        test.it("spawns the successor at once when a released owner's EXIT is still pending", function()
            activations.handoff = activation("handoff", 1)
            workflows.handoff = workflow("handoff")
            local runtime = overseer.new_runtime(CURRENT_EPOCH)
            test.is_true(select(1, overseer.reconcile(runtime, "handoff")))
            local first = observed.spawns[1]
            admit(activations.handoff, "t1", tostring(first.pid))

            release(activations.handoff)
            observed.owners["dataflow.handoff"] = nil
            advance(activations.handoff)
            test.is_true(select(1, overseer.handle_activation_hint(runtime, { dataflow_id = "handoff" })))
            test.eq(#observed.spawns, 2)
            test.eq(observed.spawns[2].args.activation_generation, 2)
            test.eq(#observed.failures, 0)

            local handled, exit_err = overseer.handle_exit(runtime, {
                kind = overseer.process.event.EXIT, from = first.pid,
                result = { value = { success = true, passivated = true } },
            })
            test.is_nil(exit_err)
            test.is_true(handled)
            test.eq(#observed.spawns, 2)
            test.eq(#observed.failures, 0)
            test.eq(runtime.ownership.by_dataflow["handoff"], observed.spawns[2].pid)
        end)

        test.it("waits for a released owner that still holds the name, then spawns on its EXIT", function()
            activations.draining = activation("draining", 1)
            workflows.draining = workflow("draining")
            local runtime = overseer.new_runtime(CURRENT_EPOCH)
            test.is_true(select(1, overseer.reconcile(runtime, "draining")))
            local first = observed.spawns[1]
            admit(activations.draining, "t1", tostring(first.pid))
            release(activations.draining)
            advance(activations.draining)

            test.is_true(select(1, overseer.handle_activation_hint(runtime, { dataflow_id = "draining" })))
            test.eq(#observed.spawns, 1, "the name holder is monitored, not replaced")

            observed.owners["dataflow.draining"] = nil
            test.is_true(select(1, overseer.handle_exit(runtime, {
                kind = overseer.process.event.EXIT, from = first.pid, result = { value = { passivated = true } },
            })))
            test.eq(#observed.spawns, 2)
            test.eq(#observed.failures, 0)
        end)

        test.it("spawns exactly once while requests advance between observation and spawn", function()
            activations.burst = activation("burst", 1)
            workflows.burst = workflow("burst")
            local runtime = overseer.new_runtime(CURRENT_EPOCH)
            local reconstruct = overseer.execution_frame.reconstruct
            overseer.execution_frame.reconstruct = function(actor_id, actor_context)
                for _ = 1, 5 do advance(activations.burst) end
                return reconstruct(actor_id, actor_context)
            end
            test.is_true(select(1, overseer.reconcile(runtime, "burst")))
            for _ = 1, 5 do
                advance(activations.burst)
                test.is_true(select(1, overseer.handle_activation_hint(runtime, { dataflow_id = "burst" })))
            end
            test.eq(#observed.spawns, 1)
            test.eq(#observed.failures, 0)
        end)

        test.it("fails a dead running owner found by a restarted overseer of the same runtime", function()
            activations.dead = activation("dead", 4)
            admit(activations.dead, "t-dead", "pid-gone")
            advance(activations.dead)
            workflows.dead = workflow("dead")

            local ok, err = overseer.reconcile(overseer.new_runtime(CURRENT_EPOCH), "dead")
            test.is_nil(err)
            test.is_true(ok)
            test.eq(#observed.spawns, 0)
            local failures = completed_failures()
            test.eq(#failures, 1)
            test.eq(failures[1].generation, 5)
            test.eq(failures[1].fence.token, "t-dead")
            test.eq(failures[1].failure.reason, "runtime_owner_lost")
        end)

        test.it("spawns after a restart for a released owner or an owner of an earlier runtime", function()
            activations.released = activation("released", 2)
            admit(activations.released, "t-released", "pid-gone")
            release(activations.released)
            advance(activations.released)
            workflows.released = workflow("released")
            activations.rebooted = activation("rebooted", 2)
            admit(activations.rebooted, "t-old", "pid-old", "runtime-before")
            workflows.rebooted = workflow("rebooted")

            local runtime = overseer.new_runtime(CURRENT_EPOCH)
            test.is_true(select(1, overseer.reconcile(runtime, "released")))
            test.is_true(select(1, overseer.reconcile(runtime, "rebooted")))
            test.eq(#observed.failures, 0)
            test.eq(#observed.spawns, 2)
            test.eq(observed.spawns[1].args.activation_generation, 3)
            test.eq(observed.spawns[2].args.activation_generation, 2)
        end)

        test.it("fails the latest request once when an unreleased owner exits after advances", function()
            activations.crash = activation("crash", 4)
            workflows.crash = workflow("crash")
            local runtime = overseer.new_runtime(CURRENT_EPOCH)
            test.is_true(select(1, overseer.reconcile(runtime, "crash")))
            local pid = observed.spawns[1].pid
            admit(activations.crash, "t-crash", pid)
            advance(activations.crash)
            advance(activations.crash)
            observed.owners["dataflow.crash"] = nil

            local handled, exit_err = overseer.handle_exit(runtime, {
                kind = overseer.process.event.EXIT,
                from = pid,
                result = { error = "executor panicked" },
            })
            test.is_nil(exit_err)
            test.is_true(handled)
            local failures = completed_failures()
            test.eq(#failures, 1)
            test.eq(failures[1].generation, 6)
            test.eq(failures[1].failure.message, "executor panicked")
            test.eq(#observed.spawns, 1)

            local _, safety_err = overseer.safety_reconcile(runtime)
            test.is_nil(safety_err)
            test.eq(#completed_failures(), 1)
            test.eq(#observed.spawns, 1)
        end)

        test.it("adopts a successor that took the name before the old owner's EXIT", function()
            activations.race = activation("race", 1)
            workflows.race = workflow("race")
            local runtime = overseer.new_runtime(CURRENT_EPOCH)
            test.is_true(select(1, overseer.reconcile(runtime, "race")))
            local old_pid = observed.spawns[1].pid
            admit(activations.race, "t-old", old_pid)
            release(activations.race)
            advance(activations.race)
            observed.owners["dataflow.race"] = "pid-new"
            admit(activations.race, "t-new", "pid-new")

            local handled, err = overseer.handle_exit(runtime, {
                kind = overseer.process.event.EXIT,
                from = old_pid,
                result = { error = "old owner exited" },
            })
            test.is_nil(err)
            test.is_true(handled)
            test.eq(#observed.failures, 0)
            test.eq(#observed.spawns, 1)
            test.eq(observed.monitors[#observed.monitors], "pid-new")
        end)

        test.it("defers an observation error and converges on the next reconcile", function()
            activations.retry = activation("retry", 1)
            workflows.retry = workflow("retry")
            local runtime = overseer.new_runtime(CURRENT_EPOCH)
            table.insert(locked_read_errors, "database is locked")
            local first, first_err = overseer.reconcile(runtime, "retry")
            test.is_nil(first)
            test.contains(tostring(first_err), "database is locked")
            test.eq(#observed.spawns, 0)

            test.is_true(select(1, overseer.reconcile(runtime, "retry")))
            test.eq(#observed.spawns, 1)
            test.eq(#observed.failures, 0)
        end)

        test.it("re-observes after losing the canonical name to a concurrent owner", function()
            activations.conflict = activation("conflict", 1)
            workflows.conflict = workflow("conflict")
            local reconstruct = overseer.execution_frame.reconstruct
            overseer.execution_frame.reconstruct = function(actor_id, actor_context)
                observed.owners["dataflow.conflict"] = "pid-winner"
                return reconstruct(actor_id, actor_context)
            end
            local ok, err = overseer.reconcile(overseer.new_runtime(CURRENT_EPOCH), "conflict")
            test.is_nil(err)
            test.is_true(ok)
            test.eq(#observed.spawns, 0)
            test.eq(#observed.failures, 0)
            test.eq(observed.monitors[#observed.monitors], "pid-winner")
        end)

        test.it("fails an unreconstructable execution frame instead of root-spawning or retrying", function()
            activations.frame = activation("frame", 2)
            workflows.frame = workflow("frame")
            overseer.execution_frame = {
                reconstruct = function() return nil, nil, "policy no longer exists" end,
            }
            local ok, err = overseer.reconcile(overseer.new_runtime(CURRENT_EPOCH), "frame")
            test.is_nil(err)
            test.is_true(ok)
            test.eq(#observed.spawns, 0)
            local failures = completed_failures()
            test.eq(#failures, 1)
            test.eq(failures[1].failure.reason, "orchestrator_spawn_failed")
            test.contains(failures[1].failure.message, "policy no longer exists")
            test.eq(failures[1].fence.generation, 2)
        end)

        test.it("stops a monitored process after durable cancellation", function()
            activations.cancelled = activation("cancelled", 5)
            workflows.cancelled = workflow("cancelled")
            local runtime = overseer.new_runtime(CURRENT_EPOCH)
            test.is_true(select(1, overseer.reconcile(runtime, "cancelled")))
            local pid = observed.spawns[1].pid
            activations.cancelled.desired_active = false
            workflows.cancelled.status = overseer.consts.STATUS.CANCELLED

            local ok, err = overseer.reconcile(runtime, "cancelled")
            test.is_nil(err)
            test.is_true(ok)
            test.eq(#observed.cancels, 1)
            test.eq(observed.cancels[1].pid, pid)
            test.eq(#observed.failures, 0)
        end)

        test.it("promotes an exact due wake once and wakes the live owner", function()
            local due = activation("due", 6)
            due.promoted = true
            admit(due, "t-due", "pid-due")
            activations.due = due
            workflows.due = workflow("due")
            observed.owners["dataflow.due"] = "pid-due"
            local calls = 0
            overseer.pending_due = function()
                return { { dataflow_id = "due", wake_key = "yield:one", wake_at = "2026-07-24T00:00:00Z" } }, nil
            end
            overseer.activation_repo.activate_due_tx = function(_tx, id, key)
                calls = calls + 1
                test.eq(id, "due")
                test.eq(key, "yield:one")
                if calls == 1 then return due, nil end
                return { promoted = false, already_promoted = true, generation = 6 }, nil
            end

            local runtime = overseer.new_runtime(CURRENT_EPOCH)
            local first, first_err = overseer.promote_due(runtime)
            local second, second_err = overseer.promote_due(runtime)
            test.is_nil(first_err)
            test.is_nil(second_err)
            test.eq(first, 1)
            test.eq(second, 0)
            test.eq(#observed.spawns, 0)
            test.eq(#observed.sends, 1)
            test.eq(observed.sends[1].pid, "pid-due")
            test.eq(observed.sends[1].topic, overseer.consts.MESSAGE_TOPIC.WAKE)
            test.eq(observed.sends[1].payload.generation, 6)
        end)

        test.it("wakes a live owner once per durable generation, across restarts", function()
            activations.quiet = activation("quiet", 1)
            admit(activations.quiet, "t-quiet", "pid-quiet")
            workflows.quiet = workflow("quiet")
            observed.owners["dataflow.quiet"] = "pid-quiet"
            -- A signal committed, but its producer died before sending COMMIT.
            advance(activations.quiet)

            local runtime = overseer.new_runtime(CURRENT_EPOCH)
            test.is_true(select(1, overseer.handle_activation_hint(runtime, { dataflow_id = "quiet" })))
            test.not_nil(select(1, overseer.safety_reconcile(runtime)))
            test.eq(#observed.sends, 1)
            advance(activations.quiet)
            test.not_nil(select(1, overseer.safety_reconcile(runtime)))
            test.not_nil(select(1, overseer.bootstrap(overseer.new_runtime(CURRENT_EPOCH))))
            test.eq(#observed.sends, 3)
            local generations = { 2, 3, 3 }
            for index, sent in ipairs(observed.sends) do
                test.eq(sent.pid, "pid-quiet")
                test.eq(sent.topic, overseer.consts.MESSAGE_TOPIC.WAKE)
                test.eq(sent.payload.generation, generations[index])
            end
            test.eq(#observed.spawns, 0)
        end)

        test.it("leaves a released owner in its unregister window to exit on its own", function()
            activations.window = activation("window", 1)
            admit(activations.window, "t-window", "pid-window")
            release(activations.window)
            workflows.window = workflow("window", overseer.consts.STATUS.WAITING)
            observed.owners["dataflow.window"] = "pid-window"
            local ok, err = overseer.reconcile(overseer.new_runtime(CURRENT_EPOCH), "window")
            test.is_nil(err)
            test.is_true(ok)
            test.eq(#observed.cancels, 0)
            test.eq(#observed.terminates, 0)
        end)

        test.it("never cancels an unadmitted name holder that a later request admits", function()
            activations.successor = activation("successor", 1)
            admit(activations.successor, "t-old", "pid-old")
            release(activations.successor)
            workflows.successor = workflow("successor", overseer.consts.STATUS.WAITING)
            observed.owners["dataflow.successor"] = "pid-successor"
            local runtime = overseer.new_runtime(CURRENT_EPOCH)
            test.is_true(select(1, overseer.reconcile(runtime, "successor")))

            advance(activations.successor)
            admit(activations.successor, "t-successor", "pid-successor")
            test.is_true(select(1, overseer.reconcile(runtime, "successor")))
            test.eq(#observed.cancels, 0)
            test.eq(#observed.failures, 0)
            test.eq(runtime.ownership.by_dataflow["successor"], "pid-successor")
        end)

        test.it("bounds restarts of an orchestrator that exits before admission", function()
            activations.unstartable = activation("unstartable", 2)
            workflows.unstartable = workflow("unstartable")
            local runtime = overseer.new_runtime(CURRENT_EPOCH)
            test.is_true(select(1, overseer.reconcile(runtime, "unstartable")))
            for _ = 1, 5 do
                local latest = observed.spawns[#observed.spawns]
                observed.owners["dataflow.unstartable"] = nil
                local handled, exit_err = overseer.handle_exit(runtime, {
                    kind = overseer.process.event.EXIT,
                    from = latest.pid,
                    result = { error = "not allowed: app:db" },
                })
                test.is_nil(exit_err)
                test.is_true(handled)
            end
            test.eq(#observed.spawns, 3)
            local failures = completed_failures()
            test.eq(#failures, 1)
            test.eq(failures[1].failure.reason, "orchestrator_start_failed")
            test.eq(failures[1].failure.message, "not allowed: app:db")
            test.eq(failures[1].fence.generation, 2)
        end)

        test.it("restarts an orchestrator that exited before admission once it can start", function()
            activations.flaky = activation("flaky", 1)
            workflows.flaky = workflow("flaky")
            local runtime = overseer.new_runtime(CURRENT_EPOCH)
            test.is_true(select(1, overseer.reconcile(runtime, "flaky")))
            observed.owners["dataflow.flaky"] = nil
            test.is_true(select(1, overseer.handle_exit(runtime, {
                kind = overseer.process.event.EXIT, from = observed.spawns[1].pid,
                result = { error = "transient start failure" },
            })))
            test.eq(#observed.spawns, 2)
            admit(activations.flaky, "t-flaky", tostring(observed.spawns[2].pid))
            test.not_nil(select(1, overseer.safety_reconcile(runtime)))
            test.eq(#observed.spawns, 2)
            test.eq(#observed.failures, 0)
        end)

        test.it("fails a running owner whose process dies between observation and monitoring", function()
            activations.vanishing = activation("vanishing", 3)
            admit(activations.vanishing, "t-vanishing", "pid-vanishing")
            workflows.vanishing = workflow("vanishing")
            observed.owners["dataflow.vanishing"] = "pid-vanishing"
            overseer.process.monitor = function(pid)
                table.insert(observed.monitors, tostring(pid))
                observed.owners["dataflow.vanishing"] = nil
                return nil, "process not found"
            end
            local ok, err = overseer.reconcile(overseer.new_runtime(CURRENT_EPOCH), "vanishing")
            test.is_nil(err)
            test.is_true(ok)
            local failures = completed_failures()
            test.eq(#failures, 1)
            test.eq(failures[1].failure.reason, "runtime_owner_lost")
            test.eq(#observed.spawns, 0)
        end)

        test.it("retries failure persistence on the next reconcile after a write error", function()
            activations.persist = activation("persist", 4)
            admit(activations.persist, "t-persist", "pid-gone")
            workflows.persist = workflow("persist")
            local fail_activation = overseer.commit.fail_activation
            local attempts = 0
            overseer.commit.fail_activation = function(id, fence, failure)
                attempts = attempts + 1
                if attempts == 1 then return nil, "database is locked" end
                return fail_activation(id, fence, failure)
            end
            local runtime = overseer.new_runtime(CURRENT_EPOCH)
            local first, first_err = overseer.reconcile(runtime, "persist")
            test.is_nil(first)
            test.contains(tostring(first_err), "database is locked")
            advance(activations.persist)
            test.is_true(select(1, overseer.reconcile(runtime, "persist")))
            local failures = completed_failures()
            test.eq(#failures, 1)
            test.eq(failures[1].generation, 5)
            test.eq(#observed.spawns, 0)
        end)

        test.it("recovers an activation whose notification was lost on the safety scan", function()
            local runtime = overseer.new_runtime(CURRENT_EPOCH)
            activations.lost = activation("lost", 9)
            workflows.lost = workflow("lost")
            local count, err = overseer.safety_reconcile(runtime)
            test.is_nil(err)
            test.eq(count, 1)
            test.eq(#observed.spawns, 1)
            test.eq(observed.spawns[1].args.activation_generation, 9)
        end)

        test.it("arms a timer only for a wake that is not yet due", function()
            local past = overseer.time.now():add(-1 * overseer.time.SECOND):format(overseer.time.RFC3339NANO)
            local future = overseer.time.now():add(60 * overseer.time.SECOND):format(overseer.time.RFC3339NANO)
            local due_timer, due = overseer.arm_wake({ wake_at = past })
            test.is_nil(due_timer)
            test.is_true(due)
            local timer, pending = overseer.arm_wake({ wake_at = future })
            test.not_nil(timer)
            test.is_false(pending)
        end)

        test.it("recognizes missing SQLite and PostgreSQL migration state", function()
            test.is_true(overseer.schema_not_ready("no such table: dataflow_activations"))
            test.is_true(overseer.schema_not_ready('relation "dataflow_wakes" does not exist'))
            test.is_false(overseer.schema_not_ready("database connection lost"))
        end)

        test.it("notifies only the overseer topology", function()
            local ok, err = overseer.notify({ dataflow_id = "df", generation = 1 })
            test.is_nil(err)
            test.is_true(ok)
            test.eq(#observed.sends, 1)
            test.eq(observed.sends[1].pid, "dataflow.overseer")
            test.eq(observed.sends[1].topic, "dataflow.activation.changed")
        end)
    end)
end

local function run_durable_tests()
    test.describe("Dataflow overseer on durable ownership records", function()
        local originals
        local observed
        local created: { string } = {}
        -- The runtime's own overseer reconciles these rows too. Holding their
        -- canonical names keeps it to monitoring this process, while the
        -- overseer under test observes the scripted registry.
        local live_process = overseer.process

        local function now(): string
            return overseer.time.now():format(overseer.time.RFC3339NANO)
        end

        local function create_dataflow(): string
            local id = uuid.v7()
            local db = test.not_nil(select(1, overseer.sql.get("app:db"))) :: any
            local _, insert_err = overseer.sql.builder.insert("dataflows"):set_map({
                dataflow_id = id,
                actor_id = "overseer-durable-test",
                type = "overseer-durable-test",
                status = overseer.consts.STATUS.RUNNING,
                metadata = "{}",
                created_at = now(),
                updated_at = now(),
            }):run_with(db):exec()
            db:release()
            test.is_nil(insert_err)
            test.is_true(select(1, live_process.registry.register("dataflow." .. id)))
            table.insert(created, id)
            return id
        end

        local function request(id: string): number
            local activation = test.not_nil(select(1, overseer.commit.request_activation(
                id, {}, { notify = false }))) :: any
            return tonumber(activation.generation) or 0
        end

        local function admit(id: string, token: string, pid: string, runtime_epoch: string?)
            local admitted = test.not_nil(select(1, overseer.commit.admit_owner(id, 1, {
                token = token, pid = pid, runtime_epoch = runtime_epoch or CURRENT_EPOCH,
            }))) :: any
            test.is_true(admitted.admitted)
        end

        local function passivate(id: string, token: string, generation: number)
            local result = test.not_nil(select(1, overseer.commit.execute(id, nil, { {
                type = overseer.consts.COMMAND_TYPES.PASSIVATE_WORKFLOW,
                payload = {
                    activation_generation = generation,
                    owner = { token = token, phase = overseer.consts.OWNER_PHASE.RUNNING },
                },
            } }, { publish = false, owner_token = token }))) :: any
            test.is_true(result.results[1].released)
        end

        local function row(id: string): any
            return test.not_nil(select(1, overseer.activation_repo.get(id)))
        end

        test.before_each(function()
            originals = {
                process = overseer.process,
                execution_frame = overseer.execution_frame,
                commit = overseer.commit,
            }
            observed = captures()
            overseer.process = process_mock(observed)
            local durable_commit = overseer.commit
            overseer.commit = setmetatable({
                fail_activation = function(id, fence, failure)
                    local result, err = durable_commit.fail_activation(id, fence, failure)
                    table.insert(observed.failures, {
                        dataflow_id = id, fence = fence, failure = failure,
                        completed = result ~= nil and result.completed == true,
                    })
                    return result, err
                end,
            }, { __index = durable_commit })
            overseer.execution_frame = {
                reconstruct = function(actor_id)
                    table.insert(observed.reconstructions, { actor_id = actor_id })
                    return "restored:" .. tostring(actor_id), "scope", nil
                end,
            }
        end)

        test.after_each(function()
            for key, value in pairs(originals) do overseer[key] = value end
        end)

        test.after_all(function()
            local db = test.not_nil(select(1, overseer.sql.get("app:db"))) :: any
            for _, id in ipairs(created) do
                for _, table_name in ipairs({ "dataflow_wakes", "dataflow_activations", "dataflows" }) do
                    overseer.sql.builder.delete(table_name):where("dataflow_id = ?", id):run_with(db):exec()
                end
                live_process.registry.unregister("dataflow." .. id)
            end
            db:release()
        end)

        test.it("reads a pending wake deadline at full precision on the live dialect", function()
            local db = test.not_nil(select(1, overseer.sql.get("app:db"))) :: any
            local tx = test.not_nil(select(1, db:begin())) :: any
            local id = uuid.v7()
            local _, flow_err = overseer.sql.builder.insert("dataflows"):set_map({
                dataflow_id = id, actor_id = "overseer-durable-test", type = "overseer-durable-test",
                status = overseer.consts.STATUS.WAITING, metadata = "{}", created_at = now(), updated_at = now(),
            }):run_with(tx):exec()
            test.is_nil(flow_err)
            local _, wake_err = overseer.sql.builder.insert("dataflow_wakes"):set_map({
                dataflow_id = id, wake_key = "yield:precision", wake_at = "2099-01-01T00:00:00.123456Z",
            }):run_with(tx):exec()
            test.is_nil(wake_err)
            local db_type = select(1, tx:db_type())
            local placeholder = "?"
            if db_type == "postgres" then placeholder = "$1" end
            local rows, query_err = tx:query("SELECT " .. overseer.wake_at_column(db_type) ..
                " AS wake_at FROM dataflow_wakes WHERE dataflow_id = " .. placeholder, { id })
            tx:rollback()
            db:release()
            test.is_nil(query_err)
            local deadline = test.not_nil(select(1, overseer.time.parse(
                overseer.time.RFC3339NANO, tostring(rows[1].wake_at)))) :: any
            test.eq(deadline:utc():format(overseer.time.RFC3339NANO), "2099-01-01T00:00:00.123456Z")
        end)

        test.it("never cancels a name holder that a later request admits", function()
            local id = create_dataflow()
            request(id)
            admit(id, "t-old", "pid-old")
            passivate(id, "t-old", 1)
            observed.owners["dataflow." .. id] = "pid-successor"
            local runtime = overseer.new_runtime(CURRENT_EPOCH)
            test.is_true(select(1, overseer.reconcile(runtime, id)))

            test.eq(request(id), 2)
            admit(id, "t-successor", "pid-successor")
            test.is_true(select(1, overseer.reconcile(runtime, id)))
            test.eq(#observed.cancels, 0)
            test.eq(#observed.terminates, 0)
            test.eq(#observed.spawns, 0)
            test.eq(observed.sends[#observed.sends].pid, "pid-successor")
            test.eq(observed.sends[#observed.sends].payload.generation, 2)
            test.eq(row(id).owner_token, "t-successor")
            test.eq(row(id).owner_phase, "running")
        end)

        test.it("fails a dead running owner of this runtime at the latest request", function()
            local id = create_dataflow()
            request(id)
            admit(id, "t-dead", "pid-dead")
            test.eq(request(id), 2)
            local runtime = overseer.new_runtime(CURRENT_EPOCH)
            test.is_true(select(1, overseer.reconcile(runtime, id)))
            test.eq(#observed.spawns, 0)
            local failed = row(id)
            test.is_false(failed.desired_active)
            test.eq(failed.generation, 2)
            test.eq(failed.owner_phase, "released")
            local workflow = test.not_nil(select(1, overseer.dataflow_repo.get(id))) :: any
            test.eq(workflow.status, overseer.consts.STATUS.COMPLETED_FAILURE)
        end)

        test.it("replaces a released owner or an owner of an earlier runtime exactly once", function()
            local released = create_dataflow()
            request(released)
            admit(released, "t-released", "pid-released")
            passivate(released, "t-released", 1)
            test.eq(request(released), 2)
            local rebooted = create_dataflow()
            request(rebooted)
            admit(rebooted, "t-rebooted", "pid-rebooted", "runtime-before")

            local runtime = overseer.new_runtime(CURRENT_EPOCH)
            for _ = 1, 2 do
                test.is_true(select(1, overseer.reconcile(runtime, released)))
                test.is_true(select(1, overseer.reconcile(runtime, rebooted)))
            end
            test.eq(#observed.spawns, 2)
            test.eq(observed.spawns[1].args.activation_generation, 2)
            test.eq(observed.spawns[2].args.activation_generation, 1)
            test.eq(#observed.failures, 0)
        end)
    end)
end

return { run_tests = test.run_cases(function()
    run_tests()
    run_durable_tests()
end) }
