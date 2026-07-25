local test = require("test")
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
        claims = {},
        reconstructions = {},
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
            local pid = "pid-" .. tostring(args.dataflow_id) .. "-" .. tostring(args.activation_generation)
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

local function run_tests()
    test.describe("Dataflow overseer IO", function()
        local originals
        local observed
        local activations: { [string]: any } = {}
        local workflows: { [string]: any } = {}

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
                get = function(id) return activations[id], nil end,
                claim_epoch_tx = function(_tx, id, generation, observed_epoch, runtime_epoch)
                    local row = activations[id]
                    local matches = row and row.generation == generation and row.desired_active and
                        row.owner_epoch == observed_epoch
                    table.insert(observed.claims, {
                        dataflow_id = id,
                        generation = generation,
                        observed_epoch = observed_epoch,
                        runtime_epoch = runtime_epoch,
                        claimed = matches == true,
                    })
                    if matches then row.owner_epoch = runtime_epoch end
                    if not row then return nil, "activation missing" end
                    local result = {}
                    for key, value in pairs(row) do result[key] = value end
                    result.claimed = matches == true
                    return result, nil
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
                fail_activation = function(id, generation, failure)
                    table.insert(observed.failures, {
                        dataflow_id = id,
                        generation = generation,
                        failure = failure,
                    })
                    activations[id].desired_active = false
                    workflows[id].status = overseer.consts.STATUS.COMPLETED_FAILURE
                    return { completed = true, current_generation = generation }, nil
                end,
            }
            overseer.with_tx = function(fn) return fn({}) end
            overseer.pending_due = function() return {}, nil end
        end)

        test.after_each(function()
            for key, value in pairs(originals) do overseer[key] = value end
        end)

        test.it("recovers each durable boot activation once under its frozen actor and scope", function()
            activations.boot = activation("boot", 3, { init_func_id = "app:init" })
            activations.boot.owner_epoch = "runtime-before-restart"
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
            test.eq(#observed.claims, 1)
            test.eq(observed.claims[1].observed_epoch, "runtime-before-restart")

            local second, second_err = overseer.safety_reconcile(runtime)
            test.is_nil(second_err)
            test.eq(second, 1)
            test.eq(#observed.spawns, 1)
        end)

        test.it("adopts an existing canonical owner without reconstructing or spawning", function()
            activations.live = activation("live", 1)
            workflows.live = workflow("live")
            observed.owners["dataflow.live"] = "pid-existing"
            local ok, err = overseer.reconcile_activation(
                overseer.new_runtime(CURRENT_EPOCH), test.not_nil(activations.live) :: any)
            test.is_nil(err)
            test.is_true(ok)
            test.eq(#observed.spawns, 0)
            test.eq(#observed.reconstructions, 0)
            test.eq(#observed.claims, 1)
            test.eq(observed.claims[1].runtime_epoch, CURRENT_EPOCH)
            test.eq(observed.monitors[1], "pid-existing")
        end)

        test.it("accepts the idempotent monitor result from spawn_monitored", function()
            activations.monitored = activation("monitored", 1)
            workflows.monitored = workflow("monitored")
            overseer.process.monitor = function(pid)
                table.insert(observed.monitors, tostring(pid))
                return nil, "already monitoring pid"
            end

            local ok, err = overseer.reconcile_activation(
                overseer.new_runtime(CURRENT_EPOCH), activations.monitored)
            test.is_nil(err)
            test.is_true(ok)
            test.eq(#observed.spawns, 1)
            test.eq(#observed.failures, 0)
            test.eq(observed.monitors[1], observed.spawns[1].pid)
        end)

        test.it("fails a missing owner after an overseer-only restart in the same runtime epoch", function()
            activations.service_restart = activation("service_restart", 7)
            activations.service_restart.owner_epoch = CURRENT_EPOCH
            workflows.service_restart = workflow("service_restart")

            local ok, err = overseer.reconcile_activation(
                overseer.new_runtime(CURRENT_EPOCH),
                test.not_nil(activations.service_restart) :: any)
            test.is_nil(err)
            test.is_true(ok)
            test.eq(#observed.claims, 0)
            test.eq(#observed.spawns, 0)
            test.eq(#observed.failures, 1)
            test.eq(observed.failures[1].failure.reason, "same_runtime_owner_missing")
        end)

        test.it("terminalizes a runtime owner loss once and never respawns it", function()
            activations.crash = activation("crash", 4)
            workflows.crash = workflow("crash")
            local runtime = overseer.new_runtime(CURRENT_EPOCH)
            test.is_true(select(1, overseer.reconcile_activation(runtime, activations.crash)))
            local pid = observed.spawns[1].pid
            observed.owners["dataflow.crash"] = nil

            local handled, exit_err = overseer.handle_exit(runtime, {
                kind = overseer.process.event.EXIT,
                from = pid,
                result = { error = "executor panicked" },
            })
            test.is_nil(exit_err)
            test.is_true(handled)
            test.eq(#observed.failures, 1)
            test.eq(observed.failures[1].generation, 4)
            test.eq(observed.failures[1].failure.message, "executor panicked")
            test.eq(#observed.spawns, 1)

            local _, safety_err = overseer.safety_reconcile(runtime)
            test.is_nil(safety_err)
            test.eq(#observed.failures, 1)
            test.eq(#observed.spawns, 1)
        end)

        test.it("does not let a stale EXIT fail a newer activation generation", function()
            activations.race = activation("race", 1)
            workflows.race = workflow("race")
            local runtime = overseer.new_runtime(CURRENT_EPOCH)
            test.is_true(select(1, overseer.reconcile_activation(runtime, activations.race)))
            local old_pid = observed.spawns[1].pid

            activations.race = activation("race", 2)
            observed.owners["dataflow.race"] = "pid-new"
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

        test.it("fails an unreconstructable execution frame instead of root-spawning or retrying", function()
            activations.frame = activation("frame", 2)
            workflows.frame = workflow("frame")
            overseer.execution_frame = {
                reconstruct = function() return nil, nil, "policy no longer exists" end,
            }
            local ok, err = overseer.reconcile_activation(
                overseer.new_runtime(CURRENT_EPOCH), test.not_nil(activations.frame) :: any)
            test.is_nil(err)
            test.is_true(ok)
            test.eq(#observed.spawns, 0)
            test.eq(#observed.failures, 1)
            test.contains(observed.failures[1].failure.message, "policy no longer exists")
        end)

        test.it("stops a monitored process after durable cancellation", function()
            activations.cancelled = activation("cancelled", 5)
            workflows.cancelled = workflow("cancelled")
            local runtime = overseer.new_runtime(CURRENT_EPOCH)
            test.is_true(select(1, overseer.reconcile_activation(runtime, activations.cancelled)))
            local pid = observed.spawns[1].pid
            activations.cancelled.desired_active = false
            workflows.cancelled.status = overseer.consts.STATUS.CANCELLED

            local ok, err = overseer.reconcile_activation(
                runtime, test.not_nil(activations.cancelled) :: any,
                test.not_nil(workflows.cancelled) :: any)
            test.is_nil(err)
            test.is_true(ok)
            test.eq(#observed.cancels, 1)
            test.eq(observed.cancels[1].pid, pid)
            test.eq(#observed.failures, 0)
        end)

        test.it("promotes an exact due wake once and nudges the acquired owner", function()
            local due = activation("due", 6)
            due.promoted = true
            activations.due = due
            workflows.due = workflow("due")
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
            test.eq(#observed.spawns, 1)
            test.eq(#observed.sends, 1)
            test.eq(observed.sends[1].topic, overseer.consts.MESSAGE_TOPIC.WAKE)
            test.eq(observed.sends[1].payload.wake_key, "yield:one")
            test.eq(observed.sends[1].payload.generation, 6)
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

return { run_tests = test.run_cases(run_tests) }
