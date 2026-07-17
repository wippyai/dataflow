local test = require("test")
local wake_process = require("wake_process")

local function activation(id, generation, launch_args)
    return {
        dataflow_id = id,
        generation = generation,
        desired_active = true,
        launch_args = launch_args or {},
    }
end

local function workflow(id, status)
    return {
        dataflow_id = id,
        actor_id = "actor:" .. id,
        actor_context = { kind = "test" },
        status = status or wake_process.consts.STATUS.RUNNING,
    }
end

local function process_error(kind, message)
    return setmetatable({
        kind = function() return kind end,
    }, {
        __tostring = function() return message end,
    })
end

local function process_mock(captured, lookup)
    local mock = {
        event = { EXIT = "EXIT", LINK_DOWN = "LINK_DOWN", CANCEL = "CANCEL" },
        registry = {
            lookup = function(name)
                table.insert(captured.lookups, name)
                if lookup then return lookup(name) end
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
    }
    mock.with_context = function(context)
        captured.context = context
        local spawner = {}
        function spawner:with_actor(actor)
            captured.actor = actor
            return self
        end
        function spawner:with_scope(scope)
            captured.scope = scope
            return self
        end
        function spawner:with_name(name)
            captured.name = name
            return self
        end
        function spawner:spawn_monitored(source, host, args)
            table.insert(captured.spawns, { source = source, host = host, args = args })
            return "pid-" .. tostring(args.dataflow_id), nil
        end
        return spawner
    end
    return mock
end

local function captures(): any
    return { lookups = {}, monitors = {}, spawns = {}, sends = {} }
end

local function run_tests()
    test.describe("Dataflow overseer IO", function()
        local originals

        test.before_each(function()
            originals = {
                activation_repo = wake_process.activation_repo,
                dataflow_repo = wake_process.dataflow_repo,
                execution_frame = wake_process.execution_frame,
                process = wake_process.process,
                sql = wake_process.sql,
                with_tx = wake_process.with_tx,
                pending_due = wake_process.pending_due,
            }
            wake_process.execution_frame = {
                reconstruct = function(actor_id, _context)
                    return { id = actor_id }, { policies = {} }, nil
                end,
            }
            wake_process.with_tx = function(fn) return fn({}) end
            wake_process.pending_due = function() return {}, nil end
        end)

        test.after_each(function()
            for key, value in pairs(originals) do wake_process[key] = value end
        end)

        test.it("boot recovers legacy RUNNING only and leaves bare PENDING inert", function()
            local recovered = {}
            local active = activation("running", 1, { init_func_id = "app:init" })
            local observed = captures()
            wake_process.dataflow_repo = {
                list_non_terminal = function()
                    return {
                        workflow("pending", wake_process.consts.STATUS.PENDING),
                        workflow("running", wake_process.consts.STATUS.RUNNING),
                    }, nil
                end,
                get = function(id) return workflow(id), nil end,
            }
            wake_process.activation_repo = {
                ensure_running_recovery_tx = function(_tx, id)
                    table.insert(recovered, id)
                    return active, nil
                end,
                list_active = function() return { active }, nil end,
                get = function() return active, nil end,
            }
            wake_process.process = process_mock(observed)

            local runtime = wake_process.new_runtime()
            local count, err = wake_process.bootstrap(runtime)
            test.is_nil(err)
            test.eq(count, 1)
            test.eq(#recovered, 1)
            test.eq(recovered[1], "running")
            test.eq(#observed.spawns, 1)
            test.eq(observed.spawns[1].source, wake_process.consts.ORCHESTRATOR)
            test.eq(observed.spawns[1].host, wake_process.consts.HOST_ID)
            test.eq(observed.spawns[1].args.dataflow_id, "running")
            test.eq(observed.spawns[1].args.activation_generation, 1)
            test.eq(observed.spawns[1].args.init_func_id, "app:init")
            test.eq(observed.name, "dataflow.running")
            test.eq(observed.monitors[1], "pid-running")
        end)

        test.it("safety reconciliation recovers a lost notification and duplicates do not respawn", function()
            local active = activation("lost", 7)
            local observed = captures()
            wake_process.dataflow_repo = { get = function(id) return workflow(id), nil end }
            wake_process.activation_repo = {
                list_active = function() return { active }, nil end,
                get = function() return active, nil end,
            }
            wake_process.process = process_mock(observed)
            local runtime = wake_process.new_runtime()
            runtime.boot_recovered = true

            local first, first_err = wake_process.safety_reconcile(runtime)
            local second, second_err = wake_process.handle_notification(runtime, {
                dataflow_id = "lost",
                generation = 7,
            })
            test.is_nil(first_err)
            test.is_nil(second_err)
            test.eq(first, 1)
            test.is_true(second)
            test.eq(#observed.spawns, 1)
            test.eq(#observed.monitors, 1)
        end)

        test.it("promotes each exact due wake transactionally and ignores an already promoted duplicate", function()
            local observed = captures()
            local promoted = activation("due", 3)
            promoted.promoted = true
            local calls = 0
            wake_process.dataflow_repo = { get = function(id) return workflow(id), nil end }
            wake_process.activation_repo = {
                activate_due_tx = function(_tx, id, key)
                    calls = calls + 1
                    test.eq(id, "due")
                    test.eq(key, "yield:one")
                    if calls == 1 then return promoted, nil end
                    return { promoted = false, already_promoted = true, generation = 3 }, nil
                end,
                get = function() return promoted, nil end,
            }
            wake_process.pending_due = function(_now, limit)
                    test.eq(limit, 100)
                    return { { dataflow_id = "due", wake_key = "yield:one" } }, nil
                end
            wake_process.process = process_mock(observed)
            local runtime = wake_process.new_runtime()

            local first, first_err = wake_process.promote_due(runtime)
            local second, second_err = wake_process.promote_due(runtime)
            test.is_nil(first_err)
            test.is_nil(second_err)
            test.eq(first, 1)
            test.eq(second, 0)
            test.eq(#observed.spawns, 1)
        end)

        test.it("nudges one canonical live owner for a due generation without spawning", function()
            local observed = captures()
            local promoted = activation("live", 5)
            promoted.promoted = true
            wake_process.dataflow_repo = { get = function(id) return workflow(id), nil end }
            wake_process.activation_repo = {
                activate_due_tx = function() return promoted, nil end,
                get = function() return promoted, nil end,
            }
            wake_process.pending_due = function()
                return { {
                    dataflow_id = "live",
                    wake_key = "yield:due",
                    wake_at = "2026-07-17T00:00:00Z",
                } }, nil
            end
            wake_process.process = process_mock(observed, function() return "pid-live" end)

            local count, err = wake_process.promote_due(wake_process.new_runtime())
            test.is_nil(err)
            test.eq(count, 1)
            test.eq(#observed.spawns, 0)
            test.eq(#observed.monitors, 1)
            test.eq(#observed.sends, 1)
            test.eq(observed.sends[1].pid, "pid-live")
            test.eq(observed.sends[1].topic, wake_process.consts.MESSAGE_TOPIC.WAKE)
            test.eq(observed.sends[1].payload.generation, 5)
            test.eq(observed.sends[1].payload.wake_key, "yield:due")
        end)

        test.it("targets one durable activation without scanning every active flow", function()
            local current = activation("target", 9)
            local observed = captures()
            wake_process.dataflow_repo = { get = function(id) return workflow(id), nil end }
            wake_process.activation_repo = {
                get = function(id)
                    test.eq(id, "target")
                    return current, nil
                end,
                list_active = function() error("targeted hint must not scan all activations") end,
            }
            wake_process.process = process_mock(observed)

            local ok, err = wake_process.handle_notification(wake_process.new_runtime(), {
                dataflow_id = "target",
                generation = 9,
            })
            test.is_true(ok)
            test.is_nil(err)
            test.eq(#observed.spawns, 1)
        end)

        test.it("filters already-promoted wakes before the due batch limit", function()
            local query_seen
            wake_process.pending_due = originals.pending_due
            wake_process.sql = {
                type = { POSTGRES = "postgres" },
                get = function()
                    return {
                        type = function() return "sqlite", nil end,
                        query = function(_self, query, params)
                            query_seen = query
                            test.not_nil(params[1])
                            return {}, nil
                        end,
                        release = function() end,
                    }, nil
                end,
            }
            local rows, err = wake_process.pending_due("2026-07-17T00:00:00Z", 100)
            test.is_nil(err)
            test.eq(#rows, 0)
            test.contains(query_seen, "activation_generation IS NULL")
            test.contains(query_seen, "LIMIT 100")
            test.is_true(query_seen:find("activation_generation IS NULL", 1, true) <
                query_seen:find("LIMIT 100", 1, true))
        end)

        test.it("active EXIT uses bounded retry while durable inactive EXIT never restarts", function()
            local current = activation("crash", 2)
            local observed = captures()
            wake_process.dataflow_repo = { get = function(id) return workflow(id), nil end }
            wake_process.activation_repo = { get = function() return current, nil end }
            wake_process.process = process_mock(observed)
            local runtime = wake_process.new_runtime({ retry_base_ms = 1, retry_max_ms = 4 })
            local started, start_err = wake_process.reconcile_activation(runtime, current)
            test.is_true(started)
            test.is_nil(start_err)

            local handled, exit_err = wake_process.handle_exit(runtime, {
                kind = wake_process.process.event.EXIT,
                from = "pid-crash",
                result = { error = "boom" },
            })
            test.is_true(handled)
            test.is_nil(exit_err)
            test.not_nil(runtime.retries.crash)
            runtime.retries.crash.due_at = "2000-01-01T00:00:00Z"
            local restarted, retry_err = wake_process.run_due_retries(runtime)
            test.is_nil(retry_err)
            test.eq(restarted, 1)
            test.eq(#observed.spawns, 2)

            current = { dataflow_id = "crash", generation = 2, desired_active = false }
            local stopped, stopped_err = wake_process.handle_exit(runtime, {
                kind = wake_process.process.event.EXIT,
                from = "pid-crash",
                result = { value = { success = true } },
            })
            test.is_true(stopped)
            test.is_nil(stopped_err)
            test.is_nil(runtime.retries.crash)
            test.eq(#observed.spawns, 2)
        end)

        test.it("adopts and explicitly monitors the canonical owner returned by spawn-or-signal", function()
            local current = activation("owned", 4)
            local observed = captures()
            local lookup_count = 0
            wake_process.dataflow_repo = { get = function(id) return workflow(id), nil end }
            wake_process.activation_repo = { get = function() return current, nil end }
            wake_process.process = process_mock(observed, function()
                lookup_count = lookup_count + 1
                if lookup_count > 1 then return "pid-existing" end
                return nil
            end)

            local ok, err = wake_process.reconcile_activation(wake_process.new_runtime(), current)
            test.is_true(ok)
            test.is_nil(err)
            test.eq(observed.monitors[1], "pid-existing")
        end)

        test.it("backs off an operational owner lookup error instead of assuming absence", function()
            local current = activation("lookup-error", 1)
            local observed = captures()
            wake_process.dataflow_repo = { get = function(id) return workflow(id), nil end }
            wake_process.activation_repo = { get = function() return current, nil end }
            wake_process.process = process_mock(observed, function()
                return nil, process_error("Unavailable", "topology unavailable")
            end)
            local runtime = wake_process.new_runtime()

            local ok, err = wake_process.reconcile_activation(runtime, current)
            test.is_true(ok)
            test.is_nil(err)
            test.eq(#observed.spawns, 0)
            test.not_nil(runtime.retries["lookup-error"])
        end)

        test.it("treats missing activation and wake tables as readiness without hiding other failures", function()
            test.is_true(wake_process.schema_not_ready("no such table: dataflow_wakes"))
            test.is_true(wake_process.schema_not_ready('relation "dataflow_activations" does not exist'))
            test.is_false(wake_process.schema_not_ready("database connection lost"))

            wake_process.dataflow_repo = {
                list_non_terminal = function() return nil, "no such table: dataflow_activations" end,
            }
            local runtime = wake_process.new_runtime()
            local result, err = wake_process.bootstrap(runtime)
            test.is_nil(result)
            test.contains(err, "dataflow_activations")
            test.is_false(runtime.boot_recovered)
        end)

        test.it("keeps legacy wake notifications compatible with the canonical overseer", function()
            local sent
            wake_process.process = {
                send = function(name, topic, payload)
                    sent = { name = name, topic = topic, payload = payload }
                    return true, nil
                end,
            }
            local ok, err = wake_process.notify()
            test.is_true(ok)
            test.is_nil(err)
            test.eq(wake_process.NAME, "dataflow.overseer")
            test.eq(sent.name, "dataflow.wakes")
            test.eq(sent.topic, "dataflow.wake.changed")
        end)
    end)
end

return { run_tests = test.run_cases(run_tests) }
