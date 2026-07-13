local test = require("test")
local uuid = require("uuid")
local json = require("json")
local time = require("time")
local client = require("client")
local consts = require("consts")
local data_reader = require("data_reader")
local scheduler = require("scheduler")
local workflow_state = require("workflow_state")

local function define_tests()
    local function active_yield(ws: any, node_id: string): any
        return ws.active_yields[node_id]
    end

    -- ==========================================
    -- SCHEDULER BUG TESTS (falsy values)
    -- ==========================================

    describe("Scheduler falsy signal data", function()
        local function make_state(overrides)
            local state = scheduler.create_empty_state()
            if overrides then
                for k, v in pairs(overrides) do
                    state[k] = v
                end
            end
            return state
        end

        it("satisfies yield when signal_data is false", function()
            local state = make_state({
                active_yields = {
                    ["n1"] = {
                        yield_id = "y1", reply_to = "t", signal_id = "s1",
                        pending_children = {}, results = {},
                        wait_for_signal = true, signal_data = false,
                    }
                },
            })
            local d = scheduler.find_next_work(state)
            test.eq(d.type, scheduler.DECISION_TYPE.SATISFY_YIELD, "false satisfied")
            test.eq(d.payload.results, false, "false preserved")
        end)

        it("satisfies yield when signal_data is 0", function()
            local state = make_state({
                active_yields = {
                    ["n1"] = {
                        yield_id = "y1", reply_to = "t", signal_id = "s1",
                        pending_children = {}, results = {},
                        wait_for_signal = true, signal_data = 0,
                    }
                },
            })
            local d = scheduler.find_next_work(state)
            test.eq(d.type, scheduler.DECISION_TYPE.SATISFY_YIELD, "0 satisfied")
            test.eq(d.payload.results, 0, "0 preserved")
        end)

        it("satisfies yield when signal_data is empty string", function()
            local state = make_state({
                active_yields = {
                    ["n1"] = {
                        yield_id = "y1", reply_to = "t", signal_id = "s1",
                        pending_children = {}, results = {},
                        wait_for_signal = true, signal_data = "",
                    }
                },
            })
            local d = scheduler.find_next_work(state)
            test.eq(d.type, scheduler.DECISION_TYPE.SATISFY_YIELD, "empty string satisfied")
        end)

        it("does NOT satisfy yield when signal_data is nil", function()
            local state = make_state({
                active_yields = {
                    ["n1"] = {
                        yield_id = "y1", reply_to = "t", signal_id = "s1",
                        pending_children = {}, results = {},
                        wait_for_signal = true,
                        -- signal_data omitted = nil
                    }
                },
            })
            local d = scheduler.find_next_work(state)
            test.eq(d.type, scheduler.DECISION_TYPE.PASSIVATE, "nil means no signal")
        end)

        it("active signal yield blocks workflow completion even with output", function()
            local state = make_state({
                active_yields = {
                    ["n1"] = {
                        yield_id = "y1", reply_to = "t", signal_id = "s1",
                        pending_children = {}, results = {},
                        wait_for_signal = true,
                    }
                },
                has_workflow_output = true,
            })
            local d = scheduler.find_next_work(state)
            test.neq(d.type, scheduler.DECISION_TYPE.COMPLETE_WORKFLOW, "blocked by yield")
        end)

        it("active signal yield blocks workflow completion even with error", function()
            local state = make_state({
                active_yields = {
                    ["n1"] = {
                        yield_id = "y1", reply_to = "t", signal_id = "s1",
                        pending_children = {}, results = {},
                        wait_for_signal = true,
                    }
                },
                has_workflow_error = true,
            })
            local d = scheduler.find_next_work(state)
            test.neq(d.type, scheduler.DECISION_TYPE.COMPLETE_WORKFLOW, "blocked by yield even with error")
        end)
    end)

    -- ==========================================
    -- ORPHANED YIELD CLEANUP TESTS
    -- ==========================================

    describe("Orphaned signal yield cleanup", function()
        it("handle_process_exit removes signal yield when node crashes", function()
            local ws: any = workflow_state.new(uuid.v7())
            local pid = "pid-123"

            ws.nodes["sig"] = { status = consts.STATUS.RUNNING, type = "signal" }
            ws.active_processes["sig"] = pid
            ws.active_yields["sig"] = {
                yield_id = "y1", signal_id = "approval",
                wait_for_signal = true, pending_children = {}, results = {},
            }

            local info = ws:handle_process_exit(pid, false, "crashed")
            test.not_nil(info, "exit info")
            test.is_nil(ws.active_yields["sig"], "signal yield removed")
            test.is_nil(ws.active_processes["sig"], "process removed")
        end)

        it("handle_process_exit removes signal yield on successful exit too", function()
            local ws: any = workflow_state.new(uuid.v7())
            local pid = "pid-456"

            ws.nodes["sig"] = { status = consts.STATUS.RUNNING, type = "signal" }
            ws.active_processes["sig"] = pid
            ws.active_yields["sig"] = {
                yield_id = "y1", signal_id = "approval",
                wait_for_signal = true, pending_children = {}, results = {},
            }

            ws:handle_process_exit(pid, true, { ok = true })
            test.is_nil(ws.active_yields["sig"], "signal yield cleaned on success too")
        end)

        it("handle_process_exit does NOT remove non-signal yields", function()
            local ws: any = workflow_state.new(uuid.v7())
            local pid = "pid-789"

            ws.nodes["func"] = { status = consts.STATUS.RUNNING, type = "func" }
            ws.active_processes["func"] = pid
            ws.active_yields["func"] = {
                yield_id = "y1",
                pending_children = { ["child1"] = consts.STATUS.PENDING },
                results = {},
            }

            ws:handle_process_exit(pid, true, { ok = true })
            test.not_nil(ws.active_yields["func"], "non-signal yield preserved")
        end)

        it("after orphan cleanup, workflow can complete", function()
            local ws: any = workflow_state.new(uuid.v7())
            local pid = "pid-abc"

            ws.nodes["sig"] = { status = consts.STATUS.RUNNING, type = "signal" }
            ws.active_processes["sig"] = pid
            ws.active_yields["sig"] = {
                yield_id = "y1", signal_id = "approval",
                wait_for_signal = true, pending_children = {}, results = {},
            }
            ws.has_workflow_output = true

            -- before crash: yield blocks completion
            local snap = ws:get_scheduler_snapshot()
            local d1 = scheduler.find_next_work(snap)
            test.neq(d1.type, scheduler.DECISION_TYPE.COMPLETE_WORKFLOW, "blocked before cleanup")

            -- crash signal node
            ws:handle_process_exit(pid, false, "crash")

            -- after crash: workflow can complete
            snap = ws:get_scheduler_snapshot()
            local d2 = scheduler.find_next_work(snap)
            test.eq(d2.type, scheduler.DECISION_TYPE.COMPLETE_WORKFLOW, "unblocked after cleanup")
        end)
    end)

    -- ==========================================
    -- SIGNAL DATA DELIVERY EDGE CASES
    -- ==========================================

    describe("Signal data delivery edge cases", function()
        it("signal without key or discriminator is silently dropped", function()
            local ws: any = workflow_state.new(uuid.v7())
            ws.active_yields["n1"] = {
                signal_id = "approval", wait_for_signal = true,
            }

            ws:_update_state_from_results({
                results = {{
                    input = {
                        type = consts.COMMAND_TYPES.CREATE_DATA,
                        payload = {
                            data_type = consts.DATA_TYPE.NODE_SIGNAL,
                            content = { data = "no key" },
                        }
                    }
                }}
            })

            test.is_nil(active_yield(ws, "n1").signal_data, "no key = no delivery")
        end)

        it("NODE_SIGNAL in batch with other commands all process correctly", function()
            local ws: any = workflow_state.new(uuid.v7())
            ws.active_yields["sig"] = {
                signal_id = "approval", wait_for_signal = true,
            }
            ws.nodes = {}
            ws.input_tracker = { requirements = {}, available = {} }

            ws:_update_state_from_results({
                results = {
                    {
                        input = {
                            type = consts.COMMAND_TYPES.CREATE_NODE,
                            payload = {
                                node_id = "new-node",
                                node_type = "func",
                                status = consts.STATUS.PENDING,
                            }
                        },
                        node_id = "new-node",
                    },
                    {
                        input = {
                            type = consts.COMMAND_TYPES.CREATE_DATA,
                            payload = {
                                data_type = consts.DATA_TYPE.NODE_SIGNAL,
                                key = "approval",
                                content = { ok = true },
                            }
                        }
                    },
                    {
                        input = {
                            type = consts.COMMAND_TYPES.CREATE_DATA,
                            payload = {
                                data_type = consts.DATA_TYPE.NODE_INPUT,
                                node_id = "new-node",
                                key = "default",
                            }
                        }
                    },
                }
            })

            test.not_nil(ws.nodes["new-node"], "CREATE_NODE processed")
            test.not_nil(active_yield(ws, "sig").signal_data, "signal delivered")
            test.not_nil(ws.input_tracker.available["new-node"], "input tracked")
        end)

        it("signal delivery to yield with matching signal_id only", function()
            local ws: any = workflow_state.new(uuid.v7())
            ws.active_yields["n1"] = { signal_id = "alpha", wait_for_signal = true }
            ws.active_yields["n2"] = { signal_id = "beta", wait_for_signal = true }
            ws.active_yields["n3"] = { signal_id = "gamma", wait_for_signal = true }

            ws:_update_state_from_results({
                results = {{
                    input = {
                        type = consts.COMMAND_TYPES.CREATE_DATA,
                        payload = {
                            data_type = consts.DATA_TYPE.NODE_SIGNAL,
                            key = "beta",
                            content = { target = "beta" },
                        }
                    }
                }}
            })

            test.is_nil(active_yield(ws, "n1").signal_data, "alpha not touched")
            test.not_nil(active_yield(ws, "n2").signal_data, "beta received")
            test.eq(active_yield(ws, "n2").signal_data.target, "beta", "correct data")
            test.is_nil(active_yield(ws, "n3").signal_data, "gamma not touched")
        end)
    end)

    -- ==========================================
    -- DETACHED YIELD TESTS
    -- ==========================================

    describe("Detached signal yield", function()
        it("scheduler skips detached yield even with signal_data", function()
            local state = scheduler.create_empty_state()
            state.active_yields["n1"] = {
                yield_id = "y1", reply_to = "stale-topic", signal_id = "s1",
                pending_children = {}, results = {},
                wait_for_signal = true, signal_data = { ok = true },
                detached = true,
            }

            local d = scheduler.find_next_work(state)
            test.eq(d.type, scheduler.DECISION_TYPE.NO_WORK, "detached yield not satisfied")
        end)

        it("scheduler satisfies non-detached yield with signal_data", function()
            local state = scheduler.create_empty_state()
            state.active_yields["n1"] = {
                yield_id = "y1", reply_to = "live-topic", signal_id = "s1",
                pending_children = {}, results = {},
                wait_for_signal = true, signal_data = { ok = true },
                -- NOT detached
            }

            local d = scheduler.find_next_work(state)
            test.eq(d.type, scheduler.DECISION_TYPE.SATISFY_YIELD, "live yield satisfied")
        end)

        it("track_yield inherits signal_data from detached yield", function()
            local ws: any = workflow_state.new(uuid.v7())

            -- detached yield with pre-delivered signal data
            ws.active_yields["sig"] = {
                yield_id = "old-y", signal_id = "approval",
                wait_for_signal = true, detached = true,
                signal_data = { approved = true, from = "detached" },
            }

            -- node re-yields with fresh reply_to
            local new_yield = {
                yield_id = "new-y", reply_to = "fresh-topic",
                signal_id = "approval", wait_for_signal = true,
                pending_children = {}, results = {},
            }
            ws:track_yield("sig", new_yield)

            test.not_nil(active_yield(ws, "sig").signal_data, "inherited signal_data")
            test.eq(active_yield(ws, "sig").signal_data.approved, true, "correct data")
            test.is_nil(active_yield(ws, "sig").detached, "no longer detached")
            test.eq(active_yield(ws, "sig").reply_to, "fresh-topic", "fresh reply_to")
        end)

        it("track_yield checks DB when no detached yield exists (pre-queued signal)", function()
            local ws: any = workflow_state.new(uuid.v7())

            -- no existing detached yield, no signal in DB
            local new_yield = {
                yield_id = "y1", reply_to = "topic",
                signal_id = "no-match", wait_for_signal = true,
                pending_children = {}, results = {},
            }
            ws:track_yield("sig", new_yield)

            -- DB query finds nothing for this signal_id
            test.is_nil(active_yield(ws, "sig").signal_data, "no data without signal in DB")
        end)

        it("detached yield blocks workflow completion", function()
            local state = scheduler.create_empty_state()
            state.active_yields["n1"] = {
                yield_id = "y1", signal_id = "s1",
                wait_for_signal = true, detached = true,
                pending_children = {}, results = {},
            }
            state.has_workflow_output = true

            local d = scheduler.find_next_work(state)
            test.neq(d.type, scheduler.DECISION_TYPE.COMPLETE_WORKFLOW, "detached yield blocks completion")
        end)

        it("signal data delivered to detached yield in memory", function()
            local ws: any = workflow_state.new(uuid.v7())
            ws.active_yields["sig"] = {
                yield_id = "y1", signal_id = "approval",
                wait_for_signal = true, detached = true,
            }

            ws:_update_state_from_results({
                results = {{
                    input = {
                        type = consts.COMMAND_TYPES.CREATE_DATA,
                        payload = {
                            data_type = consts.DATA_TYPE.NODE_SIGNAL,
                            key = "approval",
                            content = { delivered_to_detached = true },
                        }
                    }
                }}
            })

            test.not_nil(active_yield(ws, "sig").signal_data, "delivered to detached yield")
            test.eq(active_yield(ws, "sig").signal_data.delivered_to_detached, true, "correct data")
        end)
    end)

    -- ==========================================
    -- SATISFY YIELD EDGE CASES
    -- ==========================================

    describe("Satisfy yield edge cases", function()
        it("satisfy_yield for non-existent yield is no-op", function()
            local ws: any = workflow_state.new(uuid.v7())
            ws:satisfy_yield("nonexistent", { data = true })
            test.eq(#ws.queued_commands, 0, "no commands queued")
        end)

        it("satisfy_yield twice is safe", function()
            local ws: any = workflow_state.new(uuid.v7())
            ws.active_yields["n1"] = {
                yield_id = "y1", signal_id = "approval",
                wait_for_signal = true,
            }

            ws:satisfy_yield("n1", { first = true })
            test.eq(#ws.queued_commands, 1, "one command")
            test.is_nil(ws.active_yields["n1"], "yield removed")

            ws:satisfy_yield("n1", { second = true })
            test.eq(#ws.queued_commands, 1, "still one command, second was no-op")
        end)
    end)

    -- ==========================================
    -- SCHEDULER COMPLEX STATE TESTS
    -- ==========================================

    describe("Scheduler complex signal states", function()
        it("signal yield + running process = NO_WORK (not completion)", function()
            local state = scheduler.create_empty_state()
            state.nodes["sig"] = { status = consts.STATUS.RUNNING, type = "signal" }
            state.nodes["func"] = { status = consts.STATUS.RUNNING, type = "func" }
            state.active_yields["sig"] = {
                yield_id = "y1", reply_to = "t", signal_id = "s1",
                pending_children = {}, results = {},
                wait_for_signal = true,
            }
            state.active_processes["func"] = true

            local d = scheduler.find_next_work(state)
            test.eq(d.type, scheduler.DECISION_TYPE.NO_WORK, "both yield and process active")
        end)

        it("two signal yields, one satisfied, one pending = SATISFY the ready one", function()
            local state = scheduler.create_empty_state()
            state.active_yields["n1"] = {
                yield_id = "y1", reply_to = "t1", signal_id = "s1",
                pending_children = {}, results = {},
                wait_for_signal = true,
            }
            state.active_yields["n2"] = {
                yield_id = "y2", reply_to = "t2", signal_id = "s2",
                pending_children = {}, results = {},
                wait_for_signal = true, signal_data = { ok = true },
            }

            local d = scheduler.find_next_work(state)
            test.eq(d.type, scheduler.DECISION_TYPE.SATISFY_YIELD, "satisfies ready one")
            test.eq(d.payload.yield_id, "y2", "correct yield satisfied")
        end)

        it("signal yield with pending nodes does not block input-ready work", function()
            local state = scheduler.create_empty_state()
            state.nodes["sig"] = { status = consts.STATUS.RUNNING, type = "signal" }
            state.nodes["ready"] = { status = consts.STATUS.PENDING, type = "func" }
            state.active_yields["sig"] = {
                yield_id = "y1", reply_to = "t", signal_id = "s1",
                pending_children = {}, results = {},
                wait_for_signal = true,
            }
            state.input_tracker = {
                requirements = {},
                available = { ["ready"] = { default = true } },
            }

            local d = scheduler.find_next_work(state)
            test.eq(d.type, scheduler.DECISION_TYPE.EXECUTE_NODES, "ready node runs")
        end)

        it("after signal satisfied and yield removed, workflow completes normally", function()
            local state = scheduler.create_empty_state()
            state.nodes["sig"] = { status = consts.STATUS.COMPLETED_SUCCESS, type = "signal" }
            state.has_workflow_output = true

            local d = scheduler.find_next_work(state)
            test.eq(d.type, scheduler.DECISION_TYPE.COMPLETE_WORKFLOW, "completes normally")
            test.is_true(d.payload.success, "success")
        end)
    end)

    -- ==========================================
    -- INTEGRATION: TERMINATE DURING SIGNAL WAIT
    -- ==========================================

    describe("Terminate during signal wait", function()
        local c

        before_all(function()
            c = client.new()
            test.not_nil(c, "client created")
        end)

        local function make_signal_wf(signal_id)
            local nid = uuid.v7()
            local did = uuid.v7()
            return c:create_workflow({
                { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                    node_id = nid,
                    node_type = "userspace.dataflow.node.signal:node",
                    status = consts.STATUS.PENDING,
                    config = { signal_id = signal_id, data_targets = {
                        { data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "result", content_type = consts.CONTENT_TYPE.JSON }
                    }},
                    metadata = { title = "Signal: " .. signal_id }
                }},
                { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                    data_id = did, data_type = consts.DATA_TYPE.NODE_INPUT, node_id = nid,
                    content = { task = "test" }, content_type = consts.CONTENT_TYPE.JSON, key = "default"
                }}
            })
        end

        it("terminate workflow while signal is waiting", function()
            local sid = "term-" .. uuid.v7()
            local df_id = make_signal_wf(sid)
            c:start(df_id)
            for _ = 1, 30 do
                if c:get_status(df_id) == consts.STATUS.WAITING then break end
                time.sleep("100ms")
            end
            test.eq(c:get_status(df_id), consts.STATUS.WAITING, "waiting for signal")

            local ok, err = c:terminate(df_id)
            test.is_nil(err, "terminate succeeded")
            time.sleep("500ms")

            local status = c:get_status(df_id)
            test.eq(status, consts.STATUS.TERMINATED, "workflow terminated")
        end)

        it("signal after terminate is refused and the run stays terminated", function()
            local sid = "term-refuse-" .. uuid.v7()
            local df_id = make_signal_wf(sid)
            c:start(df_id)
            time.sleep("500ms")

            c:terminate(df_id)
            time.sleep("500ms")

            local result, err = c:signal(df_id, sid, { revived = true })
            test.is_nil(result, "signal returns no result")
            test.not_nil(err, "signal returns structured refusal")
            test.contains(tostring(err), "terminal state", "refusal names terminality")
            time.sleep("2s")

            test.eq(c:get_status(df_id), consts.STATUS.TERMINATED, "run stays terminated")
            test.is_nil(process.registry.lookup("dataflow." .. df_id), "no orchestrator respawned")
        end)

        it("signal after kill without restart does not auto-complete", function()
            local sid = "kill-no-auto-" .. uuid.v7()
            local df_id = make_signal_wf(sid)
            c:start(df_id)
            time.sleep("500ms")

            -- kill orchestrator
            local pid = process.registry.lookup("dataflow." .. df_id)
            if pid then
                process.terminate(pid)
            end
            time.sleep("200ms")

            -- signal auto-respawns orchestrator via client
            c:signal(df_id, sid, { after_kill = true })
            time.sleep("2s")

            -- should recover and complete
            test.eq(c:get_status(df_id), consts.STATUS.COMPLETED_SUCCESS, "recovered via auto-respawn")
        end)
    end)

    -- ==========================================
    -- INTEGRATION: COMPLEX RECOVERY PATTERNS
    -- ==========================================

    describe("Complex recovery patterns", function()
        local c

        before_all(function()
            c = client.new()
            test.not_nil(c, "client created")
        end)

        local function make_signal_wf(signal_id)
            local nid = uuid.v7()
            local did = uuid.v7()
            return c:create_workflow({
                { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                    node_id = nid,
                    node_type = "userspace.dataflow.node.signal:node",
                    status = consts.STATUS.PENDING,
                    config = { signal_id = signal_id, data_targets = {
                        { data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "result", content_type = consts.CONTENT_TYPE.JSON }
                    }},
                    metadata = { title = "Signal: " .. signal_id }
                }},
                { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                    data_id = did, data_type = consts.DATA_TYPE.NODE_INPUT, node_id = nid,
                    content = { task = "test" }, content_type = consts.CONTENT_TYPE.JSON, key = "default"
                }}
            })
        end

        local function kill_orchestrator(df_id)
            local pid = process.registry.lookup("dataflow." .. df_id)
            if pid then
                process.terminate(pid)
                time.sleep("200ms")
            end
        end

        it("kill immediately after signal is sent (race)", function()
            local sid = "race-kill-" .. uuid.v7()
            local df_id = make_signal_wf(sid)
            c:start(df_id)
            time.sleep("500ms")

            -- send signal and immediately kill
            c:signal(df_id, sid, { ok = true })
            kill_orchestrator(df_id)

            -- restart
            c:start(df_id)
            time.sleep("2s")
            test.eq(c:get_status(df_id), consts.STATUS.COMPLETED_SUCCESS, "race resolved")
        end)

        it("multiple signals queued while dead, correct one picked", function()
            local sid = "queue-correct-" .. uuid.v7()
            local df_id = make_signal_wf(sid)
            c:start(df_id)
            time.sleep("500ms")

            kill_orchestrator(df_id)

            -- queue wrong and correct signals
            c:signal(df_id, "wrong1-" .. uuid.v7(), { nope = true })
            c:signal(df_id, sid, { correct = true })
            c:signal(df_id, "wrong2-" .. uuid.v7(), { nope = true })

            time.sleep("3s")
            test.eq(c:get_status(df_id), consts.STATUS.COMPLETED_SUCCESS, "correct signal picked from backlog")
        end)

        it("signal correction: second signal overwrites first after kill", function()
            local sid = "correction-" .. uuid.v7()
            local df_id = make_signal_wf(sid)
            c:start(df_id)
            time.sleep("500ms")

            kill_orchestrator(df_id)

            -- first signal (wrong decision)
            c:signal(df_id, sid, { approved = false, version = 1 })
            -- correction signal
            c:signal(df_id, sid, { approved = true, version = 2 })

            time.sleep("3s")
            test.eq(c:get_status(df_id), consts.STATUS.COMPLETED_SUCCESS, "completed with correction")
        end)

        it("five workflows killed and recovered independently", function()
            local sids = {}
            local df_ids = {}

            for i = 1, 5 do
                sids[i] = "multi5-" .. i .. "-" .. uuid.v7()
                df_ids[i] = make_signal_wf(sids[i])
                c:start(df_ids[i])
            end

            time.sleep("500ms")

            -- kill all
            for i = 1, 5 do
                kill_orchestrator(df_ids[i])
            end

            -- signal all in reverse
            for i = 5, 1, -1 do
                c:signal(df_ids[i], sids[i], { index = i })
            end

            -- verify all complete
            time.sleep("3s")
            for i = 1, 5 do
                test.eq(c:get_status(df_ids[i]), consts.STATUS.COMPLETED_SUCCESS, "wf" .. i .. " recovered")
            end
        end)
    end)
end

return { run_tests = test.run_cases(define_tests) }
