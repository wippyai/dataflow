local test = require("test")
local uuid = require("uuid")
local json = require("json")
local sql = require("sql")
local time = require("time")
local consts = require("consts")

local function define_tests()
    test.describe("signal node", function()
        local db

        test.before_all(function()
            db = sql.get("app:db")
        end)

        test.after_all(function()
            if db then db:release() end
        end)

        test.describe("unit", function()
            test.it("creates signal node with config", function()
                local signal_mod = require("signal_node")
                test.not_nil(signal_mod, "module loaded")
                test.not_nil(signal_mod.run, "run method exists")
            end)
        end)
    end)

    -- ==========================================
    -- CLIENT SIGNAL METHOD
    -- ==========================================

    test.describe("client.signal", function()
        local client_mod = require("client")
        local security = require("security")

        local function make_mock_client(captured: any)
            local mock_commit = {
                submit = function(dataflow_id, op_id, commands)
                    captured.dataflow_id = dataflow_id
                    captured.op_id = op_id
                    captured.commands = commands
                    return { commit_id = "test-commit" }, nil
                end
            }
            local mock_security = {
                actor = function() return security.new_actor("test-user", {}) end,
                new_actor = security.new_actor,
            }
            return client_mod.new({
                dataflow_repo = {
                    get = function(dataflow_id)
                        return {
                            dataflow_id = dataflow_id,
                            actor_id = "test-user",
                            status = "running",
                        }, nil
                    end,
                },
                commit = mock_commit,
                data_reader = {},
                process = process,
                funcs = require("funcs"),
                security = mock_security,
            })
        end

        local function first_command(captured: any): any
            return captured.commands[1]
        end

        test.it("creates signal commit with correct structure", function()
            local captured = {}
            local client = make_mock_client(captured)

            local result, err = client:signal("df-123", "approval", { ok = true })
            test.is_nil(err, "no error")
            test.not_nil(result, "has result")
            test.eq(captured.dataflow_id, "df-123", "correct dataflow_id")
            test.eq(#captured.commands, 1, "one command")
            test.eq(first_command(captured).type, consts.COMMAND_TYPES.CREATE_DATA, "CREATE_DATA command")
            test.eq(first_command(captured).payload.data_type, consts.DATA_TYPE.NODE_SIGNAL, "NODE_SIGNAL type")
            test.eq(first_command(captured).payload.key, "approval", "signal_id as key")
            test.eq(first_command(captured).payload.content.ok, true, "signal data passed")
        end)

        test.it("rejects empty dataflow_id", function()
            local client = client_mod.new()
            local _, err = client:signal("", "test", {})
            test.not_nil(err, "should error on empty dataflow_id")
        end)

        test.it("rejects nil dataflow_id", function()
            local client = client_mod.new()
            local _, err = client:signal(nil, "test", {})
            test.not_nil(err, "should error on nil dataflow_id")
        end)

        test.it("rejects empty signal_id", function()
            local client = client_mod.new()
            local _, err = client:signal("some-id", "", {})
            test.not_nil(err, "should error on empty signal_id")
        end)

        test.it("rejects nil signal_id", function()
            local client = client_mod.new()
            local _, err = client:signal("some-id", nil, {})
            test.not_nil(err, "should error on nil signal_id")
        end)

        test.it("uses empty table when data is nil", function()
            local captured = {}
            local client = make_mock_client(captured)
            local _, err = client:signal("df-123", "sig-1", nil)
            test.is_nil(err, "no error")
            test.not_nil(first_command(captured).payload.content, "content is not nil")
        end)

        test.it("passes complex nested data", function()
            local captured = {}
            local client = make_mock_client(captured)
            local data = {
                user = { name = "alice", role = "admin" },
                decision = "approved",
                scores = { 10, 20, 30 },
                metadata = { timestamp = "2024-01-01", tags = { "urgent", "reviewed" } }
            }
            local _, err = client:signal("df-456", "review", data)
            test.is_nil(err, "no error")
            test.eq(first_command(captured).payload.content.user.name, "alice", "nested data preserved")
            test.eq(first_command(captured).payload.content.decision, "approved", "top-level data preserved")
        end)

        test.it("generates unique data_id per signal", function()
            local captured1 = {}
            local captured2 = {}
            local client1 = make_mock_client(captured1)
            local client2 = make_mock_client(captured2)
            client1:signal("df-1", "sig-1", {})
            client2:signal("df-1", "sig-1", {})
            test.neq(first_command(captured1).payload.data_id, first_command(captured2).payload.data_id, "unique data_ids")
        end)

        test.it("sets content_type to JSON", function()
            local captured = {}
            local client = make_mock_client(captured)
            client:signal("df-1", "sig-1", { x = 1 })
            test.eq(first_command(captured).payload.content_type, consts.CONTENT_TYPE.JSON, "JSON content type")
        end)
    end)

    -- ==========================================
    -- SCHEDULER SIGNAL YIELD TESTS
    -- ==========================================

    test.describe("scheduler signal yield", function()
        local scheduler = require("scheduler")

        local function make_state(overrides)
            local state = scheduler.create_empty_state()
            if overrides then
                for k, v in pairs(overrides) do
                    state[k] = v
                end
            end
            return state
        end

        local function make_signal_yield(signal_id, opts)
            opts = opts or {}
            return {
                yield_id = opts.yield_id or ("yield-" .. uuid.v7()),
                reply_to = opts.reply_to or "reply.topic",
                signal_id = signal_id,
                pending_children = opts.pending_children or {},
                results = opts.results or {},
                wait_for_signal = true,
                signal_data = opts.signal_data,
            }
        end

        local function make_normal_yield(opts)
            opts = opts or {}
            return {
                yield_id = opts.yield_id or ("yield-" .. uuid.v7()),
                reply_to = opts.reply_to or "reply.topic",
                pending_children = opts.pending_children or {},
                results = opts.results or {},
            }
        end

        test.it("does not satisfy signal yield without signal data", function()
            local state = make_state({
                active_yields = {
                    ["node-1"] = make_signal_yield("approval"),
                },
            })

            local decision = scheduler.find_next_work(state)
            test.not_nil(decision, "has decision")
            test.eq(decision.type, scheduler.DECISION_TYPE.NO_WORK, "NO_WORK when signal not arrived")
        end)

        test.it("satisfies signal yield when signal data arrives", function()
            local state = make_state({
                active_yields = {
                    ["node-1"] = make_signal_yield("approval", {
                        signal_data = { approved = true, reviewer = "alice" },
                    }),
                },
            })

            local decision = scheduler.find_next_work(state)
            test.not_nil(decision, "has decision")
            test.eq(decision.type, scheduler.DECISION_TYPE.SATISFY_YIELD, "satisfies yield")
            test.eq(decision.payload.results.approved, true, "signal data passed through")
            test.eq(decision.payload.results.reviewer, "alice", "all fields present")
        end)

        test.it("blocks workflow completion while signal yield active", function()
            local state = make_state({
                nodes = {
                    ["node-1"] = { status = consts.STATUS.RUNNING, type = "signal" },
                },
                active_yields = {
                    ["node-1"] = make_signal_yield("approval"),
                },
            })

            local decision = scheduler.find_next_work(state)
            test.neq(decision.type, scheduler.DECISION_TYPE.COMPLETE_WORKFLOW, "does not complete while signal pending")
        end)

        test.it("handles multiple signal yields - only satisfies the one with data", function()
            local state = make_state({
                active_yields = {
                    ["node-1"] = make_signal_yield("sig-a"),
                    ["node-2"] = make_signal_yield("sig-b", {
                        signal_data = { value = 42 },
                    }),
                },
            })

            local decision = scheduler.find_next_work(state)
            test.eq(decision.type, scheduler.DECISION_TYPE.SATISFY_YIELD, "satisfies the one with data")
            test.eq(decision.payload.results.value, 42, "correct signal data")
        end)

        test.it("mixed signal and normal yields - satisfies normal when children complete", function()
            local state = make_state({
                active_yields = {
                    ["signal-node"] = make_signal_yield("approval"),
                    ["normal-node"] = make_normal_yield({
                        pending_children = { ["child-1"] = consts.STATUS.COMPLETED_SUCCESS },
                        results = { ["child-1"] = "result-data-id" },
                    }),
                },
            })

            local decision = scheduler.find_next_work(state)
            test.eq(decision.type, scheduler.DECISION_TYPE.SATISFY_YIELD, "satisfies normal yield")
            test.eq(decision.payload.parent_id, "normal-node", "satisfies the normal yield, not signal")
        end)

        test.it("signal yield with empty signal_data table is satisfied", function()
            local state = make_state({
                active_yields = {
                    ["node-1"] = make_signal_yield("approval", {
                        signal_data = {},
                    }),
                },
            })

            local decision = scheduler.find_next_work(state)
            test.eq(decision.type, scheduler.DECISION_TYPE.SATISFY_YIELD, "empty table counts as data")
        end)

        test.it("two signal yields both with data - satisfies one", function()
            local state = make_state({
                active_yields = {
                    ["node-1"] = make_signal_yield("sig-a", {
                        yield_id = "y1",
                        signal_data = { a = 1 },
                    }),
                    ["node-2"] = make_signal_yield("sig-b", {
                        yield_id = "y2",
                        signal_data = { b = 2 },
                    }),
                },
            })

            local decision = scheduler.find_next_work(state)
            test.eq(decision.type, scheduler.DECISION_TYPE.SATISFY_YIELD, "satisfies one of them")
            -- at least one gets satisfied
            local is_valid = decision.payload.yield_id == "y1" or decision.payload.yield_id == "y2"
            test.is_true(is_valid, "valid yield_id")
        end)

        test.it("signal yield does not block other pending nodes from executing", function()
            local state = make_state({
                nodes = {
                    ["signal-node"] = { status = consts.STATUS.RUNNING, type = "signal" },
                    ["func-node"] = { status = consts.STATUS.PENDING, type = "func" },
                },
                active_yields = {
                    ["signal-node"] = make_signal_yield("approval"),
                },
                input_tracker = {
                    requirements = {},
                    available = {
                        ["func-node"] = { default = true },
                    },
                },
            })

            local decision = scheduler.find_next_work(state)
            test.eq(decision.type, scheduler.DECISION_TYPE.EXECUTE_NODES, "executes func node despite pending signal")
        end)

        test.it("signal yield prevents COMPLETE_WORKFLOW even with workflow output", function()
            local state = make_state({
                nodes = {
                    ["node-1"] = { status = consts.STATUS.RUNNING, type = "signal" },
                },
                active_yields = {
                    ["node-1"] = make_signal_yield("approval"),
                },
                has_workflow_output = true,
            })

            local decision = scheduler.find_next_work(state)
            test.neq(decision.type, scheduler.DECISION_TYPE.COMPLETE_WORKFLOW, "yield blocks completion even with output")
        end)

        test.it("completes workflow after signal yield is satisfied and removed", function()
            local state = make_state({
                nodes = {
                    ["node-1"] = { status = consts.STATUS.COMPLETED_SUCCESS, type = "signal" },
                },
                has_workflow_output = true,
            })

            local decision = scheduler.find_next_work(state)
            test.eq(decision.type, scheduler.DECISION_TYPE.COMPLETE_WORKFLOW, "completes after yield removed")
            test.is_true(decision.payload.success, "successful completion")
        end)

        test.it("signal yield with false signal_data IS satisfied", function()
            local state = make_state({
                active_yields = {
                    ["node-1"] = make_signal_yield("approval", {
                        signal_data = false,
                    }),
                },
            })

            local decision = scheduler.find_next_work(state)
            test.eq(decision.type, scheduler.DECISION_TYPE.SATISFY_YIELD, "false is valid signal data")
            test.eq(decision.payload.results, false, "false value preserved")
        end)

        test.it("signal yield with 0 signal_data IS satisfied", function()
            local state = make_state({
                active_yields = {
                    ["node-1"] = make_signal_yield("approval", {
                        signal_data = 0,
                    }),
                },
            })

            local decision = scheduler.find_next_work(state)
            test.eq(decision.type, scheduler.DECISION_TYPE.SATISFY_YIELD, "0 is valid signal data")
            test.eq(decision.payload.results, 0, "0 value preserved")
        end)

        test.it("signal yield with empty string signal_data IS satisfied", function()
            local state = make_state({
                active_yields = {
                    ["node-1"] = make_signal_yield("approval", {
                        signal_data = "",
                    }),
                },
            })

            local decision = scheduler.find_next_work(state)
            test.eq(decision.type, scheduler.DECISION_TYPE.SATISFY_YIELD, "empty string is valid signal data")
        end)

        test.it("signal yield with nil signal_data is NOT satisfied", function()
            local state = make_state({
                active_yields = {
                    ["node-1"] = make_signal_yield("approval"),
                },
            })

            -- signal_data defaults to nil
            local decision = scheduler.find_next_work(state)
            test.eq(decision.type, scheduler.DECISION_TYPE.NO_WORK, "nil means no signal arrived")
        end)

        test.it("signal yield preserves reply_to for satisfaction", function()
            local state = make_state({
                active_yields = {
                    ["node-1"] = make_signal_yield("approval", {
                        reply_to = "dataflow.yield_reply.specific-topic",
                        signal_data = { ok = true },
                    }),
                },
            })

            local decision = scheduler.find_next_work(state)
            test.eq(decision.payload.reply_to, "dataflow.yield_reply.specific-topic", "reply_to preserved")
        end)

        test.it("signal yield preserves parent_id in satisfaction decision", function()
            local state = make_state({
                active_yields = {
                    ["my-signal-node"] = make_signal_yield("approval", {
                        signal_data = { ok = true },
                    }),
                },
            })

            local decision = scheduler.find_next_work(state)
            test.eq(decision.payload.parent_id, "my-signal-node", "parent_id is the signal node")
        end)
    end)

    -- ==========================================
    -- WORKFLOW STATE SIGNAL DATA DELIVERY
    -- ==========================================

    test.describe("workflow_state signal data delivery", function()
        local workflow_state = require("workflow_state")

        local function make_ws(): any
            return workflow_state.new(uuid.v7())
        end

        local function active_yield(ws: any, node_id: string): any
            return ws.active_yields[node_id]
        end

        local function make_signal_result(signal_id, content)
            return {
                results = {
                    {
                        input = {
                            type = consts.COMMAND_TYPES.CREATE_DATA,
                            payload = {
                                data_type = consts.DATA_TYPE.NODE_SIGNAL,
                                key = signal_id,
                                content = content,
                            }
                        }
                    }
                }
            }
        end

        test.it("delivers NODE_SIGNAL data to matching signal yield", function()
            local ws: any = make_ws()
            ws.active_yields["node-1"] = {
                yield_id = "yield-1",
                reply_to = "reply.topic",
                signal_id = "my_signal",
                pending_children = {},
                results = {},
                wait_for_signal = true,
            }

            ws:_update_state_from_results(make_signal_result("my_signal", { approved = true }))

            test.not_nil(active_yield(ws, "node-1").signal_data, "signal data delivered")
            test.eq(active_yield(ws, "node-1").signal_data.approved, true, "correct data")
        end)

        test.it("ignores NODE_SIGNAL for non-matching signal_id", function()
            local ws: any = make_ws()
            ws.active_yields["node-1"] = {
                yield_id = "yield-1",
                signal_id = "approval",
                wait_for_signal = true,
            }

            ws:_update_state_from_results(make_signal_result("wrong_signal", { data = "nope" }))

            test.is_nil(active_yield(ws, "node-1").signal_data, "no delivery for wrong id")
        end)

        test.it("delivers to first matching yield only", function()
            local ws: any = make_ws()
            ws.active_yields["node-1"] = {
                signal_id = "same_signal",
                wait_for_signal = true,
            }
            ws.active_yields["node-2"] = {
                signal_id = "same_signal",
                wait_for_signal = true,
            }

            ws:_update_state_from_results(make_signal_result("same_signal", { first = true }))

            -- one of them should have data, the other should not
            local delivered_count = 0
            if active_yield(ws, "node-1").signal_data then delivered_count = delivered_count + 1 end
            if active_yield(ws, "node-2").signal_data then delivered_count = delivered_count + 1 end
            test.eq(delivered_count, 1, "exactly one yield receives the signal")
        end)

        test.it("delivers different signals to different yields", function()
            local ws: any = make_ws()
            ws.active_yields["node-1"] = {
                signal_id = "sig-a",
                wait_for_signal = true,
            }
            ws.active_yields["node-2"] = {
                signal_id = "sig-b",
                wait_for_signal = true,
            }

            ws:_update_state_from_results(make_signal_result("sig-a", { a = true }))
            ws:_update_state_from_results(make_signal_result("sig-b", { b = true }))

            test.not_nil(active_yield(ws, "node-1").signal_data, "node-1 received sig-a")
            test.eq(active_yield(ws, "node-1").signal_data.a, true, "correct data for sig-a")
            test.not_nil(active_yield(ws, "node-2").signal_data, "node-2 received sig-b")
            test.eq(active_yield(ws, "node-2").signal_data.b, true, "correct data for sig-b")
        end)

        test.it("does not deliver to non-signal yields", function()
            local ws: any = make_ws()
            ws.active_yields["node-1"] = {
                yield_id = "yield-1",
                signal_id = "approval",
                -- wait_for_signal is NOT set (normal yield)
                pending_children = {},
                results = {},
            }

            ws:_update_state_from_results(make_signal_result("approval", { ok = true }))

            test.is_nil(active_yield(ws, "node-1").signal_data, "normal yield ignores signal data")
        end)

        test.it("delivers signal with empty content", function()
            local ws: any = make_ws()
            ws.active_yields["node-1"] = {
                signal_id = "approval",
                wait_for_signal = true,
            }

            ws:_update_state_from_results(make_signal_result("approval", {}))

            test.not_nil(active_yield(ws, "node-1").signal_data, "empty table delivered")
        end)

        test.it("delivers signal with nested objects", function()
            local ws: any = make_ws()
            ws.active_yields["node-1"] = {
                signal_id = "review",
                wait_for_signal = true,
            }

            local data = {
                reviewer = { name = "alice", team = "security" },
                decision = "approved",
                comments = { "looks good", "no issues found" },
                score = 95,
            }
            ws:_update_state_from_results(make_signal_result("review", data))

            test.eq(active_yield(ws, "node-1").signal_data.reviewer.name, "alice", "nested object preserved")
            test.eq(active_yield(ws, "node-1").signal_data.score, 95, "number preserved")
        end)

        test.it("signal with overwritten data uses latest value", function()
            local ws: any = make_ws()
            ws.active_yields["node-1"] = {
                signal_id = "approval",
                wait_for_signal = true,
            }

            -- simulate two signals arriving in sequence (e.g. user correction)
            ws:_update_state_from_results(make_signal_result("approval", { version = 1, approved = false }))
            ws:_update_state_from_results(make_signal_result("approval", { version = 2, approved = true }))

            -- latest signal should win
            test.eq(active_yield(ws, "node-1").signal_data.version, 2, "latest signal wins")
            test.eq(active_yield(ws, "node-1").signal_data.approved, true, "correction applied")
        end)

        test.it("second signal to same yield overwrites first", function()
            local ws: any = make_ws()
            ws.active_yields["node-1"] = {
                signal_id = "approval",
                wait_for_signal = true,
            }

            ws:_update_state_from_results(make_signal_result("approval", { version = 1 }))
            ws:_update_state_from_results(make_signal_result("approval", { version = 2 }))

            test.eq(active_yield(ws, "node-1").signal_data.version, 2, "second signal overwrites")
        end)

        test.it("uses discriminator as fallback for signal_id matching", function()
            local ws: any = make_ws()
            ws.active_yields["node-1"] = {
                signal_id = "approval",
                wait_for_signal = true,
            }

            ws:_update_state_from_results({
                results = {
                    {
                        input = {
                            type = consts.COMMAND_TYPES.CREATE_DATA,
                            payload = {
                                data_type = consts.DATA_TYPE.NODE_SIGNAL,
                                discriminator = "approval",
                                content = { via_discriminator = true },
                            }
                        }
                    }
                }
            })

            test.not_nil(active_yield(ws, "node-1").signal_data, "matched via discriminator")
            test.eq(active_yield(ws, "node-1").signal_data.via_discriminator, true, "correct data")
        end)

        test.it("handles NODE_INPUT alongside NODE_SIGNAL in same batch", function()
            local ws: any = make_ws()
            ws.active_yields["sig-node"] = {
                signal_id = "approval",
                wait_for_signal = true,
            }
            ws.input_tracker = { requirements = {}, available = {} }

            ws:_update_state_from_results({
                results = {
                    {
                        input = {
                            type = consts.COMMAND_TYPES.CREATE_DATA,
                            payload = {
                                data_type = consts.DATA_TYPE.NODE_INPUT,
                                node_id = "other-node",
                                key = "default",
                            }
                        }
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
                }
            })

            test.not_nil(active_yield(ws, "sig-node").signal_data, "signal delivered despite mixed batch")
            test.not_nil(ws.input_tracker.available["other-node"], "input also tracked")
        end)

        test.it("signal without key or discriminator is ignored", function()
            local ws: any = make_ws()
            ws.active_yields["node-1"] = {
                signal_id = "approval",
                wait_for_signal = true,
            }

            ws:_update_state_from_results({
                results = {
                    {
                        input = {
                            type = consts.COMMAND_TYPES.CREATE_DATA,
                            payload = {
                                data_type = consts.DATA_TYPE.NODE_SIGNAL,
                                content = { data = "no key" },
                            }
                        }
                    }
                }
            })

            test.is_nil(active_yield(ws, "node-1").signal_data, "no delivery without key")
        end)
    end)

    -- ==========================================
    -- WORKFLOW STATE TRACK_YIELD RECOVERY
    -- ==========================================

    test.describe("workflow_state track_yield", function()
        local workflow_state = require("workflow_state")

        test.it("stores signal yield in active_yields", function()
            local ws: any = workflow_state.new(uuid.v7())
            local yield_info = {
                yield_id = "y1",
                reply_to = "topic",
                signal_id = "approval",
                pending_children = {},
                results = {},
                wait_for_signal = true,
            }

            ws:track_yield("node-1", yield_info)
            test.not_nil(ws.active_yields["node-1"], "yield tracked")
            test.eq(ws.active_yields["node-1"].signal_id, "approval", "signal_id stored")
        end)

        test.it("stores non-signal yield without DB lookup", function()
            local ws: any = workflow_state.new(uuid.v7())
            local yield_info = {
                yield_id = "y1",
                reply_to = "topic",
                pending_children = { ["child-1"] = consts.STATUS.PENDING },
                results = {},
            }

            ws:track_yield("node-1", yield_info)
            test.not_nil(ws.active_yields["node-1"], "yield tracked")
            test.is_nil(ws.active_yields["node-1"].signal_data, "no signal data for normal yield")
        end)

        test.it("satisfy_yield removes yield from active_yields", function()
            local ws: any = workflow_state.new(uuid.v7())
            ws.active_yields["node-1"] = {
                yield_id = "y1",
                signal_id = "approval",
                wait_for_signal = true,
                signal_data = { ok = true },
            }

            ws:satisfy_yield("node-1", { ok = true })
            test.is_nil(ws.active_yields["node-1"], "yield removed after satisfaction")
        end)

        test.it("handle_process_exit cleans up orphaned signal yield", function()
            local ws: any = workflow_state.new(uuid.v7())
            local pid = "pid-" .. uuid.v7()

            -- track a process with a signal yield
            ws.nodes["sig-node"] = { status = consts.STATUS.RUNNING, type = "signal" }
            ws.active_processes["sig-node"] = pid
            ws.active_yields["sig-node"] = {
                yield_id = "y1",
                signal_id = "approval",
                wait_for_signal = true,
                pending_children = {},
                results = {},
            }

            -- simulate signal node process crash
            local exit_info = ws:handle_process_exit(pid, false, "process crashed")

            test.not_nil(exit_info, "exit info returned")
            test.eq(exit_info.node_id, "sig-node", "correct node")
            test.is_nil(ws.active_yields["sig-node"], "orphaned signal yield cleaned up")
            test.is_nil(ws.active_processes["sig-node"], "process removed")
        end)

        test.it("handle_process_exit preserves non-signal yields on exit", function()
            local ws: any = workflow_state.new(uuid.v7())
            local pid = "pid-" .. uuid.v7()

            -- track a process with a normal (non-signal) yield
            ws.nodes["func-node"] = { status = consts.STATUS.RUNNING, type = "func" }
            ws.active_processes["func-node"] = pid
            ws.active_yields["func-node"] = {
                yield_id = "y1",
                pending_children = { ["child-1"] = consts.STATUS.PENDING },
                results = {},
            }

            -- simulate func node process exit (success)
            ws:handle_process_exit(pid, true, { ok = true })

            -- non-signal yield should NOT be cleaned up
            test.not_nil(ws.active_yields["func-node"], "normal yield preserved")
        end)

        test.it("satisfy_yield queues NODE_YIELD_RESULT command", function()
            local ws: any = workflow_state.new(uuid.v7())
            ws.active_yields["node-1"] = {
                yield_id = "y1",
                signal_id = "approval",
                wait_for_signal = true,
            }

            ws:satisfy_yield("node-1", { result = "data" })

            test.eq(#ws.queued_commands, 1, "one command queued")
            test.eq(ws.queued_commands[1].type, consts.COMMAND_TYPES.CREATE_DATA, "CREATE_DATA command")
            test.eq(ws.queued_commands[1].payload.data_type, consts.DATA_TYPE.NODE_YIELD_RESULT, "yield result type")
            test.eq(ws.queued_commands[1].payload.key, "y1", "yield_id as key")
        end)
    end)

    -- ==========================================
    -- SCHEDULER STATE INTERACTION
    -- ==========================================

    test.describe("scheduler state completeness", function()
        local scheduler = require("scheduler")

        test.it("empty state produces COMPLETE_WORKFLOW with success", function()
            local state = scheduler.create_empty_state()
            local decision = scheduler.find_next_work(state)
            test.eq(decision.type, scheduler.DECISION_TYPE.COMPLETE_WORKFLOW, "empty = complete")
            test.is_true(decision.payload.success, "empty workflow is success")
        end)

        test.it("only signal yields with no other nodes = NO_WORK", function()
            local state = scheduler.create_empty_state()
            state.active_yields["node-1"] = {
                yield_id = "y1",
                reply_to = "topic",
                signal_id = "approval",
                pending_children = {},
                results = {},
                wait_for_signal = true,
            }

            local decision = scheduler.find_next_work(state)
            test.eq(decision.type, scheduler.DECISION_TYPE.NO_WORK, "waiting for signal")
        end)

        test.it("signal yield + completed node with output = NO_WORK (yield blocks)", function()
            local state = scheduler.create_empty_state()
            state.nodes["func-1"] = { status = consts.STATUS.COMPLETED_SUCCESS, type = "func" }
            state.active_yields["sig-1"] = {
                yield_id = "y1",
                reply_to = "topic",
                signal_id = "approval",
                pending_children = {},
                results = {},
                wait_for_signal = true,
            }
            state.has_workflow_output = true

            local decision = scheduler.find_next_work(state)
            test.eq(decision.type, scheduler.DECISION_TYPE.NO_WORK, "yield blocks even with output")
        end)

        test.it("workflow error with active signal yield = NO_WORK", function()
            local state = scheduler.create_empty_state()
            state.active_yields["sig-1"] = {
                yield_id = "y1",
                reply_to = "topic",
                signal_id = "approval",
                pending_children = {},
                results = {},
                wait_for_signal = true,
            }
            state.has_workflow_error = true

            local decision = scheduler.find_next_work(state)
            test.eq(decision.type, scheduler.DECISION_TYPE.NO_WORK, "error + yield = no work")
        end)
    end)
end

return { run_tests = test.run_cases(define_tests) }
