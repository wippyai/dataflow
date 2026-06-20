local test = require("test")
local orchestrator = require("orchestrator")
local consts = require("consts")

type MockWorkflowState = {
    load_state: (self: MockWorkflowState) -> (MockWorkflowState?, string?),
    get_nodes: () -> { [string]: any },
    get_dataflow_metadata: () -> { [string]: any },
    get_actor_id: () -> string?,
    get_scheduler_snapshot: () -> { [string]: any },
    get_failed_node_errors: () -> string?,
    track_process: (self: MockWorkflowState, node_id: string, pid: string) -> MockWorkflowState,
    queue_commands: (self: MockWorkflowState, commands: any) -> MockWorkflowState,
    persist: (self: MockWorkflowState) -> ({ changes_made: boolean }?, string?),
    get_node: (self: MockWorkflowState, node_id: string) -> { [string]: any }?,
    handle_process_exit: (self: MockWorkflowState, pid: string, success: boolean, result: any) -> string?,
    process_commits: (self: MockWorkflowState, commit_ids: any) -> ({ changes_made: boolean }?, string?),
    track_yield: (self: MockWorkflowState, node_id: string, yield_info: any) -> MockWorkflowState,
    satisfy_yield: (self: MockWorkflowState, parent_id: string, results: any) -> MockWorkflowState,
    has_workflow_output: boolean,
}

type MockScheduler = {
    find_next_work: (snapshot: any) -> { type: string, payload: any },
    DECISION_TYPE: { [string]: string },
}

type MockProcess = {
    registry: { register: (name: string) -> nil, unregister: (name: string) -> nil },
    set_options: (options: any) -> nil,
    with_context: (ctx: any) -> any,
    spawn_linked_monitored: (node_type: string, host: string, args: any) -> (string?, string?),
    send: (dest: string, topic: string, payload: any) -> nil,
    terminate: (pid: string) -> nil,
    inbox: () -> any,
    events: () -> any,
    event: { [string]: string },
}

type MockCommit = {
    get_pending_commits: (dataflow_id: string) -> ({ string }?, string?)
}

local function define_tests()
    describe("Orchestrator", function()
        local mock_workflow_state = nil :: MockWorkflowState
        local mock_scheduler = nil :: MockScheduler
        local mock_process = nil :: MockProcess
        local mock_commit = nil :: MockCommit
        local current_actor: any = nil
        local captured_actors: { any } = {}

        before_each(function()
            captured_actors = {}
            current_actor = {
                id = function()
                    return "test-actor-123"
                end,
            }

            mock_workflow_state = {
                load_state = function(self: MockWorkflowState): (MockWorkflowState?, string?)
                    return self, nil
                end,
                get_nodes = function(): { [string]: any }
                    return {
                        ["node-1"] = {
                            type = "test_node",
                            status = consts.STATUS.PENDING,
                            parent_node_id = nil,
                        },
                    }
                end,
                get_dataflow_metadata = function(): { [string]: any }
                    return { test = "metadata" }
                end,
                get_actor_id = function(): string?
                    return "test-actor-123"
                end,
                get_scheduler_snapshot = function(): { [string]: any }
                    return {
                        nodes = {
                            ["node-1"] = {
                                type = "test_node",
                                status = consts.STATUS.PENDING,
                            },
                        },
                        active_yields = {},
                        active_processes = {},
                        input_tracker = {
                            requirements = {},
                            available = { ["node-1"] = { input = true } },
                        },
                        has_workflow_output = false,
                    }
                end,
                get_failed_node_errors = function(): string?
                    return nil
                end,
                track_process = function(self: MockWorkflowState, _node_id: string, _pid: string): MockWorkflowState
                    return self
                end,
                queue_commands = function(self: MockWorkflowState, _commands: any): MockWorkflowState
                    return self
                end,
                persist = function(_self: MockWorkflowState): ({ changes_made: boolean }?, string?)
                    return { changes_made = true }, nil
                end,
                get_node = function(_self: MockWorkflowState, _node_id: string): { [string]: any }?
                    return {
                        type = "test_node",
                        status = consts.STATUS.PENDING,
                        parent_node_id = nil,
                    }
                end,
                handle_process_exit = function(_self: MockWorkflowState, _pid: string, _success: boolean, _result: any): string?
                    return nil
                end,
                process_commits = function(_self: MockWorkflowState, _commit_ids: any): ({ changes_made: boolean }?, string?)
                    return { changes_made = false }, nil
                end,
                track_yield = function(self: MockWorkflowState, _node_id: string, _yield_info: any): MockWorkflowState
                    return self
                end,
                satisfy_yield = function(self: MockWorkflowState, _parent_id: string, _results: any): MockWorkflowState
                    return self
                end,
                has_workflow_output = false,
            }

            mock_scheduler = {
                find_next_work = function(_snapshot: any): { type: string, payload: any }
                    return {
                        type = "complete_workflow",
                        payload = { success = true, message = "Test complete" },
                    }
                end,
                DECISION_TYPE = {
                    EXECUTE_NODES = "execute_nodes",
                    SATISFY_YIELD = "satisfy_yield",
                    COMPLETE_WORKFLOW = "complete_workflow",
                    NO_WORK = "no_work",
                },
            }

            mock_process = {
                registry = {
                    register = function(_name: string) end,
                    unregister = function(_name: string) end,
                },
                set_options = function(_options: any) end,
                with_context = function(_ctx: any): any
                    local spawner = {}
                    function spawner:with_actor(actor: any): any
                        table.insert(captured_actors, actor)
                        return self
                    end
                    function spawner:spawn_linked_monitored(node_type: string, host: string, args: any): (string?, string?)
                        return mock_process.spawn_linked_monitored(node_type, host, args)
                    end
                    return spawner
                end,
                spawn_linked_monitored = function(_node_type: string, _host: string, _args: any): (string?, string?)
                    return "mock-pid-123", nil
                end,
                send = function(_dest: string, _topic: string, _payload: any) end,
                terminate = function(_pid: string) end,
                inbox = function(): any
                    return {
                        case_receive = function(): any
                            return { channel = "inbox", case = function(): boolean return false end }
                        end,
                    }
                end,
                events = function(): any
                    return {
                        case_receive = function(): any
                            return { channel = "events", case = function(): boolean return false end }
                        end,
                    }
                end,
                event = {
                    EXIT = "pid.exit",
                    LINK_DOWN = "pid.link.down",
                    CANCEL = "pid.cancel",
                },
            }

            mock_commit = {
                get_pending_commits = function(_dataflow_id: string): ({ string }?, string?)
                    return {}, nil
                end
            }

            local mock_channel = {
                select = function(_cases: any): { ok: boolean }
                    return { ok = false }
                end,
            }

            channel = mock_channel

            orchestrator.workflow_state = {
                new = function(_dataflow_id: string): (MockWorkflowState?, string?)
                    return mock_workflow_state, nil
                end,
            }
            orchestrator.scheduler = mock_scheduler
            orchestrator.process = mock_process
            orchestrator.commit = mock_commit
            orchestrator.funcs = {
                new = function(): any
                    local executor = {}
                    function executor:with_actor(actor: any): any
                        table.insert(captured_actors, actor)
                        return self
                    end
                    function executor:call(_func_id: string, _args: any): (any?, string?)
                        return { success = true }, nil
                    end
                    return executor
                end,
            }
            orchestrator.security = {
                actor = function()
                    return current_actor
                end,
                new_actor = function(actor_id: string)
                    return {
                        id = function()
                            return actor_id
                        end,
                    }
                end,
            }
        end)

        describe("Initialization", function()
            it("should fail with missing dataflow_id", function()
                local result = orchestrator.run({})

                test.is_false(result.success)
                test.contains(result.error, "Missing required dataflow_id")
            end)

            it("should fail with nil args", function()
                local result = orchestrator.run(nil)

                test.is_false(result.success)
                test.contains(result.error, "Missing required dataflow_id")
            end)

            it("should fail with empty string dataflow_id", function()
                local result = orchestrator.run({ dataflow_id = "" })

                test.is_false(result.success)
                test.contains(result.error, "Missing required dataflow_id")
            end)

            it("should handle workflow state creation failure", function()
                orchestrator.workflow_state.new = function(_dataflow_id: string): (MockWorkflowState?, string?)
                    return nil, "Failed to create workflow state"
                end

                local result = orchestrator.run({ dataflow_id = "test-workflow" })

                test.is_false(result.success)
                test.contains(result.error, "Failed to create workflow state")
            end)

            it("should handle workflow state loading failure", function()
                mock_workflow_state.load_state = function(_self: MockWorkflowState): (MockWorkflowState?, string?)
                    return nil, "Failed to load state"
                end

                local result = orchestrator.run({ dataflow_id = "test-workflow" })

                test.is_false(result.success)
                test.contains(result.error, "Failed to load workflow state")
            end)

            it("should process startup pending commits before scheduling", function()
                local commits_received = nil :: { string }?
                local scheduler_called = false

                mock_commit.get_pending_commits = function(_dataflow_id: string): ({ string }?, string?)
                    return { "commit-1", "commit-2" }, nil
                end

                mock_workflow_state.process_commits = function(_self: MockWorkflowState, commit_ids: any): ({ changes_made: boolean }?, string?)
                    commits_received = commit_ids
                    return { changes_made = true }, nil
                end

                mock_scheduler.find_next_work = function(_snapshot: any): { type: string, payload: any }
                    scheduler_called = true
                    return {
                        type = "complete_workflow",
                        payload = { success = true, message = "Backlog drained" },
                    }
                end

                local result = orchestrator.run({ dataflow_id = "test-workflow" })

                test.is_true(result.success)
                test.is_true(scheduler_called)
                test.not_nil(commits_received)
                local processed = test.not_nil(commits_received)
                test.eq(#processed, 2)
                test.eq(processed[1], "commit-1")
                test.eq(processed[2], "commit-2")
            end)

            it("should process startup commits before empty-workflow shortcut", function()
                local backlog_applied = false
                local scheduler_called = false

                mock_workflow_state.get_nodes = function(): { [string]: any }
                    if backlog_applied then
                        return {
                            ["node-from-commit"] = {
                                type = "test_node",
                                status = consts.STATUS.PENDING
                            }
                        }
                    end
                    return {}
                end

                mock_commit.get_pending_commits = function(_dataflow_id: string): ({ string }?, string?)
                    return { "commit-1" }, nil
                end

                mock_workflow_state.process_commits = function(_self: MockWorkflowState, _commit_ids: any): ({ changes_made: boolean }?, string?)
                    backlog_applied = true
                    return { changes_made = true }, nil
                end

                mock_scheduler.find_next_work = function(_snapshot: any): { type: string, payload: any }
                    scheduler_called = true
                    return {
                        type = "complete_workflow",
                        payload = { success = true, message = "Executed after backlog" },
                    }
                end

                local result = orchestrator.run({ dataflow_id = "test-workflow" })

                test.is_true(result.success)
                test.is_true(backlog_applied)
                test.is_true(scheduler_called)
                local output = test.not_nil(result.output)
                test.eq(output.message, "Executed after backlog")
            end)

            it("should fail when startup pending commits cannot be loaded", function()
                mock_commit.get_pending_commits = function(_dataflow_id: string): ({ string }?, string?)
                    return nil, "database unavailable"
                end

                local result = orchestrator.run({ dataflow_id = "test-workflow" })

                test.is_false(result.success)
                test.contains(result.error, "Failed to load pending commits")
                test.contains(result.error, "database unavailable")
            end)

            it("should handle empty workflow", function()
                mock_workflow_state.get_nodes = function(): { [string]: any }
                    return {}
                end

                local result = orchestrator.run({ dataflow_id = "empty-workflow" })

                test.is_true(result.success)
                test.not_nil(result.output)
                test.contains(result.output.message, "Empty workflow")
                test.eq(result.dataflow_id, "empty-workflow")
            end)

            it("should call init function if provided", function()
                local init_called = false
                local init_args: any = nil

                orchestrator.funcs = {
                    new = function(): any
                        local executor = {}
                        function executor:with_actor(actor: any): any
                            table.insert(captured_actors, actor)
                            return self
                        end
                        function executor:call(_func_id: string, args: any): (any?, string?)
                            init_called = true
                            init_args = args
                            return { success = true }, nil
                        end
                        return executor
                    end,
                }

                local result = orchestrator.run({
                    dataflow_id = "test-workflow",
                    init_func_id = "app:test_init",
                })

                test.is_true(result.success)
                test.is_true(init_called)
                test.not_nil(init_args)
                test.eq(init_args.dataflow_id, "test-workflow")
                test.has_key(init_args.metadata, "test")
            end)

            it("should reuse current actor when restored workflow actor id matches", function()
                orchestrator.security.new_actor = function(actor_id: string)
                    error("should not create actor " .. actor_id)
                end

                local result = orchestrator.run({
                    dataflow_id = "test-workflow",
                    init_func_id = "app:test_init",
                })

                test.is_true(result.success)
                test.eq(#captured_actors, 1)
                test.is_true(captured_actors[1] == current_actor)
                test.eq(captured_actors[1]:id(), "test-actor-123")
            end)

            it("should continue if init function fails", function()
                orchestrator.funcs = {
                    new = function(): any
                        local executor = {}
                        function executor:with_actor(actor: any): any
                            table.insert(captured_actors, actor)
                            return self
                        end
                        function executor:call(_func_id: string, _args: any): (any?, string?)
                            return nil, "Init function failed"
                        end
                        return executor
                    end,
                }

                local result = orchestrator.run({
                    dataflow_id = "test-workflow",
                    init_func_id = "app:failing_init",
                })

                test.is_true(result.success)
            end)
        end)

        describe("Node Execution", function()
            it("should execute node when scheduler returns execute_nodes", function()
                local spawn_calls: { { node_type: string, host: string, args: any } } = {}
                mock_process.spawn_linked_monitored = function(node_type: string, host: string, args: any): (string?, string?)
                    table.insert(spawn_calls, { node_type = node_type, host = host, args = args })
                    return "test-pid", nil
                end

                mock_scheduler.find_next_work = function(_snapshot: any): { type: string, payload: any }
                    return {
                        type = "execute_nodes",
                        payload = {
                            nodes = {
                                {
                                    node_id = "node-1",
                                    node_type = "test_node",
                                    path = {},
                                    trigger_reason = "root_ready",
                                },
                            },
                        },
                    }
                end

                local result = orchestrator.run({ dataflow_id = "test-workflow" })

                test.is_true(result.success)
                test.eq(#spawn_calls, 1)
                test.eq(spawn_calls[1].node_type, "test_node")
                test.eq(spawn_calls[1].host, consts.HOST_ID)
                test.eq(spawn_calls[1].args.dataflow_id, "test-workflow")
                test.eq(spawn_calls[1].args.node_id, "node-1")
                test.eq(#captured_actors, 1)
                test.is_true(captured_actors[1] == current_actor)
                test.eq(captured_actors[1]:id(), "test-actor-123")
            end)

            it("should handle spawn failures", function()
                mock_process.spawn_linked_monitored = function(_node_type: string, _host: string, _args: any): (string?, string?)
                    return nil, "Spawn failed"
                end

                mock_scheduler.find_next_work = function(_snapshot: any): { type: string, payload: any }
                    return {
                        type = "execute_nodes",
                        payload = {
                            nodes = {
                                {
                                    node_id = "node-1",
                                    node_type = "test_node",
                                    path = {},
                                    trigger_reason = "root_ready",
                                },
                            },
                        },
                    }
                end

                local result = orchestrator.run({ dataflow_id = "test-workflow" })

                test.is_false(result.success)
                test.contains(result.error, "Node spawn failures")
                test.contains(result.error, "node-1")
            end)

            it("should handle persist failures during execution", function()
                mock_workflow_state.persist = function(_self: MockWorkflowState): ({ changes_made: boolean }?, string?)
                    return nil, "Persist failed"
                end

                mock_scheduler.find_next_work = function(_snapshot: any): { type: string, payload: any }
                    return {
                        type = "execute_nodes",
                        payload = {
                            nodes = {
                                {
                                    node_id = "node-1",
                                    node_type = "test_node",
                                    path = {},
                                    trigger_reason = "root_ready",
                                },
                            },
                        },
                    }
                end

                local result = orchestrator.run({ dataflow_id = "test-workflow" })

                test.is_false(result.success)
                test.contains(result.error, "Failed to persist RUNNING status")
            end)

            it("should handle multiple nodes execution", function()
                local spawn_calls: { string } = {}
                mock_process.spawn_linked_monitored = function(_node_type: string, _host: string, args: any): (string?, string?)
                    table.insert(spawn_calls, args.node_id)
                    return "test-pid-" .. args.node_id, nil
                end

                mock_scheduler.find_next_work = function(_snapshot: any): { type: string, payload: any }
                    return {
                        type = "execute_nodes",
                        payload = {
                            nodes = {
                                {
                                    node_id = "node-1",
                                    node_type = "test_node",
                                    path = {},
                                    trigger_reason = "root_ready",
                                },
                                {
                                    node_id = "node-2",
                                    node_type = "test_node",
                                    path = {},
                                    trigger_reason = "root_ready",
                                },
                            },
                        },
                    }
                end

                mock_workflow_state.get_node = function(_self: MockWorkflowState, _node_id: string): { [string]: any }?
                    return {
                        type = "test_node",
                        status = consts.STATUS.PENDING,
                        parent_node_id = nil,
                    }
                end

                local result = orchestrator.run({ dataflow_id = "test-workflow" })

                test.is_true(result.success)
                test.eq(#spawn_calls, 2)

                local saw_node_1 = false
                local saw_node_2 = false
                for _, node_id in ipairs(spawn_calls) do
                    if node_id == "node-1" then
                        saw_node_1 = true
                    elseif node_id == "node-2" then
                        saw_node_2 = true
                    end
                end

                test.is_true(saw_node_1)
                test.is_true(saw_node_2)
            end)
        end)

        describe("Yield Handling", function()
            it("should handle yield satisfaction", function()
                local send_calls: { { dest: string, topic: string, payload: any } } = {}
                mock_process.send = function(dest: string, topic: string, payload: any)
                    table.insert(send_calls, { dest = dest, topic = topic, payload = payload })
                end

                mock_scheduler.find_next_work = function(_snapshot: any): { type: string, payload: any }
                    return {
                        type = "satisfy_yield",
                        payload = {
                            parent_id = "parent-1",
                            yield_id = "yield-123",
                            reply_to = "yield_reply",
                            results = { ["child-1"] = "result-data" },
                        },
                    }
                end

                local result = orchestrator.run({ dataflow_id = "test-workflow" })

                test.is_true(result.success)
            end)

            it("should handle persist failures during yield satisfaction", function()
                local persist_call_count = 0
                mock_workflow_state.persist = function(_self: MockWorkflowState): ({ changes_made: boolean }?, string?)
                    persist_call_count = persist_call_count + 1
                    if persist_call_count == 1 then
                        return { changes_made = true }, nil
                    else
                        return nil, "Persist failed"
                    end
                end

                mock_scheduler.find_next_work = function(_snapshot: any): { type: string, payload: any }
                    return {
                        type = "satisfy_yield",
                        payload = {
                            parent_id = "parent-1",
                            yield_id = "yield-123",
                            reply_to = "yield_reply",
                            results = {},
                        },
                    }
                end

                local result = orchestrator.run({ dataflow_id = "test-workflow" })

                test.is_true(result.success)
            end)
        end)

        describe("Workflow Completion", function()
            it("should handle successful completion", function()
                mock_scheduler.find_next_work = function(_snapshot: any): { type: string, payload: any }
                    return {
                        type = "complete_workflow",
                        payload = { success = true, message = "All nodes completed" },
                    }
                end

                local result = orchestrator.run({ dataflow_id = "test-workflow" })

                test.is_true(result.success)
                test.not_nil(result.output)
                test.eq(result.output.message, "All nodes completed")
                test.eq(result.dataflow_id, "test-workflow")
                test.is_nil(result.error)
            end)

            it("should handle failed completion with error details", function()
                mock_workflow_state.get_failed_node_errors = function(): string?
                    return "Node [node-1] failed: Test error"
                end

                mock_scheduler.find_next_work = function(_snapshot: any): { type: string, payload: any }
                    return {
                        type = "complete_workflow",
                        payload = { success = false, message = "Workflow deadlocked" },
                    }
                end

                local result = orchestrator.run({ dataflow_id = "test-workflow" })

                test.is_false(result.success)
                test.contains(result.error, "Node [node-1] failed: Test error")
                test.eq(result.dataflow_id, "test-workflow")
                test.is_nil(result.output)
            end)

            it("should handle failed completion without specific errors", function()
                mock_workflow_state.get_failed_node_errors = function(): string?
                    return nil
                end

                mock_scheduler.find_next_work = function(_snapshot: any): { type: string, payload: any }
                    return {
                        type = "complete_workflow",
                        payload = { success = false, message = "Custom failure message" },
                    }
                end

                local result = orchestrator.run({ dataflow_id = "test-workflow" })

                test.is_false(result.success)
                test.eq(result.error, "Custom failure message")
            end)
        end)

        describe("Scheduler Integration", function()
            it("should handle NO_WORK decision", function()
                mock_scheduler.find_next_work = function(_snapshot: any): { type: string, payload: any }
                    return {
                        type = "no_work",
                        payload = { message = "Waiting for events" },
                    }
                end

                local result = orchestrator.run({ dataflow_id = "test-workflow" })

                test.is_true(result.success)
            end)

            it("should pass correct snapshot to scheduler", function()
                local snapshot_received: any = nil
                mock_scheduler.find_next_work = function(snapshot: any): { type: string, payload: any }
                    snapshot_received = snapshot
                    return {
                        type = "complete_workflow",
                        payload = { success = true, message = "Test" },
                    }
                end

                local result = orchestrator.run({ dataflow_id = "test-workflow" })

                test.is_true(result.success)
                test.not_nil(snapshot_received)
                test.is_table(snapshot_received.nodes)
                test.is_table(snapshot_received.active_yields)
                test.is_table(snapshot_received.active_processes)
                test.is_table(snapshot_received.input_tracker)
                test.eq(type(snapshot_received.has_workflow_output), "boolean")
            end)
        end)

        describe("Business Logic Tests", function()
            it("should properly handle different scheduler decision types", function()
                local decisions = {
                    { type = "no_work", expected_success = true },
                    { type = "complete_workflow", payload = { success = true, message = "Done" }, expected_success = true },
                    { type = "complete_workflow", payload = { success = false, message = "Failed" }, expected_success = false },
                }

                for _, decision in ipairs(decisions) do
                    mock_scheduler.find_next_work = function(_snapshot: any): { type: string, payload: any }
                        return {
                            type = decision.type,
                            payload = decision.payload or {},
                        }
                    end

                    local result = orchestrator.run({ dataflow_id = "test-workflow-" .. decision.type })

                    test.eq(result.success, decision.expected_success)
                    if decision.payload and decision.payload.message and decision.expected_success then
                        test.not_nil(result.output)
                        test.eq(result.output.message, decision.payload.message)
                    end
                end
            end)

            it("should properly inject dependencies and call workflow state methods", function()
                local load_state_called = false
                local get_nodes_called = false
                local get_snapshot_called = false

                mock_workflow_state.load_state = function(self: MockWorkflowState): (MockWorkflowState?, string?)
                    load_state_called = true
                    return self, nil
                end

                mock_workflow_state.get_nodes = function(): { [string]: any }
                    get_nodes_called = true
                    return { ["test-node"] = { type = "test", status = "pending" } }
                end

                mock_workflow_state.get_scheduler_snapshot = function(): { [string]: any }
                    get_snapshot_called = true
                    return {
                        nodes = {},
                        active_yields = {},
                        active_processes = {},
                        input_tracker = { requirements = {}, available = {} },
                        has_workflow_output = false,
                    }
                end

                local result = orchestrator.run({ dataflow_id = "test-workflow" })

                test.is_true(result.success)
                test.is_true(load_state_called)
                test.is_true(get_nodes_called)
                test.is_true(get_snapshot_called)
            end)

            it("should handle workflow state method failures properly", function()
                mock_workflow_state.load_state = function(_self: MockWorkflowState): (MockWorkflowState?, string?)
                    return nil, "Load failed"
                end

                local result1 = orchestrator.run({ dataflow_id = "test-workflow" })
                test.is_false(result1.success)
                test.contains(result1.error, "Load failed")

                mock_workflow_state.load_state = function(self: MockWorkflowState): (MockWorkflowState?, string?)
                    return self, nil
                end
                mock_workflow_state.persist = function(_self: MockWorkflowState): ({ changes_made: boolean }?, string?)
                    return nil, "Persist failed"
                end

                mock_scheduler.find_next_work = function(_snapshot: any): { type: string, payload: any }
                    return {
                        type = "execute_nodes",
                        payload = {
                            nodes = {
                                {
                                    node_id = "test-node",
                                    node_type = "test_type",
                                    path = {},
                                    trigger_reason = "root_ready",
                                },
                            },
                        },
                    }
                end

                local result2 = orchestrator.run({ dataflow_id = "test-workflow" })
                test.is_false(result2.success)
                test.contains(result2.error, "Persist failed")
            end)

            it("should maintain consistent error format", function()
                mock_scheduler.find_next_work = function(_snapshot: any): { type: string, payload: any }
                    return {
                        type = "complete_workflow",
                        payload = { success = false, message = "Test failure" },
                    }
                end

                local result = orchestrator.run({ dataflow_id = "test-workflow" })

                test.is_false(result.success)
                test.eq(type(result.error), "string")
                test.eq(result.dataflow_id, "test-workflow")
                test.is_nil(result.output)
            end)
        end)

        describe("Integration Points", function()
            it("should correctly setup process registry and options", function()
                local registry_calls: { string } = {}
                local options_calls: { any } = {}

                mock_process.registry.register = function(name: string)
                    table.insert(registry_calls, name)
                end

                mock_process.set_options = function(options: any)
                    table.insert(options_calls, options)
                end

                local result = orchestrator.run({ dataflow_id = "test-registry" })

                test.is_true(result.success)
                test.eq(#registry_calls, 1)
                test.eq(registry_calls[1], "dataflow.test-registry")
                test.eq(#options_calls, 1)
                local first_options = options_calls[1]
                test.not_nil(first_options)
                test.is_true((first_options :: { trap_links: boolean }).trap_links)
            end)

            it("should properly format success and failure results", function()
                mock_scheduler.find_next_work = function(_snapshot: any): { type: string, payload: any }
                    return {
                        type = "complete_workflow",
                        payload = { success = true, message = "Success result" },
                    }
                end

                local success_result = orchestrator.run({ dataflow_id = "success-test" })

                test.is_true(success_result.success)
                test.eq(success_result.dataflow_id, "success-test")
                test.not_nil(success_result.output)
                test.eq(success_result.output.message, "Success result")
                test.is_nil(success_result.error)

                mock_scheduler.find_next_work = function(_snapshot: any): { type: string, payload: any }
                    return {
                        type = "complete_workflow",
                        payload = { success = false, message = "Failure result" },
                    }
                end

                local failure_result = orchestrator.run({ dataflow_id = "failure-test" })

                test.is_false(failure_result.success)
                test.eq(failure_result.dataflow_id, "failure-test")
                test.eq(failure_result.error, "Failure result")
                test.is_nil(failure_result.output)
            end)
        end)
    end)
end

return test.run_cases(define_tests)
