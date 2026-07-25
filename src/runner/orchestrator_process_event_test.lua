local test = require("test")
local orchestrator = require("orchestrator")
local consts = require("consts")

local function define_tests()
    describe("Orchestrator process events", function()
        local function assert_failure_reason_is_preserved(failure_reason: any, result_value: any, event_error: any?)
            local pending_node_result: any = nil
            local persisted_node_result: any = nil
            local scheduler_calls = 0
            local event_delivered = false
            local runtime: any = {}
            local inbox_channel = { case_receive = function(): any return { channel = "inbox" } end }
            local events_channel = { case_receive = function(): any return { channel = "events" } end }

            local workflow_state = {
                load_state = function(self: any): (any?, string?) return self, nil end,
                get_nodes = function(): { [string]: any }
                    return { ["node-1"] = { type = "test_node", status = consts.STATUS.PENDING } }
                end,
                get_dataflow_metadata = function(): { [string]: any } return {} end,
                get_dataflow_status = function(): string? return nil end,
                get_actor_id = function(): string? return "dataflow.test" end,
                get_actor_context = function(): any return { kind = "test" } end,
                get_scheduler_snapshot = function(): { [string]: any }
                    return {
                        nodes = { ["node-1"] = { type = "test_node", status = consts.STATUS.PENDING } },
                        active_yields = {},
                        active_processes = {},
                        input_tracker = { requirements = {}, available = { ["node-1"] = { input = true } } },
                        has_workflow_output = false,
                    }
                end,
                track_process = function(_self: any, _node_id: string, _pid: string)
                end,
                queue_commands = function(_self: any, _commands: any) end,
                persist = function(_self: any): ({ changes_made: boolean }?, string?)
                    if pending_node_result ~= nil then
                        persisted_node_result = pending_node_result
                        pending_node_result = nil
                        return { changes_made = true }, nil
                    end
                    if persisted_node_result ~= nil then
                        return {
                            changes_made = true,
                            results = { { completed = true, released = true } },
                        }, nil
                    end
                    return { changes_made = true }, nil
                end,
                get_node = function(_self: any, _node_id: string): { [string]: any }?
                    return { type = "test_node", status = consts.STATUS.PENDING }
                end,
                handle_process_exit = function(_self: any, _pid: string, success: boolean, result: any): any
                    pending_node_result = result or (success and "Completed" or "Failed")
                    return nil
                end,
                process_commits = function(_self: any, _commit_ids: any): ({ changes_made: boolean }?, string?)
                    return { changes_made = false }, nil
                end,
                get_failed_node_errors = function(): string?
                    local persisted_message = persisted_node_result
                    if type(persisted_node_result) == "table" then
                        if type(persisted_node_result.error) == "table" then
                            persisted_message = persisted_node_result.error.message
                        else
                            persisted_message = persisted_node_result.message
                        end
                    end
                    return "Node [node-1] failed: " .. (persisted_message or "Unknown error")
                end,
            }

            runtime.workflow_state = {
                new = function(_dataflow_id: string): (any?, string?) return workflow_state, nil end,
            }
            runtime.scheduler = {
                DECISION_TYPE = {
                    EXECUTE_NODES = "execute_nodes",
                    COMPLETE_WORKFLOW = "complete_workflow",
                    SATISFY_YIELD = "satisfy_yield",
                    NO_WORK = "no_work",
                },
                find_next_work = function(_snapshot: any): { type: string, payload: any }
                    scheduler_calls = scheduler_calls + 1
                    if scheduler_calls == 1 then
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
                    return { type = "complete_workflow", payload = { success = false } }
                end,
            }
            runtime.commit = {
                get_pending_commits = function(_dataflow_id: string): ({ string }?, string?) return {}, nil end,
            }
            runtime.overseer = {
                notify = function(): (boolean, string?) return true, nil end,
            }
            runtime.activation_repo = {
                get = function()
                    return { generation = 1, desired_active = true }, nil
                end,
            }
            runtime.execution_frame = {
                reconstruct = function()
                    return { id = function() return "dataflow.test" end }, "test-scope", nil
                end,
            }
            runtime.process = {
                registry = {
                    lookup = function() return nil, "not_found: name not registered" end,
                    register = function() return true, nil end,
                    unregister = function(_name: string) end,
                },
                pid = function() return "orchestrator-pid" end,
                send = function() return true, nil end,
                set_options = function(_options: any) end,
                with_context = function(_context: any): any
                    local spawner = {
                        spawn_linked_monitored = function(_self: any, _node_type: string, _host: string, _args: any): (string?, string?)
                            return "mock-pid-123", nil
                        end,
                    }
                    function spawner:with_actor(_actor: any): any return self end
                    function spawner:with_scope(_scope: any): any return self end
                    return spawner
                end,
                inbox = function(): any
                    return inbox_channel
                end,
                events = function(): any
                    return events_channel
                end,
                event = { EXIT = "pid.exit", LINK_DOWN = "pid.link.down", CANCEL = "pid.cancel" },
            }

            runtime.channel = {
                select = function(_cases: any): { ok: boolean, channel: any, value: any? }
                    if not event_delivered then
                        event_delivered = true
                        return {
                            ok = true,
                            channel = events_channel,
                            value = {
                                kind = "pid.exit",
                                from = "mock-pid-123",
                                result = {
                                    error = event_error == false and nil or (event_error or failure_reason),
                                    value = result_value,
                                },
                            },
                        }
                    end
                    return { ok = false }
                end,
            }

            local result = orchestrator.run({
                dataflow_id = "failure-reason-workflow",
                activation_generation = 1,
            }, runtime) :: any

            test.is_false(result.success)
            local expected_message = type(failure_reason) == "table" and
                tostring(failure_reason.message) or tostring(failure_reason)
            test.contains(tostring(result.error), expected_message)
        end

        it("preserves a plain-string child failure reason through process exit persistence", function()
            local failure_reason = "exit schema validation failed: missing required field url"
            assert_failure_reason_is_preserved(failure_reason, nil)
        end)

        it("preserves a structured child failure reason through process exit persistence", function()
            local failure_reason = {
                code = "MISSING_FUNC_ID",
                message = "Function ID not specified in node configuration",
            }
            assert_failure_reason_is_preserved(failure_reason, {
                success = false,
                message = "Missing func_id in node config",
                error = failure_reason,
                data_ids = {},
            })
        end)

        it("preserves an output-transform completion envelope through process exit persistence", function()
            local expression = "output.passed_items[1].contact_id"
            local failure_reason = "Output transform failed for target[1] (expression: " .. expression .. "): index out of range"
            local completion = {
                success = false,
                message = "Node [node-1] failed to route outputs: " .. failure_reason,
                error = failure_reason,
                data_ids = {},
            }
            assert_failure_reason_is_preserved(failure_reason, completion, false)
        end)
    end)
end

return test.run_cases(define_tests)
