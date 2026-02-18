local test = require("test")
local uuid = require("uuid")
local time = require("time")

local client = require("client")
local consts = require("consts")

local function define_tests()
    describe("Workflow Client", function()
        local mock_deps: any
        local captured_calls
        local mock_security_actor: any

        before_each(function()
            captured_calls = {
                commit_execute = {},
                funcs_call = {},
                process_spawn = {},
                process_cancel = {},
                process_terminate = {},
                process_lookup = {},
                dataflow_repo_get = {},
                data_reader_calls = {}
            }

            mock_security_actor = {
                id = function() return "test-actor-123" end
            }

            mock_deps = {
                dataflow_repo = {
                    get_by_user = function(dataflow_id: string, actor_id: string)
                        table.insert(captured_calls.dataflow_repo_get, {
                            dataflow_id = dataflow_id,
                            actor_id = actor_id
                        })
                        return {
                            dataflow_id = dataflow_id,
                            status = "running",
                            actor_id = actor_id,
                            type = "test_workflow"
                        }, nil
                    end
                },
                commit = {
                    execute = function(dataflow_id: string, op_id: string, commands: { any }, options: any?)
                        table.insert(captured_calls.commit_execute, {
                            dataflow_id = dataflow_id,
                            op_id = op_id,
                            commands = commands,
                            options = options
                        })
                        return { changes_made = true }, nil
                    end
                },
                data_reader = {
                    with_dataflow = function(dataflow_id: string)
                        table.insert(captured_calls.data_reader_calls, {
                            method = "with_dataflow",
                            dataflow_id = dataflow_id
                        })
                        return {
                            with_data_types = function(data_types: { string })
                                table.insert(captured_calls.data_reader_calls, {
                                    method = "with_data_types",
                                    data_types = data_types
                                })
                                return {
                                    fetch_options = function(options: any?)
                                        table.insert(captured_calls.data_reader_calls, {
                                            method = "fetch_options",
                                            options = options
                                        })
                                        return {
                                            all = function()
                                                table.insert(captured_calls.data_reader_calls, {
                                                    method = "all"
                                                })
                                                return {
                                                    {
                                                        key = "result",
                                                        content = '{"message":"test output","processed":true}',
                                                        content_type = consts.CONTENT_TYPE.JSON
                                                    },
                                                    {
                                                        key = "backup",
                                                        content = '{"backup_data":"saved"}',
                                                        content_type = consts.CONTENT_TYPE.JSON
                                                    }
                                                }
                                            end,
                                            one = function()
                                                table.insert(captured_calls.data_reader_calls, {
                                                    method = "one"
                                                })
                                                return {
                                                    key = "",
                                                    content = '{"root_output":"test data"}',
                                                    content_type = consts.CONTENT_TYPE.JSON
                                                }
                                            end
                                        }
                                    end,
                                    all = function()
                                        table.insert(captured_calls.data_reader_calls, {
                                            method = "all"
                                        })
                                        return {}
                                    end
                                }
                            end
                        }
                    end
                },
                process = {
                    spawn = function(process_type: string, host: string, args: any)
                        table.insert(captured_calls.process_spawn, {
                            process_type = process_type,
                            host = host,
                            args = args
                        })
                        return "mock-pid-456"
                    end,
                    registry = {
                        lookup = function(process_name: string)
                            table.insert(captured_calls.process_lookup, { process_name = process_name })
                            return "mock-registry-pid-789"
                        end
                    },
                    cancel = function(pid: string, timeout: string)
                        table.insert(captured_calls.process_cancel, { pid = pid, timeout = timeout })
                        return true, nil
                    end,
                    terminate = function(pid: string)
                        table.insert(captured_calls.process_terminate, { pid = pid })
                        return true, nil
                    end
                },
                funcs = {
                    new = function()
                        return {
                            call = function(_self: any, func_name: string, args: any)
                                table.insert(captured_calls.funcs_call, {
                                    func_name = func_name,
                                    args = args
                                })
                                return {
                                    success = true,
                                    dataflow_id = args.dataflow_id or "generated-id-123",
                                    output = { message = "execution completed" }
                                }, nil
                            end
                        }
                    end
                },
                security = {
                    actor = function()
                        return mock_security_actor
                    end
                }
            }
        end)

        describe("Constructor", function()
            it("should create client with current security actor", function()
                local instance, err = client.new(mock_deps)

                test.is_nil(err)
                test.not_nil(instance)
                test.eq(instance._actor_id, "test-actor-123")
                test.eq(instance._deps, mock_deps)
            end)

            it("should create client with default dependencies when none provided", function()
                local instance, err = client.new(mock_deps)

                test.is_nil(err)
                test.not_nil(instance)
                test.eq(instance._actor_id, "test-actor-123")
            end)

            it("should fail when no security actor available", function()
                local no_actor_deps: { [string]: any } = {}
                for k, v in pairs(mock_deps) do
                    no_actor_deps[k] = v
                end
                no_actor_deps.security = {
                    actor = function() return nil end
                }

                local instance, err = client.new(no_actor_deps)

                test.is_nil(instance)
                test.contains(err, "No current security actor available")
            end)

            it("should fail when security actor has no id function", function()
                local bad_actor_deps: { [string]: any } = {}
                for k, v in pairs(mock_deps) do
                    bad_actor_deps[k] = v
                end
                bad_actor_deps.security = {
                    actor = function() return {} end
                }

                local instance, err = client.new(bad_actor_deps)

                test.is_nil(instance)
                test.contains(err, "Security actor does not have id() method")
            end)

            it("should fail when security actor id() returns empty string", function()
                local empty_id_deps: { [string]: any } = {}
                for k, v in pairs(mock_deps) do
                    empty_id_deps[k] = v
                end
                empty_id_deps.security = {
                    actor = function()
                        return {
                            id = function() return "" end
                        }
                    end
                }

                local instance, err = client.new(empty_id_deps)

                test.is_nil(instance)
                test.contains(err, "Actor ID cannot be empty")
            end)
        end)

        describe("Create Workflow Method", function()
            local test_client: any

            before_each(function()
                test_client, _ = client.new(mock_deps)
                test.not_nil(test_client._deps)
                test.not_nil(test_client._deps.data_reader)
            end)

            it("should create workflow with no additional commands", function()
                local dataflow_id, err = test_client:create_workflow()

                test.is_nil(err)
                test.not_nil(dataflow_id)
                test.eq(type(dataflow_id), "string")

                test.eq(#captured_calls.commit_execute, 1)
                local call = captured_calls.commit_execute[1]
                test.eq(call.dataflow_id, dataflow_id)
                test.eq(#call.commands, 1)
                test.eq(call.commands[1].type, consts.COMMAND_TYPES.CREATE_WORKFLOW)
                test.eq(call.commands[1].payload.actor_id, "test-actor-123")
                test.eq(call.commands[1].payload.type, "workflow")
            end)

            it("should create workflow with additional commands", function()
                local additional_commands = {
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = "test-node-1",
                            node_type = "func"
                        }
                    },
                    {
                        type = consts.COMMAND_TYPES.CREATE_DATA,
                        payload = {
                            data_id = "test-data-1",
                            data_type = consts.DATA_TYPE.NODE_INPUT,
                            content = "test input"
                        }
                    }
                }

                local dataflow_id, err = test_client:create_workflow(additional_commands)

                test.is_nil(err)
                test.not_nil(dataflow_id)

                local call = captured_calls.commit_execute[1]
                test.eq(#call.commands, 3)
                test.eq(call.commands[1].type, consts.COMMAND_TYPES.CREATE_WORKFLOW)
                test.eq(call.commands[2].type, consts.COMMAND_TYPES.CREATE_NODE)
                test.eq(call.commands[3].type, consts.COMMAND_TYPES.CREATE_DATA)
            end)

            it("should create workflow with custom options", function()
                local custom_id = "custom-workflow-id"
                local dataflow_id, err = test_client:create_workflow({}, {
                    dataflow_id = custom_id,
                    type = "custom_type",
                    metadata = { version = "1.0" }
                })

                test.is_nil(err)
                test.eq(dataflow_id, custom_id)

                local call = captured_calls.commit_execute[1]
                test.eq(call.dataflow_id, custom_id)
                test.eq(call.commands[1].payload.type, "custom_type")
                test.eq(call.commands[1].payload.metadata.version, "1.0")
            end)

            it("should handle commit execution failure", function()
                mock_deps.commit.execute = function()
                    return nil, "Database error"
                end

                local dataflow_id, err = test_client:create_workflow()

                test.is_nil(dataflow_id)
                test.eq(err, "Database error")
            end)
        end)

        describe("Execute Method", function()
            local test_client: any

            before_each(function()
                test_client, _ = client.new(mock_deps)
            end)

            it("should execute existing workflow synchronously with outputs", function()
                test.not_nil(mock_deps.data_reader)
                test.eq(type(mock_deps.data_reader.with_dataflow), "function")

                local result, err = test_client:execute("existing-workflow-123")

                test.is_nil(err)
                test.not_nil(result)
                test.is_true(result.success)
                test.eq(result.dataflow_id, "existing-workflow-123")
                test.not_nil(result.data)
                test.not_nil(result.data.result)
                test.eq(result.data.result.message, "test output")
                test.not_nil(result.data.backup)

                test.eq(#captured_calls.funcs_call, 1)
                local call = captured_calls.funcs_call[1]
                test.eq(call.func_name, consts.ORCHESTRATOR)
                test.eq(call.args.dataflow_id, "existing-workflow-123")
                test.is_nil(call.args.init_func_id)
            end)

            it("should execute workflow with init function", function()
                local result, err = test_client:execute("existing-workflow-123", {
                    init_func_id = "app:visualizer"
                })

                test.is_nil(err)
                test.is_true(result.success)

                local call = captured_calls.funcs_call[1]
                test.eq(call.args.init_func_id, "app:visualizer")
            end)

            it("should execute workflow without fetching outputs when disabled", function()
                local result, err = test_client:execute("existing-workflow-123", {
                    fetch_output = false
                })

                test.is_nil(err)
                test.is_true(result.success)
                test.is_nil(result.data)

                local reader_calls: number = 0
                for _, call in ipairs(captured_calls.data_reader_calls) do
                    if call.method == "with_dataflow" then
                        reader_calls = reader_calls + 1
                    end
                end
                test.eq(reader_calls, 0)
            end)

            it("should fail with missing dataflow_id", function()
                local result, err = test_client:execute("")

                test.is_nil(result)
                test.contains(err, "Dataflow ID is required")
            end)

            it("should handle funcs execution failure", function()
                mock_deps.funcs.new = function()
                    return {
                        call = function()
                            return nil, "Orchestrator failed"
                        end
                    }
                end

                local result, err = test_client:execute("existing-workflow-123")

                test.is_nil(result)
                test.contains(err, "Orchestrator failed")
            end)

            it("should handle orchestrator returning workflow failure", function()
                mock_deps.funcs.new = function()
                    return {
                        call = function()
                            return {
                                success = false,
                                dataflow_id = "existing-workflow-123",
                                error = "Workflow deadlocked"
                            }, nil
                        end
                    }
                end

                local result, err = test_client:execute("existing-workflow-123")

                test.is_nil(err)
                test.not_nil(result)
                test.is_false(result.success)
                test.eq(result.error, "Workflow deadlocked")
                test.is_nil(result.data)
            end)
        end)

        describe("Output Method", function()
            local test_client: any

            before_each(function()
                test_client, _ = client.new(mock_deps)
                test.not_nil(test_client._deps)
                test.not_nil(test_client._deps.data_reader)
            end)

            it("should fetch workflow outputs as key-value pairs", function()
                local outputs, err = test_client:output("test-workflow-id")

                test.is_nil(err)
                test.not_nil(outputs)
                test.not_nil(outputs.result)
                test.eq(outputs.result.message, "test output")
                test.not_nil(outputs.backup)
                test.eq(outputs.backup.backup_data, "saved")
            end)

            it("should return empty table when no outputs exist", function()
                mock_deps.data_reader.with_dataflow = function()
                    return {
                        with_data_types = function()
                            return {
                                fetch_options = function()
                                    return {
                                        all = function()
                                            return {}
                                        end
                                    }
                                end
                            }
                        end
                    }
                end

                local outputs, err = test_client:output("test-workflow-id")

                test.is_nil(err)
                test.not_nil(outputs)
                test.is_nil(next(outputs))
            end)

            it("should handle root output correctly", function()
                mock_deps.data_reader.with_dataflow = function()
                    return {
                        with_data_types = function()
                            return {
                                fetch_options = function()
                                    return {
                                        all = function()
                                            return {
                                                {
                                                    key = "",
                                                    content = '{"root_data":"test"}',
                                                    content_type = consts.CONTENT_TYPE.JSON
                                                }
                                            }
                                        end
                                    }
                                end
                            }
                        end
                    }
                end

                local outputs, err = test_client:output("test-workflow-id")

                test.is_nil(err)
                test.not_nil(outputs)
                test.eq(outputs.root_data, "test")
            end)

            it("should fail with missing dataflow_id", function()
                local outputs, err = test_client:output("")

                test.is_nil(outputs)
                test.contains(err, "Dataflow ID is required")
            end)
        end)

        describe("Start Method", function()
            local test_client: any

            before_each(function()
                test_client, _ = client.new(mock_deps)
                test.not_nil(test_client._deps)
                test.not_nil(test_client._deps.data_reader)
            end)

            it("should start workflow asynchronously", function()
                local dataflow_id, err = test_client:start("existing-workflow-456")

                test.is_nil(err)
                test.eq(dataflow_id, "existing-workflow-456")

                test.eq(#captured_calls.process_spawn, 1)
                local spawn_call = captured_calls.process_spawn[1]
                test.eq(spawn_call.process_type, consts.ORCHESTRATOR)
                test.eq(spawn_call.host, consts.HOST_ID)
                test.eq(spawn_call.args.dataflow_id, "existing-workflow-456")
                test.is_nil(spawn_call.args.init_func_id)
            end)

            it("should start workflow with init function", function()
                local dataflow_id, err = test_client:start("existing-workflow-456", {
                    init_func_id = "app:setup"
                })

                test.is_nil(err)
                test.eq(dataflow_id, "existing-workflow-456")

                local spawn_call = captured_calls.process_spawn[1]
                test.eq(spawn_call.args.init_func_id, "app:setup")
            end)

            it("should fail with missing dataflow_id", function()
                local dataflow_id, err = test_client:start("")

                test.is_nil(dataflow_id)
                test.contains(err, "Dataflow ID is required")
            end)

            it("should fail when process spawn fails", function()
                mock_deps.process.spawn = function() return nil end

                local dataflow_id, err = test_client:start("existing-workflow-456")

                test.is_nil(dataflow_id)
                test.contains(err, "Failed to spawn workflow process")
            end)
        end)

        describe("Cancel Method", function()
            local test_client: any

            before_each(function()
                test_client, _ = client.new(mock_deps)
                test.not_nil(test_client._deps)
                test.not_nil(test_client._deps.data_reader)
            end)

            it("should cancel workflow successfully", function()
                local success, err, info = test_client:cancel("workflow-123", "45s")

                test.is_true(success)
                test.is_nil(err)
                test.not_nil(info)
                local i = info :: any
                test.eq(i.dataflow_id, "workflow-123")
                test.eq(i.timeout, "45s")
                test.contains(i.message, "Cancel signal sent")

                test.eq(#captured_calls.dataflow_repo_get, 1)
                test.eq(#captured_calls.process_lookup, 1)
                test.eq(#captured_calls.process_cancel, 1)

                test.eq(captured_calls.process_lookup[1].process_name, "dataflow.workflow-123")
                test.eq(captured_calls.process_cancel[1].timeout, "45s")
            end)

            it("should use default timeout", function()
                test_client:cancel("workflow-123")

                test.eq(captured_calls.process_cancel[1].timeout, "30s")
            end)

            it("should fail with missing dataflow_id", function()
                local success, err = test_client:cancel("")

                test.is_false(success)
                test.contains(err, "Workflow ID is required")
            end)

            it("should fail when workflow not found", function()
                mock_deps.dataflow_repo.get_by_user = function() return nil, "Workflow not found" end

                local success, err = test_client:cancel("workflow-123")

                test.is_false(success)
                test.eq(err, "Workflow not found")
            end)

            it("should fail when workflow not in cancellable state", function()
                mock_deps.dataflow_repo.get_by_user = function()
                    return { status = "completed" }, nil
                end

                local success, err = test_client:cancel("workflow-123")

                test.is_false(success)
                test.contains(err, "cannot be cancelled in current state: completed")
            end)

            it("should fail when process not found in registry", function()
                mock_deps.process.registry.lookup = function() return nil end

                local success, err = test_client:cancel("workflow-123")

                test.is_false(success)
                test.contains(err, "Workflow process not found in registry")
            end)

            it("should fail when cancel signal fails", function()
                mock_deps.process.cancel = function() return false, "Cancel failed" end

                local success, err = test_client:cancel("workflow-123")

                test.is_false(success)
                test.contains(err, "Failed to send cancel signal: Cancel failed")
            end)
        end)

        describe("Terminate Method", function()
            local test_client: any

            before_each(function()
                test_client, _ = client.new(mock_deps)
                test.not_nil(test_client._deps)
                test.not_nil(test_client._deps.data_reader)
            end)

            it("should terminate workflow successfully", function()
                local success, err, info = test_client:terminate("workflow-456")

                test.is_true(success)
                test.is_nil(err)
                test.not_nil(info)
                local i = info :: any
                test.eq(i.dataflow_id, "workflow-456")
                test.is_true(i.process_terminated)
                test.is_true(i.status_updated)

                test.eq(#captured_calls.dataflow_repo_get, 1)
                test.eq(#captured_calls.process_lookup, 1)
                test.eq(#captured_calls.process_terminate, 1)
                test.eq(#captured_calls.commit_execute, 1)

                local commit_call = captured_calls.commit_execute[1]
                test.eq(commit_call.commands[1].type, consts.COMMAND_TYPES.UPDATE_WORKFLOW)
                test.eq(commit_call.commands[1].payload.status, "terminated")
            end)

            it("should fail with missing dataflow_id", function()
                local success, err = test_client:terminate("")

                test.is_false(success)
                test.contains(err, "Workflow ID is required")
            end)

            it("should fail when workflow already finished", function()
                mock_deps.dataflow_repo.get_by_user = function()
                    return { status = "completed" }, nil
                end

                local success, err = test_client:terminate("workflow-456")

                test.is_false(success)
                test.contains(err, "already finished with status: completed")
            end)

            it("should handle process not found gracefully", function()
                mock_deps.process.registry.lookup = function() return nil end

                local success, err, info = test_client:terminate("workflow-456")

                test.is_true(success)
                test.is_nil(err)
                local i = info :: any
                test.is_false(i.process_terminated)
                test.is_true(i.status_updated)
            end)

            it("should handle process termination failure", function()
                mock_deps.process.terminate = function() return false, "Terminate failed" end

                local success, _err, info = test_client:terminate("workflow-456")

                test.is_true(success)
                local i = info :: any
                test.is_false(i.process_terminated)
                test.eq(i.terminate_error, "Terminate failed")
            end)

            it("should fail when commit execution fails", function()
                mock_deps.commit.execute = function() return nil, "Commit failed" end

                local success, err, info = test_client:terminate("workflow-456")

                test.is_false(success)
                test.contains(err, "Failed to update workflow status: Commit failed")
                test.not_nil(info)
                local i = info :: any
                test.is_true(i.process_terminated)
            end)
        end)

        describe("Get Status Method", function()
            local test_client: any

            before_each(function()
                test_client, _ = client.new(mock_deps)
                test.not_nil(test_client._deps)
                test.not_nil(test_client._deps.data_reader)
            end)

            it("should get workflow status successfully", function()
                local status, err = test_client:get_status("workflow-789")

                test.is_nil(err)
                test.eq(status, "running")

                test.eq(#captured_calls.dataflow_repo_get, 1)
                test.eq(captured_calls.dataflow_repo_get[1].dataflow_id, "workflow-789")
                test.eq(captured_calls.dataflow_repo_get[1].actor_id, "test-actor-123")
            end)

            it("should fail with missing dataflow_id", function()
                local status, err = test_client:get_status("")

                test.is_nil(status)
                test.contains(err, "Workflow ID is required")
            end)

            it("should fail when workflow not found", function()
                mock_deps.dataflow_repo.get_by_user = function() return nil, "Not found" end

                local status, err = test_client:get_status("workflow-789")

                test.is_nil(status)
                test.eq(err, "Not found")
            end)

            it("should return different statuses", function()
                mock_deps.dataflow_repo.get_by_user = function()
                    return { status = "completed" }, nil
                end

                local status, err = test_client:get_status("workflow-789")

                test.is_nil(err)
                test.eq(status, "completed")
            end)
        end)

        describe("Integration Scenarios", function()
            local test_client: any

            before_each(function()
                test_client, _ = client.new(mock_deps)
                test.not_nil(test_client._deps)
                test.not_nil(test_client._deps.data_reader)
            end)

            it("should handle complete workflow lifecycle", function()
                local dataflow_id, create_err = test_client:create_workflow({
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = "test-node",
                            node_type = "func"
                        }
                    }
                })
                test.is_nil(create_err)
                test.not_nil(dataflow_id)

                local result, exec_err = test_client:execute(dataflow_id)
                test.is_nil(exec_err)
                test.is_true(result.success)
                test.not_nil(result.data)

                local status, status_err = test_client:get_status(dataflow_id)
                test.is_nil(status_err)
                test.eq(status, "running")

                local cancel_success, cancel_err = test_client:cancel(dataflow_id)
                test.is_true(cancel_success)
                test.is_nil(cancel_err)
            end)

            it("should handle actor ownership verification", function()
                test_client:get_status("test-workflow")
                test_client:cancel("test-workflow")
                test_client:terminate("test-workflow")

                test.eq(#captured_calls.dataflow_repo_get, 3)
                for _, call in ipairs(captured_calls.dataflow_repo_get) do
                    test.eq(call.actor_id, "test-actor-123")
                end
            end)

            it("should handle create and start workflow separately", function()
                local dataflow_id, create_err = test_client:create_workflow({
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = "async-node",
                            node_type = "func"
                        }
                    }
                })
                test.is_nil(create_err)

                local start_id, start_err = test_client:start(dataflow_id)
                test.is_nil(start_err)
                test.eq(start_id, dataflow_id)

                local spawn_call = captured_calls.process_spawn[1]
                test.eq(spawn_call.process_type, consts.ORCHESTRATOR)
                test.eq(spawn_call.args.dataflow_id, dataflow_id)
            end)

            it("should handle workflow failure with proper error structure", function()
                mock_deps.funcs.new = function()
                    return {
                        call = function()
                            return {
                                success = false,
                                dataflow_id = "failed-workflow",
                                error = "Node execution failed"
                            }, nil
                        end
                    }
                end

                local result, err = test_client:execute("failed-workflow")

                test.is_nil(err)
                test.not_nil(result)
                test.is_false(result.success)
                test.eq(result.dataflow_id, "failed-workflow")
                test.eq(result.error, "Node execution failed")
                test.is_nil(result.data)
            end)
        end)
    end)
end

return test.run_cases(define_tests)
