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
                commit_submit = {},
                funcs_call = {},
                process_spawn = {},
                process_with_actor = {},
                process_with_scope = {},
                process_cancel = {},
                process_terminate = {},
                process_lookup = {},
                dataflow_repo_get = {},
                dataflow_repo_get_direct = {},
                funcs_with_actor = {},
                funcs_with_scope = {},
                identity_capture = {},
                identity_spawn = {},
                data_reader_calls = {}
            }

            mock_security_actor = {
                id = function() return "test-actor-123" end
            }

            mock_deps = {
                dataflow_repo = {
                    get = function(dataflow_id: string)
                        table.insert(captured_calls.dataflow_repo_get_direct, {
                            dataflow_id = dataflow_id
                        })
                        return {
                            dataflow_id = dataflow_id,
                            status = "running",
                            actor_id = "test-actor-123",
                            type = "test_workflow"
                        }, nil
                    end,
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
                    end,
                    submit = function(dataflow_id: string, op_id: string, commands: { any }, context: any?)
                        table.insert(captured_calls.commit_submit, {
                            dataflow_id = dataflow_id,
                            op_id = op_id,
                            commands = commands,
                            context = context
                        })
                        return { commit_id = "mock-commit-id" }, nil
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
                    with_context = function(_ctx: any)
                        local spawner = {}
                        function spawner:with_actor(actor: any)
                            table.insert(captured_calls.process_with_actor, actor)
                            return self
                        end
                        function spawner:with_scope(scope: any)
                            table.insert(captured_calls.process_with_scope, scope)
                            return self
                        end
                        function spawner:spawn(process_type: string, host: string, args: any)
                            return mock_deps.process.spawn(process_type, host, args)
                        end
                        return spawner
                    end,
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
                        local executor = {}
                        function executor:with_actor(actor: any)
                            table.insert(captured_calls.funcs_with_actor, actor)
                            return self
                        end
                        function executor:with_scope(scope: any)
                            table.insert(captured_calls.funcs_with_scope, scope)
                            return self
                        end
                        function executor:call(func_name: string, args: any)
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
                        return executor
                    end
                },
                security = {
                    actor = function()
                        return mock_security_actor
                    end,
                    scope = function()
                        return "test-scope"
                    end,
                    new_actor = function(actor_id: string, meta: any)
                        return {
                            id = function() return actor_id end,
                            meta = meta
                        }
                    end
                },
                identity_contract = false
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

            it("should fail closed when no security actor is available", function()
                local no_actor_deps: { [string]: any } = {}
                for k, v in pairs(mock_deps) do
                    no_actor_deps[k] = v
                end
                no_actor_deps.security = {
                    actor = function() return nil end,
                    scope = function() return "test-scope" end,
                }

                local instance, err = client.new(no_actor_deps)

                test.is_nil(instance)
                test.contains(err, "No current security actor")
            end)

            it("should fail closed when no security scope is available", function()
                local no_scope_deps: { [string]: any } = {}
                for k, v in pairs(mock_deps) do
                    no_scope_deps[k] = v
                end
                no_scope_deps.security = {
                    actor = function() return mock_security_actor end,
                    scope = function() return nil end,
                }

                local instance, err = client.new(no_scope_deps)

                test.is_nil(instance)
                test.contains(err, "No current security scope")
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
                    end,
                    scope = function() return "test-scope" end,
                }

                local instance, err = client.new(empty_id_deps)

                test.is_nil(instance)
                test.contains(err, "Actor ID cannot be empty")
            end)
        end)

        describe("Workflow Actor Propagation", function()
            it("should reuse current actor when workflow actor id matches", function()
                mock_deps.security.new_actor = function(actor_id: string)
                    error("should not create actor " .. actor_id)
                end

                local instance, err = client.new(mock_deps)
                test.is_nil(err)

                local actor = instance:_actor_for_workflow("workflow-123")

                test.not_nil(actor)
                test.eq(actor:id(), "test-actor-123")
                test.eq(#captured_calls.dataflow_repo_get_direct, 1)
            end)

            it("should reject direct actor reconstruction when stored actor differs", function()
                mock_deps.dataflow_repo.get = function(dataflow_id: string)
                    return {
                        dataflow_id = dataflow_id,
                        actor_id = "other-actor",
                        status = "running"
                    }, nil
                end

                mock_deps.security.new_actor = function(actor_id: string, meta: any)
                    error("should not create actor " .. actor_id)
                end

                local instance, err = client.new(mock_deps)
                test.is_nil(err)

                local actor, actor_err = instance:_actor_for_workflow("workflow-456")

                test.is_nil(actor)
                test.contains(actor_err, "workflow actor differs")
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
                test.is_nil(call.commands[1].payload.actor_context)
                test.eq(call.commands[1].payload.type, "workflow")
            end)

            it("should persist opaque actor_context when execution identity contract is bound", function()
                mock_deps.identity_contract = {
                    capture = function(_self: any, args: any): any
                        table.insert(captured_calls.identity_capture, args)
                        return {
                            success = true,
                            actor_id = "test-actor-123",
                            actor_context = '{"scope":"captured"}',
                        }
                    end,
                }
                test_client, _ = client.new(mock_deps)

                local dataflow_id, err = test_client:create_workflow()

                test.is_nil(err)
                test.not_nil(dataflow_id)
                test.eq(#captured_calls.identity_capture, 1)
                local call = captured_calls.commit_execute[1]
                test.eq(call.commands[1].payload.actor_context, '{"scope":"captured"}')
            end)

            it("should fail creation when the identity contract captures a different actor", function()
                mock_deps.identity_contract = {
                    capture = function(_self: any, _args: any): any
                        return {
                            success = true,
                            actor_id = "other-actor",
                            actor_context = '{"scope":"captured"}',
                        }
                    end,
                }
                test_client, _ = client.new(mock_deps)

                local dataflow_id, err = test_client:create_workflow()

                test.is_nil(dataflow_id)
                test.contains(err, "actor mismatch")
                test.eq(#captured_calls.commit_execute, 0)
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

            it("should persist on_complete hook into workflow metadata", function()
                local dataflow_id, err = test_client:create_workflow({}, {
                    on_complete = "app:notify_complete"
                })

                test.is_nil(err)
                test.not_nil(dataflow_id)

                local call = captured_calls.commit_execute[1]
                test.eq(call.commands[1].payload.metadata.on_complete, "app:notify_complete")
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
                test.eq(#captured_calls.funcs_with_scope, 1)
                test.eq(captured_calls.funcs_with_scope[1], "test-scope")
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

            it("returns the ordinary Dataflow handle when synchronous execution passivates", function()
                mock_deps.funcs.new = function()
                    local executor = {}
                    function executor:with_actor(_actor: any) return self end
                    function executor:with_scope(_scope: any) return self end
                    function executor:call(_ref: string, args: any)
                        return {
                            success = true,
                            dataflow_id = args.dataflow_id,
                            pending = true,
                            passivated = true,
                        }, nil
                    end
                    return executor
                end
                local result, err = test_client:execute("waiting-workflow")
                test.is_nil(err)
                test.eq(result.dataflow_id, "waiting-workflow")
                test.eq(result.pending, true)
                test.eq(result.passivated, true)
                test.is_nil(result.data)
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
                    local executor = {}
                    function executor:with_actor(actor: any)
                        return self
                    end
                    function executor:with_scope(_scope: any) return self end
                    function executor:call()
                            return nil, "Orchestrator failed"
                    end
                    return executor
                end

                local result, err = test_client:execute("existing-workflow-123")

                test.is_nil(result)
                test.contains(err, "Orchestrator failed")
            end)

            it("should handle orchestrator returning workflow failure", function()
                mock_deps.funcs.new = function()
                    local executor = {}
                    function executor:with_actor(actor: any)
                        return self
                    end
                    function executor:with_scope(_scope: any) return self end
                    function executor:call()
                            return {
                                success = false,
                                dataflow_id = "existing-workflow-123",
                                error = "Workflow deadlocked"
                            }, nil
                    end
                    return executor
                end

                local result, err = test_client:execute("existing-workflow-123")

                test.not_nil(result, "result returned on failure")
                test.eq(result.success, false, "marked as failed")
                test.eq(result.error, "Workflow deadlocked", "error message in result")
                test.not_nil(err, "structured error also returned")
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
                test.eq(#captured_calls.process_with_actor, 1)
                test.is_true(captured_calls.process_with_actor[1] == mock_security_actor)
                test.eq(captured_calls.process_with_actor[1]:id(), "test-actor-123")
                test.eq(#captured_calls.process_with_scope, 1)
                test.eq(captured_calls.process_with_scope[1], "test-scope")
            end)

            it("keeps the scope captured at client creation", function()
                local captured_scope = { grants = { "agent:narrow" } }
                local replacement_scope = { grants = { "application:wide" } }
                mock_deps.security.scope = function() return captured_scope end
                test_client, _ = client.new(mock_deps)
                mock_deps.security.scope = function() return replacement_scope end

                local dataflow_id, err = test_client:start("existing-workflow-456")

                test.is_nil(err)
                test.eq(dataflow_id, "existing-workflow-456")
                test.eq(#captured_calls.process_with_scope, 1)
                test.is_true(captured_calls.process_with_scope[1] == captured_scope)
                test.is_false(captured_calls.process_with_scope[1] == replacement_scope)
            end)

            it("does not depend on a separate wake supervisor during first launch", function()
                mock_deps.process.send = function()
                    error("workflow launch must not message a wake supervisor")
                end
                mock_deps.process.terminate = function()
                    error("workflow launch must not terminate a successful native spawn")
                end

                local dataflow_id, err = test_client:start("first-workflow")

                test.is_nil(err)
                test.eq(dataflow_id, "first-workflow")
                test.eq(#captured_calls.process_spawn, 1)
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

            it("should thread on_complete hook into orchestrator args", function()
                local dataflow_id, err = test_client:start("existing-workflow-456", {
                    on_complete = "app:notify_complete"
                })

                test.is_nil(err)
                test.eq(dataflow_id, "existing-workflow-456")

                local spawn_call = captured_calls.process_spawn[1]
                test.eq(spawn_call.args.on_complete, "app:notify_complete")
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

            it("should cancel a parked run via direct status update when no live process exists", function()
                mock_deps.process.registry.lookup = function() return nil end
                mock_deps.dataflow_repo.get_by_user = function()
                    return { status = consts.STATUS.WAITING }, nil
                end

                local success, err, info = test_client:cancel("workflow-123")

                test.is_true(success)
                test.is_nil(err)
                test.eq(#captured_calls.process_cancel, 0)
                test.eq(#captured_calls.commit_execute, 1)

                local commit_call = captured_calls.commit_execute[1]
                test.eq(commit_call.commands[1].type, consts.COMMAND_TYPES.UPDATE_WORKFLOW)
                test.eq(commit_call.commands[1].payload.status, consts.STATUS.CANCELLED)

                local i = info :: any
                test.is_false(i.process_cancelled)
                test.is_true(i.status_updated)
            end)

            it("should fail when parked-run cancel commit fails", function()
                mock_deps.process.registry.lookup = function() return nil end
                mock_deps.commit.execute = function() return nil, "Commit failed" end

                local success, err = test_client:cancel("workflow-123")

                test.is_false(success)
                test.contains(err, "Failed to cancel workflow: Commit failed")
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

        describe("Signal Method", function()
            local test_client: any

            before_each(function()
                test_client, _ = client.new(mock_deps)
                test.not_nil(test_client._deps)
            end)

            it("should write a durable NODE_SIGNAL commit for a running workflow", function()
                local result, err = test_client:signal("workflow-123", "approve", { decision = "yes" })

                test.is_nil(err)
                test.not_nil(result)
                test.eq(#captured_calls.commit_submit, 1)

                local submit_call = captured_calls.commit_submit[1]
                test.eq(submit_call.dataflow_id, "workflow-123")
                test.eq(submit_call.commands[1].type, consts.COMMAND_TYPES.CREATE_DATA)
                test.eq(submit_call.commands[1].payload.data_type, consts.DATA_TYPE.NODE_SIGNAL)
                test.eq(submit_call.commands[1].payload.key, "approve")
            end)

            it("should not respawn when the orchestrator is alive", function()
                local _, err = test_client:signal("workflow-123", "approve", {})

                test.is_nil(err)
                test.eq(#captured_calls.process_spawn, 0)
            end)

            it("should respawn the orchestrator when it is dead", function()
                mock_deps.process.registry.lookup = function() return nil end

                local _, err = test_client:signal("workflow-123", "approve", {})

                test.is_nil(err)
                test.eq(#captured_calls.commit_submit, 1)
                test.eq(#captured_calls.process_spawn, 1)
                test.eq(captured_calls.process_spawn[1].args.dataflow_id, "workflow-123")
            end)

            it("should refuse to signal a terminal workflow", function()
                mock_deps.dataflow_repo.get = function(dataflow_id: string)
                    return {
                        dataflow_id = dataflow_id,
                        status = consts.STATUS.COMPLETED_SUCCESS,
                        actor_id = "test-actor-123"
                    }, nil
                end

                local result, err = test_client:signal("workflow-123", "approve", {})

                test.is_nil(result)
                test.not_nil(err)
                test.contains(tostring(err), "terminal state")
                test.eq(#captured_calls.commit_submit, 0)
            end)

            it("should refuse to signal a missing workflow", function()
                mock_deps.dataflow_repo.get = function() return nil, "Workflow not found" end

                local result, err = test_client:signal("workflow-123", "approve", {})

                test.is_nil(result)
                test.not_nil(err)
                test.contains(tostring(err), "not found")
                test.eq(#captured_calls.commit_submit, 0)
            end)

            it("should fail with missing signal_id", function()
                local result, err = test_client:signal("workflow-123", "")

                test.is_nil(result)
                test.contains(err, "Signal ID is required")
            end)

            it("should fail with missing dataflow_id", function()
                local result, err = test_client:signal("", "approve")

                test.is_nil(result)
                test.contains(err, "Workflow ID is required")
            end)
        end)

        describe("Revive Method", function()
            local test_client: any

            before_each(function()
                test_client, _ = client.new(mock_deps)
                test.not_nil(test_client._deps)
            end)

            it("should return the live pid without respawning", function()
                local pid, err, info = test_client:revive("workflow-123")

                test.is_nil(err)
                test.eq(pid, "mock-registry-pid-789")
                test.eq(#captured_calls.process_spawn, 0)
                local i = info :: any
                test.is_false(i.spawned)
            end)

            it("should respawn the orchestrator when none is registered", function()
                mock_deps.process.registry.lookup = function() return nil end

                local pid, err, info = test_client:revive("workflow-123")

                test.is_nil(err)
                test.eq(pid, "mock-pid-456")
                test.eq(#captured_calls.process_spawn, 1)
                test.eq(captured_calls.process_spawn[1].args.dataflow_id, "workflow-123")
                local i = info :: any
                test.is_true(i.spawned)
            end)

            it("should use execution identity contract when reviving another actor's workflow", function()
                mock_deps.process.registry.lookup = function() return nil end
                mock_deps.dataflow_repo.get = function(dataflow_id: string)
                    return {
                        dataflow_id = dataflow_id,
                        actor_id = "other-actor",
                        actor_context = '{"scope":"captured"}',
                        status = "running"
                    }, nil
                end
                mock_deps.identity_contract = {
                    spawn_orchestrator = function(_self: any, args: any): any
                        table.insert(captured_calls.identity_spawn, args)
                        return { success = true, pid = "contract-pid-999" }
                    end,
                }
                test_client, _ = client.new(mock_deps)

                local pid, err, info = test_client:revive("workflow-456")

                test.is_nil(err)
                test.eq(pid, "contract-pid-999")
                test.eq(#captured_calls.process_spawn, 0)
                test.eq(#captured_calls.identity_spawn, 1)
                test.eq(captured_calls.identity_spawn[1].dataflow_id, "workflow-456")
                test.eq(captured_calls.identity_spawn[1].process_id, consts.ORCHESTRATOR)
                test.eq(captured_calls.identity_spawn[1].host_id, consts.HOST_ID)
                test.eq(captured_calls.identity_spawn[1].args.dataflow_id, "workflow-456")
                local i = info :: any
                test.is_true(i.spawned)
            end)

            it("should fail cross-actor revive when execution identity contract is unbound", function()
                mock_deps.process.registry.lookup = function() return nil end
                mock_deps.dataflow_repo.get = function(dataflow_id: string)
                    return {
                        dataflow_id = dataflow_id,
                        actor_id = "other-actor",
                        status = "running"
                    }, nil
                end
                mock_deps.identity_contract = false
                test_client, _ = client.new(mock_deps)

                local pid, err = test_client:revive("workflow-456")

                test.is_nil(pid)
                test.contains(err, "execution identity contract is not bound")
            end)

            it("should fail cross-actor revive without crashing when contract definition is unbound", function()
                mock_deps.process.registry.lookup = function() return nil end
                mock_deps.dataflow_repo.get = function(dataflow_id: string)
                    return {
                        dataflow_id = dataflow_id,
                        actor_id = "other-actor",
                        status = "running"
                    }, nil
                end
                mock_deps.identity_contract = nil
                mock_deps.contract = {
                    get = function(contract_id: string)
                        test.eq(contract_id, "userspace.dataflow:execution_identity")
                        return {
                            with_actor = function(_self: any, _actor: any)
                                return nil, "contract binding missing"
                            end,
                        }, nil
                    end,
                }
                test_client, _ = client.new(mock_deps)

                local pid, err = test_client:revive("workflow-456")

                test.is_nil(pid)
                test.contains(err, "execution identity contract is not bound")
                test.eq(#captured_calls.process_spawn, 0)
                test.eq(#captured_calls.identity_spawn, 0)
            end)

            it("should fail with missing dataflow_id", function()
                local pid, err = test_client:revive("")

                test.is_nil(pid)
                test.contains(err, "Workflow ID is required")
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

            it("should preserve error field on success result for partial-success workflows", function()
                mock_deps.funcs.new = function()
                    local executor = {}
                    function executor:with_actor(actor: any)
                        return self
                    end
                    function executor:with_scope(_scope: any) return self end
                    function executor:call()
                            return {
                                success = true,
                                dataflow_id = "partial-workflow",
                                error = "Node execution failed"
                            }, nil
                    end
                    return executor
                end

                local result, err = test_client:execute("partial-workflow")

                -- success=true takes precedence; error field is informational
                test.not_nil(result, "result returned on partial success")
                test.eq(result.success, true, "success flag honored")
                test.eq(result.error, "Node execution failed", "error preserved in result")
                test.is_nil(err, "no structured error when success=true")
            end)

            it("BC_REGRESSION_C1_client_execute_returns_result_with_failure_marker", function()
                local real_client, client_err = client.new()
                test.is_nil(client_err)
                test.not_nil(real_client)

                local node_id = uuid.v7()
                local input_data_id = uuid.v7()
                local node_input_id = uuid.v7()

                local dataflow_id, create_err = (real_client :: any):create_workflow({
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = node_id,
                            node_type = "userspace.dataflow.node.func:node",
                            status = consts.STATUS.PENDING,
                            config = {
                                func_id = "userspace.dataflow.node:nonexistent_func",
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                        key = "result",
                                        content_type = consts.CONTENT_TYPE.JSON
                                    }
                                }
                            }
                        }
                    },
                    {
                        type = consts.COMMAND_TYPES.CREATE_DATA,
                        payload = {
                            data_id = input_data_id,
                            data_type = consts.DATA_TYPE.WORKFLOW_INPUT,
                            content = { message = "bc regression" },
                            content_type = consts.CONTENT_TYPE.JSON
                        }
                    },
                    {
                        type = consts.COMMAND_TYPES.CREATE_DATA,
                        payload = {
                            data_id = node_input_id,
                            data_type = consts.DATA_TYPE.NODE_INPUT,
                            node_id = node_id,
                            key = input_data_id,
                            discriminator = "default",
                            content = "",
                            content_type = consts.CONTENT_TYPE.REFERENCE
                        }
                    }
                }, { metadata = { title = "BC C1 execute failure contract" } })
                test.is_nil(create_err)
                test.not_nil(dataflow_id)

                local result, err = (real_client :: any):execute(dataflow_id :: string)

                -- execute returns both values on failure: result table AND
                -- a structured error, so callers can use either `if err then`
                -- or `if not result.success then`.
                test.not_nil(result, "result table always returned")
                test.not_nil(err, "error always surfaced on failure")
                test.eq(result.success, false, "result.success is false")
                test.eq(type(result.error), "string", "result.error is a non-empty string")
                test.is_true(#result.error > 0)
            end)
        end)
    end)
end

return test.run_cases(define_tests)
