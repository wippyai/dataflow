local test = require("test")
local uuid = require("uuid")
local json = require("json")
local client = require("client")
local consts = require("consts")
local data_reader = require("data_reader")
local node_reader = require("node_reader")

local function define_tests()
    describe("Function Node Integration Tests", function()
        describe("Native failure persistence", function()
            for _, case in ipairs({
                { name = "retryable", retryable = true },
                { name = "non-retryable", retryable = false },
                { name = "unknown" },
            }) do
                it("preserves " .. case.name .. " function causes across persistence and execution", function()
                    local c, client_err = client.new()
                    test.is_nil(client_err)
                    local failed_id, handler_id = uuid.v7(), uuid.v7()
                    local dataflow_id, create_err = c:create_workflow({
                        { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                            node_id = failed_id, node_type = "userspace.dataflow.node.func:node",
                            status = consts.STATUS.PENDING, config = {
                                func_id = "userspace.dataflow.node.func:native_error_func",
                                error_targets = { { data_type = consts.DATA_TYPE.NODE_INPUT,
                                    node_id = handler_id, key = "default" } }
                            }
                        } },
                        { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                            node_id = handler_id, node_type = "userspace.dataflow.node.func:node",
                            status = consts.STATUS.PENDING, config = {
                                func_id = "userspace.dataflow.node.func:test_func"
                            }
                        } },
                        { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                            data_id = uuid.v7(), data_type = consts.DATA_TYPE.NODE_INPUT,
                            node_id = failed_id, key = "default", content_type = consts.CONTENT_TYPE.JSON,
                            content = { kind = "Unavailable", message = "Service unavailable",
                                retryable = case.retryable, details = { code = "X" } }
                        } },
                    })
                    test.is_nil(create_err)
                    local result, exec_err = c:execute(dataflow_id)
                    test.not_nil(result)
                    test.is_false(result.success)
                    test.not_nil(exec_err)
                    local function assert_cause(payload)
                        test.eq(payload.code, "FUNCTION_EXECUTION_FAILED")
                        test.eq(payload.message, "Service unavailable")
                        test.not_nil(payload.cause)
                        test.eq(payload.cause.kind, "Unavailable")
                        test.eq(payload.cause.message, "Service unavailable")
                        test.eq(payload.cause.retryable, case.retryable)
                        test.eq(payload.cause.details.code, "X")
                    end
                    local failed = node_reader.with_dataflow(dataflow_id):with_nodes(failed_id):one()
                    test.not_nil(failed)
                    assert_cause(failed.metadata.error)
                    local rows = data_reader.with_dataflow(dataflow_id):with_nodes(failed_id)
                        :with_data_types(consts.DATA_TYPE.NODE_RESULT):all()
                    test.eq(#rows, 1)
                    test.eq(rows[1].discriminator, "result.error")
                    local persisted = json.decode(rows[1].content)
                    assert_cause(persisted.error)
                    local routed = data_reader.with_dataflow(dataflow_id):with_nodes(handler_id)
                        :with_data_types(consts.DATA_TYPE.NODE_INPUT)
                        :fetch_options({ replace_references = true }):one()
                    test.not_nil(routed)
                    assert_cause(json.decode(routed.content))
                    test.eq(#result.error_details, 1)
                    assert_cause(result.error_details[1].error)
                    assert_cause(exec_err:details().error_details[1].error)
                end)
            end
        end)

        describe("Error routing failures", function()
            for _, case in ipairs({
                { name = "condition", expression = "missing.value", expected = "Error condition evaluation failed" },
                { name = "transform", expression = "missing(error.message)", expected = "Error transform failed" },
            }) do
                it("persists the primary failure and the " .. case.name .. " failure without partial outputs", function()
                    local c, client_err = client.new()
                    test.is_nil(client_err)
                    local failed_id = uuid.v7()
                    local bad_target: any = { data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "invalid" }
                    bad_target[case.name] = case.expression
                    local dataflow_id, create_err = c:create_workflow({
                        { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                            node_id = failed_id, node_type = "userspace.dataflow.node.func:node",
                            status = consts.STATUS.PENDING, config = {
                                func_id = "userspace.dataflow.node.func:native_error_func",
                                error_targets = {
                                    { data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "first" },
                                    bad_target,
                                    { data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "last" }
                                }
                            }
                        } },
                        { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                            data_id = uuid.v7(), data_type = consts.DATA_TYPE.NODE_INPUT,
                            node_id = failed_id, key = "default", content_type = consts.CONTENT_TYPE.JSON,
                            content = { kind = "Unavailable", message = "Service unavailable",
                                retryable = true, details = { code = "X" } }
                        } },
                    })
                    test.is_nil(create_err)
                    local result, exec_err = c:execute(dataflow_id)
                    test.not_nil(exec_err)
                    test.is_false(result.success)
                    local function assert_failures(payload)
                        test.contains(payload.message, case.expected)
                        test.contains(payload.message, case.expression)
                        test.eq(payload.error.code, "FUNCTION_EXECUTION_FAILED")
                        test.eq(payload.error.message, "Service unavailable")
                        test.eq(payload.error.cause.kind, "Unavailable")
                        test.is_true(payload.error.cause.retryable)
                        test.eq(payload.error.cause.details.code, "X")
                    end
                    local failed = node_reader.with_dataflow(dataflow_id):with_nodes(failed_id):one()
                    assert_failures(failed.metadata.error)
                    local row = data_reader.with_dataflow(dataflow_id):with_nodes(failed_id)
                        :with_data_types(consts.DATA_TYPE.NODE_RESULT):one()
                    test.not_nil(row)
                    test.eq(row.discriminator, "result.error")
                    assert_failures(json.decode(row.content).error)
                    test.eq(#data_reader.with_dataflow(dataflow_id)
                        :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT):all(), 0)
                    test.eq(#result.error_details, 1)
                    assert_failures(result.error_details[1].error)
                    assert_failures(exec_err:details().error_details[1].error)
                end)
            end
        end)

        describe("Basic Function Execution", function()
            it("should execute test_func successfully via func node", function()
                print("=== FUNC NODE INTEGRATION TEST START ===")

                local c, err = client.new()
                test.is_nil(err)
                test.not_nil(c)

                local node_id: string = uuid.v7()
                local input_data_id: string = uuid.v7()
                local node_input_id: string = uuid.v7()

                local test_input = {
                    message = "Function node test",
                    delay_ms = 50,
                    should_fail = false
                }

                local workflow_commands = {
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = node_id,
                            node_type = "userspace.dataflow.node.func:node",
                            status = consts.STATUS.PENDING,
                            config = {
                                func_id = "userspace.dataflow.node.func:test_func",
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                        key = "result",
                                        content_type = consts.CONTENT_TYPE.JSON
                                    }
                                }
                            },
                            metadata = {
                                title = "Basic Function Execution Test Node"
                            }
                        }
                    },
                    {
                        type = consts.COMMAND_TYPES.CREATE_DATA,
                        payload = {
                            data_id = input_data_id,
                            data_type = consts.DATA_TYPE.WORKFLOW_INPUT,
                            content = test_input,
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
                            content_type = "dataflow/reference"
                        }
                    }
                }

                local dataflow_id, create_err = c:create_workflow(workflow_commands, {
                    metadata = {
                        title = "Basic Function Execution Test Workflow"
                    }
                })
                test.is_nil(create_err)
                test.not_nil(dataflow_id)

                local result, exec_err = c:execute(dataflow_id)
                test.is_nil(exec_err)
                test.not_nil(result)
                test.is_true(result.success)

                -- Verify output was created
                local output_data = data_reader.with_dataflow(dataflow_id)
                    :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                    :fetch_options({ replace_references = true })
                    :one()

                test.not_nil(output_data)

                local output_content: any = output_data.content
                if type(output_content) == "string" then
                    local decoded, _decode_err = json.decode(output_content)
                    if not _decode_err then
                        output_content = decoded
                    end
                end

                test.eq(output_content.message, "Function node test")
                test.eq(output_content.processed_by, "test_function")
                test.is_true(output_content.success)

                print("Function node executed successfully via test_func")
            end)

            it("should handle function failure correctly", function()
                local c, err = client.new()
                test.is_nil(err)

                local node_id: string = uuid.v7()
                local input_data_id: string = uuid.v7()
                local node_input_id: string = uuid.v7()

                local test_input = {
                    message = "Should fail",
                    should_fail = true
                }

                local workflow_commands = {
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = node_id,
                            node_type = "userspace.dataflow.node.func:node",
                            status = consts.STATUS.PENDING,
                            config = {
                                func_id = "userspace.dataflow.node.func:test_func",
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                        key = "result"
                                    }
                                },
                                error_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                        key = "error"
                                    }
                                }
                            },
                            metadata = {
                                title = "Function Failure Test Node"
                            }
                        }
                    },
                    {
                        type = consts.COMMAND_TYPES.CREATE_DATA,
                        payload = {
                            data_id = input_data_id,
                            data_type = consts.DATA_TYPE.WORKFLOW_INPUT,
                            content = test_input,
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
                            content_type = "dataflow/reference"
                        }
                    }
                }

                local dataflow_id, create_err = c:create_workflow(workflow_commands, {
                    metadata = {
                        title = "Function Failure Test Workflow"
                    }
                })
                test.is_nil(create_err)

                local result, exec_err = c:execute(dataflow_id)
                test.is_nil(exec_err)
                test.is_true(result.success)

                -- Check that error was routed via error_targets
                local error_outputs = data_reader.with_dataflow(dataflow_id)
                    :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                    :with_data_keys("error")
                    :fetch_options({ replace_references = true })
                    :all()

                test.gt(#error_outputs, 0)

                print("Function node handled failure and routed error correctly")
            end)

            it("should fail when func_id is missing", function()
                local c, err = client.new()
                test.is_nil(err)

                local node_id: string = uuid.v7()
                local input_data_id: string = uuid.v7()
                local node_input_id: string = uuid.v7()

                local test_input = { message = "test" }

                local workflow_commands = {
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = node_id,
                            node_type = "userspace.dataflow.node.func:node",
                            status = consts.STATUS.PENDING,
                            config = {
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                        key = "result"
                                    }
                                }
                            },
                            metadata = {
                                title = "Missing Func ID Test Node"
                            }
                        }
                    },
                    {
                        type = consts.COMMAND_TYPES.CREATE_DATA,
                        payload = {
                            data_id = input_data_id,
                            data_type = consts.DATA_TYPE.WORKFLOW_INPUT,
                            content = test_input,
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
                            content_type = "dataflow/reference"
                        }
                    }
                }

                local dataflow_id, create_err = c:create_workflow(workflow_commands, {
                    metadata = {
                        title = "Missing Func ID Test Workflow"
                    }
                })
                test.is_nil(create_err)

                local result, exec_err = c:execute(dataflow_id)
                test.not_nil(exec_err)
                test.is_false(result.success)
                test.contains(result.error, "Function ID not specified")

                print("Function node correctly failed with missing func_id")
            end)

            it("should fail when no input data provided", function()
                local c, err = client.new()
                test.is_nil(err)

                local node_id: string = uuid.v7()

                local workflow_commands = {
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = node_id,
                            node_type = "userspace.dataflow.node.func:node",
                            status = consts.STATUS.PENDING,
                            config = {
                                func_id = "userspace.dataflow.node.func:test_func",
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                        key = "result"
                                    }
                                }
                            },
                            metadata = {
                                title = "No Input Data Test Node"
                            }
                        }
                    }
                }

                local dataflow_id, create_err = c:create_workflow(workflow_commands, {
                    metadata = {
                        title = "No Input Data Test Workflow"
                    }
                })
                test.is_nil(create_err)

                local result, exec_err = c:execute(dataflow_id)
                test.not_nil(exec_err)
                test.is_false(result.success)
                test.contains(result.error, "No input data provided")

                print("Function node correctly failed with no input data")
            end)

            it("should fail when function does not exist", function()
                local c, err = client.new()
                test.is_nil(err)

                local node_id: string = uuid.v7()
                local input_data_id: string = uuid.v7()
                local node_input_id: string = uuid.v7()

                local test_input = { message = "test" }

                local workflow_commands = {
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
                                        key = "result"
                                    }
                                }
                            },
                            metadata = {
                                title = "Nonexistent Function Test Node"
                            }
                        }
                    },
                    {
                        type = consts.COMMAND_TYPES.CREATE_DATA,
                        payload = {
                            data_id = input_data_id,
                            data_type = consts.DATA_TYPE.WORKFLOW_INPUT,
                            content = test_input,
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
                            content_type = "dataflow/reference"
                        }
                    }
                }

                local dataflow_id, create_err = c:create_workflow(workflow_commands, {
                    metadata = {
                        title = "Nonexistent Function Test Workflow"
                    }
                })
                test.is_nil(create_err)

                local result, exec_err = c:execute(dataflow_id)
                test.not_nil(exec_err)
                test.is_false(result.success)
                test.contains(result.error, "failed")

                print("Function node correctly failed with nonexistent function")
            end)

            it("should pass context to function when configured", function()
                local c, err = client.new()
                test.is_nil(err)

                local node_id: string = uuid.v7()
                local input_data_id: string = uuid.v7()
                local node_input_id: string = uuid.v7()

                local test_input = {
                    message = "Context test",
                    delay_ms = 50
                }

                local test_context = {
                    user_id = "test_user_123",
                    environment = "integration_test"
                }

                local workflow_commands = {
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = node_id,
                            node_type = "userspace.dataflow.node.func:node",
                            status = consts.STATUS.PENDING,
                            config = {
                                func_id = "userspace.dataflow.node.func:test_func",
                                context = test_context,
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                        key = "result",
                                        content_type = consts.CONTENT_TYPE.JSON
                                    }
                                }
                            },
                            metadata = {
                                title = "Context Test Node"
                            }
                        }
                    },
                    {
                        type = consts.COMMAND_TYPES.CREATE_DATA,
                        payload = {
                            data_id = input_data_id,
                            data_type = consts.DATA_TYPE.WORKFLOW_INPUT,
                            content = test_input,
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
                            content_type = "dataflow/reference"
                        }
                    }
                }

                local dataflow_id, create_err = c:create_workflow(workflow_commands, {
                    metadata = {
                        title = "Context Test Workflow"
                    }
                })
                test.is_nil(create_err)

                local result, exec_err = c:execute(dataflow_id)
                test.is_nil(exec_err)
                test.is_true(result.success)

                -- Verify output contains context data
                local output_data = data_reader.with_dataflow(dataflow_id)
                    :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                    :fetch_options({ replace_references = true })
                    :one()

                test.not_nil(output_data)

                local output_content: any = output_data.content
                if type(output_content) == "string" then
                    local decoded, _decode_err = json.decode(output_content)
                    if not _decode_err then
                        output_content = decoded
                    end
                end

                test.eq(output_content.message, "Context test")
                test.eq(output_content.processed_by, "test_function")

                print("Function node passed context successfully")
            end)
        end)

        describe("Input Handling", function()
            it("should handle string input correctly", function()
                local c, err = client.new()
                test.is_nil(err)

                local node_id: string = uuid.v7()
                local input_data_id: string = uuid.v7()
                local node_input_id: string = uuid.v7()

                local test_input = "Simple string input"

                local workflow_commands = {
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = node_id,
                            node_type = "userspace.dataflow.node.func:node",
                            status = consts.STATUS.PENDING,
                            config = {
                                func_id = "userspace.dataflow.node.func:test_func",
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                        key = "result"
                                    }
                                }
                            },
                            metadata = {
                                title = "String Input Test Node"
                            }
                        }
                    },
                    {
                        type = consts.COMMAND_TYPES.CREATE_DATA,
                        payload = {
                            data_id = input_data_id,
                            data_type = consts.DATA_TYPE.WORKFLOW_INPUT,
                            content = test_input,
                            content_type = consts.CONTENT_TYPE.TEXT
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
                            content_type = "dataflow/reference"
                        }
                    }
                }

                local dataflow_id, create_err = c:create_workflow(workflow_commands, {
                    metadata = {
                        title = "String Input Test Workflow"
                    }
                })
                test.is_nil(create_err)

                local result, exec_err = c:execute(dataflow_id)
                test.is_nil(exec_err)
                test.is_true(result.success)

                print("Function node handled string input correctly")
            end)

            it("should use first available input when no default key", function()
                local c, err = client.new()
                test.is_nil(err)

                local node_id: string = uuid.v7()
                local input_data_id: string = uuid.v7()
                local node_input_id: string = uuid.v7()

                local test_input = { message = "Named input test" }

                local workflow_commands = {
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = node_id,
                            node_type = "userspace.dataflow.node.func:node",
                            status = consts.STATUS.PENDING,
                            config = {
                                func_id = "userspace.dataflow.node.func:test_func",
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                        key = "result"
                                    }
                                }
                            },
                            metadata = {
                                title = "Named Input Test Node"
                            }
                        }
                    },
                    {
                        type = consts.COMMAND_TYPES.CREATE_DATA,
                        payload = {
                            data_id = input_data_id,
                            data_type = consts.DATA_TYPE.WORKFLOW_INPUT,
                            content = test_input,
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
                            content = "",
                            content_type = "dataflow/reference",
                            metadata = {
                                input_key = "named_input"
                            }
                        }
                    }
                }

                local dataflow_id, create_err = c:create_workflow(workflow_commands, {
                    metadata = {
                        title = "Named Input Test Workflow"
                    }
                })
                test.is_nil(create_err)

                local result, exec_err = c:execute(dataflow_id)
                test.is_nil(exec_err)
                test.is_true(result.success)

                print("Function node used first available input correctly")
            end)

            it("BC_REGRESSION_C4_func_single_named_input_wrapped", function()
                local c, err = client.new()
                test.is_nil(err)
                test.not_nil(c)

                local node_id: string = uuid.v7()
                local input_data_id: string = uuid.v7()
                local node_input_id: string = uuid.v7()

                local test_input = {
                    message = "wrapped named input"
                }

                local workflow_commands = {
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = node_id,
                            node_type = "userspace.dataflow.node.func:node",
                            status = consts.STATUS.PENDING,
                            config = {
                                func_id = "userspace.dataflow.node.func:test_func",
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                        key = "result",
                                        content_type = consts.CONTENT_TYPE.JSON
                                    }
                                }
                            },
                            metadata = {
                                title = "Single Named Input Wrapped Regression Node"
                            }
                        }
                    },
                    {
                        type = consts.COMMAND_TYPES.CREATE_DATA,
                        payload = {
                            data_id = input_data_id,
                            data_type = consts.DATA_TYPE.WORKFLOW_INPUT,
                            content = test_input,
                            content_type = consts.CONTENT_TYPE.JSON
                        }
                    },
                    {
                        type = consts.COMMAND_TYPES.CREATE_DATA,
                        payload = {
                            data_id = node_input_id,
                            data_type = consts.DATA_TYPE.NODE_INPUT,
                            node_id = node_id,
                            discriminator = "my_data",
                            key = input_data_id,
                            content = "",
                            content_type = "dataflow/reference"
                        }
                    }
                }

                local dataflow_id, create_err = c:create_workflow(workflow_commands, {
                    metadata = {
                        title = "Single Named Input Wrapped Regression Workflow"
                    }
                })
                test.is_nil(create_err)
                test.not_nil(dataflow_id)

                local result, exec_err = c:execute(dataflow_id)
                test.is_nil(exec_err)
                test.not_nil(result)
                test.is_true(result.success)

                local output_data = data_reader.with_dataflow(dataflow_id)
                    :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                    :fetch_options({ replace_references = true })
                    :one()

                test.not_nil(output_data)

                local output_content: any = output_data.content
                if type(output_content) == "string" then
                    local decoded, _decode_err = json.decode(output_content)
                    if not _decode_err then
                        output_content = decoded
                    end
                end

                test.not_nil(output_content.input_echo)
                test.is_nil(output_content.input_echo.message)
                test.not_nil(output_content.input_echo.my_data)
                test.eq(output_content.input_echo.my_data.message, test_input.message)
            end)
        end)
    end)
end

return test.run_cases(define_tests)
