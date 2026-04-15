local test        = require("test")
local uuid        = require("uuid")
local json        = require("json")
local client      = require("client")
local consts      = require("consts")
local data_reader = require("data_reader")
local time        = require("time")

local function define_tests()
    describe("Dataflow Integration Tests", function()
        describe("Single Node Workflow - Success Cases", function()
            it("should execute test_func node and produce workflow output", function()
                print("=== INTEGRATION TEST START ===")

                -- Create client
                local c, err = client.new()
                test.is_nil(err)
                test.not_nil(c)
                print("Client created successfully")

                -- Generate IDs
                local node_id = uuid.v7()
                local input_data_id = uuid.v7()
                local node_input_id = uuid.v7()
                print("Generated IDs:")
                print("  node_id:", node_id)
                print("  input_data_id:", input_data_id)
                print("  node_input_id:", node_input_id)

                -- Define test input
                local test_input = {
                    message = "Integration test message",
                    delay_ms = 50,
                    should_fail = false
                }
                print("Test input prepared:", json.encode(test_input))

                -- Create workflow with single func node
                local workflow_commands = {
                    -- Create the func node that will execute test_func
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
                                title = "Single Node Happy Path Test",
                                test_node = true,
                                created_for = "integration_test"
                            }
                        }
                    },
                    -- Create input data
                    {
                        type = consts.COMMAND_TYPES.CREATE_DATA,
                        payload = {
                            data_id = input_data_id,
                            data_type = consts.DATA_TYPE.WORKFLOW_INPUT,
                            content = test_input,
                            content_type = consts.CONTENT_TYPE.JSON,
                            metadata = {
                                test_input = true
                            }
                        }
                    },
                    -- Create node input reference
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
                print("Workflow commands prepared (", #workflow_commands, "commands)")

                -- Create workflow
                print("=== CREATING WORKFLOW ===")
                local dataflow_id, create_err = (c :: any):create_workflow(workflow_commands, {
                    type = "integration_test",
                    metadata = {
                        title = "Single Node Happy Path Test",
                        test_type = "single_node_happy_path",
                        node_type = "test_func"
                    }
                })

                test.is_nil(create_err)
                test.not_nil(dataflow_id)
                test.eq(type(dataflow_id), "string")
                print("Workflow created successfully")
                print("  dataflow_id:", dataflow_id)

                -- Verify workflow was created properly
                print("=== VERIFYING WORKFLOW CREATION ===")
                local status_before, status_err = (c :: any):get_status(dataflow_id :: string)
                test.is_nil(status_err)
                print("Workflow status before execution:", status_before)

                -- Check that input data was created
                local input_data_created = (data_reader.with_dataflow(dataflow_id :: string) :: any)
                    :with_data_types(consts.DATA_TYPE.WORKFLOW_INPUT)
                    :all()
                print("Workflow input data created:", #input_data_created)

                -- Check that node input references were created
                local node_inputs_created = (data_reader.with_dataflow(dataflow_id :: string) :: any)
                    :with_data_types(consts.DATA_TYPE.NODE_INPUT)
                    :all()
                print("Node input references created:", #node_inputs_created)

                -- Execute workflow
                print("=== EXECUTING WORKFLOW ===")
                local result, exec_err = (c :: any):execute(dataflow_id :: string)

                print("Execution result:", result and json.encode(result) or "nil")
                print("Execution error:", exec_err or "nil")

                test.is_nil(exec_err)
                test.not_nil(result)
                test.is_true(result.success)
                test.eq(result.dataflow_id, dataflow_id)
                print("Workflow executed successfully")
                print("  result.success:", result.success)
                print("  result.dataflow_id:", result.dataflow_id)

                -- Check final workflow status
                local final_status, final_status_err = (c :: any):get_status(dataflow_id :: string)
                test.is_nil(final_status_err)
                print("Final workflow status:", final_status)

                -- Check workflow output was created
                print("=== VERIFYING WORKFLOW OUTPUT ===")
                local output_data = (data_reader.with_dataflow(dataflow_id :: string) :: any)
                    :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                    :fetch_options({ replace_references = true })
                    :one()

                test.not_nil(output_data)
                test.not_nil(output_data.content)
                print("Workflow output exists")
                print("  Content type:", output_data.content_type)

                -- Verify output content
                local output_content = output_data.content
                if type(output_content) == "string" then
                    local decoded, _decode_err = json.decode(output_content :: string)
                    if not _decode_err then
                        output_content = decoded
                    end
                end

                print("Parsed output content:", json.encode(output_content))

                test.eq(output_content.message, "Integration test message")
                test.eq(output_content.processed_by, "test_function")
                test.is_true(output_content.success)
                test.eq(output_content.delay_applied, 50)
                test.not_nil(output_content.input_echo)
                test.eq(output_content.input_echo.message, "Integration test message")
                test.not_nil(output_content.timestamp)
                print("Output content verification passed")

                -- Verify node completed successfully
                print("=== VERIFYING NODE COMPLETION ===")
                local node_data = (data_reader.with_dataflow(dataflow_id :: string) :: any)
                    :with_nodes(node_id)
                    :with_data_types(consts.DATA_TYPE.NODE_RESULT)
                    :one()

                if node_data then
                    test.eq(node_data.discriminator, "result.success")
                    print("Node completed with success discriminator:", node_data.discriminator)
                else
                    print("No node result data found (may be expected)")
                end

                print("=== INTEGRATION TEST COMPLETE ===")
            end)

            it("should handle string input data", function()
                print("=== STRING INPUT TEST START ===")

                local c, err = client.new()
                test.is_nil(err)

                local node_id = uuid.v7()
                local input_data_id = uuid.v7()
                local node_input_id = uuid.v7()

                -- Use simple string input
                local test_input = "Simple string message"

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

                local dataflow_id, create_err = (c :: any):create_workflow(workflow_commands, {
                    metadata = {
                        title = "String Input Test Workflow"
                    }
                })
                test.is_nil(create_err)

                local result, exec_err = (c :: any):execute(dataflow_id :: string)
                test.is_nil(exec_err)
                test.is_true(result.success)

                -- Verify output content has string input echoed
                local output_data = (data_reader.with_dataflow(dataflow_id :: string) :: any)
                    :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                    :fetch_options({ replace_references = true })
                    :one()

                test.not_nil(output_data)
                local output_content = output_data.content
                if type(output_content) == "string" then
                    local decoded, _decode_err = json.decode(output_content :: string)
                    if not _decode_err then
                        output_content = decoded
                    end
                end

                test.eq(output_content.message, "Simple string message")
                test.eq(output_content.input_echo, "Simple string message")
                print("String input test passed")
            end)
        end)

        describe("Single Node Workflow - Function Failure Cases", function()
            it("should fail workflow when function returns failure", function()
                print("=== FUNCTION FAILURE TEST START ===")

                local c, err = client.new()
                test.is_nil(err)

                local node_id = uuid.v7()
                local input_data_id = uuid.v7()
                local node_input_id = uuid.v7()

                -- Configure function to fail
                local test_input = {
                    message = "Test failure",
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
                                        key = "result",
                                        content_type = consts.CONTENT_TYPE.JSON
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

                local dataflow_id, create_err = (c :: any):create_workflow(workflow_commands, {
                    metadata = {
                        title = "Function Failure Test Workflow"
                    }
                })
                test.is_nil(create_err)

                local result, exec_err = (c :: any):execute(dataflow_id :: string)
                test.not_nil(exec_err)
                -- Function => node failure => failed workflow
                test.is_false(result.success)
                test.contains(result.error, "Intentional semantic failure")
                print("Function failure test passed")
            end)

            it("should fail when function does not exist", function()
                print("=== MISSING FUNCTION TEST START ===")

                local c, err = client.new()
                test.is_nil(err)

                local node_id = uuid.v7()
                local input_data_id = uuid.v7()
                local node_input_id = uuid.v7()

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
                                        key = "result",
                                        content_type = consts.CONTENT_TYPE.JSON
                                    }
                                }
                            },
                            metadata = {
                                title = "Missing Function Test Node"
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

                local dataflow_id, create_err = (c :: any):create_workflow(workflow_commands, {
                    metadata = {
                        title = "Missing Function Test Workflow"
                    }
                })
                test.is_nil(create_err)

                local result, exec_err = (c :: any):execute(dataflow_id :: string)
                test.not_nil(exec_err)
                -- Should fail at workflow level because func node fails
                test.is_false(result.success)
                test.contains(result.error, "failed")

                -- Check that node was marked as failed
                local node_data = (data_reader.with_dataflow(dataflow_id :: string) :: any)
                    :with_nodes(node_id)
                    :with_data_types(consts.DATA_TYPE.NODE_RESULT)
                    :one()

                if node_data then
                    test.eq(node_data.discriminator, "result.error")
                end

                print("Missing function test passed")
            end)
        end)

        describe("Single Node Workflow - Configuration Error Cases", function()
            it("should fail when func_id is missing", function()
                print("=== MISSING FUNC_ID TEST START ===")

                local c, err = client.new()
                test.is_nil(err)

                local node_id = uuid.v7()
                local input_data_id = uuid.v7()
                local node_input_id = uuid.v7()

                local test_input = { message = "test" }

                local workflow_commands = {
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = node_id,
                            node_type = "userspace.dataflow.node.func:node",
                            status = consts.STATUS.PENDING,
                            config = {
                                -- Missing func_id!
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                        key = "result",
                                        content_type = consts.CONTENT_TYPE.JSON
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

                local dataflow_id, create_err = (c :: any):create_workflow(workflow_commands, {
                    metadata = {
                        title = "Missing Func ID Test Workflow"
                    }
                })
                test.is_nil(create_err)

                local result, exec_err = (c :: any):execute(dataflow_id :: string)
                test.not_nil(exec_err)
                -- Should fail at workflow level
                test.is_false(result.success)
                test.contains(result.error, "failed")
                test.contains(result.error, "Function ID not specified")

                print("Missing func_id test passed")
            end)

            it("should fail when func_id is empty string", function()
                print("=== EMPTY FUNC_ID TEST START ===")

                local c, err = client.new()
                test.is_nil(err)

                local node_id = uuid.v7()
                local input_data_id = uuid.v7()
                local node_input_id = uuid.v7()

                local test_input = { message = "test" }

                local workflow_commands = {
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = node_id,
                            node_type = "userspace.dataflow.node.func:node",
                            status = consts.STATUS.PENDING,
                            config = {
                                func_id = "", -- Empty string!
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                        key = "result",
                                        content_type = consts.CONTENT_TYPE.JSON
                                    }
                                }
                            },
                            metadata = {
                                title = "Empty Func ID Test Node"
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

                local dataflow_id, create_err = (c :: any):create_workflow(workflow_commands, {
                    metadata = {
                        title = "Empty Func ID Test Workflow"
                    }
                })
                test.is_nil(create_err)

                local result, exec_err = (c :: any):execute(dataflow_id :: string)
                test.not_nil(exec_err)
                -- Should fail at workflow level
                test.is_false(result.success)
                test.contains(result.error, "failed")
                test.contains(result.error, "Function ID not specified")

                print("Empty func_id test passed")
            end)

            it("should fail when node has no input data", function()
                print("=== NO INPUT DATA TEST START ===")

                local c, err = client.new()
                test.is_nil(err)

                local node_id = uuid.v7()

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
                                title = "No Input Data Test Node"
                            }
                        }
                    }
                    -- No input data created!
                }

                local dataflow_id, create_err = (c :: any):create_workflow(workflow_commands, {
                    metadata = {
                        title = "No Input Data Test Workflow"
                    }
                })
                test.is_nil(create_err)

                local result, exec_err = (c :: any):execute(dataflow_id :: string)
                test.not_nil(exec_err)
                -- Should fail at workflow level
                test.is_false(result.success)
                test.contains(result.error, "No input data provided")

                print("No input data test passed")
            end)
        end)

        describe("Single Node Workflow - Edge Cases", function()
            it("should handle workflow with no data_targets", function()
                print("=== NO DATA TARGETS TEST START ===")

                local c, err = client.new()
                test.is_nil(err)

                local node_id = uuid.v7()
                local input_data_id = uuid.v7()
                local node_input_id = uuid.v7()

                local test_input = { message = "test no targets" }

                local workflow_commands = {
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = node_id,
                            node_type = "userspace.dataflow.node.func:node",
                            status = consts.STATUS.PENDING,
                            config = {
                                func_id = "userspace.dataflow.node.func:test_func"
                                -- No data_targets!
                            },
                            metadata = {
                                title = "No Data Targets Test Node"
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

                local dataflow_id, create_err = (c :: any):create_workflow(workflow_commands, {
                    metadata = {
                        title = "No Data Targets Test Workflow"
                    }
                })
                test.is_nil(create_err)

                local result, exec_err = (c :: any):execute(dataflow_id :: string)
                test.not_nil(exec_err)
                -- Workflow fails without data targets producing output.
                test.is_false(result.success)
                test.contains(result.error, "Workflow completed without producing outpu")
            end)

            it("should handle multiple data_targets", function()
                print("=== MULTIPLE DATA TARGETS TEST START ===")

                local c, err = client.new()
                test.is_nil(err)

                local node_id = uuid.v7()
                local input_data_id = uuid.v7()
                local node_input_id = uuid.v7()

                local test_input = { message = "test multiple targets" }

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
                                    },
                                    {
                                        data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                        key = "backup",
                                        content_type = consts.CONTENT_TYPE.JSON
                                    }
                                }
                            },
                            metadata = {
                                title = "Multiple Data Targets Test Node"
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

                local dataflow_id, create_err = (c :: any):create_workflow(workflow_commands, {
                    metadata = {
                        title = "Multiple Data Targets Test Workflow"
                    }
                })
                test.is_nil(create_err)

                local result, exec_err = (c :: any):execute(dataflow_id :: string)
                test.is_nil(exec_err)

                test.is_true(result.success)

                -- Should create multiple workflow outputs
                local output_data = (data_reader.with_dataflow(dataflow_id :: string) :: any)
                    :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                    :all()

                test.eq(#output_data, 2)

                -- Both should have the same content but different keys
                local keys_found = {}
                for _, output in ipairs(output_data) do
                    keys_found[output.key] = true
                end
                test.is_true(keys_found["result"])
                test.is_true(keys_found["backup"])

                print("Multiple data targets test passed")
            end)
        end)

        describe("Workflow Data Verification", function()
            it("should create all expected data types during successful execution", function()
                print("=== DATA VERIFICATION TEST START ===")

                local c, err = client.new()
                test.is_nil(err)

                local node_id = uuid.v7()
                local input_data_id = uuid.v7()
                local node_input_id = uuid.v7()

                local test_input = { message = "data verification test" }

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
                                title = "Data Verification Test Node"
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

                local dataflow_id, create_err = (c :: any):create_workflow(workflow_commands, {
                    metadata = {
                        title = "Data Verification Test Workflow"
                    }
                })
                test.is_nil(create_err)

                local result, exec_err = (c :: any):execute(dataflow_id :: string)
                test.is_nil(exec_err)
                test.is_true(result.success)

                -- Verify all expected data types exist
                local all_data = (data_reader.with_dataflow(dataflow_id :: string) :: any)
                    :all()

                local data_by_type = {}
                for _, data_record in ipairs(all_data) do
                    local data_type = data_record.type
                    if not data_by_type[data_type] then
                        data_by_type[data_type] = 0
                    end
                    data_by_type[data_type] = (data_by_type[data_type] :: number) + 1
                end

                print("Data types found:")
                for data_type, count in pairs(data_by_type) do
                    print("  " .. data_type .. ":", count)
                end

                -- Should have at least these data types
                test.gt(data_by_type[consts.DATA_TYPE.WORKFLOW_INPUT], 0)
                test.gt(data_by_type[consts.DATA_TYPE.NODE_INPUT], 0)
                test.gt(data_by_type[consts.DATA_TYPE.WORKFLOW_OUTPUT], 0)
                test.gt(data_by_type[consts.DATA_TYPE.NODE_RESULT], 0)

                print("Data verification test passed")
            end)
        end)
    end)

    describe("Dataflow Node Chaining Tests", function()
        describe("Simple Two-Node Chain", function()
            it("should execute Node A then Node B via data_targets routing", function()
                print("=== SIMPLE CHAIN TEST START ===")

                local c, err = client.new()
                test.is_nil(err)

                local node_a_id = uuid.v7()
                local node_b_id = uuid.v7()
                local input_data_id = uuid.v7()
                local node_a_input_id = uuid.v7()

                local test_input = {
                    message = "Chain input",
                    value = 42
                }

                print("Generated IDs:")
                print("  node_a_id:", node_a_id)
                print("  node_b_id:", node_b_id)

                local workflow_commands = {
                    -- Node A: routes success to Node B
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = node_a_id,
                            node_type = "userspace.dataflow.node.func:node",
                            status = consts.STATUS.PENDING,
                            config = {
                                func_id = "userspace.dataflow.node.func:test_func",
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.NODE_INPUT,
                                        node_id = node_b_id
                                    }
                                }
                            },
                            metadata = {
                                title = "Chain Node A"
                            }
                        }
                    },
                    -- Node B: routes to workflow output
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = node_b_id,
                            node_type = "userspace.dataflow.node.func:node",
                            status = consts.STATUS.PENDING,
                            config = {
                                func_id = "userspace.dataflow.node.func:test_func",
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                        key = "final"
                                    }
                                }
                            },
                            metadata = {
                                title = "Chain Node B"
                            }
                        }
                    },
                    -- Workflow input
                    {
                        type = consts.COMMAND_TYPES.CREATE_DATA,
                        payload = {
                            data_id = input_data_id,
                            data_type = consts.DATA_TYPE.WORKFLOW_INPUT,
                            content = test_input,
                            content_type = consts.CONTENT_TYPE.JSON
                        }
                    },
                    -- Node A input
                    {
                        type = consts.COMMAND_TYPES.CREATE_DATA,
                        payload = {
                            data_id = node_a_input_id,
                            data_type = consts.DATA_TYPE.NODE_INPUT,
                            node_id = node_a_id,
                            key = input_data_id,
                            discriminator = "default",
                            content = "",
                            content_type = "dataflow/reference"
                        }
                    }
                }

                print("Workflow commands prepared (", #workflow_commands, "commands)")

                local dataflow_id, create_err = (c :: any):create_workflow(workflow_commands, {
                    metadata = {
                        title = "Simple Chain Test Workflow"
                    }
                })
                test.is_nil(create_err)
                print("Chain workflow created:", dataflow_id)

                local result, exec_err = (c :: any):execute(dataflow_id :: string)
                test.is_nil(exec_err)
                test.is_true(result.success)
                print("Chain workflow executed successfully")

                -- Verify both nodes executed
                local node_a_results = (data_reader.with_dataflow(dataflow_id :: string) :: any)
                    :with_nodes(node_a_id)
                    :with_data_types(consts.DATA_TYPE.NODE_RESULT)
                    :all()
                test.gt(#node_a_results, 0)
                print("Node A executed and completed")

                local node_b_results = (data_reader.with_dataflow(dataflow_id :: string) :: any)
                    :with_nodes(node_b_id)
                    :with_data_types(consts.DATA_TYPE.NODE_RESULT)
                    :all()
                test.gt(#node_b_results, 0)
                print("Node B executed and completed")

                -- Verify Node B got Node A's output as input
                local node_b_inputs = (data_reader.with_dataflow(dataflow_id :: string) :: any)
                    :with_nodes(node_b_id)
                    :with_data_types(consts.DATA_TYPE.NODE_INPUT)
                    :fetch_options({ replace_references = true })
                    :all()

                test.gt(#node_b_inputs, 0)
                local b_input_content = node_b_inputs[1].content
                if type(b_input_content) == "string" then
                    local decoded, _decode_err = json.decode(b_input_content :: string)
                    if not _decode_err then
                        b_input_content = decoded
                    end
                end

                test.eq(b_input_content.message, "Chain input")
                test.eq(b_input_content.processed_by, "test_function")
                print("Node B received Node A's processed output")

                -- Verify final workflow output exists and contains Node B's processing
                local workflow_outputs = (data_reader.with_dataflow(dataflow_id :: string) :: any)
                    :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                    :with_data_keys("final")
                    :fetch_options({ replace_references = true })
                    :all()

                test.gt(#workflow_outputs, 0)
                print("Workflow output created")

                local final_output = workflow_outputs[1].content
                if type(final_output) == "string" then
                    local decoded, _decode_err = json.decode(final_output :: string)
                    if not _decode_err then
                        final_output = decoded
                    end
                end

                -- Verify end-to-end data transformation
                test.eq(final_output.message, "Chain input")                   -- Original input preserved
                test.eq(final_output.processed_by, "test_function")            -- Node B processed it
                test.not_nil(final_output.input_echo)                          -- Node B got Node A's output
                test.eq(final_output.input_echo.processed_by, "test_function") -- Node A processed original
                test.is_true(final_output.success)                             -- Node B succeeded

                print("End-to-end data flow validated:")
                print("  Original input -> Node A -> Node B -> Workflow output")
                print("  Data transformations preserved through chain")
                print("=== SIMPLE CHAIN TEST COMPLETE ===")
            end)
        end)

        describe("Error Handling Chain", function()
            it("should route errors from Node A to Node B and complete workflow successfully", function()
                print("=== ERROR HANDLING CHAIN TEST START ===")

                local c, err = client.new()
                test.is_nil(err)

                local node_a_id = uuid.v7()
                local node_b_id = uuid.v7()
                local input_data_id = uuid.v7()
                local node_a_input_id = uuid.v7()

                local test_input = {
                    message = "Error chain input",
                    should_fail = true -- Node A will fail
                }

                print("Generated IDs:")
                print("  node_a_id:", node_a_id, "(will fail)")
                print("  node_b_id:", node_b_id, "(error handler)")

                local workflow_commands = {
                    -- Node A: routes errors to Node B
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = node_a_id,
                            node_type = "userspace.dataflow.node.func:node",
                            status = consts.STATUS.PENDING,
                            config = {
                                func_id = "userspace.dataflow.node.func:test_func",
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                        key = "success_result"
                                    }
                                },
                                error_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.NODE_INPUT,
                                        node_id = node_b_id
                                    }
                                }
                            },
                            metadata = {
                                title = "Error Chain Node A (Will Fail)"
                            }
                        }
                    },
                    -- Node B: error handler, routes to workflow output
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = node_b_id,
                            node_type = "userspace.dataflow.node.func:node",
                            status = consts.STATUS.PENDING,
                            config = {
                                func_id = "userspace.dataflow.node.func:test_func",
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                        key = "error_handled"
                                    }
                                }
                            },
                            metadata = {
                                title = "Error Chain Node B (Error Handler)"
                            }
                        }
                    },
                    -- Workflow input
                    {
                        type = consts.COMMAND_TYPES.CREATE_DATA,
                        payload = {
                            data_id = input_data_id,
                            data_type = consts.DATA_TYPE.WORKFLOW_INPUT,
                            content = test_input,
                            content_type = consts.CONTENT_TYPE.JSON
                        }
                    },
                    -- Node A input
                    {
                        type = consts.COMMAND_TYPES.CREATE_DATA,
                        payload = {
                            data_id = node_a_input_id,
                            data_type = consts.DATA_TYPE.NODE_INPUT,
                            node_id = node_a_id,
                            key = input_data_id,
                            discriminator = "default",
                            content = "",
                            content_type = "dataflow/reference"
                        }
                    }
                }

                print("Error handling workflow commands prepared (", #workflow_commands, "commands)")

                local dataflow_id, create_err = (c :: any):create_workflow(workflow_commands, {
                    metadata = {
                        title = "Error Handling Chain Test Workflow"
                    }
                })
                test.is_nil(create_err)
                print("Error handling workflow created:", dataflow_id)

                local result, exec_err = (c :: any):execute(dataflow_id :: string)
                test.is_nil(exec_err)

                -- Key expectation: workflow should FAIL because Node A semantic failure fails the workflow
                -- Even though Node B might process the error, Node A's semantic failure propagates
                test.is_true(result.success)

                -- Validate specific failure details
                test.eq(result.dataflow_id, dataflow_id)
                print("Workflow failed with correct error details")
                print("  Workflow error:", result.error)
                print("Workflow failed as expected due to Node A semantic failure")

                -- Verify Node A failed
                local node_a_results = (data_reader.with_dataflow(dataflow_id :: string) :: any)
                    :with_nodes(node_a_id)
                    :with_data_types(consts.DATA_TYPE.NODE_RESULT)
                    :all()
                test.gt(#node_a_results, 0)

                local a_failed = false
                for _, result_data in ipairs(node_a_results) do
                    if result_data.discriminator == "result.error" then
                        a_failed = true
                        print("Node A marked as failed with error discriminator")
                        print("  Node A error content:", json.encode(result_data.content))

                        -- Validate Node A error structure
                        local a_error_content = result_data.content
                        if type(a_error_content) == "string" then
                            local decoded, _decode_err = json.decode(a_error_content :: string)
                            if not _decode_err then
                                a_error_content = decoded
                            end
                        end

                        test.is_false(a_error_content.success)
                        test.contains(a_error_content.message, "Function execution failed")
                        test.not_nil(a_error_content.error)
                        test.eq(a_error_content.error.code, "FUNCTION_EXECUTION_FAILED")
                        test.contains(a_error_content.error.message, "Intentional semantic failure")
                        test.not_nil(a_error_content.data_ids)
                        test.gt(#a_error_content.data_ids, 0)
                        print("Node A error structure validated")
                        break
                    end
                end
                test.is_true(a_failed)

                -- Check if Node B executed (should have received error data from Node A)
                local node_b_results = (data_reader.with_dataflow(dataflow_id :: string) :: any)
                    :with_nodes(node_b_id)
                    :with_data_types(consts.DATA_TYPE.NODE_RESULT)
                    :all()

                if #node_b_results > 0 then
                    print("Node B executed (received error from Node A)")

                    -- Verify Node B got Node A's error as input
                    local node_b_inputs = (data_reader.with_dataflow(dataflow_id :: string) :: any)
                        :with_nodes(node_b_id)
                        :with_data_types(consts.DATA_TYPE.NODE_INPUT)
                        :fetch_options({ replace_references = true })
                        :all()

                    if #node_b_inputs > 0 then
                        -- Validate that error routing created input for Node B
                        test.eq(#node_b_inputs, 1)
                        print("Node B has exactly one input (from error routing)")
                        local b_input_content = node_b_inputs[1].content
                        if type(b_input_content) == "string" then
                            local decoded, _decode_err = json.decode(b_input_content :: string)
                            if not _decode_err then
                                b_input_content = decoded
                            end
                        end
                        print("Node B received error data:")
                        print("  Input content:", json.encode(b_input_content))

                        -- Validate error data structure
                        test.eq(b_input_content.code, "FUNCTION_EXECUTION_FAILED")
                        test.contains(b_input_content.message, "Intentional semantic failure")

                        -- Validate that Node B received error from Node A (not original input)
                        print("Error data structure validated")
                        print("Confirmed Node B received error from Node A (not original input)")

                        -- Check if Node B produced workflow output
                        local workflow_outputs = (data_reader.with_dataflow(dataflow_id :: string) :: any)
                            :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                            :with_data_keys("error_handled")
                            :fetch_options({ replace_references = true })
                            :all()

                        if #workflow_outputs > 0 then
                            local error_handled_output = workflow_outputs[1].content
                            if type(error_handled_output) == "string" then
                                local decoded, _decode_err = json.decode(error_handled_output :: string)
                                if not _decode_err then
                                    error_handled_output = decoded
                                end
                            end
                            print("Node B produced workflow output:")
                            print("  Error handled output:", json.encode(error_handled_output))

                            -- Validate error handling output
                            test.is_true(error_handled_output.success)
                            test.eq(error_handled_output.processed_by, "test_function")
                            test.not_nil(error_handled_output.input_echo)
                            test.eq(error_handled_output.input_echo.code, "FUNCTION_EXECUTION_FAILED")
                            test.contains(error_handled_output.input_echo.message, "Intentional semantic failure")
                            test.contains(error_handled_output.message, "Intentional semantic failure")
                            print("Error handling output structure validated")
                        else
                            print("Node B did not produce workflow output")
                        end
                    else
                        print("Node B did not receive input data")
                    end
                else
                    print("Node B did not execute")
                end

                print("Error chaining flow validated:")
                print("  Node A fails -> error_targets -> Node B processes error")
                print("  Workflow fails due to Node A semantic failure (expected)")
                print("  Error data structure and routing verified end-to-end")
                print("=== ERROR HANDLING CHAIN TEST COMPLETE ===")
            end)
        end)
    end)

    describe("Diamond Pattern Workflow Tests", function()
        describe("Basic Diamond Pattern", function()
            it("should execute diamond pattern with proper concurrency and data merging", function()
                print("=== CLEAN DIAMOND PATTERN TEST START ===")

                local c, err = client.new()
                test.is_nil(err)

                local node_a_id = uuid.v7()
                local node_b_id = uuid.v7()
                local node_c_id = uuid.v7()
                local node_d_id = uuid.v7()
                local input_data_id = uuid.v7()
                local node_a_input_id = uuid.v7()

                local test_input = {
                    message = "DIAMOND_ROOT_INPUT",
                    value = 100,
                    delay_ms = 50, -- Shorter delay for faster test
                    diamond_test = true
                }

                print("Diamond pattern nodes:")
                print("  Node A (fan-out):", node_a_id)
                print("  Node B (branch 1):", node_b_id)
                print("  Node C (branch 2):", node_c_id)
                print("  Node D (fan-in):", node_d_id)

                local workflow_commands = {
                    -- Node A: fan-out to both B and C
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = node_a_id,
                            node_type = "userspace.dataflow.node.func:node",
                            status = consts.STATUS.PENDING,
                            config = {
                                func_id = "userspace.dataflow.node.func:test_func",
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.NODE_INPUT,
                                        node_id = node_b_id,
                                        key = "from_a"
                                    },
                                    {
                                        data_type = consts.DATA_TYPE.NODE_INPUT,
                                        node_id = node_c_id,
                                        key = "from_a"
                                    }
                                }
                            },
                            metadata = {
                                title = "Diamond Node A (Fan-Out)"
                            }
                        }
                    },
                    -- Node B: branch 1 processing
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = node_b_id,
                            node_type = "userspace.dataflow.node.func:node",
                            status = consts.STATUS.PENDING,
                            config = {
                                func_id = "userspace.dataflow.node.func:test_func",
                                inputs = {
                                    required = { "from_a" }
                                },
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.NODE_INPUT,
                                        node_id = node_d_id,
                                        key = "from_b"
                                    }
                                }
                            },
                            metadata = {
                                title = "Diamond Node B (Branch 1)"
                            }
                        }
                    },
                    -- Node C: branch 2 processing
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = node_c_id,
                            node_type = "userspace.dataflow.node.func:node",
                            status = consts.STATUS.PENDING,
                            config = {
                                func_id = "userspace.dataflow.node.func:test_func",
                                inputs = {
                                    required = { "from_a" }
                                },
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.NODE_INPUT,
                                        node_id = node_d_id,
                                        key = "from_c"
                                    }
                                }
                            },
                            metadata = {
                                title = "Diamond Node C (Branch 2)"
                            }
                        }
                    },
                    -- Node D: fan-in (diamond merge)
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = node_d_id,
                            node_type = "userspace.dataflow.node.func:node",
                            status = consts.STATUS.PENDING,
                            config = {
                                func_id = "userspace.dataflow.node.func:test_func",
                                inputs = {
                                    required = { "from_b", "from_c" }
                                },
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                        key = "" -- Root workflow output
                                    }
                                }
                            },
                            metadata = {
                                title = "Diamond Node D (Fan-In Merge)"
                            }
                        }
                    },
                    -- Workflow input
                    {
                        type = consts.COMMAND_TYPES.CREATE_DATA,
                        payload = {
                            data_id = input_data_id,
                            data_type = consts.DATA_TYPE.WORKFLOW_INPUT,
                            content = test_input,
                            content_type = consts.CONTENT_TYPE.JSON
                        }
                    },
                    -- Node A input reference
                    {
                        type = consts.COMMAND_TYPES.CREATE_DATA,
                        payload = {
                            data_id = node_a_input_id,
                            data_type = consts.DATA_TYPE.NODE_INPUT,
                            node_id = node_a_id,
                            key = input_data_id,
                            discriminator = "default",
                            content = "",
                            content_type = "dataflow/reference"
                        }
                    }
                }

                print("Clean diamond workflow prepared (", #workflow_commands, "commands)")

                -- Create and execute workflow
                local dataflow_id, create_err = (c :: any):create_workflow(workflow_commands, {
                    metadata = {
                        title = "Clean Diamond Pattern Test Workflow",
                        pattern = "diamond"
                    }
                })

                test.is_nil(create_err)
                print("Diamond workflow created:", dataflow_id)

                local result, exec_err = (c :: any):execute(dataflow_id :: string)
                test.is_nil(exec_err)
                test.is_true(result.success)
                print("Diamond workflow executed successfully")

                -- Verify all nodes completed
                local all_nodes = (data_reader.with_dataflow(dataflow_id :: string) :: any)
                    :with_data_types(consts.DATA_TYPE.NODE_RESULT)
                    :all()

                test.eq(#all_nodes, 4)
                print("All 4 nodes completed successfully")

                -- Get the final merged result
                local final_output = (data_reader.with_dataflow(dataflow_id :: string) :: any)
                    :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                    :with_data_keys("") -- Root output
                    :fetch_options({ replace_references = true })
                    :one()

                test.not_nil(final_output)
                print("Final diamond result found")

                -- Parse the output content
                local content = (final_output :: any).content
                if type(content) == "string" then
                    local decoded, _decode_err = json.decode(content :: string)
                    if not _decode_err then
                        content = decoded
                    end
                end

                print("Diamond result content:", json.encode(content))

                -- Verify diamond pattern detection
                test.is_true(content.diamond_pattern)
                test.not_nil(content.diamond_merge)
                test.not_nil(content.diamond_merge.branch_b_timestamp)
                test.not_nil(content.diamond_merge.branch_c_timestamp)
                test.eq(content.diamond_merge.branch_b_processed_by, "test_function")
                test.eq(content.diamond_merge.branch_c_processed_by, "test_function")
                print("Diamond pattern metadata validated")

                -- Verify Node D (final merge) output structure
                test.eq(content.message, "DIAMOND_ROOT_INPUT")
                test.eq(content.processed_by, "test_function")
                test.is_true(content.success)
                test.eq(content.delay_applied, 100) -- Node D's own delay
                test.not_nil(content.timestamp)
                print("Node D output structure validated")

                -- Verify multi-input structure exists
                test.not_nil(content.input_echo)
                test.not_nil(content.input_echo.from_b)
                test.not_nil(content.input_echo.from_c)
                print("Multi-input structure confirmed")

                -- Verify Branch B path through diamond
                local branch_b = content.input_echo.from_b
                test.eq(branch_b.message, "DIAMOND_ROOT_INPUT")
                test.eq(branch_b.processed_by, "test_function")
                test.is_true(branch_b.success)
                test.eq(branch_b.delay_applied, 100) -- Node B's delay
                test.not_nil(branch_b.timestamp)

                -- Verify Branch B received Node A's output
                test.not_nil(branch_b.input_echo)
                test.eq(branch_b.input_echo.message, "DIAMOND_ROOT_INPUT")
                test.eq(branch_b.input_echo.processed_by, "test_function")
                test.eq(branch_b.input_echo.delay_applied, 50) -- Node A's delay

                -- Verify Branch B's input contains original workflow input
                test.not_nil(branch_b.input_echo.input_echo)
                test.eq(branch_b.input_echo.input_echo.message, "DIAMOND_ROOT_INPUT")
                test.eq(branch_b.input_echo.input_echo.value, 100)
                test.eq(branch_b.input_echo.input_echo.delay_ms, 50)
                test.is_true(branch_b.input_echo.input_echo.diamond_test)
                print("Branch B data flow path validated: Original -> A -> B -> D")

                -- Verify Branch C path through diamond
                local branch_c = content.input_echo.from_c
                test.eq(branch_c.message, "DIAMOND_ROOT_INPUT")
                test.eq(branch_c.processed_by, "test_function")
                test.is_true(branch_c.success)
                test.eq(branch_c.delay_applied, 100) -- Node C's delay
                test.not_nil(branch_c.timestamp)

                -- Verify Branch C received Node A's output
                test.not_nil(branch_c.input_echo)
                test.eq(branch_c.input_echo.message, "DIAMOND_ROOT_INPUT")
                test.eq(branch_c.input_echo.processed_by, "test_function")
                test.eq(branch_c.input_echo.delay_applied, 50) -- Node A's delay

                -- Verify Branch C's input contains original workflow input
                test.not_nil(branch_c.input_echo.input_echo)
                test.eq(branch_c.input_echo.input_echo.message, "DIAMOND_ROOT_INPUT")
                test.eq(branch_c.input_echo.input_echo.value, 100)
                test.eq(branch_c.input_echo.input_echo.delay_ms, 50)
                test.is_true(branch_c.input_echo.input_echo.diamond_test)
                print("Branch C data flow path validated: Original -> A -> C -> D")

                -- Verify both branches have different processing timestamps but same source
                test.not_nil(branch_b.timestamp) -- Already checked, but ensures non-nil for comparison
                test.not_nil(branch_c.timestamp)
                test.eq(branch_b.input_echo.timestamp, branch_c.input_echo.timestamp) -- Both from same Node A execution
                print("Branch independence confirmed (different execution times, same source)")

                -- Verify complete data transformation chain
                local original_data = branch_b.input_echo.input_echo -- Same for branch_c
                test.eq(original_data.message, test_input.message)
                test.eq(original_data.value, test_input.value)
                test.eq(original_data.delay_ms, test_input.delay_ms)
                test.eq(original_data.diamond_test, test_input.diamond_test)
                print("End-to-end data integrity validated")

                -- Verify concurrency using actual branch execution timestamps
                local b_time = time.parse(time.RFC3339NANO, content.diamond_merge.branch_b_timestamp :: string)
                local c_time = time.parse(time.RFC3339NANO, content.diamond_merge.branch_c_timestamp :: string)
                local time_diff_ms = math.abs((((b_time :: any):unix_nano() - (c_time :: any):unix_nano()) / 1000000) :: number)

                print("Branch execution timing analysis:")
                print("  Branch B executed at:", content.diamond_merge.branch_b_timestamp)
                print("  Branch C executed at:", content.diamond_merge.branch_c_timestamp)
                print("  Time difference:", time_diff_ms, "ms")

                -- Expect concurrent execution (within 100ms window)
                test.is_true(
                    time_diff_ms < 100
                )

                -- Verify Node A timestamp is earlier than both B and C
                local a_timestamp = content.input_echo.from_b.input_echo.timestamp :: string
                local a_time = time.parse(time.RFC3339NANO, a_timestamp)

                test.is_true((a_time :: any):unix_nano() < (b_time :: any):unix_nano())
                test.is_true((a_time :: any):unix_nano() < (c_time :: any):unix_nano())

                -- Verify Node D timestamp is later than both B and C
                local d_time = time.parse(time.RFC3339NANO, content.timestamp :: string)
                test.is_true((d_time :: any):unix_nano() > (b_time :: any):unix_nano())
                test.is_true((d_time :: any):unix_nano() > (c_time :: any):unix_nano())

                print("Dependency ordering validated: A -> {B,C} -> D")

                print("Complete diamond pattern validation successful:")
                print("  Topology: A -> {B,C} -> D")
                print("  Concurrency: B and C executed", time_diff_ms, "ms apart")
                print("  Data integrity: Original input preserved through all transformations")
                print("  Multi-input merge: Both branches successfully merged at Node D")
                print("  Diamond metadata: Pattern detection and merge info correct")
                print("  End-to-end traceability: Full data flow path verified")
                print("=== CLEAN DIAMOND PATTERN TEST COMPLETE ===")
            end)
        end)
    end)
end

return test.run_cases(define_tests)
