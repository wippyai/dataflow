local test = require("test")
local uuid = require("uuid")
local json = require("json")
local client = require("client")
local consts = require("consts")
local data_reader = require("data_reader")

local function define_tests()
    describe("State Node Integration Tests", function()
        describe("Basic Collection", function()
            it("should collect single input without requirements", function()
                local c, err = client.new()
                expect(err).to_be_nil()

                local state_id = uuid.v7()
                local input_data_id = uuid.v7()

                local workflow_commands = {
                    -- Create workflow input
                    {
                        type = consts.COMMAND_TYPES.CREATE_DATA,
                        payload = {
                            data_id = input_data_id,
                            data_type = consts.DATA_TYPE.WORKFLOW_INPUT,
                            content = { message = "test data" },
                            content_type = consts.CONTENT_TYPE.JSON
                        }
                    },

                    -- Create state node WITHOUT input requirements
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = state_id,
                            node_type = "userspace.dataflow.node.state:state",
                            status = consts.STATUS.PENDING,
                            config = {
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                        key = "result"
                                    }
                                }
                            }
                        }
                    },

                    -- Create node input reference
                    {
                        type = consts.COMMAND_TYPES.CREATE_DATA,
                        payload = {
                            data_id = uuid.v7(),
                            data_type = consts.DATA_TYPE.NODE_INPUT,
                            node_id = state_id,
                            key = input_data_id,
                            discriminator = "main_input",
                            content = "",
                            content_type = "dataflow/reference"
                        }
                    }
                }

                local dataflow_id, create_err = c:create_workflow(workflow_commands)
                expect(create_err).to_be_nil()

                local result, exec_err = c:execute(dataflow_id)
                expect(exec_err).to_be_nil()
                expect(result.success).to_be_true()

                -- Check output
                local output = data_reader.with_dataflow(dataflow_id)
                    :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                    :one()

                expect(output).not_to_be_nil()

                local content = output.content
                if type(content) == "string" then
                    content = json.decode(content)
                end

                expect(content.main_input).not_to_be_nil()
                expect(content.main_input.message).to_equal("test data")
            end)

            it("should collect single input with requirements", function()
                local c, err = client.new()
                expect(err).to_be_nil()

                local state_id = uuid.v7()
                local input_data_id = uuid.v7()

                local workflow_commands = {
                    -- Create workflow input
                    {
                        type = consts.COMMAND_TYPES.CREATE_DATA,
                        payload = {
                            data_id = input_data_id,
                            data_type = consts.DATA_TYPE.WORKFLOW_INPUT,
                            content = { message = "required test" },
                            content_type = consts.CONTENT_TYPE.JSON
                        }
                    },

                    -- Create state node WITH input requirements
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = state_id,
                            node_type = "userspace.dataflow.node.state:state",
                            status = consts.STATUS.PENDING,
                            config = {
                                inputs = {
                                    required = { "required_input" }
                                },
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                        key = "result"
                                    }
                                }
                            }
                        }
                    },

                    -- Create node input reference with matching discriminator
                    {
                        type = consts.COMMAND_TYPES.CREATE_DATA,
                        payload = {
                            data_id = uuid.v7(),
                            data_type = consts.DATA_TYPE.NODE_INPUT,
                            node_id = state_id,
                            key = input_data_id,
                            discriminator = "required_input",
                            content = "",
                            content_type = "dataflow/reference"
                        }
                    }
                }

                local dataflow_id, create_err = c:create_workflow(workflow_commands)
                expect(create_err).to_be_nil()

                local result, exec_err = c:execute(dataflow_id)
                expect(exec_err).to_be_nil()
                expect(result.success).to_be_true()

                -- Check output
                local output = data_reader.with_dataflow(dataflow_id)
                    :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                    :one()

                expect(output).not_to_be_nil()

                local content = output.content
                if type(content) == "string" then
                    content = json.decode(content)
                end

                expect(content.required_input).not_to_be_nil()
                expect(content.required_input.message).to_equal("required test")
            end)
        end)

        describe("Diamond Pattern", function()
            it("should wait for both branches before executing", function()
                print("=== DIAMOND PATTERN TEST START ===")

                local c, err = client.new()
                expect(err).to_be_nil()

                local proc_a_id = uuid.v7()
                local proc_b_id = uuid.v7()
                local state_id = uuid.v7()
                local input_data_id = uuid.v7()

                print("proc_a_id:", proc_a_id)
                print("proc_b_id:", proc_b_id)
                print("state_id:", state_id)
                print("input_data_id:", input_data_id)

                local workflow_commands = {
                    -- Input
                    {
                        type = consts.COMMAND_TYPES.CREATE_DATA,
                        payload = {
                            data_id = input_data_id,
                            data_type = consts.DATA_TYPE.WORKFLOW_INPUT,
                            content = { value = 100 },
                            content_type = consts.CONTENT_TYPE.JSON
                        }
                    },

                    -- Process A
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = proc_a_id,
                            node_type = "userspace.dataflow.node.func:node",
                            status = consts.STATUS.PENDING,
                            config = {
                                func_id = "userspace.dataflow.node.func:test_func",
                                context = { branch = "A" },
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.NODE_INPUT,
                                        node_id = state_id,
                                        discriminator = "branch_a"
                                    }
                                }
                            }
                        }
                    },

                    -- Process B
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = proc_b_id,
                            node_type = "userspace.dataflow.node.func:node",
                            status = consts.STATUS.PENDING,
                            config = {
                                func_id = "userspace.dataflow.node.func:test_func",
                                context = { branch = "B" },
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.NODE_INPUT,
                                        node_id = state_id,
                                        discriminator = "branch_b"
                                    }
                                }
                            }
                        }
                    },

                    -- State collector - waits for BOTH inputs
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = state_id,
                            node_type = "userspace.dataflow.node.state:state",
                            status = consts.STATUS.PENDING,
                            config = {
                                inputs = {
                                    required = { "branch_a", "branch_b" }
                                },
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                        key = "diamond_result"
                                    }
                                }
                            }
                        }
                    },

                    -- Connect input to both processors
                    {
                        type = consts.COMMAND_TYPES.CREATE_DATA,
                        payload = {
                            data_id = uuid.v7(),
                            data_type = consts.DATA_TYPE.NODE_INPUT,
                            node_id = proc_a_id,
                            key = input_data_id,
                            content = "",
                            content_type = "dataflow/reference"
                        }
                    },
                    {
                        type = consts.COMMAND_TYPES.CREATE_DATA,
                        payload = {
                            data_id = uuid.v7(),
                            data_type = consts.DATA_TYPE.NODE_INPUT,
                            node_id = proc_b_id,
                            key = input_data_id,
                            content = "",
                            content_type = "dataflow/reference"
                        }
                    }
                }

                print("Creating diamond workflow...")
                local dataflow_id, create_err = c:create_workflow(workflow_commands)
                print("dataflow_id:", dataflow_id)
                expect(create_err).to_be_nil()

                print("Executing diamond workflow...")
                local result, exec_err = c:execute(dataflow_id)
                print("exec_err:", exec_err)
                print("result:", json.encode(result))

                if not result.success then
                    print("DIAMOND WORKFLOW FAILED!")
                    print("result.error:", result.error)

                    -- Check all data in workflow
                    print("Checking all workflow data...")
                    local all_data = data_reader.with_dataflow(dataflow_id):all()
                    print("Total data records:", #all_data)
                    for i, data in ipairs(all_data) do
                        print(string.format("Data %d: type=%s, node_id=%s, key=%s, discriminator=%s, content_type=%s",
                            i, data.data_type, data.node_id or "nil", data.key or "nil", data.discriminator or "nil", data.content_type or "nil"))
                    end

                    -- Check node inputs specifically
                    print("Checking NODE_INPUT data...")
                    local node_inputs = data_reader.with_dataflow(dataflow_id)
                        :with_data_types(consts.DATA_TYPE.NODE_INPUT)
                        :all()
                    print("Node input records:", #node_inputs)
                    for i, input in ipairs(node_inputs) do
                        print(string.format("Input %d: node_id=%s, discriminator=%s, key=%s",
                            i, input.node_id or "nil", input.discriminator or "nil", input.key or "nil"))
                    end
                else
                    print("DIAMOND WORKFLOW SUCCEEDED!")

                    -- Check what outputs we got
                    print("Checking workflow outputs...")
                    local outputs = data_reader.with_dataflow(dataflow_id)
                        :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                        :all()
                    print("Output records:", #outputs)
                    for i, output in ipairs(outputs) do
                        print(string.format("Output %d: key=%s, content=%s",
                            i, output.key or "nil", json.encode(output.content)))
                    end
                end

                expect(exec_err).to_be_nil()
                expect(result.success).to_be_true()

                -- Check diamond output
                local output = data_reader.with_dataflow(dataflow_id)
                    :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                    :one()

                expect(output).not_to_be_nil()

                local content = output.content
                if type(content) == "string" then
                    content = json.decode(content)
                end

                expect(content.branch_a).not_to_be_nil()
                expect(content.branch_b).not_to_be_nil()

                print("=== DIAMOND PATTERN TEST COMPLETE ===")
            end)
        end)

        describe("Transform Test", function()
            it("should use input_transform to restructure inputs", function()
                local c, err = client.new()
                expect(err).to_be_nil()

                local state_id = uuid.v7()
                local input1_id = uuid.v7()
                local input2_id = uuid.v7()

                local workflow_commands = {
                    -- Two input data items
                    {
                        type = consts.COMMAND_TYPES.CREATE_DATA,
                        payload = {
                            data_id = input1_id,
                            data_type = consts.DATA_TYPE.WORKFLOW_INPUT,
                            content = { name = "Alice", age = 30 },
                            content_type = consts.CONTENT_TYPE.JSON
                        }
                    },
                    {
                        type = consts.COMMAND_TYPES.CREATE_DATA,
                        payload = {
                            data_id = input2_id,
                            data_type = consts.DATA_TYPE.WORKFLOW_INPUT,
                            content = { score = 95, grade = "A" },
                            content_type = consts.CONTENT_TYPE.JSON
                        }
                    },

                    -- State node with transform
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = state_id,
                            node_type = "userspace.dataflow.node.state:state",
                            status = consts.STATUS.PENDING,
                            config = {
                                inputs = {
                                    required = { "user_data", "grade_data" }
                                },
                                input_transform = {
                                    summary = "len(input)",
                                    user_name = "input.user_data.content.name",
                                    final_grade = "input.grade_data.content.grade"
                                },
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                        key = "transform_result"
                                    }
                                }
                            }
                        }
                    },

                    -- References with discriminators
                    {
                        type = consts.COMMAND_TYPES.CREATE_DATA,
                        payload = {
                            data_id = uuid.v7(),
                            data_type = consts.DATA_TYPE.NODE_INPUT,
                            node_id = state_id,
                            key = input1_id,
                            discriminator = "user_data",
                            content = "",
                            content_type = "dataflow/reference"
                        }
                    },
                    {
                        type = consts.COMMAND_TYPES.CREATE_DATA,
                        payload = {
                            data_id = uuid.v7(),
                            data_type = consts.DATA_TYPE.NODE_INPUT,
                            node_id = state_id,
                            key = input2_id,
                            discriminator = "grade_data",
                            content = "",
                            content_type = "dataflow/reference"
                        }
                    }
                }

                local dataflow_id, create_err = c:create_workflow(workflow_commands)
                expect(create_err).to_be_nil()

                local result, exec_err = c:execute(dataflow_id)
                expect(exec_err).to_be_nil()
                expect(result.success).to_be_true()

                -- Check transform output
                local output = data_reader.with_dataflow(dataflow_id)
                    :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                    :one()

                expect(output).not_to_be_nil()

                local content = output.content
                if type(content) == "string" then
                    content = json.decode(content)
                end

                expect(content.summary).to_equal(2)
                expect(content.user_name).to_equal("Alice")
                expect(content.final_grade).to_equal("A")
            end)
        end)
    end)
end

return test.run_cases(define_tests)