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
                local c: any, err: string? = client.new()
                test.is_nil(err)

                local state_id: string = uuid.v7()
                local input_data_id: string = uuid.v7()

                local workflow_commands = {
                    {
                        type = consts.COMMAND_TYPES.CREATE_DATA,
                        payload = {
                            data_id = input_data_id,
                            data_type = consts.DATA_TYPE.WORKFLOW_INPUT,
                            content = { message = "test data" },
                            content_type = consts.CONTENT_TYPE.JSON
                        }
                    },
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

                local dataflow_id, create_err: string? = c:create_workflow(workflow_commands)
                test.is_nil(create_err)

                local result: any, exec_err: string? = c:execute(dataflow_id)
                test.is_nil(exec_err)
                test.is_true(result.success)

                local output: any = (data_reader.with_dataflow(dataflow_id) :: any)
                    :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                    :one()

                test.not_nil(output)

                local content: any = output.content
                if type(content) == "string" then
                    content = json.decode(content :: string)
                end

                test.not_nil(content.main_input)
                test.eq(content.main_input.message, "test data")
            end)

            it("should collect single input with requirements", function()
                local c: any, err: string? = client.new()
                test.is_nil(err)

                local state_id: string = uuid.v7()
                local input_data_id: string = uuid.v7()

                local workflow_commands = {
                    {
                        type = consts.COMMAND_TYPES.CREATE_DATA,
                        payload = {
                            data_id = input_data_id,
                            data_type = consts.DATA_TYPE.WORKFLOW_INPUT,
                            content = { message = "required test" },
                            content_type = consts.CONTENT_TYPE.JSON
                        }
                    },
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

                local dataflow_id, create_err: string? = c:create_workflow(workflow_commands)
                test.is_nil(create_err)

                local result: any, exec_err: string? = c:execute(dataflow_id)
                test.is_nil(exec_err)
                test.is_true(result.success)

                local output: any = (data_reader.with_dataflow(dataflow_id) :: any)
                    :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                    :one()

                test.not_nil(output)

                local content: any = output.content
                if type(content) == "string" then
                    content = json.decode(content :: string)
                end

                test.not_nil(content.required_input)
                test.eq(content.required_input.message, "required test")
            end)
        end)

        describe("Diamond Pattern", function()
            it("should wait for both branches before executing", function()
                print("=== DIAMOND PATTERN TEST START ===")

                local c: any, err: string? = client.new()
                test.is_nil(err)

                local proc_a_id: string = uuid.v7()
                local proc_b_id: string = uuid.v7()
                local state_id: string = uuid.v7()
                local input_data_id: string = uuid.v7()

                print("proc_a_id:", proc_a_id)
                print("proc_b_id:", proc_b_id)
                print("state_id:", state_id)
                print("input_data_id:", input_data_id)

                local workflow_commands = {
                    {
                        type = consts.COMMAND_TYPES.CREATE_DATA,
                        payload = {
                            data_id = input_data_id,
                            data_type = consts.DATA_TYPE.WORKFLOW_INPUT,
                            content = { value = 100 },
                            content_type = consts.CONTENT_TYPE.JSON
                        }
                    },
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
                local dataflow_id, create_err: string? = c:create_workflow(workflow_commands)
                print("dataflow_id:", dataflow_id)
                test.is_nil(create_err)

                print("Executing diamond workflow...")
                local result: any, exec_err: string? = c:execute(dataflow_id)
                print("exec_err:", exec_err)
                print("result:", json.encode(result))

                if not result.success then
                    print("DIAMOND WORKFLOW FAILED!")
                    print("result.error:", result.error)

                    print("Checking all workflow data...")
                    local all_data = (data_reader.with_dataflow(dataflow_id) :: any):all()
                    print("Total data records:", #all_data)
                    for i, data in ipairs(all_data) do
                        print(string.format("Data %d: type=%s, node_id=%s, key=%s, discriminator=%s, content_type=%s",
                            i, data.data_type, data.node_id or "nil", data.key or "nil", data.discriminator or "nil", data.content_type or "nil"))
                    end

                    print("Checking NODE_INPUT data...")
                    local node_inputs = (data_reader.with_dataflow(dataflow_id) :: any)
                        :with_data_types(consts.DATA_TYPE.NODE_INPUT)
                        :all()
                    print("Node input records:", #node_inputs)
                    for i, input_record in ipairs(node_inputs) do
                        print(string.format("Input %d: node_id=%s, discriminator=%s, key=%s",
                            i, input_record.node_id or "nil", input_record.discriminator or "nil", input_record.key or "nil"))
                    end
                else
                    print("DIAMOND WORKFLOW SUCCEEDED!")

                    print("Checking workflow outputs...")
                    local outputs = (data_reader.with_dataflow(dataflow_id) :: any)
                        :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                        :all()
                    print("Output records:", #outputs)
                    for i, output in ipairs(outputs) do
                        print(string.format("Output %d: key=%s, content=%s",
                            i, output.key or "nil", json.encode(output.content)))
                    end
                end

                test.is_nil(exec_err)
                test.is_true(result.success)

                local output: any = (data_reader.with_dataflow(dataflow_id) :: any)
                    :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                    :one()

                test.not_nil(output)

                local content: any = output.content
                if type(content) == "string" then
                    content = json.decode(content :: string)
                end

                test.not_nil(content.branch_a)
                test.not_nil(content.branch_b)

                print("=== DIAMOND PATTERN TEST COMPLETE ===")
            end)
        end)

        describe("Transform Test", function()
            it("should use input_transform to restructure inputs", function()
                local c: any, err: string? = client.new()
                test.is_nil(err)

                local state_id: string = uuid.v7()
                local input1_id: string = uuid.v7()
                local input2_id: string = uuid.v7()

                local workflow_commands = {
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

                local dataflow_id, create_err: string? = c:create_workflow(workflow_commands)
                test.is_nil(create_err)

                local result: any, exec_err: string? = c:execute(dataflow_id)
                test.is_nil(exec_err)
                test.is_true(result.success)

                local output: any = (data_reader.with_dataflow(dataflow_id) :: any)
                    :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                    :one()

                test.not_nil(output)

                local content: any = output.content
                if type(content) == "string" then
                    content = json.decode(content :: string)
                end

                test.eq(content.summary, 2)
                test.eq(content.user_name, "Alice")
                test.eq(content.final_grade, "A")
            end)
        end)
    end)
end

return test.run_cases(define_tests)
