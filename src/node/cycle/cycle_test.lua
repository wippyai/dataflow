local test = require("test")
local uuid = require("uuid")
local json = require("json")
local client = require("client")
local consts = require("consts")
local cycle = require("cycle")
local data_reader = require("data_reader")

local function decode_json_content(content: any): any
    if type(content) == "string" then
        local decoded, decode_err = json.decode(content)
        if not decode_err then
            return decoded
        end
    end
    return content
end

local function define_tests()
    describe("Cycle Node Integration Tests", function()
        describe("Unit Tests - Recovery", function()
            it("BC_REGRESSION_C5_cycle_legacy_state_resume", function()
                local load_persisted_state = cycle._load_persisted_state
                local original_node_reader = cycle._deps.node_reader
                local child_fallback_used = false

                cycle._deps.node_reader = {
                    with_dataflow = function(_dataflow_id)
                        child_fallback_used = true
                        return nil, "unexpected child fallback"
                    end
                }

                local legacy_state = {
                    refinement_count = 2,
                    quality_score = 0.6
                }

                local function_result = {
                    state = legacy_state,
                    continue = true,
                    result = {
                        rebuilt_from = "cycle.function_result",
                        quality_score = 0.75
                    }
                }

                local function build_query()
                    return {
                        _data_type = nil,
                        _data_key = nil,
                        with_data_types = function(self, data_type)
                            self._data_type = type(data_type) == "table" and data_type[1] or data_type
                            return self
                        end,
                        with_nodes = function(self, _nodes)
                            return self
                        end,
                        with_data_keys = function(self, data_key)
                            self._data_key = type(data_key) == "table" and data_key[1] or data_key
                            return self
                        end,
                        fetch_options = function(self, _options)
                            return self
                        end,
                        all = function(self)
                            if self._data_type == cycle.CYCLE_STATE_DATA_TYPE and self._data_key == "cycle_state" then
                                return {
                                    {
                                        content = legacy_state,
                                        metadata = { iteration = 3 }
                                    }
                                }, nil
                            end

                            if self._data_type == cycle.CYCLE_FUNCTION_RESULT_DATA_TYPE and
                                self._data_key == "function_result_3" then
                                return {
                                    {
                                        content = function_result,
                                        metadata = { iteration = 3 }
                                    }
                                }, nil
                            end

                            return {}, nil
                        end
                    }
                end

                local mock_node = {
                    node_id = "cycle-node",
                    dataflow_id = "dataflow-id",
                    query = function(_self)
                        return build_query()
                    end
                }

                local ok, loaded_state, start_iteration, last_result = pcall(load_persisted_state, mock_node, {
                    refinement_count = 0,
                    quality_score = 0.3
                })
                cycle._deps.node_reader = original_node_reader

                if not ok then
                    error(loaded_state)
                end

                local rebuilt_last_result = last_result :: any

                test.eq(start_iteration, 3)
                test.eq((loaded_state :: any).refinement_count, 2)
                test.eq((loaded_state :: any).quality_score, 0.6)
                test.not_nil(rebuilt_last_result)
                test.eq(rebuilt_last_result.rebuilt_from, "cycle.function_result")
                test.eq(rebuilt_last_result.quality_score, 0.75)
                test.is_false(child_fallback_used)
            end)
        end)

        describe("Basic Cycle Execution", function()
            it("should execute simple refinement cycle end-to-end", function()
                print("=== SIMPLE REFINEMENT CYCLE INTEGRATION TEST START ===")

                local c, err = client.new()
                test.is_nil(err)
                test.not_nil(c)

                local cycle_node_id: string = uuid.v7()
                local input_data_id: string = uuid.v7()
                local node_input_id: string = uuid.v7()

                local test_input = {
                    target_quality = 0.8,
                    initial_text = "Basic text needing refinement"
                }

                local workflow_commands = {
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = cycle_node_id,
                            node_type = "userspace.dataflow.node.cycle:cycle",
                            status = consts.STATUS.PENDING,
                            config = {
                                func_id = "userspace.dataflow.node.cycle.stub:refine_test_func",
                                max_iterations = 10,
                                initial_state = {
                                    refinement_count = 0,
                                    quality_score = 0.3
                                },
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                        key = "refined_result",
                                        content_type = consts.CONTENT_TYPE.JSON
                                    }
                                }
                            },
                            metadata = {
                                title = "Simple Refinement Cycle Integration Test"
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
                            node_id = cycle_node_id,
                            key = input_data_id,
                            content = "",
                            content_type = "dataflow/reference"
                        }
                    }
                }

                local dataflow_id, create_err = c:create_workflow(workflow_commands, {
                    metadata = {
                        title = "Simple Refinement Cycle Integration Test Workflow"
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
                    :with_data_keys("refined_result")
                    :fetch_options({ replace_references = true })
                    :one()

                test.not_nil(output_data)

                local output_content = decode_json_content(output_data.content)

                test.not_nil(output_content.refined_text)
                test.is_true(output_content.refinement_complete)
                test.gte(output_content.final_quality, 0.8)

                print("--- Simple refinement cycle completed successfully")
            end)

            it("should enforce custom max_iterations from config", function()
                print("=== MAX ITERATIONS ENFORCEMENT TEST START ===")

                local c, err = client.new()
                test.is_nil(err)

                local cycle_node_id: string = uuid.v7()
                local input_data_id: string = uuid.v7()
                local node_input_id: string = uuid.v7()

                local test_input = { infinite_loop = true }

                local workflow_commands = {
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = cycle_node_id,
                            node_type = "userspace.dataflow.node.cycle:cycle",
                            status = consts.STATUS.PENDING,
                            config = {
                                func_id = "userspace.dataflow.node.cycle.stub:refine_test_func",
                                max_iterations = 3,
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                        key = "result"
                                    }
                                }
                            },
                            metadata = {
                                title = "Max Iterations Enforcement Test"
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
                            node_id = cycle_node_id,
                            key = input_data_id,
                            content = "",
                            content_type = "dataflow/reference"
                        }
                    }
                }

                local dataflow_id, create_err = c:create_workflow(workflow_commands, {
                    metadata = {
                        title = "Max Iterations Enforcement Test Workflow"
                    }
                })
                test.is_nil(create_err)

                local result, exec_err = c:execute(dataflow_id)
                test.not_nil(exec_err)
                test.is_false(result.success)
                test.contains(result.error, "Maximum iterations")
                test.contains(result.error, "3")

                print("--- Max iterations from config enforced correctly")
            end)

            it("should handle missing func_id configuration", function()
                local c, err = client.new()
                test.is_nil(err)

                local cycle_node_id: string = uuid.v7()
                local input_data_id: string = uuid.v7()
                local node_input_id: string = uuid.v7()

                local test_input = { message = "test" }

                local workflow_commands = {
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = cycle_node_id,
                            node_type = "userspace.dataflow.node.cycle:cycle",
                            status = consts.STATUS.PENDING,
                            config = {
                                max_iterations = 5,
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                        key = "result"
                                    }
                                }
                            },
                            metadata = {
                                title = "Missing Func ID Test"
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
                            node_id = cycle_node_id,
                            key = input_data_id,
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
                test.contains(result.error, "Cycle requires either func_id or template nodes")

                print("--- Cycle node correctly failed with missing func_id")
            end)

            it("should handle missing input data", function()
                local c, err = client.new()
                test.is_nil(err)

                local cycle_node_id: string = uuid.v7()

                local workflow_commands = {
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = cycle_node_id,
                            node_type = "userspace.dataflow.node.cycle:cycle",
                            status = consts.STATUS.PENDING,
                            config = {
                                func_id = "userspace.dataflow.node.cycle.stub:refine_test_func",
                                max_iterations = 5,
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                        key = "result"
                                    }
                                }
                            },
                            metadata = {
                                title = "No Input Data Test"
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

                print("--- Cycle node correctly failed with no input data")
            end)

            it("should handle nonexistent function", function()
                local c, err = client.new()
                test.is_nil(err)

                local cycle_node_id: string = uuid.v7()
                local input_data_id: string = uuid.v7()
                local node_input_id: string = uuid.v7()

                local test_input = { message = "test" }

                local workflow_commands = {
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = cycle_node_id,
                            node_type = "userspace.dataflow.node.cycle:cycle",
                            status = consts.STATUS.PENDING,
                            config = {
                                func_id = "userspace.dataflow.node:nonexistent_cycle_func",
                                max_iterations = 5,
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                        key = "result"
                                    }
                                }
                            },
                            metadata = {
                                title = "Nonexistent Function Test"
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
                            node_id = cycle_node_id,
                            key = input_data_id,
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

                print("--- Cycle node correctly failed with nonexistent function")
            end)
        end)

        describe("Expression Continue Conditions", function()
            it("should use continue_condition expression to control cycle", function()
                print("=== CONTINUE CONDITION EXPRESSION TEST START ===")

                local c, err = client.new()
                test.is_nil(err)

                local cycle_node_id: string = uuid.v7()
                local input_data_id: string = uuid.v7()
                local node_input_id: string = uuid.v7()

                local test_input = {
                    target_quality = 0.85,
                    initial_text = "Text for expression condition testing"
                }

                local workflow_commands = {
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = cycle_node_id,
                            node_type = "userspace.dataflow.node.cycle:cycle",
                            status = consts.STATUS.PENDING,
                            config = {
                                func_id = "userspace.dataflow.node.cycle.stub:refine_test_func",
                                continue_condition = "state.quality_score < 0.85 && iteration < 8",
                                max_iterations = 10,
                                initial_state = {
                                    refinement_count = 0,
                                    quality_score = 0.3
                                },
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                        key = "expression_result",
                                        content_type = consts.CONTENT_TYPE.JSON
                                    }
                                }
                            },
                            metadata = {
                                title = "Expression Continue Condition Test"
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
                            node_id = cycle_node_id,
                            key = input_data_id,
                            content = "",
                            content_type = "dataflow/reference"
                        }
                    }
                }

                local dataflow_id, create_err = c:create_workflow(workflow_commands, {
                    metadata = {
                        title = "Expression Continue Condition Test Workflow"
                    }
                })
                test.is_nil(create_err)

                local result, exec_err = c:execute(dataflow_id)
                test.is_nil(exec_err)
                test.is_true(result.success)

                local output_data = data_reader.with_dataflow(dataflow_id)
                    :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                    :with_data_keys("expression_result")
                    :fetch_options({ replace_references = true })
                    :one()

                test.not_nil(output_data)

                local output_content = decode_json_content(output_data.content)

                test.is_true(output_content.refinement_complete)
                test.gte(output_content.final_quality, 0.85)

                print("--- Continue condition expression controlled cycle correctly")
            end)

            it("should handle complex continue_condition expressions", function()
                print("=== COMPLEX CONTINUE CONDITION TEST START ===")

                local c, err = client.new()
                test.is_nil(err)

                local cycle_node_id: string = uuid.v7()
                local input_data_id: string = uuid.v7()
                local node_input_id: string = uuid.v7()

                local test_input = {
                    target_quality = 0.9,
                    initial_text = "Complex condition test text"
                }

                local workflow_commands = {
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = cycle_node_id,
                            node_type = "userspace.dataflow.node.cycle:cycle",
                            status = consts.STATUS.PENDING,
                            config = {
                                func_id = "userspace.dataflow.node.cycle.stub:refine_test_func",
                                continue_condition = "(state.quality_score < input.target_quality) && (iteration <= 5) && (state.refinement_count < 6)",
                                max_iterations = 10,
                                initial_state = {
                                    refinement_count = 0,
                                    quality_score = 0.2
                                },
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                        key = "complex_result",
                                        content_type = consts.CONTENT_TYPE.JSON
                                    }
                                }
                            },
                            metadata = {
                                title = "Complex Continue Condition Test"
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
                            node_id = cycle_node_id,
                            key = input_data_id,
                            content = "",
                            content_type = "dataflow/reference"
                        }
                    }
                }

                local dataflow_id, create_err = c:create_workflow(workflow_commands, {
                    metadata = {
                        title = "Complex Continue Condition Test Workflow"
                    }
                })
                test.is_nil(create_err)

                local result, exec_err = c:execute(dataflow_id)
                test.is_nil(exec_err)
                test.is_true(result.success)

                local output_data = data_reader.with_dataflow(dataflow_id)
                    :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                    :with_data_keys("complex_result")
                    :fetch_options({ replace_references = true })
                    :one()

                test.not_nil(output_data)

                local output_content = decode_json_content(output_data.content)

                test.is_true(output_content.refinement_complete)
                test.is_true(output_content.total_refinements <= 6)

                print("--- Complex continue condition expression worked correctly")
            end)


        end)

        describe("Continue Function Support", function()
            it("should support continue_func_id when no continue_condition provided", function()
                print("=== CONTINUE FUNCTION TEST START ===")

                local c, err = client.new()
                test.is_nil(err)

                local cycle_node_id: string = uuid.v7()
                local input_data_id: string = uuid.v7()
                local node_input_id: string = uuid.v7()

                local test_input = {
                    target_quality = 0.75,
                    initial_text = "Continue function test"
                }

                local workflow_commands = {
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = cycle_node_id,
                            node_type = "userspace.dataflow.node.cycle:cycle",
                            status = consts.STATUS.PENDING,
                            config = {
                                func_id = "userspace.dataflow.node.cycle.stub:refine_test_func",
                                continue_func_id = "userspace.dataflow.node.cycle.stub:continue_refinement_func",
                                max_iterations = 8,
                                initial_state = {
                                    refinement_count = 0,
                                    quality_score = 0.3
                                },
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                        key = "func_result",
                                        content_type = consts.CONTENT_TYPE.JSON
                                    }
                                }
                            },
                            metadata = {
                                title = "Continue Function Test"
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
                            node_id = cycle_node_id,
                            key = input_data_id,
                            content = "",
                            content_type = "dataflow/reference"
                        }
                    }
                }

                local dataflow_id, create_err = c:create_workflow(workflow_commands, {
                    metadata = {
                        title = "Continue Function Test Workflow"
                    }
                })
                test.is_nil(create_err)

                local result, exec_err = c:execute(dataflow_id)
                test.is_nil(exec_err)
                test.is_true(result.success)

                local output_data = data_reader.with_dataflow(dataflow_id)
                    :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                    :with_data_keys("func_result")
                    :fetch_options({ replace_references = true })
                    :one()

                test.not_nil(output_data)

                local output_content = decode_json_content(output_data.content)

                test.is_true(output_content.refinement_complete)
                test.gte(output_content.final_quality, 0.75)

                print("--- Continue function works correctly")
            end)

            it("should reject both continue_condition and continue_func_id", function()
                print("=== INVALID CONTINUATION CONFIG TEST START ===")

                local c, err = client.new()
                test.is_nil(err)

                local cycle_node_id: string = uuid.v7()
                local input_data_id: string = uuid.v7()
                local node_input_id: string = uuid.v7()

                local test_input = {
                    target_quality = 0.8,
                    initial_text = "Config validation test"
                }

                local workflow_commands = {
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = cycle_node_id,
                            node_type = "userspace.dataflow.node.cycle:cycle",
                            status = consts.STATUS.PENDING,
                            config = {
                                func_id = "userspace.dataflow.node.cycle.stub:refine_test_func",
                                continue_condition = "iteration < 3",
                                continue_func_id = "userspace.dataflow.node.cycle.stub:continue_refinement_func",
                                max_iterations = 10,
                                initial_state = {
                                    refinement_count = 0,
                                    quality_score = 0.3
                                },
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                        key = "validation_result"
                                    }
                                }
                            },
                            metadata = {
                                title = "Invalid Continuation Config Test"
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
                            node_id = cycle_node_id,
                            key = input_data_id,
                            content = "",
                            content_type = "dataflow/reference"
                        }
                    }
                }

                local dataflow_id, create_err = c:create_workflow(workflow_commands, {
                    metadata = {
                        title = "Invalid Continuation Config Test Workflow"
                    }
                })
                test.is_nil(create_err)

                local result, exec_err = c:execute(dataflow_id)
                test.not_nil(exec_err)
                test.is_false(result.success)
                test.contains(result.error, "Only one continuation method allowed")

                print("--- Multiple continuation methods correctly rejected")
            end)
        end)

        describe("State Persistence", function()
            it("should persist and resume state across iterations", function()
                print("=== STATE PERSISTENCE TEST START ===")

                local c, err = client.new()
                test.is_nil(err)

                local cycle_node_id: string = uuid.v7()
                local input_data_id: string = uuid.v7()
                local node_input_id: string = uuid.v7()

                local test_input = {
                    target_quality = 0.9,
                    initial_text = "Basic text that needs significant refinement and improvement"
                }

                local workflow_commands = {
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = cycle_node_id,
                            node_type = "userspace.dataflow.node.cycle:cycle",
                            status = consts.STATUS.PENDING,
                            config = {
                                func_id = "userspace.dataflow.node.cycle.stub:refine_test_func",
                                max_iterations = 10,
                                initial_state = {
                                    refinement_count = 0,
                                    quality_score = 0.2
                                },
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                        key = "persistence_result",
                                        content_type = consts.CONTENT_TYPE.JSON
                                    }
                                }
                            },
                            metadata = {
                                title = "State Persistence Test"
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
                            node_id = cycle_node_id,
                            key = input_data_id,
                            content = "",
                            content_type = "dataflow/reference"
                        }
                    }
                }

                local dataflow_id, create_err = c:create_workflow(workflow_commands, {
                    metadata = {
                        title = "State Persistence Test Workflow"
                    }
                })
                test.is_nil(create_err)

                local result, exec_err = c:execute(dataflow_id)
                test.is_nil(exec_err)
                test.is_true(result.success)

                local output_data = data_reader.with_dataflow(dataflow_id)
                    :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                    :with_data_keys("persistence_result")
                    :fetch_options({ replace_references = true })
                    :one()

                test.not_nil(output_data)

                local output_content = decode_json_content(output_data.content)

                test.not_nil(output_content.refined_text)
                test.is_true(output_content.refinement_complete)
                test.gte(output_content.final_quality, 0.9)
                test.gt(output_content.total_refinements, 0)

                print("--- State persistence worked correctly")

                local state_data = data_reader.with_dataflow(dataflow_id)
                    :with_data_types("cycle.state")
                    :fetch_options({ replace_references = true })
                    :all()

                test.gt(#state_data, 0)
                print("--- Cycle state data was persisted")
            end)
        end)

        describe("Control Commands", function()
            it("should execute control commands and return child results", function()
                print("=== CYCLE CONTROL COMMANDS TEST START ===")

                local c, err = client.new()
                test.is_nil(err)

                local cycle_node_id: string = uuid.v7()
                local input_data_id: string = uuid.v7()
                local node_input_id: string = uuid.v7()

                local test_input = {
                    target_quality = 0.7,
                    initial_text = "Text for control command testing"
                }

                local workflow_commands = {
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = cycle_node_id,
                            node_type = "userspace.dataflow.node.cycle:cycle",
                            status = consts.STATUS.PENDING,
                            config = {
                                func_id = "userspace.dataflow.node.cycle.stub:refine_test_func",
                                max_iterations = 5,
                                initial_state = {
                                    refinement_count = 0,
                                    quality_score = 0.3
                                },
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                        key = "control_result",
                                        content_type = consts.CONTENT_TYPE.JSON
                                    }
                                }
                            },
                            metadata = {
                                title = "Cycle Control Commands Test"
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
                            node_id = cycle_node_id,
                            key = input_data_id,
                            content = "",
                            content_type = "dataflow/reference"
                        }
                    }
                }

                local dataflow_id, create_err = c:create_workflow(workflow_commands, {
                    metadata = {
                        title = "Cycle Control Commands Test Workflow"
                    }
                })
                test.is_nil(create_err)

                local result, exec_err = c:execute(dataflow_id)
                test.is_nil(exec_err)
                test.is_true(result.success)

                local output_data = data_reader.with_dataflow(dataflow_id)
                    :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                    :with_data_keys("control_result")
                    :fetch_options({ replace_references = true })
                    :one()

                test.not_nil(output_data)

                local output_content = decode_json_content(output_data.content)

                test.is_true(output_content.refinement_complete)
                test.not_nil(output_content.refined_text)
                test.gt(output_content.total_refinements, 0)

                print("--- Cycle control commands executed successfully")

                local child_nodes = data_reader.with_dataflow(dataflow_id)
                    :with_data_types(consts.DATA_TYPE.NODE_OUTPUT)
                    :fetch_options({ replace_references = true })
                    :all()

                test.gt(#child_nodes, 0)
                print("--- Child nodes created via control commands")
            end)
        end)

        describe("Context and Metadata", function()
            it("should pass context to function and update metadata", function()
                local c, err = client.new()
                test.is_nil(err)

                local cycle_node_id: string = uuid.v7()
                local input_data_id: string = uuid.v7()
                local node_input_id: string = uuid.v7()

                local test_input = {
                    target_quality = 0.6,
                    initial_text = "Context test text"
                }

                local test_context = {
                    user_id = "test_user_456",
                    environment = "cycle_test"
                }

                local workflow_commands = {
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = cycle_node_id,
                            node_type = "userspace.dataflow.node.cycle:cycle",
                            status = consts.STATUS.PENDING,
                            config = {
                                func_id = "userspace.dataflow.node.cycle.stub:refine_test_func",
                                context = test_context,
                                max_iterations = 5,
                                initial_state = {
                                    refinement_count = 0,
                                    quality_score = 0.3
                                },
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                        key = "context_result",
                                        content_type = consts.CONTENT_TYPE.JSON
                                    }
                                }
                            },
                            metadata = {
                                title = "Context and Metadata Test"
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
                            node_id = cycle_node_id,
                            key = input_data_id,
                            content = "",
                            content_type = "dataflow/reference"
                        }
                    }
                }

                local dataflow_id, create_err = c:create_workflow(workflow_commands, {
                    metadata = {
                        title = "Context and Metadata Test Workflow"
                    }
                })
                test.is_nil(create_err)

                local result, exec_err = c:execute(dataflow_id)
                test.is_nil(exec_err)
                test.is_true(result.success)

                local output_data = data_reader.with_dataflow(dataflow_id)
                    :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                    :with_data_keys("context_result")
                    :fetch_options({ replace_references = true })
                    :one()

                test.not_nil(output_data)

                local output_content = decode_json_content(output_data.content)

                test.is_true(output_content.refinement_complete)
                test.not_nil(output_content.refined_text)
                test.gte(output_content.final_quality, 0.6)
            end)
        end)

        describe("Template Discovery and Validation", function()
            it("should fail when no func_id or templates provided", function()
                local c, err = client.new()
                test.is_nil(err)

                local cycle_node_id: string = uuid.v7()
                local input_data_id: string = uuid.v7()
                local node_input_id: string = uuid.v7()

                local test_input = { message = "test input" }

                local workflow_commands = {
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = cycle_node_id,
                            node_type = "userspace.dataflow.node.cycle:cycle",
                            status = consts.STATUS.PENDING,
                            config = {
                                max_iterations = 5,
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                        key = "result"
                                    }
                                }
                            },
                            metadata = {
                                title = "No Execution Target Test"
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
                            node_id = cycle_node_id,
                            key = input_data_id,
                            content = "",
                            content_type = "dataflow/reference"
                        }
                    }
                }

                local dataflow_id, create_err = c:create_workflow(workflow_commands, {
                    metadata = {
                        title = "No Execution Target Test Workflow"
                    }
                })
                test.is_nil(create_err)

                local result, exec_err = c:execute(dataflow_id)
                test.not_nil(exec_err)
                test.is_false(result.success)
                test.contains(result.error, "Cycle requires either func_id or template nodes")

                print("--- Cycle correctly failed with no execution target")
            end)

            it("should detect and use template nodes", function()
                print("=== CYCLE TEMPLATE DETECTION TEST START ===")

                local c, err = client.new()
                test.is_nil(err)

                local cycle_node_id: string = uuid.v7()
                local template_node_id: string = uuid.v7()
                local input_data_id: string = uuid.v7()
                local node_input_id: string = uuid.v7()

                local test_input = {
                    initial_value = 1,
                    target = 5,
                    increment = 1
                }

                local workflow_commands = {
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = cycle_node_id,
                            node_type = "userspace.dataflow.node.cycle:cycle",
                            status = consts.STATUS.PENDING,
                            config = {
                                max_iterations = 10,
                                initial_state = { current_value = 1 },
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                        key = "template_result",
                                        content_type = consts.CONTENT_TYPE.JSON
                                    }
                                }
                            },
                            metadata = {
                                title = "Template Detection Cycle Node"
                            }
                        }
                    },
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = template_node_id,
                            node_type = "userspace.dataflow.node.func:node",
                            parent_node_id = cycle_node_id,
                            status = consts.STATUS.TEMPLATE,
                            config = {
                                func_id = "userspace.dataflow.node.cycle.stub:cycle_template_test_func",
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.NODE_OUTPUT,
                                        key = "increment_result"
                                    }
                                }
                            },
                            metadata = {
                                title = "Template Increment Function"
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
                            node_id = cycle_node_id,
                            key = input_data_id,
                            content = "",
                            content_type = "dataflow/reference"
                        }
                    }
                }

                local dataflow_id, create_err = c:create_workflow(workflow_commands, {
                    metadata = {
                        title = "Template Detection Test Workflow"
                    }
                })
                test.is_nil(create_err)

                local result, exec_err = c:execute(dataflow_id)
                test.is_nil(exec_err)
                test.is_true(result.success)

                local output_data = data_reader.with_dataflow(dataflow_id)
                    :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                    :with_data_keys("template_result")
                    :fetch_options({ replace_references = true })
                    :one()

                test.not_nil(output_data)

                local output_content = decode_json_content(output_data.content)

                test.gte(output_content.current_value, 5)
                test.is_true(output_content.template_processed)

                print("--- Template nodes detected and executed successfully")
                print("=== CYCLE TEMPLATE DETECTION TEST COMPLETE ===")
            end)

            it("should detect and use template nodes with expression continue condition", function()
                print("=== CYCLE TEMPLATE WITH EXPRESSION TEST START ===")

                local c, err = client.new()
                test.is_nil(err)

                local cycle_node_id: string = uuid.v7()
                local template_node_id: string = uuid.v7()
                local input_data_id: string = uuid.v7()
                local node_input_id: string = uuid.v7()

                local test_input = {
                    initial_value = 1,
                    target = 5,
                    increment = 1
                }

                local workflow_commands = {
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = cycle_node_id,
                            node_type = "userspace.dataflow.node.cycle:cycle",
                            status = consts.STATUS.PENDING,
                            config = {
                                continue_condition = "state.current_value < input.target && iteration < 8",
                                max_iterations = 10,
                                initial_state = { current_value = 1 },
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                        key = "template_expr_result",
                                        content_type = consts.CONTENT_TYPE.JSON
                                    }
                                }
                            },
                            metadata = {
                                title = "Template with Expression Condition Cycle Node"
                            }
                        }
                    },
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = template_node_id,
                            node_type = "userspace.dataflow.node.func:node",
                            parent_node_id = cycle_node_id,
                            status = consts.STATUS.TEMPLATE,
                            config = {
                                func_id = "userspace.dataflow.node.cycle.stub:cycle_template_test_func",
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.NODE_OUTPUT,
                                        key = "increment_result"
                                    }
                                }
                            },
                            metadata = {
                                title = "Template Increment Function with Expression"
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
                            node_id = cycle_node_id,
                            key = input_data_id,
                            content = "",
                            content_type = "dataflow/reference"
                        }
                    }
                }

                local dataflow_id, create_err = c:create_workflow(workflow_commands, {
                    metadata = {
                        title = "Template with Expression Detection Test Workflow"
                    }
                })
                test.is_nil(create_err)

                local result, exec_err = c:execute(dataflow_id)
                test.is_nil(exec_err)
                test.is_true(result.success)

                local output_data = data_reader.with_dataflow(dataflow_id)
                    :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                    :with_data_keys("template_expr_result")
                    :fetch_options({ replace_references = true })
                    :one()

                test.not_nil(output_data)

                local output_content = decode_json_content(output_data.content)

                test.gte(output_content.current_value, 5)
                test.is_true(output_content.template_processed)

                print("--- Template nodes with expression condition executed successfully")
                print("=== CYCLE TEMPLATE WITH EXPRESSION TEST COMPLETE ===")
            end)
        end)

        describe("Template Execution with State", function()
            it("should execute template with cycle context", function()
                print("=== CYCLE TEMPLATE CONTEXT TEST START ===")

                local c, err = client.new()
                test.is_nil(err)

                local cycle_node_id: string = uuid.v7()
                local template_node_id: string = uuid.v7()
                local input_data_id: string = uuid.v7()
                local node_input_id: string = uuid.v7()

                local test_input = {
                    message = "Template context test",
                    multiplier = 2
                }

                local workflow_commands = {
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = cycle_node_id,
                            node_type = "userspace.dataflow.node.cycle:cycle",
                            status = consts.STATUS.PENDING,
                            config = {
                                max_iterations = 3,
                                initial_state = {
                                    accumulator = 0,
                                    iteration_count = 0
                                },
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                        key = "context_result",
                                        content_type = consts.CONTENT_TYPE.JSON
                                    }
                                }
                            },
                            metadata = {
                                title = "Template Context Cycle Node"
                            }
                        }
                    },
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = template_node_id,
                            node_type = "userspace.dataflow.node.func:node",
                            parent_node_id = cycle_node_id,
                            status = consts.STATUS.TEMPLATE,
                            config = {
                                func_id = "userspace.dataflow.node.cycle.stub:cycle_context_test_func",
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.NODE_OUTPUT,
                                        key = "context_result"
                                    }
                                }
                            },
                            metadata = {
                                title = "Template Context Test Function"
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
                            node_id = cycle_node_id,
                            key = input_data_id,
                            content = "",
                            content_type = "dataflow/reference"
                        }
                    }
                }

                local dataflow_id, create_err = c:create_workflow(workflow_commands, {
                    metadata = {
                        title = "Template Context Test Workflow"
                    }
                })
                test.is_nil(create_err)

                local result, exec_err = c:execute(dataflow_id)
                test.is_nil(exec_err)
                test.is_true(result.success)

                local output_data = data_reader.with_dataflow(dataflow_id)
                    :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                    :with_data_keys("context_result")
                    :fetch_options({ replace_references = true })
                    :one()

                test.not_nil(output_data)

                local output_content = decode_json_content(output_data.content)

                test.not_nil(output_content.received_input)
                test.not_nil(output_content.received_state)
                test.not_nil(output_content.received_iteration)
                test.gt(output_content.final_accumulator, 0)

                print("--- Template received proper cycle context")
                print("=== CYCLE TEMPLATE CONTEXT TEST COMPLETE ===")
            end)

            it("should handle template chains within cycle", function()
                print("=== CYCLE TEMPLATE CHAIN TEST START ===")

                local c, err = client.new()
                test.is_nil(err)

                local cycle_node_id: string = uuid.v7()
                local template1_id: string = uuid.v7()
                local template2_id: string = uuid.v7()
                local input_data_id: string = uuid.v7()
                local node_input_id: string = uuid.v7()

                local test_input = {
                    start_value = 10,
                    target_threshold = 100
                }

                local workflow_commands = {
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = cycle_node_id,
                            node_type = "userspace.dataflow.node.cycle:cycle",
                            status = consts.STATUS.PENDING,
                            config = {
                                max_iterations = 5,
                                initial_state = {
                                    current_value = 10
                                },
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                        key = "chain_result",
                                        content_type = consts.CONTENT_TYPE.JSON
                                    }
                                }
                            },
                            metadata = {
                                title = "Template Chain Cycle Node"
                            }
                        }
                    },
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = template1_id,
                            node_type = "userspace.dataflow.node.func:node",
                            parent_node_id = cycle_node_id,
                            status = consts.STATUS.TEMPLATE,
                            config = {
                                func_id = "userspace.dataflow.node.cycle.stub:multiply_template_func",
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.NODE_INPUT,
                                        node_id = template2_id,
                                        key = "default"
                                    }
                                }
                            },
                            metadata = {
                                title = "Multiply Template"
                            }
                        }
                    },
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = template2_id,
                            node_type = "userspace.dataflow.node.func:node",
                            parent_node_id = cycle_node_id,
                            status = consts.STATUS.TEMPLATE,
                            config = {
                                func_id = "userspace.dataflow.node.cycle.stub:validate_template_func",
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.NODE_OUTPUT,
                                        key = "chain_result"
                                    }
                                }
                            },
                            metadata = {
                                title = "Validate Template"
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
                            node_id = cycle_node_id,
                            key = input_data_id,
                            content = "",
                            content_type = "dataflow/reference"
                        }
                    }
                }

                local dataflow_id, create_err = c:create_workflow(workflow_commands, {
                    metadata = {
                        title = "Template Chain Test Workflow"
                    }
                })
                test.is_nil(create_err)

                local result, exec_err = c:execute(dataflow_id)
                test.is_nil(exec_err)
                test.is_true(result.success)

                local output_data = data_reader.with_dataflow(dataflow_id)
                    :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                    :with_data_keys("chain_result")
                    :fetch_options({ replace_references = true })
                    :one()

                test.not_nil(output_data)

                local output_content = decode_json_content(output_data.content)

                test.is_true(output_content.processed_by_chain)
                test.gte(output_content.final_value, 100)
                test.gt(output_content.iterations_completed, 0)

                print("--- Template chain executed successfully within cycle")
                print("=== CYCLE TEMPLATE CHAIN TEST COMPLETE ===")
            end)
        end)

        describe("Template State Management", function()
            it("should persist state across template iterations", function()
                print("=== CYCLE TEMPLATE STATE PERSISTENCE TEST START ===")

                local c, err = client.new()
                test.is_nil(err)

                local cycle_node_id: string = uuid.v7()
                local template_node_id: string = uuid.v7()
                local input_data_id: string = uuid.v7()
                local node_input_id: string = uuid.v7()

                local test_input = {
                    initial_count = 0,
                    increment = 5,
                    target = 25
                }

                local workflow_commands = {
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = cycle_node_id,
                            node_type = "userspace.dataflow.node.cycle:cycle",
                            status = consts.STATUS.PENDING,
                            config = {
                                max_iterations = 10,
                                initial_state = {
                                    count = 0,
                                    history = {}
                                },
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                        key = "persistence_result",
                                        content_type = consts.CONTENT_TYPE.JSON
                                    }
                                }
                            },
                            metadata = {
                                title = "Template State Persistence Cycle"
                            }
                        }
                    },
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = template_node_id,
                            node_type = "userspace.dataflow.node.func:node",
                            parent_node_id = cycle_node_id,
                            status = consts.STATUS.TEMPLATE,
                            config = {
                                func_id = "userspace.dataflow.node.cycle.stub:state_persistence_template_func",
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.NODE_OUTPUT,
                                        key = "persistence_result"
                                    }
                                }
                            },
                            metadata = {
                                title = "State Persistence Template"
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
                            node_id = cycle_node_id,
                            key = input_data_id,
                            content = "",
                            content_type = "dataflow/reference"
                        }
                    }
                }

                local dataflow_id, create_err = c:create_workflow(workflow_commands, {
                    metadata = {
                        title = "Template State Persistence Test Workflow"
                    }
                })
                test.is_nil(create_err)

                local result, exec_err = c:execute(dataflow_id)
                test.is_nil(exec_err)
                test.is_true(result.success)

                local output_data = data_reader.with_dataflow(dataflow_id)
                    :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                    :with_data_keys("persistence_result")
                    :fetch_options({ replace_references = true })
                    :one()

                test.not_nil(output_data)

                local output_content = decode_json_content(output_data.content)

                test.gte(output_content.final_count, 25)
                test.gt(output_content.total_iterations, 0)
                test.eq(#output_content.iteration_history, output_content.total_iterations)
                test.is_true(output_content.state_persisted)

                local state_data = data_reader.with_dataflow(dataflow_id)
                    :with_data_types("cycle.state")
                    :fetch_options({ replace_references = true })
                    :all()

                test.gt(#state_data, 0)

                print("--- Template state persistence working correctly")
                print("=== CYCLE TEMPLATE STATE PERSISTENCE TEST COMPLETE ===")
            end)
        end)

        describe("Error Handling", function()
            it("should handle template execution failures", function()
                print("=== CYCLE TEMPLATE ERROR HANDLING TEST START ===")

                local c, err = client.new()
                test.is_nil(err)

                local cycle_node_id: string = uuid.v7()
                local template_node_id: string = uuid.v7()
                local input_data_id: string = uuid.v7()
                local node_input_id: string = uuid.v7()

                local test_input = {
                    should_fail = true,
                    failure_iteration = 2
                }

                local workflow_commands = {
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = cycle_node_id,
                            node_type = "userspace.dataflow.node.cycle:cycle",
                            status = consts.STATUS.PENDING,
                            config = {
                                max_iterations = 5,
                                initial_state = {
                                    iteration_count = 0
                                },
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                        key = "error_result"
                                    }
                                }
                            },
                            metadata = {
                                title = "Template Error Handling Cycle"
                            }
                        }
                    },
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = template_node_id,
                            node_type = "userspace.dataflow.node.func:node",
                            parent_node_id = cycle_node_id,
                            status = consts.STATUS.TEMPLATE,
                            config = {
                                func_id = "userspace.dataflow.node.cycle.stub:failing_template_func",
                                data_targets = {
                                    {
                                        data_type = consts.DATA_TYPE.NODE_OUTPUT,
                                        key = "error_result"
                                    }
                                }
                            },
                            metadata = {
                                title = "Failing Template"
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
                            node_id = cycle_node_id,
                            key = input_data_id,
                            content = "",
                            content_type = "dataflow/reference"
                        }
                    }
                }

                local dataflow_id, create_err = c:create_workflow(workflow_commands, {
                    metadata = {
                        title = "Template Error Handling Test Workflow"
                    }
                })
                test.is_nil(create_err)

                local result, exec_err = c:execute(dataflow_id)
                test.not_nil(exec_err)
                test.is_false(result.success)
                test.contains(result.error, "Template function failed on iteration 2 as requested")

                local node_results = data_reader.with_dataflow(dataflow_id)
                    :with_data_types(consts.DATA_TYPE.NODE_RESULT)
                    :fetch_options({ replace_references = true })
                    :all()
                local child_failure = nil
                local cycle_failure = nil
                for _, node_result in ipairs(node_results) do
                    if node_result.discriminator == "result.error" then
                        local content = decode_json_content(node_result.content)
                        if node_result.node_id == cycle_node_id then
                            cycle_failure = content
                        elseif type(content) == "table" and type(content.error) == "table" then
                            child_failure = content.error
                        end
                    end
                end

                test.is_table(child_failure)
                test.is_table(cycle_failure)
                test.is_table(cycle_failure.error)
                test.eq(cycle_failure.error.code, child_failure.code)
                test.eq(cycle_failure.error.message, child_failure.message)

                print("--- Template execution errors handled correctly")
                print("=== CYCLE TEMPLATE ERROR HANDLING TEST COMPLETE ===")
            end)
        end)
    end)
end

return test.run_cases(define_tests)
