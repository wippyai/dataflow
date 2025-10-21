local uuid = require("uuid")
local ctx = require("ctx")

local function run(input_data)
    if not input_data then
        return nil, "input data is required"
    end

    local message = input_data.message or "test input"
    local workflow_type = input_data.workflow_type or "simple"
    local current_node_id = ctx.get("node_id")

    if workflow_type == "simple" then
        -- Create a simple single-node workflow
        local child_node_id = uuid.v7()
        local output_data_id = uuid.v7()

        return {
            result = "Created simple workflow",
            message = message,
            _control = {
                commands = {
                    {
                        type = "CREATE_NODE",
                        payload = {
                            node_id = child_node_id,
                            node_type = "userspace.dataflow.node.func:node",
                            status = "pending",
                            parent_node_id = current_node_id,
                            config = {
                                func_id = "userspace.dataflow.node.func:test_func",
                                data_targets = {
                                    {
                                        data_type = "node.output",
                                        key = "simple_result",
                                        content_type = "application/json",
                                        data_id = output_data_id
                                    }
                                }
                            },
                            metadata = {
                                title = "Simple Control Child",
                                created_by_control = true
                            }
                        }
                    },
                    {
                        type = "CREATE_DATA",
                        payload = {
                            data_id = uuid.v7(),
                            data_type = "node.input",
                            content = {
                                message = message,
                                delay_ms = 50,
                                should_fail = false
                            },
                            content_type = "application/json",
                            node_id = child_node_id,
                            key = "default"
                        }
                    }
                }
            }
        }

    elseif workflow_type == "chain" then
        -- Create a 2-node chain workflow
        local node_1_id = uuid.v7()
        local node_2_id = uuid.v7()
        local intermediate_data_id = uuid.v7()
        local final_output_data_id = uuid.v7()

        return {
            result = "Created chain workflow",
            message = message,
            _control = {
                commands = {
                    -- Node 1
                    {
                        type = "CREATE_NODE",
                        payload = {
                            node_id = node_1_id,
                            node_type = "userspace.dataflow.node.func:node",
                            status = "pending",
                            parent_node_id = current_node_id,
                            config = {
                                func_id = "userspace.dataflow.node.func:test_func",
                                data_targets = {
                                    {
                                        data_type = "node.input",
                                        node_id = node_2_id,
                                        key = "default",
                                        content_type = "application/json",
                                        data_id = intermediate_data_id
                                    }
                                }
                            },
                            metadata = {
                                title = "Chain Step 1",
                                chain_step = 1
                            }
                        }
                    },
                    -- Node 2
                    {
                        type = "CREATE_NODE",
                        payload = {
                            node_id = node_2_id,
                            node_type = "userspace.dataflow.node.func:node",
                            status = "pending",
                            parent_node_id = current_node_id,
                            config = {
                                func_id = "userspace.dataflow.node.func:test_func",
                                data_targets = {
                                    {
                                        data_type = "node.output",
                                        key = "chain_result",
                                        content_type = "application/json",
                                        data_id = final_output_data_id
                                    }
                                }
                            },
                            metadata = {
                                title = "Chain Step 2",
                                chain_step = 2
                            }
                        }
                    },
                    -- Input for Node 1
                    {
                        type = "CREATE_DATA",
                        payload = {
                            data_id = uuid.v7(),
                            data_type = "node.input",
                            content = {
                                message = message .. " (step 1)",
                                delay_ms = 25
                            },
                            content_type = "application/json",
                            node_id = node_1_id,
                            key = "default"
                        }
                    }
                }
            }
        }

    elseif workflow_type == "parallel" then
        -- Create 2 parallel nodes that both contribute to output
        local node_a_id = uuid.v7()
        local node_b_id = uuid.v7()
        local output_a_data_id = uuid.v7()
        local output_b_data_id = uuid.v7()

        return {
            result = "Created parallel workflow",
            message = message,
            _control = {
                commands = {
                    -- Node A
                    {
                        type = "CREATE_NODE",
                        payload = {
                            node_id = node_a_id,
                            node_type = "userspace.dataflow.node.func:node",
                            status = "pending",
                            parent_node_id = current_node_id,
                            config = {
                                func_id = "userspace.dataflow.node.func:test_func",
                                data_targets = {
                                    {
                                        data_type = "node.output",
                                        key = "parallel_a_result",
                                        content_type = "application/json",
                                        data_id = output_a_data_id
                                    }
                                }
                            },
                            metadata = {
                                title = "Parallel Branch A",
                                branch = "A"
                            }
                        }
                    },
                    -- Node B
                    {
                        type = "CREATE_NODE",
                        payload = {
                            node_id = node_b_id,
                            node_type = "userspace.dataflow.node.func:node",
                            status = "pending",
                            parent_node_id = current_node_id,
                            config = {
                                func_id = "userspace.dataflow.node.func:test_func",
                                data_targets = {
                                    {
                                        data_type = "node.output",
                                        key = "parallel_b_result",
                                        content_type = "application/json",
                                        data_id = output_b_data_id
                                    }
                                }
                            },
                            metadata = {
                                title = "Parallel Branch B",
                                branch = "B"
                            }
                        }
                    },
                    -- Input for Node A
                    {
                        type = "CREATE_DATA",
                        payload = {
                            data_id = uuid.v7(),
                            data_type = "node.input",
                            content = {
                                message = message .. " (branch A)",
                                delay_ms = 30
                            },
                            content_type = "application/json",
                            node_id = node_a_id,
                            key = "default"
                        }
                    },
                    -- Input for Node B
                    {
                        type = "CREATE_DATA",
                        payload = {
                            data_id = uuid.v7(),
                            data_type = "node.input",
                            content = {
                                message = message .. " (branch B)",
                                delay_ms = 40
                            },
                            content_type = "application/json",
                            node_id = node_b_id,
                            key = "default"
                        }
                    }
                }
            }
        }
    else
        return {
            result = "No workflow created",
            message = message,
            error = "Unknown workflow_type: " .. workflow_type
        }
    end
end

return { run = run }