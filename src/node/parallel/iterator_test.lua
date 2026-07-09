local test = require("test")
local _uuid = require("uuid")
local json = require("json")
local consts = require("consts")
local iterator = require("iterator")

local function define_tests()
    describe("Iterator Tests", function()
        describe("Single Iteration Creation", function()
            it("should create iteration with simple template graph", function()
                local parent_node = {
                    dataflow_id = "test-df",
                    node_id = "parent",
                    path = { "ancestor1", "ancestor2" },
                    command = function(self: any, cmd: any)
                        self._commands = self._commands or {}
                        table.insert(self._commands, cmd)
                    end,
                    data = function(self: any, data_type: string, content: any, options: any?)
                        self._data_calls = self._data_calls or {}
                        table.insert(self._data_calls, {
                            data_type = data_type,
                            content = content,
                            options = options
                        })
                    end
                }

                local template_graph = {
                    nodes = {
                        ["template1"] = {
                            node_id = "template1",
                            type = "func_node",
                            config = {},
                            metadata = {
                                title = "Test Function Node",
                                description = "A test node for validation"
                            }
                        }
                    },
                    get_roots = function(_self: any)
                        return { "template1" }
                    end
                }

                local input_item = { message = "test", value = 42 }
                local iteration_index = 1
                local iteration_input_key = "default"

                local iteration_info = iterator.create_iteration(
                    parent_node, template_graph, input_item, iteration_index, iteration_input_key
                )

                test.eq(iteration_info.iteration, 1)
                test.eq(iteration_info.input_item, input_item)
                test.not_nil(iteration_info.uuid_mapping)
                test.not_nil(iteration_info.uuid_mapping["template1"])
                test.eq(#iteration_info.root_nodes, 1)

                test.eq(#iteration_info.child_path, 3)
                test.eq(iteration_info.child_path[1], "ancestor1")
                test.eq(iteration_info.child_path[2], "ancestor2")
                test.eq(iteration_info.child_path[3], "parent")

                local pn = parent_node :: any
                test.not_nil(pn._commands)
                test.eq(#pn._commands, 1)
                test.eq(pn._commands[1].type, consts.COMMAND_TYPES.CREATE_NODE)

                local created_node = pn._commands[1].payload
                test.not_nil(created_node.metadata)
                test.eq(created_node.metadata.title, "Test Function Node")
                test.eq(created_node.metadata.description, "A test node for validation")
                test.eq(created_node.metadata.iteration, 1)
                test.eq(created_node.metadata.template_source, "template1")

                test.not_nil(pn._data_calls)
                test.eq(#pn._data_calls, 1)
                test.eq(pn._data_calls[1].data_type, consts.DATA_TYPE.NODE_INPUT)
            end)

            it("should handle templates without metadata", function()
                local parent_node = {
                    dataflow_id = "test-df",
                    node_id = "parent",
                    path = {},
                    command = function(self: any, cmd: any)
                        self._commands = self._commands or {}
                        table.insert(self._commands, cmd)
                    end,
                    data = function(self: any, data_type: string, content: any, options: any?)
                        self._data_calls = self._data_calls or {}
                        table.insert(self._data_calls, {
                            data_type = data_type,
                            content = content,
                            options = options
                        })
                    end
                }

                local template_graph = {
                    nodes = {
                        ["template1"] = {
                            node_id = "template1",
                            type = "func_node",
                            config = {}
                        }
                    },
                    get_roots = function(_self: any)
                        return { "template1" }
                    end
                }

                local iteration_info = iterator.create_iteration(
                    parent_node, template_graph, { test = "data" }, 2, "default"
                )

                test.eq(iteration_info.iteration, 2)

                local created_node = (parent_node :: any)._commands[1].payload
                test.not_nil(created_node.metadata)
                test.is_nil(created_node.metadata.title)
                test.eq(created_node.metadata.iteration, 2)
                test.eq(created_node.metadata.template_source, "template1")
            end)

            it("should handle templates with metadata but no title", function()
                local parent_node = {
                    dataflow_id = "test-df",
                    node_id = "parent",
                    path = {},
                    command = function(self: any, cmd: any)
                        self._commands = self._commands or {}
                        table.insert(self._commands, cmd)
                    end,
                    data = function(self: any, data_type: string, content: any, options: any?)
                        self._data_calls = self._data_calls or {}
                        table.insert(self._data_calls, {
                            data_type = data_type,
                            content = content,
                            options = options
                        })
                    end
                }

                local template_graph = {
                    nodes = {
                        ["template1"] = {
                            node_id = "template1",
                            type = "func_node",
                            config = {},
                            metadata = {
                                description = "A node without title",
                                custom_field = "custom_value"
                            }
                        }
                    },
                    get_roots = function(_self: any)
                        return { "template1" }
                    end
                }

                local iteration_info = iterator.create_iteration(
                    parent_node, template_graph, { test = "data" }, 3, "default"
                )

                test.eq(iteration_info.iteration, 3)

                local created_node = (parent_node :: any)._commands[1].payload
                test.not_nil(created_node.metadata)
                test.is_nil(created_node.metadata.title)
                test.eq(created_node.metadata.description, "A node without title")
                test.eq(created_node.metadata.custom_field, "custom_value")
                test.eq(created_node.metadata.iteration, 3)
                test.eq(created_node.metadata.template_source, "template1")
            end)

            it("should handle multiple templates with dependencies", function()
                local parent_node = {
                    dataflow_id = "test-df",
                    node_id = "parent",
                    path = {},
                    command = function(self: any, cmd: any)
                        self._commands = self._commands or {}
                        table.insert(self._commands, cmd)
                    end,
                    data = function(self: any, data_type: string, content: any, options: any?)
                        self._data_calls = self._data_calls or {}
                        table.insert(self._data_calls, {
                            data_type = data_type,
                            content = content,
                            options = options
                        })
                    end
                }

                local template_graph = {
                    nodes = {
                        ["template1"] = {
                            node_id = "template1",
                            type = "func_node",
                            config = {
                                data_targets = {
                                    {
                                        data_type = "node.input",
                                        node_id = "template2"
                                    }
                                }
                            },
                            metadata = {
                                title = "First Node",
                                order = 1
                            }
                        },
                        ["template2"] = {
                            node_id = "template2",
                            type = "func_node",
                            config = {},
                            metadata = {
                                title = "Second Node",
                                order = 2
                            }
                        }
                    },
                    get_roots = function(_self: any)
                        return { "template1" }
                    end
                }

                local input_item = { data = "test" }
                local iteration_info = iterator.create_iteration(
                    parent_node, template_graph, input_item, 4, "default"
                )

                test.eq(#iteration_info.root_nodes, 1)
                local pn = parent_node :: any
                test.not_nil(pn._commands)
                test.eq(#pn._commands, 2)
                test.eq(#pn._data_calls, 1)

                local template1_cmd: any = nil
                local template2_cmd: any = nil

                for _, cmd in ipairs(pn._commands) do
                    if cmd.payload.metadata.template_source == "template1" then
                        template1_cmd = cmd
                    elseif cmd.payload.metadata.template_source == "template2" then
                        template2_cmd = cmd
                    end
                end

                test.not_nil(template1_cmd)
                test.not_nil(template2_cmd)

                test.eq(template1_cmd.payload.metadata.title, "First Node")
                test.eq(template1_cmd.payload.metadata.order, 1)
                test.eq(template2_cmd.payload.metadata.title, "Second Node")
                test.eq(template2_cmd.payload.metadata.order, 2)

                local data_targets = template1_cmd.payload.config.data_targets
                test.eq(data_targets[1].node_id, iteration_info.uuid_mapping["template2"])
            end)
        end)

        describe("Batch Creation", function()
            it("should create multiple iterations in batch", function()
                local parent_node = {
                    dataflow_id = "test-df",
                    node_id = "parent",
                    path = {},
                    command = function(self: any, cmd: any)
                        self._commands = self._commands or {}
                        table.insert(self._commands, cmd)
                    end,
                    data = function(self: any, data_type: string, content: any, options: any?)
                        self._data_calls = self._data_calls or {}
                        table.insert(self._data_calls, {
                            data_type = data_type,
                            content = content,
                            options = options
                        })
                    end
                }

                local template_graph = {
                    nodes = {
                        ["template1"] = {
                            node_id = "template1",
                            type = "func_node",
                            config = {},
                            metadata = {
                                title = "Batch Processing Node"
                            }
                        }
                    },
                    get_roots = function(_self: any)
                        return { "template1" }
                    end
                }

                local items = {
                    { id = 1, data = "first" },
                    { id = 2, data = "second" },
                    { id = 3, data = "third" }
                }

                local iterations, err = iterator.create_batch(
                    parent_node, template_graph, items, 1, 3, "default"
                )

                test.is_nil(err)
                test.not_nil(iterations)
                test.eq(#iterations, 3)

                for i, iteration in ipairs(iterations) do
                    test.eq(iteration.iteration, i)
                    test.eq(iteration.input_item, items[i])
                    test.eq(#iteration.root_nodes, 1)
                end

                local pn = parent_node :: any
                test.eq(#pn._commands, 3)
                test.eq(#pn._data_calls, 3)

                for i, cmd in ipairs(pn._commands) do
                    test.eq(cmd.payload.metadata.title, "Batch Processing Node")
                    test.eq(cmd.payload.metadata.iteration, i)
                end
            end)

            it("should handle partial batch range", function()
                local parent_node = {
                    dataflow_id = "test-df",
                    node_id = "parent",
                    path = {},
                    command = function(self: any, cmd: any)
                        self._commands = self._commands or {}
                        table.insert(self._commands, cmd)
                    end,
                    data = function(self: any, data_type: string, content: any, options: any?)
                        self._data_calls = self._data_calls or {}
                        table.insert(self._data_calls, {
                            data_type = data_type,
                            content = content,
                            options = options
                        })
                    end
                }

                local template_graph = {
                    nodes = {
                        ["template1"] = {
                            node_id = "template1",
                            type = "func_node",
                            config = {},
                            metadata = {
                                title = "Partial Batch Node"
                            }
                        }
                    },
                    get_roots = function(_self: any)
                        return { "template1" }
                    end
                }

                local items = { "a", "b", "c", "d", "e" }

                local iterations, err = iterator.create_batch(
                    parent_node, template_graph, items, 2, 4, "default"
                )

                test.is_nil(err)
                test.not_nil(iterations)
                local iters = iterations :: any
                test.eq(#iters, 3)
                test.eq(iters[1].iteration, 2)
                test.eq(iters[2].iteration, 3)
                test.eq(iters[3].iteration, 4)
                test.eq(iters[1].input_item, "b")
                test.eq(iters[2].input_item, "c")
                test.eq(iters[3].input_item, "d")

                local pn = parent_node :: any
                test.eq(pn._commands[1].payload.metadata.title, "Partial Batch Node")
                test.eq(pn._commands[2].payload.metadata.title, "Partial Batch Node")
                test.eq(pn._commands[3].payload.metadata.title, "Partial Batch Node")
            end)

            it("should validate batch parameters", function()
                local parent_node = {
                    dataflow_id = "test-df",
                    node_id = "parent"
                }

                local template_graph = {
                    nodes = {},
                    get_roots = function(_self: any) return {} end
                }

                local iterations1, err1 = iterator.create_batch(nil, template_graph, {}, 1, 1, "default")
                test.is_nil(iterations1)
                test.contains(err1, "Missing required parameters")

                local iterations2, err2 = iterator.create_batch(parent_node, template_graph, {}, 0, 1, "default")
                test.is_nil(iterations2)
                test.contains(err2, "Invalid batch range")

                local iterations3, err3 = iterator.create_batch(parent_node, template_graph, {"a"}, 1, 2, "default")
                test.is_nil(iterations3)
                test.contains(err3, "Invalid batch range")
            end)
        end)

        describe("Config Remapping", function()
            it("should remap data_targets node references", function()
                local config = {
                    func_id = "test_func",
                    data_targets = {
                        {
                            data_type = "node.input",
                            node_id = "template1"
                        },
                        {
                            data_type = "workflow.output",
                            key = "result"
                        }
                    }
                }

                local uuid_mapping = {
                    ["template1"] = "actual-node-123"
                }

                local remapped = iterator.remap_template_config(config, uuid_mapping) :: any

                test.eq(remapped.func_id, "test_func")
                test.eq(#remapped.data_targets, 2)
                test.eq(remapped.data_targets[1].node_id, "actual-node-123")
                test.is_nil(remapped.data_targets[2].node_id)
                test.eq(remapped.data_targets[2].key, "result")
            end)

            it("should remap error_targets node references", function()
                local config = {
                    error_targets = {
                        {
                            data_type = "node.input",
                            node_id = "template2"
                        }
                    }
                }

                local uuid_mapping = {
                    ["template2"] = "actual-node-456"
                }

                local remapped = iterator.remap_template_config(config, uuid_mapping) :: any

                test.eq(#remapped.error_targets, 1)
                test.eq(remapped.error_targets[1].node_id, "actual-node-456")
            end)

            it("should handle nil config", function()
                local remapped = iterator.remap_template_config(nil, {})
                test.is_table(remapped)
                test.is_nil(next(remapped))
            end)

            it("should preserve non-remappable references", function()
                local config = {
                    data_targets = {
                        {
                            data_type = "node.input",
                            node_id = "external-node"
                        }
                    }
                }

                local uuid_mapping = {
                    ["template1"] = "actual-node-123"
                }

                local remapped = iterator.remap_template_config(config, uuid_mapping) :: any
                test.eq(remapped.data_targets[1].node_id, "external-node")
            end)
        end)

        describe("Result Collection", function()
            it("should collect results from iteration nodes", function()
                local parent_node = {
                    dataflow_id = "test-df"
                }

                local iteration_info = {
                    uuid_mapping = {
                        ["template1"] = "actual-node-1",
                        ["template2"] = "actual-node-2"
                    }
                }

                local mock_data_reader = {
                    with_dataflow = function(_dataflow_id: any)
                        return {
                            with_nodes = function(_node_ids: any)
                                return {
                                    with_data_types = function(_data_type: any)
                                        return {
                                            fetch_options = function(_options: any)
                                                return {
                                                    all = function()
                                                        return {
                                                            {
                                                                key = "result",
                                                                content = { message = "success", value = 123 },
                                                                node_id = "actual-node-2",
                                                                discriminator = "success"
                                                            }
                                                        }, nil
                                                    end
                                                }
                                            end
                                        }
                                    end
                                }
                            end
                        }, nil
                    end
                }

                local deps = { data_reader = mock_data_reader }

                local result, err = iterator.collect_results(parent_node, iteration_info, deps)

                test.is_nil(err)
                test.not_nil(result)
                local res = result :: any
                test.eq(res.message, "success")
                test.eq(res.value, 123)
            end)

            it("should handle multiple outputs", function()
                local parent_node = {
                    dataflow_id = "test-df"
                }

                local iteration_info = {
                    uuid_mapping = {
                        ["template1"] = "actual-node-1"
                    }
                }

                local mock_data_reader = {
                    with_dataflow = function(_dataflow_id: any)
                        return {
                            with_nodes = function(_node_ids: any)
                                return {
                                    with_data_types = function(_data_type: any)
                                        return {
                                            fetch_options = function(_options: any)
                                                return {
                                                    all = function()
                                                        return {
                                                            {
                                                                key = "result1",
                                                                content = "first",
                                                                node_id = "actual-node-1",
                                                                discriminator = "success"
                                                            },
                                                            {
                                                                key = "result2",
                                                                content = "second",
                                                                node_id = "actual-node-1",
                                                                discriminator = "success"
                                                            }
                                                        }, nil
                                                    end
                                                }
                                            end
                                        }
                                    end
                                }
                            end
                        }, nil
                    end
                }

                local deps = { data_reader = mock_data_reader }

                local result, err = iterator.collect_results(parent_node, iteration_info, deps)

                test.is_nil(err)
                test.is_table(result)
                local res = result :: any
                test.eq(#res, 2)
                test.eq(res[1].content, "first")
                test.eq(res[2].content, "second")
            end)

            it("should handle data reader errors", function()
                local parent_node = {
                    dataflow_id = "test-df"
                }

                local iteration_info = {
                    uuid_mapping = { ["template1"] = "actual-node-1" }
                }

                local mock_data_reader = {
                    with_dataflow = function(_dataflow_id: any)
                        return nil, "Database connection failed"
                    end
                }

                local deps = { data_reader = mock_data_reader }

                local result, err = iterator.collect_results(parent_node, iteration_info, deps)

                test.is_nil(result)
                test.contains(err, "Failed to create data reader")
            end)

            it("should handle query errors", function()
                local parent_node = {
                    dataflow_id = "test-df"
                }

                local iteration_info = {
                    uuid_mapping = { ["template1"] = "actual-node-1" }
                }

                local mock_data_reader = {
                    with_dataflow = function(_dataflow_id: any)
                        return {
                            with_nodes = function(_node_ids: any)
                                return {
                                    with_data_types = function(_data_type: any)
                                        return {
                                            fetch_options = function(_options: any)
                                                return {
                                                    all = function()
                                                        return nil, "Query execution failed"
                                                    end
                                                }
                                            end
                                        }
                                    end
                                }
                            end
                        }, nil
                    end
                }

                local deps = { data_reader = mock_data_reader }

                local result, err = iterator.collect_results(parent_node, iteration_info, deps)

                test.is_nil(result)
                test.contains(err, "Failed to query output data")
            end)

            it("should handle no output data", function()
                local parent_node = {
                    dataflow_id = "test-df"
                }

                local iteration_info = {
                    uuid_mapping = { ["template1"] = "actual-node-1" }
                }

                local mock_data_reader = {
                    with_dataflow = function(_dataflow_id: any)
                        return {
                            with_nodes = function(_node_ids: any)
                                return {
                                    with_data_types = function(_data_type: any)
                                        return {
                                            fetch_options = function(_options: any)
                                                return {
                                                    all = function()
                                                        return {}, nil
                                                    end
                                                }
                                            end
                                        }
                                    end
                                }
                            end
                        }, nil
                    end
                }

                local deps = { data_reader = mock_data_reader }

                local result, err = iterator.collect_results(parent_node, iteration_info, deps)

                test.is_nil(result)
                test.contains(err, "No output data found")
            end)

            it("preserves a structured template failure envelope", function()
                local parent_node = {
                    dataflow_id = "test-df"
                }

                local iteration_info = {
                    uuid_mapping = { ["template1"] = "actual-node-1" }
                }

                local child_failure = {
                    code = "MISSING_CUSTOMER_ID",
                    message = "customer_id is required for item bluebird"
                }

                local mock_data_reader = {
                    with_dataflow = function(_dataflow_id: any)
                        return {
                            with_nodes = function(_node_ids: any)
                                return {
                                    with_data_types = function(_data_type: any)
                                        return {
                                            fetch_options = function(_options: any)
                                                return {
                                                    all = function()
                                                        return {
                                                            {
                                                                type = consts.DATA_TYPE.NODE_RESULT,
                                                                discriminator = "result.error",
                                                                content = {
                                                                    success = false,
                                                                    message = "Template item failed",
                                                                    error = child_failure
                                                                },
                                                                node_id = "actual-node-1"
                                                            }
                                                        }, nil
                                                    end
                                                }
                                            end
                                        }
                                    end
                                }
                            end
                        }, nil
                    end
                }

                local result, err = iterator.collect_results(parent_node, iteration_info, {
                    data_reader = mock_data_reader
                })

                test.is_nil(result)
                test.is_table(err)
                test.eq(err.code, child_failure.code)
                test.eq(err.message, child_failure.message)
            end)

            it("should parse JSON content correctly", function()
                local parent_node = {
                    dataflow_id = "test-df"
                }

                local iteration_info = {
                    uuid_mapping = { ["template1"] = "actual-node-1" }
                }

                local mock_data_reader = {
                    with_dataflow = function(_dataflow_id: any)
                        return {
                            with_nodes = function(_node_ids: any)
                                return {
                                    with_data_types = function(_data_type: any)
                                        return {
                                            fetch_options = function(_options: any)
                                                return {
                                                    all = function()
                                                        return {
                                                            {
                                                                key = "result",
                                                                content = '{"message":"parsed","success":true}',
                                                                content_type = "application/json",
                                                                node_id = "actual-node-1",
                                                                discriminator = "success"
                                                            }
                                                        }, nil
                                                    end
                                                }
                                            end
                                        }
                                    end
                                }
                            end
                        }, nil
                    end
                }

                local deps = { data_reader = mock_data_reader }

                local result, err = iterator.collect_results(parent_node, iteration_info, deps)

                test.is_nil(err)
                test.not_nil(result)
                test.is_table(result)
                local res = result :: any
                test.eq(res.message, "parsed")
                test.is_true(res.success)
            end)
        end)

        describe("Metadata Preservation Tests", function()
            it("should preserve all original metadata fields", function()
                local parent_node = {
                    dataflow_id = "test-df",
                    node_id = "parent",
                    path = {},
                    command = function(self: any, cmd: any)
                        self._commands = self._commands or {}
                        table.insert(self._commands, cmd)
                    end,
                    data = function(_self: any, _data_type: string, _content: any, _options: any?) end
                }

                local template_graph = {
                    nodes = {
                        ["template1"] = {
                            node_id = "template1",
                            type = "func_node",
                            config = {},
                            metadata = {
                                title = "Complex Node",
                                description = "A node with lots of metadata",
                                version = "1.2.3",
                                author = "test_user",
                                tags = { "processing", "data" },
                                settings = {
                                    timeout = 30,
                                    retries = 3
                                },
                                custom_field = true
                            }
                        }
                    },
                    get_roots = function(_self: any)
                        return { "template1" }
                    end
                }

                local _iteration_info = iterator.create_iteration(
                    parent_node, template_graph, { test = "data" }, 5, "default"
                )

                local created_node = (parent_node :: any)._commands[1].payload
                local metadata = created_node.metadata

                test.eq(metadata.title, "Complex Node")
                test.eq(metadata.description, "A node with lots of metadata")
                test.eq(metadata.version, "1.2.3")
                test.eq(metadata.author, "test_user")
                test.eq(#metadata.tags, 2)
                test.eq(metadata.tags[1], "processing")
                test.eq(metadata.tags[2], "data")
                test.eq(metadata.settings.timeout, 30)
                test.eq(metadata.settings.retries, 3)
                test.is_true(metadata.custom_field)

                test.eq(metadata.iteration, 5)
                test.eq(metadata.template_source, "template1")
            end)

            it("should handle complex title scenarios", function()
                local parent_node = {
                    dataflow_id = "test-df",
                    node_id = "parent",
                    path = {},
                    command = function(self: any, cmd: any)
                        self._commands = self._commands or {}
                        table.insert(self._commands, cmd)
                    end,
                    data = function(_self: any, _data_type: string, _content: any, _options: any?) end
                }

                local test_cases = {
                    {
                        original = "Simple Title",
                        iteration = 1,
                        expected = "Simple Title"
                    },
                    {
                        original = "Title with (parentheses)",
                        iteration = 42,
                        expected = "Title with (parentheses)"
                    },
                    {
                        original = "Title with #hash",
                        iteration = 7,
                        expected = "Title with #hash"
                    },
                    {
                        original = "",
                        iteration = 3,
                        expected = ""
                    }
                }

                for _i, test_case in ipairs(test_cases) do
                    (parent_node :: any)._commands = {}

                    local template_graph = {
                        nodes = {
                            ["template1"] = {
                                node_id = "template1",
                                type = "func_node",
                                config = {},
                                metadata = {
                                    title = test_case.original
                                }
                            }
                        },
                        get_roots = function(_self: any)
                            return { "template1" }
                        end
                    }

                    iterator.create_iteration(
                        parent_node, template_graph, { test = "data" }, test_case.iteration, "default"
                    )

                    local created_node = (parent_node :: any)._commands[1].payload
                    test.eq(created_node.metadata.title, test_case.expected)
                end
            end)
        end)
    end)
end

return test.run_cases(define_tests)
