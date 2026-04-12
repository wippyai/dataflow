local test = require("test")
local uuid = require("uuid")
local _json = require("json")
local _time = require("time")

local node = require("node")
local consts = require("consts")

local function define_tests()
    describe("Node SDK with DI", function()
        local mock_deps: any
        local captured_calls: any

        before_each(function()
            captured_calls = {
                commit_submit = {},
                process_send = {},
                process_listen = {},
                data_reader_calls = {}
            }

            mock_deps = {
                commit = {
                    submit = function(dataflow_id: string, op_id: string, commands: any)
                        table.insert(captured_calls.commit_submit, {
                            dataflow_id = dataflow_id,
                            op_id = op_id,
                            commands = commands
                        })
                        return { commit_id = uuid.v7() }, nil
                    end
                },
                data_reader = {
                    with_dataflow = function(dataflow_id: string)
                        table.insert(captured_calls.data_reader_calls, { method = "with_dataflow", dataflow_id = dataflow_id })
                        return {
                            with_nodes = function(node_id: string)
                                table.insert(captured_calls.data_reader_calls, { method = "with_nodes", node_id = node_id })
                                return {
                                    with_data_types = function(data_type: string)
                                        table.insert(captured_calls.data_reader_calls, { method = "with_data_types", data_type = data_type })
                                        return {
                                            fetch_options = function(options: any)
                                                table.insert(captured_calls.data_reader_calls, { method = "fetch_options", options = options })
                                                return {
                                                    all = function()
                                                        table.insert(captured_calls.data_reader_calls, { method = "all" })
                                                        return {
                                                            {
                                                                content = '{"message": "hello"}',
                                                                content_type = consts.CONTENT_TYPE.JSON,
                                                                key = "input1",
                                                                metadata = { source = "test" },
                                                                discriminator = "primary"
                                                            },
                                                            {
                                                                content = "plain text",
                                                                content_type = consts.CONTENT_TYPE.TEXT,
                                                                key = "input2",
                                                                metadata = {},
                                                                discriminator = nil
                                                            }
                                                        }
                                                    end
                                                }
                                            end
                                        }
                                    end
                                }
                            end
                        }
                    end
                },
                process = {
                    send = function(target: string, topic: string, payload: any)
                        table.insert(captured_calls.process_send, {
                            target = target,
                            topic = topic,
                            payload = payload
                        })
                        return true
                    end,
                    listen = function(topic: string)
                        table.insert(captured_calls.process_listen, { topic = topic })
                        return {
                            receive = function()
                                return {
                                    response_data = {
                                        run_node_results = {
                                            ["child-1"] = { status = "completed", output = "result1" }
                                        }
                                    }
                                }, true
                            end
                        }
                    end
                }
            }
        end)

        describe("Constructor", function()
            it("should create a node instance with required args", function()
                local args = {
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456"
                }

                local instance, err = node.new(args, mock_deps)

                test.is_nil(err)
                test.not_nil(instance)
                test.eq(instance.node_id, "test-node-123")
                test.eq(instance.dataflow_id, "test-dataflow-456")
                test.eq(instance._deps, mock_deps)
            end)

            it("should fail with missing required args", function()
                local instance, err = node.new(nil, mock_deps)
                test.is_nil(instance)
                test.contains(err, "Node args required")

                instance, err = node.new({}, mock_deps)
                test.is_nil(instance)
                test.contains(err, "node_id and dataflow_id")
            end)

            it("should handle node configuration properly from config", function()
                local args = {
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456",
                    node = {
                        config = {
                            data_targets = { { data_type = "output", key = "result" } },
                            error_targets = { { data_type = "error", key = "failure" } },
                            timeout = 30,
                            retries = 3
                        },
                        metadata = { custom = "value" }
                    }
                }

                local instance, err = node.new(args, mock_deps)

                test.is_nil(err)
                test.not_nil(instance)
                test.eq(#instance.data_targets, 1)
                test.eq(#instance.error_targets, 1)
                test.eq(instance._metadata.custom, "value")
                test.eq(instance._config.timeout, 30)
                test.eq(instance._config.retries, 3)
            end)

            it("should handle empty or missing config gracefully", function()
                local args = {
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456",
                    node = {
                        metadata = { custom = "value" }
                    }
                }

                local instance, err = node.new(args, mock_deps)

                test.is_nil(err)
                test.not_nil(instance)
                test.eq(#instance.data_targets, 0)
                test.eq(#instance.error_targets, 0)
                test.eq(type(instance._config), "table")
                test.is_nil(next(instance._config))
            end)
        end)

        describe("Config Accessor", function()
            it("should provide access to node config", function()
                local args = {
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456",
                    node = {
                        config = {
                            timeout = 30,
                            retries = 3,
                            api_endpoint = "https://api.example.com",
                            features = { "logging", "metrics" }
                        }
                    }
                }

                local instance, err = node.new(args, mock_deps)
                test.is_nil(err)
                test.not_nil(instance)

                local config = instance:config()
                test.not_nil(config)
                test.eq(config.timeout, 30)
                test.eq(config.retries, 3)
                test.eq(config.api_endpoint, "https://api.example.com")
                test.eq(type(config.features), "table")
                test.eq(#config.features, 2)
                test.eq(config.features[1], "logging")
                test.eq(config.features[2], "metrics")
            end)

            it("should return empty config when none provided", function()
                local args = {
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456"
                }

                local instance, err = node.new(args, mock_deps)
                test.is_nil(err)
                test.not_nil(instance)

                local config = instance:config()
                test.not_nil(config)
                test.eq(type(config), "table")
                test.is_nil(next(config))
            end)
        end)

        describe("Input Methods", function()
            local test_node: any

            local function make_input_query_fail_deps()
                return {
                    commit = mock_deps.commit,
                    process = mock_deps.process,
                    data_reader = {
                        with_dataflow = function(_dataflow_id: string)
                            return {
                                with_nodes = function(_node_id: string)
                                    return {
                                        with_data_types = function(_data_type: string)
                                            return {
                                                fetch_options = function(_options: any)
                                                    return {
                                                        all = function()
                                                            error("mock inputs query failed")
                                                        end
                                                    }
                                                end
                                            }
                                        end
                                    }
                                end
                            }
                        end
                    }
                }
            end

            before_each(function()
                local _err: string?
                test_node, _err = node.new({
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456"
                }, mock_deps)
            end)

            it("should get all inputs as a map and cache them", function()
                test.not_nil(test_node)
                local inputs = test_node:inputs()

                test.not_nil(inputs)
                test.not_nil(inputs.primary)
                test.eq(inputs.primary.content.message, "hello")
                test.not_nil(inputs.input2)
                test.eq(inputs.input2.content, "plain text")

                test.gt(#captured_calls.data_reader_calls, 0)

                local call_count: number = #captured_calls.data_reader_calls
                local inputs2 = test_node:inputs()
                test.eq(inputs2, inputs)
                test.eq(#captured_calls.data_reader_calls, call_count)
            end)

            it("should get specific input by key", function()
                test.not_nil(test_node)
                local input = test_node:input("primary")

                test.not_nil(input)
                test.eq(input.content.message, "hello")
                test.eq(input.key, "input1")
                test.eq(input.discriminator, "primary")
            end)

            it("should fail when input key is missing", function()
                test.not_nil(test_node)
                local input, err = test_node:input(nil)

                test.is_nil(input)
                test.contains(err, "Input key is required")
            end)

            it("BC_REGRESSION_C2_node_inputs_returns_error_pair", function()
                local failing_node, err = node.new({
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456"
                }, make_input_query_fail_deps())
                test.is_nil(err)
                test.not_nil(failing_node)

                local inputs, inputs_err = failing_node:inputs()

                test.is_nil(inputs)
                test.not_nil(inputs_err)
                test.contains(inputs_err, "mock inputs query failed")
            end)

            it("BC_REGRESSION_C2_node_input_returns_error_pair", function()
                local failing_node, err = node.new({
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456"
                }, make_input_query_fail_deps())
                test.is_nil(err)
                test.not_nil(failing_node)

                local input, input_err = failing_node:input("primary")

                test.is_nil(input)
                test.not_nil(input_err)
                test.contains(input_err, "mock inputs query failed")
            end)
        end)

        describe("Expr Input Transformation", function()
            local expr_mock_deps: any

            before_each(function()
                expr_mock_deps = {
                    commit = mock_deps.commit,
                    process = mock_deps.process,
                    data_reader = {
                        with_dataflow = function(dataflow_id: string)
                            table.insert(captured_calls.data_reader_calls, { method = "with_dataflow", dataflow_id = dataflow_id })
                            return {
                                with_nodes = function(node_id: string)
                                    table.insert(captured_calls.data_reader_calls, { method = "with_nodes", node_id = node_id })
                                    return {
                                        with_data_types = function(data_type: string)
                                            table.insert(captured_calls.data_reader_calls, { method = "with_data_types", data_type = data_type })
                                            return {
                                                fetch_options = function(options: any)
                                                    table.insert(captured_calls.data_reader_calls, { method = "fetch_options", options = options })
                                                    return {
                                                        all = function()
                                                            table.insert(captured_calls.data_reader_calls, { method = "all" })
                                                            return {
                                                                {
                                                                    content = '{"name": "John", "age": 30, "score": 85}',
                                                                    content_type = consts.CONTENT_TYPE.JSON,
                                                                    key = "user_data",
                                                                    metadata = { source = "api" },
                                                                    discriminator = nil
                                                                },
                                                                {
                                                                    content = '{"price": 100, "quantity": 3}',
                                                                    content_type = consts.CONTENT_TYPE.JSON,
                                                                    key = "order_data",
                                                                    metadata = {},
                                                                    discriminator = nil
                                                                },
                                                                {
                                                                    content = "Hello World",
                                                                    content_type = consts.CONTENT_TYPE.TEXT,
                                                                    key = "message",
                                                                    metadata = {},
                                                                    discriminator = nil
                                                                }
                                                            }
                                                        end
                                                    }
                                                end
                                            }
                                        end
                                    }
                                end
                            }
                        end
                    }
                }
            end)

            it("should transform inputs with simple string expression", function()
                local args = {
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456",
                    node = {
                        config = {
                            input_transform = "input.user_data.content.name + ' is ' + string(input.user_data.content.age) + ' years old'"
                        }
                    }
                }

                local test_node, err = node.new(args, expr_mock_deps)
                test.is_nil(err)
                test.not_nil(test_node)

                local inputs = test_node:inputs()
                test.not_nil(inputs)
                test.not_nil(inputs["default"])
                test.eq(inputs["default"].content, "John is 30 years old")
                test.eq(inputs["default"].key, "default")
            end)

            it("should transform inputs with field mapping", function()
                local args = {
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456",
                    node = {
                        config = {
                            input_transform = {
                                user_name = "input.user_data.content.name",
                                user_age = "input.user_data.content.age",
                                total_cost = "input.order_data.content.price * input.order_data.content.quantity",
                                is_adult = "input.user_data.content.age >= 18",
                                greeting = "input.message.content + ', ' + input.user_data.content.name"
                            }
                        }
                    }
                }

                local test_node, err = node.new(args, expr_mock_deps)
                test.is_nil(err)
                test.not_nil(test_node)

                local inputs = test_node:inputs()
                test.not_nil(inputs)
                test.eq(inputs.user_name.content, "John")
                test.eq(inputs.user_age.content, 30)
                test.eq(inputs.total_cost.content, 300)
                test.is_true(inputs.is_adult.content)
                test.eq(inputs.greeting.content, "Hello World, John")
            end)

            it("should handle array operations in expressions", function()
                local complex_data_mock: any = {
                    commit = mock_deps.commit,
                    process = mock_deps.process,
                    data_reader = {
                        with_dataflow = function(_dataflow_id: string)
                            return {
                                with_nodes = function(_node_id: string)
                                    return {
                                        with_data_types = function(_data_type: string)
                                            return {
                                                fetch_options = function(_options: any)
                                                    return {
                                                        all = function()
                                                            return {
                                                                {
                                                                    content = '{"items": [{"price": 10}, {"price": 20}, {"price": 30}]}',
                                                                    content_type = consts.CONTENT_TYPE.JSON,
                                                                    key = "inventory",
                                                                    metadata = {},
                                                                    discriminator = nil
                                                                }
                                                            }
                                                        end
                                                    }
                                                end
                                            }
                                        end
                                    }
                                end
                            }
                        end
                    }
                }

                local args = {
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456",
                    node = {
                        config = {
                            input_transform = {
                                item_count = "len(input.inventory.content.items)",
                                has_expensive_items = "any(input.inventory.content.items, {.price > 25})",
                                cheap_items = "filter(input.inventory.content.items, {.price <= 15})"
                            }
                        }
                    }
                }

                local test_node, err = node.new(args, complex_data_mock)
                test.is_nil(err)
                test.not_nil(test_node)

                local inputs = test_node:inputs()
                test.not_nil(inputs)
                test.eq(inputs.item_count.content, 3)
                test.is_true(inputs.has_expensive_items.content)
                test.eq(type(inputs.cheap_items.content), "table")
                test.eq(#inputs.cheap_items.content, 1)
            end)

            it("should handle mathematical expressions", function()
                local args = {
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456",
                    node = {
                        config = {
                            input_transform = {
                                calculated_score = "input.user_data.content.score * 1.2",
                                rounded_score = "round(input.user_data.content.score * 1.15)",
                                score_grade = "input.user_data.content.score >= 90 ? 'A' : input.user_data.content.score >= 80 ? 'B' : 'C'",
                                power_calc = "input.user_data.content.age ** 2",
                                abs_diff = "abs(input.user_data.content.score - 90)"
                            }
                        }
                    }
                }

                local test_node, err = node.new(args, expr_mock_deps)
                test.is_nil(err)
                test.not_nil(test_node)

                local inputs = test_node:inputs()
                test.not_nil(inputs)
                test.eq(inputs.calculated_score.content, 102)
                test.eq(inputs.rounded_score.content, 98)
                test.eq(inputs.score_grade.content, "B")
                test.eq(inputs.power_calc.content, 900)
                test.eq(inputs.abs_diff.content, 5)
            end)

            it("should handle string operations", function()
                local args = {
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456",
                    node = {
                        config = {
                            input_transform = {
                                upper_name = "upper(input.user_data.content.name)",
                                name_length = "len(input.user_data.content.name)",
                                contains_john = "input.user_data.content.name contains 'John'",
                                starts_with_j = "input.user_data.content.name startsWith 'J'",
                                trimmed_message = "trim('  ' + input.message.content + '  ')"
                            }
                        }
                    }
                }

                local test_node, err = node.new(args, expr_mock_deps)
                test.is_nil(err)
                test.not_nil(test_node)

                local inputs = test_node:inputs()
                test.not_nil(inputs)
                test.eq(inputs.upper_name.content, "JOHN")
                test.eq(inputs.name_length.content, 4)
                test.is_true(inputs.contains_john.content)
                test.is_true(inputs.starts_with_j.content)
                test.eq(inputs.trimmed_message.content, "Hello World")
            end)

            it("should preserve metadata structure in transformed inputs", function()
                local args = {
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456",
                    node = {
                        config = {
                            input_transform = {
                                processed_name = "upper(input.user_data.content.name)"
                            }
                        }
                    }
                }

                local test_node, err = node.new(args, expr_mock_deps)
                test.is_nil(err)
                test.not_nil(test_node)

                local inputs = test_node:inputs()
                test.not_nil(inputs)
                test.not_nil(inputs.processed_name)
                test.eq(inputs.processed_name.content, "JOHN")
                test.eq(inputs.processed_name.key, "processed_name")
                test.eq(type(inputs.processed_name.metadata), "table")
                test.is_nil(inputs.processed_name.discriminator)
            end)

            it("should return original inputs when no transform config", function()
                local args = {
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456"
                }

                local test_node, err = node.new(args, expr_mock_deps)
                test.is_nil(err)
                test.not_nil(test_node)

                local inputs = test_node:inputs()
                test.not_nil(inputs)
                test.not_nil(inputs.user_data)
                test.eq(inputs.user_data.content.name, "John")
                test.not_nil(inputs.order_data)
                test.not_nil(inputs.message)
            end)

            it("should cache transformed inputs", function()
                local args = {
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456",
                    node = {
                        config = {
                            input_transform = "input.user_data.content.name"
                        }
                    }
                }

                local test_node, err = node.new(args, expr_mock_deps)
                test.is_nil(err)
                test.not_nil(test_node)

                local inputs1 = test_node:inputs()
                local call_count: number = #captured_calls.data_reader_calls

                local inputs2 = test_node:inputs()
                test.eq(inputs2, inputs1)
                test.eq(#captured_calls.data_reader_calls, call_count)
            end)
        end)

        describe("Expr Error Handling", function()
            it("should handle invalid expressions gracefully", function()
                local args = {
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456",
                    node = {
                        config = {
                            input_transform = "invalid + syntax +"
                        }
                    }
                }

                local test_node, err = node.new(args, mock_deps)
                test.is_nil(err)
                test.not_nil(test_node)

                local inputs, error_msg = test_node:inputs()

                test.is_nil(inputs)
                test.contains(error_msg, "Input transformation failed")
            end)

            it("should handle undefined variables in expressions", function()
                local args = {
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456",
                    node = {
                        config = {
                            input_transform = "undefined_variable.property"
                        }
                    }
                }

                local test_node, err = node.new(args, mock_deps)
                test.is_nil(err)
                test.not_nil(test_node)

                local inputs, error_msg = test_node:inputs()

                test.is_nil(inputs)
                test.contains(error_msg, "Input transformation failed")
            end)

            it("should handle field mapping errors individually", function()
                local args = {
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456",
                    node = {
                        config = {
                            input_transform = {
                                invalid_field = "invalid + syntax +"
                            }
                        }
                    }
                }

                local test_node, err = node.new(args, mock_deps)
                test.is_nil(err)
                test.not_nil(test_node)

                local inputs, error_msg = test_node:inputs()

                test.is_nil(inputs)
                test.contains(error_msg, "Transform failed for invalid_field")
            end)

            it("should handle type conversion errors", function()
                local args = {
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456",
                    node = {
                        config = {
                            input_transform = "int('not_a_number')"
                        }
                    }
                }

                local test_node, err = node.new(args, mock_deps)
                test.is_nil(err)
                test.not_nil(test_node)

                local inputs, error_msg = test_node:inputs()

                test.is_nil(inputs)
                test.contains(error_msg, "Input transformation failed")
            end)

            it("should validate transform config types", function()
                local args = {
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456",
                    node = {
                        config = {
                            input_transform = 123
                        }
                    }
                }

                local test_node, err = node.new(args, mock_deps)
                test.is_nil(err)
                test.not_nil(test_node)

                local inputs, error_msg = test_node:inputs()

                test.is_nil(inputs)
                test.contains(error_msg, "input_transform must be string or table")
            end)
        end)

        describe("Complex Expr Scenarios", function()
            it("should handle nested object access", function()
                local complex_mock: any = {
                    commit = mock_deps.commit,
                    process = mock_deps.process,
                    data_reader = {
                        with_dataflow = function(_dataflow_id: string)
                            return {
                                with_nodes = function(_node_id: string)
                                    return {
                                        with_data_types = function(_data_type: string)
                                            return {
                                                fetch_options = function(_options: any)
                                                    return {
                                                        all = function()
                                                            return {
                                                                {
                                                                    content = '{"user": {"profile": {"settings": {"theme": "dark"}}}}',
                                                                    content_type = consts.CONTENT_TYPE.JSON,
                                                                    key = "nested_data",
                                                                    metadata = {},
                                                                    discriminator = nil
                                                                }
                                                            }
                                                        end
                                                    }
                                                end
                                            }
                                        end
                                    }
                                end
                            }
                        end
                    }
                }

                local args = {
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456",
                    node = {
                        config = {
                            input_transform = {
                                theme = "input.nested_data.content.user.profile.settings.theme",
                                has_dark_theme = "input.nested_data.content.user.profile.settings.theme == 'dark'"
                            }
                        }
                    }
                }

                local test_node, err = node.new(args, complex_mock)
                test.is_nil(err)
                test.not_nil(test_node)

                local inputs = test_node:inputs()
                test.not_nil(inputs)
                test.eq(inputs.theme.content, "dark")
                test.is_true(inputs.has_dark_theme.content)
            end)

            it("should handle basic conditional operations", function()
                local args = {
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456",
                    node = {
                        config = {
                            input_transform = {
                                current_time = "now()",
                                age_category = "input.user_data.content.age >= 65 ? 'senior' : input.user_data.content.age >= 18 ? 'adult' : 'minor'"
                            }
                        }
                    }
                }

                local expr_mock_with_user_data: any = {
                    commit = mock_deps.commit,
                    process = mock_deps.process,
                    data_reader = {
                        with_dataflow = function(_dataflow_id: string)
                            return {
                                with_nodes = function(_node_id: string)
                                    return {
                                        with_data_types = function(_data_type: string)
                                            return {
                                                fetch_options = function(_options: any)
                                                    return {
                                                        all = function()
                                                            return {
                                                                {
                                                                    content = '{"name": "John", "age": 30, "score": 85}',
                                                                    content_type = consts.CONTENT_TYPE.JSON,
                                                                    key = "user_data",
                                                                    metadata = { source = "api" },
                                                                    discriminator = nil
                                                                }
                                                            }
                                                        end
                                                    }
                                                end
                                            }
                                        end
                                    }
                                end
                            }
                        end
                    }
                }

                local test_node, err = node.new(args, expr_mock_with_user_data)
                test.is_nil(err)
                test.not_nil(test_node)

                local inputs = test_node:inputs()
                test.not_nil(inputs)
                test.eq(inputs.age_category.content, "adult")
                test.eq(type(inputs.current_time.content), "number")
            end)

            it("should handle regex matching", function()
                local args = {
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456",
                    node = {
                        config = {
                            input_transform = {
                                is_valid_name = "input.user_data.content.name matches '^[A-Za-z]+$'",
                                contains_digits = "input.message.content matches '\\\\d+'"
                            }
                        }
                    }
                }

                local expr_mock_with_both: any = {
                    commit = mock_deps.commit,
                    process = mock_deps.process,
                    data_reader = {
                        with_dataflow = function(_dataflow_id: string)
                            return {
                                with_nodes = function(_node_id: string)
                                    return {
                                        with_data_types = function(_data_type: string)
                                            return {
                                                fetch_options = function(_options: any)
                                                    return {
                                                        all = function()
                                                            return {
                                                                {
                                                                    content = '{"name": "John", "age": 30, "score": 85}',
                                                                    content_type = consts.CONTENT_TYPE.JSON,
                                                                    key = "user_data",
                                                                    metadata = { source = "api" },
                                                                    discriminator = nil
                                                                },
                                                                {
                                                                    content = "Hello World",
                                                                    content_type = consts.CONTENT_TYPE.TEXT,
                                                                    key = "message",
                                                                    metadata = {},
                                                                    discriminator = nil
                                                                }
                                                            }
                                                        end
                                                    }
                                                end
                                            }
                                        end
                                    }
                                end
                            }
                        end
                    }
                }

                local test_node, err = node.new(args, expr_mock_with_both)
                test.is_nil(err)
                test.not_nil(test_node)

                local inputs = test_node:inputs()
                test.not_nil(inputs)
                test.is_true(inputs.is_valid_name.content)
                test.is_false(inputs.contains_digits.content)
            end)
        end)

        describe("Data and Metadata Methods", function()
            local test_node: any

            before_each(function()
                local _err: string?
                test_node, _err = node.new({
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456"
                }, mock_deps)
            end)

            it("should create data with proper command queuing", function()
                test.not_nil(test_node)
                local result = test_node:data(consts.DATA_TYPE.NODE_OUTPUT, { message = "test" })

                test.eq(result, test_node)
                test.eq(#test_node._queued_commands, 1)
                test.eq(test_node._queued_commands[1].type, consts.COMMAND_TYPES.CREATE_DATA)
                test.eq(test_node._queued_commands[1].payload.data_type, consts.DATA_TYPE.NODE_OUTPUT)
            end)

            it("should update metadata properly", function()
                test.not_nil(test_node)
                local result = test_node:update_metadata({ key1 = "value1", key2 = "value2" })

                test.eq(result, test_node)
                test.eq(test_node._metadata.key1, "value1")
                test.eq(test_node._metadata.key2, "value2")
                test.eq(#test_node._queued_commands, 1)
                test.eq(test_node._queued_commands[1].type, consts.COMMAND_TYPES.UPDATE_NODE)
            end)

            it("should merge metadata without overwriting existing values", function()
                test.not_nil(test_node)
                test_node._metadata = { existing = "value", shared = "original" }

                test_node:update_metadata({ shared = "updated", new_key = "new_value" })

                test.eq(test_node._metadata.existing, "value")
                test.eq(test_node._metadata.shared, "updated")
                test.eq((test_node._metadata :: any).new_key, "new_value")
            end)

            it("should handle nil and empty metadata updates gracefully", function()
                test.not_nil(test_node)
                test_node:update_metadata(nil)
                test.eq(#test_node._queued_commands, 0)

                test_node:update_metadata({})
                test.eq(#test_node._queued_commands, 1)
            end)

            it("should determine content type automatically", function()
                test.not_nil(test_node)
                test_node:data(consts.DATA_TYPE.NODE_OUTPUT, { message = "test" })
                test_node:data(consts.DATA_TYPE.NODE_OUTPUT, "plain text")

                test.eq(test_node._queued_commands[1].payload.content_type, consts.CONTENT_TYPE.JSON)
                test.eq(test_node._queued_commands[2].payload.content_type, consts.CONTENT_TYPE.TEXT)
            end)
        end)

        describe("Child Node Creation", function()
            local test_node: any

            before_each(function()
                local _err: string?
                test_node, _err = node.new({
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456"
                }, mock_deps)
            end)

            it("should create child nodes with auto-generated IDs", function()
                test.not_nil(test_node)
                local definitions = {
                    { node_type = "child_type_1" },
                    { node_type = "child_type_2" }
                }

                local child_ids, err = test_node:with_child_nodes(definitions)

                test.is_nil(err)
                test.not_nil(child_ids)
                test.eq(#child_ids, 2)
                test.eq(#test_node._queued_commands, 2)

                for i, cmd in ipairs(test_node._queued_commands) do
                    test.eq(cmd.type, consts.COMMAND_TYPES.CREATE_NODE)
                    test.eq(cmd.payload.node_type, definitions[i].node_type)
                    test.eq(cmd.payload.parent_node_id, "test-node-123")
                end
            end)

            it("should create child nodes with provided IDs and config", function()
                test.not_nil(test_node)
                local definitions = {
                    {
                        node_id = "child-1",
                        node_type = "child_type_1",
                        config = { timeout = 60, retries = 5 }
                    },
                    {
                        node_id = "child-2",
                        node_type = "child_type_2",
                        config = { parallel = true }
                    }
                }

                local child_ids, err = test_node:with_child_nodes(definitions)

                test.is_nil(err)
                test.not_nil(child_ids)
                test.eq(child_ids[1], "child-1")
                test.eq(child_ids[2], "child-2")

                test.not_nil(test_node._queued_commands[1].payload.config)
                test.eq(test_node._queued_commands[1].payload.config.timeout, 60)
                test.is_true(test_node._queued_commands[2].payload.config.parallel)
            end)

            it("should fail with invalid child definitions", function()
                test.not_nil(test_node)
                local child_ids, err = test_node:with_child_nodes(nil)
                test.is_nil(child_ids)
                test.contains(err, "Child definitions required")

                child_ids, err = test_node:with_child_nodes({ { node_type = nil } })
                test.is_nil(child_ids)
                test.contains(err, "node_type")
            end)
        end)

        describe("Submit Functionality", function()
            local test_node: any

            before_each(function()
                local _err: string?
                test_node, _err = node.new({
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456"
                }, mock_deps)
            end)

            it("should submit queued commands without yielding", function()
                test.not_nil(test_node)
                test_node:data(consts.DATA_TYPE.NODE_OUTPUT, { message = "test1" })
                test_node:update_metadata({ status = "processing" })
                test_node:data(consts.DATA_TYPE.NODE_OUTPUT, { message = "test2" })

                test.eq(#test_node._queued_commands, 3)

                local success, err = test_node:submit()

                test.is_true(success)
                test.is_nil(err)
                test.eq(#test_node._queued_commands, 0)

                test.eq(#captured_calls.commit_submit, 1)
                test.eq(#captured_calls.commit_submit[1].commands, 3)

                test.eq(#captured_calls.process_send, 0)
            end)

            it("should handle empty queue gracefully", function()
                test.not_nil(test_node)
                test.eq(#test_node._queued_commands, 0)

                local success, err = test_node:submit()

                test.is_true(success)
                test.is_nil(err)

                test.eq(#captured_calls.commit_submit, 0)
            end)

            it("should handle commit failures gracefully", function()
                local failing_deps: any = {
                    commit = {
                        submit = function()
                            return nil, "Database connection failed"
                        end
                    },
                    process = mock_deps.process,
                    data_reader = mock_deps.data_reader
                }

                local failing_node, _err = node.new({
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456"
                }, failing_deps)
                test.not_nil(failing_node)

                failing_node:data(consts.DATA_TYPE.NODE_OUTPUT, { message = "test" })
                test.eq(#failing_node._queued_commands, 1)

                local success, err = failing_node:submit()

                test.is_false(success)
                test.eq(err, "Database connection failed")
                test.eq(#failing_node._queued_commands, 1)
            end)

            it("should be chainable after submit success", function()
                test.not_nil(test_node)
                test_node:data(consts.DATA_TYPE.NODE_OUTPUT, { message = "test1" })
                local success, err = test_node:submit()
                test.is_true(success)
                test.eq(#test_node._queued_commands, 0)

                test_node:data(consts.DATA_TYPE.NODE_OUTPUT, { message = "test2" })
                test_node:update_metadata({ status = "updated" })
                test.eq(#test_node._queued_commands, 2)

                success, err = test_node:submit()
                test.is_true(success)
                test.eq(#test_node._queued_commands, 0)

                test.eq(#captured_calls.commit_submit, 2)
            end)

            it("should preserve command order when submitting", function()
                test.not_nil(test_node)
                test_node:data("type1", "content1")
                test_node:update_metadata({ key1 = "value1" })
                test_node:data("type2", "content2")

                local success, _err = test_node:submit()
                test.is_true(success)

                local submit_call = captured_calls.commit_submit[1]
                test.eq(#submit_call.commands, 3)
                test.eq(submit_call.commands[1].payload.data_type, "type1")
                test.eq(submit_call.commands[2].type, consts.COMMAND_TYPES.UPDATE_NODE)
                test.eq(submit_call.commands[3].payload.data_type, "type2")
            end)

            it("should maintain dataflow_id and generate op_id correctly", function()
                test.not_nil(test_node)
                test_node:data(consts.DATA_TYPE.NODE_OUTPUT, { message = "test" })

                local success, _err = test_node:submit()
                test.is_true(success)

                local submit_call = captured_calls.commit_submit[1]
                test.eq(submit_call.dataflow_id, "test-dataflow-456")
                test.not_nil(submit_call.op_id)
                test.eq(type(submit_call.op_id), "string")
                test.gt(string.len(submit_call.op_id), 0)
            end)
        end)

        describe("Yield Functionality", function()
            local test_node: any

            before_each(function()
                local _err: string?
                test_node, _err = node.new({
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456"
                }, mock_deps)
            end)

            it("should handle empty yield", function()
                test.not_nil(test_node)
                local result, err = test_node:yield()

                test.is_nil(err)
                test.not_nil(result)
                test.eq(type(result), "table")

                test.eq(#captured_calls.commit_submit, 1)

                test.eq(#captured_calls.process_send, 1)
                test.eq(captured_calls.process_send[1].topic, consts.MESSAGE_TOPIC.YIELD_REQUEST)
            end)

            it("should yield and wait for children", function()
                test.not_nil(test_node)
                local result, err = test_node:yield({ run_nodes = { "child-1" } })

                test.is_nil(err)
                test.not_nil(result)
                test.not_nil(result["child-1"])
                test.eq(result["child-1"].status, "completed")

                test.eq(#captured_calls.commit_submit, 1)
                test.eq(#captured_calls.process_send, 1)
            end)

            it("should create yield persistence record", function()
                test.not_nil(test_node)
                test_node:yield()

                test.eq(#captured_calls.commit_submit, 1)
                local submit_call = captured_calls.commit_submit[1]
                test.not_nil(submit_call.commands)
                test.eq(#submit_call.commands, 1)
                test.eq(submit_call.commands[1].type, consts.COMMAND_TYPES.CREATE_DATA)
                test.eq(submit_call.commands[1].payload.data_type, consts.DATA_TYPE.NODE_YIELD)
            end)

            it("should differ from submit in that it sends process signals", function()
                test.not_nil(test_node)
                test_node:data(consts.DATA_TYPE.NODE_OUTPUT, { message = "test" })

                local success, _err = test_node:submit()
                test.is_true(success)
                test.eq(#captured_calls.process_send, 0)

                captured_calls.process_send = {}
                captured_calls.commit_submit = {}

                test_node:data(consts.DATA_TYPE.NODE_OUTPUT, { message = "test2" })
                local result, yield_err = test_node:yield()

                test.is_nil(yield_err)
                test.eq(#captured_calls.process_send, 1)
                test.eq(captured_calls.process_send[1].topic, consts.MESSAGE_TOPIC.YIELD_REQUEST)
            end)
        end)

        describe("Output and Error Routing", function()
            local test_node: any

            before_each(function()
                local _err: string?
                test_node, _err = node.new({
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456",
                    node = {
                        config = {
                            data_targets = {
                                { data_type = "output.result", key = "main" },
                                { data_type = "output.summary", key = "summary" }
                            },
                            error_targets = {
                                { data_type = "error.details", key = "error" },
                                { data_type = "error.summary", key = "summary" }
                            }
                        }
                    }
                }, mock_deps)
            end)

            it("should route outputs via data_targets from config on complete", function()
                test.not_nil(test_node)
                local result = test_node:complete({ message = "success" })

                test.is_true(result.success)

                test.eq(#captured_calls.commit_submit, 1)

                local first_submit = captured_calls.commit_submit[1]
                local data_commands: number = 0
                for _, cmd in ipairs(first_submit.commands) do
                    if cmd.type == consts.COMMAND_TYPES.CREATE_DATA then
                        data_commands = data_commands + 1
                    end
                end
                test.eq(data_commands, 2)
            end)

            it("should route errors via error_targets from config on fail", function()
                test.not_nil(test_node)
                local result = test_node:fail("Something went wrong")

                test.is_false(result.success)
                test.eq(#captured_calls.commit_submit, 1)

                local first_submit = captured_calls.commit_submit[1]
                local data_commands: number = 0
                for _, cmd in ipairs(first_submit.commands) do
                    if cmd.type == consts.COMMAND_TYPES.CREATE_DATA then
                        data_commands = data_commands + 1
                    end
                end
                test.eq(data_commands, 2)
            end)

            it("should handle complete without output content", function()
                local test_node_no_targets, _err = node.new({
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456"
                }, mock_deps)
                test.not_nil(test_node_no_targets)

                local result = test_node_no_targets:complete()

                test.is_true(result.success)
                test.contains(result.message, "completed successfully")
            end)

            it("should handle metadata updates in complete/fail", function()
                test.not_nil(test_node)
                test_node:complete(nil, "Custom message", { final_status = "success" })

                test.eq(test_node._metadata.status_message, "Custom message")
                test.eq(test_node._metadata.final_status, "success")
            end)

            it("should verify config-based targets are loaded correctly", function()
                test.not_nil(test_node)
                test.eq(#test_node.data_targets, 2)
                test.eq(#test_node.error_targets, 2)
                test.eq(test_node.data_targets[1].data_type, "output.result")
                test.eq(test_node.error_targets[1].data_type, "error.details")

                local config = test_node:config()
                test.eq(#config.data_targets, 2)
                test.eq(#config.error_targets, 2)
            end)
        end)

        describe("Error Handling", function()
            local test_node: any
            local failing_deps: any

            before_each(function()
                local _err: string?
                test_node, _err = node.new({
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456"
                }, mock_deps)

                failing_deps = {
                    commit = {
                        submit = function()
                            return nil, "Database connection failed"
                        end
                    },
                    process = {
                        send = function() return false end,
                        listen = function() return { receive = function() return nil, false end } end
                    },
                    data_reader = mock_deps.data_reader
                }
            end)

            it("should handle commit submission failures", function()
                local failing_node, _err = node.new({
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456"
                }, failing_deps)
                test.not_nil(failing_node)

                local result, err = failing_node:yield()

                test.is_nil(result)
                test.not_nil(err)
                test.contains(err, "Failed to submit yield")
                test.contains(err, "Database connection failed")
            end)

            it("should handle process send failures", function()
                local process_fail_deps: any = {
                    commit = mock_deps.commit,
                    process = {
                        send = function() return false end,
                        listen = mock_deps.process.listen
                    },
                    data_reader = mock_deps.data_reader
                }

                local failing_node, _err = node.new({
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456"
                }, process_fail_deps)
                test.not_nil(failing_node)

                local result, err = failing_node:yield()

                test.is_nil(result)
                test.contains(err, "Failed to send yield signal")
            end)

            it("should handle yield channel failures", function()
                local channel_fail_deps: any = {
                    commit = mock_deps.commit,
                    process = {
                        send = mock_deps.process.send,
                        listen = function()
                            return {
                                receive = function() return nil, false end
                            }
                        end
                    },
                    data_reader = mock_deps.data_reader
                }

                local failing_node, _err = node.new({
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456"
                }, channel_fail_deps)
                test.not_nil(failing_node)

                local result, err = failing_node:yield({ run_nodes = { "child-1" } })

                test.is_nil(result)
                test.contains(err, "Yield channel closed")
            end)
        end)

        describe("Query Operations", function()
            local test_node: any

            before_each(function()
                local _err: string?
                test_node, _err = node.new({
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456"
                }, mock_deps)
            end)

            it("should handle query operations", function()
                test.not_nil(test_node)
                local query_builder = test_node:query()

                test.not_nil(query_builder)
                test.eq(type(query_builder.with_nodes), "function")
            end)
        end)

        describe("Expr Output Routing", function()
            local test_node: any

            before_each(function()
                local _err: string?
                test_node, _err = node.new({
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456",
                    node = {
                        config = {
                            data_targets = {
                                {
                                    data_type = "workflow.output",
                                    key = "transformed_result",
                                    transform = "{ processed: output.message, timestamp: now() }"
                                },
                                {
                                    data_type = "metrics.count",
                                    key = "word_count",
                                    transform = "len(split(output.message, ' '))"
                                }
                            }
                        }
                    }
                }, mock_deps)
            end)

            it("should apply transforms to output content", function()
                test.not_nil(test_node)
                local result = test_node:complete({ message = "hello world test" }, "Processing complete")

                test.is_true(result.success)
                test.eq(#captured_calls.commit_submit, 1)

                local submit_call = captured_calls.commit_submit[1]
                test.eq(#submit_call.commands, 3) -- metadata + 2 data targets

                local data_commands: {any} = {}
                for _, cmd in ipairs(submit_call.commands) do
                    if cmd.type == consts.COMMAND_TYPES.CREATE_DATA then
                        table.insert(data_commands, cmd)
                    end
                end

                test.eq(#data_commands, 2)

                local transformed_cmd: any = nil
                local word_count_cmd: any = nil
                for _, cmd in ipairs(data_commands) do
                    if cmd.payload.key == "transformed_result" then
                        transformed_cmd = cmd
                    elseif cmd.payload.key == "word_count" then
                        word_count_cmd = cmd
                    end
                end

                test.not_nil(transformed_cmd)
                test.eq(type(transformed_cmd.payload.content), "table")
                test.eq(transformed_cmd.payload.content.processed, "hello world test")
                test.eq(type(transformed_cmd.payload.content.timestamp), "number")

                test.not_nil(word_count_cmd)
                test.eq(word_count_cmd.payload.content, 3)
            end)

            it("should handle simple value transforms", function()
                local test_node_simple, _err = node.new({
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456",
                    node = {
                        config = {
                            data_targets = {
                                {
                                    data_type = "simple.output",
                                    key = "upper_message",
                                    transform = "upper(output.message)"
                                }
                            }
                        }
                    }
                }, mock_deps)
                test.not_nil(test_node_simple)

                local result = test_node_simple:complete({ message = "hello world" })

                test.is_true(result.success)

                local submit_call = captured_calls.commit_submit[1]
                local data_cmd: any = nil
                for _, cmd in ipairs(submit_call.commands) do
                    if cmd.type == consts.COMMAND_TYPES.CREATE_DATA then
                        data_cmd = cmd
                        break
                    end
                end

                test.not_nil(data_cmd)
                test.eq(data_cmd.payload.content, "HELLO WORLD")
            end)

            it("should pass through untransformed content when no transform specified", function()
                local test_node_no_transform, _err = node.new({
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456",
                    node = {
                        config = {
                            data_targets = {
                                {
                                    data_type = "raw.output",
                                    key = "original"
                                }
                            }
                        }
                    }
                }, mock_deps)
                test.not_nil(test_node_no_transform)

                local output_content = { message = "original content", score = 85 }
                local result = test_node_no_transform:complete(output_content)

                test.is_true(result.success)

                local submit_call = captured_calls.commit_submit[1]
                local data_cmd: any = nil
                for _, cmd in ipairs(submit_call.commands) do
                    if cmd.type == consts.COMMAND_TYPES.CREATE_DATA then
                        data_cmd = cmd
                        break
                    end
                end

                test.not_nil(data_cmd)
                test.eq(data_cmd.payload.content, output_content)
            end)

            it("should handle mathematical and string operations in transforms", function()
                local test_node_complex, _err = node.new({
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456",
                    node = {
                        config = {
                            data_targets = {
                                {
                                    data_type = "calculated.output",
                                    key = "math_result",
                                    transform = "{ score_doubled: output.score * 2, grade: output.score >= 90 ? 'A' : 'B', name_upper: upper(output.name) }"
                                }
                            }
                        }
                    }
                }, mock_deps)
                test.not_nil(test_node_complex)

                local result = test_node_complex:complete({ score = 85, name = "john" })

                test.is_true(result.success)

                local submit_call = captured_calls.commit_submit[1]
                local data_cmd: any = nil
                for _, cmd in ipairs(submit_call.commands) do
                    if cmd.type == consts.COMMAND_TYPES.CREATE_DATA then
                        data_cmd = cmd
                        break
                    end
                end

                test.not_nil(data_cmd)
                test.eq(data_cmd.payload.content.score_doubled, 170)
                test.eq(data_cmd.payload.content.grade, "B")
                test.eq(data_cmd.payload.content.name_upper, "JOHN")
            end)
        end)

        describe("Conditional Output Routing", function()
            local test_node: any

            before_each(function()
                local _err: string?
                test_node, _err = node.new({
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456",
                    node = {
                        config = {
                            data_targets = {
                                {
                                    data_type = "high_score.output",
                                    key = "high_score",
                                    condition = "output.score >= 80",
                                    transform = "{ message: 'Great job!', score: output.score }"
                                },
                                {
                                    data_type = "low_score.output",
                                    key = "low_score",
                                    condition = "output.score < 80",
                                    transform = "{ message: 'Keep trying!', score: output.score }"
                                },
                                {
                                    data_type = "always.output",
                                    key = "summary",
                                    transform = "{ total_attempts: 1, final_score: output.score }"
                                }
                            }
                        }
                    }
                }, mock_deps)
            end)

            it("should create data only when condition is true", function()
                test.not_nil(test_node)
                local result = test_node:complete({ score = 85 })

                test.is_true(result.success)

                local submit_call = captured_calls.commit_submit[1]
                local data_commands: {any} = {}
                for _, cmd in ipairs(submit_call.commands) do
                    if cmd.type == consts.COMMAND_TYPES.CREATE_DATA then
                        table.insert(data_commands, cmd)
                    end
                end

                test.eq(#data_commands, 2)

                local keys: {string} = {}
                for _, cmd in ipairs(data_commands) do
                    table.insert(keys, cmd.payload.key)
                end

                local has_high_score: boolean = false
                local has_summary: boolean = false
                local has_low_score: boolean = false

                for _, key in ipairs(keys) do
                    if key == "high_score" then has_high_score = true end
                    if key == "summary" then has_summary = true end
                    if key == "low_score" then has_low_score = true end
                end

                test.is_true(has_high_score)
                test.is_true(has_summary)
                test.is_false(has_low_score)
            end)

            it("should evaluate different conditions correctly", function()
                local test_node_low, _err = node.new({
                    node_id = "test-node-456",
                    dataflow_id = "test-dataflow-456",
                    node = {
                        config = {
                            data_targets = {
                                {
                                    data_type = "high_score.output",
                                    key = "high_score",
                                    condition = "output.score >= 80"
                                },
                                {
                                    data_type = "low_score.output",
                                    key = "low_score",
                                    condition = "output.score < 80"
                                }
                            }
                        }
                    }
                }, mock_deps)
                test.not_nil(test_node_low)

                local result = test_node_low:complete({ score = 65 })

                test.is_true(result.success)

                local submit_call = captured_calls.commit_submit[1]
                local data_commands: {any} = {}
                for _, cmd in ipairs(submit_call.commands) do
                    if cmd.type == consts.COMMAND_TYPES.CREATE_DATA then
                        table.insert(data_commands, cmd)
                    end
                end

                test.eq(#data_commands, 1)
                test.eq((data_commands[1] :: any).payload.key, "low_score")
            end)

            it("should handle complex conditional expressions", function()
                local test_node_complex, _err = node.new({
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456",
                    node = {
                        config = {
                            data_targets = {
                                {
                                    data_type = "qualified.output",
                                    key = "qualified",
                                    condition = "output.score >= 70 && output.attendance > 0.8",
                                    transform = "{ qualified: true, final_grade: output.score }"
                                },
                                {
                                    data_type = "failed.output",
                                    key = "failed",
                                    condition = "output.score < 70 || output.attendance <= 0.8",
                                    transform = "{ qualified: false, reason: output.score < 70 ? 'low_score' : 'poor_attendance' }"
                                }
                            }
                        }
                    }
                }, mock_deps)
                test.not_nil(test_node_complex)

                local result = test_node_complex:complete({ score = 75, attendance = 0.9 })

                test.is_true(result.success)

                local submit_call = captured_calls.commit_submit[1]
                local data_commands: {any} = {}
                for _, cmd in ipairs(submit_call.commands) do
                    if cmd.type == consts.COMMAND_TYPES.CREATE_DATA then
                        table.insert(data_commands, cmd)
                    end
                end

                test.eq(#data_commands, 1)
                test.eq((data_commands[1] :: any).payload.key, "qualified")
                test.is_true((data_commands[1] :: any).payload.content.qualified)
            end)

            it("should skip target when condition evaluates to false and handle empty commits", function()
                local test_node_skip, _err = node.new({
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456",
                    node = {
                        config = {
                            data_targets = {
                                {
                                    data_type = "never.output",
                                    key = "never_created",
                                    condition = "false",
                                    transform = "{ should_not_exist: true }"
                                }
                            }
                        }
                    }
                }, mock_deps)
                test.not_nil(test_node_skip)

                local result = test_node_skip:complete({ message = "test" })

                test.is_true(result.success)

                test.eq(#captured_calls.commit_submit, 0)
            end)

            it("should handle mixed conditions with some true and some false", function()
                local test_node_mixed, _err = node.new({
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456",
                    node = {
                        config = {
                            data_targets = {
                                {
                                    data_type = "true_condition.output",
                                    key = "should_create",
                                    condition = "true"
                                },
                                {
                                    data_type = "false_condition.output",
                                    key = "should_not_create",
                                    condition = "false"
                                },
                                {
                                    data_type = "no_condition.output",
                                    key = "always_create"
                                }
                            }
                        }
                    }
                }, mock_deps)
                test.not_nil(test_node_mixed)

                local result = test_node_mixed:complete({ message = "test" })

                test.is_true(result.success)

                local submit_call = captured_calls.commit_submit[1]
                local data_commands: {any} = {}
                for _, cmd in ipairs(submit_call.commands) do
                    if cmd.type == consts.COMMAND_TYPES.CREATE_DATA then
                        table.insert(data_commands, cmd)
                    end
                end

                test.eq(#data_commands, 2)

                local keys: {string} = {}
                for _, cmd in ipairs(data_commands) do
                    table.insert(keys, cmd.payload.key)
                end

                local has_should_create: boolean = false
                local has_always_create: boolean = false
                local has_should_not_create: boolean = false

                for _, key in ipairs(keys) do
                    if key == "should_create" then has_should_create = true end
                    if key == "always_create" then has_always_create = true end
                    if key == "should_not_create" then has_should_not_create = true end
                end

                test.is_true(has_should_create)
                test.is_true(has_always_create)
                test.is_false(has_should_not_create)
            end)
        end)

        describe("Error Target Expr Support", function()
            local test_node: any

            before_each(function()
                local _err: string?
                test_node, _err = node.new({
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456",
                    node = {
                        config = {
                            error_targets = {
                                {
                                    data_type = "user.notification",
                                    key = "user_message",
                                    transform = "error.code == 'TIMEOUT' ? 'Service temporarily unavailable' : 'An error occurred'"
                                },
                                {
                                    data_type = "system.alert",
                                    key = "alert",
                                    condition = "error.severity == 'high'",
                                    transform = "{ error_code: error.code, timestamp: now(), details: error.message }"
                                },
                                {
                                    data_type = "audit.log",
                                    key = "error_log",
                                    transform = "{ error: error.message, node_id: node.node_id }"
                                }
                            }
                        }
                    }
                }, mock_deps)
            end)

            it("should apply transforms to error content", function()
                test.not_nil(test_node)
                local error_details = { code = "TIMEOUT", message = "Request timed out", severity = "medium" }
                local result = test_node:fail(error_details, "Operation failed")

                test.is_false(result.success)

                local submit_call = captured_calls.commit_submit[1]
                local data_commands: {any} = {}
                for _, cmd in ipairs(submit_call.commands) do
                    if cmd.type == consts.COMMAND_TYPES.CREATE_DATA then
                        table.insert(data_commands, cmd)
                    end
                end

                test.eq(#data_commands, 2)

                local user_msg_cmd: any = nil
                local audit_cmd: any = nil
                for _, cmd in ipairs(data_commands) do
                    if cmd.payload.key == "user_message" then
                        user_msg_cmd = cmd
                    elseif cmd.payload.key == "error_log" then
                        audit_cmd = cmd
                    end
                end

                test.not_nil(user_msg_cmd)
                test.eq(user_msg_cmd.payload.content, "Service temporarily unavailable")

                test.not_nil(audit_cmd)
                test.eq(audit_cmd.payload.content.error, "Request timed out")
                test.eq(audit_cmd.payload.content.node_id, "test-node-123")
            end)

            it("should handle conditional error targets", function()
                test.not_nil(test_node)
                local error_details = { code = "DATABASE_ERROR", message = "Connection failed", severity = "high" }
                local result = test_node:fail(error_details, "Database error")

                test.is_false(result.success)

                local submit_call = captured_calls.commit_submit[1]
                local data_commands: {any} = {}
                for _, cmd in ipairs(submit_call.commands) do
                    if cmd.type == consts.COMMAND_TYPES.CREATE_DATA then
                        table.insert(data_commands, cmd)
                    end
                end

                test.eq(#data_commands, 3)

                local alert_cmd: any = nil
                for _, cmd in ipairs(data_commands) do
                    if cmd.payload.key == "alert" then
                        alert_cmd = cmd
                        break
                    end
                end

                test.not_nil(alert_cmd)
                test.eq(alert_cmd.payload.content.error_code, "DATABASE_ERROR")
                test.eq(type(alert_cmd.payload.content.timestamp), "number")
            end)

            it("should gracefully handle error transform failures", function()
                local test_node_bad_transform, _err = node.new({
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456",
                    node = {
                        config = {
                            error_targets = {
                                {
                                    data_type = "error.output",
                                    key = "bad_transform",
                                    transform = "undefined_function(error.message)"
                                }
                            }
                        }
                    }
                }, mock_deps)
                test.not_nil(test_node_bad_transform)

                local error_details = { message = "original error" }
                local result = test_node_bad_transform:fail(error_details, "Test error")

                test.is_false(result.success)
                test.eq(result.message, "Test error")

                local submit_call = captured_calls.commit_submit[1]
                local data_commands: {any} = {}
                for _, cmd in ipairs(submit_call.commands) do
                    if cmd.type == consts.COMMAND_TYPES.CREATE_DATA then
                        table.insert(data_commands, cmd)
                    end
                end

                test.eq(#data_commands, 1)
                test.eq((data_commands[1] :: any).payload.content, error_details)
            end)

            it("should skip error targets when condition fails", function()
                local test_node_condition_fail, _err = node.new({
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456",
                    node = {
                        config = {
                            error_targets = {
                                {
                                    data_type = "error.high",
                                    key = "high_priority",
                                    condition = "error.severity == 'critical'"
                                },
                                {
                                    data_type = "error.general",
                                    key = "general_error"
                                }
                            }
                        }
                    }
                }, mock_deps)
                test.not_nil(test_node_condition_fail)

                local error_details = { severity = "medium", message = "moderate error" }
                local result = test_node_condition_fail:fail(error_details)

                test.is_false(result.success)

                local submit_call = captured_calls.commit_submit[1]
                local data_commands: {any} = {}
                for _, cmd in ipairs(submit_call.commands) do
                    if cmd.type == consts.COMMAND_TYPES.CREATE_DATA then
                        table.insert(data_commands, cmd)
                    end
                end

                test.eq(#data_commands, 1)
                test.eq((data_commands[1] :: any).payload.key, "general_error")
            end)
        end)

        describe("Expr Error Handling in Output Routing", function()
            it("BC_REGRESSION_C2_node_complete_returns_error_pair", function()
                local test_node_bad, _err = node.new({
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456",
                    node = {
                        config = {
                            data_targets = {
                                {
                                    data_type = "bad.output",
                                    key = "bad_transform",
                                    transform = "invalid + syntax +"
                                }
                            }
                        }
                    }
                }, mock_deps)
                test.not_nil(test_node_bad)

                local result, err = test_node_bad:complete({ message = "test" })

                test.is_nil(result)
                test.not_nil(err)
                test.contains(err, "Output transform failed")
            end)

            it("should fail when data target transform has invalid expression", function()
                local test_node_bad, _err = node.new({
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456",
                    node = {
                        config = {
                            data_targets = {
                                {
                                    data_type = "bad.output",
                                    key = "bad_transform",
                                    transform = "invalid + syntax +"
                                }
                            }
                        }
                    }
                }, mock_deps)
                test.not_nil(test_node_bad)

                local result, error_msg = test_node_bad:complete({ message = "test" })

                test.is_nil(result)
                test.contains(error_msg, "Output transform failed")
            end)

            it("should fail when data target condition has invalid expression", function()
                local test_node_bad_condition, _err = node.new({
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456",
                    node = {
                        config = {
                            data_targets = {
                                {
                                    data_type = "bad.output",
                                    key = "bad_condition",
                                    condition = "undefined_var.property"
                                }
                            }
                        }
                    }
                }, mock_deps)
                test.not_nil(test_node_bad_condition)

                local result, error_msg = test_node_bad_condition:complete({ message = "test" })

                test.is_nil(result)
                test.contains(error_msg, "Output condition evaluation failed")
            end)

            it("should gracefully skip error targets with bad conditions", function()
                local test_node_bad_error_condition, _err = node.new({
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456",
                    node = {
                        config = {
                            error_targets = {
                                {
                                    data_type = "error.bad",
                                    key = "bad_condition",
                                    condition = "undefined_var.property"
                                },
                                {
                                    data_type = "error.good",
                                    key = "good_target"
                                }
                            }
                        }
                    }
                }, mock_deps)
                test.not_nil(test_node_bad_error_condition)

                local result = test_node_bad_error_condition:fail({ message = "test error" })

                test.is_false(result.success)

                local submit_call = captured_calls.commit_submit[1]
                local data_commands: {any} = {}
                for _, cmd in ipairs(submit_call.commands) do
                    if cmd.type == consts.COMMAND_TYPES.CREATE_DATA then
                        table.insert(data_commands, cmd)
                    end
                end

                test.eq(#data_commands, 1)
                test.eq((data_commands[1] :: any).payload.key, "good_target")
            end)

            it("should validate transform expressions properly", function()
                local test_node_validation, _err = node.new({
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456",
                    node = {
                        config = {
                            data_targets = {
                                {
                                    data_type = "validation.output",
                                    key = "validation_test",
                                    transform = "output.score > 50 ? 'pass' : 'fail'"
                                }
                            }
                        }
                    }
                }, mock_deps)
                test.not_nil(test_node_validation)

                local result = test_node_validation:complete({ score = 75 })
                test.is_true(result.success)

                local submit_call = captured_calls.commit_submit[1]
                local data_cmd: any = nil
                for _, cmd in ipairs(submit_call.commands) do
                    if cmd.type == consts.COMMAND_TYPES.CREATE_DATA then
                        data_cmd = cmd
                        break
                    end
                end

                test.not_nil(data_cmd)
                test.eq(data_cmd.payload.content, "pass")
            end)

            it("should handle type errors in expressions gracefully", function()
                local test_node_type_error, _err = node.new({
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456",
                    node = {
                        config = {
                            data_targets = {
                                {
                                    data_type = "type.error",
                                    key = "type_error",
                                    transform = "len(output.number)"
                                }
                            }
                        }
                    }
                }, mock_deps)
                test.not_nil(test_node_type_error)

                local result, error_msg = test_node_type_error:complete({ number = 42 })

                test.is_nil(result)
                test.contains(error_msg, "Output transform failed")
            end)

            it("should handle nested object transforms", function()
                local test_node_nested, _err = node.new({
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456",
                    node = {
                        config = {
                            data_targets = {
                                {
                                    data_type = "nested.output",
                                    key = "nested_result",
                                    transform = "{ user: { name: output.user.name, age: output.user.age }, metadata: { processed: true, timestamp: now() } }"
                                }
                            }
                        }
                    }
                }, mock_deps)
                test.not_nil(test_node_nested)

                local result = test_node_nested:complete({ user = { name = "Alice", age = 25 } })
                test.is_true(result.success)

                local submit_call = captured_calls.commit_submit[1]
                local data_cmd: any = nil
                for _, cmd in ipairs(submit_call.commands) do
                    if cmd.type == consts.COMMAND_TYPES.CREATE_DATA then
                        data_cmd = cmd
                        break
                    end
                end

                test.not_nil(data_cmd)
                test.eq(data_cmd.payload.content.user.name, "Alice")
                test.eq(data_cmd.payload.content.user.age, 25)
                test.is_true(data_cmd.payload.content.metadata.processed)
                test.eq(type(data_cmd.payload.content.metadata.timestamp), "number")
            end)
        end)

    end)
end

return test.run_cases(define_tests)
