local test = require("test")
local uuid = require("uuid")
local json = require("json")
local time = require("time")

local node = require("node")
local consts = require("consts")

local function define_tests()
    describe("Node SDK with DI", function()
        local mock_deps
        local captured_calls

        before_each(function()
            captured_calls = {
                commit_submit = {},
                process_send = {},
                process_listen = {},
                data_reader_calls = {}
            }

            mock_deps = {
                commit = {
                    submit = function(dataflow_id, op_id, commands)
                        table.insert(captured_calls.commit_submit, {
                            dataflow_id = dataflow_id,
                            op_id = op_id,
                            commands = commands
                        })
                        return { commit_id = uuid.v7() }, nil
                    end
                },
                data_reader = {
                    with_dataflow = function(dataflow_id)
                        table.insert(captured_calls.data_reader_calls, { method = "with_dataflow", dataflow_id = dataflow_id })
                        return {
                            with_nodes = function(node_id)
                                table.insert(captured_calls.data_reader_calls, { method = "with_nodes", node_id = node_id })
                                return {
                                    with_data_types = function(data_type)
                                        table.insert(captured_calls.data_reader_calls, { method = "with_data_types", data_type = data_type })
                                        return {
                                            fetch_options = function(options)
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
                    send = function(target, topic, payload)
                        table.insert(captured_calls.process_send, {
                            target = target,
                            topic = topic,
                            payload = payload
                        })
                        return true
                    end,
                    listen = function(topic)
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

                expect(err).to_be_nil()
                expect(instance).not_to_be_nil()
                expect(instance.node_id).to_equal("test-node-123")
                expect(instance.dataflow_id).to_equal("test-dataflow-456")
                expect(instance._deps).to_equal(mock_deps)
            end)

            it("should fail with missing required args", function()
                local instance, err = node.new(nil, mock_deps)
                expect(instance).to_be_nil()
                expect(err).to_contain("Node args required")

                instance, err = node.new({}, mock_deps)
                expect(instance).to_be_nil()
                expect(err).to_contain("node_id and dataflow_id")
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

                expect(err).to_be_nil()
                expect(instance).not_to_be_nil()
                expect(#instance.data_targets).to_equal(1)
                expect(#instance.error_targets).to_equal(1)
                expect(instance._metadata.custom).to_equal("value")
                expect(instance._config.timeout).to_equal(30)
                expect(instance._config.retries).to_equal(3)
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

                expect(err).to_be_nil()
                expect(instance).not_to_be_nil()
                expect(#instance.data_targets).to_equal(0)
                expect(#instance.error_targets).to_equal(0)
                expect(type(instance._config)).to_equal("table")
                expect(next(instance._config)).to_be_nil()
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
                expect(err).to_be_nil()

                local config = instance:config()
                expect(config).not_to_be_nil()
                expect(config.timeout).to_equal(30)
                expect(config.retries).to_equal(3)
                expect(config.api_endpoint).to_equal("https://api.example.com")
                expect(type(config.features)).to_equal("table")
                expect(#config.features).to_equal(2)
                expect(config.features[1]).to_equal("logging")
                expect(config.features[2]).to_equal("metrics")
            end)

            it("should return empty config when none provided", function()
                local args = {
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456"
                }

                local instance, err = node.new(args, mock_deps)
                expect(err).to_be_nil()

                local config = instance:config()
                expect(config).not_to_be_nil()
                expect(type(config)).to_equal("table")
                expect(next(config)).to_be_nil()
            end)
        end)

        describe("Input Methods", function()
            local test_node

            before_each(function()
                test_node, _ = node.new({
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456"
                }, mock_deps)
            end)

            it("should get all inputs as a map and cache them", function()
                local inputs = test_node:inputs()

                expect(inputs).not_to_be_nil()
                -- Input with discriminator="primary" is stored as inputs["primary"]
                expect(inputs.primary).not_to_be_nil()
                expect(inputs.primary.content.message).to_equal("hello")
                -- Input with no discriminator is stored by key
                expect(inputs.input2).not_to_be_nil()
                expect(inputs.input2.content).to_equal("plain text")

                expect(#captured_calls.data_reader_calls).to_be_greater_than(0)

                local call_count = #captured_calls.data_reader_calls
                local inputs2 = test_node:inputs()
                expect(inputs2).to_equal(inputs)
                expect(#captured_calls.data_reader_calls).to_equal(call_count)
            end)

            it("should get specific input by key", function()
                local input = test_node:input("primary")

                expect(input).not_to_be_nil()
                expect(input.content.message).to_equal("hello")
                expect(input.key).to_equal("input1")
                expect(input.discriminator).to_equal("primary")
            end)

            it("should fail when input key is missing", function()
                local success, err = pcall(function()
                    test_node:input(nil)
                end)

                expect(success).to_be_false()
                expect(err).to_contain("Input key is required")
            end)
        end)

        describe("Expr Input Transformation", function()
            local expr_mock_deps

            before_each(function()
                expr_mock_deps = {
                    commit = mock_deps.commit,
                    process = mock_deps.process,
                    data_reader = {
                        with_dataflow = function(dataflow_id)
                            table.insert(captured_calls.data_reader_calls, { method = "with_dataflow", dataflow_id = dataflow_id })
                            return {
                                with_nodes = function(node_id)
                                    table.insert(captured_calls.data_reader_calls, { method = "with_nodes", node_id = node_id })
                                    return {
                                        with_data_types = function(data_type)
                                            table.insert(captured_calls.data_reader_calls, { method = "with_data_types", data_type = data_type })
                                            return {
                                                fetch_options = function(options)
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
                expect(err).to_be_nil()

                local inputs = test_node:inputs()
                expect(inputs).not_to_be_nil()
                expect(inputs["default"]).not_to_be_nil()
                expect(inputs["default"].content).to_equal("John is 30 years old")
                expect(inputs["default"].key).to_equal("default")
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
                expect(err).to_be_nil()

                local inputs = test_node:inputs()
                expect(inputs).not_to_be_nil()
                expect(inputs.user_name.content).to_equal("John")
                expect(inputs.user_age.content).to_equal(30)
                expect(inputs.total_cost.content).to_equal(300)
                expect(inputs.is_adult.content).to_be_true()
                expect(inputs.greeting.content).to_equal("Hello World, John")
            end)

            it("should handle array operations in expressions", function()
                local complex_data_mock = {
                    commit = mock_deps.commit,
                    process = mock_deps.process,
                    data_reader = {
                        with_dataflow = function(dataflow_id)
                            return {
                                with_nodes = function(node_id)
                                    return {
                                        with_data_types = function(data_type)
                                            return {
                                                fetch_options = function(options)
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
                expect(err).to_be_nil()

                local inputs = test_node:inputs()
                expect(inputs).not_to_be_nil()
                expect(inputs.item_count.content).to_equal(3)
                expect(inputs.has_expensive_items.content).to_be_true()
                expect(type(inputs.cheap_items.content)).to_equal("table")
                expect(#inputs.cheap_items.content).to_equal(1)
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
                expect(err).to_be_nil()

                local inputs = test_node:inputs()
                expect(inputs).not_to_be_nil()
                expect(inputs.calculated_score.content).to_equal(102)
                expect(inputs.rounded_score.content).to_equal(98)
                expect(inputs.score_grade.content).to_equal("B")
                expect(inputs.power_calc.content).to_equal(900)
                expect(inputs.abs_diff.content).to_equal(5)
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
                expect(err).to_be_nil()

                local inputs = test_node:inputs()
                expect(inputs).not_to_be_nil()
                expect(inputs.upper_name.content).to_equal("JOHN")
                expect(inputs.name_length.content).to_equal(4)
                expect(inputs.contains_john.content).to_be_true()
                expect(inputs.starts_with_j.content).to_be_true()
                expect(inputs.trimmed_message.content).to_equal("Hello World")
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
                expect(err).to_be_nil()

                local inputs = test_node:inputs()
                expect(inputs).not_to_be_nil()
                expect(inputs.processed_name).not_to_be_nil()
                expect(inputs.processed_name.content).to_equal("JOHN")
                expect(inputs.processed_name.key).to_equal("processed_name")
                expect(type(inputs.processed_name.metadata)).to_equal("table")
                expect(inputs.processed_name.discriminator).to_be_nil()
            end)

            it("should return original inputs when no transform config", function()
                local args = {
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456"
                }

                local test_node, err = node.new(args, expr_mock_deps)
                expect(err).to_be_nil()

                local inputs = test_node:inputs()
                expect(inputs).not_to_be_nil()
                expect(inputs.user_data).not_to_be_nil()
                expect(inputs.user_data.content.name).to_equal("John")
                expect(inputs.order_data).not_to_be_nil()
                expect(inputs.message).not_to_be_nil()
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
                expect(err).to_be_nil()

                local inputs1 = test_node:inputs()
                local call_count = #captured_calls.data_reader_calls

                local inputs2 = test_node:inputs()
                expect(inputs2).to_equal(inputs1)
                expect(#captured_calls.data_reader_calls).to_equal(call_count)
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
                expect(err).to_be_nil()

                local success, error_msg = pcall(function()
                    test_node:inputs()
                end)

                expect(success).to_be_false()
                expect(error_msg).to_contain("Input transformation failed")
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
                expect(err).to_be_nil()

                local success, error_msg = pcall(function()
                    test_node:inputs()
                end)

                expect(success).to_be_false()
                expect(error_msg).to_contain("Input transformation failed")
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
                expect(err).to_be_nil()

                local success, error_msg = pcall(function()
                    test_node:inputs()
                end)

                expect(success).to_be_false()
                expect(error_msg).to_contain("Transform failed for invalid_field")
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
                expect(err).to_be_nil()

                local success, error_msg = pcall(function()
                    test_node:inputs()
                end)

                expect(success).to_be_false()
                expect(error_msg).to_contain("Input transformation failed")
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
                expect(err).to_be_nil()

                local success, error_msg = pcall(function()
                    test_node:inputs()
                end)

                expect(success).to_be_false()
                expect(error_msg).to_contain("input_transform must be string or table")
            end)
        end)

        describe("Complex Expr Scenarios", function()
            it("should handle nested object access", function()
                local complex_mock = {
                    commit = mock_deps.commit,
                    process = mock_deps.process,
                    data_reader = {
                        with_dataflow = function(dataflow_id)
                            return {
                                with_nodes = function(node_id)
                                    return {
                                        with_data_types = function(data_type)
                                            return {
                                                fetch_options = function(options)
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
                expect(err).to_be_nil()

                local inputs = test_node:inputs()
                expect(inputs).not_to_be_nil()
                expect(inputs.theme.content).to_equal("dark")
                expect(inputs.has_dark_theme.content).to_be_true()
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

                local expr_mock_with_user_data = {
                    commit = mock_deps.commit,
                    process = mock_deps.process,
                    data_reader = {
                        with_dataflow = function(dataflow_id)
                            return {
                                with_nodes = function(node_id)
                                    return {
                                        with_data_types = function(data_type)
                                            return {
                                                fetch_options = function(options)
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
                expect(err).to_be_nil()

                local inputs = test_node:inputs()
                expect(inputs).not_to_be_nil()
                expect(inputs.age_category.content).to_equal("adult")
                expect(type(inputs.current_time.content)).to_equal("number")
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

                local expr_mock_with_both = {
                    commit = mock_deps.commit,
                    process = mock_deps.process,
                    data_reader = {
                        with_dataflow = function(dataflow_id)
                            return {
                                with_nodes = function(node_id)
                                    return {
                                        with_data_types = function(data_type)
                                            return {
                                                fetch_options = function(options)
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
                expect(err).to_be_nil()

                local inputs = test_node:inputs()
                expect(inputs).not_to_be_nil()
                expect(inputs.is_valid_name.content).to_be_true()
                expect(inputs.contains_digits.content).to_be_false()
            end)
        end)

        describe("Data and Metadata Methods", function()
            local test_node

            before_each(function()
                test_node, _ = node.new({
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456"
                }, mock_deps)
            end)

            it("should create data with proper command queuing", function()
                local result = test_node:data(consts.DATA_TYPE.NODE_OUTPUT, { message = "test" })

                expect(result).to_equal(test_node)
                expect(#test_node._queued_commands).to_equal(1)
                expect(test_node._queued_commands[1].type).to_equal(consts.COMMAND_TYPES.CREATE_DATA)
                expect(test_node._queued_commands[1].payload.data_type).to_equal(consts.DATA_TYPE.NODE_OUTPUT)
            end)

            it("should update metadata properly", function()
                local result = test_node:update_metadata({ key1 = "value1", key2 = "value2" })

                expect(result).to_equal(test_node)
                expect(test_node._metadata.key1).to_equal("value1")
                expect(test_node._metadata.key2).to_equal("value2")
                expect(#test_node._queued_commands).to_equal(1)
                expect(test_node._queued_commands[1].type).to_equal(consts.COMMAND_TYPES.UPDATE_NODE)
            end)

            it("should merge metadata without overwriting existing values", function()
                test_node._metadata = { existing = "value", shared = "original" }

                test_node:update_metadata({ shared = "updated", new_key = "new_value" })

                expect(test_node._metadata.existing).to_equal("value")
                expect(test_node._metadata.shared).to_equal("updated")
                expect(test_node._metadata.new_key).to_equal("new_value")
            end)

            it("should handle nil and empty metadata updates gracefully", function()
                test_node:update_metadata(nil)
                expect(#test_node._queued_commands).to_equal(0)

                test_node:update_metadata({})
                expect(#test_node._queued_commands).to_equal(1)
            end)

            it("should determine content type automatically", function()
                test_node:data(consts.DATA_TYPE.NODE_OUTPUT, { message = "test" })
                test_node:data(consts.DATA_TYPE.NODE_OUTPUT, "plain text")

                expect(test_node._queued_commands[1].payload.content_type).to_equal(consts.CONTENT_TYPE.JSON)
                expect(test_node._queued_commands[2].payload.content_type).to_equal(consts.CONTENT_TYPE.TEXT)
            end)
        end)

        describe("Child Node Creation", function()
            local test_node

            before_each(function()
                test_node, _ = node.new({
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456"
                }, mock_deps)
            end)

            it("should create child nodes with auto-generated IDs", function()
                local definitions = {
                    { node_type = "child_type_1" },
                    { node_type = "child_type_2" }
                }

                local child_ids, err = test_node:with_child_nodes(definitions)

                expect(err).to_be_nil()
                expect(child_ids).not_to_be_nil()
                expect(#child_ids).to_equal(2)
                expect(#test_node._queued_commands).to_equal(2)

                for i, cmd in ipairs(test_node._queued_commands) do
                    expect(cmd.type).to_equal(consts.COMMAND_TYPES.CREATE_NODE)
                    expect(cmd.payload.node_type).to_equal(definitions[i].node_type)
                    expect(cmd.payload.parent_node_id).to_equal("test-node-123")
                end
            end)

            it("should create child nodes with provided IDs and config", function()
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

                expect(err).to_be_nil()
                expect(child_ids[1]).to_equal("child-1")
                expect(child_ids[2]).to_equal("child-2")

                expect(test_node._queued_commands[1].payload.config).not_to_be_nil()
                expect(test_node._queued_commands[1].payload.config.timeout).to_equal(60)
                expect(test_node._queued_commands[2].payload.config.parallel).to_be_true()
            end)

            it("should fail with invalid child definitions", function()
                local child_ids, err = test_node:with_child_nodes(nil)
                expect(child_ids).to_be_nil()
                expect(err).to_contain("Child definitions required")

                child_ids, err = test_node:with_child_nodes({ { node_type = nil } })
                expect(child_ids).to_be_nil()
                expect(err).to_contain("node_type")
            end)
        end)

        describe("Submit Functionality", function()
            local test_node

            before_each(function()
                test_node, _ = node.new({
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456"
                }, mock_deps)
            end)

            it("should submit queued commands without yielding", function()
                test_node:data(consts.DATA_TYPE.NODE_OUTPUT, { message = "test1" })
                test_node:update_metadata({ status = "processing" })
                test_node:data(consts.DATA_TYPE.NODE_OUTPUT, { message = "test2" })

                expect(#test_node._queued_commands).to_equal(3)

                local success, err = test_node:submit()

                expect(success).to_be_true()
                expect(err).to_be_nil()
                expect(#test_node._queued_commands).to_equal(0)

                expect(#captured_calls.commit_submit).to_equal(1)
                expect(#captured_calls.commit_submit[1].commands).to_equal(3)

                expect(#captured_calls.process_send).to_equal(0)
            end)

            it("should handle empty queue gracefully", function()
                expect(#test_node._queued_commands).to_equal(0)

                local success, err = test_node:submit()

                expect(success).to_be_true()
                expect(err).to_be_nil()

                expect(#captured_calls.commit_submit).to_equal(0)
            end)

            it("should handle commit failures gracefully", function()
                local failing_deps = {
                    commit = {
                        submit = function()
                            return nil, "Database connection failed"
                        end
                    },
                    process = mock_deps.process,
                    data_reader = mock_deps.data_reader
                }

                local failing_node, _ = node.new({
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456"
                }, failing_deps)

                failing_node:data(consts.DATA_TYPE.NODE_OUTPUT, { message = "test" })
                expect(#failing_node._queued_commands).to_equal(1)

                local success, err = failing_node:submit()

                expect(success).to_be_false()
                expect(err).to_equal("Database connection failed")
                expect(#failing_node._queued_commands).to_equal(1)
            end)

            it("should be chainable after submit success", function()
                test_node:data(consts.DATA_TYPE.NODE_OUTPUT, { message = "test1" })
                local success, err = test_node:submit()
                expect(success).to_be_true()
                expect(#test_node._queued_commands).to_equal(0)

                test_node:data(consts.DATA_TYPE.NODE_OUTPUT, { message = "test2" })
                test_node:update_metadata({ status = "updated" })
                expect(#test_node._queued_commands).to_equal(2)

                success, err = test_node:submit()
                expect(success).to_be_true()
                expect(#test_node._queued_commands).to_equal(0)

                expect(#captured_calls.commit_submit).to_equal(2)
            end)

            it("should preserve command order when submitting", function()
                test_node:data("type1", "content1")
                test_node:update_metadata({ key1 = "value1" })
                test_node:data("type2", "content2")

                local success, err = test_node:submit()
                expect(success).to_be_true()

                local submit_call = captured_calls.commit_submit[1]
                expect(#submit_call.commands).to_equal(3)
                expect(submit_call.commands[1].payload.data_type).to_equal("type1")
                expect(submit_call.commands[2].type).to_equal(consts.COMMAND_TYPES.UPDATE_NODE)
                expect(submit_call.commands[3].payload.data_type).to_equal("type2")
            end)

            it("should maintain dataflow_id and generate op_id correctly", function()
                test_node:data(consts.DATA_TYPE.NODE_OUTPUT, { message = "test" })

                local success, err = test_node:submit()
                expect(success).to_be_true()

                local submit_call = captured_calls.commit_submit[1]
                expect(submit_call.dataflow_id).to_equal("test-dataflow-456")
                expect(submit_call.op_id).not_to_be_nil()
                expect(type(submit_call.op_id)).to_equal("string")
                expect(string.len(submit_call.op_id)).to_be_greater_than(0)
            end)
        end)

        describe("Yield Functionality", function()
            local test_node

            before_each(function()
                test_node, _ = node.new({
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456"
                }, mock_deps)
            end)

            it("should handle empty yield", function()
                local result, err = test_node:yield()

                expect(err).to_be_nil()
                expect(result).not_to_be_nil()
                expect(type(result)).to_equal("table")

                expect(#captured_calls.commit_submit).to_equal(1)

                expect(#captured_calls.process_send).to_equal(1)
                expect(captured_calls.process_send[1].topic).to_equal(consts.MESSAGE_TOPIC.YIELD_REQUEST)
            end)

            it("should yield and wait for children", function()
                local result, err = test_node:yield({ run_nodes = { "child-1" } })

                expect(err).to_be_nil()
                expect(result).not_to_be_nil()
                expect(result["child-1"]).not_to_be_nil()
                expect(result["child-1"].status).to_equal("completed")

                expect(#captured_calls.commit_submit).to_equal(1)
                expect(#captured_calls.process_send).to_equal(1)
            end)

            it("should create yield persistence record", function()
                test_node:yield()

                expect(#captured_calls.commit_submit).to_equal(1)
                local submit_call = captured_calls.commit_submit[1]
                expect(submit_call.commands).not_to_be_nil()
                expect(#submit_call.commands).to_equal(1)
                expect(submit_call.commands[1].type).to_equal(consts.COMMAND_TYPES.CREATE_DATA)
                expect(submit_call.commands[1].payload.data_type).to_equal(consts.DATA_TYPE.NODE_YIELD)
            end)

            it("should differ from submit in that it sends process signals", function()
                test_node:data(consts.DATA_TYPE.NODE_OUTPUT, { message = "test" })

                local success, err = test_node:submit()
                expect(success).to_be_true()
                expect(#captured_calls.process_send).to_equal(0)

                captured_calls.process_send = {}
                captured_calls.commit_submit = {}

                test_node:data(consts.DATA_TYPE.NODE_OUTPUT, { message = "test2" })
                local result, yield_err = test_node:yield()

                expect(yield_err).to_be_nil()
                expect(#captured_calls.process_send).to_equal(1)
                expect(captured_calls.process_send[1].topic).to_equal(consts.MESSAGE_TOPIC.YIELD_REQUEST)
            end)
        end)

        describe("Output and Error Routing", function()
            local test_node

            before_each(function()
                test_node, _ = node.new({
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
                local result = test_node:complete({ message = "success" })

                expect(result.success).to_be_true()

                expect(#captured_calls.commit_submit).to_equal(1)

                local first_submit = captured_calls.commit_submit[1]
                local data_commands = 0
                for _, cmd in ipairs(first_submit.commands) do
                    if cmd.type == consts.COMMAND_TYPES.CREATE_DATA then
                        data_commands = data_commands + 1
                    end
                end
                expect(data_commands).to_equal(2)
            end)

            it("should route errors via error_targets from config on fail", function()
                local result = test_node:fail("Something went wrong")

                expect(result.success).to_be_false()
                expect(#captured_calls.commit_submit).to_equal(1)

                local first_submit = captured_calls.commit_submit[1]
                local data_commands = 0
                for _, cmd in ipairs(first_submit.commands) do
                    if cmd.type == consts.COMMAND_TYPES.CREATE_DATA then
                        data_commands = data_commands + 1
                    end
                end
                expect(data_commands).to_equal(2)
            end)

            it("should handle complete without output content", function()
                local test_node_no_targets, _ = node.new({
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456"
                }, mock_deps)

                local result = test_node_no_targets:complete()

                expect(result.success).to_be_true()
                expect(result.message).to_contain("completed successfully")
            end)

            it("should handle metadata updates in complete/fail", function()
                test_node:complete(nil, "Custom message", { final_status = "success" })

                expect(test_node._metadata.status_message).to_equal("Custom message")
                expect(test_node._metadata.final_status).to_equal("success")
            end)

            it("should verify config-based targets are loaded correctly", function()
                expect(#test_node.data_targets).to_equal(2)
                expect(#test_node.error_targets).to_equal(2)
                expect(test_node.data_targets[1].data_type).to_equal("output.result")
                expect(test_node.error_targets[1].data_type).to_equal("error.details")

                local config = test_node:config()
                expect(#config.data_targets).to_equal(2)
                expect(#config.error_targets).to_equal(2)
            end)
        end)

        describe("Error Handling", function()
            local test_node
            local failing_deps

            before_each(function()
                test_node, _ = node.new({
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
                local failing_node, _ = node.new({
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456"
                }, failing_deps)

                local result, err = failing_node:yield()

                expect(result).to_be_nil()
                expect(err).not_to_be_nil()
                expect(err).to_contain("Failed to submit yield")
                expect(err).to_contain("Database connection failed")
            end)

            it("should handle process send failures", function()
                local process_fail_deps = {
                    commit = mock_deps.commit,
                    process = {
                        send = function() return false end,
                        listen = mock_deps.process.listen
                    },
                    data_reader = mock_deps.data_reader
                }

                local failing_node, _ = node.new({
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456"
                }, process_fail_deps)

                local result, err = failing_node:yield()

                expect(result).to_be_nil()
                expect(err).to_contain("Failed to send yield signal")
            end)

            it("should handle yield channel failures", function()
                local channel_fail_deps = {
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

                local failing_node, _ = node.new({
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456"
                }, channel_fail_deps)

                local result, err = failing_node:yield({ run_nodes = { "child-1" } })

                expect(result).to_be_nil()
                expect(err).to_contain("Yield channel closed")
            end)
        end)

        describe("Query Operations", function()
            local test_node

            before_each(function()
                test_node, _ = node.new({
                    node_id = "test-node-123",
                    dataflow_id = "test-dataflow-456"
                }, mock_deps)
            end)

            it("should handle query operations", function()
                local query_builder = test_node:query()

                expect(query_builder).not_to_be_nil()
                expect(type(query_builder.with_nodes)).to_equal("function")
            end)
        end)

describe("Expr Output Routing", function()
            local test_node

            before_each(function()
                test_node, _ = node.new({
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
                local result = test_node:complete({ message = "hello world test" }, "Processing complete")

                expect(result.success).to_be_true()
                expect(#captured_calls.commit_submit).to_equal(1)

                local submit_call = captured_calls.commit_submit[1]
                expect(#submit_call.commands).to_equal(3) -- metadata + 2 data targets

                local data_commands = {}
                for _, cmd in ipairs(submit_call.commands) do
                    if cmd.type == consts.COMMAND_TYPES.CREATE_DATA then
                        table.insert(data_commands, cmd)
                    end
                end

                expect(#data_commands).to_equal(2)

                local transformed_cmd = nil
                local word_count_cmd = nil
                for _, cmd in ipairs(data_commands) do
                    if cmd.payload.key == "transformed_result" then
                        transformed_cmd = cmd
                    elseif cmd.payload.key == "word_count" then
                        word_count_cmd = cmd
                    end
                end

                expect(transformed_cmd).not_to_be_nil()
                expect(type(transformed_cmd.payload.content)).to_equal("table")
                expect(transformed_cmd.payload.content.processed).to_equal("hello world test")
                expect(type(transformed_cmd.payload.content.timestamp)).to_equal("number")

                expect(word_count_cmd).not_to_be_nil()
                expect(word_count_cmd.payload.content).to_equal(3)
            end)

            it("should handle simple value transforms", function()
                local test_node_simple, _ = node.new({
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

                local result = test_node_simple:complete({ message = "hello world" })

                expect(result.success).to_be_true()

                local submit_call = captured_calls.commit_submit[1]
                local data_cmd = nil
                for _, cmd in ipairs(submit_call.commands) do
                    if cmd.type == consts.COMMAND_TYPES.CREATE_DATA then
                        data_cmd = cmd
                        break
                    end
                end

                expect(data_cmd).not_to_be_nil()
                expect(data_cmd.payload.content).to_equal("HELLO WORLD")
            end)

            it("should pass through untransformed content when no transform specified", function()
                local test_node_no_transform, _ = node.new({
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

                local output_content = { message = "original content", score = 85 }
                local result = test_node_no_transform:complete(output_content)

                expect(result.success).to_be_true()

                local submit_call = captured_calls.commit_submit[1]
                local data_cmd = nil
                for _, cmd in ipairs(submit_call.commands) do
                    if cmd.type == consts.COMMAND_TYPES.CREATE_DATA then
                        data_cmd = cmd
                        break
                    end
                end

                expect(data_cmd).not_to_be_nil()
                expect(data_cmd.payload.content).to_equal(output_content)
            end)

            it("should handle mathematical and string operations in transforms", function()
                local test_node_complex, _ = node.new({
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

                local result = test_node_complex:complete({ score = 85, name = "john" })

                expect(result.success).to_be_true()

                local submit_call = captured_calls.commit_submit[1]
                local data_cmd = nil
                for _, cmd in ipairs(submit_call.commands) do
                    if cmd.type == consts.COMMAND_TYPES.CREATE_DATA then
                        data_cmd = cmd
                        break
                    end
                end

                expect(data_cmd).not_to_be_nil()
                expect(data_cmd.payload.content.score_doubled).to_equal(170)
                expect(data_cmd.payload.content.grade).to_equal("B")
                expect(data_cmd.payload.content.name_upper).to_equal("JOHN")
            end)
        end)

        describe("Conditional Output Routing", function()
            local test_node

            before_each(function()
                test_node, _ = node.new({
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
                local result = test_node:complete({ score = 85 })

                expect(result.success).to_be_true()

                local submit_call = captured_calls.commit_submit[1]
                local data_commands = {}
                for _, cmd in ipairs(submit_call.commands) do
                    if cmd.type == consts.COMMAND_TYPES.CREATE_DATA then
                        table.insert(data_commands, cmd)
                    end
                end

                expect(#data_commands).to_equal(2)

                local keys = {}
                for _, cmd in ipairs(data_commands) do
                    table.insert(keys, cmd.payload.key)
                end

                local has_high_score = false
                local has_summary = false
                local has_low_score = false

                for _, key in ipairs(keys) do
                    if key == "high_score" then has_high_score = true end
                    if key == "summary" then has_summary = true end
                    if key == "low_score" then has_low_score = true end
                end

                expect(has_high_score).to_be_true()
                expect(has_summary).to_be_true()
                expect(has_low_score).to_be_false()
            end)

            it("should evaluate different conditions correctly", function()
                local test_node_low, _ = node.new({
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

                local result = test_node_low:complete({ score = 65 })

                expect(result.success).to_be_true()

                local submit_call = captured_calls.commit_submit[1]
                local data_commands = {}
                for _, cmd in ipairs(submit_call.commands) do
                    if cmd.type == consts.COMMAND_TYPES.CREATE_DATA then
                        table.insert(data_commands, cmd)
                    end
                end

                expect(#data_commands).to_equal(1)
                expect(data_commands[1].payload.key).to_equal("low_score")
            end)

            it("should handle complex conditional expressions", function()
                local test_node_complex, _ = node.new({
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

                local result = test_node_complex:complete({ score = 75, attendance = 0.9 })

                expect(result.success).to_be_true()

                local submit_call = captured_calls.commit_submit[1]
                local data_commands = {}
                for _, cmd in ipairs(submit_call.commands) do
                    if cmd.type == consts.COMMAND_TYPES.CREATE_DATA then
                        table.insert(data_commands, cmd)
                    end
                end

                expect(#data_commands).to_equal(1)
                expect(data_commands[1].payload.key).to_equal("qualified")
                expect(data_commands[1].payload.content.qualified).to_be_true()
            end)

            it("should skip target when condition evaluates to false and handle empty commits", function()
                local test_node_skip, _ = node.new({
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

                local result = test_node_skip:complete({ message = "test" })

                expect(result.success).to_be_true()

                -- When no data targets are created and no metadata is updated, no commit happens
                expect(#captured_calls.commit_submit).to_equal(0)
            end)

            it("should handle mixed conditions with some true and some false", function()
                local test_node_mixed, _ = node.new({
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

                local result = test_node_mixed:complete({ message = "test" })

                expect(result.success).to_be_true()

                local submit_call = captured_calls.commit_submit[1]
                local data_commands = {}
                for _, cmd in ipairs(submit_call.commands) do
                    if cmd.type == consts.COMMAND_TYPES.CREATE_DATA then
                        table.insert(data_commands, cmd)
                    end
                end

                expect(#data_commands).to_equal(2)

                local keys = {}
                for _, cmd in ipairs(data_commands) do
                    table.insert(keys, cmd.payload.key)
                end

                local has_should_create = false
                local has_always_create = false
                local has_should_not_create = false

                for _, key in ipairs(keys) do
                    if key == "should_create" then has_should_create = true end
                    if key == "always_create" then has_always_create = true end
                    if key == "should_not_create" then has_should_not_create = true end
                end

                expect(has_should_create).to_be_true()
                expect(has_always_create).to_be_true()
                expect(has_should_not_create).to_be_false()
            end)
        end)

        describe("Error Target Expr Support", function()
            local test_node

            before_each(function()
                test_node, _ = node.new({
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
                local error_details = { code = "TIMEOUT", message = "Request timed out", severity = "medium" }
                local result = test_node:fail(error_details, "Operation failed")

                expect(result.success).to_be_false()

                local submit_call = captured_calls.commit_submit[1]
                local data_commands = {}
                for _, cmd in ipairs(submit_call.commands) do
                    if cmd.type == consts.COMMAND_TYPES.CREATE_DATA then
                        table.insert(data_commands, cmd)
                    end
                end

                expect(#data_commands).to_equal(2)

                local user_msg_cmd = nil
                local audit_cmd = nil
                for _, cmd in ipairs(data_commands) do
                    if cmd.payload.key == "user_message" then
                        user_msg_cmd = cmd
                    elseif cmd.payload.key == "error_log" then
                        audit_cmd = cmd
                    end
                end

                expect(user_msg_cmd).not_to_be_nil()
                expect(user_msg_cmd.payload.content).to_equal("Service temporarily unavailable")

                expect(audit_cmd).not_to_be_nil()
                expect(audit_cmd.payload.content.error).to_equal("Request timed out")
                expect(audit_cmd.payload.content.node_id).to_equal("test-node-123")
            end)

            it("should handle conditional error targets", function()
                local error_details = { code = "DATABASE_ERROR", message = "Connection failed", severity = "high" }
                local result = test_node:fail(error_details, "Database error")

                expect(result.success).to_be_false()

                local submit_call = captured_calls.commit_submit[1]
                local data_commands = {}
                for _, cmd in ipairs(submit_call.commands) do
                    if cmd.type == consts.COMMAND_TYPES.CREATE_DATA then
                        table.insert(data_commands, cmd)
                    end
                end

                expect(#data_commands).to_equal(3)

                local alert_cmd = nil
                for _, cmd in ipairs(data_commands) do
                    if cmd.payload.key == "alert" then
                        alert_cmd = cmd
                        break
                    end
                end

                expect(alert_cmd).not_to_be_nil()
                expect(alert_cmd.payload.content.error_code).to_equal("DATABASE_ERROR")
                expect(type(alert_cmd.payload.content.timestamp)).to_equal("number")
            end)

            it("should gracefully handle error transform failures", function()
                local test_node_bad_transform, _ = node.new({
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

                local error_details = { message = "original error" }
                local result = test_node_bad_transform:fail(error_details, "Test error")

                expect(result.success).to_be_false()
                expect(result.message).to_equal("Test error")

                local submit_call = captured_calls.commit_submit[1]
                local data_commands = {}
                for _, cmd in ipairs(submit_call.commands) do
                    if cmd.type == consts.COMMAND_TYPES.CREATE_DATA then
                        table.insert(data_commands, cmd)
                    end
                end

                expect(#data_commands).to_equal(1)
                expect(data_commands[1].payload.content).to_equal(error_details)
            end)

            it("should skip error targets when condition fails", function()
                local test_node_condition_fail, _ = node.new({
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

                local error_details = { severity = "medium", message = "moderate error" }
                local result = test_node_condition_fail:fail(error_details)

                expect(result.success).to_be_false()

                local submit_call = captured_calls.commit_submit[1]
                local data_commands = {}
                for _, cmd in ipairs(submit_call.commands) do
                    if cmd.type == consts.COMMAND_TYPES.CREATE_DATA then
                        table.insert(data_commands, cmd)
                    end
                end

                expect(#data_commands).to_equal(1)
                expect(data_commands[1].payload.key).to_equal("general_error")
            end)
        end)

        describe("Expr Error Handling in Output Routing", function()
            it("should fail when data target transform has invalid expression", function()
                local test_node_bad, _ = node.new({
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

                local success, error_msg = pcall(function()
                    test_node_bad:complete({ message = "test" })
                end)

                expect(success).to_be_false()
                expect(error_msg).to_contain("Output transform failed")
            end)

            it("should fail when data target condition has invalid expression", function()
                local test_node_bad_condition, _ = node.new({
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

                local success, error_msg = pcall(function()
                    test_node_bad_condition:complete({ message = "test" })
                end)

                expect(success).to_be_false()
                expect(error_msg).to_contain("Output condition evaluation failed")
            end)

            it("should gracefully skip error targets with bad conditions", function()
                local test_node_bad_error_condition, _ = node.new({
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

                local result = test_node_bad_error_condition:fail({ message = "test error" })

                expect(result.success).to_be_false()

                local submit_call = captured_calls.commit_submit[1]
                local data_commands = {}
                for _, cmd in ipairs(submit_call.commands) do
                    if cmd.type == consts.COMMAND_TYPES.CREATE_DATA then
                        table.insert(data_commands, cmd)
                    end
                end

                expect(#data_commands).to_equal(1)
                expect(data_commands[1].payload.key).to_equal("good_target")
            end)

            it("should validate transform expressions properly", function()
                local test_node_validation, _ = node.new({
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

                local result = test_node_validation:complete({ score = 75 })
                expect(result.success).to_be_true()

                local submit_call = captured_calls.commit_submit[1]
                local data_cmd = nil
                for _, cmd in ipairs(submit_call.commands) do
                    if cmd.type == consts.COMMAND_TYPES.CREATE_DATA then
                        data_cmd = cmd
                        break
                    end
                end

                expect(data_cmd).not_to_be_nil()
                expect(data_cmd.payload.content).to_equal("pass")
            end)

            it("should handle type errors in expressions gracefully", function()
                local test_node_type_error, _ = node.new({
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

                local success, error_msg = pcall(function()
                    test_node_type_error:complete({ number = 42 })
                end)

                expect(success).to_be_false()
                expect(error_msg).to_contain("Output transform failed")
            end)

            it("should handle nested object transforms", function()
                local test_node_nested, _ = node.new({
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

                local result = test_node_nested:complete({ user = { name = "Alice", age = 25 } })
                expect(result.success).to_be_true()

                local submit_call = captured_calls.commit_submit[1]
                local data_cmd = nil
                for _, cmd in ipairs(submit_call.commands) do
                    if cmd.type == consts.COMMAND_TYPES.CREATE_DATA then
                        data_cmd = cmd
                        break
                    end
                end

                expect(data_cmd).not_to_be_nil()
                expect(data_cmd.payload.content.user.name).to_equal("Alice")
                expect(data_cmd.payload.content.user.age).to_equal(25)
                expect(data_cmd.payload.content.metadata.processed).to_be_true()
                expect(type(data_cmd.payload.content.metadata.timestamp)).to_equal("number")
            end)
        end)

    end)
end

return test.run_cases(define_tests)