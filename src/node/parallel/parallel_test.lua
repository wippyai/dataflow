local test = require("test")
local uuid = require("uuid")
local json = require("json")
local client = require("client")
local consts = require("consts")
local data_reader = require("data_reader")
local parallel = require("parallel")

local function define_tests()
    describe("Parallel Tests", function()
        describe("Unit Tests - Configuration Validation", function()
            it("should validate failure strategies", function()
                local mock_node = {
                    config = function(_self: any)
                        return {
                            source_array_key = "items",
                            on_error = "invalid_strategy"
                        }
                    end,
                    inputs = function(_self: any)
                        return {
                            default = {
                                content = { items = { "a", "b" } }
                            }
                        }
                    end,
                    fail = function(_self: any, error_details: any, message: string?)
                        return {
                            success = false,
                            error = error_details,
                            message = message
                        }
                    end
                }

                parallel._deps.node = {
                    new = function(_args: any)
                        return mock_node, nil
                    end
                }

                local result = parallel.run({})
                test.is_false(result.success)
                test.eq(result.error.code, parallel.ERRORS.INVALID_ON_ERROR_STRATEGY)
            end)

            it("should validate batch size", function()
                local mock_node = {
                    config = function(_self: any)
                        return {
                            source_array_key = "items",
                            batch_size = -1
                        }
                    end,
                    inputs = function(_self: any)
                        return {
                            default = {
                                content = { items = { "a", "b" } }
                            }
                        }
                    end,
                    fail = function(_self: any, error_details: any, message: string?)
                        return {
                            success = false,
                            error = error_details,
                            message = message
                        }
                    end
                }

                parallel._deps.node = {
                    new = function(_args: any)
                        return mock_node, nil
                    end
                }

                local result = parallel.run({})
                test.is_false(result.success)
                test.eq(result.error.code, parallel.ERRORS.INVALID_BATCH_SIZE)
            end)

            it("should require source_array_key", function()
                local mock_node = {
                    config = function(_self: any)
                        return {}
                    end,
                    fail = function(_self: any, error_details: any, message: string?)
                        return {
                            success = false,
                            error = error_details,
                            message = message
                        }
                    end
                }

                parallel._deps.node = {
                    new = function(_args: any)
                        return mock_node, nil
                    end
                }

                local result = parallel.run({})
                test.is_false(result.success)
                test.eq(result.error.code, parallel.ERRORS.MISSING_SOURCE_ARRAY_KEY)
            end)

            it("should require input data", function()
                local mock_node = {
                    config = function(_self: any)
                        return {
                            source_array_key = "items"
                        }
                    end,
                    inputs = function(_self: any)
                        return {}
                    end,
                    fail = function(_self: any, error_details: any, message: string?)
                        return {
                            success = false,
                            error = error_details,
                            message = message
                        }
                    end
                }

                parallel._deps.node = {
                    new = function(_args: any)
                        return mock_node, nil
                    end
                }

                local result = parallel.run({})
                test.is_false(result.success)
                test.eq(result.error.code, parallel.ERRORS.NO_INPUT_DATA)
            end)

            it("should validate input structure", function()
                local mock_node = {
                    config = function(_self: any)
                        return {
                            source_array_key = "items"
                        }
                    end,
                    inputs = function(_self: any)
                        return {
                            default = {
                                content = { wrong_key = { "a", "b" } }
                            }
                        }
                    end,
                    fail = function(_self: any, error_details: any, message: string?)
                        return {
                            success = false,
                            error = error_details,
                            message = message
                        }
                    end
                }

                parallel._deps.node = {
                    new = function(_args: any)
                        return mock_node, nil
                    end
                }

                local result = parallel.run({})
                test.is_false(result.success)
                test.eq(result.error.code, parallel.ERRORS.INVALID_INPUT_STRUCTURE)
            end)

            it("should require non-empty array", function()
                local mock_node = {
                    config = function(_self: any)
                        return {
                            source_array_key = "items"
                        }
                    end,
                    inputs = function(_self: any)
                        return {
                            default = {
                                content = { items = {} }
                            }
                        }
                    end,
                    fail = function(_self: any, error_details: any, message: string?)
                        return {
                            success = false,
                            error = error_details,
                            message = message
                        }
                    end
                }

                parallel._deps.node = {
                    new = function(_args: any)
                        return mock_node, nil
                    end
                }

                local result = parallel.run({})
                test.is_false(result.success)
                test.eq(result.error.code, parallel.ERRORS.INVALID_INPUT_STRUCTURE)
            end)

            it("should validate item_steps configuration", function()
                local mock_node = {
                    config = function(_self: any)
                        return {
                            source_array_key = "items",
                            item_steps = "invalid_pipeline"
                        }
                    end,
                    inputs = function(_self: any)
                        return {
                            default = {
                                content = { items = { "a", "b" } }
                            }
                        }
                    end,
                    fail = function(_self: any, error_details: any, message: string?)
                        return {
                            success = false,
                            error = error_details,
                            message = message
                        }
                    end
                }

                parallel._deps.node = {
                    new = function(_args: any)
                        return mock_node, nil
                    end
                }

                local result = parallel.run({})
                test.is_false(result.success)
                test.eq(result.error.code, (parallel.ERRORS :: any).INVALID_PIPELINE_STEP)
            end)

            it("should validate item step structure", function()
                local mock_node = {
                    config = function(_self: any)
                        return {
                            source_array_key = "items",
                            item_steps = {
                                { type = "invalid_type", func_id = "test_func" }
                            }
                        }
                    end,
                    inputs = function(_self: any)
                        return {
                            default = {
                                content = { items = { "a", "b" } }
                            }
                        }
                    end,
                    fail = function(_self: any, error_details: any, message: string?)
                        return {
                            success = false,
                            error = error_details,
                            message = message
                        }
                    end
                }

                parallel._deps.node = {
                    new = function(_args: any)
                        return mock_node, nil
                    end
                }

                local result = parallel.run({})
                test.is_false(result.success)
                test.eq(result.error.code, (parallel.ERRORS :: any).INVALID_PIPELINE_STEP)
            end)

            it("should validate extractor names", function()
                local mock_node = {
                    config = function(_self: any)
                        return {
                            source_array_key = "items",
                            reduction_extract = "invalid_extractor"
                        }
                    end,
                    inputs = function(_self: any)
                        return {
                            default = {
                                content = { items = { "a", "b" } }
                            }
                        }
                    end,
                    fail = function(_self: any, error_details: any, message: string?)
                        return {
                            success = false,
                            error = error_details,
                            message = message
                        }
                    end
                }

                parallel._deps.node = {
                    new = function(_args: any)
                        return mock_node, nil
                    end
                }

                local result = parallel.run({})
                test.is_false(result.success)
                test.eq(result.error.code, (parallel.ERRORS :: any).INVALID_EXTRACTOR)
            end)

            it("should validate reduction pipeline compatibility", function()
                local mock_node = {
                    config = function(_self: any)
                        return {
                            source_array_key = "items",
                            reduction_extract = "invalid_extractor"
                        }
                    end,
                    inputs = function(_self: any)
                        return {
                            default = {
                                content = { items = { "a", "b" } }
                            }
                        }
                    end,
                    fail = function(_self: any, error_details: any, message: string?)
                        return {
                            success = false,
                            error = error_details,
                            message = message
                        }
                    end
                }

                parallel._deps.node = {
                    new = function(_args: any)
                        return mock_node, nil
                    end
                }

                local result = parallel.run({})
                test.is_false(result.success)
                test.eq(result.error.code, (parallel.ERRORS :: any).INVALID_EXTRACTOR)
            end)
        end)

        describe("Unit Tests - Extractors", function()
            it("should extract successes correctly", function()
                local parallel_result = {
                    successes = {
                        { iteration = 1, item = "a", result = "result_a" },
                        { iteration = 2, item = "b", result = "result_b" }
                    },
                    failures = {
                        { iteration = 3, item = "c", error = "failed" }
                    },
                    success_count = 2,
                    failure_count = 1,
                    total_iterations = 3
                }

                local extractor = parallel.extractors[parallel.EXTRACTORS.SUCCESSES]
                test.not_nil(extractor)
                local result = (extractor :: any)(parallel_result)

                test.eq(#result, 2)
                test.eq(result[1], "result_a")
                test.eq(result[2], "result_b")
            end)

            it("should extract failures correctly", function()
                local parallel_result = {
                    successes = {
                        { iteration = 1, item = "a", result = "result_a" }
                    },
                    failures = {
                        { iteration = 2, item = "b", error = "error_b" },
                        { iteration = 3, item = "c", error = "error_c" }
                    },
                    success_count = 1,
                    failure_count = 2,
                    total_iterations = 3
                }

                local extractor = parallel.extractors[parallel.EXTRACTORS.FAILURES]
                test.not_nil(extractor)
                local result = (extractor :: any)(parallel_result)

                test.eq(#result, 2)
                test.eq(result[1].item, "b")
                test.eq(result[1].error, "error_b")
                test.eq(result[2].item, "c")
                test.eq(result[2].error, "error_c")
            end)

            it("BC_REGRESSION_C3_legacy_output_shape_preserved", function()
                local mock_node = {
                    config = function(_self: any)
                        return {
                            source_array_key = "items",
                            output_shape = {
                                filter = "successes",
                                unwrap = true
                            }
                        }
                    end,
                    inputs = function(_self: any)
                        return {
                            default = {
                                content = { items = { "a" } }
                            }
                        }
                    end,
                    yield = function(_self: any, _options: any)
                        return {}, nil
                    end,
                    complete = function(_self: any, result: any, message: string?)
                        return {
                            success = true,
                            result = result,
                            message = message
                        }
                    end
                }

                local mock_template_graph = {
                    is_empty = function(_self: any)
                        return false
                    end
                }

                local mock_iterator = {
                    create_batch = function(_n: any, _template_graph: any, _items: any, _batch_start: number, _batch_end: number, _iteration_input_key: string)
                        return {
                            {
                                iteration = 1,
                                iteration_index = 1,
                                input_item = "a",
                                root_nodes = { "node1" }
                            }
                        }, nil
                    end,
                    collect_results = function(_n: any, _iteration: any)
                        return "result_a", nil
                    end
                }

                parallel._deps.node = {
                    new = function(_args: any)
                        return mock_node, nil
                    end
                }

                parallel._deps.template_graph = {
                    build_for_node = function(_node: any)
                        return mock_template_graph, nil
                    end
                }

                parallel._deps.iterator = mock_iterator

                local result = parallel.run({})
                test.is_true(result.success)
                test.eq(#result.result, 1)
                test.eq(result.result[1], "result_a")
                test.is_nil(result.result.successes)
            end)
        end)

        describe("Unit Tests - Template Discovery", function()
            it("should require template nodes", function()
                local mock_node = {
                    config = function(_self: any)
                        return {
                            source_array_key = "items"
                        }
                    end,
                    inputs = function(_self: any)
                        return {
                            default = {
                                content = { items = { "a", "b" } }
                            }
                        }
                    end,
                    fail = function(_self: any, error_details: any, message: string?)
                        return {
                            success = false,
                            error = error_details,
                            message = message
                        }
                    end
                }

                local mock_template_graph = {
                    is_empty = function(_self: any)
                        return true
                    end
                }

                parallel._deps.node = {
                    new = function(_args: any)
                        return mock_node, nil
                    end
                }

                parallel._deps.template_graph = {
                    build_for_node = function(_node: any)
                        return mock_template_graph, nil
                    end
                }

                local result = parallel.run({})
                test.is_false(result.success)
                test.eq(result.error.code, parallel.ERRORS.NO_TEMPLATES)
            end)

            it("should handle template discovery errors", function()
                local mock_node = {
                    config = function(_self: any)
                        return {
                            source_array_key = "items"
                        }
                    end,
                    inputs = function(_self: any)
                        return {
                            default = {
                                content = { items = { "a", "b" } }
                            }
                        }
                    end,
                    fail = function(_self: any, error_details: any, message: string?)
                        return {
                            success = false,
                            error = error_details,
                            message = message
                        }
                    end
                }

                parallel._deps.node = {
                    new = function(_args: any)
                        return mock_node, nil
                    end
                }

                parallel._deps.template_graph = {
                    build_for_node = function(_node: any)
                        return nil, "Template discovery failed"
                    end
                }

                local result = parallel.run({})
                test.is_false(result.success)
                test.eq(result.error.code, parallel.ERRORS.TEMPLATE_DISCOVERY_FAILED)
            end)
        end)

        describe("Unit Tests - Batch Processing", function()
            it("should process successful batch", function()
                local batches_processed: number = 0
                local yield_called: boolean = false

                local mock_node = {
                    config = function(_self: any)
                        return {
                            source_array_key = "items",
                            batch_size = 2
                        }
                    end,
                    inputs = function(_self: any)
                        return {
                            default = {
                                content = { items = { "a", "b", "c" } }
                            }
                        }
                    end,
                    yield = function(_self: any, _options: any)
                        yield_called = true
                        return {}, nil
                    end,
                    complete = function(_self: any, result: any, message: string?)
                        return {
                            success = true,
                            result = result,
                            message = message
                        }
                    end
                }

                local mock_template_graph = {
                    is_empty = function(_self: any)
                        return false
                    end
                }

                local mock_iterator = {
                    create_batch = function(_n: any, _template_graph: any, items: any, batch_start: number, _batch_end: number, _iteration_input_key: string)
                        batches_processed = batches_processed + 1
                        return {
                            {
                                iteration_index = batch_start,
                                input_item = items[batch_start],
                                root_nodes = { "node1" }
                            }
                        }, nil
                    end,
                    collect_results = function(_n: any, iteration: any)
                        return "result_" .. iteration.iteration_index, nil
                    end
                }

                parallel._deps.node = {
                    new = function(_args: any)
                        return mock_node, nil
                    end
                }

                parallel._deps.template_graph = {
                    build_for_node = function(_node: any)
                        return mock_template_graph, nil
                    end
                }

                parallel._deps.iterator = mock_iterator
                parallel._deps.time = {
                    sleep = function(_delay: any)
                    end
                }

                local result = parallel.run({})
                test.is_true(result.success)
                test.eq(batches_processed, 2)
                test.is_true(yield_called)
            end)

            it("should handle fail_fast strategy", function()
                local mock_node = {
                    config = function(_self: any)
                        return {
                            source_array_key = "items",
                            on_error = parallel.ON_ERROR_STRATEGIES.FAIL_FAST
                        }
                    end,
                    inputs = function(_self: any)
                        return {
                            default = {
                                content = { items = { "a", "b" } }
                            }
                        }
                    end,
                    yield = function(_self: any, _options: any)
                        return {}, nil
                    end,
                    fail = function(_self: any, error_details: any, message: string?)
                        return {
                            success = false,
                            error = error_details,
                            message = message
                        }
                    end
                }

                local mock_template_graph = {
                    is_empty = function(_self: any)
                        return false
                    end
                }

                local mock_iterator = {
                    create_batch = function(_n: any, _template_graph: any, _items: any, _batch_start: number, _batch_end: number, _iteration_input_key: string)
                        return nil, "Iteration creation failed"
                    end
                }

                parallel._deps.node = {
                    new = function(_args: any)
                        return mock_node, nil
                    end
                }

                parallel._deps.template_graph = {
                    build_for_node = function(_node: any)
                        return mock_template_graph, nil
                    end
                }

                parallel._deps.iterator = mock_iterator

                local result = parallel.run({})
                test.is_false(result.success)
                test.eq(result.error.code, parallel.ERRORS.ITERATION_FAILED)
            end)

            it("should recover completed iterations from durable result rows before rerunning an active batch", function()
                local created_iteration_indices = {}
                local query_calls = 0

                local mock_node = {
                    node_id = "parallel-node",
                    config = function(_self: any)
                        return {
                            source_array_key = "items",
                            batch_size = 2
                        }
                    end,
                    inputs = function(_self: any)
                        return {
                            default = {
                                content = { items = { "a", "b" } }
                            }
                        }
                    end,
                    query = function(_self: any)
                        query_calls = query_calls + 1
                        return {
                            with_nodes = function(query_self: any, _node_id: any)
                                return query_self
                            end,
                            with_data_types = function(query_self: any, _data_types: any)
                                return query_self
                            end,
                            all = function()
                                local rows = {
                                    {
                                        data_id = "cursor-row",
                                        node_id = "parallel-node",
                                        type = consts.DATA_TYPE.PARALLEL_PROGRESS,
                                        key = "cursor",
                                        content_type = consts.CONTENT_TYPE.JSON,
                                        content = {
                                            next_batch_start = 1,
                                            active_batch = {
                                                batch_start = 1,
                                                batch_end = 2,
                                                attempt_id = "attempt-1",
                                                submitted_iterations = { 1, 2 }
                                            }
                                        },
                                        metadata = {},
                                        created_at = "2026-01-01T00:00:00.000000001Z"
                                    }
                                }

                                if query_calls > 1 then
                                    rows[2] = {
                                        data_id = "result-row-1",
                                        node_id = "parallel-node",
                                        type = consts.DATA_TYPE.ITERATION_RESULT,
                                        discriminator = "iteration.000001.attempt-1",
                                        content_type = consts.CONTENT_TYPE.TEXT,
                                        content = "persisted-result-1",
                                        metadata = {
                                            iteration = 1,
                                            attempt_id = "attempt-1"
                                        },
                                        created_at = "2026-01-01T00:00:00.000000002Z"
                                    }
                                    rows[3] = {
                                        data_id = "result-row-2",
                                        node_id = "parallel-node",
                                        type = consts.DATA_TYPE.ITERATION_RESULT,
                                        discriminator = "iteration.000002.attempt-1",
                                        content_type = consts.CONTENT_TYPE.TEXT,
                                        content = "persisted-result-2",
                                        metadata = {
                                            iteration = 2,
                                            attempt_id = "attempt-1"
                                        },
                                        created_at = "2026-01-01T00:00:00.000000003Z"
                                    }
                                end

                                return rows
                            end
                        }
                    end,
                    data = function(_self: any, _data_type: any, _content: any, _options: any)
                        return true, nil
                    end,
                    submit = function(_self: any)
                        return true, nil
                    end,
                    yield = function(_self: any, _options: any)
                        return {}, nil
                    end,
                    complete = function(_self: any, result: any, message: string?)
                        return {
                            success = true,
                            result = result,
                            message = message
                        }
                    end,
                    fail = function(_self: any, error_details: any, message: string?)
                        return {
                            success = false,
                            error = error_details,
                            message = message
                        }
                    end
                }

                local mock_template_graph = {
                    is_empty = function(_self: any)
                        return false
                    end
                }

                local mock_iterator = {
                    create_batch = function(_n: any, _template_graph: any, items: any, _batch_start: number,
                                            _batch_end: number, _iteration_input_key: string, _passthrough_inputs: any,
                                            options: any)
                        created_iteration_indices = options.iteration_indices
                        return {
                            {
                                iteration = 2,
                                input_item = items[2],
                                attempt_id = "attempt-2",
                                root_nodes = {}
                            }
                        }, nil
                    end,
                    collect_results = function(_n: any, _iteration: any)
                        return nil, "No output data found for iteration"
                    end
                }

                parallel._deps.node = {
                    new = function(_args: any)
                        return mock_node, nil
                    end
                }

                parallel._deps.template_graph = {
                    build_for_node = function(_node: any)
                        return mock_template_graph, nil
                    end
                }

                parallel._deps.iterator = mock_iterator

                local result = parallel.run({})
                test.is_true(result.success)
                test.eq(#created_iteration_indices, 0)
                test.eq(result.result.success_count, 2)
                test.eq(result.result.successes[1].result, "persisted-result-1")
                test.eq(result.result.successes[2].result, "persisted-result-2")
            end)
        end)

        describe("Unit Tests - Item Pipeline Functionality", function()
            it("should apply item pipeline when configured", function()
                local item_pipeline_called: boolean = false
                local item_pipeline_input: any = nil

                local mock_node = {
                    config = function(_self: any)
                        return {
                            source_array_key = "items",
                            item_steps = {
                                { type = "map", func_id = "transform_item" }
                            }
                        }
                    end,
                    inputs = function(_self: any)
                        return {
                            default = {
                                content = { items = { "a" } }
                            }
                        }
                    end,
                    yield = function(_self: any, _options: any)
                        return {}, nil
                    end,
                    complete = function(_self: any, result: any, message: string?)
                        return {
                            success = true,
                            result = result,
                            message = message
                        }
                    end
                }

                local mock_template_graph = {
                    is_empty = function(_self: any)
                        return false
                    end
                }

                local mock_iterator = {
                    create_batch = function(_n: any, _template_graph: any, _items: any, _batch_start: number, _batch_end: number, _iteration_input_key: string)
                        return {
                            {
                                iteration_index = 1,
                                input_item = "a",
                                root_nodes = { "node1" }
                            }
                        }, nil
                    end,
                    collect_results = function(_n: any, _iteration: any)
                        return "large_result", nil
                    end
                }

                local mock_funcs = {
                    new = function()
                        return {
                            with_context = function(self: any, _context: any)
                                return self
                            end,
                            call = function(_self: any, _func_id: string, data: any)
                                item_pipeline_called = true
                                item_pipeline_input = data
                                return "processed_" .. tostring(data), nil
                            end
                        }
                    end
                }

                parallel._deps.node = {
                    new = function(_args: any)
                        return mock_node, nil
                    end
                }

                parallel._deps.template_graph = {
                    build_for_node = function(_node: any)
                        return mock_template_graph, nil
                    end
                }

                parallel._deps.iterator = mock_iterator
                parallel._deps.funcs = mock_funcs

                local result = parallel.run({})
                test.is_true(result.success)
                test.is_true(item_pipeline_called)
                test.eq(item_pipeline_input, "large_result")
                test.eq(result.result[1].result, "processed_large_result")
            end)

            it("should handle item pipeline errors with fail_fast strategy", function()
                local mock_node = {
                    config = function(_self: any)
                        return {
                            source_array_key = "items",
                            on_error = parallel.ON_ERROR_STRATEGIES.FAIL_FAST,
                            item_steps = {
                                { type = "map", func_id = "failing_transform" }
                            }
                        }
                    end,
                    inputs = function(_self: any)
                        return {
                            default = {
                                content = { items = { "a" } }
                            }
                        }
                    end,
                    yield = function(_self: any, _options: any)
                        return {}, nil
                    end,
                    complete = function(_self: any, result: any, message: string?)
                        return {
                            success = true,
                            result = result,
                            message = message
                        }
                    end,
                    fail = function(_self: any, error_details: any, message: string?)
                        return {
                            success = false,
                            error = error_details,
                            message = message
                        }
                    end
                }

                local mock_template_graph = {
                    is_empty = function(_self: any)
                        return false
                    end
                }

                local mock_iterator = {
                    create_batch = function(_n: any, _template_graph: any, _items: any, _batch_start: number, _batch_end: number, _iteration_input_key: string)
                        return {
                            {
                                iteration_index = 1,
                                input_item = "a",
                                root_nodes = { "node1" }
                            }
                        }, nil
                    end,
                    collect_results = function(_n: any, _iteration: any)
                        return "large_result", nil
                    end
                }

                local mock_funcs = {
                    new = function()
                        return {
                            with_context = function(self: any, _context: any)
                                return self
                            end,
                            call = function(_self: any, _func_id: string, _data: any)
                                return nil, "Item transform failed"
                            end
                        }
                    end
                }

                parallel._deps.node = {
                    new = function(_args: any)
                        return mock_node, nil
                    end
                }

                parallel._deps.template_graph = {
                    build_for_node = function(_node: any)
                        return mock_template_graph, nil
                    end
                }

                parallel._deps.iterator = mock_iterator
                parallel._deps.funcs = mock_funcs

                local result = parallel.run({})
                test.is_false(result.success)
                test.eq(result.error.code, parallel.ERRORS.ITERATION_FAILED)
                test.contains(result.error.message, "Item pipeline failed")
            end)

            it("should handle item pipeline errors with continue strategy", function()
                local mock_node = {
                    config = function(_self: any)
                        return {
                            source_array_key = "items",
                            on_error = parallel.ON_ERROR_STRATEGIES.CONTINUE,
                            item_steps = {
                                { type = "map", func_id = "failing_transform" }
                            }
                        }
                    end,
                    inputs = function(_self: any)
                        return {
                            default = {
                                content = { items = { "a" } }
                            }
                        }
                    end,
                    yield = function(_self: any, _options: any)
                        return {}, nil
                    end,
                    complete = function(_self: any, result: any, message: string?)
                        return {
                            success = true,
                            result = result,
                            message = message
                        }
                    end,
                    fail = function(_self: any, error_details: any, message: string?)
                        return {
                            success = false,
                            error = error_details,
                            message = message
                        }
                    end
                }

                local mock_template_graph = {
                    is_empty = function(_self: any)
                        return false
                    end
                }

                local mock_iterator = {
                    create_batch = function(_n: any, _template_graph: any, _items: any, _batch_start: number, _batch_end: number, _iteration_input_key: string)
                        return {
                            {
                                iteration_index = 1,
                                input_item = "a",
                                root_nodes = { "node1" }
                            }
                        }, nil
                    end,
                    collect_results = function(_n: any, _iteration: any)
                        return "large_result", nil
                    end
                }

                local mock_funcs = {
                    new = function()
                        return {
                            with_context = function(self: any, _context: any)
                                return self
                            end,
                            call = function(_self: any, _func_id: string, _data: any)
                                return nil, "Item transform failed"
                            end
                        }
                    end
                }

                parallel._deps.node = {
                    new = function(_args: any)
                        return mock_node, nil
                    end
                }

                parallel._deps.template_graph = {
                    build_for_node = function(_node: any)
                        return mock_template_graph, nil
                    end
                }

                parallel._deps.iterator = mock_iterator
                parallel._deps.funcs = mock_funcs

                local result = parallel.run({})
                test.is_true(result.success)
                test.eq(#result.result, 1)
                test.contains(result.result[1].error, "Item pipeline failed")
            end)

            it("preserves a structured template failure in the collected item result", function()
                local child_failure = {
                    code = "MISSING_CUSTOMER_ID",
                    message = "customer_id is required for item bluebird"
                }
                local mock_node = {
                    config = function(_self: any)
                        return {
                            source_array_key = "items",
                            on_error = parallel.ON_ERROR_STRATEGIES.CONTINUE
                        }
                    end,
                    inputs = function(_self: any)
                        return {
                            default = {
                                content = { items = { "bluebird" } }
                            }
                        }
                    end,
                    yield = function(_self: any, _options: any)
                        return {}, nil
                    end,
                    complete = function(_self: any, result: any, message: string?)
                        return {
                            success = true,
                            result = result,
                            message = message
                        }
                    end
                }

                parallel._deps.node = {
                    new = function(_args: any)
                        return mock_node, nil
                    end
                }
                parallel._deps.template_graph = {
                    build_for_node = function(_node: any)
                        return {
                            is_empty = function()
                                return false
                            end
                        }, nil
                    end
                }
                parallel._deps.iterator = {
                    create_batch = function(_n: any, _template_graph: any, items: any)
                        return {
                            {
                                iteration = 1,
                                iteration_index = 1,
                                input_item = items[1],
                                root_nodes = { "template-item-1" }
                            }
                        }, nil
                    end,
                    collect_results = function()
                        return nil, child_failure
                    end
                }

                local result = parallel.run({})

                test.is_true(result.success)
                test.eq(#result.result, 1)
                test.is_table(result.result[1].error)
                test.eq(result.result[1].error.code, child_failure.code)
                test.eq(result.result[1].error.message, child_failure.message)
            end)

            it("preserves a structured template failure rebuilt from durable iteration data", function()
                local child_failure = {
                    code = "MISSING_CUSTOMER_ID",
                    message = "customer_id is required for item bluebird"
                }
                local mock_node = {
                    node_id = "parallel-node",
                    config = function(_self: any)
                        return { source_array_key = "items" }
                    end,
                    inputs = function(_self: any)
                        return {
                            default = {
                                content = { items = { "bluebird" } }
                            }
                        }
                    end,
                    query = function(_self: any)
                        return {
                            with_nodes = function(self: any, _node_ids: any)
                                return self
                            end,
                            with_data_types = function(self: any, _types: any)
                                return self
                            end,
                            all = function()
                                return {
                                    {
                                        type = consts.DATA_TYPE.ITERATION_ERROR,
                                        metadata = { iteration = 1 },
                                        content = { error = child_failure }
                                    }
                                }, nil
                            end
                        }
                    end,
                    complete = function(_self: any, result: any, message: string?)
                        return {
                            success = true,
                            result = result,
                            message = message
                        }
                    end
                }

                parallel._deps.node = {
                    new = function(_args: any)
                        return mock_node, nil
                    end
                }
                parallel._deps.template_graph = {
                    build_for_node = function(_node: any)
                        return {
                            is_empty = function()
                                return false
                            end
                        }, nil
                    end
                }

                local result = parallel.run({})

                test.is_true(result.success)
                test.eq(result.result.failure_count, 1)
                test.is_table(result.result.failures[1].error)
                test.eq(result.result.failures[1].error.code, child_failure.code)
                test.eq(result.result.failures[1].error.message, child_failure.message)
            end)

            it("should support multi-step item pipelines", function()
                local step_calls: {string} = {}

                local mock_node = {
                    config = function(_self: any)
                        return {
                            source_array_key = "items",
                            item_steps = {
                                { type = "map",    func_id = "step1" },
                                { type = "filter", func_id = "step2" },
                                { type = "map",    func_id = "step3" }
                            }
                        }
                    end,
                    inputs = function(_self: any)
                        return {
                            default = {
                                content = { items = { "a" } }
                            }
                        }
                    end,
                    yield = function(_self: any, _options: any)
                        return {}, nil
                    end,
                    complete = function(_self: any, result: any, message: string?)
                        return {
                            success = true,
                            result = result,
                            message = message
                        }
                    end
                }

                local mock_template_graph = {
                    is_empty = function(_self: any)
                        return false
                    end
                }

                local mock_iterator = {
                    create_batch = function(_n: any, _template_graph: any, _items: any, _batch_start: number, _batch_end: number, _iteration_input_key: string)
                        return {
                            {
                                iteration_index = 1,
                                input_item = "a",
                                root_nodes = { "node1" }
                            }
                        }, nil
                    end,
                    collect_results = function(_n: any, _iteration: any)
                        return "original", nil
                    end
                }

                local mock_funcs = {
                    new = function()
                        return {
                            with_context = function(self: any, _context: any)
                                return self
                            end,
                            call = function(_self: any, func_id: string, data: any)
                                table.insert(step_calls, func_id)
                                if func_id == "step1" then
                                    return "transformed", nil
                                elseif func_id == "step2" then
                                    return true, nil
                                elseif func_id == "step3" then
                                    return "final", nil
                                end
                            end
                        }
                    end
                }

                parallel._deps.node = {
                    new = function(_args: any)
                        return mock_node, nil
                    end
                }

                parallel._deps.template_graph = {
                    build_for_node = function(_node: any)
                        return mock_template_graph, nil
                    end
                }

                parallel._deps.iterator = mock_iterator
                parallel._deps.funcs = mock_funcs

                local result = parallel.run({})
                test.is_true(result.success)
                test.eq(#step_calls, 3)
                test.eq(step_calls[1], "step1")
                test.eq(step_calls[2], "step2")
                test.eq(step_calls[3], "step3")
                test.eq(result.result[1].result, "final")
            end)

            it("should handle item filtering correctly", function()
                local mock_node = {
                    config = function(_self: any)
                        return {
                            source_array_key = "items",
                            item_steps = {
                                { type = "filter", func_id = "filter_out_item" }
                            }
                        }
                    end,
                    inputs = function(_self: any)
                        return {
                            default = {
                                content = { items = { "a", "b" } }
                            }
                        }
                    end,
                    yield = function(_self: any, _options: any)
                        return {}, nil
                    end,
                    complete = function(_self: any, result: any, message: string?)
                        return {
                            success = true,
                            result = result,
                            message = message
                        }
                    end
                }

                local mock_template_graph = {
                    is_empty = function(_self: any)
                        return false
                    end
                }

                local mock_iterator = {
                    create_batch = function(_n: any, _template_graph: any, items: any, batch_start: number, _batch_end: number, _iteration_input_key: string)
                        return {
                            {
                                iteration_index = batch_start,
                                input_item = items[batch_start],
                                root_nodes = { "node1" }
                            }
                        }, nil
                    end,
                    collect_results = function(_n: any, iteration: any)
                        return "result_" .. iteration.iteration_index, nil
                    end
                }

                local mock_funcs = {
                    new = function()
                        return {
                            with_context = function(self: any, _context: any)
                                return self
                            end,
                            call = function(_self: any, _func_id: string, data: any)
                                return data ~= "result_1", nil
                            end
                        }
                    end
                }

                parallel._deps.node = {
                    new = function(_args: any)
                        return mock_node, nil
                    end
                }

                parallel._deps.template_graph = {
                    build_for_node = function(_node: any)
                        return mock_template_graph, nil
                    end
                }

                parallel._deps.iterator = mock_iterator
                parallel._deps.funcs = mock_funcs

                local result = parallel.run({})
                test.is_true(result.success)
                test.eq(#result.result, 1)
                test.eq(result.result[1].result, "result_2")
            end)
        end)

        describe("Unit Tests - Reduction Functionality", function()
            it("should apply reducer when configured", function()
                local reducer_called: boolean = false
                local reducer_input: any = nil

                local mock_node = {
                    config = function(_self: any)
                        return {
                            source_array_key = "items",
                            reduction_extract = "successes",
                            reduction_steps = {
                                { type = "aggregate", func_id = "test_reducer" }
                            }
                        }
                    end,
                    inputs = function(_self: any)
                        return {
                            default = {
                                content = { items = { "a" } }
                            }
                        }
                    end,
                    yield = function(_self: any, _options: any)
                        return {}, nil
                    end,
                    complete = function(_self: any, result: any, message: string?)
                        return {
                            success = true,
                            result = result,
                            message = message
                        }
                    end
                }

                local mock_template_graph = {
                    is_empty = function(_self: any)
                        return false
                    end
                }

                local mock_iterator = {
                    create_batch = function(_n: any, _template_graph: any, _items: any, _batch_start: number, _batch_end: number, _iteration_input_key: string)
                        return {
                            {
                                iteration_index = 1,
                                input_item = "a",
                                root_nodes = { "node1" }
                            }
                        }, nil
                    end,
                    collect_results = function(_n: any, _iteration: any)
                        return "result_a", nil
                    end
                }

                local mock_funcs = {
                    new = function()
                        return {
                            with_context = function(self: any, _context: any)
                                return self
                            end,
                            call = function(_self: any, _func_id: string, data: any)
                                reducer_called = true
                                reducer_input = data
                                return "reduced_result", nil
                            end
                        }
                    end
                }

                parallel._deps.node = {
                    new = function(_args: any)
                        return mock_node, nil
                    end
                }

                parallel._deps.template_graph = {
                    build_for_node = function(_node: any)
                        return mock_template_graph, nil
                    end
                }

                parallel._deps.iterator = mock_iterator
                parallel._deps.funcs = mock_funcs

                local result = parallel.run({})
                test.is_true(result.success)
                test.is_true(reducer_called)
                test.not_nil(reducer_input)
                test.eq(result.result, "reduced_result")
            end)

            it("should extract successes and process with pipeline", function()
                local pipeline_calls = {} :: {any}

                local mock_node = {
                    config = function(_self: any)
                        return {
                            source_array_key = "items",
                            reduction_extract = "successes",
                            reduction_steps = {
                                { type = "map",       func_id = "extract_score" },
                                { type = "aggregate", func_id = "sum_scores" }
                            }
                        }
                    end,
                    inputs = function(_self: any)
                        return {
                            default = {
                                content = { items = { "a", "b" } }
                            }
                        }
                    end,
                    yield = function(_self: any, _options: any)
                        return {}, nil
                    end,
                    complete = function(_self: any, result: any, message: string?)
                        return {
                            success = true,
                            result = result,
                            message = message
                        }
                    end
                }

                local mock_template_graph = {
                    is_empty = function(_self: any)
                        return false
                    end
                }

                local mock_iterator = {
                    create_batch = function(_n: any, _template_graph: any, items: any, batch_start: number, _batch_end: number, _iteration_input_key: string)
                        return {
                            {
                                iteration_index = batch_start,
                                input_item = items[batch_start],
                                root_nodes = { "node1" }
                            }
                        }, nil
                    end,
                    collect_results = function(_n: any, iteration: any)
                        return { user_id = iteration.iteration_index, score = iteration.iteration_index * 10 }, nil
                    end
                }

                local mock_funcs = {
                    new = function()
                        return {
                            with_context = function(self: any, _context: any)
                                return self
                            end,
                            call = function(_self: any, func_id: string, data: any)
                                table.insert(pipeline_calls, { func_id = func_id, data_type = type(data) })

                                if func_id == "extract_score" then
                                    return data.score, nil
                                elseif func_id == "sum_scores" then
                                    local total = 0
                                    for _, score in ipairs(data) do
                                        total = total + score
                                    end
                                    return { total_score = total, count = #data }, nil
                                end
                            end
                        }
                    end
                }

                parallel._deps.node = {
                    new = function(_args: any)
                        return mock_node, nil
                    end
                }

                parallel._deps.template_graph = {
                    build_for_node = function(_node: any)
                        return mock_template_graph, nil
                    end
                }

                parallel._deps.iterator = mock_iterator
                parallel._deps.funcs = mock_funcs

                local result = parallel.run({})
                test.is_true(result.success)
                test.eq(#pipeline_calls, 3)
                test.eq(pipeline_calls[1].func_id, "extract_score")
                test.eq(pipeline_calls[2].func_id, "extract_score")
                test.eq(pipeline_calls[3].func_id, "sum_scores")
                test.eq(result.result.total_score, 30)
                test.eq(result.result.count, 2)
            end)
        end)

        describe("Unit Tests - Pipeline Functions", function()
            it("should validate item pipeline steps correctly", function()
                local valid_steps = {
                    { type = "map",    func_id = "test_func" },
                    { type = "filter", func_id = "test_func" }
                }

                for _, step in ipairs(valid_steps) do
                    local valid, err = parallel.validate_item_pipeline_step(step)
                    test.is_true(valid)
                    test.is_nil(err)
                end

                local invalid_steps = {
                    nil,
                    {},
                    { func_id = "test" },
                    { type = "invalid_type", func_id = "test" },
                    { type = "map" },
                    { type = "group",        func_id = "test" },
                }

                for _, step in ipairs(invalid_steps) do
                    local valid, err = parallel.validate_item_pipeline_step(step)
                    test.is_false(valid)
                    test.not_nil(err)
                end
            end)

            it("should validate reduction pipeline flow correctly", function()
                local valid, err = parallel.validate_reduction_pipeline_flow("successes", {
                    { type = "map", func_id = "test" }
                })
                test.is_true(valid)
                test.is_nil(err)

                valid, err = parallel.validate_reduction_pipeline_flow("failures", {
                    { type = "filter", func_id = "test" }
                })
                test.is_true(valid)
                test.is_nil(err)

                valid, err = parallel.validate_reduction_pipeline_flow("successes", {
                    { type = "aggregate", func_id = "test" }
                })
                test.is_true(valid)
                test.is_nil(err)
            end)

            it("should execute item pipeline map step correctly", function()
                local call_data: any = nil
                parallel._deps.funcs = {
                    new = function()
                        return {
                            with_context = function(self: any, _context: any)
                                return self
                            end,
                            call = function(_self: any, _func_id: string, data: any)
                                call_data = data
                                return "transformed_" .. data, nil
                            end
                        }
                    end
                }

                local step = { type = "map", func_id = "transform" }
                local data: string = "single_value"

                local result, err = parallel.execute_item_pipeline_step(step, data)
                test.is_nil(err)
                test.eq(result, "transformed_single_value")
                test.eq(call_data, "single_value")
            end)

            it("should execute reduction pipeline map step correctly", function()
                local calls: {any} = {}
                local call_count: number = 0

                parallel._deps.funcs = {
                    new = function()
                        return {
                            with_context = function(self: any, _context: any)
                                return self
                            end,
                            call = function(_self: any, _func_id: string, data: any)
                                call_count = call_count + 1
                                table.insert(calls, data)
                                return "transformed_" .. data, nil
                            end
                        }
                    end
                }

                local step = { type = "map", func_id = "transform" }
                local data = { "a", "b", "c" }

                local result: any, err = parallel.execute_reduction_pipeline_step(step, data)
                test.is_nil(err)
                test.eq(#result, 3)
                test.eq((result :: any)[1], "transformed_a")
                test.eq((result :: any)[2], "transformed_b")
                test.eq((result :: any)[3], "transformed_c")
                test.eq(call_count, 3)
            end)

            it("should execute reduction pipeline group step correctly", function()
                parallel._deps.funcs = {
                    new = function()
                        return {
                            with_context = function(self: any, _context: any)
                                return self
                            end,
                            call = function(_self: any, _func_id: string, item: any)
                                return item.category, nil
                            end
                        }
                    end
                }

                local step = { type = "group", key_func_id = "get_category" }
                local data = {
                    { category = "A", value = 1 },
                    { category = "B", value = 2 },
                    { category = "A", value = 3 }
                }

                local result: any, err = parallel.execute_reduction_pipeline_step(step, data)
                test.is_nil(err)
                test.not_nil((result :: any)["A"])
                test.not_nil((result :: any)["B"])
                test.eq(#(result :: any)["A"], 2)
                test.eq(#(result :: any)["B"], 1)
            end)
        end)

        describe("Unit Tests - Per-Step Context Functionality", function()
            it("should validate per-step context in item steps", function()
                local step_with_invalid_context = {
                    type = "map",
                    func_id = "test_func",
                    context = "invalid_context"
                }

                local valid, err = parallel.validate_item_pipeline_step(step_with_invalid_context)
                test.is_false(valid)
                test.contains(err, "context must be a table")
            end)

            it("should validate per-step context in reduction steps", function()
                local step_with_invalid_context = {
                    type = "map",
                    func_id = "test_func",
                    context = 123
                }

                local valid, err = parallel.validate_reduction_pipeline_step(step_with_invalid_context, "array")
                test.is_false(valid)
                test.contains(err, "context must be a table")
            end)

            it("should execute item step with per-step context", function()
                local context_received: any = nil
                local data_received: any = nil

                parallel._deps.funcs = {
                    new = function()
                        return {
                            with_context = function(self: any, context: any)
                                context_received = context
                                return self
                            end,
                            call = function(_self: any, _func_id: string, data: any)
                                data_received = data
                                return "transformed_" .. data, nil
                            end
                        }
                    end
                }

                local step = {
                    type = "map",
                    func_id = "transform_data",
                    context = {
                        transform_mode = "aggressive",
                        preserve_type = true
                    }
                }

                local result, err = parallel.execute_item_pipeline_step(step, "test_data")

                test.is_nil(err)
                test.eq(result, "transformed_test_data")
                test.not_nil(context_received)
                test.eq(context_received.transform_mode, "aggressive")
                test.is_true(context_received.preserve_type)
                test.eq(data_received, "test_data")
            end)

            it("should execute reduction step with per-step context", function()
                local context_received: any = nil

                parallel._deps.funcs = {
                    new = function()
                        return {
                            with_context = function(self: any, context: any)
                                context_received = context
                                return self
                            end,
                            call = function(_self: any, _func_id: string, data: any)
                                return { aggregated = data }, nil
                            end
                        }
                    end
                }

                local step = {
                    type = "aggregate",
                    func_id = "aggregate_data",
                    context = {
                        aggregation_method = "sum",
                        include_metadata = true
                    }
                }

                local result: any, err = parallel.execute_reduction_pipeline_step(step, { "value1", "value2" })

                test.is_nil(err)
                test.not_nil((result :: any).aggregated)
                test.not_nil(context_received)
                test.eq(context_received.aggregation_method, "sum")
                test.is_true(context_received.include_metadata)
            end)

            it("should work without per-step context", function()
                local context_received: any = nil

                parallel._deps.funcs = {
                    new = function()
                        return {
                            with_context = function(self: any, context: any)
                                context_received = context
                                return self
                            end,
                            call = function(_self: any, _func_id: string, data: any)
                                return "processed_" .. data, nil
                            end
                        }
                    end
                }

                local step = {
                    type = "map",
                    func_id = "process_data"
                }

                local result, err = parallel.execute_item_pipeline_step(step, "test_data")

                test.is_nil(err)
                test.eq(result, "processed_test_data")
                test.is_nil(context_received)
            end)

            it("should merge context correctly in reduction map steps", function()
                local contexts_received: {any} = {}

                parallel._deps.funcs = {
                    new = function()
                        return {
                            with_context = function(self: any, context: any)
                                table.insert(contexts_received, context)
                                return self
                            end,
                            call = function(_self: any, _func_id: string, data: any)
                                return "processed_" .. data, nil
                            end
                        }
                    end
                }

                local step = {
                    type = "map",
                    func_id = "process",
                    context = {
                        global_setting = "test",
                        step_setting = "map_specific"
                    }
                }
                local data = { "item1", "item2" }

                local result, err = parallel.execute_reduction_pipeline_step(step, data)
                test.is_nil(err)
                test.eq(#contexts_received, 2)

                for i, context in ipairs(contexts_received) do
                    test.eq(context.global_setting, "test")
                    test.eq(context.step_setting, "map_specific")
                    test.eq(context.current_item, data[i])
                    test.eq(context.item_index, i)
                end
            end)
        end)
    end)

    describe("Integration Tests", function()
            describe("Basic Parallel Workflow", function()
                it("should execute simple map-reduce with multiple items", function()
                    print("=== BASIC MAP-REDUCE INTEGRATION TEST START ===")

                    local c, err = client.new()
                    test.is_nil(err)
                    test.not_nil(c)

                    local parallel_node_id: string = uuid.v7()
                    local template_node_id: string = uuid.v7()
                    local input_data_id: string = uuid.v7()
                    local node_input_id: string = uuid.v7()

                    local test_input = {
                        items = {
                            { message = "Process item A", value = 10, delay_ms = 50 },
                            { message = "Process item B", value = 20, delay_ms = 50 },
                            { message = "Process item C", value = 30, delay_ms = 50 }
                        }
                    }

                    local workflow_commands = {
                        {
                            type = consts.COMMAND_TYPES.CREATE_NODE,
                            payload = {
                                node_id = parallel_node_id,
                                node_type = "userspace.dataflow.node.parallel:parallel",
                                status = consts.STATUS.PENDING,
                                config = {
                                    source_array_key = "items",
                                    iteration_input_key = "default",
                                    batch_size = 1,
                                    on_error = "continue",
                                    data_targets = {
                                        {
                                            data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                            key = "parallel_result",
                                            content_type = consts.CONTENT_TYPE.JSON
                                        }
                                    }
                                },
                                metadata = {
                                    title = "Basic Parallel Node"
                                }
                            }
                        },
                        {
                            type = consts.COMMAND_TYPES.CREATE_NODE,
                            payload = {
                                node_id = template_node_id,
                                node_type = "userspace.dataflow.node.func:node",
                                parent_node_id = parallel_node_id,
                                status = consts.STATUS.TEMPLATE,
                                config = {
                                    func_id = "userspace.dataflow.node.func:test_func",
                                    data_targets = {
                                        {
                                            data_type = consts.DATA_TYPE.NODE_OUTPUT,
                                            key = "processed_result"
                                        }
                                    }
                                },
                                metadata = {
                                    title = "Basic Parallel Template Node"
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
                                node_id = parallel_node_id,
                                key = input_data_id,
                                discriminator = "default",
                                content = "",
                                content_type = "dataflow/reference"
                            }
                        }
                    }

                    local dataflow_id, create_err = c:create_workflow(workflow_commands, {
                        metadata = {
                            title = "Basic Parallel Integration Test Workflow"
                        }
                    })
                    test.is_nil(create_err)
                    test.not_nil(dataflow_id)
                    print("Basic map-reduce workflow created:", dataflow_id)

                    local result, exec_err = c:execute(dataflow_id)
                    test.is_nil(exec_err)
                    test.not_nil(result)
                    test.is_true(result.success)
                    print("Basic map-reduce workflow executed successfully")

                    local output_data = data_reader.with_dataflow(dataflow_id)
                        :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                        :with_data_keys("parallel_result")
                        :fetch_options({ replace_references = true })
                        :one()

                    test.not_nil(output_data)

                    local output_content: any = output_data.content
                    if type(output_content) == "string" then
                        local decoded, decode_err = json.decode(output_content)
                        if not decode_err then
                            output_content = decoded
                        end
                    end

                    test.eq(#output_content, 3)

                    for i, entry in ipairs(output_content) do
                        test.eq(entry.iteration, i)
                        test.not_nil(entry.item)
                        test.contains(entry.item.message, "Process item")
                        test.not_nil(entry.result)

                        local parsed_result: any = entry.result
                        if type(entry.result) == "string" then
                            local decoded, decode_err = json.decode(entry.result)
                            if not decode_err then
                                parsed_result = decoded
                            end
                        end

                        test.eq(parsed_result.processed_by, "test_function")
                        test.is_true(parsed_result.success)
                        print("  Iteration", i, "processed:", entry.item.message)
                    end

                    print("=== BASIC MAP-REDUCE INTEGRATION TEST COMPLETE ===")
                end)

                it("should handle batch processing correctly", function()
                    print("=== BATCH PROCESSING TEST START ===")

                    local c, err = client.new()
                    test.is_nil(err)

                    local parallel_node_id: string = uuid.v7()
                    local template_node_id: string = uuid.v7()
                    local input_data_id: string = uuid.v7()
                    local node_input_id: string = uuid.v7()

                    local test_input = {
                        items = {
                            { message = "Batch item 1", value = 1 },
                            { message = "Batch item 2", value = 2 },
                            { message = "Batch item 3", value = 3 },
                            { message = "Batch item 4", value = 4 },
                            { message = "Batch item 5", value = 5 }
                        }
                    }

                    local workflow_commands = {
                        {
                            type = consts.COMMAND_TYPES.CREATE_NODE,
                            payload = {
                                node_id = parallel_node_id,
                                node_type = "userspace.dataflow.node.parallel:parallel",
                                status = consts.STATUS.PENDING,
                                config = {
                                    source_array_key = "items",
                                    batch_size = 2,
                                    on_error = "continue",
                                    data_targets = {
                                        {
                                            data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                            key = "batched_result"
                                        }
                                    }
                                },
                                metadata = {
                                    title = "Batch Processing Parallel Node"
                                }
                            }
                        },
                        {
                            type = consts.COMMAND_TYPES.CREATE_NODE,
                            payload = {
                                node_id = template_node_id,
                                node_type = "userspace.dataflow.node.func:node",
                                parent_node_id = parallel_node_id,
                                status = consts.STATUS.TEMPLATE,
                                config = {
                                    func_id = "userspace.dataflow.node.func:test_func",
                                    data_targets = {
                                        {
                                            data_type = consts.DATA_TYPE.NODE_OUTPUT,
                                            key = "batch_result"
                                        }
                                    }
                                },
                                metadata = {
                                    title = "Batch Processing Template Node"
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
                                node_id = parallel_node_id,
                                key = input_data_id,
                                discriminator = "default",
                                content = "",
                                content_type = "dataflow/reference"
                            }
                        }
                    }

                    local dataflow_id, create_err = c:create_workflow(workflow_commands, {
                        metadata = {
                            title = "Batch Processing Test Workflow"
                        }
                    })
                    test.is_nil(create_err)

                    local result, exec_err = c:execute(dataflow_id)
                    test.is_nil(exec_err)
                    test.is_true(result.success)

                    local output_data = data_reader.with_dataflow(dataflow_id)
                        :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                        :with_data_keys("batched_result")
                        :fetch_options({ replace_references = true })
                        :one()

                    test.not_nil(output_data)

                    local output_content: any = output_data.content
                    if type(output_content) == "string" then
                        local decoded, decode_err = json.decode(output_content)
                        if not decode_err then
                            output_content = decoded
                        end
                    end

                    test.eq(#output_content, 5)

                    print("Batch processing completed with", #output_content, "successes")
                    print("=== BATCH PROCESSING TEST COMPLETE ===")
                end)
            end)

            describe("Item Pipeline Integration", function()
                it("should apply item pipeline to compress iteration results", function()
                    print("=== ITEM PIPELINE COMPRESS TEST START ===")

                    local c, err = client.new()
                    test.is_nil(err)

                    local parallel_node_id: string = uuid.v7()
                    local template_node_id: string = uuid.v7()
                    local input_data_id: string = uuid.v7()
                    local node_input_id: string = uuid.v7()

                    local test_input = {
                        items = {
                            { message = "Item 1", value = 100 },
                            { message = "Item 2", value = 200 }
                        }
                    }

                    local workflow_commands = {
                        {
                            type = consts.COMMAND_TYPES.CREATE_NODE,
                            payload = {
                                node_id = parallel_node_id,
                                node_type = "userspace.dataflow.node.parallel:parallel",
                                status = consts.STATUS.PENDING,
                                config = {
                                    source_array_key = "items",
                                    on_error = "continue",
                                    item_steps = {
                                        {
                                            type = "map",
                                            func_id = "userspace.dataflow.node.parallel.stub:compress_item",
                                            context = {
                                                extract_only = true
                                            }
                                        }
                                    },
                                    data_targets = {
                                        {
                                            data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                            key = "compressed_result"
                                        }
                                    }
                                },
                                metadata = {
                                    title = "Item Pipeline Compress Node"
                                }
                            }
                        },
                        {
                            type = consts.COMMAND_TYPES.CREATE_NODE,
                            payload = {
                                node_id = template_node_id,
                                node_type = "userspace.dataflow.node.func:node",
                                parent_node_id = parallel_node_id,
                                status = consts.STATUS.TEMPLATE,
                                config = {
                                    func_id = "userspace.dataflow.node.func:test_func",
                                    data_targets = {
                                        {
                                            data_type = consts.DATA_TYPE.NODE_OUTPUT,
                                            key = "result"
                                        }
                                    }
                                },
                                metadata = {
                                    title = "Item Pipeline Compress Template Node"
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
                                node_id = parallel_node_id,
                                key = input_data_id,
                                discriminator = "default",
                                content = "",
                                content_type = "dataflow/reference"
                            }
                        }
                    }

                    local dataflow_id, create_err = c:create_workflow(workflow_commands, {
                        metadata = {
                            title = "Item Pipeline Compress Test Workflow"
                        }
                    })
                    test.is_nil(create_err)

                    local result, exec_err = c:execute(dataflow_id)
                    test.is_nil(exec_err)
                    test.is_true(result.success)

                    local output_data = data_reader.with_dataflow(dataflow_id)
                        :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                        :with_data_keys("compressed_result")
                        :fetch_options({ replace_references = true })
                        :one()

                    test.not_nil(output_data)

                    local output_content: any = output_data.content
                    if type(output_content) == "string" then
                        local decoded, decode_err = json.decode(output_content)
                        if not decode_err then
                            output_content = decoded
                        end
                    end

                    test.eq(#output_content, 2)

                    for _, entry in ipairs(output_content) do
                        test.eq(entry.result.compressed_by, "compress_item")
                        test.not_nil(entry.result.original_data)
                        print("Item compressed:", entry.item.message)
                    end

                    print("=== ITEM PIPELINE COMPRESS TEST COMPLETE ===")
                end)

                it("should apply item pipeline validation filter", function()
                    print("=== ITEM PIPELINE VALIDATION TEST START ===")

                    local c, err = client.new()
                    test.is_nil(err)

                    local parallel_node_id: string = uuid.v7()
                    local template_node_id: string = uuid.v7()
                    local input_data_id: string = uuid.v7()
                    local node_input_id: string = uuid.v7()

                    local test_input = {
                        items = {
                            { message = "Good item",         value = 50 },
                            { message = "Bad item",          value = 5 },
                            { message = "Another good item", value = 75 }
                        }
                    }

                    local workflow_commands = {
                        {
                            type = consts.COMMAND_TYPES.CREATE_NODE,
                            payload = {
                                node_id = parallel_node_id,
                                node_type = "userspace.dataflow.node.parallel:parallel",
                                status = consts.STATUS.PENDING,
                                config = {
                                    source_array_key = "items",
                                    on_error = "continue",
                                    item_steps = {
                                        {
                                            type = "filter",
                                            func_id = "userspace.dataflow.node.parallel.stub:validate_item",
                                            context = {
                                                validation_mode = "value_check",
                                                min_value = 20
                                            }
                                        }
                                    },
                                    data_targets = {
                                        {
                                            data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                            key = "filtered_result"
                                        }
                                    }
                                },
                                metadata = {
                                    title = "Item Pipeline Validation Node"
                                }
                            }
                        },
                        {
                            type = consts.COMMAND_TYPES.CREATE_NODE,
                            payload = {
                                node_id = template_node_id,
                                node_type = "userspace.dataflow.node.func:node",
                                parent_node_id = parallel_node_id,
                                status = consts.STATUS.TEMPLATE,
                                config = {
                                    func_id = "userspace.dataflow.node.func:test_func",
                                    data_targets = {
                                        {
                                            data_type = consts.DATA_TYPE.NODE_OUTPUT,
                                            key = "result"
                                        }
                                    }
                                },
                                metadata = {
                                    title = "Item Pipeline Validation Template Node"
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
                                node_id = parallel_node_id,
                                key = input_data_id,
                                discriminator = "default",
                                content = "",
                                content_type = "dataflow/reference"
                            }
                        }
                    }

                    local dataflow_id, create_err = c:create_workflow(workflow_commands, {
                        metadata = {
                            title = "Item Pipeline Validation Test Workflow"
                        }
                    })
                    test.is_nil(create_err)

                    local result, exec_err = c:execute(dataflow_id)
                    test.is_nil(exec_err)
                    test.is_true(result.success)

                    local output_data = data_reader.with_dataflow(dataflow_id)
                        :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                        :with_data_keys("filtered_result")
                        :fetch_options({ replace_references = true })
                        :one()

                    test.not_nil(output_data)

                    local output_content: any = output_data.content
                    if type(output_content) == "string" then
                        local decoded, decode_err = json.decode(output_content)
                        if not decode_err then
                            output_content = decoded
                        end
                    end

                    test.eq(#output_content, 2)

                    for _, entry in ipairs(output_content) do
                        test.gte(entry.item.value, 20)
                        print("Item passed filter:", entry.item.message, "value:", entry.item.value)
                    end

                    print("=== ITEM PIPELINE VALIDATION TEST COMPLETE ===")
                end)
            end)

            describe("Reduction Pipeline Integration", function()
                it("should apply reduction pipeline with aggregation", function()
                    print("=== REDUCTION PIPELINE TEST START ===")

                    local c, err = client.new()
                    test.is_nil(err)

                    local parallel_node_id: string = uuid.v7()
                    local template_node_id: string = uuid.v7()
                    local input_data_id: string = uuid.v7()
                    local node_input_id: string = uuid.v7()

                    local test_input = {
                        items = {
                            { message = "Item 1", value = 10 },
                            { message = "Item 2", value = 20 },
                            { message = "Item 3", value = 30 }
                        }
                    }

                    local workflow_commands = {
                        {
                            type = consts.COMMAND_TYPES.CREATE_NODE,
                            payload = {
                                node_id = parallel_node_id,
                                node_type = "userspace.dataflow.node.parallel:parallel",
                                status = consts.STATUS.PENDING,
                                config = {
                                    source_array_key = "items",
                                    on_error = "continue",
                                    reduction_extract = "successes",
                                    reduction_steps = {
                                        {
                                            type = "map",
                                            func_id = "userspace.dataflow.node.parallel.stub:extract_number",
                                            context = {
                                                field = "value"
                                            }
                                        },
                                        {
                                            type = "aggregate",
                                            func_id = "userspace.dataflow.node.parallel.stub:calculate_stats"
                                        }
                                    },
                                    data_targets = {
                                        {
                                            data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                            key = "reduced_result"
                                        }
                                    }
                                },
                                metadata = {
                                    title = "Reduction Pipeline Aggregation Node"
                                }
                            }
                        },
                        {
                            type = consts.COMMAND_TYPES.CREATE_NODE,
                            payload = {
                                node_id = template_node_id,
                                node_type = "userspace.dataflow.node.func:node",
                                parent_node_id = parallel_node_id,
                                status = consts.STATUS.TEMPLATE,
                                config = {
                                    func_id = "userspace.dataflow.node.func:test_func",
                                    data_targets = {
                                        {
                                            data_type = consts.DATA_TYPE.NODE_OUTPUT,
                                            key = "result"
                                        }
                                    }
                                },
                                metadata = {
                                    title = "Reduction Pipeline Aggregation Template Node"
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
                                node_id = parallel_node_id,
                                key = input_data_id,
                                discriminator = "default",
                                content = "",
                                content_type = "dataflow/reference"
                            }
                        }
                    }

                    local dataflow_id, create_err = c:create_workflow(workflow_commands, {
                        metadata = {
                            title = "Reduction Pipeline Aggregation Test Workflow"
                        }
                    })
                    test.is_nil(create_err)

                    local result, exec_err = c:execute(dataflow_id)
                    test.is_nil(exec_err)
                    test.is_true(result.success)

                    local output_data = data_reader.with_dataflow(dataflow_id)
                        :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                        :with_data_keys("reduced_result")
                        :fetch_options({ replace_references = true })
                        :one()

                    test.not_nil(output_data)

                    local output_content: any = output_data.content
                    if type(output_content) == "string" then
                        local decoded, decode_err = json.decode(output_content)
                        if not decode_err then
                            output_content = decoded
                        end
                    end

                    test.eq(output_content.sum, 60)
                    test.eq(output_content.count, 3)
                    test.eq(output_content.calculated_by, "calculate_stats")

                    print("Reduction pipeline applied:")
                    print("  Total value:", output_content.sum)
                    print("  Item count:", output_content.count)
                    print("=== REDUCTION PIPELINE TEST COMPLETE ===")
                end)

                it("should apply reduction pipeline with extract and aggregate", function()
                    print("=== ADVANCED REDUCTION PIPELINE TEST START ===")

                    local c, err = client.new()
                    test.is_nil(err)

                    local parallel_node_id: string = uuid.v7()
                    local template_node_id: string = uuid.v7()
                    local input_data_id: string = uuid.v7()
                    local node_input_id: string = uuid.v7()

                    local test_input = {
                        items = {
                            { message = "Score 1", value = 85 },
                            { message = "Score 2", value = 92 },
                            { message = "Score 3", value = 78 }
                        }
                    }

                    local workflow_commands = {
                        {
                            type = consts.COMMAND_TYPES.CREATE_NODE,
                            payload = {
                                node_id = parallel_node_id,
                                node_type = "userspace.dataflow.node.parallel:parallel",
                                status = consts.STATUS.PENDING,
                                config = {
                                    source_array_key = "items",
                                    on_error = "continue",
                                    reduction_extract = "successes",
                                    reduction_steps = {
                                        {
                                            type = "map",
                                            func_id = "userspace.dataflow.node.parallel.stub:extract_number",
                                            context = {
                                                field = "value"
                                            }
                                        },
                                        {
                                            type = "aggregate",
                                            func_id = "userspace.dataflow.node.parallel.stub:calculate_stats",
                                            context = {
                                                include_min_max = true
                                            }
                                        },
                                        {
                                            type = "aggregate",
                                            func_id = "userspace.dataflow.node.parallel.stub:format_report",
                                            context = {
                                                title = "Test Scores Report",
                                                style = "summary"
                                            }
                                        }
                                    },
                                    data_targets = {
                                        {
                                            data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                            key = "pipeline_result"
                                        }
                                    }
                                },
                                metadata = {
                                    title = "Advanced Reduction Pipeline Node"
                                }
                            }
                        },
                        {
                            type = consts.COMMAND_TYPES.CREATE_NODE,
                            payload = {
                                node_id = template_node_id,
                                node_type = "userspace.dataflow.node.func:node",
                                parent_node_id = parallel_node_id,
                                status = consts.STATUS.TEMPLATE,
                                config = {
                                    func_id = "userspace.dataflow.node.func:test_func",
                                    data_targets = {
                                        {
                                            data_type = consts.DATA_TYPE.NODE_OUTPUT,
                                            key = "result"
                                        }
                                    }
                                },
                                metadata = {
                                    title = "Advanced Reduction Pipeline Template Node"
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
                                node_id = parallel_node_id,
                                key = input_data_id,
                                discriminator = "default",
                                content = "",
                                content_type = "dataflow/reference"
                            }
                        }
                    }

                    local dataflow_id, create_err = c:create_workflow(workflow_commands, {
                        metadata = {
                            title = "Advanced Reduction Pipeline Test Workflow"
                        }
                    })
                    test.is_nil(create_err)

                    local result, exec_err = c:execute(dataflow_id)
                    test.is_nil(exec_err)
                    test.is_true(result.success)

                    local output_data = data_reader.with_dataflow(dataflow_id)
                        :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                        :with_data_keys("pipeline_result")
                        :fetch_options({ replace_references = true })
                        :one()

                    test.not_nil(output_data)

                    local output_content: any = output_data.content
                    if type(output_content) == "string" then
                        local decoded, decode_err = json.decode(output_content)
                        if not decode_err then
                            output_content = decoded
                        end
                    end

                    test.eq(output_content.title, "Test Scores Report")
                    test.eq(output_content.formatted_by, "format_report")
                    test.not_nil(output_content.data)

                    local stats_data = output_content.data
                    test.eq(stats_data.sum, 255)
                    test.eq(stats_data.count, 3)
                    test.eq(stats_data.average, 85)
                    test.eq(stats_data.min, 78)
                    test.eq(stats_data.max, 92)
                    test.eq(stats_data.calculated_by, "calculate_stats")

                    print("Advanced reduction pipeline completed:")
                    print("  Title:", output_content.title)
                    print("  Sum:", stats_data.sum)
                    print("  Average:", stats_data.average)
                    print("  Min:", stats_data.min)
                    print("  Max:", stats_data.max)
                    print("=== ADVANCED REDUCTION PIPELINE TEST COMPLETE ===")
                end)
            end)

            describe("Failure Handling Integration", function()
                it("should handle fail_fast strategy correctly", function()
                    print("=== FAIL_FAST INTEGRATION TEST START ===")

                    local c, err = client.new()
                    test.is_nil(err)

                    local parallel_node_id: string = uuid.v7()
                    local template_node_id: string = uuid.v7()
                    local input_data_id: string = uuid.v7()
                    local node_input_id: string = uuid.v7()

                    local test_input = {
                        items = {
                            { message = "Success item",    should_fail = false },
                            { message = "Failure item",    should_fail = true },
                            { message = "Never processed", should_fail = false }
                        }
                    }

                    local workflow_commands = {
                        {
                            type = consts.COMMAND_TYPES.CREATE_NODE,
                            payload = {
                                node_id = parallel_node_id,
                                node_type = "userspace.dataflow.node.parallel:parallel",
                                status = consts.STATUS.PENDING,
                                config = {
                                    source_array_key = "items",
                                    batch_size = 1,
                                    on_error = "fail_fast",
                                    data_targets = {
                                        {
                                            data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                            key = "fail_fast_result"
                                        }
                                    }
                                },
                                metadata = {
                                    title = "Fail Fast Strategy Node"
                                }
                            }
                        },
                        {
                            type = consts.COMMAND_TYPES.CREATE_NODE,
                            payload = {
                                node_id = template_node_id,
                                node_type = "userspace.dataflow.node.func:node",
                                parent_node_id = parallel_node_id,
                                status = consts.STATUS.TEMPLATE,
                                config = {
                                    func_id = "userspace.dataflow.node.func:test_func",
                                    data_targets = {
                                        {
                                            data_type = consts.DATA_TYPE.NODE_OUTPUT,
                                            key = "result"
                                        }
                                    }
                                },
                                metadata = {
                                    title = "Fail Fast Strategy Template Node"
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
                                node_id = parallel_node_id,
                                key = input_data_id,
                                discriminator = "default",
                                content = "",
                                content_type = "dataflow/reference"
                            }
                        }
                    }

                    local dataflow_id, create_err = c:create_workflow(workflow_commands, {
                        metadata = {
                            title = "Fail Fast Strategy Test Workflow"
                        }
                    })
                    test.is_nil(create_err)

                    local result, exec_err = c:execute(dataflow_id)
                    test.not_nil(exec_err)

                    test.is_false(result.success)
                    test.contains(result.error, "Iteration failed")

                    print("Fail_fast strategy correctly failed the workflow")
                    print("=== FAIL_FAST INTEGRATION TEST COMPLETE ===")
                end)

                it("should handle continue strategy correctly", function()
                    print("=== CONTINUE STRATEGY INTEGRATION TEST START ===")

                    local c, err = client.new()
                    test.is_nil(err)

                    local parallel_node_id: string = uuid.v7()
                    local template_node_id: string = uuid.v7()
                    local input_data_id: string = uuid.v7()
                    local node_input_id: string = uuid.v7()

                    local test_input = {
                        items = {
                            { message = "Success item 1", should_fail = false },
                            { message = "Failure item",   should_fail = true },
                            { message = "Success item 2", should_fail = false }
                        }
                    }

                    local workflow_commands = {
                        {
                            type = consts.COMMAND_TYPES.CREATE_NODE,
                            payload = {
                                node_id = parallel_node_id,
                                node_type = "userspace.dataflow.node.parallel:parallel",
                                status = consts.STATUS.PENDING,
                                config = {
                                    source_array_key = "items",
                                    batch_size = 1,
                                    on_error = "continue",
                                    data_targets = {
                                        {
                                            data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                            key = "collect_errors_result"
                                        }
                                    }
                                },
                                metadata = {
                                    title = "Collect Errors Strategy Node"
                                }
                            }
                        },
                        {
                            type = consts.COMMAND_TYPES.CREATE_NODE,
                            payload = {
                                node_id = template_node_id,
                                node_type = "userspace.dataflow.node.func:node",
                                parent_node_id = parallel_node_id,
                                status = consts.STATUS.TEMPLATE,
                                config = {
                                    func_id = "userspace.dataflow.node.func:test_func",
                                    data_targets = {
                                        {
                                            data_type = consts.DATA_TYPE.NODE_OUTPUT,
                                            key = "result"
                                        }
                                    }
                                },
                                metadata = {
                                    title = "Collect Errors Strategy Template Node"
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
                                node_id = parallel_node_id,
                                key = input_data_id,
                                discriminator = "default",
                                content = "",
                                content_type = "dataflow/reference"
                            }
                        }
                    }

                    local dataflow_id, create_err = c:create_workflow(workflow_commands, {
                        metadata = {
                            title = "Collect Errors Strategy Test Workflow"
                        }
                    })
                    test.is_nil(create_err)

                    local result, exec_err = c:execute(dataflow_id)
                    test.is_nil(exec_err)
                    test.is_true(result.success)

                    local output_data = data_reader.with_dataflow(dataflow_id)
                        :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                        :with_data_keys("collect_errors_result")
                        :fetch_options({ replace_references = true })
                        :one()

                    test.not_nil(output_data)

                    local output_content: any = output_data.content
                    if type(output_content) == "string" then
                        local decoded, decode_err = json.decode(output_content)
                        if not decode_err then
                            output_content = decoded
                        end
                    end

                    test.eq(#output_content, 3)

                    local successes = 0
                    local failures = 0
                    for _, entry in ipairs(output_content) do
                        if entry.error then
                            failures = failures + 1
                        else
                            successes = successes + 1
                        end
                    end
                    test.eq(successes, 2)
                    test.eq(failures, 1)

                    print("Continue strategy processed all items:")
                    print("  Successes:", successes)
                    print("  Failures:", failures)
                    print("=== CONTINUE STRATEGY INTEGRATION TEST COMPLETE ===")
                end)
            end)
        end)
end

return test.run_cases(define_tests)
