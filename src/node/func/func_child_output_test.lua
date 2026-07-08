local test = require("test")
local uuid = require("uuid")
local consts = require("consts")
local func = require("func")

local function child_output_row()
    return {
        data_id = "child-output-1",
        data_type = consts.DATA_TYPE.NODE_OUTPUT,
        node_id = "child-1",
        key = "result",
        content = { child = true },
        content_type = consts.CONTENT_TYPE.JSON
    }
end

local function make_reader(state)
    return {
        with_nodes = function(self: any, node_ids: any)
            state.queried_nodes = node_ids
            return self
        end,
        with_data_types = function(self: any, data_type: any)
            state.queried_data_type = data_type
            return self
        end,
        fetch_options = function(self: any, options: any)
            state.fetch_options = options
            return self
        end,
        all = function()
            state.output_query_count = state.output_query_count + 1
            if state.output_available and state.output_available(state) then
                return { child_output_row() }
            end
            return {}
        end
    }
end

local function make_async_command(result)
    local response_topic = "func-child-output-response-" .. uuid.v7()
    local response_channel = process.listen(response_topic)

    return {
        topic = response_topic,
        response_channel = response_channel,
        command = {
            response = function()
                return response_channel
            end,
            is_canceled = function()
                return false
            end,
            result = function()
                return {
                    data = function()
                        return result
                    end
                }, nil
            end,
            cancel = function()
            end
        }
    }
end

local function install_async_function(result)
    local command_pair = make_async_command(result)

    func._deps.funcs = {
        new = function()
            return {
                with_context = function(self: any)
                    return self
                end,
                async = function()
                    return command_pair.command
                end
            }
        end
    }

    process.send(process.pid(), command_pair.topic, {})
end

local function make_mock_node(state, options)
    options = options or {}

    return {
        dataflow_id = "dataflow-1",
        node_id = "func-1",
        path = {},
        config = function()
            return options.config or { func_id = "userspace.dataflow.node.func:test_func" }
        end,
        metadata = function()
            return options.metadata or {}
        end,
        inputs = function()
            return {
                default = {
                    content = options.input or { ok = true }
                }
            }
        end,
        update_metadata = function(_self: any, metadata: any)
            table.insert(state.metadata_updates, metadata)
            return true
        end,
        command = function(_self: any, cmd: any)
            table.insert(state.commands, cmd)
            return true
        end,
        yield = function(_self: any, yield_options: any)
            state.yield_calls = state.yield_calls + 1
            table.insert(state.yield_options, yield_options)
            if options.yield_error then
                return nil, options.yield_error
            end
            return {}, nil
        end,
        query = function()
            state.query_calls = state.query_calls + 1
            return make_reader(state)
        end,
        complete = function(_self: any, output: any, message: string?)
            state.completed = { output = output, message = message }
            return { success = true, result = output, message = message }
        end,
        fail = function(_self: any, err: any, message: string?)
            state.failed = { err = err, message = message }
            return { success = false, error = err, message = message }
        end
    }
end

local function make_state(output_available)
    return {
        commands = {},
        metadata_updates = {},
        yield_options = {},
        yield_calls = 0,
        query_calls = 0,
        output_query_count = 0,
        output_available = output_available
    }
end

local function define_tests()
    describe("Func Node Child Output Collection", function()
        local original_deps: any

        before_each(function()
            original_deps = {
                node = func._deps.node,
                funcs = func._deps.funcs,
                consts = func._deps.consts
            }
        end)

        after_each(function()
            func._deps.node = original_deps.node
            func._deps.funcs = original_deps.funcs
            func._deps.consts = original_deps.consts
        end)

        it("waits again when a child output is not visible after the first yield", function()
            local state = make_state(function(s)
                return s.yield_calls >= 2
            end)
            local function_result = {
                _control = {
                    commands = {
                        {
                            type = consts.COMMAND_TYPES.CREATE_NODE,
                            payload = {
                                node_id = "child-1",
                                node_type = "userspace.dataflow.node.func:node",
                                status = consts.STATUS.PENDING,
                                config = {}
                            }
                        }
                    }
                }
            }

            install_async_function(function_result)
            func._deps.node = {
                new = function()
                    return make_mock_node(state), nil
                end
            }

            local result = func.run({})

            test.is_true(result.success)
            test.eq(state.yield_calls, 2, "func re-yields existing children when output is absent")
            test.eq(result.result._dataflow_ref, "child-output-1", "func completes from child output")
        end)

        it("collects pending child output on recovery before yielding", function()
            local state = make_state(function()
                return true
            end)

            func._deps.node = {
                new = function()
                    return make_mock_node(state, {
                        metadata = {
                            func_pending = {
                                child_node_ids = { "child-1" }
                            }
                        },
                        config = {},
                        yield_error = "recovery should collect existing child output"
                    }), nil
                end
            }

            local result = func.run({})

            test.is_true(result.success)
            test.eq(state.yield_calls, 0, "recovery uses durable child output without a fresh yield")
            test.eq(result.result._dataflow_ref, "child-output-1", "recovery completes from child output")
        end)

        it("does not yield or query when the function returns no embedded children", function()
            local state = make_state(function()
                return false
            end)

            install_async_function({ ok = true })
            func._deps.node = {
                new = function()
                    return make_mock_node(state, {
                        yield_error = "plain function result should not yield"
                    }), nil
                end
            }

            local result = func.run({})

            test.is_true(result.success)
            test.eq(result.result.ok, true)
            test.eq(state.yield_calls, 0, "plain function result does not yield")
            test.eq(state.query_calls, 0, "plain function result does not query child output")
        end)
    end)
end

return test.run_cases(define_tests)
