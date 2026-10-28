local test = require("test")
local func = require("func")
local client = require("client")
local consts = require("consts")
local data_reader = require("data_reader")
local uuid = require("uuid")
local time = require("time")
local json = require("json")

local function define_tests()
    describe("Function call outcomes", function()
        local original_node
        local original_funcs
        local original_process
        local original_channel

        before_each(function()
            original_node = func._deps.node
            original_funcs = func._deps.funcs
            original_process = func._deps.process
            original_channel = func._deps.channel
        end)

        after_each(function()
            func._deps.node = original_node
            func._deps.funcs = original_funcs
            func._deps.process = original_process
            func._deps.channel = original_channel
        end)

        local function run_function(input)
            func._deps.node = {
                new = function()
                    return {
                        dataflow_id = "workflow",
                        node_id = "function",
                        config = function() return { func_id = "app:call_loop_fixture" } end,
                        metadata = function() return {} end,
                        inputs = function() return { default = { content = input } } end,
                        complete = function(_, output, message)
                            return { success = true, output = output, message = message }
                        end,
                        fail = function(_, failure, message)
                            return { success = false, error = failure, message = message }
                        end,
                    }, nil
                end,
            }
            return func.run({})
        end

        for _, case in ipairs({
            { name = "unknown target", target = "app:unknown_call_target", message = "no handler registered for target" },
            { name = "invalid target", target = "missing_namespace", message = "namespace required in function ID" },
        }) do
            it("routes an " .. case.name .. " failure through error targets", function()
                local c = test.not_nil(client.new()) :: any
                local node_id = uuid.v7()
                local dataflow_id, create_err = c:create_workflow({
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = node_id,
                            node_type = "userspace.dataflow.node.func:node",
                            status = consts.STATUS.PENDING,
                            config = {
                                func_id = case.target,
                                error_targets = {
                                    { data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "error" },
                                },
                            },
                        },
                    },
                    {
                        type = consts.COMMAND_TYPES.CREATE_DATA,
                        payload = {
                            data_id = uuid.v7(),
                            data_type = consts.DATA_TYPE.NODE_INPUT,
                            node_id = node_id,
                            key = "default",
                            content = { message = "input" },
                            content_type = consts.CONTENT_TYPE.JSON,
                        },
                    },
                })
                test.is_nil(create_err)
                local result, exec_err = c:execute(dataflow_id)
                test.is_nil(exec_err)
                test.is_true(result.success)
                local output, read_err = data_reader.with_dataflow(dataflow_id)
                    :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                    :with_data_keys("error")
                    :fetch_options({ replace_references = true }):one()
                test.is_nil(read_err)
                output = test.not_nil(output) :: any
                local failure, decode_err = json.decode(output.content)
                test.is_nil(decode_err)
                test.eq(failure.code, "FUNCTION_EXECUTION_FAILED")
                test.eq(failure.message, case.message)
            end)
        end

        it("passes the native synchronous start error to fail", function()
            local cause = errors.new({ kind = errors.PERMISSION_DENIED,
                message = "Function access denied", retryable = false })
            func._deps.funcs = {
                new = function()
                    return {
                        with_context = function(self) return self end,
                        async = function() return nil, cause end,
                    }
                end,
            }
            local result = run_function({})
            test.is_false(result.success)
            test.eq(result.error.code, "FUNCTION_EXECUTION_FAILED")
            test.eq(result.error.message, cause)
            test.eq(result.message, "Function execution failed: Function access denied")
        end)

        it("waits for the function when the events channel closes", function()
            local events = channel.new(0)
            events:close()
            func._deps.process = { events = function() return events end, event = process.event }
            local closed_selections = 0
            func._deps.channel = { select = function(cases)
                local selected = original_channel.select(cases)
                if selected.channel == events then
                    closed_selections = closed_selections + 1
                    test.eq(closed_selections, 1, "closed events channel is removed from selection")
                    test.is_false(selected.ok)
                end
                return selected
            end }
            local result = run_function({ delay = "100ms", value = "completed" })
            test.eq(closed_selections, 1)
            test.is_true(result.success)
            test.eq(result.output, "completed")
        end)

        it("waits for the function after an unrelated process exits during selection", function()
            local pid, spawn_err = process.spawn_monitored("app:call_loop_event", "app:processes")
            test.is_nil(spawn_err)
            test.not_nil(pid)
            local events = process.events()
            local started = false
            local exited = false
            func._deps.channel = { select = function(cases)
                if not started then
                    started = true
                    test.is_nil(select(2, process.send(pid, "exit", {})))
                end
                local selected = original_channel.select(cases)
                if selected.channel == events then
                    test.eq(selected.value.kind, process.event.EXIT)
                    test.eq(selected.value.from, pid)
                    exited = true
                else
                    test.is_true(exited, "EXIT arrives before the function response")
                end
                return selected
            end }
            local result = run_function({ delay = "150ms", value = "completed" })
            test.is_true(result.success)
            test.eq(result.output, "completed")
            test.is_true(exited)
            local remaining = original_channel.select({ events:case_receive(), default = true })
            test.is_nil(remaining.channel, "no process event remains queued")
        end)

        it("cancels the in-flight command after an unrelated event", function()
            local events = channel.new(2)
            local sent = channel.new(1)
            func._deps.process = { events = function() return events end, event = process.event }
            coroutine.spawn(function()
                time.sleep("20ms")
                events:send({ kind = process.event.EXIT })
                time.sleep("20ms")
                events:send({ kind = process.event.CANCEL })
                sent:send(true)
            end)
            local observed = {}
            func._deps.channel = { select = function(cases)
                local selected = original_channel.select(cases)
                if selected.channel == events then
                    table.insert(observed, selected.value.kind)
                end
                return selected
            end }
            local command
            func._deps.funcs = {
                new = function()
                    local executor = original_funcs.new()
                    return {
                        with_context = function(self, context)
                            executor = executor:with_context(context)
                            return self
                        end,
                        async = function(_, target, input)
                            local err
                            command, err = executor:async(target, input)
                            return command, err
                        end,
                    }
                end,
            }
            local ok, result = pcall(run_function, { delay = "1s", value = "completed" })
            sent:receive()
            test.is_true(ok)
            test.eq(#observed, 2)
            test.eq(observed[1], process.event.EXIT)
            test.eq(observed[2], process.event.CANCEL)
            test.is_false(result.success)
            test.eq(result.error.code, "FUNCTION_CANCELED")
            test.eq(result.message, "Function execution was canceled")
            test.is_true(command:is_canceled())
            local remaining = original_channel.select({ events:case_receive(), default = true })
            test.is_nil(remaining.channel, "no injected event remains queued")
        end)

        it("includes a native rich result error in the failure message", function()
            local result = run_function({ rich_error = true })
            test.is_false(result.success)
            test.eq(result.error.code, "FUNCTION_EXECUTION_FAILED")
            test.eq(result.message, "Function execution failed: Function access denied")
            test.eq(result.error.message:kind(), errors.PERMISSION_DENIED)
        end)
    end)
end

return test.run_cases(define_tests)
