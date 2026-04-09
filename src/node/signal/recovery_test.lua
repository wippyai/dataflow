local test = require("test")
local uuid = require("uuid")
local json = require("json")
local time = require("time")
local client = require("client")
local consts = require("consts")
local data_reader = require("data_reader")

local function define_tests()
    describe("Signal Recovery Tests", function()
        local c

        before_all(function()
            c = client.new()
            test.not_nil(c, "client created")
        end)

        local function create_signal_workflow(signal_id)
            local node_id = uuid.v7()
            local input_data_id = uuid.v7()

            local dataflow_id, err = c:create_workflow({
                {
                    type = consts.COMMAND_TYPES.CREATE_NODE,
                    payload = {
                        node_id = node_id,
                        node_type = "userspace.dataflow.node.signal:node",
                        status = consts.STATUS.PENDING,
                        config = {
                            signal_id = signal_id,
                            data_targets = {
                                {
                                    data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                    key = "result",
                                    content_type = consts.CONTENT_TYPE.JSON
                                }
                            }
                        },
                        metadata = { title = "Signal: " .. signal_id }
                    }
                },
                {
                    type = consts.COMMAND_TYPES.CREATE_DATA,
                    payload = {
                        data_id = input_data_id,
                        data_type = consts.DATA_TYPE.NODE_INPUT,
                        node_id = node_id,
                        content = { task = "test" },
                        content_type = consts.CONTENT_TYPE.JSON,
                        key = "default"
                    }
                }
            })

            return dataflow_id, err
        end

        describe("basic signal flow", function()
            it("starts, waits, receives signal, completes", function()
                local signal_id = "basic-" .. uuid.v7()
                local dataflow_id, err = create_signal_workflow(signal_id)
                test.is_nil(err, "create: " .. tostring(err))

                local _, start_err = c:start(dataflow_id)
                test.is_nil(start_err, "start: " .. tostring(start_err))

                time.sleep("300ms")

                local status = c:get_status(dataflow_id)
                test.eq(status, consts.STATUS.RUNNING, "should be running while waiting")

                local _, sig_err = c:signal(dataflow_id, signal_id, { approved = true })
                test.is_nil(sig_err, "signal: " .. tostring(sig_err))

                time.sleep("500ms")

                local final = c:get_status(dataflow_id)
                test.eq(final, consts.STATUS.COMPLETED_SUCCESS, "should complete after signal")
            end)
        end)

        describe("kill and recover", function()
            it("recovers after orchestrator kill during signal wait", function()
                local signal_id = "kill-" .. uuid.v7()
                local dataflow_id, err = create_signal_workflow(signal_id)
                test.is_nil(err, "create")

                c:start(dataflow_id)
                time.sleep("300ms")

                test.eq(c:get_status(dataflow_id), consts.STATUS.RUNNING, "running before kill")

                local pid = process.registry.lookup("dataflow." .. dataflow_id)
                test.not_nil(pid, "orchestrator registered")
                process.terminate(pid)
                time.sleep("200ms")

                local dead = process.registry.lookup("dataflow." .. dataflow_id)
                test.is_nil(dead, "orchestrator dead after terminate")

                local _, sig_err = c:signal(dataflow_id, signal_id, { approved = true })
                test.is_nil(sig_err, "signal triggers respawn")

                time.sleep("3s")

                test.eq(c:get_status(dataflow_id), consts.STATUS.COMPLETED_SUCCESS, "recovered and completed")
            end)
        end)

        describe("commit backlog", function()
            it("processes signal sent while orchestrator was dead", function()
                local signal_id = "backlog-" .. uuid.v7()
                local dataflow_id, err = create_signal_workflow(signal_id)
                test.is_nil(err, "create")

                c:start(dataflow_id)
                time.sleep("300ms")

                local pid = process.registry.lookup("dataflow." .. dataflow_id)
                process.terminate(pid)
                time.sleep("200ms")

                c:signal(dataflow_id, signal_id, { data = "from-backlog" })

                time.sleep("3s")

                test.eq(c:get_status(dataflow_id), consts.STATUS.COMPLETED_SUCCESS, "backlog processed")
            end)
        end)

        describe("idempotency", function()
            it("handles duplicate signals", function()
                local signal_id = "dup-" .. uuid.v7()
                local dataflow_id, err = create_signal_workflow(signal_id)
                test.is_nil(err, "create")

                c:start(dataflow_id)
                time.sleep("300ms")

                c:signal(dataflow_id, signal_id, { first = true })
                c:signal(dataflow_id, signal_id, { second = true })

                time.sleep("500ms")

                test.eq(c:get_status(dataflow_id), consts.STATUS.COMPLETED_SUCCESS, "handles duplicates")
            end)

            it("handles concurrent signal + respawn race", function()
                local signal_id = "race-" .. uuid.v7()
                local dataflow_id, err = create_signal_workflow(signal_id)
                test.is_nil(err, "create")

                c:start(dataflow_id)
                time.sleep("300ms")

                local pid = process.registry.lookup("dataflow." .. dataflow_id)
                process.terminate(pid)
                time.sleep("100ms")

                c:signal(dataflow_id, signal_id, { a = 1 })
                c:signal(dataflow_id, signal_id, { b = 2 })

                time.sleep("3s")

                test.eq(c:get_status(dataflow_id), consts.STATUS.COMPLETED_SUCCESS, "concurrent respawn handled")
            end)
        end)
    end)
end

return { run_tests = test.run_cases(define_tests) }
