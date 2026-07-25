local test = require("test")
local uuid = require("uuid")
local time = require("time")
local client = require("client")
local consts = require("consts")
local activation_repo = require("activation_repo")

local function wait_until(predicate, timeout_ms: number): boolean
    local attempts = math.ceil(timeout_ms / 50)
    for _ = 1, attempts do
        if predicate() then return true end
        time.sleep("50ms")
    end
    return false
end

local function run_tests()
    test.describe("Dataflow runtime ownership failure", function()
        test.it("terminalizes a killed orchestrator and never resurrects its generation", function()
            local c = test.not_nil(client.new()) :: any
            local node_id = uuid.v7()
            local dataflow_id = test.not_nil(c:create_workflow({
                {
                    type = consts.COMMAND_TYPES.CREATE_NODE,
                    payload = {
                        node_id = node_id,
                        node_type = "userspace.dataflow.node.func:node",
                        status = consts.STATUS.PENDING,
                        config = {
                            func_id = "userspace.dataflow.node.func:test_func",
                            data_targets = {
                                { data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT },
                            },
                        },
                        metadata = { title = "Runtime ownership failure probe" },
                    },
                },
                {
                    type = consts.COMMAND_TYPES.CREATE_DATA,
                    payload = {
                        data_id = uuid.v7(),
                        data_type = consts.DATA_TYPE.NODE_INPUT,
                        node_id = node_id,
                        content = { message = "never replay", delay_ms = 5000 },
                        content_type = consts.CONTENT_TYPE.JSON,
                        key = "default",
                    },
                },
            })) :: string

            local started, start_err = c:start(dataflow_id)
            test.is_nil(start_err)
            test.eq(started, dataflow_id)

            local process_name = "dataflow." .. dataflow_id
            local owner_pid = nil
            test.is_true(wait_until(function()
                owner_pid = process.registry.lookup(process_name)
                return owner_pid ~= nil
            end, 3000), "canonical orchestrator became observable")

            local terminated, terminate_err = process.terminate(
                test.not_nil(owner_pid) :: string)
            test.is_nil(terminate_err)
            test.is_true(terminated)
            test.is_true(wait_until(function()
                return c:get_status(dataflow_id) == consts.STATUS.COMPLETED_FAILURE
            end, 5000), "owner loss became a durable workflow failure")

            local activation, activation_err = activation_repo.get(dataflow_id)
            test.is_nil(activation_err)
            activation = test.not_nil(activation) :: any
            test.is_false(activation.desired_active)
            local failed_generation = activation.generation
            test.is_nil(process.registry.lookup(process_name))

            local restarted, restart_err = c:start(dataflow_id)
            test.is_nil(restarted)
            test.contains(tostring(restart_err), "terminal state")

            time.sleep("500ms")
            local after, after_err = activation_repo.get(dataflow_id)
            test.is_nil(after_err)
            after = test.not_nil(after) :: any
            test.eq(after.generation, failed_generation)
            test.is_false(after.desired_active)
            test.eq(c:get_status(dataflow_id), consts.STATUS.COMPLETED_FAILURE)
            test.is_nil(process.registry.lookup(process_name))
        end)
    end)
end

return { run_tests = test.run_cases(run_tests) }
