local test = require("test")
local uuid = require("uuid")
local time = require("time")
local client = require("client")
local consts = require("consts")

local RUNTIME_EPOCH_ENV = "userspace.dataflow.env:runtime_epoch"

local function wait_until(predicate: () -> boolean, timeout_ms: number): boolean
    local attempts = math.ceil(timeout_ms / 50)
    for _ = 1, attempts do
        if predicate() then return true end
        time.sleep("50ms")
    end
    return false
end

-- Runs as the restricted caller: it may use Dataflow but cannot read the
-- module-owned runtime epoch.
local function probe(args: any): any
    local _, epoch_err = env.get(RUNTIME_EPOCH_ENV)
    local c, client_err = client.new()
    if not c then return { error = tostring(client_err) } end
    local node_id = uuid.v7()
    local dataflow_id, create_err = (c :: any):create_workflow({
        {
            type = consts.COMMAND_TYPES.CREATE_NODE,
            payload = {
                node_id = node_id,
                node_type = "userspace.dataflow.node.func:node",
                status = consts.STATUS.PENDING,
                config = {
                    func_id = "userspace.dataflow.node.func:test_func",
                    data_targets = { { data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT } },
                },
                metadata = { title = "Restricted caller probe" },
            },
        },
        {
            type = consts.COMMAND_TYPES.CREATE_DATA,
            payload = {
                data_id = uuid.v7(),
                data_type = consts.DATA_TYPE.NODE_INPUT,
                node_id = node_id,
                content = { message = "restricted" },
                content_type = consts.CONTENT_TYPE.JSON,
                key = "default",
            },
        },
    })
    if create_err then return { error = tostring(create_err) } end
    local id = tostring(dataflow_id)
    if args and args.mode == "start" then
        local _, start_err = (c :: any):start(id)
        if start_err then return { error = tostring(start_err) } end
        wait_until(function()
            local status = (c :: any):get_status(id)
            return status == consts.STATUS.COMPLETED_SUCCESS or status == consts.STATUS.COMPLETED_FAILURE
        end, 5000)
    else
        local _, execute_err = (c :: any):execute(id)
        if execute_err then return { error = tostring(execute_err), status = (c :: any):get_status(id) } end
    end
    return {
        epoch_denied = epoch_err ~= nil,
        status = (c :: any):get_status(id),
    }
end

local function run_tests()
    test.describe("Dataflow under a restricted caller scope", function()
        local function restricted_scope(): any
            local policy = test.not_nil(select(1, security.policy("app:caller_without_runtime_epoch")))
            return test.not_nil(select(1, security.new_scope({ policy })))
        end

        test.it("reads the runtime epoch through the module-owned reader without caller env access", function()
            local read, read_err = funcs.new():with_scope(restricted_scope())
                :call("userspace.dataflow.runner:runtime_epoch")
            test.is_nil(read_err)
            test.not_nil((test.not_nil(read) :: any).epoch)
        end)

        for _, mode in ipairs({ "execute", "start" }) do
            test.it("runs a workflow to completion through " .. mode, function()
                local result, err = funcs.new():with_scope(restricted_scope())
                    :call("app:restricted_caller_probe", { mode = mode })
                test.is_nil(err)
                local observed = test.not_nil(result) :: any
                test.is_nil(observed.error)
                test.is_true(observed.epoch_denied, "the caller scope cannot read the runtime epoch")
                test.eq(observed.status, consts.STATUS.COMPLETED_SUCCESS)
            end)
        end
    end)
end

return { run_tests = test.run_cases(run_tests), probe = probe }
