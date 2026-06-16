local test = require("test")
local uuid = require("uuid")
local time = require("time")
local process = require("process")
local client = require("client")
local consts = require("consts")
local node_reader = require("node_reader")

local SIGNAL_NODE_TYPE = "userspace.dataflow.node.signal:node"

local function define_tests()
    describe("Func Node Control Recovery", function()
        local c

        before_all(function()
            c = client.new()
            test.not_nil(c, "client created")
        end)

        local function kill_orchestrator(df_id)
            local pid = process.registry.lookup("dataflow." .. df_id)
            if pid then
                process.terminate(pid)
                time.sleep("200ms")
            end
        end

        local function wait_until(predicate, timeout_ms, interval_ms)
            local timeout = timeout_ms or 20000
            local interval = interval_ms or 100
            local attempts = math.ceil(timeout / interval)
            for _ = 1, attempts do
                local ok, value = pcall(predicate)
                if ok and value then
                    return value
                end
                time.sleep(tostring(interval) .. "ms")
            end
            return nil
        end

        local function wait_complete(df_id, timeout_ms)
            return wait_until(function()
                local status = c:get_status(df_id)
                if status == consts.STATUS.COMPLETED_SUCCESS then
                    return true
                end
                if status == consts.STATUS.COMPLETED_FAILURE then
                    return false
                end
                return nil
            end, timeout_ms or 25000, 100)
        end

        local function signal_children(df_id, func_node_id)
            return (node_reader.with_dataflow(df_id) :: any)
                :with_parent_nodes(func_node_id)
                :with_node_types(SIGNAL_NODE_TYPE)
                :all()
        end

        it("does not duplicate a func _control child across a mid-yield crash", function()
            local func_node_id = uuid.v7()
            local signal_id = "func-recovery-signal-" .. uuid.v7()

            local df_id = c:create_workflow({
                { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                    node_id = func_node_id,
                    node_type = "userspace.dataflow.node.func:node",
                    status = consts.STATUS.PENDING,
                    config = {
                        func_id = "userspace.dataflow.node.func:signal_child_func",
                        data_targets = {
                            { data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "result", content_type = consts.CONTENT_TYPE.JSON }
                        }
                    },
                    metadata = { title = "Func With Signal Child" }
                }},
                { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                    data_id = uuid.v7(),
                    data_type = consts.DATA_TYPE.NODE_INPUT,
                    node_id = func_node_id,
                    key = "default",
                    content = { signal_id = signal_id },
                    content_type = consts.CONTENT_TYPE.JSON
                }}
            })

            c:start(df_id)
            test.not_nil(wait_until(function()
                local rows = signal_children(df_id, func_node_id)
                if rows and #rows >= 1 then
                    return rows
                end
                return nil
            end, 15000, 100), "signal child created by the func control directive")

            -- crash while the func node is yielded waiting on its signal child
            kill_orchestrator(df_id)
            c:start(df_id)
            test.not_nil(wait_until(function()
                if c:get_status(df_id) == consts.STATUS.RUNNING then
                    return true
                end
                return nil
            end, 10000, 100), "workflow restarted")

            c:signal(df_id, signal_id, { ok = true, value = 42 })
            test.is_true(wait_complete(df_id, 25000), "func node completed after recovery")

            -- the func must have resumed the existing child, not re-run and duplicated it
            local children = signal_children(df_id, func_node_id)
            test.eq(#children, 1, "func _control child not duplicated on recovery")
        end)
    end)
end

return test.run_cases(define_tests)
