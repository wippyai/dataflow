local test = require("test")
local uuid = require("uuid")
local json = require("json")
local time = require("time")
local client = require("client")
local consts = require("consts")
local data_reader = require("data_reader")

local function decode_cycle_state(data)
    local content = data.content

    if type(content) == "string" then
        local decoded, decode_err = json.decode(content)
        if not decode_err then
            content = decoded
        end
    end

    if type(content) == "table" and content._cycle_persisted == true then
        return content.state
    end

    return content
end

local function get_cycle_states(dataflow_id, node_id)
    local rows = data_reader.with_dataflow(dataflow_id)
        :with_nodes(node_id)
        :with_data_types("cycle.state")
        :all()

    table.sort(rows, function(a, b)
        local a_iteration = (a.metadata and a.metadata.iteration) or 0
        local b_iteration = (b.metadata and b.metadata.iteration) or 0
        return a_iteration < b_iteration
    end)

    return rows
end

local function wait_for_cycle_state_count(dataflow_id, node_id, expected_count, timeout_ms)
    local attempts = math.ceil((timeout_ms or 8000) / 20)

    for _ = 1, attempts do
        local rows = get_cycle_states(dataflow_id, node_id)
        if #rows >= expected_count then
            return rows
        end

        time.sleep("20ms")
    end

    return nil
end

local function wait_complete(c, dataflow_id, timeout_ms)
    local attempts = math.ceil((timeout_ms or 15000) / 200)

    for _ = 1, attempts do
        local status = c:get_status(dataflow_id)
        if status == consts.STATUS.COMPLETED_SUCCESS then
            return true
        end
        if status == consts.STATUS.COMPLETED_FAILURE then
            return false
        end

        time.sleep("200ms")
    end

    return false
end

local function kill_orchestrator(dataflow_id)
    local pid = process.registry.lookup("dataflow." .. dataflow_id)
    if pid then
        process.terminate(pid)
        time.sleep("200ms")
    end
end

local function define_tests()
    describe("Cycle Recovery Regression Tests", function()
        local c

        before_all(function()
            c = client.new()
            test.not_nil(c, "client created")
        end)

        it("replays the prior child result after a restart", function()
            local cycle_node_id = uuid.v7()
            local workflow_input_id = uuid.v7()
            local node_input_id = uuid.v7()

            local dataflow_id = c:create_workflow({
                { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                    node_id = cycle_node_id,
                    node_type = "userspace.dataflow.node.cycle:cycle",
                    status = consts.STATUS.PENDING,
                    config = {
                        func_id = "userspace.dataflow.node.cycle.stub:refine_test_func",
                        max_iterations = 10,
                        initial_state = {
                            refinement_count = 0,
                            quality_score = 0.3
                        },
                        data_targets = {
                            {
                                data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                key = "result",
                                content_type = consts.CONTENT_TYPE.JSON
                            }
                        }
                    },
                    metadata = { title = "Cycle Recovery Regression" }
                } },
                { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                    data_id = workflow_input_id,
                    data_type = consts.DATA_TYPE.WORKFLOW_INPUT,
                    content = {
                        target_quality = 0.8,
                        initial_text = "Recovery regression",
                        resume_delay_ms = 400
                    },
                    content_type = consts.CONTENT_TYPE.JSON
                } },
                { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                    data_id = node_input_id,
                    data_type = consts.DATA_TYPE.NODE_INPUT,
                    node_id = cycle_node_id,
                    key = workflow_input_id,
                    content = "",
                    content_type = consts.CONTENT_TYPE.REFERENCE
                } }
            })

            c:start(dataflow_id)

            local first_state_rows = wait_for_cycle_state_count(dataflow_id, cycle_node_id, 1, 8000)
            test.not_nil(first_state_rows, "first cycle iteration persisted")
            test.eq(#first_state_rows, 1, "restart happens before second iteration persists")

            local first_state = decode_cycle_state(first_state_rows[1])
            test.eq(first_state.quality_score, 0.3, "first persisted state is still pre-child")
            test.eq(first_state.refinement_count, 0, "no refinement applied before restart")

            kill_orchestrator(dataflow_id)
            c:start(dataflow_id)

            local second_state_rows = wait_for_cycle_state_count(dataflow_id, cycle_node_id, 2, 10000)
            test.not_nil(second_state_rows, "second cycle iteration persisted after restart")

            local second_state = decode_cycle_state(second_state_rows[2])
            test.gte(second_state.quality_score, 0.45, "restart replayed child quality into iteration 2")
            test.gte(second_state.refinement_count, 1, "restart preserved child contribution count")

            test.is_true(wait_complete(c, dataflow_id, 15000), "cycle completed after restart")
            test.eq(c:get_status(dataflow_id), consts.STATUS.COMPLETED_SUCCESS, "workflow succeeded")
        end)
    end)
end

return { run_tests = test.run_cases(define_tests) }
