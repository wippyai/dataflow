local test = require("test")
local uuid = require("uuid")
local time = require("time")
local json = require("json")
local client = require("client")
local commit = require("commit")
local func = require("func")
local consts = require("consts")
local data_reader = require("data_reader")
local node_reader = require("node_reader")
local inspection = require("inspection")
local workflow_state = require("workflow_state")
local scheduler = require("scheduler")
local activation_repo = require("activation_repo")

local function rows(id, kind, node_id)
    local reader = data_reader.with_dataflow(id):with_data_types(kind)
    if node_id then reader = reader:with_nodes(node_id) end
    local found, err = reader:all()
    test.is_nil(err)
    return found
end

local function decode(value)
    if type(value) == "string" then return json.decode(value) end
    return value
end

local function create(config, input, metadata)
    local c, err = client.new()
    test.is_nil(err)
    local node_id = uuid.v7()
    config.func_id = config.func_id or "userspace.dataflow.node.func:retry_fixture"
    config.data_targets = {{data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "result"}}
    config.error_targets = {{data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "error"}}
    local id, create_err = c:create_workflow({
        {type = consts.COMMAND_TYPES.CREATE_NODE, payload = {node_id = node_id,
            node_type = "userspace.dataflow.node.func:node", status = consts.STATUS.PENDING,
            config = config, metadata = metadata}},
        {type = consts.COMMAND_TYPES.CREATE_DATA, payload = {data_id = uuid.v7(), node_id = node_id,
            data_type = consts.DATA_TYPE.NODE_INPUT, key = "default", content = input, content_type = consts.CONTENT_TYPE.JSON}},
    })
    test.is_nil(create_err)
    return c, id, node_id
end

local function finish(c, id)
    local result, exec_err = c:execute(id)
    test.is_nil(exec_err)
    test.not_nil(result)
    local deadline = time.now():add(5 * time.SECOND)
    while time.now():before(deadline) do
        local status, status_err = c:get_status(id)
        test.is_nil(status_err)
        if status == consts.STATUS.COMPLETED_SUCCESS or status == consts.STATUS.COMPLETED_FAILURE then return end
        time.sleep("10ms")
    end
    error("workflow does not finish within 5s")
end

local function define_tests()
    describe("Durable func retry integration", function()
        it("fails twice, passivates between attempts and succeeds without an error route", function()
            local c, id, node_id = create({retry_policy = {initial_interval = "30ms", maximum_attempts = 4}},
                {fail_attempts = 2, value = "done"})
            finish(c, id)
            local output = rows(id, consts.DATA_TYPE.WORKFLOW_OUTPUT)
            test.eq(#output, 1)
            test.eq(output[1].key, "result")
            local result = decode(output[1].content)
            test.eq(result.attempt, 3)
            test.eq(result.idempotency_key, id .. ":" .. node_id)
            test.eq(result.value, "done")
            local node = node_reader.with_dataflow(id):with_nodes(node_id):one()
            test.is_false(node.metadata.func_retry)
            local waits = rows(id, consts.DATA_TYPE.NODE_YIELD, node_id)
            test.eq(#waits, 2)
            local deadlines = {}
            for _, row in ipairs(waits) do
                test.eq(row.data_id:sub(15, 15), "7", "durable data ids remain time ordered")
                local content = decode(row.content)
                test.is_true(content.yield_context.wait_for_timer)
                test.is_nil(content.yield_context.wait_for_signal)
                test.is_nil(content.yield_context.signal_id)
                deadlines[content.yield_id] = content.yield_context.timeout_deadline
            end
            test.eq(#rows(id, consts.DATA_TYPE.NODE_YIELD_RESULT, node_id), 2)
            test.not_nil(deadlines[node_id .. ":retry:1"])
            test.not_nil(deadlines[node_id .. ":retry:2"])
        end)

        for _, case in ipairs({
            {name = "maximum attempts", policy = {initial_interval = "10ms", maximum_attempts = 3}, attempts = 3},
            {name = "schedule window", policy = {initial_interval = "1h"}, schedule = "1s", attempts = 1},
            {name = "native non-retryable flag", policy = {}, retryable = false, attempts = 1},
            {name = "non-retryable kind", policy = {non_retryable_error_types = {"Invalid"}}, kind = "Invalid", attempts = 1},
            {name = "cancellation", policy = {}, kind = "Canceled", retryable = true, attempts = 1},
        }) do
            it("emits one structured final failure for " .. case.name, function()
                local c, id, node_id = create({retry_policy = case.policy, schedule_to_close_timeout = case.schedule},
                    {fail_attempts = 10, kind = case.kind, retryable = case.retryable})
                finish(c, id)
                local output = rows(id, consts.DATA_TYPE.WORKFLOW_OUTPUT)
                test.eq(#output, 1)
                test.eq(output[1].key, "error")
                local failure = decode(output[1].content)
                test.eq(failure.attempts, case.attempts)
                test.eq(failure.cause.kind, case.kind or "Unavailable")
                test.eq(failure.last_error.details.code, "CALL_FAILED")
                local node = node_reader.with_dataflow(id):with_nodes(node_id):one()
                test.eq(node.metadata.error.attempts, case.attempts)
                test.eq(#rows(id, consts.DATA_TYPE.NODE_RESULT, node_id), 1)
            end)
        end

        it("times out the first attempt, cancels it and succeeds on the second", function()
            local c, id = create({retry_policy = {initial_interval = "10ms", maximum_attempts = 3},
                start_to_close_timeout = "30ms"}, {timeout_attempts = 1})
            finish(c, id)
            local output = rows(id, consts.DATA_TYPE.WORKFLOW_OUTPUT)
            test.eq(#output, 1)
            test.eq(output[1].key, "result")
            test.eq(decode(output[1].content).attempt, 2)
        end)

        it("rejects retry and timeout declarations on every other raw node kind", function()
            local c, err = client.new()
            test.is_nil(err)
            for _, kind in ipairs({"userspace.dataflow.node.agent:node", "userspace.dataflow.node.cycle:cycle",
                "userspace.dataflow.node.parallel:parallel", "userspace.dataflow.node.signal:node",
                "userspace.dataflow.node.state:state", "example:await"}) do
                for _, field in ipairs({"retry_policy", "start_to_close_timeout", "schedule_to_close_timeout"}) do
                    local declaration = {[field] = field == "retry_policy" and {} or "1s"}
                    local _, create_err = c:create_workflow({{type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = uuid.v7(), node_type = kind, config = declaration,
                    }}})
                    test.not_nil(create_err)
                    test.contains(tostring(create_err), "only on func")
                end
            end
        end)

        it("keeps default failures at one attempt without retry metadata or yields", function()
            local c, id, node_id = create({}, {fail_attempts = 10})
            finish(c, id)
            local failure = decode(rows(id, consts.DATA_TYPE.WORKFLOW_OUTPUT)[1].content)
            test.is_nil(failure.attempts)
            test.eq(#rows(id, consts.DATA_TYPE.NODE_YIELD), 0)
            test.is_nil(node_reader.with_dataflow(id):with_nodes(node_id):one().metadata.func_retry)
        end)

        it("passivates an hour backoff, preserves its deadline across restart and ignores signals", function()
            local c, id, node_id = create({retry_policy = {initial_interval = "1h", maximum_attempts = 3}}, {fail_attempts = 1})
            local result, exec_err = c:execute(id)
            test.is_nil(exec_err)
            test.is_true(result.passivated, "expected passivation: " .. json.encode(result))
            local node = node_reader.with_dataflow(id):with_nodes(node_id):one()
            test.eq(node.status, consts.STATUS.WAITING)
            test.eq(node.metadata.func_retry.attempt, 1)
            local deadline = node.metadata.func_retry.wait_until
            test.is_true(time.parse(time.RFC3339NANO, deadline):after(time.now():add(59 * time.MINUTE)), "hour deadline remains in the future")
            local pid = process.registry.lookup("dataflow." .. id)
            test.is_nil(pid, "orchestrator process is released")
            local state = workflow_state.new(id)
            test.is_nil(select(2, state:load_state()))
            test.is_nil(select(2, state:recover()))
            local snapshot = state:get_scheduler_snapshot()
            test.is_nil(next(snapshot.active_processes), "no node process survives reconstruction")
            test.eq(snapshot.active_yields[node_id].timeout_deadline, deadline)
            local decision = scheduler.find_next_work(snapshot)
            test.eq(decision.type, scheduler.DECISION_TYPE.PASSIVATE)
            test.eq(decision.payload.wake_at, deadline)
            local wait_id = node_id .. ":retry:1"
            for _, signal_id in ipairs({wait_id, node_id}) do
                local pending, pending_err = inspection.pending_signal(id, signal_id, "userspace.dataflow.node.func:node")
                test.is_nil(pending)
                test.not_nil(pending_err)
                test.is_nil(select(2, c:signal(id, signal_id, {timer_completed = true})))
            end
            local drained = false
            local signal_deadline = time.now():add(5 * time.SECOND)
            while time.now():before(signal_deadline) do
                local activation, activation_err = activation_repo.get(id)
                test.is_nil(activation_err)
                if not activation.desired_active and not process.registry.lookup("dataflow." .. id) then
                    drained = true
                    break
                end
                time.sleep("10ms")
            end
            test.is_true(drained, "signal activation drains and releases its orchestrator")
            result, exec_err = c:execute(id)
            test.is_nil(exec_err)
            test.is_true(result.passivated, "expected passivation: " .. json.encode(result))
            node = node_reader.with_dataflow(id):with_nodes(node_id):one()
            test.eq(node.metadata.func_retry.attempt, 1)
            test.eq(node.metadata.func_retry.wait_until, deadline)
            test.eq(#rows(id, consts.DATA_TYPE.NODE_YIELD_RESULT), 0)
            test.eq(#rows(id, consts.DATA_TYPE.WORKFLOW_OUTPUT), 0)
            test.is_nil(select(2, c:cancel(id)))
        end)

        it("replays a completed backoff without recording another satisfaction", function()
            local deadline = time.now():add(time.HOUR):format(time.RFC3339NANO)
            local c, id, node_id = create({retry_policy = {maximum_attempts = 3}}, {}, {func_retry = {
                attempt = 1, first_attempt_at = time.now():unix_nano() / time.MILLISECOND,
                in_flight = false, wait_until = deadline, last_error = {kind = "Unavailable", message = "Call failed"},
            }})
            local wait_id = node_id .. ":retry:1"
            local data_id = uuid.v7()
            local _, persist_err = commit.execute(id, uuid.v7(), {
                {type = consts.COMMAND_TYPES.UPDATE_NODE, payload = {node_id = node_id, status = consts.STATUS.WAITING}},
                {type = consts.COMMAND_TYPES.CREATE_DATA, payload = {data_id = data_id, node_id = node_id,
                    data_type = consts.DATA_TYPE.NODE_YIELD, key = wait_id, content_type = consts.CONTENT_TYPE.JSON,
                    content = {node_id = node_id, yield_id = wait_id, reply_to = consts.MESSAGE_TOPIC.YIELD_REPLY_PREFIX .. node_id,
                        yield_context = {run_nodes = {}, wait_for_timer = true, timeout_deadline = deadline, park_ack = true}}}},
                {type = consts.COMMAND_TYPES.CREATE_DATA, payload = {data_id = uuid.v7(), node_id = node_id,
                    data_type = consts.DATA_TYPE.NODE_YIELD_RESULT, key = wait_id, content_type = consts.CONTENT_TYPE.JSON,
                    content = {timer_completed = true}}},
            })
            test.is_nil(persist_err)
            finish(c, id)
            test.eq(decode(rows(id, consts.DATA_TYPE.WORKFLOW_OUTPUT)[1].content).attempt, 2)
            test.eq(#rows(id, consts.DATA_TYPE.NODE_YIELD_RESULT, node_id), 1)
        end)

        for _, stage in ipairs({"reservation", "wait", "inputs", "missing_func", "missing_input", "policy"}) do
            it("keeps " .. stage .. " failures terminal through process exit and recovery", function()
                local c, id, node_id = create({retry_policy = {maximum_attempts = 3}}, {}, {func_retry = {
                    attempt = 1, first_attempt_at = time.now():unix_nano() / time.MILLISECOND, in_flight = true,
                }})
                test.is_nil(select(2, commit.execute(id, uuid.v7(), {{type = consts.COMMAND_TYPES.UPDATE_NODE,
                    payload = {node_id = node_id, status = consts.STATUS.RUNNING}}})))
                local original = func._deps.node
                local ok, failure = pcall(function()
                    func._deps.node = {new = function(args)
                        local n, err = original.new(args)
                        test.is_nil(err)
                        local declaration = n:config()
                        if stage == "reservation" then
                            n:update_metadata({func_retry = false})
                            n.submit = function() return nil, "Reservation rejected" end
                        elseif stage == "wait" then
                            n.yield = function() return nil, "Wait rejected" end
                        elseif stage == "inputs" then
                            n.inputs = function() return nil, "Inputs rejected" end
                        elseif stage == "missing_input" then
                            n.inputs = function() return {} end
                        elseif stage == "missing_func" then
                            declaration.func_id = nil
                        elseif stage == "policy" then
                            declaration.retry_policy.maximum_attempts = -1
                        end
                        return n
                    end}
                    local stored = node_reader.with_dataflow(id):with_nodes(node_id):one()
                    local result = func.run({dataflow_id = id, node_id = node_id, node = stored})
                    test.is_false(result.success)
                    local ws = workflow_state.new(id)
                    test.is_nil(select(2, ws:load_state()))
                    local pending = commit.get_pending_commits(id)
                    test.is_nil(select(2, ws:process_commits(pending)))
                    test.is_false(ws:get_nodes()[node_id].metadata.func_retry)
                    ws:track_process(node_id, "failed-process")
                    ws:handle_process_exit("failed-process", false, result)
                    test.eq(ws:get_nodes()[node_id].status, consts.STATUS.COMPLETED_FAILURE)
                    test.is_nil(select(2, ws:persist()))
                    local recovered = workflow_state.new(id)
                    test.is_nil(select(2, recovered:load_state()))
                    test.is_nil(select(2, recovered:recover()))
                    test.eq(recovered:get_nodes()[node_id].status, consts.STATUS.COMPLETED_FAILURE)
                    test.is_nil(recovered:get_scheduler_snapshot().active_yields[node_id])
                    test.eq(#rows(id, consts.DATA_TYPE.NODE_RESULT, node_id), 1)
                    test.eq(#rows(id, consts.DATA_TYPE.WORKFLOW_OUTPUT), 1)
                end)
                func._deps.node = original
                if not ok then error(failure) end
            end)
        end

        it("recovers a committed non-call failure without error routes before process exit", function()
            local _, id, node_id = create({retry_policy = {}}, {}, {func_retry = {
                attempt = 1, first_attempt_at = 123, in_flight = true,
            }})
            local stored = node_reader.with_dataflow(id):with_nodes(node_id):one()
            stored.config.func_id = nil
            stored.config.error_targets = {}
            stored.status = consts.STATUS.RUNNING
            test.is_nil(select(2, commit.execute(id, uuid.v7(), {{type = consts.COMMAND_TYPES.UPDATE_NODE,
                payload = {node_id = node_id, config = stored.config, status = stored.status}}})))
            local result = func.run({dataflow_id = id, node_id = node_id, node = stored})
            test.is_false(result.success)
            local recovered = workflow_state.new(id)
            test.is_nil(select(2, recovered:load_state()))
            test.is_nil(select(2, recovered:process_commits(commit.get_pending_commits(id))))
            test.is_nil(select(2, recovered:recover()))
            test.eq(recovered:get_nodes()[node_id].status, consts.STATUS.COMPLETED_FAILURE)
            test.is_false(recovered:get_nodes()[node_id].metadata.func_retry)
            test.eq(#rows(id, consts.DATA_TYPE.NODE_RESULT, node_id), 1)
        end)

        it("keeps policy-free restart behavior after failure metadata commits without error routes", function()
            local _, id, node_id = create({}, {})
            local stored = node_reader.with_dataflow(id):with_nodes(node_id):one()
            stored.config.func_id = nil
            stored.config.error_targets = {}
            test.is_nil(select(2, commit.execute(id, uuid.v7(), {{type = consts.COMMAND_TYPES.UPDATE_NODE,
                payload = {node_id = node_id, config = stored.config, status = consts.STATUS.RUNNING}}})))
            test.is_false(func.run({dataflow_id = id, node_id = node_id, node = stored}).success)
            local recovered = workflow_state.new(id)
            test.is_nil(select(2, recovered:load_state()))
            test.is_nil(select(2, recovered:process_commits(commit.get_pending_commits(id))))
            test.is_nil(select(2, recovered:recover()))
            test.eq(recovered:get_nodes()[node_id].status, consts.STATUS.PENDING)
            test.is_nil(recovered:get_nodes()[node_id].metadata.func_retry)
        end)

        it("consumes an interrupted reservation once before backoff and resumes with the next attempt", function()
            local first = time.now():unix_nano() / time.MILLISECOND
            local c, id, node_id = create({retry_policy = {initial_interval = "20ms", maximum_attempts = 3}}, {},
                {func_retry = {attempt = 1, first_attempt_at = first, in_flight = true}})
            finish(c, id)
            local output = rows(id, consts.DATA_TYPE.WORKFLOW_OUTPUT)
            test.eq(#output, 1)
            test.eq(output[1].key, "result")
            test.eq(decode(output[1].content).attempt, 2)
            test.eq(#rows(id, consts.DATA_TYPE.NODE_YIELD, node_id), 1)
        end)
    end)
end

return test.run_cases(define_tests)
