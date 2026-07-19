local consts = require("consts")
local time = require("time")

local scheduler = {}

local DECISION_TYPE = {
    EXECUTE_NODES = "execute_nodes",
    SATISFY_YIELD = "satisfy_yield",
    COMPLETE_WORKFLOW = "complete_workflow",
    NO_WORK = "no_work",
    PASSIVATE = "passivate"
}

local TRIGGER_REASON = {
    YIELD_DRIVEN = "yield_driven",
    INPUT_READY = "input_ready",
    ROOT_READY = "root_ready"
}

local CONCURRENT_CONFIG = {
    MAX_CONCURRENT_NODES = 10,
    ENABLE_YIELD_CONCURRENCY = false,
    ENABLE_INPUT_CONCURRENCY = true,
    ENABLE_ROOT_CONCURRENCY = true
}

local function create_decision(decision_type, payload)
    return {
        type = decision_type,
        payload = payload or {}
    }
end

local function create_nodes_execution(nodes)
    return create_decision(DECISION_TYPE.EXECUTE_NODES, {
        nodes = nodes
    })
end

local function parse_timeout_deadline(deadline_value: any): any
    if type(deadline_value) ~= "string" or deadline_value == "" then
        return nil
    end

    local deadline, err = time.parse(time.RFC3339NANO, deadline_value)
    if err then
        deadline, err = time.parse(time.RFC3339, deadline_value)
    end
    if err then
        return nil
    end

    return deadline
end

local function signal_timeout_result(parent_id, yield_info)
    return {
        timeout = true,
        error = true,
        code = "SIGNAL_TIMEOUT",
        message = "Signal timed out: " .. tostring(yield_info.signal_id or parent_id),
        signal_id = yield_info.signal_id,
        timeout_deadline = yield_info.timeout_deadline,
        timeout_ms = yield_info.timeout_ms
    }
end

local function signal_timeout_expired(yield_info, now)
    local deadline = parse_timeout_deadline(yield_info.timeout_deadline)
    if not deadline then
        return false
    end

    return now:after(deadline) or now:equal(deadline)
end

local function node_has_required_inputs(node_id, node_data, input_tracker)
    if not input_tracker.requirements[node_id] then
        local available = input_tracker.available[node_id] or {}
        return next(available) ~= nil
    end

    local requirements = input_tracker.requirements[node_id]
    local available = input_tracker.available[node_id] or {}

    for _, required_key in ipairs(requirements.required or {}) do
        if not available[required_key] then
            return false
        end
    end

    return true
end

local function node_has_available_inputs(node_id, input_tracker)
    local available = input_tracker.available[node_id] or {}
    return next(available) ~= nil
end

local function yield_children_complete(yield_info)
    if not yield_info.pending_children then
        return true
    end

    for child_id, status in pairs(yield_info.pending_children) do
        if status ~= consts.STATUS.COMPLETED_SUCCESS and
           status ~= consts.STATUS.COMPLETED_FAILURE and
           status ~= consts.STATUS.CANCELLED and
           status ~= consts.STATUS.TERMINATED and
           status ~= consts.STATUS.SKIPPED then
            return false
        end
    end

    return true
end

local function node_is_terminal(node)
    if not node then return false end
    return node.status == consts.STATUS.COMPLETED_SUCCESS or
        node.status == consts.STATUS.COMPLETED_FAILURE or
        node.status == consts.STATUS.CANCELLED or
        node.status == consts.STATUS.TERMINATED or
        node.status == consts.STATUS.SKIPPED
end

local function find_yield_driven_work(state)
    local now = time.now()

    for parent_id, yield_info in pairs(state.active_yields) do
        local parent = state.nodes[parent_id]
        if node_is_terminal(parent) then goto continue end

        -- detached yields belong to a dead process (parent was reset on recovery).
        -- satisfying them would send the reply into a void AND trick find_yield_driven_work
        -- into thinking this iteration's work is progressing, blocking re-execution.
        -- leave them in place so track_yield() can inherit accumulated signal_data
        -- when the node re-yields, but never satisfy here.
        if yield_info.detached then
            local ready = yield_info.wait_for_signal and
                (yield_info.signal_data ~= nil or signal_timeout_expired(yield_info, now)) or
                (not yield_info.wait_for_signal and yield_children_complete(yield_info))
            if ready and parent and
               (parent.status == consts.STATUS.PENDING or parent.status == consts.STATUS.WAITING) and
               not state.active_processes[parent_id] then
                return create_nodes_execution({ {
                    node_id = parent_id,
                    node_type = parent.type,
                    path = {},
                    trigger_reason = TRIGGER_REASON.YIELD_DRIVEN,
                } })
            end
            goto continue
        end

        -- signal yields wait for external data, not child completion
        if yield_info.wait_for_signal then
            if yield_info.signal_data ~= nil then
                return create_decision(DECISION_TYPE.SATISFY_YIELD, {
                    parent_id = parent_id,
                    yield_id = yield_info.yield_id,
                    reply_to = yield_info.reply_to,
                    results = yield_info.signal_data
                })
            end
            if signal_timeout_expired(yield_info, now) then
                return create_decision(DECISION_TYPE.SATISFY_YIELD, {
                    parent_id = parent_id,
                    yield_id = yield_info.yield_id,
                    reply_to = yield_info.reply_to,
                    results = signal_timeout_result(parent_id, yield_info)
                })
            end
            -- signal not received yet, skip this yield
        elseif yield_children_complete(yield_info) then
            return create_decision(DECISION_TYPE.SATISFY_YIELD, {
                parent_id = parent_id,
                yield_id = yield_info.yield_id,
                reply_to = yield_info.reply_to,
                results = yield_info.results or {}
            })
        end

        ::continue::
    end

    local ready_yield_children = {}

    for parent_id, yield_info in pairs(state.active_yields) do
        if node_is_terminal(state.nodes[parent_id]) then goto continue end

        if yield_info.pending_children then
            local has_any_pending = false
            local has_any_runnable = false
            local has_any_running = false

            for child_id, _ in pairs(yield_info.pending_children) do
                local child_node = state.nodes[child_id]
                if child_node then
                    if child_node.status == consts.STATUS.RUNNING then
                        has_any_running = true
                    elseif child_node.status == consts.STATUS.PENDING then
                        has_any_pending = true
                        if node_has_required_inputs(child_id, child_node, state.input_tracker) then
                            has_any_runnable = true
                            table.insert(ready_yield_children, {
                                node_id = child_id,
                                node_type = child_node.type,
                                path = yield_info.child_path or {},
                                trigger_reason = TRIGGER_REASON.YIELD_DRIVEN,
                                parent_id = parent_id
                            })
                        end
                    end
                end
            end

            if has_any_pending and not has_any_runnable and not has_any_running then
                return create_decision(DECISION_TYPE.NO_WORK, {
                    message = "Yield children pending for parent " .. parent_id .. ": waiting for inputs"
                })
            end
        end

        ::continue::
    end

    if #ready_yield_children > 0 then
        return create_nodes_execution({ ready_yield_children[1] })
    end

    return nil
end

function scheduler.next_wake_duration(state)
    local now = time.now()
    local soonest_ns = nil

    for _, yield_info in pairs(state.active_yields or {}) do
        if yield_info.wait_for_signal and not yield_info.detached and yield_info.signal_data == nil then
            local deadline = parse_timeout_deadline(yield_info.timeout_deadline)
            if deadline then
                if now:after(deadline) or now:equal(deadline) then
                    return 0
                end

                local remaining_ns = deadline:sub(now):nanoseconds()
                if remaining_ns > 0 and (soonest_ns == nil or remaining_ns < soonest_ns) then
                    soonest_ns = remaining_ns
                end
            end
        end
    end

    return soonest_ns
end

local function find_input_ready_work(state)
    local ready_nodes = {}

    for node_id, node_data in pairs(state.nodes) do
        if node_data.status == consts.STATUS.PENDING and
           not state.active_processes[node_id] and
           not state.active_yields[node_id] and
           state.input_tracker.requirements[node_id] and
           node_has_required_inputs(node_id, node_data, state.input_tracker) then

            local is_yield_child = false
            for _, yield_info in pairs(state.active_yields) do
                if yield_info.pending_children and yield_info.pending_children[node_id] then
                    is_yield_child = true
                    break
                end
            end

            if not is_yield_child then
                table.insert(ready_nodes, {
                    node_id = node_id,
                    node_type = node_data.type,
                    path = {},
                    trigger_reason = TRIGGER_REASON.INPUT_READY
                })
            end
        end
    end

    return decide_execution_strategy(ready_nodes, CONCURRENT_CONFIG.ENABLE_INPUT_CONCURRENCY)
end

local function find_root_driven_work(state)
    local ready_nodes = {}

    for node_id, node_data in pairs(state.nodes) do
        if node_data.status == consts.STATUS.PENDING and
           not state.active_processes[node_id] and
           not state.active_yields[node_id] and
           not state.input_tracker.requirements[node_id] and
           node_has_available_inputs(node_id, state.input_tracker) then

            table.insert(ready_nodes, {
                node_id = node_id,
                node_type = node_data.type,
                path = {},
                trigger_reason = TRIGGER_REASON.ROOT_READY
            })
        end
    end

    return decide_execution_strategy(ready_nodes, CONCURRENT_CONFIG.ENABLE_ROOT_CONCURRENCY)
end

function decide_execution_strategy(ready_nodes, allow_concurrent)
    if #ready_nodes == 0 then
        return nil
    elseif #ready_nodes == 1 then
        return create_nodes_execution(ready_nodes)
    elseif allow_concurrent then
        local limit = math.min(#ready_nodes, CONCURRENT_CONFIG.MAX_CONCURRENT_NODES)
        local nodes_to_execute = {}
        for i = 1, limit do
            table.insert(nodes_to_execute, ready_nodes[i])
        end
        return create_nodes_execution(nodes_to_execute)
    else
        return create_nodes_execution({ ready_nodes[1] })
    end
end

local function check_workflow_completion(state)
    if next(state.active_processes) then
        return nil
    end

    for parent_id in pairs(state.active_yields) do
        if not node_is_terminal(state.nodes[parent_id]) then return nil end
    end

    local has_nodes = false
    for _ in pairs(state.nodes) do
        has_nodes = true
        break
    end

    if not has_nodes then
        return create_decision(DECISION_TYPE.COMPLETE_WORKFLOW, {
            success = true,
            message = "Empty workflow completed"
        })
    end

    if state.has_workflow_error then
        return create_decision(DECISION_TYPE.COMPLETE_WORKFLOW, {
            success = false,
            message = "Workflow terminated with error"
        })
    end

    if state.has_workflow_output then
        return create_decision(DECISION_TYPE.COMPLETE_WORKFLOW, {
            success = true,
            message = "Workflow completed successfully"
        })
    end

    local has_pending = false
    local runnable_nodes = 0
    local nodes_with_no_requirements_or_inputs = 0

    for node_id, node_data in pairs(state.nodes) do
        if node_data.status == consts.STATUS.PENDING then
            has_pending = true

            local has_requirements = state.input_tracker.requirements[node_id] ~= nil
            local has_inputs = node_has_available_inputs(node_id, state.input_tracker)

            if has_requirements then
                if node_has_required_inputs(node_id, node_data, state.input_tracker) then
                    runnable_nodes = runnable_nodes + 1
                end
            elseif has_inputs then
                runnable_nodes = runnable_nodes + 1
            else
                nodes_with_no_requirements_or_inputs = nodes_with_no_requirements_or_inputs + 1
            end
        end
    end

    if not has_pending then
        return create_decision(DECISION_TYPE.COMPLETE_WORKFLOW, {
            success = false,
            message = "Workflow completed without producing output"
        })
    end

    if runnable_nodes == 0 then
        local message = "Workflow failed to produce output"
        if nodes_with_no_requirements_or_inputs > 0 then
            message = "No input data provided"
        else
            message = "Workflow deadlocked: nodes pending but no inputs available"
        end

        return create_decision(DECISION_TYPE.COMPLETE_WORKFLOW, {
            success = false,
            message = message
        })
    end

    return nil
end

local function passivation_wake(state)
    if next(state.active_processes) then return false end
    local found = false
    local wake_at = nil
    for parent_id, yield_info in pairs(state.active_yields or {}) do
        if node_is_terminal(state.nodes[parent_id]) then goto continue end
        found = true
        if not yield_info.wait_for_signal or yield_info.signal_data ~= nil then
            return false, nil
        end
        local candidate = yield_info.timeout_deadline
        if type(candidate) == "string" and candidate ~= "" and
           (wake_at == nil or candidate < wake_at) then
            wake_at = candidate
        end
        ::continue::
    end
    return found, wake_at
end

function scheduler.find_next_work(state)
    local decision = find_yield_driven_work(state)
    if decision then
        return decision
    end

    decision = find_input_ready_work(state)
    if decision then
        return decision
    end

    decision = find_root_driven_work(state)
    if decision then
        return decision
    end


    local passivate, wake_at = passivation_wake(state)
    if passivate then
        return create_decision(DECISION_TYPE.PASSIVATE, {
            message = "Signal waits are durable; releasing resident processes",
            wake_at = wake_at,
        })
    end

    decision = check_workflow_completion(state)
    if decision then
        return decision
    end

    return create_decision(DECISION_TYPE.NO_WORK, {
        message = "No work available, waiting for events"
    })
end

function scheduler.create_empty_state()
    return {
        nodes = {},
        active_yields = {},
        active_processes = {},
        input_tracker = {
            requirements = {},
            available = {}
        },
        has_workflow_output = false,
        has_workflow_error = false
    }
end

scheduler.DECISION_TYPE = DECISION_TYPE

return scheduler
