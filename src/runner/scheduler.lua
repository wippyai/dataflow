local consts = require("consts")

local scheduler = {}

local DECISION_TYPE = {
    EXECUTE_NODES = "execute_nodes",
    SATISFY_YIELD = "satisfy_yield",
    COMPLETE_WORKFLOW = "complete_workflow",
    NO_WORK = "no_work"
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
        if status == consts.STATUS.PENDING then
            return false
        end
    end

    return true
end

local function find_yield_driven_work(state)
    for parent_id, yield_info in pairs(state.active_yields) do
        if yield_children_complete(yield_info) then
            return create_decision(DECISION_TYPE.SATISFY_YIELD, {
                parent_id = parent_id,
                yield_id = yield_info.yield_id,
                reply_to = yield_info.reply_to,
                results = yield_info.results or {}
            })
        end
    end

    local ready_yield_children = {}

    for parent_id, yield_info in pairs(state.active_yields) do
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
                return create_decision(DECISION_TYPE.COMPLETE_WORKFLOW, {
                    success = false,
                    message = "Yield deadlock at parent " .. parent_id .. ": children pending but inputs unavailable"
                })
            end
        end
    end

    if #ready_yield_children > 0 then
        return create_nodes_execution({ ready_yield_children[1] })
    end

    return nil
end

local function find_input_ready_work(state)
    local ready_nodes = {}

    for node_id, node_data in pairs(state.nodes) do
        if node_data.status == consts.STATUS.PENDING and
           not state.active_processes[node_id] and
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
    if next(state.active_processes) or next(state.active_yields) then
        return nil
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