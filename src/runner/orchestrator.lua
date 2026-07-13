local uuid = require("uuid")
local json = require("json")
local time = require("time")
local consts = require("consts")
local security = require("security")
local logger = require("logger"):named("dataflow.orchestrator")

local orchestrator = {
    workflow_state = require("workflow_state"),
    scheduler = require("scheduler"),
    process = process,
    channel = channel,
    funcs = require("funcs"),
    commit = require("commit"),
    wake_repo = require("wake_repo"),
    wake_process = require("wake_process"),
    security = security
}

local TERMINAL_STATUS = {
    [consts.STATUS.COMPLETED_SUCCESS] = true,
    [consts.STATUS.COMPLETED_FAILURE] = true,
    [consts.STATUS.CANCELLED] = true,
    [consts.STATUS.TERMINATED] = true
}

---Invoke the legacy on_complete hook at most once in this orchestrator life.
---Best-effort: hook failures are logged and never block completion persistence,
---which has already happened by the time this runs.
---@param state table Orchestrator state
---@param result table|nil Final orchestration result
local function fire_completion_hook(state, result)
    if state.completion_hook_fired then
        return
    end
    state.completion_hook_fired = true

    local hook_id = state.on_complete_id
    if type(hook_id) ~= "string" or hook_id == "" then
        return
    end

    local status = state.final_status
    if type(status) ~= "string" or status == "" then
        status = (result and result.success) and consts.STATUS.COMPLETED_SUCCESS or consts.STATUS.COMPLETED_FAILURE
    end

    local hook_args: { [string]: any } = {
        dataflow_id = state.dataflow_id,
        status = status
    }
    if result and result.error then
        hook_args.error = result.error
    end

    local executor = orchestrator.funcs.new()
    if state.actor then
        executor = executor:with_actor(state.actor)
    end
    if state.scope then
        executor = executor:with_scope(state.scope)
    end

    local ok, call_err = pcall(function()
        local _, err = executor:call(hook_id, hook_args)
        if err then
            error(err)
        end
    end)
    if not ok then
        logger:warn("on_complete hook failed", {
            dataflow_id = state.dataflow_id,
            hook = hook_id,
            error = tostring(call_err)
        })
    end
end

---Fire the completion hook and return the result. Used at every terminal exit of run().
---@param state table Orchestrator state
---@param result table Final orchestration result
---@return table result
local function finish(state, result)
    if TERMINAL_STATUS[state.final_status] then fire_completion_hook(state, result) end
    return result
end

local function workflow_identity(actor_id: string?, dataflow_id: string): (any?, any?, string?)
    if type(actor_id) ~= "string" or actor_id == "" then
        return nil, nil, nil
    end
    local current_actor = orchestrator.security.actor()
    if current_actor and current_actor:id() == actor_id then
        return current_actor, orchestrator.security.scope(), nil
    end
    return nil, nil, "workflow " .. dataflow_id .. " started under the wrong actor"
end

---Execute a single node
---@param state table Orchestrator state
---@param node_info table Node execution information
---@return string|nil error Error message if spawn failed
local function execute_single_node(state, node_info)
    local node_id = node_info.node_id
    local node_type = node_info.node_type
    local path = node_info.path or {}

    if type(node_type) ~= "string" or node_type == "" then
        return "Invalid node type for node: " .. tostring(node_id)
    end

    if state.active_processes[node_id] then
        return nil -- Already running, skip
    end

    local node_data = state.workflow_state:get_node(node_id)
    if not node_data then
        return "Node not found: " .. node_id
    end

    local spawner = orchestrator.process.with_context({})
    if state.actor then
        spawner = spawner:with_actor(state.actor)
    end
    if state.scope then
        spawner = spawner:with_scope(state.scope)
    end

    local pid, err_spawn = spawner:spawn_linked_monitored(node_type, consts.HOST_ID, {
        dataflow_id = state.dataflow_id,
        node_id = node_id,
        node = node_data,
        path = path
    })

    if not pid then
        return "Failed to spawn node process for node: " .. node_id .. ". Reason: " .. tostring(err_spawn)
    end

    state.workflow_state:track_process(node_id, pid)
    state.active_processes[node_id] = { pid = pid, path = path }

    return nil
end

---Process pending commits immediately
---@param state table Orchestrator state
---@return boolean success Whether processing succeeded
local function process_pending_commits(state: any)
    if #state.incoming_commit_queue == 0 then
        return true
    end

    -- Find new commits to process
    local commits_to_process = {}
    for _, commit_id in ipairs(state.incoming_commit_queue) do
        local already_processed = false
        for _, processed_id in ipairs(state.processed_commit_ids) do
            if processed_id == commit_id then
                already_processed = true
                break
            end
        end
        if not already_processed then
            table.insert(commits_to_process, commit_id)
        end
    end

    if #commits_to_process == 0 then
        return true
    end

    local result, err = state.workflow_state:process_commits(commits_to_process)
    if err then
        state.workflow_state:queue_commands({
            type = consts.COMMAND_TYPES.UPDATE_WORKFLOW,
            payload = {
                status = consts.STATUS.COMPLETED_FAILURE,
                metadata = { error = "Commit processing failed: " .. err }
            }
        })
        local persist_result, persist_err = state.workflow_state:persist()
        state.final_status = consts.STATUS.COMPLETED_FAILURE
        state.exit_result = {
            success = false,
            dataflow_id = state.dataflow_id,
            error = "Commit processing failed: " .. err
        }
        state.running = false
        return false
    end

    for _, commit_id in ipairs(commits_to_process) do
        table.insert(state.processed_commit_ids, commit_id)
    end

    return true
end

---Load pending commits from durable storage for crash/restart recovery
---@param state table Orchestrator state
---@return boolean success Whether loading succeeded
local function load_startup_pending_commits(state: any)
    local pending_commit_ids, pending_err = orchestrator.commit.get_pending_commits(state.dataflow_id)
    if pending_err then
        local failure_message = "Failed to load pending commits: " .. pending_err
        state.workflow_state:queue_commands({
            type = consts.COMMAND_TYPES.UPDATE_WORKFLOW,
            payload = {
                status = consts.STATUS.COMPLETED_FAILURE,
                metadata = { error = failure_message }
            }
        })
        local _persist_result, _persist_err = state.workflow_state:persist()
        state.final_status = consts.STATUS.COMPLETED_FAILURE
        state.exit_result = {
            success = false,
            dataflow_id = state.dataflow_id,
            error = failure_message
        }
        state.running = false
        return false
    end

    for _, commit_id in ipairs(pending_commit_ids or {}) do
        table.insert(state.incoming_commit_queue, commit_id)
    end

    return true
end

---Call scheduler and handle the result immediately
---@param state table Orchestrator state
---@return boolean continue Whether to continue processing
local function call_scheduler_and_handle(state: any)
    -- loop through SATISFY_YIELD decisions: they mutate state (clear active_yields)
    -- but don't guarantee forward progress on their own, especially when the yield's
    -- parent process is dead (recovery case). keep scheduling until a node starts,
    -- the workflow completes, or no more work can be dispatched.
    local max_iterations = 64
    while max_iterations > 0 do
        max_iterations = max_iterations - 1

        local snapshot = state.workflow_state:get_scheduler_snapshot()
        local decision = orchestrator.scheduler.find_next_work(snapshot)

        if decision.type == orchestrator.scheduler.DECISION_TYPE.EXECUTE_NODES then
            return handle_execute_nodes(state, decision.payload)
        elseif decision.type == orchestrator.scheduler.DECISION_TYPE.COMPLETE_WORKFLOW then
            return handle_complete_workflow(state, decision.payload)
        elseif decision.type == orchestrator.scheduler.DECISION_TYPE.SATISFY_YIELD then
            local cont = handle_satisfy_yield(state, decision.payload)
            if not cont or not state.running then
                return cont
            end
            -- re-enter the loop: yield satisfied, state changed, re-schedule
        elseif decision.type == orchestrator.scheduler.DECISION_TYPE.PASSIVATE then
            state.workflow_state:queue_commands({
                type = consts.COMMAND_TYPES.UPDATE_WORKFLOW,
                payload = { status = consts.STATUS.WAITING },
            })
            local _, status_err = state.workflow_state:persist()
            if status_err then
                state.running = false
                state.exit_result = {
                    success = false,
                    dataflow_id = state.dataflow_id,
                    error = "Failed to persist waiting status: " .. tostring(status_err),
                }
                return false
            end
            local unclaimed_wakes = {}
            if type(state.workflow_state.take_unclaimed_signal_wake_keys) == "function" then
                unclaimed_wakes = state.workflow_state:take_unclaimed_signal_wake_keys()
            end
            for _, wake_key in ipairs(unclaimed_wakes) do
                local _, cleanup_err = orchestrator.wake_repo.remove(state.dataflow_id, wake_key)
                if cleanup_err then
                    logger:warn("unclaimed signal wake cleanup failed", {
                        dataflow_id = state.dataflow_id,
                        wake_key = wake_key,
                        error = tostring(cleanup_err),
                    })
                end
            end
            -- NODE_YIELD projected its deadline atomically. Do not rewrite the
            -- wake here: a concurrent NODE_SIGNAL may already have replaced it
            -- with an immediate wake between this decision and persistence.
            orchestrator.wake_process.notify()
            state.running = false
            state.exit_result = {
                success = true,
                pending = true,
                passivated = true,
                dataflow_id = state.dataflow_id,
            }
            return false
        else
            return true
        end
    end

    return true
end

---Handle node execution immediately
---@param state table Orchestrator state
---@param payload table Execution payload
---@return boolean continue Whether to continue processing
function handle_execute_nodes(state: any, payload: any)
    local nodes = payload.nodes or {}

    if #nodes == 0 then
        return true
    end

    -- Filter out already running nodes
    local nodes_to_execute = {}
    for _, node_info in ipairs(nodes) do
        local node_id = node_info.node_id
        if not state.active_processes[node_id] then
            table.insert(nodes_to_execute, node_info)
        end
    end

    if #nodes_to_execute == 0 then
        return true
    end

    -- Update all nodes to RUNNING status first
    local commands = {}
    for _, node_info in ipairs(nodes_to_execute) do
        table.insert(commands, {
            type = consts.COMMAND_TYPES.UPDATE_NODE,
            payload = {
                node_id = node_info.node_id,
                status = consts.STATUS.RUNNING
            }
        })
    end

    -- Update workflow status if needed
    if not state.workflow_status_updated then
        table.insert(commands, {
            type = consts.COMMAND_TYPES.UPDATE_WORKFLOW,
            payload = {
                status = consts.STATUS.RUNNING
            }
        })
        state.workflow_status_updated = true
    end

    state.workflow_state:queue_commands(commands)
    local result, err = state.workflow_state:persist()
    if err then
        local fail_msg = "Failed to persist RUNNING status for nodes: " .. err
        local fail_commands = {}
        for _, node_info in ipairs(nodes_to_execute) do
            table.insert(fail_commands, {
                type = consts.COMMAND_TYPES.UPDATE_NODE,
                payload = {
                    node_id = node_info.node_id,
                    status = consts.STATUS.COMPLETED_FAILURE,
                    metadata = { error = fail_msg }
                }
            })
        end
        table.insert(fail_commands, {
            type = consts.COMMAND_TYPES.UPDATE_WORKFLOW,
            payload = {
                status = consts.STATUS.COMPLETED_FAILURE,
                metadata = { error = fail_msg }
            }
        })
        state.workflow_state:queue_commands(fail_commands)
        local persist_result, persist_err = state.workflow_state:persist()
        state.final_status = consts.STATUS.COMPLETED_FAILURE
        state.exit_result = {
            success = false,
            dataflow_id = state.dataflow_id,
            error = fail_msg
        }
        state.running = false
        return false
    end

    -- Spawn processes
    local execution_failures = {}
    for _, node_info in ipairs(nodes_to_execute) do
        local spawn_err = execute_single_node(state, node_info)
        if spawn_err then
            table.insert(execution_failures, {
                node_id = node_info.node_id,
                error = spawn_err
            })
        end
    end

    -- Handle any spawn failures
    if #execution_failures > 0 then
        local fail_commands = {}
        local error_messages = {}

        for _, failure in ipairs(execution_failures) do
            table.insert(fail_commands, {
                type = consts.COMMAND_TYPES.UPDATE_NODE,
                payload = {
                    node_id = failure.node_id,
                    status = consts.STATUS.COMPLETED_FAILURE,
                    metadata = { error = failure.error }
                }
            })
            table.insert(error_messages, failure.node_id .. ": " .. failure.error)
        end

        local combined_error = "Node spawn failures: " .. table.concat(error_messages, "; ")
        table.insert(fail_commands, {
            type = consts.COMMAND_TYPES.UPDATE_WORKFLOW,
            payload = {
                status = consts.STATUS.COMPLETED_FAILURE,
                metadata = { error = combined_error }
            }
        })

        state.workflow_state:queue_commands(fail_commands)
        local persist_result, persist_err = state.workflow_state:persist()
        state.final_status = consts.STATUS.COMPLETED_FAILURE
        state.exit_result = {
            success = false,
            dataflow_id = state.dataflow_id,
            error = combined_error
        }
        state.running = false
        return false
    end

    return true
end

---Handle yield satisfaction immediately
---@param state table Orchestrator state
---@param payload table Yield payload
---@return boolean continue Whether to continue processing
function handle_satisfy_yield(state: any, payload: any)
    local parent_id = payload.parent_id
    local yield_id = payload.yield_id
    local reply_to = payload.reply_to
    local results = payload.results or {}
    if type(parent_id) ~= "string" then
        return true
    end
    if type(results) ~= "table" then
        results = {}
    end

    -- Queue yield satisfaction commands
    state.workflow_state:satisfy_yield(parent_id, results)

    -- Persist queued commands BEFORE sending reply
    local persist_result, persist_err = state.workflow_state:persist()
    if persist_err then
        return true
    end
    -- NODE_YIELD_RESULT consumes its durable wake in the same transaction.
    -- Notify only after commit so the exact-deadline service can release its
    -- delivery monitor and move to the next indexed deadline.
    orchestrator.wake_process.notify()

    -- Send reply to yielding process ONLY AFTER successful persistence
    local process_info = state.active_processes[parent_id]
    if process_info and type(reply_to) == "string" then
        orchestrator.process.send(tostring(process_info.pid), reply_to, {
            yield_id = yield_id,
            response_data = {
                ok = true,
                run_node_results = results,
                all_completed = true
            }
        })
    end

    return true
end

---Handle workflow completion immediately
---@param state table Orchestrator state
---@param payload table Completion payload
---@return boolean continue Whether to continue processing (always false)
function handle_complete_workflow(state: any, payload: any)
    local success = payload.success
    local message = payload.message
    local final_status = success and consts.STATUS.COMPLETED_SUCCESS or consts.STATUS.COMPLETED_FAILURE

    -- If workflow failed, get detailed node error information
    local detailed_error = message
    if not success then
        local failed_node_errors = state.workflow_state:get_failed_node_errors()
        if failed_node_errors then
            detailed_error = failed_node_errors
        elseif not message then
            detailed_error = "Workflow failed"
        end
    end

    local commands = {
        {
            type = consts.COMMAND_TYPES.UPDATE_WORKFLOW,
            payload = {
                status = final_status,
                metadata = { error = not success and detailed_error or nil }
            }
        }
    }

    state.workflow_state:queue_commands(commands)
    local persist_result, persist_err = state.workflow_state:persist()

    if persist_err then
        state.exit_result = {
            success = false,
            dataflow_id = state.dataflow_id,
            error = "Failed to persist workflow completion: " .. tostring(persist_err),
        }
        state.running = false
        return false
    end

    local _, clear_err = orchestrator.wake_repo.clear(state.dataflow_id)
    if clear_err then logger:warn("terminal wake cleanup failed", { dataflow_id = state.dataflow_id, error = tostring(clear_err) }) end
    orchestrator.wake_process.notify()

    state.final_status = final_status

    if success then
        state.exit_result = {
            success = true,
            dataflow_id = state.dataflow_id,
            output = { message = message or "Workflow completed successfully" }
        }
    else
        state.exit_result = {
            success = false,
            dataflow_id = state.dataflow_id,
            error = detailed_error or "Workflow failed"
        }
    end

    state.running = false
    return false
end

-- Invoke a persisted park arm under the workflow's recovered authority. The
-- declaration contains only a function ref and data; it cannot override actor
-- or scope. Returns a structured error payload for the waiting node.
function orchestrator.arm_parked_yield(state: any, arm: any, idempotency_key: string?): any?
    local declaration = type(arm) == "table" and arm or {}
    local arm_ref = type(declaration.ref) == "string" and declaration.ref or ""
    if arm_ref == "" then
        return { code = "PARK_ARM_FAILED", message = "park arm.ref is required" }
    end

    local executor = orchestrator.funcs.new()
    if state.actor then executor = executor:with_actor(state.actor) end
    if state.scope then executor = executor:with_scope(state.scope) end
    local arm_args = {}
    for key, value in pairs(type(declaration.args) == "table" and declaration.args or {}) do arm_args[key] = value end
    -- Reserved by Dataflow: retries across any number of process crashes must
    -- address the same external side effect.
    arm_args.idempotency_key = idempotency_key
    local ok, result_or_error, call_err = pcall(function()
        return executor:call(arm_ref, arm_args)
    end)
    if not ok then
        return { code = "PARK_ARM_FAILED", message = tostring(result_or_error) }
    end
    if call_err then
        return { code = "PARK_ARM_FAILED", message = tostring(call_err) }
    end
    return nil
end

-- Track first, then arm, then acknowledge. The caller invokes the scheduler only
-- after this returns, so even an already-persisted signal cannot beat the ACK to
-- the node's reply mailbox.
function orchestrator.track_signal_yield(state: any, node_id: string, yield_info: any, from_pid: any)
    local before = state.workflow_state:get_scheduler_snapshot().active_yields[node_id]
    yield_info.episode_id = yield_info.yield_id
    yield_info.wake_keys = { "yield:" .. tostring(yield_info.yield_id) }
    if before and before.signal_id == yield_info.signal_id then
        -- A reattached node keeps the original absolute deadline. Recomputing
        -- it from config would make every restart extend the timeout.
        yield_info.timeout = before.timeout
        yield_info.timeout_ms = before.timeout_ms
        yield_info.timeout_deadline = before.timeout_deadline
        yield_info.episode_id = before.episode_id or before.yield_id
        yield_info.wake_keys = {}
        for _, wake_key in ipairs(type(before.wake_keys) == "table" and before.wake_keys or
            { "yield:" .. tostring(before.yield_id) }) do
            table.insert(yield_info.wake_keys, wake_key)
        end
        table.insert(yield_info.wake_keys, "yield:" .. tostring(yield_info.yield_id))
        yield_info.signal_data = before.signal_data
        yield_info.signal_wake_key = before.signal_wake_key
        yield_info.signal_wake_keys = before.signal_wake_keys
    end
    local has_arm = type(yield_info.arm) == "table" and type(yield_info.arm.ref) == "string"
    local already_armed = not has_arm or (before and before.arm_completed == true)
    state.workflow_state:track_yield(node_id, yield_info)
    if yield_info.park_ack ~= true or type(yield_info.reply_to) ~= "string" or not yield_info.yield_id then
        return
    end

    -- A durable signal or due wake may already be present when this passivated
    -- node reattaches. Persist satisfaction and combine it with the park ACK so
    -- the node never exits between acknowledgement and its resume value.
    local decision = orchestrator.scheduler.find_next_work(state.workflow_state:get_scheduler_snapshot())
    if decision.type == orchestrator.scheduler.DECISION_TYPE.SATISFY_YIELD and
       decision.payload.parent_id == node_id then
        state.workflow_state:satisfy_yield(node_id, decision.payload.results or {})
        local _, persist_err = state.workflow_state:persist()
        if persist_err then
            state.workflow_state:abandon_yield(node_id)
            orchestrator.process.send(tostring(from_pid), yield_info.reply_to, {
                yield_id = yield_info.yield_id,
                parked = false,
                error = { code = "PARK_RESUME_FAILED", message = tostring(persist_err) },
            })
            return
        end
        orchestrator.wake_process.notify()
        orchestrator.process.send(tostring(from_pid), yield_info.reply_to, {
            yield_id = yield_info.yield_id,
            parked = true,
            response_data = {
                ok = true,
                run_node_results = decision.payload.results or {},
                all_completed = true,
            },
        })
        return
    end

    local arm_error = nil
    local arm_key = state.dataflow_id .. ":park:" .. tostring(yield_info.episode_id)
    if not already_armed then arm_error = orchestrator.arm_parked_yield(state, yield_info.arm, arm_key) end
    if not arm_error and has_arm then
        state.workflow_state:queue_commands({
            type = consts.COMMAND_TYPES.CREATE_DATA,
            payload = {
                data_id = uuid.v7(),
                data_type = consts.DATA_TYPE.NODE_PARK_ARMED,
                content = { armed = true },
                key = yield_info.yield_id,
                node_id = node_id,
            },
        })
        local _, armed_err = state.workflow_state:persist()
        if armed_err then arm_error = { code = "PARK_ARM_STATE_FAILED", message = tostring(armed_err) } end
    end
    if arm_error then
        state.workflow_state:abandon_yield(node_id)
        orchestrator.process.send(tostring(from_pid), yield_info.reply_to, {
            yield_id = yield_info.yield_id,
            parked = false,
            error = arm_error,
        })
    else
        local prepared, prepare_err = state.workflow_state:prepare_passivation(node_id)
        if not prepared then
            orchestrator.process.send(tostring(from_pid), yield_info.reply_to, {
                yield_id = yield_info.yield_id,
                parked = false,
                error = { code = "PARK_PASSIVATION_FAILED", message = tostring(prepare_err) },
            })
            return
        end
        local _, persist_err = state.workflow_state:persist()
        if persist_err then
            orchestrator.process.send(tostring(from_pid), yield_info.reply_to, {
                yield_id = yield_info.yield_id,
                parked = false,
                error = { code = "PARK_PASSIVATION_FAILED", message = tostring(persist_err) },
            })
            return
        end
        if type(yield_info.timeout_deadline) == "string" then orchestrator.wake_process.notify() end
        local ack_sent = orchestrator.process.send(tostring(from_pid), yield_info.reply_to, {
            yield_id = yield_info.yield_id,
            parked = true,
        })
        if not ack_sent then
            -- The durable WAITING transition is already committed. Do not
            -- leave a live node blocked forever on a reply that never arrived;
            -- LINK_DOWN recovery will detach it and the targeted wake retries.
            orchestrator.process.terminate(tostring(from_pid))
        end
    end
end

---Handle yield request immediately
---@param state table Orchestrator state
---@param msg_payload table Yield request payload
---@param from_pid string Process ID that sent the request
local function handle_yield_request(state: any, msg_payload: any, from_pid: any)
    local node_id = nil
    local current_path = nil
    for nid, process_info in pairs(state.active_processes) do
        if process_info.pid == from_pid then
            node_id = nid
            current_path = process_info.path or {}
            break
        end
    end

    if not node_id then
        return
    end

    local yield_id = msg_payload and msg_payload.request_context and msg_payload.request_context.yield_id
    local yield_context = msg_payload and msg_payload.yield_context or {}
    local run_nodes = yield_context.run_nodes or {}
    if type(run_nodes) ~= "table" then
        run_nodes = {}
    end

    if #run_nodes == 0 then
        local wait_for_signal = yield_context.wait_for_signal
        local reply_to = msg_payload and msg_payload.request_context and msg_payload.request_context.reply_to

        if wait_for_signal then
            -- signal yield: track the yield and wait for an external NODE_SIGNAL CREATE_DATA
            -- commit (client:signal writes it durably; the scheduler satisfies the yield on arrival)
            local yield_info = {
                yield_id = yield_id,
                reply_to = reply_to,
                signal_id = yield_context.signal_id or yield_id,
                timeout = yield_context.timeout,
                timeout_ms = yield_context.timeout_ms,
                timeout_deadline = yield_context.timeout_deadline,
                pending_children = {},
                results = {},
                wait_for_signal = true,
                park_ack = yield_context.park_ack == true,
                arm = yield_context.arm,
            }
            orchestrator.track_signal_yield(state, tostring(node_id), yield_info, from_pid)
        elseif type(reply_to) == "string" and yield_id then
            orchestrator.process.send(tostring(from_pid), reply_to, {
                yield_id = yield_id,
                response_data = {
                    ok = true,
                    run_node_results = {},
                    all_completed = true
                }
            })
        end
    else
        local child_path = {}
        for _, ancestor_id in ipairs(current_path) do
            table.insert(child_path, ancestor_id)
        end
        table.insert(child_path, node_id)

        local yield_info = {
            yield_id = yield_id,
            reply_to = msg_payload and msg_payload.request_context and msg_payload.request_context.reply_to,
            pending_children = {},
            results = {},
            child_path = child_path
        }

        -- Only track non-template nodes in pending_children
        for _, child_id in ipairs(run_nodes) do
            if type(child_id) == "string" then
                local child_node = state.workflow_state:get_node(child_id)
                if child_node and child_node.status ~= consts.STATUS.TEMPLATE then
                    -- Recovery may re-establish a barrier after some or all
                    -- children have already committed terminal state. Preserve
                    -- that durable status instead of inventing pending work
                    -- that can never emit another EXIT event.
                    yield_info.pending_children[child_id] = child_node.status
                    if child_node.status == consts.STATUS.COMPLETED_SUCCESS or
                        child_node.status == consts.STATUS.COMPLETED_FAILURE then
                        local result_data_id = type(state.workflow_state.get_node_result_data_id) == "function" and
                            state.workflow_state:get_node_result_data_id(child_id) or nil
                        if result_data_id then yield_info.results[child_id] = result_data_id end
                    end
                end
            end
        end

        state.workflow_state:track_yield(node_id, yield_info)
    end
end

---Handle process events immediately
---@param state table Orchestrator state
---@param event table Process event
---@return boolean continue Whether to continue processing
local function handle_process_event(state: any, event: any)
    if event.kind ~= orchestrator.process.event.EXIT and event.kind ~= orchestrator.process.event.LINK_DOWN then
        return true
    end

    local from_pid = event.from
    local node_id = nil

    for nid, process_info in pairs(state.active_processes) do
        if process_info.pid == from_pid then
            node_id = nid
            break
        end
    end

    if not node_id then
        return true
    end


    local snapshot = state.workflow_state:get_scheduler_snapshot()
    local parked = snapshot.active_yields and snapshot.active_yields[node_id]
    local parked_node = snapshot.nodes and snapshot.nodes[node_id]
    local event_value = event.result and event.result.value
    local clean_exit = event.kind == orchestrator.process.event.EXIT and
        (not event.result or not event.result.error) and
        not (type(event_value) == "table" and event_value.success == false)
    local prepared_link_down = event.kind == orchestrator.process.event.LINK_DOWN and
        parked and parked.detached == true and parked_node and parked_node.status == consts.STATUS.WAITING
    if (clean_exit or prepared_link_down) and parked and parked.park_ack == true and parked.wait_for_signal == true then
        state.active_processes[node_id] = nil
        state.workflow_state:passivate_process(from_pid)
        return true
    end

    state.active_processes[node_id] = nil

    local success = false
    local error_reason: any = "Unknown exit reason"
    local result_data = nil

    if event.kind == orchestrator.process.event.EXIT then
        if event.result then
            result_data = event.result.value

            if event.result.error then
                success = false
                error_reason = event.result.error
            elseif type(result_data) == "table" and result_data.success == false then
                success = false
                error_reason = result_data.error or result_data.message or "Node returned {success=false}"
            else
                success = true
            end
        else
            success = true
        end
    elseif event.kind == orchestrator.process.event.LINK_DOWN then
        success = false
        error_reason = "Node process linked down"
    end

    local terminal_result = result_data
    if not success and (type(result_data) ~= "table" or result_data.success ~= false) then
        terminal_result = error_reason
    end
    -- A child can submit its routed output immediately before EXIT. Apply that
    -- durable commit before deadlock analysis so newly-runnable descendants are
    -- never mistaken for unreachable branches and cancelled.
    if not load_startup_pending_commits(state) or not process_pending_commits(state) then
        return false
    end
    local exit_info = state.workflow_state:handle_process_exit(from_pid, success, terminal_result)

    local persist_result, persist_err = state.workflow_state:persist()

    if exit_info and exit_info.yield_complete then
        if not load_startup_pending_commits(state) then
            return false
        end
        if not process_pending_commits(state) then
            return false
        end
        return handle_satisfy_yield(state, {
            parent_id = exit_info.yield_complete.parent_id,
            yield_id = exit_info.yield_complete.yield_info.yield_id,
            reply_to = exit_info.yield_complete.yield_info.reply_to,
            results = exit_info.yield_complete.yield_info.results
        })
    end

    return true
end

---Handle commit message immediately
---@param state table Orchestrator state
---@param msg_payload table Commit payload
local function handle_commit_message(state: any, msg_payload: any)
    local commit_id = msg_payload and msg_payload.commit_id
    if commit_id then
        table.insert(state.incoming_commit_queue, commit_id)
    end
end

---Handle cancellation request
---@param state table Orchestrator state
---@param event table Cancel event
local function handle_cancellation(state: any, event: any)
    for node_id, process_info in pairs(state.active_processes) do
        if type(process_info.pid) == "string" then
            orchestrator.process.terminate(process_info.pid)
        end
    end

    state.workflow_state:queue_commands({
        type = consts.COMMAND_TYPES.UPDATE_WORKFLOW,
        payload = {
            status = consts.STATUS.CANCELLED,
            metadata = { cancellation_reason = "Received cancellation request" }
        }
    })
    local persist_result, persist_err = state.workflow_state:persist()
    orchestrator.wake_repo.clear(state.dataflow_id)
    orchestrator.wake_process.notify()

    state.final_status = consts.STATUS.CANCELLED
    state.exit_result = {
        success = false,
        dataflow_id = state.dataflow_id,
        error = "Workflow cancelled by request"
    }
    state.running = false
end

---Main orchestrator function
---@param args table Arguments containing dataflow_id and optional init_func_id
---@return table result Orchestration result with success/error
local function run(args)
    local dataflow_id_raw = args and args.dataflow_id
    local init_func_id = args and args.init_func_id

    if type(dataflow_id_raw) ~= "string" or dataflow_id_raw == "" then
        return { success = false, error = "Missing required dataflow_id" }
    end
    local dataflow_id = dataflow_id_raw

    local ws, ws_err = orchestrator.workflow_state.new(dataflow_id)
    if ws_err then
        return { success = false, error = "Failed to create workflow state: " .. ws_err }
    end
    if not ws then
        return { success = false, dataflow_id = dataflow_id, error = "Failed to create workflow state" }
    end
    local workflow_state = ws :: any

    -- Initialize state
    local state = ({
        dataflow_id = dataflow_id,
        workflow_state = workflow_state,
        active_processes = {},
        incoming_commit_queue = {},
        processed_commit_ids = {},
        workflow_status_updated = false,
        actor = nil :: any,
        scope = nil :: any,
        on_complete_id = nil :: any,
        running = true,
        exit_result = nil
    } :: any)

    -- Register process — if another orchestrator is already running, exit
    local _, reg_err = orchestrator.process.registry.register("dataflow." .. dataflow_id)
    if reg_err then
        return {
            success = true,
            dataflow_id = dataflow_id,
            error = nil,
            message = "Another orchestrator is already running for this workflow"
        }
    end
    orchestrator.process.set_options({ trap_links = true, upgradable = false })

    -- Load workflow state
    local result, load_err = workflow_state:load_state()
    if load_err then
        return {
            success = false,
            dataflow_id = dataflow_id,
            error = "Failed to load workflow state: " .. load_err
        }
    end

    -- Terminal-status guard: a respawned orchestrator (due wake, late signal,
    -- duplicate spawn) must not schedule work on an already-finished dataflow.
    -- The legacy completion hook is intentionally not replayed here: it is a
    -- best-effort compatibility callback, not a durable completion contract.
    local loaded_status = workflow_state:get_dataflow_status()
    if loaded_status and TERMINAL_STATUS[loaded_status] then
        local _, clear_err = orchestrator.wake_repo.clear(dataflow_id)
        if clear_err then
            return {
                success = false,
                dataflow_id = dataflow_id,
                error = "Failed to clear stale terminal wake: " .. tostring(clear_err),
            }
        end
        orchestrator.wake_process.notify()
        orchestrator.process.registry.unregister("dataflow." .. dataflow_id)
        return {
            success = true,
            dataflow_id = dataflow_id,
            message = "Dataflow already in terminal state: " .. loaded_status
        }
    end

    -- Resolve execution identity and the legacy completion hook before terminal
    -- paths. Metadata preserves the callback reference across orchestrator lives,
    -- but delivery remains best-effort; graph terminal nodes are the durable form.
    local raw_actor_id = workflow_state:get_actor_id()
    local actor_id: string? = nil
    if type(raw_actor_id) == "string" and raw_actor_id ~= "" then
        actor_id = raw_actor_id
    end
    local run_actor, run_scope, identity_err = workflow_identity(actor_id, dataflow_id)
    if identity_err then
        return finish(state, {
            success = false,
            dataflow_id = dataflow_id,
            error = identity_err,
        })
    end
    state.actor = run_actor
    state.scope = run_scope
    local runtime_state: any = state

    local dataflow_metadata = workflow_state:get_dataflow_metadata() or {}
    local metadata_hook = dataflow_metadata.on_complete
    if type(metadata_hook) == "string" and metadata_hook ~= "" then
        state.on_complete_id = metadata_hook
    elseif args and type(args.on_complete) == "string" and args.on_complete ~= "" then
        state.on_complete_id = args.on_complete
    end

    -- Recover commit backlog that may have accumulated while orchestrator was offline.
    -- This must happen before empty-workflow detection because commits can create nodes.
    local backlog_loaded = load_startup_pending_commits(runtime_state)
    if not backlog_loaded then
        return finish(state, state.exit_result or {
            success = false,
            dataflow_id = dataflow_id,
            error = "Failed to recover pending commits"
        })
    end

    local pending_processed = process_pending_commits(runtime_state)
    if not pending_processed then
        return finish(state, state.exit_result or {
            success = false,
            dataflow_id = dataflow_id,
            error = "Failed to process pending commits"
        })
    end

    -- Check for empty workflow after applying pending commits
    local nodes = workflow_state:get_nodes()
    local node_count = 0
    for _ in pairs(nodes) do
        node_count = node_count + 1
    end

    if node_count == 0 then
        return finish(state, {
            success = true,
            dataflow_id = dataflow_id,
            output = { message = "Empty workflow - no nodes to execute" }
        })
    end

    -- Call init function if provided
    if type(init_func_id) == "string" and init_func_id ~= "" then
        local executor = orchestrator.funcs.new()
        if state.actor then
            executor = executor:with_actor(state.actor)
        end
        if state.scope then
            executor = executor:with_scope(state.scope)
        end
        local _, _ = executor:call(init_func_id, {
            dataflow_id = dataflow_id,
            metadata = workflow_state:get_dataflow_metadata()
        })
    end

    -- Set up channels
    local inbox = orchestrator.process.inbox()
    local events = orchestrator.process.events()

    -- Initial scheduler call
    local continue = call_scheduler_and_handle(runtime_state)
    if not continue then
        return finish(state, state.exit_result or {
            success = false,
            dataflow_id = dataflow_id,
            error = "Orchestrator exited without result"
        })
    end

    -- Main processing loop. Signal waits leave through PASSIVATE; a durable
    -- signal commit or indexed due wake starts the next orchestrator life.
    while state.running do
        local select_cases = {
            inbox:case_receive(),
            events:case_receive()
        }

        local result = orchestrator.channel.select(select_cases)

        if not result.ok then
            break
        end

        if result.channel == inbox then
            local msg = result.value
            local topic = msg:topic()
            local payload = msg:payload():data()
            local payload_table = nil
            if type(payload) == "table" then
                payload_table = payload
            end
            local from_pid = msg:from()

            if topic == consts.MESSAGE_TOPIC.COMMIT then
                handle_commit_message(runtime_state, payload_table)
                local success = process_pending_commits(runtime_state)
                if success and state.running then
                    call_scheduler_and_handle(runtime_state)
                end
            elseif topic == consts.MESSAGE_TOPIC.YIELD_REQUEST then
                -- Process pending commits FIRST, before ANY yield handling
                local success = process_pending_commits(runtime_state)
                if success and state.running then
                    handle_yield_request(runtime_state, payload_table, from_pid)
                    call_scheduler_and_handle(runtime_state)
                end
            elseif topic == consts.MESSAGE_TOPIC.WAKE then
                -- The targeted wake row is already due. No status polling or
                -- broad scan: re-enter the pure scheduler against durable state.
                if payload_table and type(runtime_state.workflow_state.observe_signal_wake) == "function" then
                    runtime_state.workflow_state:observe_signal_wake(payload_table.wake_key)
                end
                local loaded = load_startup_pending_commits(runtime_state)
                if loaded then process_pending_commits(runtime_state) end
                call_scheduler_and_handle(runtime_state)
            end
        elseif result.channel == events then
            local event = result.value

            if event.kind == orchestrator.process.event.CANCEL then
                handle_cancellation(runtime_state, event)
            else
                local continue = handle_process_event(runtime_state, event)
                if continue and state.running then
                    -- load pending commits from DB before scheduling
                    -- exiting node may have submitted output data (commit in DB but message not yet received)
                    load_startup_pending_commits(runtime_state)
                    process_pending_commits(runtime_state)
                    call_scheduler_and_handle(runtime_state)
                end
            end
        end
    end

    -- Clean up and return result
    return finish(state, state.exit_result or { success = true, dataflow_id = dataflow_id })
end

orchestrator.run = run
return orchestrator
