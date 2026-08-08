local uuid = require("uuid")
local json = require("json")
local time = require("time")
local consts = require("consts")
local logger = require("logger"):named("dataflow.orchestrator")

local orchestrator = {
    workflow_state = require("workflow_state"),
    scheduler = require("scheduler"),
    process = process,
    channel = channel,
    funcs = require("funcs"),
    commit = require("commit"),
    activation_repo = require("activation_repo"),
    execution_frame = require("execution_frame"),
    wake_repo = require("wake_repo"),
    overseer = require("overseer"),
}

-- Runtime dependencies are intentionally abstract: production binds registry
-- modules while the protocol suite supplies deterministic adapters. The
-- lifecycle state itself is concrete so ownership fields cannot silently drift.
type Runtime = {
    workflow_state: any,
    scheduler: any,
    process: any,
    channel: any,
    funcs: any,
    commit: any,
    activation_repo: any,
    execution_frame: any,
    wake_repo: any,
    overseer: any,
}

type OrchestratorState = {
    dataflow_id: string,
    workflow_state: any,
    active_processes: { [string]: any },
    incoming_commit_queue: { string },
    processed_commit_ids: { [string]: boolean },
    workflow_status_updated: boolean,
    actor: any,
    scope: any,
    on_complete_id: string?,
    activation_generation: number,
    running: boolean,
    exit_result: any,
    final_status: string?,
    completion_hook_fired: boolean?,
    reschedule_requested: boolean?,
    runtime: Runtime,
}

type ParkArmState = {
    runtime: any,
    actor: any,
    scope: any,
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
    if not state.actor or not state.scope then
        logger:warn("on_complete hook skipped without execution identity", {
            dataflow_id = state.dataflow_id,
            hook = hook_id,
        })
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

    local executor = state.runtime.funcs.new()
        :with_actor(state.actor)
        :with_scope(state.scope)

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

local function workflow_identity(runtime: any, actor_id: string?, actor_context: any, dataflow_id: string): (any?, any?, string?)
    if type(actor_id) ~= "string" or actor_id == "" then
        return nil, nil, "workflow " .. dataflow_id .. " has no execution actor"
    end
    local resolver = runtime.execution_frame.resolve or runtime.execution_frame.reconstruct
    local actor, scope, reconstruct_err = resolver(actor_id, actor_context)
    if reconstruct_err or not actor or not scope then
        return nil, nil, "workflow " .. dataflow_id .. " execution frame is unavailable: " ..
            tostring(reconstruct_err or "invalid reconstructed identity")
    end
    return actor, scope, nil
end

local function command_projection(result: any): any
    local results = result and result.results or nil
    if type(results) ~= "table" or #results == 0 then return nil end
    for _, projection in ipairs(results) do
        if type(projection) == "table" and projection.completed ~= nil then
            return projection
        end
    end
    return results[1]
end

local function stop_for_existing_terminal(state: OrchestratorState, projection: any)
    local status = projection and projection.status or nil
    state.running = false
    state.exit_result = {
        success = status == consts.STATUS.COMPLETED_SUCCESS,
        terminal = true,
        status = status,
        dataflow_id = state.dataflow_id,
        message = "Workflow became terminal in another lifecycle transition",
    }
    -- This life did not win terminal persistence and must not deliver the hook.
    state.final_status = nil
    return false
end

local function adopt_projection_generation(state: OrchestratorState, projection: any)
    local current_generation = projection and tonumber(
        projection.current_generation or projection.generation)
    if not current_generation or current_generation < 1 or current_generation % 1 ~= 0 then
        return nil, "activation fence conflict did not report a valid current generation"
    end
    if current_generation <= state.activation_generation then
        return nil, "activation fence conflict did not advance the current generation"
    end
    state.activation_generation = current_generation
    return current_generation, nil
end

-- Persist an orchestrator-owned failure through the same generation CAS as a
-- successful completion. A newer activation wins and asks the caller to
-- re-evaluate durable state; only invariant failures (such as an unusable
-- execution frame) may follow the advanced generation and retry directly.
local function persist_fenced_failure(
    state: OrchestratorState,
    failure_message: string,
    post_commands: any?,
    invariant_failure: boolean?
)
    local attempts = 0
    while attempts < 8 do
        attempts = attempts + 1
        if type(state.workflow_state.discard_queued_commands) == "function" then
            state.workflow_state:discard_queued_commands()
        end
        state.workflow_state:queue_commands({
            type = consts.COMMAND_TYPES.COMPLETE_WORKFLOW,
            payload = {
                activation_generation = state.activation_generation,
                status = consts.STATUS.COMPLETED_FAILURE,
                metadata = { error = failure_message },
            },
        })
        if type(post_commands) == "table" and #post_commands > 0 then
            state.workflow_state:queue_commands(post_commands)
        end
        local persist_result, persist_err = state.workflow_state:persist()
        if persist_err then
            state.running = false
            state.exit_result = {
                success = false,
                dataflow_id = state.dataflow_id,
                error = failure_message .. "; failed to persist terminal state: " .. tostring(persist_err),
            }
            state.final_status = nil
            return false, false
        end

        local projection = command_projection(persist_result)
        if projection and projection.completed == true then
            state.final_status = consts.STATUS.COMPLETED_FAILURE
            state.exit_result = {
                success = false,
                dataflow_id = state.dataflow_id,
                error = failure_message,
            }
            state.running = false
            return false, false
        end
        if projection and projection.terminal == true then
            return stop_for_existing_terminal(state, projection), false
        end

        local _, generation_err = adopt_projection_generation(state, projection)
        if generation_err then
            state.running = false
            state.exit_result = {
                success = false,
                dataflow_id = state.dataflow_id,
                error = failure_message .. "; " .. generation_err,
            }
            state.final_status = nil
            return false, false
        end
        if not invariant_failure then
            return true, true
        end
    end

    state.running = false
    state.exit_result = {
        success = false,
        dataflow_id = state.dataflow_id,
        error = failure_message .. "; activation generation changed too many times",
    }
    state.final_status = nil
    return false, false
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

    local spawner = state.runtime.process.with_context({})
        :with_actor(state.actor)
        :with_scope(state.scope)

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
local function process_pending_commits(state: OrchestratorState)
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
        local continue, reschedule = persist_fenced_failure(
            state, "Commit processing failed: " .. err)
        state.reschedule_requested = reschedule == true
        return continue
    end

    for _, commit_id in ipairs(commits_to_process) do
        table.insert(state.processed_commit_ids, commit_id)
    end

    return true
end

---Load pending commits from durable storage for crash/restart recovery
---@param state table Orchestrator state
---@return boolean success Whether loading succeeded
local function load_startup_pending_commits(state: OrchestratorState)
    local pending_commit_ids, pending_err = state.runtime.commit.get_pending_commits(state.dataflow_id)
    if pending_err then
        local failure_message = "Failed to load pending commits: " .. pending_err
        local continue, reschedule = persist_fenced_failure(state, failure_message)
        state.reschedule_requested = reschedule == true
        return continue
    end

    for _, commit_id in ipairs(pending_commit_ids or {}) do
        table.insert(state.incoming_commit_queue, commit_id)
    end

    return true
end

---Call scheduler and handle the result immediately
---@param state table Orchestrator state
---@return boolean continue Whether to continue processing
local function call_scheduler_and_handle(state: OrchestratorState)
    -- loop through SATISFY_YIELD decisions: they mutate state (clear active_yields)
    -- but don't guarantee forward progress on their own, especially when the yield's
    -- parent process is dead (recovery case). keep scheduling until a node starts,
    -- the workflow completes, or no more work can be dispatched.
    local max_iterations = 64
    while max_iterations > 0 do
        ::continue_scheduler::
        max_iterations = max_iterations - 1

        if state.reschedule_requested then
            state.reschedule_requested = false
            if not load_startup_pending_commits(state) then return false end
            if not process_pending_commits(state) then return false end
            if state.reschedule_requested then
                -- A second fence race occurred while reconciling. Stay in this
                -- bounded scheduler loop rather than waiting for another hint.
                goto continue_scheduler
            end
        end

        local snapshot = state.workflow_state:get_scheduler_snapshot()
        local decision = state.runtime.scheduler.find_next_work(snapshot)

        if decision.type == state.runtime.scheduler.DECISION_TYPE.EXECUTE_NODES then
            local continue = handle_execute_nodes(state, decision.payload)
            if state.reschedule_requested and continue then goto continue_scheduler end
            return continue
        elseif decision.type == state.runtime.scheduler.DECISION_TYPE.COMPLETE_WORKFLOW then
            local continue, reschedule = handle_complete_workflow(state, decision.payload)
            if reschedule and continue then goto continue_scheduler end
            return continue
        elseif decision.type == state.runtime.scheduler.DECISION_TYPE.SATISFY_YIELD then
            local cont = handle_satisfy_yield(state, decision.payload)
            if not cont or not state.running then
                return cont
            end
            -- re-enter the loop: yield satisfied, state changed, re-schedule
        elseif decision.type == state.runtime.scheduler.DECISION_TYPE.PASSIVATE then
            state.workflow_state:queue_commands({
                type = consts.COMMAND_TYPES.PASSIVATE_WORKFLOW,
                payload = { activation_generation = state.activation_generation },
            })
            local passivate_result, status_err = state.workflow_state:persist()
            if status_err then
                state.running = false
                state.exit_result = {
                    success = false,
                    dataflow_id = state.dataflow_id,
                    error = "Failed to persist waiting status: " .. tostring(status_err),
                }
                return false
            end

            local projection = passivate_result and passivate_result.results and passivate_result.results[1] or nil
            if not projection or projection.released ~= true then
                if projection and projection.terminal == true then
                    return stop_for_existing_terminal(state, projection)
                end
                local _, generation_err = adopt_projection_generation(state, projection)
                if generation_err then
                    state.running = false
                    state.exit_result = {
                        success = false,
                        dataflow_id = state.dataflow_id,
                        error = generation_err,
                    }
                    return false
                end
                -- A start, signal, or due deadline advanced the durable
                -- generation while this life was deciding to park. The failed
                -- CAS is the handoff: reload durable work and schedule again
                -- now, without waiting for a second message that may never come.
                state.reschedule_requested = true
                goto continue_scheduler
            end
            local unclaimed_wakes = {}
            if type(state.workflow_state.take_unclaimed_signal_wake_keys) == "function" then
                unclaimed_wakes = state.workflow_state:take_unclaimed_signal_wake_keys()
            end
            for _, wake_key in ipairs(unclaimed_wakes) do
                local _, cleanup_err = state.runtime.wake_repo.remove(state.dataflow_id, wake_key)
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
            state.runtime.overseer.notify()
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

    state.running = false
    state.exit_result = {
        success = false,
        dataflow_id = state.dataflow_id,
        error = "Scheduler did not converge after activation generation changes",
    }
    return false
end

---Handle node execution immediately
---@param state table Orchestrator state
---@param payload table Execution payload
---@return boolean continue Whether to continue processing
function handle_execute_nodes(state: OrchestratorState, payload: any)
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
        local continue, reschedule = persist_fenced_failure(state, fail_msg, fail_commands)
        state.reschedule_requested = reschedule == true
        return continue
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
        local continue, reschedule = persist_fenced_failure(state, combined_error, fail_commands)
        state.reschedule_requested = reschedule == true
        return continue
    end

    return true
end

---Handle yield satisfaction immediately
---@param state table Orchestrator state
---@param payload table Yield payload
---@return boolean continue Whether to continue processing
function handle_satisfy_yield(state: OrchestratorState, payload: any)
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
    state.runtime.overseer.notify()

    -- Send reply to yielding process ONLY AFTER successful persistence
    local process_info = state.active_processes[parent_id]
    if process_info and type(reply_to) == "string" then
        state.runtime.process.send(tostring(process_info.pid), reply_to, {
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
function handle_complete_workflow(state: OrchestratorState, payload: any)
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
            type = consts.COMMAND_TYPES.COMPLETE_WORKFLOW,
            payload = {
                activation_generation = state.activation_generation,
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

    local projection = command_projection(persist_result)
    if not projection or projection.completed ~= true then
        if projection and projection.terminal == true then
            return stop_for_existing_terminal(state, projection), false
        end
        local _, generation_err = adopt_projection_generation(state, projection)
        if generation_err then
            state.exit_result = {
                success = false,
                dataflow_id = state.dataflow_id,
                error = generation_err,
            }
            state.running = false
            return false, false
        end
        -- The completion decision was made against an older activation. Let the
        -- scheduler reload and re-evaluate the newer durable work immediately.
        state.reschedule_requested = true
        return true, true
    end

    -- Terminal status, activation disablement, and wake cleanup are one
    -- persistence transaction. The message only makes the overseer observe it
    -- sooner.
    state.runtime.overseer.notify()

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
    return false, false
end

-- Invoke a persisted park arm under the workflow's recovered authority. The
-- declaration contains only a function ref and data; it cannot override actor
-- or scope. Returns a structured error payload for the waiting node.
local function arm_parked_yield(state: ParkArmState, arm: any, idempotency_key: string?): any?
    local declaration = type(arm) == "table" and arm or {}
    local arm_ref = type(declaration.ref) == "string" and declaration.ref or ""
    if arm_ref == "" then
        return { code = "PARK_ARM_FAILED", message = "park arm.ref is required" }
    end

    local executor = state.runtime.funcs.new()
        :with_actor(state.actor)
        :with_scope(state.scope)
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
local function track_signal_yield(state: OrchestratorState, node_id: string, yield_info: any, from_pid: any)
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
        if type(before.signal_wake_keys) == "table" then
            yield_info.signal_wake_keys = {}
            for _, wake_key in ipairs(before.signal_wake_keys) do
                table.insert(yield_info.signal_wake_keys, wake_key)
            end
        end
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
    local decision = state.runtime.scheduler.find_next_work(state.workflow_state:get_scheduler_snapshot())
    if decision.type == state.runtime.scheduler.DECISION_TYPE.SATISFY_YIELD and
       decision.payload.parent_id == node_id then
        state.workflow_state:satisfy_yield(node_id, decision.payload.results or {})
        local _, persist_err = state.workflow_state:persist()
        if persist_err then
            state.workflow_state:abandon_yield(node_id)
            state.runtime.process.send(tostring(from_pid), yield_info.reply_to, {
                yield_id = yield_info.yield_id,
                parked = false,
                error = { code = "PARK_RESUME_FAILED", message = tostring(persist_err) },
            })
            return
        end
        state.runtime.overseer.notify()
        state.runtime.process.send(tostring(from_pid), yield_info.reply_to, {
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
    if not already_armed then arm_error = arm_parked_yield(state, yield_info.arm, arm_key) end
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
        state.runtime.process.send(tostring(from_pid), yield_info.reply_to, {
            yield_id = yield_info.yield_id,
            parked = false,
            error = arm_error,
        })
    else
        local prepared, prepare_err = state.workflow_state:prepare_passivation(node_id)
        if not prepared then
            state.runtime.process.send(tostring(from_pid), yield_info.reply_to, {
                yield_id = yield_info.yield_id,
                parked = false,
                error = { code = "PARK_PASSIVATION_FAILED", message = tostring(prepare_err) },
            })
            return
        end
        local _, persist_err = state.workflow_state:persist()
        if persist_err then
            state.runtime.process.send(tostring(from_pid), yield_info.reply_to, {
                yield_id = yield_info.yield_id,
                parked = false,
                error = { code = "PARK_PASSIVATION_FAILED", message = tostring(persist_err) },
            })
            return
        end
        if type(yield_info.timeout_deadline) == "string" then state.runtime.overseer.notify() end
        local ack_sent = state.runtime.process.send(tostring(from_pid), yield_info.reply_to, {
            yield_id = yield_info.yield_id,
            parked = true,
        })
        if not ack_sent then
            -- The durable WAITING transition is already committed. Do not
            -- leave a live node blocked forever on a reply that never arrived;
            -- LINK_DOWN recovery will detach it and the targeted wake retries.
            state.runtime.process.terminate(tostring(from_pid))
        end
    end
end

---Handle yield request immediately
---@param state table Orchestrator state
---@param msg_payload table Yield request payload
---@param from_pid string Process ID that sent the request
local function handle_yield_request(state: OrchestratorState, msg_payload: any, from_pid: any)
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
            track_signal_yield(state, tostring(node_id), yield_info, from_pid)
        elseif type(reply_to) == "string" and yield_id then
            state.runtime.process.send(tostring(from_pid), reply_to, {
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
            child_path = child_path,
            completion_policy = yield_context.completion_policy,
            concurrency_group_key = yield_context.concurrency_group_key,
            max_concurrent_nodes = yield_context.max_concurrent_nodes
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
local function handle_process_event(state: OrchestratorState, event: any)
    if event.kind ~= state.runtime.process.event.EXIT and event.kind ~= state.runtime.process.event.LINK_DOWN then
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
    local clean_exit = event.kind == state.runtime.process.event.EXIT and
        (not event.result or not event.result.error) and
        not (type(event_value) == "table" and event_value.success == false)
    local prepared_link_down = event.kind == state.runtime.process.event.LINK_DOWN and
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

    if event.kind == state.runtime.process.event.EXIT then
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
    elseif event.kind == state.runtime.process.event.LINK_DOWN then
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
        local completed_yield = exit_info.yield_complete
        if not load_startup_pending_commits(state) then
            return false
        end
        if not process_pending_commits(state) then
            return false
        end
        if not state.workflow_state:yield_requires_satisfaction(
            completed_yield.parent_id, completed_yield.yield_info.yield_id) then
            return true
        end
        return handle_satisfy_yield(state, {
            parent_id = completed_yield.parent_id,
            yield_id = completed_yield.yield_info.yield_id,
            reply_to = completed_yield.yield_info.reply_to,
            results = completed_yield.yield_info.results
        })
    end

    return true
end

---Handle commit message immediately
---@param state table Orchestrator state
---@param msg_payload table Commit payload
local function handle_commit_message(state: OrchestratorState, msg_payload: any)
    local commit_id = msg_payload and msg_payload.commit_id
    if commit_id then
        table.insert(state.incoming_commit_queue, commit_id)
    end
end

---Stop this runtime life after a process cancellation.
---Administrative cancellation is persisted by the client before the signal is
---sent. Runtime shutdown uses the same untyped process event, so the
---orchestrator must not invent a terminal business outcome here.
---@param state table Orchestrator state
---@param event table Cancel event
local function handle_cancellation(state: OrchestratorState, event: any)
    for node_id, process_info in pairs(state.active_processes) do
        if type(process_info.pid) == "string" then
            state.runtime.process.terminate(process_info.pid)
        end
    end

    state.final_status = nil
    state.exit_result = {
        success = true,
        pending = true,
        dataflow_id = state.dataflow_id,
        message = "Orchestrator stopped by runtime cancellation",
    }
    state.running = false
end

local function registry_name_missing(err)
    if err ~= nil then
        local kind_ok, kind = pcall(function() return err:kind() end)
        if kind_ok and tostring(kind) == "NotFound" then return true end
    end
    local message = string.lower(tostring(err or ""))
    return string.find(message, "not_found", 1, true) ~= nil or
        string.find(message, "name not registered", 1, true) ~= nil
end

local function duplicate_owner_result(dataflow_id)
    return {
        success = true,
        pending = true,
        dataflow_id = dataflow_id,
        error = nil,
        message = "Another orchestrator is already running for this workflow",
    }
end

---Main orchestrator function
---@param args table Arguments containing dataflow_id and optional init_func_id
---@return table result Orchestration result with success/error
local function run(args, runtime_override: any?)
    local bound = runtime_override or orchestrator
    local runtime: Runtime = {
        workflow_state = bound.workflow_state,
        scheduler = bound.scheduler,
        process = bound.process,
        channel = bound.channel,
        funcs = bound.funcs,
        commit = bound.commit,
        activation_repo = bound.activation_repo,
        execution_frame = bound.execution_frame,
        wake_repo = bound.wake_repo,
        overseer = bound.overseer,
    }
    local dataflow_id_raw = args and args.dataflow_id
    local init_func_id = args and args.init_func_id
    local activation_generation_raw = args and args.activation_generation
    local activation_generation = tonumber(activation_generation_raw)

    if type(dataflow_id_raw) ~= "string" or dataflow_id_raw == "" then
        return { success = false, error = "Missing required dataflow_id" }
    end
    local dataflow_id = dataflow_id_raw
    if activation_generation_raw == nil then
        return {
            success = false,
            dataflow_id = dataflow_id,
            error = "Missing required activation_generation",
        }
    end
    if not activation_generation or activation_generation < 1 or activation_generation % 1 ~= 0 then
        return {
            success = false,
            dataflow_id = dataflow_id,
            error = "Invalid activation_generation",
        }
    end

    -- A named overseer spawn owns the canonical name before Lua starts. Direct
    -- synchronous calls claim it here. Resolve ownership before any durable
    -- state is loaded so competing starts cannot replay the same work.
    local process_name = "dataflow." .. dataflow_id
    local self_pid = type(runtime.process.pid) == "function" and tostring(runtime.process.pid()) or nil
    local registered_pid, lookup_err = runtime.process.registry.lookup(process_name)
    if registered_pid and (not self_pid or tostring(registered_pid) ~= self_pid) then
        return duplicate_owner_result(dataflow_id)
    end
    if not registered_pid and lookup_err and not registry_name_missing(lookup_err) then
        return {
            success = false,
            dataflow_id = dataflow_id,
            error = "Failed to inspect orchestrator ownership: " .. tostring(lookup_err),
        }
    end
    if not registered_pid then
        local registered, reg_err = runtime.process.registry.register(process_name)
        if not registered then
            local winner_pid, winner_err = runtime.process.registry.lookup(process_name)
            if winner_pid and (not self_pid or tostring(winner_pid) ~= self_pid) then
                return duplicate_owner_result(dataflow_id)
            end
            return {
                success = false,
                dataflow_id = dataflow_id,
                error = "Failed to claim orchestrator ownership: " .. tostring(
                    reg_err or winner_err or "registry returned no owner"),
            }
        end
    end
    runtime.process.set_options({ trap_links = true, upgradable = false })

    local ws, ws_err = runtime.workflow_state.new(dataflow_id)
    if ws_err then
        return { success = false, error = "Failed to create workflow state: " .. ws_err }
    end
    if not ws then
        return { success = false, dataflow_id = dataflow_id, error = "Failed to create workflow state" }
    end
    local workflow_state = ws :: any

    -- Initialize state
    local state: OrchestratorState = {
        dataflow_id = dataflow_id,
        workflow_state = workflow_state,
        active_processes = {},
        incoming_commit_queue = {},
        processed_commit_ids = {},
        workflow_status_updated = false,
        actor = nil,
        scope = nil,
        on_complete_id = nil,
        activation_generation = activation_generation,
        running = true,
        exit_result = nil,
        runtime = runtime,
    }

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
        local _, cleanup_err = runtime.commit.disable_terminal_activation(dataflow_id)
        if cleanup_err then
            return {
                success = false,
                dataflow_id = dataflow_id,
                error = "Failed to disable stale terminal activation: " .. tostring(cleanup_err),
            }
        end
        runtime.process.registry.unregister("dataflow." .. dataflow_id)
        return {
            success = true,
            dataflow_id = dataflow_id,
            message = "Dataflow already in terminal state: " .. loaded_status
        }
    end

    -- A process may start after a newer activation generation has already won.
    -- The canonical named process owns that handoff: adopt a newer durable
    -- fence before executing rather than exiting and creating a false runtime
    -- owner loss. A generation older than the spawn request is invalid.
    local activation, activation_err = runtime.activation_repo.get(dataflow_id)
    if activation_err then
        return {
            success = false,
            dataflow_id = dataflow_id,
            error = "Failed to load workflow activation: " .. tostring(activation_err),
        }
    end
    local durable_generation = activation and tonumber(activation.generation) or nil
    if not activation or activation.desired_active ~= true or not durable_generation or
        durable_generation < activation_generation then
        runtime.process.registry.unregister("dataflow." .. dataflow_id)
        return {
            success = true,
            pending = true,
            dataflow_id = dataflow_id,
            message = "Stale or inactive workflow activation",
        }
    end
    activation_generation = durable_generation
    local durable_launch_args = type(activation.launch_args) == "table" and activation.launch_args or nil
    if durable_launch_args then
        if type(durable_launch_args.init_func_id) == "string" then
            init_func_id = durable_launch_args.init_func_id
        end
        if args and type(durable_launch_args.on_complete) == "string" then
            args.on_complete = durable_launch_args.on_complete
        end
    end
    state.activation_generation = activation_generation

    -- The spawn path is already monitored atomically. This notification lets
    -- a synchronous client-owned invocation be adopted by the same overseer;
    -- it is a latency hint and carries no lifecycle authority.
    if type(runtime.process.pid) == "function" then
        runtime.process.send("dataflow.overseer", "dataflow.activation.changed", {
            dataflow_id = dataflow_id,
            generation = activation_generation,
        })
    end

    -- Resolve execution identity and the legacy completion hook before terminal
    -- paths. Metadata preserves the callback reference across orchestrator lives,
    -- but delivery remains best-effort; graph terminal nodes are the durable form.
    local raw_actor_id = workflow_state:get_actor_id()
    local actor_id: string? = nil
    if type(raw_actor_id) == "string" and raw_actor_id ~= "" then
        actor_id = raw_actor_id
    end
    local run_actor, run_scope, identity_err = workflow_identity(
        runtime,
        actor_id,
        workflow_state:get_actor_context(),
        dataflow_id
    )
    if identity_err then
        persist_fenced_failure(state, identity_err, nil, true)
        return finish(state, state.exit_result or {
            success = false,
            dataflow_id = dataflow_id,
            error = identity_err,
        })
    end
    state.actor = run_actor
    state.scope = run_scope
    local runtime_state: OrchestratorState = state

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
    local initial_scheduler_ran = false
    for _ in pairs(nodes) do
        node_count = node_count + 1
    end

    if node_count == 0 then
        local continue, reschedule = handle_complete_workflow(runtime_state, {
            success = true,
            message = "Empty workflow - no nodes to execute",
        })
        if not continue or not reschedule then
            return finish(state, state.exit_result or {
                success = true,
                dataflow_id = dataflow_id,
            })
        end
        initial_scheduler_ran = true
        local scheduler_continue = call_scheduler_and_handle(runtime_state)
        if not scheduler_continue then
            return finish(state, state.exit_result or {
                success = false,
                dataflow_id = dataflow_id,
                error = "Orchestrator exited while reconciling empty workflow activation",
            })
        end
    end

    -- Call init function if provided
    local initial_activation = loaded_status == consts.STATUS.PENDING or loaded_status == consts.STATUS.READY
    if not initial_scheduler_ran and initial_activation and type(init_func_id) == "string" and init_func_id ~= "" then
        local executor = runtime.funcs.new()
            :with_actor(state.actor)
            :with_scope(state.scope)
        local _, _ = executor:call(init_func_id, {
            dataflow_id = dataflow_id,
            metadata = workflow_state:get_dataflow_metadata()
        })
    end

    -- Set up channels
    local inbox = runtime.process.inbox()
    local events = runtime.process.events()

    -- Initial scheduler call
    if not initial_scheduler_ran then
        local continue = call_scheduler_and_handle(runtime_state)
        if not continue then
            return finish(state, state.exit_result or {
                success = false,
                dataflow_id = dataflow_id,
                error = "Orchestrator exited without result"
            })
        end
    end

    -- Main processing loop. Signal waits leave through PASSIVATE; a durable
    -- signal commit or indexed due wake starts the next orchestrator life.
    while state.running do
        local select_cases = {
            inbox:case_receive(),
            events:case_receive()
        }

        local result = runtime.channel.select(select_cases)

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
                local delivered_generation = payload_table and tonumber(payload_table.generation)
                if delivered_generation and delivered_generation > state.activation_generation then
                    -- Message generations are hints only. Reconcile against the
                    -- durable activation row before changing the lifecycle fence.
                    local activation, activation_err = runtime.activation_repo.get(state.dataflow_id)
                    local durable_generation = activation and tonumber(activation.generation) or nil
                    if not activation_err and activation and activation.desired_active == true and
                        durable_generation and durable_generation > state.activation_generation then
                        state.activation_generation = durable_generation
                    elseif activation_err then
                        logger:warn("wake generation hint could not be verified", {
                            dataflow_id = state.dataflow_id,
                            error = tostring(activation_err),
                        })
                    end
                end
                if payload_table and type(runtime_state.workflow_state.observe_signal_wake) == "function" then
                    runtime_state.workflow_state:observe_signal_wake(payload_table.wake_key)
                end
                local loaded = load_startup_pending_commits(runtime_state)
                if loaded then process_pending_commits(runtime_state) end
                call_scheduler_and_handle(runtime_state)
            end
        elseif result.channel == events then
            local event = result.value

            if event.kind == runtime.process.event.CANCEL then
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

orchestrator.arm_parked_yield = arm_parked_yield
orchestrator.track_signal_yield = track_signal_yield
orchestrator.run = run
return orchestrator
