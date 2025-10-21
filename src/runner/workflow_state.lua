local sql = require("sql")
local json = require("json")
local uuid = require("uuid")
local time = require("time")
local commit = require("commit")
local dataflow_repo = require("dataflow_repo")
local data_reader = require("data_reader")
local consts = require("consts")

local workflow_state = {}
local methods = {}
local workflow_state_mt = { __index = methods }

local function get_db()
    local db, err = sql.get(consts.APP_DB)
    if err then
        return nil, "Failed to connect to database: " .. err
    end
    return db
end

function workflow_state.new(dataflow_id, options)
    if not dataflow_id or dataflow_id == "" then
        return nil, "Dataflow ID is required"
    end

    local instance = {
        dataflow_id = dataflow_id,
        options = options or {},

        nodes = {},
        dataflow_metadata = {},
        loaded = false,

        active_processes = {},
        active_yields = {},

        input_tracker = {
            requirements = {},
            available = {}
        },

        has_workflow_output = false,
        has_workflow_error = false,

        queued_commands = {}
    }

    return setmetatable(instance, workflow_state_mt), nil
end

function methods:_set_input_requirements_from_config(node_id, config)
    if not config or type(config) ~= "table" then
        return
    end

    local inputs = config.inputs
    if inputs and type(inputs) == "table" then
        self:set_input_requirements(node_id, {
            required = inputs.required or {},
            optional = inputs.optional or {}
        })
    end
end

function methods:load_state()
    if self.loaded then
        return self, nil
    end

    local dataflow, err_df = dataflow_repo.get(self.dataflow_id)
    if err_df then
        return nil, "Failed to load dataflow: " .. err_df
    end

    if not dataflow then
        return nil, "Dataflow not found: " .. self.dataflow_id
    end

    self.dataflow_metadata = dataflow.metadata or {}

    local nodes, err_nodes = dataflow_repo.get_nodes_for_dataflow(self.dataflow_id)
    if err_nodes then
        return nil, "Failed to load nodes: " .. err_nodes
    end

    self.nodes = {}
    for _, node in ipairs(nodes or {}) do
        local config = node.config

        self.nodes[node.node_id] = {
            status = node.status,
            type = node.type,
            parent_node_id = node.parent_node_id,
            metadata = node.metadata or {},
            config = config
        }

        self:_set_input_requirements_from_config(node.node_id, config)
    end

    self:_load_existing_data()

    local reset_err = self:_reset_running_nodes()
    if reset_err then
        return nil, reset_err
    end

    self:_reconstruct_active_yields()

    self.loaded = true
    return self, nil
end

function methods:_load_existing_data()
    local workflow_outputs = data_reader.with_dataflow(self.dataflow_id)
        :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
        :all()

    for _, output in ipairs(workflow_outputs) do
        if output.discriminator == "error" then
            self.has_workflow_error = true
        else
            self.has_workflow_output = true
        end
    end

    self:_update_input_availability()
end

function methods:_reset_running_nodes()
    local reset_commands = {}

    for node_id, node_data in pairs(self.nodes) do
        if node_data.status == consts.STATUS.RUNNING then
            table.insert(reset_commands, {
                type = consts.COMMAND_TYPES.UPDATE_NODE,
                payload = {
                    node_id = node_id,
                    status = consts.STATUS.PENDING,
                    metadata = {
                        orchestrator_restarted_at = time.now():format(time.RFC3339NANO),
                        previous_status_on_restart = consts.STATUS.RUNNING
                    }
                }
            })
            node_data.status = consts.STATUS.PENDING
        end
    end

    if #reset_commands > 0 then
        local result, err = commit.execute(self.dataflow_id, uuid.v7(), reset_commands, { publish = false })
        if err then
            return "Failed to reset RUNNING nodes: " .. err
        end
    end

    return nil
end

function methods:_reconstruct_active_yields()
    local yield_records = data_reader.with_dataflow(self.dataflow_id)
        :with_data_types(consts.DATA_TYPE.NODE_YIELD)
        :all()

    for _, yield_record in ipairs(yield_records) do
        local parent_node_id = yield_record.node_id
        local parent_node = self.nodes[parent_node_id]

        if parent_node and parent_node.status == consts.STATUS.PENDING then
            local yield_content
            if type(yield_record.content) == "string" then
                local success, parsed = pcall(json.decode, yield_record.content)
                if success then
                    yield_content = parsed
                else
                    goto continue
                end
            else
                yield_content = yield_record.content
            end

            if not yield_content then
                goto continue
            end

            local yield_id = yield_content.yield_id
            local reply_to = yield_content.reply_to
            local yield_context = yield_content.yield_context or {}
            local run_nodes = yield_context.run_nodes or {}
            local child_path = yield_content.child_path or {}

            local pending_children = {}
            local results = {}

            for _, child_id in ipairs(run_nodes) do
                local child_node = self.nodes[child_id]
                if child_node and child_node.status ~= consts.STATUS.TEMPLATE then
                    pending_children[child_id] = child_node.status

                    if child_node.status == consts.STATUS.COMPLETED_SUCCESS or
                       child_node.status == consts.STATUS.COMPLETED_FAILURE then
                        local result_data = data_reader.with_dataflow(self.dataflow_id)
                            :with_nodes(child_id)
                            :with_data_types(consts.DATA_TYPE.NODE_RESULT)
                            :one()
                        if result_data then
                            results[child_id] = result_data.data_id
                        end
                    end
                end
            end

            local yield_info = {
                yield_id = yield_id,
                reply_to = reply_to,
                pending_children = pending_children,
                results = results,
                child_path = child_path
            }

            self.active_yields[parent_node_id] = yield_info
        end

        ::continue::
    end
end

function methods:_update_input_availability()
    for node_id, _ in pairs(self.input_tracker.requirements) do
        self.input_tracker.available[node_id] = {}
    end

    local node_inputs = data_reader.with_dataflow(self.dataflow_id)
        :with_data_types(consts.DATA_TYPE.NODE_INPUT)
        :all()

    for _, input in ipairs(node_inputs) do
        if input.node_id then
            if not self.input_tracker.available[input.node_id] then
                self.input_tracker.available[input.node_id] = {}
            end

            local key = input.discriminator or input.key or "default"
            self.input_tracker.available[input.node_id][key] = true
        end
    end
end

function methods:process_commits(commit_ids)
    if not commit_ids or #commit_ids == 0 then
        return { changes_made = false, message = "No commits to process" }, nil
    end

    for _, commit_id in ipairs(commit_ids) do
        table.insert(self.queued_commands, {
            type = consts.COMMAND.APPLY_COMMIT,
            payload = {
                commit_id = commit_id
            }
        })
    end

    local result, err = self:persist()
    if err then
        return nil, err
    end

    self:_update_state_from_results(result)

    return result, nil
end

function methods:_update_state_from_results(results)
    if not results or not results.results then
        return
    end

    for _, result in ipairs(results.results) do
        if not result or not result.input then
            goto continue
        end

        local command = result.input
        local command_type = command.type
        local payload = command.payload or {}

        if command_type == consts.COMMAND_TYPES.CREATE_NODE and result.node_id then
            local config = payload.config

            local node = {
                status = payload.status or consts.STATUS.PENDING,
                type = payload.node_type,
                parent_node_id = payload.parent_node_id,
                metadata = payload.metadata or {},
                config = config
            }
            self.nodes[result.node_id] = node

            self:_set_input_requirements_from_config(result.node_id, config)

        elseif command_type == consts.COMMAND_TYPES.UPDATE_NODE and payload.node_id then
            local node_id = payload.node_id
            local node = self.nodes[node_id]

            if node then
                if payload.node_type then
                    node.type = payload.node_type
                end
                if payload.status then
                    node.status = payload.status
                end
                if payload.metadata then
                    node.metadata = payload.metadata
                end
                if payload.config then
                    node.config = payload.config
                    self:_set_input_requirements_from_config(node_id, payload.config)
                end
            end
        elseif command_type == consts.COMMAND_TYPES.DELETE_NODE and payload.node_id then
            self.nodes[payload.node_id] = nil

        elseif command_type == consts.COMMAND_TYPES.UPDATE_WORKFLOW then
            if payload.metadata then
                for k, v in pairs(payload.metadata) do
                    self.dataflow_metadata[k] = v
                end
            end

        elseif command_type == consts.COMMAND_TYPES.CREATE_DATA then
            if payload.data_type == consts.DATA_TYPE.WORKFLOW_OUTPUT then
                if payload.discriminator == "error" then
                    self.has_workflow_error = true
                else
                    self.has_workflow_output = true
                end
            elseif payload.data_type == consts.DATA_TYPE.NODE_INPUT and payload.node_id then
                if not self.input_tracker.available[payload.node_id] then
                    self.input_tracker.available[payload.node_id] = {}
                end
                local key = payload.discriminator or payload.key or "default"
                self.input_tracker.available[payload.node_id][key] = true
            end
        end

        ::continue::
    end
end

function methods:get_scheduler_snapshot()
    local active_proc_map = {}
    for node_id, pid in pairs(self.active_processes) do
        active_proc_map[node_id] = true
    end

    return {
        nodes = self.nodes,
        active_yields = self.active_yields,
        active_processes = active_proc_map,
        input_tracker = self.input_tracker,
        has_workflow_output = self.has_workflow_output,
        has_workflow_error = self.has_workflow_error
    }
end

function methods:get_failed_node_errors()
    local workflow_errors = data_reader.with_dataflow(self.dataflow_id)
        :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
        :all()

    for _, err_data in ipairs(workflow_errors) do
        if err_data.discriminator == "error" then
            local content = err_data.content
            if type(content) == "string" then
                return content
            elseif type(content) == "table" then
                return json.encode(content)
            else
                return tostring(content)
            end
        end
    end

    local failed_nodes = {}
    for node_id, node_data in pairs(self.nodes) do
        if node_data.status == consts.STATUS.COMPLETED_FAILURE then
            table.insert(failed_nodes, node_id)
        end
    end

    if #failed_nodes == 0 then
        return nil
    end

    local error_details = {}
    for _, node_id in ipairs(failed_nodes) do
        local result_data = data_reader.with_dataflow(self.dataflow_id)
            :with_nodes(node_id)
            :with_data_types(consts.DATA_TYPE.NODE_RESULT)
            :all()

        local error_message = "Unknown error"
        for _, result in ipairs(result_data) do
            if result.discriminator == "result.error" then
                local content = result.content or "Unknown error"

                if result.content_type == "application/json" or result.content_type == consts.CONTENT_TYPE.JSON then
                    local success, parsed = pcall(json.decode, content)
                    if success and type(parsed) == "table" then
                        if parsed.error and type(parsed.error) == "table" and parsed.error.message then
                            error_message = tostring(parsed.error.message)
                        elseif parsed.message then
                            error_message = tostring(parsed.message)
                        else
                            error_message = tostring(content)
                        end
                    else
                        error_message = tostring(content)
                    end
                else
                    error_message = tostring(content)
                end
                break
            end
        end

        table.insert(error_details, "Node [" .. node_id .. "] failed: " .. error_message)
    end

    return table.concat(error_details, "; ")
end

function methods:_node_has_required_inputs(node_id)
    local requirements = self.input_tracker.requirements[node_id]
    if not requirements then
        local available = self.input_tracker.available[node_id] or {}
        return next(available) ~= nil
    end

    local available = self.input_tracker.available[node_id] or {}
    for _, required_key in ipairs(requirements.required or {}) do
        if not available[required_key] then
            return false
        end
    end

    return true
end

function methods:cancel_deadlocked_yield_children(parent_id, yield_info)
    if not yield_info or not yield_info.pending_children then
        return
    end

    local has_runnable_child = false
    for child_id, status in pairs(yield_info.pending_children) do
        if status == consts.STATUS.PENDING then
            local child_node = self.nodes[child_id]
            if child_node and self:_node_has_required_inputs(child_id) then
                has_runnable_child = true
                break
            end
        end
    end

    if not has_runnable_child then
        local cancel_commands = {}

        for child_id, status in pairs(yield_info.pending_children) do
            if status == consts.STATUS.PENDING then
                yield_info.pending_children[child_id] = consts.STATUS.CANCELLED
                table.insert(cancel_commands, {
                    type = consts.COMMAND_TYPES.UPDATE_NODE,
                    payload = {
                        node_id = child_id,
                        status = consts.STATUS.CANCELLED,
                        metadata = {
                            cancellation_reason = "Yield deadlock: no pending nodes can run"
                        }
                    }
                })

                if self.nodes[child_id] then
                    self.nodes[child_id].status = consts.STATUS.CANCELLED
                end
            end
        end

        if #cancel_commands > 0 then
            self:queue_commands(cancel_commands)
        end
    end
end

function methods:track_process(node_id, pid)
    self.active_processes[node_id] = pid
    return self
end

function methods:handle_process_exit(pid, success, result)
    local exited_node_id = nil
    for node_id, tracked_pid in pairs(self.active_processes) do
        if tracked_pid == pid then
            exited_node_id = node_id
            break
        end
    end

    if not exited_node_id then
        return nil
    end

    self.active_processes[exited_node_id] = nil

    local new_status = success and consts.STATUS.COMPLETED_SUCCESS or consts.STATUS.COMPLETED_FAILURE
    if self.nodes[exited_node_id] then
        self.nodes[exited_node_id].status = new_status
    end

    local result_data_id = uuid.v7()
    local discriminator = success and "result.success" or "result.error"

    table.insert(self.queued_commands, {
        type = consts.COMMAND_TYPES.UPDATE_NODE,
        payload = {
            node_id = exited_node_id,
            status = new_status
        }
    })

    table.insert(self.queued_commands, {
        type = consts.COMMAND_TYPES.CREATE_DATA,
        payload = {
            data_id = result_data_id,
            data_type = consts.DATA_TYPE.NODE_RESULT,
            content = result or (success and "Completed" or "Failed"),
            node_id = exited_node_id,
            discriminator = discriminator
        }
    })

    local exit_info = {
        node_id = exited_node_id,
        success = success,
        result = result,
        result_data_id = result_data_id
    }

    local node_data = self.nodes[exited_node_id]
    if node_data and node_data.parent_node_id then
        local parent_id = node_data.parent_node_id
        local yield_info = self.active_yields[parent_id]

        if yield_info and yield_info.pending_children and yield_info.pending_children[exited_node_id] then
            yield_info.pending_children[exited_node_id] = success and consts.STATUS.COMPLETED_SUCCESS or
            consts.STATUS.COMPLETED_FAILURE
            yield_info.results[exited_node_id] = result_data_id

            self:cancel_deadlocked_yield_children(parent_id, yield_info)

            local all_complete = true
            for child_id, status in pairs(yield_info.pending_children) do
                if status == consts.STATUS.PENDING then
                    all_complete = false
                    break
                end
            end

            if all_complete then
                exit_info.yield_complete = {
                    parent_id = parent_id,
                    yield_info = yield_info
                }
            end
        end
    end

    return exit_info
end

function methods:track_yield(node_id, yield_info)
    self.active_yields[node_id] = yield_info
    return self
end

function methods:satisfy_yield(node_id, results)
    local yield_info = self.active_yields[node_id]
    if yield_info then
        table.insert(self.queued_commands, {
            type = consts.COMMAND_TYPES.CREATE_DATA,
            payload = {
                data_id = uuid.v7(),
                data_type = consts.DATA_TYPE.NODE_YIELD_RESULT,
                content = results,
                key = yield_info.yield_id,
                node_id = node_id
            }
        })

        self.active_yields[node_id] = nil
    end

    return self
end

function methods:set_input_requirements(node_id, requirements)
    self.input_tracker.requirements[node_id] = requirements

    if not self.input_tracker.available[node_id] then
        self.input_tracker.available[node_id] = {}
    end

    return self
end

function methods:queue_commands(commands)
    if type(commands) == "table" and commands.type then
        table.insert(self.queued_commands, commands)
    elseif type(commands) == "table" then
        for _, cmd in ipairs(commands) do
            table.insert(self.queued_commands, cmd)
        end
    end
    return self
end

function methods:persist()
    if #self.queued_commands == 0 then
        return { changes_made = false, message = "No commands to persist" }, nil
    end

    local op_id = uuid.v7()
    local result, err = commit.execute(self.dataflow_id, op_id, self.queued_commands, { publish = true })

    if err then
        return nil, "Failed to persist commands: " .. err
    end

    self:_update_state_from_results(result)

    self.queued_commands = {}

    return result, nil
end

function methods:get_nodes()
    return self.nodes
end

function methods:get_node(node_id)
    return self.nodes[node_id]
end

function methods:get_dataflow_metadata()
    return self.dataflow_metadata
end

function methods:is_node_active(node_id)
    if self.active_processes[node_id] then
        return true
    end

    if self.active_yields[node_id] then
        return true
    end

    for _, yield_info in pairs(self.active_yields) do
        if yield_info.pending_children and yield_info.pending_children[node_id] == consts.STATUS.PENDING then
            return true
        end
    end

    return false
end

return workflow_state