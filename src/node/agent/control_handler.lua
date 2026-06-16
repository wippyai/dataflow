local json = require("json")
local uuid = require("uuid")
local agent_consts = require("agent_consts")
local consts = require("consts")

local control_handler = {}

-- Returns a shallow copy of a named field from the node's current metadata so
-- control mutations merge with existing state rather than overwriting it.
local function copy_metadata_field(node_sdk, field)
    local meta = node_sdk:metadata() or {}
    local out = {}
    if type(meta[field]) == "table" then
        for k, v in pairs(meta[field]) do
            out[k] = v
        end
    end
    return out
end

-- Process session context changes
function control_handler.process_session_context(control, node_sdk)
    if not control.context or not control.context.session then
        return {}
    end

    local changes = {}
    local current_metadata = copy_metadata_field(node_sdk, "session_context")

    if control.context.session.set then
        for k, v in pairs(control.context.session.set) do
            current_metadata[k] = v
            changes[k] = { action = "set", value = v }
        end
    end

    if control.context.session.delete then
        for _, k in ipairs(control.context.session.delete) do
            current_metadata[k] = nil
            changes[k] = { action = "delete" }
        end
    end

    if next(changes) then
        node_sdk:update_metadata({ session_context = current_metadata })
        return { session_context = changes }
    end

    return {}
end

-- Process public metadata changes
function control_handler.process_public_metadata(control, node_sdk)
    if not control.context or not control.context.public_meta then
        return {}
    end

    local changes = {}
    local current_public_meta = copy_metadata_field(node_sdk, "public_meta")

    if control.context.public_meta.clear then
        local to_remove = {}
        for id, item in pairs(current_public_meta) do
            if type(item) == "table" and item.type == control.context.public_meta.clear then
                table.insert(to_remove, id)
            end
        end
        for _, id in ipairs(to_remove) do
            current_public_meta[id] = nil
        end
        changes.clear = control.context.public_meta.clear
    end

    if control.context.public_meta.set then
        changes.set = control.context.public_meta.set
        if type(control.context.public_meta.set) == "table" and #control.context.public_meta.set > 0 then
            for _, item in ipairs(control.context.public_meta.set) do
                if item.id then
                    current_public_meta[item.id] = item
                end
            end
        else
            for key, value in pairs(control.context.public_meta.set) do
                current_public_meta[key] = value
            end
        end
    end

    if control.context.public_meta.delete then
        changes.delete = control.context.public_meta.delete
        for _, id in ipairs(control.context.public_meta.delete) do
            current_public_meta[id] = nil
        end
    end

    if next(changes) then
        node_sdk:update_metadata({ public_meta = current_public_meta })
        return { public_meta = changes }
    end

    return {}
end

-- Process memory operations
function control_handler.process_memory_operations(control, node_sdk, iteration)
    if not control.memory then
        return {}
    end

    local memory_changes = {}

    if control.memory.add then
        for _, memory_item in ipairs(control.memory.add) do
            if memory_item.type and memory_item.text then
                node_sdk:data(agent_consts.DATA_TYPE.AGENT_MEMORY, memory_item.text, {
                    key = memory_item.type .. "_" .. iteration,
                    discriminator = memory_item.type,
                    node_id = node_sdk.node_id,
                    metadata = {
                        memory_type = memory_item.type,
                        created_by_control = true,
                        iteration = iteration
                    }
                })
                table.insert(memory_changes, {
                    action = "add",
                    type = memory_item.type,
                    text_length = #memory_item.text
                })
            end
        end
    end

    if control.memory.clear then
        local clear_types = type(control.memory.clear) == "table"
            and control.memory.clear
            or { control.memory.clear }

        for _, memory_type in ipairs(clear_types) do
            local rows = node_sdk:find_data(agent_consts.DATA_TYPE.AGENT_MEMORY, memory_type) or {}
            for _, row in ipairs(rows) do
                node_sdk:delete_data(row.data_id)
            end
            table.insert(memory_changes, {
                action = "clear",
                type = memory_type,
                deleted = #rows
            })
        end
    end

    if control.memory.delete then
        for _, memory_id in ipairs(control.memory.delete) do
            node_sdk:delete_data(memory_id)
            table.insert(memory_changes, {
                action = "delete",
                memory_id = memory_id
            })
        end
    end

    return memory_changes
end

-- Process artifact creation
function control_handler.process_artifacts(control, node_sdk, iteration)
    if not control.artifacts then
        return {}
    end

    local artifact_changes = {}

    for _, artifact in ipairs(control.artifacts) do
        if artifact.content then
            local content_type = artifact.content_type or consts.CONTENT_TYPE.TEXT
            local artifact_id = uuid.v7()
            local title = artifact.title or "Untitled"

            node_sdk:data(consts.DATA_TYPE.ARTIFACT, artifact.content, {
                data_id = artifact_id,
                key = title,
                content_type = content_type,
                metadata = {
                    title = title,
                    comment = artifact.description,
                    artifact_type = artifact.type or "inline",
                    created_in_control = true,
                    iteration = iteration
                }
            })

            table.insert(artifact_changes, {
                artifact_id = artifact_id,
                title = title,
                type = artifact.type or "inline"
            })
        end
    end

    return artifact_changes
end

-- Process direct commands
function control_handler.process_commands(control, node_sdk)
    if not control.commands then
        return {}
    end

    local command_changes = {}
    local created_node_ids = {}

    for _, cmd in ipairs(control.commands) do
        if cmd.type and cmd.payload then
            node_sdk:command(cmd)

            if cmd.type == consts.COMMAND_TYPES.CREATE_NODE and cmd.payload.node_id then
                table.insert(created_node_ids, cmd.payload.node_id)
            end

            table.insert(command_changes, {
                type = cmd.type,
                payload_summary = control_handler._summarize_command_payload(cmd.payload)
            })
        end
    end

    return {
        commands = command_changes,
        created_nodes = created_node_ids
    }
end

-- Create summary of command payload for logging
function control_handler._summarize_command_payload(payload)
    local summary = {
        type = payload.type or "unknown"
    }

    if payload.node_id then
        summary.node_id = payload.node_id
    end

    if payload.data_type then
        summary.data_type = payload.data_type
    end

    if payload.key then
        summary.key = payload.key
    end

    if payload.content then
        local content_type = type(payload.content)
        if content_type == "string" then
            summary.content_length = #payload.content
        elseif content_type == "table" then
            summary.content_type = "table"
        else
            summary.content_type = content_type
        end
    end

    return summary
end

-- Process a single control directive from tool result
-- Returns: cleaned_result, control_response
function control_handler.process_control_directive(tool_result, node_sdk, iteration)
    if type(tool_result) ~= "table" or not tool_result._control then
        return tool_result, nil
    end

    local control = tool_result._control
    local control_response = {
        agent_change = nil,
        model_change = nil,
        traits_change = nil,
        tools_change = nil,
        yield = nil,
        delegate = nil,
        changes_applied = {},
        _original_control = control  -- Preserve original for metadata
    }

    -- Process session context changes
    local session_changes = control_handler.process_session_context(control, node_sdk)
    if next(session_changes) then
        control_response.changes_applied.session_context = session_changes
    end

    -- Process public metadata changes
    local metadata_changes = control_handler.process_public_metadata(control, node_sdk)
    if next(metadata_changes) then
        control_response.changes_applied.public_meta = metadata_changes
    end

    -- Process memory operations
    local memory_changes = control_handler.process_memory_operations(control, node_sdk, iteration)
    if #memory_changes > 0 then
        control_response.changes_applied.memory = memory_changes
    end

    -- Process artifact creation
    local artifact_changes = control_handler.process_artifacts(control, node_sdk, iteration)
    if #artifact_changes > 0 then
        control_response.changes_applied.artifacts = artifact_changes
    end

    -- Process direct commands
    local command_result = control_handler.process_commands(control, node_sdk)
    if command_result.commands and #command_result.commands > 0 then
        control_response.changes_applied.commands = command_result.commands
        control_response.changes_applied.created_nodes = command_result.created_nodes
    end

    -- Handle configuration changes (agent/model/traits/tools)
    if control.config then
        if control.config.agent then
            control_response.agent_change = control.config.agent
        end

        if control.config.model then
            control_response.model_change = control.config.model
        end

        if control.config.traits ~= nil then
            control_response.traits_change = control.config.traits
        end

        if control.config.tools ~= nil then
            control_response.tools_change = control.config.tools
        end
    end

    -- Handle yield requests
    if control.yield then
        control_response.yield = control.yield

        -- Queue yield commands if any
        if control.yield.commands then
            for _, cmd in ipairs(control.yield.commands) do
                if cmd.type and cmd.payload then
                    node_sdk:command(cmd)
                end
            end
        end
    end

    -- Handle delegate requests - pass through the entire delegate array
    if control.delegate then
        control_response.delegate = control.delegate
    end

    -- Remove _control from tool result
    tool_result._control = nil

    return tool_result, control_response
end

-- Apply collected control responses to agent context and node state
-- Returns: changes_summary, error
function control_handler.apply_control_responses(control_responses, agent_context, node_sdk)
    local changes_summary = {
        agent_changed = false,
        model_changed = false,
        traits_changed = false,
        tools_changed = false,
        yielded = false,
        errors = {}
    }

    for _, response in ipairs(control_responses) do
        -- Handle agent changes. Persisted to node config so a re-run/recovery loads
        -- the new agent (config.agent is read at node startup). switch_to_agent also
        -- resets any active trait/tool overlays, so it must run before they are set.
        if response.agent_change then
            local success, err = agent_context:switch_to_agent(response.agent_change)
            if success then
                changes_summary.agent_changed = true
                changes_summary.new_agent = response.agent_change
                -- switch_to_agent reset the in-memory overlays (they are agent-specific);
                -- clear the persisted ones too so recovery does not re-apply a prior agent's
                -- overlay to the new agent. A traits/tools change in the same directive runs
                -- after this and overwrites the cleared value.
                node_sdk:update_config({ agent = response.agent_change, active_traits = false, active_tools = false })
            else
                table.insert(changes_summary.errors,
                    string.format("Failed to change agent: %s", err or "unknown error"))
            end
        end

        -- Handle model changes (needs a current agent id, so before set_active_*).
        if response.model_change then
            local success, err = agent_context:switch_to_model(response.model_change)
            if success then
                changes_summary.model_changed = true
                changes_summary.new_model = response.model_change
                node_sdk:update_config({ model = response.model_change })
            else
                table.insert(changes_summary.errors,
                    string.format("Failed to change model: %s", err or "unknown error"))
            end
        end

        -- Declarative active trait/tool overlays. Applied to the agent context and
        -- persisted to node config (active_traits/active_tools) so they are reapplied
        -- on a re-run. set_active_* invalidates the loaded agent; node.lua reloads it.
        if response.traits_change ~= nil then
            agent_context:set_active_traits(response.traits_change)
            changes_summary.traits_changed = true
            node_sdk:update_config({ active_traits = response.traits_change })
        end

        if response.tools_change ~= nil then
            agent_context:set_active_tools(response.tools_change)
            changes_summary.tools_changed = true
            node_sdk:update_config({ active_tools = response.tools_change })
        end

        -- Handle yield requests
        if response.yield then
            local yield_options = {}

            if response.yield.user_context and response.yield.user_context.run_node_ids then
                yield_options.run_nodes = response.yield.user_context.run_node_ids
            end

            local yield_result, yield_err = node_sdk:yield(yield_options)
            if yield_result then
                changes_summary.yielded = true
                changes_summary.yield_result = yield_result
            else
                table.insert(changes_summary.errors,
                    string.format("Failed to yield: %s", yield_err or "unknown error"))
            end
        end

        -- Note: delegate is handled separately in node.lua main loop
        -- It doesn't require any agent context changes, just child node creation
    end

    return changes_summary, (#changes_summary.errors > 0) and table.concat(changes_summary.errors, "; ") or nil
end

return control_handler