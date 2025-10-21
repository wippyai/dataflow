local json = require("json")
local uuid = require("uuid")
local expr = require("expr")
local consts = require("consts")

local compiler = {}

compiler.OP_TYPES = {
    WITH_INPUT = "with_input",
    FUNC = "func",
    AGENT = "agent",
    CYCLE = "cycle",
    MAP_REDUCE = "map_reduce",
    STATE = "state",
    USE = "use",
    AS = "as",
    TO = "to",
    ERROR_TO = "error_to",
    WHEN = "when"
}

local FlowGraph = {}
local flow_graph_mt = { __index = FlowGraph }

function FlowGraph.new()
    return setmetatable({
        operations = {},
        nodes = {},
        node_order = {},
        edges = {},
        references = {},
        input_data = nil,
        input_name = nil,
        input_routes = {},
        last_node_id = nil,
        pending_routes = {},
        has_explicit_routing = false,
        session_parent_id = nil,
        forced_success_nodes = {},
        forced_failure_nodes = {}
    }, flow_graph_mt)
end

function FlowGraph:add_operation(op_type, config)
    table.insert(self.operations, {
        type = op_type,
        config = config or {}
    })
    return self, nil
end

function FlowGraph:create_node(node_type, config, metadata)
    local node_id = uuid.v7()

    self.nodes[node_id] = {
        node_id = node_id,
        node_type = node_type,
        config = config or {},
        metadata = metadata or {},
        status = consts.STATUS.PENDING
    }

    table.insert(self.node_order, node_id)

    self.edges[node_id] = {
        targets = {},
        error_targets = {}
    }

    self.last_node_id = node_id
    return node_id, nil
end

function FlowGraph:create_template_nodes(template, parent_node_id)
    if not template or not template.operations then
        return {}
    end

    local template_node_ids = {}
    local last_template_node_id = nil

    for _, op in ipairs(template.operations) do
        if op.type == compiler.OP_TYPES.FUNC then
            local template_node_id = uuid.v7()

            local config = {
                func_id = op.config.func_id,
                inputs = op.config.inputs,
                context = op.config.context,
                input_transform = op.config.input_transform
            }

            if last_template_node_id then
                if not self.nodes[last_template_node_id].config.data_targets then
                    self.nodes[last_template_node_id].config.data_targets = {}
                end
                table.insert(self.nodes[last_template_node_id].config.data_targets, {
                    data_type = consts.DATA_TYPE.NODE_INPUT,
                    node_id = template_node_id,
                    discriminator = "default"
                })
            end

            local metadata = op.config.metadata or {}

            self.nodes[template_node_id] = {
                node_id = template_node_id,
                node_type = "userspace.dataflow.node.func:node",
                config = config,
                metadata = metadata,
                status = consts.STATUS.TEMPLATE,
                parent_node_id = parent_node_id
            }

            self.edges[template_node_id] = {
                targets = {},
                error_targets = {}
            }

            table.insert(self.node_order, template_node_id)
            table.insert(template_node_ids, template_node_id)
            last_template_node_id = template_node_id

        elseif op.type == compiler.OP_TYPES.AGENT then
            local template_node_id = uuid.v7()

            local config = {
                agent = op.config.agent_id,
                model = op.config.model,
                arena = op.config.arena,
                inputs = op.config.inputs,
                show_tool_calls = op.config.show_tool_calls,
                input_transform = op.config.input_transform
            }

            if last_template_node_id then
                if not self.nodes[last_template_node_id].config.data_targets then
                    self.nodes[last_template_node_id].config.data_targets = {}
                end
                table.insert(self.nodes[last_template_node_id].config.data_targets, {
                    data_type = consts.DATA_TYPE.NODE_INPUT,
                    node_id = template_node_id,
                    discriminator = "default"
                })
            end

            local metadata = op.config.metadata or {}

            self.nodes[template_node_id] = {
                node_id = template_node_id,
                node_type = "userspace.dataflow.node.agent:node",
                config = config,
                metadata = metadata,
                status = consts.STATUS.TEMPLATE,
                parent_node_id = parent_node_id
            }

            self.edges[template_node_id] = {
                targets = {},
                error_targets = {}
            }

            table.insert(self.node_order, template_node_id)
            table.insert(template_node_ids, template_node_id)
            last_template_node_id = template_node_id

        elseif op.type == compiler.OP_TYPES.CYCLE then
            local template_node_id = uuid.v7()

            local config = {
                func_id = op.config.func_id,
                continue_condition = op.config.continue_condition,
                max_iterations = op.config.max_iterations,
                initial_state = op.config.initial_state,
                inputs = op.config.inputs,
                context = op.config.context,
                input_transform = op.config.input_transform
            }

            if last_template_node_id then
                if not self.nodes[last_template_node_id].config.data_targets then
                    self.nodes[last_template_node_id].config.data_targets = {}
                end
                table.insert(self.nodes[last_template_node_id].config.data_targets, {
                    data_type = consts.DATA_TYPE.NODE_INPUT,
                    node_id = template_node_id,
                    discriminator = "default"
                })
            end

            local metadata = op.config.metadata or {}

            self.nodes[template_node_id] = {
                node_id = template_node_id,
                node_type = "userspace.dataflow.node.cycle:cycle",
                config = config,
                metadata = metadata,
                status = consts.STATUS.TEMPLATE,
                parent_node_id = parent_node_id
            }

            self.edges[template_node_id] = {
                targets = {},
                error_targets = {}
            }

            table.insert(self.node_order, template_node_id)
            table.insert(template_node_ids, template_node_id)

            if op.config.template then
                local cycle_template_nodes = self:create_template_nodes(op.config.template, template_node_id)
                for _, child_id in ipairs(cycle_template_nodes) do
                    table.insert(template_node_ids, child_id)
                end
            end

            last_template_node_id = template_node_id
        end
    end

    if last_template_node_id then
        local last_node = self.nodes[last_template_node_id]
        if not last_node.config.data_targets then
            last_node.config.data_targets = {}
        end
        table.insert(last_node.config.data_targets, {
            data_type = consts.DATA_TYPE.NODE_OUTPUT,
            discriminator = "result"
        })
    end

    return template_node_ids
end

function FlowGraph:add_reference(name, node_id)
    if self.references[name] then
        return nil, "Duplicate node name: " .. name
    end
    self.references[name] = node_id
    return true, nil
end

function FlowGraph:resolve_reference(name)
    local node_id = self.references[name]
    if not node_id then
        return nil, "Undefined node reference: " .. name
    end
    return node_id, nil
end

function FlowGraph:detect_cycles()
    local visited = {}
    local rec_stack = {}

    local function dfs(node_id, path)
        if rec_stack[node_id] then
            local cycle_start = nil
            for i, id in ipairs(path) do
                if id == node_id then
                    cycle_start = i
                    break
                end
            end

            if cycle_start then
                local cycle = {}
                for i = cycle_start, #path do
                    table.insert(cycle, path[i])
                end
                table.insert(cycle, node_id)
                return true, "Cycle detected: " .. table.concat(cycle, " -> ")
            end
            return true, "Cycle detected at node: " .. node_id
        end

        if visited[node_id] then
            return false, nil
        end

        visited[node_id] = true
        rec_stack[node_id] = true
        table.insert(path, node_id)

        local edges = self.edges[node_id]
        if edges then
            for _, edge in ipairs(edges.targets) do
                if edge.target_node_id then
                    local has_cycle, cycle_desc = dfs(edge.target_node_id, path)
                    if has_cycle then
                        return true, cycle_desc
                    end
                end
            end
            for _, edge in ipairs(edges.error_targets) do
                if edge.target_node_id then
                    local has_cycle, cycle_desc = dfs(edge.target_node_id, path)
                    if has_cycle then
                        return true, cycle_desc
                    end
                end
            end
        end

        rec_stack[node_id] = false
        table.remove(path)
        return false, nil
    end

    for node_id, _ in pairs(self.nodes) do
        if not visited[node_id] then
            local has_cycle, cycle_desc = dfs(node_id, {})
            if has_cycle then
                return true, cycle_desc
            end
        end
    end

    return false, nil
end

function compiler.build_graph(operations, session_context)
    if not operations or #operations == 0 then
        return nil, "No operations provided"
    end

    local graph = FlowGraph.new()

    if session_context and session_context.node_id then
        graph.session_parent_id = session_context.node_id
    end

    for _, op in ipairs(operations) do
        if op.type == compiler.OP_TYPES.WITH_INPUT then
            graph.input_data = op.config.data

        elseif op.type == compiler.OP_TYPES.FUNC then
            local config = {
                func_id = op.config.func_id,
                inputs = op.config.inputs,
                context = op.config.context,
                input_transform = op.config.input_transform
            }

            local node_id, err = graph:create_node("userspace.dataflow.node.func:node", config, op.config.metadata)
            if err then
                return nil, err
            end

        elseif op.type == compiler.OP_TYPES.AGENT then
            local config = {
                agent = op.config.agent_id,
                model = op.config.model,
                arena = op.config.arena,
                inputs = op.config.inputs,
                show_tool_calls = op.config.show_tool_calls,
                input_transform = op.config.input_transform
            }

            local node_id, err = graph:create_node("userspace.dataflow.node.agent:node", config, op.config.metadata)
            if err then
                return nil, err
            end

        elseif op.type == compiler.OP_TYPES.CYCLE then
            local config = {
                func_id = op.config.func_id,
                continue_condition = op.config.continue_condition,
                max_iterations = op.config.max_iterations,
                initial_state = op.config.initial_state,
                inputs = op.config.inputs,
                context = op.config.context,
                input_transform = op.config.input_transform
            }

            local node_id, err = graph:create_node("userspace.dataflow.node.cycle:cycle", config, op.config.metadata)
            if err then
                return nil, err
            end

            if op.config.template then
                graph:create_template_nodes(op.config.template, node_id)
            end

        elseif op.type == compiler.OP_TYPES.MAP_REDUCE then
            local config = {
                source_array_key = op.config.source_array_key,
                iteration_input_key = op.config.iteration_input_key,
                batch_size = op.config.batch_size,
                failure_strategy = op.config.failure_strategy,
                item_steps = op.config.item_steps,
                reduction_extract = op.config.reduction_extract,
                reduction_steps = op.config.reduction_steps,
                inputs = op.config.inputs,
                input_transform = op.config.input_transform
            }

            local node_id, err = graph:create_node("userspace.dataflow.node.map_reduce:map_reduce", config, op.config.metadata)
            if err then
                return nil, err
            end

            if op.config.template then
                graph:create_template_nodes(op.config.template, node_id)
            end

        elseif op.type == compiler.OP_TYPES.STATE then
            local config = {
                inputs = op.config.inputs,
                input_transform = op.config.input_transform
            }

            local node_id, err = graph:create_node("userspace.dataflow.node.state:state", config, op.config.metadata)
            if err then
                return nil, err
            end

        elseif op.type == compiler.OP_TYPES.USE then
            if op.config.operations then
                for _, template_op in ipairs(op.config.operations) do
                    table.insert(graph.operations, template_op)
                end
            end

        elseif op.type == compiler.OP_TYPES.AS then
            if graph.input_data and not graph.input_name and not graph.last_node_id then
                graph.input_name = op.config.name
                graph:add_reference(op.config.name, "INPUT")
            elseif graph.last_node_id then
                local success, err = graph:add_reference(op.config.name, graph.last_node_id)
                if err then
                    return nil, err
                end
            else
                return nil, "Cannot name node: no previous node or input to name"
            end

        elseif op.type == compiler.OP_TYPES.TO then
            if op.config.target == "@success" or op.config.target == "@end" then
                if not graph.last_node_id then
                    return nil, "Cannot route to @success: no source node"
                end
                graph.forced_success_nodes[graph.last_node_id] = true
                graph.has_explicit_routing = true
                table.insert(graph.pending_routes, {
                    from_node_id = graph.last_node_id,
                    is_workflow_success = true,
                    transform = op.config.transform,
                    condition = nil
                })
            elseif graph.input_data and not graph.last_node_id then
                table.insert(graph.input_routes, {
                    target_name = op.config.target,
                    input_key = op.config.input_key or "default",
                    transform = op.config.transform
                })
                graph.has_explicit_routing = true
            elseif graph.last_node_id then
                graph.has_explicit_routing = true
                table.insert(graph.pending_routes, {
                    from_node_id = graph.last_node_id,
                    target_name = op.config.target,
                    input_key = op.config.input_key,
                    transform = op.config.transform,
                    is_error = false,
                    condition = nil
                })
            else
                return nil, "Cannot add route: no source node or input"
            end

        elseif op.type == compiler.OP_TYPES.ERROR_TO then
            if op.config.target == "@fail" or op.config.target == "@end" then
                if not graph.last_node_id then
                    return nil, "Cannot route to @fail: no source node"
                end
                graph.forced_failure_nodes[graph.last_node_id] = true
                graph.has_explicit_routing = true
                table.insert(graph.pending_routes, {
                    from_node_id = graph.last_node_id,
                    is_workflow_failure = true,
                    transform = op.config.transform,
                    is_error = true,
                    condition = nil
                })
            else
                if not graph.last_node_id then
                    return nil, "Cannot add error route: no source node"
                end
                graph.has_explicit_routing = true
                table.insert(graph.pending_routes, {
                    from_node_id = graph.last_node_id,
                    target_name = op.config.target,
                    input_key = op.config.input_key,
                    transform = op.config.transform,
                    is_error = true,
                    condition = nil
                })
            end

        elseif op.type == compiler.OP_TYPES.WHEN then
            if #graph.pending_routes == 0 then
                return nil, "Cannot add condition: no preceding route"
            end
            graph.pending_routes[#graph.pending_routes].condition = op.config.condition
        end

        local success, err = graph:add_operation(op.type, op.config)
        if err then
            return nil, err
        end
    end

    for _, route in ipairs(graph.pending_routes) do
        if route.is_workflow_success or route.is_workflow_failure then
            local edges = graph.edges[route.from_node_id]
            local edge_list = route.is_error and edges.error_targets or edges.targets
            table.insert(edge_list, {
                target_node_id = nil,
                is_workflow_terminal = true,
                is_success = route.is_workflow_success,
                transform = route.transform,
                condition = route.condition
            })
        else
            local target_node_id, resolve_err = graph:resolve_reference(route.target_name)
            if resolve_err then
                return nil, resolve_err
            end
            local edges = graph.edges[route.from_node_id]
            local edge_list = route.is_error and edges.error_targets or edges.targets
            table.insert(edge_list, {
                target_node_id = target_node_id,
                transform = route.transform,
                condition = route.condition,
                input_key = route.input_key
            })
        end
    end

    local has_cycles, cycle_desc = graph:detect_cycles()
    if has_cycles then
        return nil, "Flow contains cycles: " .. cycle_desc
    end

    return graph, nil
end

function compiler.find_root_nodes(graph)
    local nodes_with_incoming = {}

    for _, edges in pairs(graph.edges) do
        for _, edge in ipairs(edges.targets) do
            if edge.target_node_id then
                nodes_with_incoming[edge.target_node_id] = true
            end
        end
        for _, edge in ipairs(edges.error_targets) do
            if edge.target_node_id then
                nodes_with_incoming[edge.target_node_id] = true
            end
        end
    end

    local roots = {}
    for node_id, node_def in pairs(graph.nodes) do
        if not nodes_with_incoming[node_id] and not node_def.parent_node_id then
            table.insert(roots, node_id)
        end
    end

    return roots, nil
end

function compiler.find_leaf_nodes(graph)
    local leaves = {}

    for node_id, edges in pairs(graph.edges) do
        local has_node_targets = false
        for _, edge in ipairs(edges.targets) do
            if edge.target_node_id then
                has_node_targets = true
                break
            end
        end
        for _, edge in ipairs(edges.error_targets) do
            if edge.target_node_id then
                has_node_targets = true
                break
            end
        end
        if not has_node_targets then
            table.insert(leaves, node_id)
        end
    end

    return leaves, nil
end

function compiler.compile_to_commands(graph, session_context)
    if not graph then
        return nil, "Graph is required"
    end

    local commands = {}
    local input_data_id = nil
    local is_nested = session_context and session_context.dataflow_id

    local auto_chained = {}
    for i = 1, #graph.node_order - 1 do
        local current_node_id = graph.node_order[i]
        local next_node_id = graph.node_order[i + 1]
        local current_node = graph.nodes[current_node_id]
        local next_node = graph.nodes[next_node_id]

        if not current_node.parent_node_id and not next_node.parent_node_id then
            local current_edges = graph.edges[current_node_id]

            local has_any_targets = false
            for _, edge in ipairs(current_edges.targets) do
                if edge.target_node_id or edge.is_workflow_terminal then
                    has_any_targets = true
                    break
                end
            end
            for _, edge in ipairs(current_edges.error_targets) do
                if edge.target_node_id or edge.is_workflow_terminal then
                    has_any_targets = true
                    break
                end
            end

            if not has_any_targets then
                auto_chained[current_node_id] = next_node_id
            end
        end
    end

    if graph.input_data then
        if is_nested then
            if #graph.input_routes > 0 then
                for _, route in ipairs(graph.input_routes) do
                    local target_node_id, err = graph:resolve_reference(route.target_name)
                    if err then
                        return nil, err
                    end

                    local content = graph.input_data
                    if route.transform then
                        local transform_env = {
                            input = graph.input_data,
                            output = graph.input_data
                        }
                        local transformed, eval_err = expr.eval(route.transform, transform_env)
                        if eval_err then
                            return nil, "Input route transform failed: " .. eval_err
                        end
                        content = transformed
                    end

                    table.insert(commands, {
                        type = consts.COMMAND_TYPES.CREATE_DATA,
                        payload = {
                            data_id = uuid.v7(),
                            data_type = consts.DATA_TYPE.NODE_INPUT,
                            node_id = target_node_id,
                            discriminator = route.input_key,
                            content = content,
                            content_type = type(content) == "table" and consts.CONTENT_TYPE.JSON or consts.CONTENT_TYPE.TEXT
                        }
                    })
                end
            else
                local root_nodes, roots_err = compiler.find_root_nodes(graph)
                if roots_err then
                    return nil, roots_err
                end

                for _, node_id in ipairs(root_nodes) do
                    table.insert(commands, {
                        type = consts.COMMAND_TYPES.CREATE_DATA,
                        payload = {
                            data_id = uuid.v7(),
                            data_type = consts.DATA_TYPE.NODE_INPUT,
                            node_id = node_id,
                            discriminator = "default",
                            content = graph.input_data,
                            content_type = type(graph.input_data) == "table" and consts.CONTENT_TYPE.JSON or consts.CONTENT_TYPE.TEXT
                        }
                    })
                end
            end
        else
            input_data_id = uuid.v7()
            table.insert(commands, {
                type = consts.COMMAND_TYPES.CREATE_DATA,
                payload = {
                    data_id = input_data_id,
                    data_type = consts.DATA_TYPE.WORKFLOW_INPUT,
                    content = graph.input_data,
                    content_type = type(graph.input_data) == "table" and consts.CONTENT_TYPE.JSON or consts.CONTENT_TYPE.TEXT
                }
            })
        end
    end

    local leaf_nodes, leaf_err = compiler.find_leaf_nodes(graph)
    if leaf_err then
        return nil, leaf_err
    end

    for _, node_id in ipairs(graph.node_order) do
        local node_def = graph.nodes[node_id]
        local config = {}

        for k, v in pairs(node_def.config) do
            config[k] = v
        end

        local edges = graph.edges[node_id]
        local has_explicit_edges = false

        for _, edge in ipairs(edges.targets) do
            if edge.target_node_id or edge.is_workflow_terminal then
                has_explicit_edges = true
                break
            end
        end
        for _, edge in ipairs(edges.error_targets) do
            if edge.target_node_id or edge.is_workflow_terminal then
                has_explicit_edges = true
                break
            end
        end

        if has_explicit_edges then
            config.data_targets = {}
            config.error_targets = {}

            for _, edge in ipairs(edges.targets) do
                if edge.is_workflow_terminal then
                    local has_parent = node_def.parent_node_id or graph.session_parent_id
                    local output_type = has_parent and consts.DATA_TYPE.NODE_OUTPUT or consts.DATA_TYPE.WORKFLOW_OUTPUT

                    table.insert(config.data_targets, {
                        data_type = output_type,
                        discriminator = "result",
                        condition = edge.condition,
                        transform = edge.transform
                    })
                else
                    table.insert(config.data_targets, {
                        data_type = consts.DATA_TYPE.NODE_INPUT,
                        node_id = edge.target_node_id,
                        discriminator = edge.input_key or "default",
                        condition = edge.condition,
                        transform = edge.transform
                    })
                end
            end

            for _, edge in ipairs(edges.error_targets) do
                if edge.is_workflow_terminal then
                    local has_parent = node_def.parent_node_id or graph.session_parent_id
                    local output_type = has_parent and consts.DATA_TYPE.NODE_OUTPUT or consts.DATA_TYPE.WORKFLOW_OUTPUT

                    table.insert(config.error_targets, {
                        data_type = output_type,
                        discriminator = "error",
                        condition = edge.condition,
                        transform = edge.transform
                    })
                else
                    table.insert(config.error_targets, {
                        data_type = consts.DATA_TYPE.NODE_INPUT,
                        node_id = edge.target_node_id,
                        discriminator = edge.input_key or "default",
                        condition = edge.condition,
                        transform = edge.transform
                    })
                end
            end
        else
            local is_auto_chained = auto_chained[node_id] ~= nil
            local is_leaf = false
            local is_template = node_def.status == consts.STATUS.TEMPLATE

            for _, leaf_id in ipairs(leaf_nodes) do
                if leaf_id == node_id then
                    is_leaf = true
                    break
                end
            end

            if is_auto_chained then
                local next_node_id = auto_chained[node_id]
                config.data_targets = {
                    {
                        data_type = consts.DATA_TYPE.NODE_INPUT,
                        node_id = next_node_id,
                        discriminator = "default"
                    }
                }
            elseif is_leaf and not is_template then
                local has_parent = node_def.parent_node_id or graph.session_parent_id
                local output_data_type = has_parent and consts.DATA_TYPE.NODE_OUTPUT or consts.DATA_TYPE.WORKFLOW_OUTPUT

                config.data_targets = {
                    {
                        data_type = output_data_type,
                        discriminator = "result",
                        content_type = consts.CONTENT_TYPE.JSON
                    }
                }
            end
        end

        local node_payload = {
            node_id = node_id,
            node_type = node_def.node_type,
            status = node_def.status,
            config = config,
            metadata = node_def.metadata
        }

        if node_def.parent_node_id then
            node_payload.parent_node_id = node_def.parent_node_id
        elseif graph.session_parent_id then
            node_payload.parent_node_id = graph.session_parent_id
        end

        table.insert(commands, {
            type = consts.COMMAND_TYPES.CREATE_NODE,
            payload = node_payload
        })
    end

    if input_data_id and not is_nested then
        local root_nodes, roots_err = compiler.find_root_nodes(graph)
        if roots_err then
            return nil, roots_err
        end

        if #graph.input_routes > 0 then
            for _, route in ipairs(graph.input_routes) do
                local target_node_id, err = graph:resolve_reference(route.target_name)
                if err then
                    return nil, err
                end

                table.insert(commands, {
                    type = consts.COMMAND_TYPES.CREATE_DATA,
                    payload = {
                        data_id = uuid.v7(),
                        data_type = consts.DATA_TYPE.NODE_INPUT,
                        node_id = target_node_id,
                        key = input_data_id,
                        discriminator = route.input_key,
                        content = "",
                        content_type = "dataflow/reference"
                    }
                })
            end
        else
            for _, node_id in ipairs(root_nodes) do
                table.insert(commands, {
                    type = consts.COMMAND_TYPES.CREATE_DATA,
                    payload = {
                        data_id = uuid.v7(),
                        data_type = consts.DATA_TYPE.NODE_INPUT,
                        node_id = node_id,
                        key = input_data_id,
                        discriminator = "default",
                        content = "",
                        content_type = "dataflow/reference"
                    }
                })
            end
        end
    end

    return commands, nil
end

function compiler.compile(operations, session_context)
    if not operations or #operations == 0 then
        return nil, "No operations to compile"
    end

    local graph, graph_err = compiler.build_graph(operations, session_context)
    if graph_err then
        return nil, graph_err
    end

    local commands, commands_err = compiler.compile_to_commands(graph, session_context)
    if commands_err then
        return nil, commands_err
    end

    return {
        commands = commands,
        graph = graph
    }, nil
end

return compiler