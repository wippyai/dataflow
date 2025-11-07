local json = require("json")
local uuid = require("uuid")
local node_sdk = require("node_sdk")
local agent_context = require("agent_context")
local tool_caller = require("tool_caller")
local prompt_builder = require("prompt_builder")
local control_handler = require("control_handler")
local delegation_handler = require("delegation_handler")
local agent_consts = require("agent_consts")
local consts = require("consts")
local tools = require("tools")
local registry = require("registry")
local funcs = require("funcs")

local function merge_contexts(base_context, input_context)
    local merged = {}
    if base_context then
        for k, v in pairs(base_context) do
            merged[k] = v
        end
    end
    if input_context then
        for k, v in pairs(input_context) do
            merged[k] = v
        end
    end
    return merged
end

local function format_token_count(count)
    if count >= 1000 then
        return string.format("%.1fK", count / 1000)
    else
        return tostring(count)
    end
end

local function build_status_message(iteration, max_iterations, total_tokens, tool_calls_count, is_final, task_complete)
    local status_parts = {}

    if is_final then
        if task_complete then
            table.insert(status_parts, string.format("Completed %d/%d", iteration, max_iterations))
        else
            table.insert(status_parts, string.format("Max iterations %d/%d", iteration, max_iterations))
        end
    else
        if iteration == 0 then
            table.insert(status_parts, "Starting agent")
        else
            table.insert(status_parts, string.format("Iteration %d/%d", iteration, max_iterations))
        end
    end

    local details = {}

    if total_tokens.prompt_tokens and total_tokens.prompt_tokens > 0 then
        table.insert(details, "in: " .. format_token_count(total_tokens.prompt_tokens))
    end

    local completion_total = (total_tokens.completion_tokens or 0) + (total_tokens.thinking_tokens or 0)
    if completion_total > 0 then
        table.insert(details, "out: " .. format_token_count(completion_total))
    end

    if tool_calls_count > 0 then
        table.insert(details, "T: " .. tool_calls_count)
    end

    if #details > 0 then
        table.insert(status_parts, table.concat(details, ", "))
    end

    return table.concat(status_parts, " - ")
end

local function process_multiple_inputs(inputs)
    local input_context = nil
    if inputs.context then
        local context_content = inputs.context.content
        if type(context_content) ~= "table" then
            return nil, nil, nil, nil, "context must be a table/object"
        end
        input_context = context_content
    end

    local agent_id_override = nil
    if inputs.agent_id then
        local agent_id_content = inputs.agent_id.content
        if type(agent_id_content) ~= "string" or agent_id_content == "" then
            return nil, nil, nil, nil, "agent_id must be a non-empty string"
        end
        agent_id_override = agent_id_content
    end

    local model_override = nil
    if inputs.model then
        local model_content = inputs.model.content
        if type(model_content) ~= "string" or model_content == "" then
            return nil, nil, nil, nil, "model must be a non-empty string"
        end
        model_override = model_content
    end

    local parts = {}
    for key, input in pairs(inputs) do
        if key ~= "context" and key ~= "agent_id" and key ~= "model" then
            local content = input.content
            if type(content) == "table" then
                content = json.encode(content)
            else
                content = tostring(content)
            end
            table.insert(parts, string.format('<input key="%s">\n%s\n</input>', key, content))
        end
    end

    if #parts == 0 then
        return input_context, agent_id_override, model_override, "", nil
    end

    return input_context, agent_id_override, model_override, table.concat(parts, "\n\n"), nil
end

local function validate_and_resolve_config(config)
    if not config then
        return nil, agent_consts.ERROR_MSG.INVALID_CONFIG
    end

    if not config.arena then
        return nil, "Arena configuration is required"
    end

    local tool_calling = config.arena.tool_calling or agent_consts.DEFAULTS.TOOL_CALLING
    local has_exit_schema = config.arena.exit_schema ~= nil

    if tool_calling == agent_consts.TOOL_CALLING.AUTO and has_exit_schema then
        config.arena.tool_calling = tool_calling
    end

    if tool_calling == agent_consts.TOOL_CALLING.ANY and not has_exit_schema then
        return nil, "any mode requires exit_schema to be defined"
    end

    if tool_calling == agent_consts.TOOL_CALLING.NONE and has_exit_schema then
        return nil, "none mode cannot have exit_schema"
    end

    return config, nil
end

local function setup_exit_tool(agent_ctx, arena_config)
    local exit_tool_name = nil
    local should_add_exit_tool = (arena_config.tool_calling == agent_consts.TOOL_CALLING.ANY) or
        (arena_config.tool_calling == agent_consts.TOOL_CALLING.AUTO and arena_config.exit_schema)

    if should_add_exit_tool then
        exit_tool_name = "finish"

        local exit_schema = arena_config.exit_schema or {
            type = "object",
            properties = {
                answer = {
                    type = "string",
                    description = "Your final answer to complete the task"
                }
            },
            required = { "answer" }
        }

        agent_ctx:add_tools({
            {
                id = exit_tool_name,
                name = exit_tool_name,
                description = "Call this tool when you have completed the task and want to provide your final answer",
                schema = exit_schema
            }
        })
    end

    if arena_config.tools and #arena_config.tools > 0 then
        agent_ctx:add_tools(arena_config.tools)
    end

    return exit_tool_name
end

local function accumulate_tokens(total_tokens, new_tokens)
    if not new_tokens then
        return total_tokens
    end

    total_tokens.total_tokens = (total_tokens.total_tokens or 0) + (new_tokens.total_tokens or 0)
    total_tokens.prompt_tokens = (total_tokens.prompt_tokens or 0) + (new_tokens.prompt_tokens or 0)
    total_tokens.completion_tokens = (total_tokens.completion_tokens or 0) + (new_tokens.completion_tokens or 0)
    total_tokens.cache_read_tokens = (total_tokens.cache_read_tokens or 0) + (new_tokens.cache_read_tokens or 0)
    total_tokens.cache_write_tokens = (total_tokens.cache_write_tokens or 0) + (new_tokens.cache_write_tokens or 0)
    total_tokens.thinking_tokens = (total_tokens.thinking_tokens or 0) + (new_tokens.thinking_tokens or 0)

    return total_tokens
end

local function update_node_progress(n, iteration, max_iterations, total_tokens, tool_calls_count, status_message,
                                    agent_id, model_name)
    local state_info = {
        current_iteration = iteration,
        max_iterations = max_iterations,
        agent_id = agent_id,
        model = model_name,
        total_tokens = total_tokens,
        tool_calls = tool_calls_count
    }

    n:update_metadata({
        status_message = status_message,
        state = state_info
    })
end

local function store_agent_action(n, agent_result, iteration, agent_id, model_name, exit_tool_name, control_metadata)
    local action_content = {
        result = agent_result.result,
        tool_calls = agent_result.tool_calls,
        delegate_calls = agent_result.delegate_calls
    }

    local is_exit_action = false
    if exit_tool_name and agent_result.tool_calls then
        for _, tool_call in ipairs(agent_result.tool_calls) do
            if tool_call.name == exit_tool_name then
                is_exit_action = true
                break
            end
        end
    end

    local action_key = is_exit_action and (iteration .. "_final") or (iteration .. "_action")

    local metadata = {
        iteration = iteration,
        agent_id = agent_id,
        model = model_name,
        tokens = agent_result.tokens,
        finish_reason = agent_result.finish_reason,
        llm_meta = agent_result.metadata or {},
    }

    if control_metadata and next(control_metadata) then
        metadata._control = control_metadata
    end

    n:data(agent_consts.DATA_TYPE.AGENT_ACTION, action_content, {
        key = action_key,
        content_type = consts.CONTENT_TYPE.JSON,
        node_id = n.node_id,
        metadata = metadata
    })
end

local function store_memory_recall(n, agent_result, iteration)
    if not agent_result.memory_prompt then
        return
    end

    n:data(agent_consts.DATA_TYPE.AGENT_MEMORY, agent_result.memory_prompt.content, {
        key = iteration .. "_memory",
        content_type = consts.CONTENT_TYPE.TEXT,
        node_id = n.node_id,
        metadata = {
            iteration = iteration,
            memory_ids = agent_result.memory_prompt.metadata and agent_result.memory_prompt.metadata.memory_ids,
            llm_meta = agent_result.memory_prompt.metadata or {}
        }
    })
end

local function get_tool_title_by_registry_id(registry_id, tool_name)
    if not registry_id then
        return tool_name
    end

    local tool_schema = tools.get_tool_schema(registry_id)
    if tool_schema and tool_schema.title then
        return tool_schema.title
    end

    return tool_name
end

local function create_tool_viz_nodes(n, tool_calls, iteration, show_tool_calls, exit_tool_name)
    local tool_call_to_node_id = {}

    if show_tool_calls == false or not tool_calls or #tool_calls == 0 then
        return tool_call_to_node_id
    end

    for _, tool_call in ipairs(tool_calls) do
        if exit_tool_name and tool_call.name == exit_tool_name then
            goto continue
        end

        local viz_node_id = uuid.v7()
        tool_call_to_node_id[tool_call.id] = viz_node_id

        local input_size = 0
        if tool_call.arguments then
            local args_json = json.encode(tool_call.arguments)
            input_size = string.len(args_json)
        end

        local tool_title = get_tool_title_by_registry_id(tool_call.registry_id, tool_call.name)

        local metadata = {
            tool_name = tool_call.name,
            tool_call_id = tool_call.id,
            iteration = iteration,
            title = tool_title,
            input_size_bytes = input_size
        }

        if tool_call.registry_id then
            metadata.registry_id = tool_call.registry_id
        end

        n:command({
            type = consts.COMMAND_TYPES.CREATE_NODE,
            payload = {
                node_id = viz_node_id,
                node_type = "tool.call",
                parent_node_id = n.node_id,
                status = consts.STATUS.RUNNING,
                config = {},
                metadata = metadata
            }
        })

        ::continue::
    end

    return tool_call_to_node_id
end

local function update_tool_viz_nodes(n, tool_results, tool_call_to_node_id)
    if not tool_results or not tool_call_to_node_id then
        return
    end

    for call_id, result_data in pairs(tool_results) do
        local viz_node_id = tool_call_to_node_id[call_id]
        if viz_node_id then
            local tool_result = result_data.result
            local tool_error = result_data.error

            local output_size = 0
            local output_content = tool_result or tool_error
            if output_content then
                local output_json = type(output_content) == "table" and json.encode(output_content) or
                    tostring(output_content)
                output_size = string.len(output_json)
            end

            local final_status = tool_error and consts.STATUS.COMPLETED_FAILURE or consts.STATUS.COMPLETED_SUCCESS

            n:command({
                type = consts.COMMAND_TYPES.UPDATE_NODE,
                payload = {
                    node_id = viz_node_id,
                    status = final_status,
                    metadata = {
                        has_error = tool_error ~= nil,
                        error_message = tool_error,
                        output_size_bytes = output_size
                    }
                }
            })
        end
    end
end

local function execute_tools(agent_result, caller, session_context)
    if not agent_result.tool_calls or #agent_result.tool_calls == 0 then
        return {}
    end

    local validated_tools, validate_err = caller:validate(agent_result.tool_calls)
    if validate_err then
        return {}
    end

    local tool_results = caller:execute(session_context or {}, validated_tools)
    return tool_results or {}
end

local function process_tool_results(n, tool_results, iteration, exit_tool_name, agent_result, arena_config, session_context)
    local control_responses = {}
    local control_delegations = {}
    local task_complete = false
    local final_result = nil

    if exit_tool_name and agent_result.tool_calls then
        for _, original_tool_call in ipairs(agent_result.tool_calls) do
            if original_tool_call.name == exit_tool_name then
                local exit_arguments = original_tool_call.arguments

                if arena_config.exit_func_id then
                    local validated_result, validation_err = funcs.new()
                        :with_context(session_context)
                        :call(arena_config.exit_func_id, exit_arguments)

                    if validation_err then
                        local tool_key = iteration .. "_" .. exit_tool_name
                        n:data(agent_consts.DATA_TYPE.AGENT_OBSERVATION, "error: " .. validation_err, {
                            key = tool_key,
                            content_type = consts.CONTENT_TYPE.TEXT,
                            node_id = n.node_id,
                            metadata = {
                                iteration = iteration,
                                tool_call_id = original_tool_call.id,
                                tool_name = exit_tool_name,
                                is_error = true,
                                exit_validation = true
                            }
                        })
                        task_complete = false
                    else
                        task_complete = true
                        final_result = validated_result
                    end
                else
                    task_complete = true
                    final_result = exit_arguments or { success = false, error = "Exit tool called without arguments" }
                end
                break
            end
        end
    end

    if task_complete then
        return control_responses, control_delegations, task_complete, final_result
    end

    if agent_result.tool_calls then
        for _, tool_call in ipairs(agent_result.tool_calls) do
            local call_id = tool_call.id
            local result_data = tool_results[call_id]

            if result_data then
                local tool_result = result_data.result
                local tool_error = result_data.error

                local cleaned_result, control_response = control_handler.process_control_directive(
                    tool_result, n, iteration
                )
                if control_response then
                    table.insert(control_responses, control_response)

                    if control_response.delegate then
                        for _, delegation in ipairs(control_response.delegate) do
                            table.insert(control_delegations, {
                                delegation = delegation,
                                tool_call = tool_call,
                                control_response = control_response
                            })
                        end
                    end
                end

                if not (control_response and control_response.delegate) then
                    local obs_content = cleaned_result or tool_error
                    if obs_content == nil then
                        obs_content = "nil"
                    end

                    local tool_key = iteration .. "_" .. tool_call.name

                    n:data(agent_consts.DATA_TYPE.AGENT_OBSERVATION, obs_content, {
                        key = tool_key,
                        content_type = type(obs_content) == "table" and consts.CONTENT_TYPE.JSON or consts.CONTENT_TYPE.TEXT,
                        node_id = n.node_id,
                        metadata = {
                            iteration = iteration,
                            tool_call_id = call_id,
                            tool_name = tool_call.name,
                            is_error = tool_error ~= nil
                        }
                    })
                end
            end
        end
    end

    return control_responses, control_delegations, task_complete, final_result
end

local function tools_were_attempted(agent_result)
    if agent_result.tool_calls and #agent_result.tool_calls > 0 then
        return true
    end

    if agent_result.delegate_calls and #agent_result.delegate_calls > 0 then
        return true
    end

    return false
end

local function check_completion(tool_calling, agent_result, iteration, min_iterations, exit_tool_name, n)
    local task_complete = false
    local final_result = nil

    if iteration < min_iterations then
        return task_complete, final_result
    end

    if tool_calling == agent_consts.TOOL_CALLING.NONE then
        if agent_result.result and agent_result.result ~= "" then
            task_complete = true
            final_result = agent_result.result
        end
    elseif tool_calling == agent_consts.TOOL_CALLING.AUTO then
        if not tools_were_attempted(agent_result) then
            if agent_result.result and agent_result.result ~= nil then
                task_complete = true
                final_result = agent_result.result
            else
                local feedback = agent_consts.FEEDBACK.NO_TOOLS_CALLED
                n:data(agent_consts.DATA_TYPE.AGENT_OBSERVATION, feedback, {
                    key = iteration .. "_no_tools_called",
                    content_type = consts.CONTENT_TYPE.TEXT,
                    node_id = n.node_id,
                    metadata = {
                        iteration = iteration
                    }
                })
            end
        end
    elseif tool_calling == agent_consts.TOOL_CALLING.ANY then
        if not tools_were_attempted(agent_result) then
            local feedback = agent_consts.FEEDBACK.NO_TOOLS_CALLED
            if exit_tool_name then
                feedback = feedback .. " " .. string.format(agent_consts.FEEDBACK.EXIT_AVAILABLE, exit_tool_name)
            end
            n:data(agent_consts.DATA_TYPE.AGENT_OBSERVATION, feedback, {
                key = iteration .. "_no_tools_called",
                content_type = consts.CONTENT_TYPE.TEXT,
                node_id = n.node_id,
                metadata = {
                    iteration = iteration
                }
            })
        end
    end

    return task_complete, final_result
end

local function get_delegation_data_id(n)
    local reader = n:query()
        :with_nodes(n.node_id)
        :with_data_types(agent_consts.DATA_TYPE.AGENT_DELEGATION)

    local delegation_data = reader:all()
    if delegation_data and #delegation_data > 0 then
        return delegation_data[#delegation_data].data_id
    end

    return nil
end

local function run(args)
    local n, err = node_sdk.new(args)
    if err then
        error(err)
    end

    local config = n:config()
    local validated_config, config_err = validate_and_resolve_config(config)
    if config_err then
        return n:fail({
            code = agent_consts.ERROR.INVALID_CONFIG,
            message = config_err
        }, config_err)
    end

    local inputs, inputs_err = n:inputs()
    if inputs_err then
        return n:fail({
            code = agent_consts.ERROR.INPUT_VALIDATION_FAILED,
            message = inputs_err
        }, inputs_err)
    end

    local input_context, agent_id_override, model_override, input_data, input_err = process_multiple_inputs(inputs)
    if input_err then
        return n:fail({
            code = agent_consts.ERROR.INPUT_VALIDATION_FAILED,
            message = input_err
        }, input_err)
    end

    if agent_id_override then
        n:update_config({ agent = agent_id_override })

        local entry = registry.get(agent_id_override)
        if entry and entry.meta and (entry.meta.title or entry.meta.name) then
            local title = entry.meta.title or entry.meta.name
            n:update_metadata({title = title})
        else
            n:update_metadata({title = "Agent: " .. agent_id_override})
        end
    end

    model_override = model_override or config.model or config.arena.model

    local arena_context = config.arena.context or {}
    local session_context = {
        dataflow_id = n.dataflow_id,
        node_id = n.node_id,
    }
    for k, v in pairs(arena_context) do
        session_context[k] = v
    end

    local base_context = {
        enable_cache = false,
        delegate_tools = {
            enabled = agent_consts.DELEGATE_DEFAULTS.GENERATE_TOOL_SCHEMAS,
            description_suffix = agent_consts.DELEGATE_DEFAULTS.DESCRIPTION_SUFFIX,
            default_schema = agent_consts.DELEGATE_DEFAULTS.SCHEMA
        }
    }

    if model_override then
        base_context.model = model_override
    end

    local merged_context = merge_contexts(base_context, input_context)
    local agent_ctx = agent_context.new(merged_context)

    local exit_tool_name = setup_exit_tool(agent_ctx, config.arena)

    local agent_to_load = agent_id_override or config.agent

    if not agent_to_load or agent_to_load == "" then
        return n:fail({
            code = agent_consts.ERROR.AGENT_LOAD_FAILED,
            message = "Agent ID not specified in config or inputs"
        }, "Agent ID not specified in config or inputs")
    end

    local load_options = model_override and {model = model_override} or nil
    local agent_instance, agent_err = agent_ctx:load_agent(agent_to_load, load_options)
    if not agent_instance then
        return n:fail({
            code = agent_consts.ERROR.AGENT_LOAD_FAILED,
            message = string.format(agent_consts.ERROR_MSG.AGENT_LOAD_FAILED, agent_err or "unknown error")
        }, string.format(agent_consts.ERROR_MSG.AGENT_LOAD_FAILED, agent_err or "unknown error"))
    end

    local agent_config = agent_ctx:get_config()
    local agent_id = agent_config.current_agent_id or
        (type(agent_to_load) == "table" and agent_to_load.id or agent_to_load)
    local model_name = agent_config.current_model or "unknown"

    local builder, builder_err = prompt_builder.new(n.dataflow_id, n.node_id, n.path)
    if builder_err then
        return n:fail({
            code = agent_consts.ERROR.PROMPT_BUILD_FAILED,
            message = builder_err
        }, builder_err)
    end

    builder:with_arena_config(config.arena):with_initial_input(input_data)
    local caller = tool_caller.new()

    local iteration = 0
    local max_iterations = config.arena.max_iterations or agent_consts.DEFAULTS.MAX_ITERATIONS
    local min_iterations = config.arena.min_iterations or agent_consts.DEFAULTS.MIN_ITERATIONS
    local tool_calling = config.arena.tool_calling
    local show_tool_calls = config.show_tool_calls ~= false
    local task_complete = false
    local final_result = nil

    local total_tokens = {
        total_tokens = 0,
        prompt_tokens = 0,
        completion_tokens = 0,
        cache_read_tokens = 0,
        cache_write_tokens = 0,
        thinking_tokens = 0
    }
    local tool_calls_count = 0

    local initial_status = build_status_message(0, max_iterations, total_tokens, tool_calls_count, false, false)
    update_node_progress(n, 0, max_iterations, total_tokens, tool_calls_count, initial_status, agent_id, model_name)

    while iteration < max_iterations and not task_complete do
        iteration = iteration + 1

        local prompt, prompt_err = builder:build_prompt(config.arena.prompt)
        if prompt_err then
            return n:fail({
                code = agent_consts.ERROR.PROMPT_BUILD_FAILED,
                message = prompt_err
            }, prompt_err)
        end

        local step_options = { tool_call = tool_calling, context = session_context }
        local agent_result, step_err = agent_instance:step(prompt, step_options)
        if step_err then
            return n:fail({
                code = agent_consts.ERROR.AGENT_EXEC_FAILED,
                message = step_err
            }, step_err)
        end

        local regular_tool_calls = agent_result.tool_calls or {}
        local delegate_calls = agent_result.delegate_calls or {}

        for _, tool_call in ipairs(regular_tool_calls) do
            if not exit_tool_name or tool_call.name ~= exit_tool_name then
                tool_calls_count = tool_calls_count + 1
            end
        end

        total_tokens = accumulate_tokens(total_tokens, agent_result.tokens)

        local status_msg = build_status_message(iteration, max_iterations, total_tokens, tool_calls_count, false, false)
        update_node_progress(n, iteration, max_iterations, total_tokens, tool_calls_count, status_msg, agent_id,
            model_name)

        store_memory_recall(n, agent_result, iteration)

        local tool_call_to_node_id = create_tool_viz_nodes(n, regular_tool_calls, iteration, show_tool_calls,
            exit_tool_name)

        local tool_results = execute_tools({ tool_calls = regular_tool_calls }, caller, session_context)

        if show_tool_calls then
            update_tool_viz_nodes(n, tool_results, tool_call_to_node_id)
        end

        local control_metadata = {}
        for _, tool_call in ipairs(regular_tool_calls) do
            local result_data = tool_results[tool_call.id]
            if result_data and type(result_data.result) == "table" and result_data.result._control then
                control_metadata[tool_call.id] = result_data.result._control
            end
        end

        store_agent_action(n, agent_result, iteration, agent_id, model_name, exit_tool_name, control_metadata)

        local control_responses, control_delegations, tool_complete, tool_result = process_tool_results(n, tool_results, iteration,
            exit_tool_name, { tool_calls = regular_tool_calls }, config.arena, session_context)

        for _, control_del in ipairs(control_delegations) do
            local delegation = control_del.delegation
            local tool_call = control_del.tool_call

            local delegate_call = {
                agent_id = delegation.agent_id,
                arguments = delegation.input_data,
                context = delegation.context,
                name = "delegate_" .. delegation.agent_id,
                id = tool_call.id,
                system_prompt = delegation.system_prompt,
                max_iterations = delegation.max_iterations,
                tool_calling = delegation.tool_calling,
                traits = delegation.traits,
                tools = delegation.tools,
                exit_schema = delegation.exit_schema
            }
            table.insert(delegate_calls, delegate_call)
        end

        local remaining_iterations = max_iterations - iteration
        if remaining_iterations == 2 then
            local warning_msg = string.format(agent_consts.FEEDBACK.ITERATIONS_WARNING, remaining_iterations)
            n:data(agent_consts.DATA_TYPE.AGENT_OBSERVATION, warning_msg, {
                key = iteration .. "_iterations_warning",
                content_type = consts.CONTENT_TYPE.TEXT,
                node_id = n.node_id,
                metadata = {
                    iteration = iteration,
                    remaining_iterations = remaining_iterations
                }
            })
        elseif remaining_iterations == 1 then
            local warning_msg = agent_consts.FEEDBACK.FINAL_ITERATION
            n:data(agent_consts.DATA_TYPE.AGENT_OBSERVATION, warning_msg, {
                key = iteration .. "_final_warning",
                content_type = consts.CONTENT_TYPE.TEXT,
                node_id = n.node_id,
                metadata = {
                    iteration = iteration,
                    remaining_iterations = remaining_iterations
                }
            })
        elseif remaining_iterations == 0 then
            local warning_msg = agent_consts.FEEDBACK.CRITICAL_FINAL
            n:data(agent_consts.DATA_TYPE.AGENT_OBSERVATION, warning_msg, {
                key = iteration .. "_critical_warning",
                content_type = consts.CONTENT_TYPE.TEXT,
                node_id = n.node_id,
                metadata = {
                    iteration = iteration,
                    remaining_iterations = remaining_iterations
                }
            })
        end

        n:yield()

        if tool_complete then
            task_complete = true
            final_result = tool_result
        end

        local has_delegations = #delegate_calls > 0

        if has_delegations then
            if #control_responses > 0 then
                local changes_summary, changes_err = control_handler.apply_control_responses(control_responses, agent_ctx,
                    n)
                if changes_err then
                    return n:fail({
                        code = agent_consts.ERROR.STEP_FUNCTION_FAILED,
                        message = changes_err
                    }, changes_err)
                end
            end

            local delegation_infos = delegation_handler.create_delegation_batch(
                { delegate_calls = delegate_calls },
                n,
                session_context
            )

            local delegation_results, delegation_err = delegation_handler.execute_delegation_batch(delegation_infos, n)
            if delegation_err then
                return n:fail({
                    code = agent_consts.ERROR.DELEGATION_FAILED,
                    message = delegation_err
                }, delegation_err)
            end

            delegation_handler.map_delegation_results_to_conversation(delegation_results, n, iteration)
            n:yield()
        elseif #control_responses > 0 then
            local changes_summary, changes_err = control_handler.apply_control_responses(control_responses, agent_ctx, n)
            if changes_err then
                return n:fail({
                    code = agent_consts.ERROR.STEP_FUNCTION_FAILED,
                    message = changes_err
                }, changes_err)
            end

            local created_node_ids = {}
            for _, response in ipairs(control_responses) do
                if response.changes_applied and response.changes_applied.commands and response.changes_applied.created_nodes then
                    for _, node_id in ipairs(response.changes_applied.created_nodes) do
                        table.insert(created_node_ids, node_id)
                    end
                end
            end

            if #created_node_ids > 0 then
                local yield_result, yield_err = n:yield({ run_nodes = created_node_ids })
                if yield_result then
                    local reader = n:query()
                        :with_nodes(created_node_ids)
                        :with_data_types(consts.DATA_TYPE.NODE_OUTPUT)
                        :fetch_options({ replace_references = true })

                    local output_data = reader:all()

                    if output_data and #output_data > 0 then
                        local output_content = output_data[1].content
                        if #output_data > 1 then
                            local all_outputs = {}
                            for _, output in ipairs(output_data) do
                                table.insert(all_outputs, output.content)
                            end
                            output_content = all_outputs
                        end

                        n:data(agent_consts.DATA_TYPE.AGENT_OBSERVATION, output_content, {
                            key = iteration .. "_commands_output",
                            content_type = type(output_content) == "table" and consts.CONTENT_TYPE.JSON or consts.CONTENT_TYPE.TEXT,
                            node_id = n.node_id,
                            metadata = {
                                iteration = iteration,
                                created_nodes = created_node_ids
                            }
                        })
                    end
                elseif yield_err then
                    n:data(agent_consts.DATA_TYPE.AGENT_OBSERVATION, "Command execution failed: " .. yield_err, {
                        key = iteration .. "_commands_error",
                        content_type = consts.CONTENT_TYPE.TEXT,
                        node_id = n.node_id,
                        metadata = {
                            iteration = iteration,
                            is_error = true
                        }
                    })
                end
            end
        end

        if not task_complete and not has_delegations then
            task_complete, final_result = check_completion(tool_calling, agent_result, iteration, min_iterations,
                exit_tool_name, n)
        end
    end

    if not task_complete and iteration >= max_iterations then
        local final_status = build_status_message(iteration, max_iterations, total_tokens, tool_calls_count, true, false)
        update_node_progress(n, iteration, max_iterations, total_tokens, tool_calls_count, final_status, agent_id,
            model_name)

        return n:fail({
            code = agent_consts.ERROR.AGENT_EXEC_FAILED,
            message = "Maximum iterations reached without completion"
        }, "Maximum iterations reached")
    end

    local final_status = build_status_message(iteration, max_iterations, total_tokens, tool_calls_count, true,
        task_complete)
    update_node_progress(n, iteration, max_iterations, total_tokens, tool_calls_count, final_status, agent_id, model_name)

    local output_content = final_result or { success = false, error = "No result produced" }
    local success = final_result and final_result.success ~= false
    local message = success and agent_consts.STATUS.COMPLETED_SUCCESS or
        (final_result and final_result.error and (agent_consts.STATUS.COMPLETED_ERROR .. final_result.error) or "Agent execution failed")

    local delegation_data_id = get_delegation_data_id(n)
    if delegation_data_id then
        n:update_metadata({
            delegation_output_data_id = delegation_data_id
        })
    end

    local output_content = final_result or { success = false, error = "No result produced" }
    return n:complete(output_content, message)
end

return { run = run }