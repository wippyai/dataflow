local json = require("json")
local uuid = require("uuid")
local expr = require("expr")
local consts = require("consts")

local default_deps = {
    commit = require("commit"),
    data_reader = require("data_reader"),
    process = process
}

local node = {}
local methods = {}
local mt = { __index = methods }

local function merge_metadata(existing, new_fields)
    local existing_count = 0
    local new_count = 0

    if type(existing) == "table" then
        for _ in pairs(existing) do
            existing_count = existing_count + 1
        end
    end

    if type(new_fields) == "table" then
        for _ in pairs(new_fields) do
            new_count = new_count + 1
        end
    end

    local result = table.create(0, existing_count + new_count)

    if type(existing) == "table" then
        for k, v in pairs(existing) do
            result[k] = v
        end
    end
    if type(new_fields) == "table" then
        for k, v in pairs(new_fields) do
            result[k] = v
        end
    end
    return result
end

local function create_transform_env(raw_inputs)
    local input_count = 0
    for _ in pairs(raw_inputs) do
        input_count = input_count + 1
    end

    local inputs_by_key = table.create(0, input_count)
    local default_content = nil

    for key, input_data in pairs(raw_inputs) do
        inputs_by_key[key] = input_data.content

        if key == "default" or key == "" then
            default_content = input_data.content
        end
    end

    local primary_input
    if input_count == 1 then
        for _, content in pairs(inputs_by_key) do
            primary_input = content
            break
        end
    else
        primary_input = default_content
    end

    return {
        input = primary_input,
        inputs = inputs_by_key
    }
end

local function is_dataflow_reference(value)
    return type(value) == "table" and value._dataflow_ref ~= nil
end

local function create_reference_data(self, target, ref_id)
    local data_id = uuid.v7()
    table.insert(self._created_data_ids, data_id)

    local command = {
        type = consts.COMMAND_TYPES.CREATE_DATA,
        payload = {
            data_id = data_id,
            data_type = target.data_type,
            key = ref_id,
            content = "",
            content_type = "dataflow/reference",
            discriminator = target.discriminator,
            node_id = target.node_id or self.node_id,
            metadata = target.metadata
        }
    }

    table.insert(self._queued_commands, command)
    return data_id
end

function node.new(args, deps)
    if not args then
        return nil, "Node args required"
    end
    if not args.node_id or not args.dataflow_id then
        return nil, "Node args must contain node_id and dataflow_id"
    end

    deps = deps or default_deps

    local yield_reply_topic = consts.MESSAGE_TOPIC.YIELD_REPLY_PREFIX .. args.node_id
    local yield_channel = deps.process.listen(yield_reply_topic)

    local instance = {
        node_id = args.node_id,
        dataflow_id = args.dataflow_id,
        node = args.node or {},
        path = args.path or table.create(1, 0),

        _config = (args.node and args.node.config) or {},
        data_targets = (args.node and args.node.config and args.node.config.data_targets) or table.create(0, 0),
        error_targets = (args.node and args.node.config and args.node.config.error_targets) or table.create(0, 0),

        _metadata = (args.node and args.node.metadata) or {},
        _queued_commands = table.create(10, 0),
        _created_data_ids = table.create(5, 0),
        _cached_inputs = nil,

        _yield_channel = yield_channel,
        _yield_reply_topic = yield_reply_topic,
        _last_yield_id = nil,

        _deps = deps
    }

    if not instance.path[1] or instance.path[1] ~= args.node_id then
        table.insert(instance.path, args.node_id)
    end

    return setmetatable(instance, mt), nil
end

function methods:config()
    return self._config
end

function methods:_transform_inputs_with_expr(raw_inputs, transform_config)
    local env = create_transform_env(raw_inputs)

    if type(transform_config) == "string" then
        local content, err = expr.eval(transform_config, env)
        if err then
            return nil, "Input transform failed: " .. err
        end
        return {
            ["default"] = {
                content = content,
                metadata = {},
                key = "default",
                discriminator = nil
            }
        }, nil
    end

    if type(transform_config) ~= "table" then
        return nil, "input_transform must be string or table"
    end

    local field_count = 0
    for _ in pairs(transform_config) do
        field_count = field_count + 1
    end

    local result = table.create(0, field_count)
    for field_name, expression in pairs(transform_config) do
        local content, err = expr.eval(expression, env)
        if err then
            return nil, "Transform failed for " .. field_name .. ": " .. err
        end
        result[field_name] = {
            content = content,
            metadata = {},
            discriminator = field_name
        }
    end
    return result, nil
end

function methods:_load_raw_inputs()
    local input_data = self._deps.data_reader.with_dataflow(self.dataflow_id)
        :with_nodes(self.node_id)
        :with_data_types(consts.DATA_TYPE.NODE_INPUT)
        :fetch_options({ replace_references = true })
        :all()

    local inputs_map = table.create(0, #input_data)

    for _, input in ipairs(input_data) do
        local parsed_content = input.content

        if input.content_type == consts.CONTENT_TYPE.JSON and type(input.content) == "string" then
            local parsed, err = json.decode(input.content)
            if not err then
                parsed_content = parsed
            end
        end

        local map_key = input.discriminator or ""
        inputs_map[map_key] = {
            content = parsed_content,
            metadata = input.metadata or {},
            key = input.key,
            discriminator = input.discriminator
        }
    end

    return inputs_map
end

function methods:inputs()
    if self._cached_inputs then
        return self._cached_inputs
    end

    local raw_inputs = self:_load_raw_inputs()

    local transform_config = self._config.input_transform
    if transform_config then
        local transformed, err = self:_transform_inputs_with_expr(raw_inputs, transform_config)
        if err then
            return nil, err
        end
        self._cached_inputs = transformed
        return transformed, nil
    end

    self._cached_inputs = raw_inputs
    return raw_inputs, nil
end

function methods:input(key)
    if not key then
        return nil, "Input key is required"
    end

    local inputs_map, err = self:inputs()
    if err then
        return nil, err
    end
    return inputs_map[key], nil
end

function methods:data(data_type, content, options)
    if not data_type or data_type == "" then
        return nil, "Data type is required"
    end
    if content == nil then
        return nil, "Content is required"
    end

    options = options or {}

    local content_type = options.content_type
    if not content_type then
        if type(content) == "table" then
            content_type = consts.CONTENT_TYPE.JSON
        else
            content_type = consts.CONTENT_TYPE.TEXT
        end
    end

    local data_id = options.data_id or uuid.v7()
    table.insert(self._created_data_ids, data_id)

    local command = {
        type = consts.COMMAND_TYPES.CREATE_DATA,
        payload = {
            data_id = data_id,
            data_type = data_type,
            key = options.key,
            content = content,
            content_type = content_type,
            discriminator = options.discriminator,
            node_id = options.node_id,
            metadata = options.metadata
        }
    }

    table.insert(self._queued_commands, command)
    return self, nil
end

function methods:update_metadata(updates)
    if not updates or type(updates) ~= "table" then
        return self, nil
    end

    self._metadata = merge_metadata(self._metadata, updates)

    local command = {
        type = consts.COMMAND_TYPES.UPDATE_NODE,
        payload = {
            node_id = self.node_id,
            metadata = self._metadata
        }
    }

    table.insert(self._queued_commands, command)
    return self, nil
end

function methods:update_config(updates)
    if not updates or type(updates) ~= "table" then
        return self, nil
    end

    self._config = merge_metadata(self._config, updates)

    local command = {
        type = consts.COMMAND_TYPES.UPDATE_NODE,
        payload = {
            node_id = self.node_id,
            config = self._config
        }
    }

    table.insert(self._queued_commands, command)
    return self, nil
end

function methods:submit()
    if #self._queued_commands == 0 then
        return true, nil
    end

    local op_id = uuid.v7()
    local success, err = self._deps.commit.submit(self.dataflow_id, op_id, self._queued_commands)

    if success then
        self._queued_commands = table.create(10, 0)
        return true, nil
    else
        return false, err
    end
end

function methods:yield(options)
    options = options or {}

    local yield_id = uuid.v7()
    local op_id = uuid.v7()

    local yield_command = {
        type = consts.COMMAND_TYPES.CREATE_DATA,
        payload = {
            data_id = uuid.v7(),
            data_type = consts.DATA_TYPE.NODE_YIELD,
            content = {
                node_id = self.node_id,
                yield_id = yield_id,
                reply_to = self._yield_reply_topic,
                yield_context = {
                    run_nodes = options.run_nodes or table.create(0, 0)
                }
            },
            content_type = consts.CONTENT_TYPE.JSON,
            key = yield_id,
            node_id = self.node_id
        }
    }
    table.insert(self._queued_commands, yield_command)

    local submitted, err = self._deps.commit.submit(self.dataflow_id, op_id, self._queued_commands)
    if not submitted then
        return nil, "Failed to submit yield: " .. (err or "unknown error")
    end
    self._queued_commands = table.create(10, 0)

    local yield_signal = {
        request_context = {
            yield_id = yield_id,
            node_id = self.node_id,
            reply_to = self._yield_reply_topic
        },
        yield_context = {
            run_nodes = options.run_nodes or table.create(0, 0)
        }
    }

    local success = self._deps.process.send(
        "dataflow." .. self.dataflow_id,
        consts.MESSAGE_TOPIC.YIELD_REQUEST,
        yield_signal
    )

    if not success then
        return nil, "Failed to send yield signal"
    end

    local received, ok = self._yield_channel:receive()
    if not ok then
        return nil, "Yield channel closed or error"
    end

    self._last_yield_id = yield_id

    if received and received.response_data then
        return received.response_data.run_node_results or table.create(0, 0), nil
    end

    return table.create(0, 0), nil
end

function methods:query()
    return self._deps.data_reader.with_dataflow(self.dataflow_id)
end

function methods:_route_outputs(content)
    local routed_data_ids = table.create(#self.data_targets, 0)
    local data_id_count = 0

    local env = {
        output = content
    }

    for _, target in ipairs(self.data_targets) do
        if target.condition then
            local should_create, condition_err = expr.eval(target.condition, env)
            if condition_err then
                return nil, "Output condition evaluation failed for target " .. (target.key or "unknown") .. ": " .. condition_err
            end
            if not should_create then
                goto continue
            end
        end

        local output_content = content
        if target.transform then
            local transformed, transform_err = expr.eval(target.transform, env)
            if transform_err then
                return nil, "Output transform failed for target " .. (target.key or "unknown") .. ": " .. transform_err
            end
            output_content = transformed
        end

        if is_dataflow_reference(output_content) then
            local ref_id = create_reference_data(self, target, output_content._dataflow_ref)
            data_id_count = data_id_count + 1
            routed_data_ids[data_id_count] = ref_id
        elseif type(output_content) == "table" and #output_content > 0 and is_dataflow_reference(output_content[1]) then
            for _, ref_item in ipairs(output_content) do
                if is_dataflow_reference(ref_item) then
                    local ref_id = create_reference_data(self, target, ref_item._dataflow_ref)
                    data_id_count = data_id_count + 1
                    routed_data_ids[data_id_count] = ref_id
                end
            end
        else
            local data_id = uuid.v7()
            data_id_count = data_id_count + 1
            routed_data_ids[data_id_count] = data_id

            local _, data_err = self:data(target.data_type, output_content, {
                data_id = data_id,
                key = target.key,
                discriminator = target.discriminator,
                node_id = target.node_id or self.node_id,
                content_type = target.content_type,
                metadata = target.metadata
            })
            if data_err then
                return nil, data_err
            end
        end

        ::continue::
    end

    return routed_data_ids, nil
end

function methods:_route_errors(error_content)
    local routed_data_ids = table.create(#self.error_targets, 0)
    local data_id_count = 0

    local env = {
        error = error_content
    }

    for _, target in ipairs(self.error_targets) do
        if target.condition then
            local should_create, condition_err = expr.eval(target.condition, env)
            if condition_err then
                goto continue
            end
            if not should_create then
                goto continue
            end
        end

        local error_output = error_content
        if target.transform then
            local transformed, transform_err = expr.eval(target.transform, env)
            if not transform_err then
                error_output = transformed
            end
        end

        local data_id = uuid.v7()
        data_id_count = data_id_count + 1
        routed_data_ids[data_id_count] = data_id

        self:data(target.data_type, error_output, {
            data_id = data_id,
            key = target.key,
            discriminator = target.discriminator,
            node_id = target.node_id,
            content_type = target.content_type,
            metadata = target.metadata
        })

        ::continue::
    end

    return routed_data_ids, nil
end

function methods:_submit_final()
    if #self._queued_commands == 0 then
        return true, nil
    end

    local result, err = self._deps.commit.submit(
        self.dataflow_id,
        uuid.v7(),
        self._queued_commands
    )

    self._queued_commands = table.create(10, 0)
    return result ~= nil, err
end

function methods:complete(output_content, message, extra_metadata)
    if extra_metadata then
        local _, meta_err = self:update_metadata(extra_metadata)
        if meta_err then
            return {
                success = false,
                message = "Failed to update metadata: " .. meta_err,
                error = meta_err,
                data_ids = table.create(0, 0)
            }
        end
    end

    if message then
        local _, msg_err = self:update_metadata({ status_message = message })
        if msg_err then
            return {
                success = false,
                message = "Failed to set status message: " .. msg_err,
                error = msg_err,
                data_ids = table.create(0, 0)
            }
        end
    end

    local data_ids = table.create(#self.data_targets, 0)
    if output_content ~= nil then
        local routed_ids, route_err = self:_route_outputs(output_content)
        if route_err then
            return {
                success = false,
                message = "Failed to route outputs: " .. route_err,
                error = route_err,
                data_ids = table.create(0, 0)
            }
        end
        data_ids = routed_ids
    end

    local success, err = self:_submit_final()
    if not success then
        return {
            success = false,
            message = "Failed to submit final commands: " .. (err or "unknown error"),
            error = err,
            data_ids = table.create(0, 0)
        }
    end

    return {
        success = true,
        message = message or "Node execution completed successfully",
        data_ids = data_ids
    }
end

function methods:fail(error_details, message, extra_metadata)
    local error_msg = error_details or "Unknown error"
    local status_msg = message or error_msg

    local error_metadata = {
        status_message = status_msg,
        error = error_msg
    }

    if extra_metadata then
        error_metadata = merge_metadata(error_metadata, extra_metadata)
    end

    self:update_metadata(error_metadata)

    local data_ids = table.create(#self.error_targets, 0)
    if error_details ~= nil then
        local routed_ids, route_err = self:_route_errors(error_details)
        if not route_err then
            data_ids = routed_ids
        end
    end

    local success, err = self:_submit_final()
    if not success then
        return {
            success = false,
            message = "Failed to submit final commands: " .. (err or "unknown error"),
            error = err,
            data_ids = table.create(0, 0)
        }
    end

    return {
        success = false,
        message = status_msg,
        error = error_msg,
        data_ids = data_ids
    }
end

function methods:command(cmd)
    if not cmd or not cmd.type then
        return nil, "Command must have a type"
    end

    table.insert(self._queued_commands, cmd)
    return self, nil
end

function methods:created_data_ids()
    return self._created_data_ids
end

return node