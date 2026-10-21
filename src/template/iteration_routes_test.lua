local test = require("test")
local uuid = require("uuid")
local json = require("json")
local client = require("client")
local consts = require("consts")
local data_reader = require("data_reader")

local function decode(content)
    if type(content) == "string" then
        local value, err = json.decode(content)
        test.is_nil(err)
        return value
    end
    return content
end

local function execute(kind, route_transform, continue_condition, expected_count)
    local loop_id, root_id, join_id = uuid.v7(), uuid.v7(), uuid.v7()
    local config = {
        iteration_input_routes = {
            { node_id = root_id, discriminator = "default" },
            { node_id = join_id, discriminator = "item", transform = route_transform },
        },
        data_targets = { { data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "result" } },
    }
    local input
    if kind == "parallel" then
        config.source_array_key = "items"
        config.unwrap = true
        config.filter = "successes"
        input = { items = { 3, 7 } }
    else
        config.max_iterations = 3
        config.continue_condition = continue_condition or "true"
        input = { value = 3 }
    end
    local commands = {
        { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
            node_id = loop_id, node_type = "userspace.dataflow.node." .. kind .. ":" .. kind,
            status = consts.STATUS.PENDING, config = config,
        } },
        { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
            node_id = root_id, node_type = "userspace.dataflow.node.state:state",
            parent_node_id = loop_id, status = consts.STATUS.TEMPLATE,
            config = { data_targets = { {
                node_id = join_id, data_type = consts.DATA_TYPE.NODE_INPUT,
                discriminator = "derived", transform = kind == "parallel" and "output * 2" or "output.iteration * 2",
            } } },
        } },
        { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
            node_id = join_id, node_type = "userspace.dataflow.node.state:state",
            parent_node_id = loop_id, status = consts.STATUS.TEMPLATE,
            config = { inputs = { required = { "item", "derived" } }, data_targets = { {
                data_type = consts.DATA_TYPE.NODE_OUTPUT,
            } } },
        } },
        { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
            data_id = uuid.v7(), data_type = consts.DATA_TYPE.NODE_INPUT,
            node_id = loop_id, discriminator = "default", content = input,
            content_type = consts.CONTENT_TYPE.JSON,
        } },
    }
    local c, client_err = client.new()
    test.is_nil(client_err)
    local id, create_err = c:create_workflow(commands)
    test.is_nil(create_err)
    local result, err = c:execute(id)
    if kind == "cycle" and continue_condition == nil then
        test.not_nil(err)
        test.is_false(result.success)
        test.contains(result.error, "Maximum iterations (3) exceeded")
    else
        test.is_nil(err)
        test.is_true(result.success)
    end
    local reader, reader_err = data_reader.with_dataflow(id)
    test.is_nil(reader_err)
    local output, output_err = reader:with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
        :with_data_keys("result"):one()
    test.is_nil(output_err)
    if kind == "parallel" or continue_condition ~= nil then test.not_nil(output) end
    local rows, rows_err = reader:with_data_types(consts.DATA_TYPE.NODE_INPUT):all()
    test.is_nil(rows_err)
    local items, derived = {}, {}
    for _, row in ipairs(rows) do
        if row.discriminator == "item" then
            test.is_nil(items[row.node_id])
            items[row.node_id] = decode(row.content)
        elseif row.discriminator == "derived" then
            test.is_nil(derived[row.node_id])
            derived[row.node_id] = decode(row.content)
        end
    end
    local count = 0
    for node_id, item in pairs(items) do
        count = count + 1
        test.eq(derived[node_id], item * 2)
    end
    test.eq(count, expected_count or (kind == "parallel" and 2 or 3))
    if kind == "cycle" and continue_condition == nil then return count end
    return decode(output.content)
end

local function define_tests()
    describe("Iteration input routes", function()
        it("routes each item to a join once alongside a body edge", function()
            local output = execute("parallel", "output")
            test.eq(#output, 2)
            test.eq(output[1].item, 3)
            test.eq(output[1].derived, 6)
            test.eq(output[2].item, 7)
            test.eq(output[2].derived, 14)
        end)

        it("returns a plain join object when a routed Cycle stops", function()
            local output = execute("cycle", "output.iteration", "false", 1)
            test.eq(output.item, 1)
            test.eq(output.derived, 2)
        end)

        it("terminates a routed Cycle at its iteration limit", function()
            local output = execute("cycle", "output.iteration")
            test.eq(output, 3)
        end)
    end)
end

return test.run_cases(define_tests)
