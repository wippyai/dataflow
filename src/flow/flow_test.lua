local test = require("test")
local flow = require("flow")
local compiler = require("compiler")
local consts = require("consts")

local function agent_node(graph: any, parent_required: boolean): any
    for _, node in pairs(graph.nodes) do
        if node.node_type == "userspace.dataflow.node.agent:node"
            and ((node.parent_node_id ~= nil) == parent_required) then
            return node
        end
    end
    return nil
end

local function define_tests()
    describe("flow builder", function()
        for _, placement in ipairs({"root", "parallel", "cycle"}) do
            it("inlines reusable templates without retry fields in " .. placement, function()
                local template = (flow.template() :: any):func("example:first", {context = {value = 7}})
                    :func("example:second", {context = {label = "reused"}})
                local nested = (flow.template() :: any):use(template)
                local builder = (flow.create() :: any):with_input({items = {1}})
                if placement == "root" then builder:use(nested)
                elseif placement == "parallel" then builder:parallel({source_array_key = "items", template = nested})
                else builder:cycle({template = nested, max_iterations = 2}) end
                local result, err = compiler.compile(builder.operations, {})
                test.is_nil(err)
                local functions = {}
                for _, node in pairs(result.graph.nodes) do
                    if node.node_type == "userspace.dataflow.node.func:node" then
                        functions[node.config.func_id] = node
                        test.eq(node.parent_node_id ~= nil, placement ~= "root")
                    end
                end
                local first = test.not_nil(functions["example:first"])
                local second = test.not_nil(functions["example:second"])
                test.eq(first.config.context.value, 7)
                test.eq(second.config.context.label, "reused")
                local linked = false
                if placement == "root" then
                    for _, target in ipairs(result.graph.edges[first.node_id].targets) do
                        if target.target_node_id == second.node_id then linked = true end
                    end
                else
                    for _, target in ipairs(first.config.data_targets) do
                        if target.node_id == second.node_id then linked = true end
                    end
                end
                test.is_true(linked)
            end)
        end

        it("preserves func retry declarations through the builder and nested templates", function()
            local config = { retry_policy = { maximum_attempts = 3 },
                start_to_close_timeout = "30s", schedule_to_close_timeout = "6h" }
            local template = (flow.template() :: any):func("example:call", config)
            for _, builder in ipairs({
                (flow.create() :: any):with_input({}):func("example:call", config),
                (flow.create() :: any):with_input({items = {1}}):parallel({source_array_key = "items", template = template}),
                (flow.create() :: any):with_input({}):cycle({template = template, max_iterations = 2}),
            }) do
                local result, err = compiler.compile(builder.operations, {})
                test.is_nil(err)
                local found = false
                for _, node in pairs(result.graph.nodes) do
                    if node.node_type == "userspace.dataflow.node.func:node" then
                        found = true
                        test.eq(node.config.start_to_close_timeout, "30s")
                        test.eq(node.config.schedule_to_close_timeout, "6h")
                        test.eq(node.config.retry_policy.maximum_attempts, 3)
                    end
                end
                test.is_true(found)
            end
        end)

        it("inlines reusable templates with the same func retry declarations at every depth", function()
            local declaration = {retry_policy = {maximum_attempts = 3}, start_to_close_timeout = "30s"}
            local template = (flow.template() :: any):func("example:call", declaration)
            local nested = (flow.template() :: any):use(template)
            for _, builder in ipairs({
                (flow.create() :: any):with_input({}):use(template),
                (flow.create() :: any):with_input({items = {1}}):parallel({source_array_key = "items", template = nested}),
            }) do
                local result, err = compiler.compile(builder.operations, {})
                test.is_nil(err)
                local found = false
                for _, node in pairs(result.graph.nodes) do
                    if node.node_type == "userspace.dataflow.node.func:node" then
                        found = true
                        test.eq(node.config.start_to_close_timeout, "30s")
                        test.eq(node.config.retry_policy.maximum_attempts, 3)
                    end
                end
                test.is_true(found)
            end
        end)

        it("rejects retry declarations before non-func builders discard them", function()
            for _, method in ipairs({"agent", "cycle", "parallel", "signal", "join"}) do
                for _, field in ipairs({"retry_policy", "start_to_close_timeout", "schedule_to_close_timeout"}) do
                    local builder = flow.create() :: any
                    local config = {[field] = field == "retry_policy" and {} or "1s"}
                    local ok, err = pcall(function()
                        if method == "agent" then builder:agent("example.agent", config)
                        else builder[method](builder, config) end
                    end)
                    test.is_false(ok)
                    test.contains(tostring(err), "only on func")
                end
            end
        end)

        it("rejects retry declarations on other node kinds including nested operations", function()
            for _, kind in ipairs({ "agent", "cycle", "parallel", "signal", "state" }) do
                for _, field in ipairs({ "retry_policy", "start_to_close_timeout", "schedule_to_close_timeout" }) do
                    local config: any = { func_id = "example:call", agent_id = "example.agent" }
                    config[field] = field == "retry_policy" and {} or "1s"
                    local _, err = compiler.compile({{type = kind, config = config}}, {})
                    test.not_nil(err)
                    test.contains(err, "func")
                    _, err = compiler.compile({{type = "cycle", config = { template = {
                        operations = {{type = kind, config = config}} }}}}, {})
                    test.not_nil(err)
                    test.contains(err, "func")
                end
            end
        end)

        it("preserves explicit agent capability overlays", function()
            local builder = (flow.create() :: any):agent("example.agent", {
                active_traits = {},
                active_tools = { "example.tools:search" },
            })
            local operation = builder.operations[1]

            test.eq(type(operation.config.active_traits), "table")
            test.eq(#operation.config.active_traits, 0)
            test.eq(operation.config.active_tools[1], "example.tools:search")
        end)

        it("compiles capability overlays into a root agent node", function()
            local builder = (flow.create() :: any)
                :with_input({ subject = "example" })
                :agent("example.agent", {
                    active_traits = {},
                    active_tools = { "example.tools:search" },
                })
            local result, err = compiler.compile(builder.operations, {})

            test.is_nil(err)
            local node = test.not_nil(agent_node(result.graph, false))
            test.eq(type(node.config.active_traits), "table")
            test.eq(#node.config.active_traits, 0)
            test.eq(node.config.active_tools[1], "example.tools:search")
        end)

        it("compiles capability overlays into a nested agent template", function()
            local template = (flow.template() :: any):agent("example.agent", {
                active_traits = {},
                active_tools = { "example.tools:search" },
            })
            local builder = (flow.create() :: any)
                :with_input({ items = { "example" } })
                :parallel({
                    source_array_key = "items",
                    template = template,
                })
            local result, err = compiler.compile(builder.operations, {})

            test.is_nil(err)
            local node = test.not_nil(agent_node(result.graph, true))
            test.eq(type(node.config.active_traits), "table")
            test.eq(#node.config.active_traits, 0)
            test.eq(node.config.active_tools[1], "example.tools:search")
        end)

        it("compiles the unproductive-step bound into the agent arena", function()
            local builder = (flow.create() :: any)
                :with_input({ subject = "example" })
                :agent("example.agent", {
                    arena = { prompt = "Work.", max_unproductive_steps = 5 },
                })
            local result, err = compiler.compile(builder.operations, {})

            test.is_nil(err)
            local node = test.not_nil(agent_node(result.graph, false))
            test.eq(node.config.arena.max_unproductive_steps, 5)
        end)

        it("leaves the unproductive-step bound to the agent node default when unset", function()
            local builder = (flow.create() :: any)
                :with_input({ subject = "example" })
                :agent("example.agent", { arena = { prompt = "Work." } })
            local result, err = compiler.compile(builder.operations, {})

            test.is_nil(err)
            local node = test.not_nil(agent_node(result.graph, false))
            test.is_nil(node.config.arena.max_unproductive_steps)
        end)

        it("leaves tool_calling to the agent node default when unset", function()
            local builder = (flow.create() :: any)
                :with_input({ subject = "example" })
                :agent("example.agent", { arena = { prompt = "Work." } })
            local result, err = compiler.compile(builder.operations, {})

            test.is_nil(err)
            local node = test.not_nil(agent_node(result.graph, false))
            test.is_nil(node.config.arena.tool_calling)
        end)

        it("compiles an explicit tool_calling mode into the agent arena", function()
            local builder = (flow.create() :: any)
                :with_input({ subject = "example" })
                :agent("example.agent", { arena = { prompt = "Work.", tool_calling = "none" } })
            local result, err = compiler.compile(builder.operations, {})

            test.is_nil(err)
            local node = test.not_nil(agent_node(result.graph, false))
            test.eq(node.config.arena.tool_calling, "none")
        end)

        it("compiles rolling parallel scheduling", function()
            local template = (flow.template() :: any):func("example.process")
            local builder = (flow.create() :: any)
                :with_input({ items = { "a", "b" } })
                :parallel({
                    source_array_key = "items",
                    batch_size = 2,
                    scheduling = "rolling",
                    template = template,
                })
            local result, err = compiler.compile(builder.operations, {})

            test.is_nil(err)
            local parallel_node = nil
            for _, node in pairs(result.graph.nodes) do
                if node.node_type == "userspace.dataflow.node.parallel:parallel" then
                    parallel_node = node
                    break
                end
            end
            parallel_node = test.not_nil(parallel_node)
            test.eq(parallel_node.config.batch_size, 2)
            test.eq(parallel_node.config.scheduling, "rolling")
        end)

        it("records workflow input as system-authored at compilation", function()
            local builder = (flow.create() :: any):with_input("Compile this workflow")
                :func("example.process")
            local result, err = compiler.compile(builder.operations, {})
            test.is_nil(err)

            local input_command = nil
            for _, command in ipairs(result.commands) do
                if command.payload.data_type == consts.DATA_TYPE.WORKFLOW_INPUT then
                    input_command = command
                    break
                end
            end
            input_command = test.not_nil(input_command)
            test.eq(input_command.payload.metadata.author.kind, "system")
        end)
    end)
end

return test.run_cases(define_tests)
