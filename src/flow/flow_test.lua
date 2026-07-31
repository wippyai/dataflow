local test = require("test")
local flow = require("flow")
local compiler = require("compiler")

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
    end)
end

return test.run_cases(define_tests)
