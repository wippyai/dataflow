local test = require("test")
local scheduler = require("scheduler")
local consts = require("consts")

local function define_tests()
    describe("Dataflow Scheduler", function()
        describe("Empty State Handling", function()
            it("should complete empty workflow", function()
                local state = scheduler.create_empty_state()

                local decision = scheduler.find_next_work(state)

                test.eq(decision.type, scheduler.DECISION_TYPE.COMPLETE_WORKFLOW)
                test.is_true(decision.payload.success)
                test.contains(decision.payload.message, "Empty workflow")
            end)

            it("should return no work when only completed nodes exist", function()
                local state = scheduler.create_empty_state()
                state.nodes = {
                    ["node-1"] = {
                        status = consts.STATUS.COMPLETED_SUCCESS,
                        type = "test_node",
                    },
                }
                state.has_workflow_output = true

                local decision = scheduler.find_next_work(state)

                test.eq(decision.type, scheduler.DECISION_TYPE.COMPLETE_WORKFLOW)
                test.is_true(decision.payload.success)
            end)
        end)

        describe("Root Node Execution", function()
            it("should execute root node with inputs", function()
                local state = scheduler.create_empty_state()
                state.nodes = {
                    ["root-1"] = {
                        status = consts.STATUS.PENDING,
                        type = "root_processor",
                        parent_node_id = nil,
                    },
                }
                state.input_tracker = {
                    requirements = {},
                    available = {
                        ["root-1"] = { config = true },
                    },
                }

                local decision = scheduler.find_next_work(state)

                test.eq(decision.type, scheduler.DECISION_TYPE.EXECUTE_NODES)
                test.eq(#decision.payload.nodes, 1)
                test.eq(decision.payload.nodes[1].node_id, "root-1")
                test.eq(decision.payload.nodes[1].trigger_reason, "root_ready")
                test.eq(#decision.payload.nodes[1].path, 0)
            end)

            it("should not execute root node without required inputs", function()
                local state = scheduler.create_empty_state()
                state.nodes = {
                    ["root-1"] = {
                        status = consts.STATUS.PENDING,
                        type = "root_processor",
                        parent_node_id = nil,
                    },
                }
                state.input_tracker = {
                    requirements = {
                        ["root-1"] = { required = { "config", "data" }, optional = {} },
                    },
                    available = {
                        ["root-1"] = { config = true },
                    },
                }

                local decision = scheduler.find_next_work(state)

                test.eq(decision.type, scheduler.DECISION_TYPE.COMPLETE_WORKFLOW)
                test.is_false(decision.payload.success)
                test.contains(decision.payload.message, "deadlocked")
            end)

            it("should execute root node even when other nodes are running", function()
                local state = scheduler.create_empty_state()
                state.nodes = {
                    ["root-1"] = {
                        status = consts.STATUS.PENDING,
                        type = "root_processor",
                        parent_node_id = nil,
                    },
                }
                state.input_tracker = {
                    requirements = {},
                    available = {
                        ["root-1"] = { config = true },
                    },
                }
                state.active_processes = {
                    ["other-node"] = true,
                }

                local decision = scheduler.find_next_work(state)

                test.eq(decision.type, scheduler.DECISION_TYPE.EXECUTE_NODES)
                test.eq(#decision.payload.nodes, 1)
                test.eq(decision.payload.nodes[1].node_id, "root-1")
                test.eq(decision.payload.nodes[1].trigger_reason, "root_ready")
            end)
        end)

        describe("Yield-Driven Execution", function()
            it("should satisfy completed yield", function()
                local state = scheduler.create_empty_state()
                state.active_yields = {
                    ["parent-1"] = {
                        yield_id = "yield-123",
                        reply_to = "dataflow.yield_reply.parent-1",
                        pending_children = {
                            ["child-1"] = consts.STATUS.COMPLETED_SUCCESS,
                            ["child-2"] = consts.STATUS.COMPLETED_SUCCESS,
                        },
                        results = {
                            ["child-1"] = "result-data-1",
                            ["child-2"] = "result-data-2",
                        },
                    },
                }

                local decision = scheduler.find_next_work(state)

                test.eq(decision.type, scheduler.DECISION_TYPE.SATISFY_YIELD)
                test.eq(decision.payload.parent_id, "parent-1")
                test.eq(decision.payload.yield_id, "yield-123")
                test.eq(decision.payload.reply_to, "dataflow.yield_reply.parent-1")
            end)

            it("should execute yield child with inputs", function()
                local state = scheduler.create_empty_state()
                state.nodes = {
                    ["child-1"] = {
                        status = consts.STATUS.PENDING,
                        type = "child_processor",
                        parent_node_id = "parent-1",
                    },
                }
                state.active_yields = {
                    ["parent-1"] = {
                        yield_id = "yield-123",
                        reply_to = "dataflow.yield_reply.parent-1",
                        pending_children = {
                            ["child-1"] = consts.STATUS.PENDING,
                            ["child-2"] = consts.STATUS.COMPLETED_SUCCESS,
                        },
                        child_path = { "ancestor-1", "parent-1" },
                    },
                }
                state.input_tracker = {
                    requirements = {
                        ["child-1"] = { required = { "input" }, optional = {} },
                    },
                    available = {
                        ["child-1"] = { input = true },
                    },
                }

                local decision = scheduler.find_next_work(state)

                test.eq(decision.type, scheduler.DECISION_TYPE.EXECUTE_NODES)
                test.eq(#decision.payload.nodes, 1)
                test.eq(decision.payload.nodes[1].node_id, "child-1")
                test.eq(decision.payload.nodes[1].trigger_reason, "yield_driven")
                test.eq((decision.payload.nodes[1] :: any).parent_id, "parent-1")
                test.eq(#decision.payload.nodes[1].path, 2)
            end)

            it("should not execute yield child without inputs", function()
                local state = scheduler.create_empty_state()
                state.nodes = {
                    ["child-1"] = {
                        status = consts.STATUS.PENDING,
                        type = "child_processor",
                        parent_node_id = "parent-1",
                    },
                }
                state.active_yields = {
                    ["parent-1"] = {
                        yield_id = "yield-123",
                        pending_children = {
                            ["child-1"] = consts.STATUS.PENDING,
                        },
                    },
                }
                state.input_tracker = {
                    requirements = {
                        ["child-1"] = { required = { "input" }, optional = {} },
                    },
                    available = {
                        ["child-1"] = {},
                    },
                }

                local decision = scheduler.find_next_work(state)

                test.eq(decision.type, scheduler.DECISION_TYPE.NO_WORK)
            end)
        end)

        describe("Input-Ready Execution", function()
            it("should execute non-root node with satisfied inputs", function()
                local state = scheduler.create_empty_state()
                state.nodes = {
                    ["node-1"] = {
                        status = consts.STATUS.PENDING,
                        type = "processor",
                        parent_node_id = "some-parent",
                    },
                }
                state.input_tracker = {
                    requirements = {
                        ["node-1"] = { required = { "data", "config" }, optional = { "metadata" } },
                    },
                    available = {
                        ["node-1"] = { data = true, config = true, metadata = true },
                    },
                }

                local decision = scheduler.find_next_work(state)

                test.eq(decision.type, scheduler.DECISION_TYPE.EXECUTE_NODES)
                test.eq(#decision.payload.nodes, 1)
                test.eq(decision.payload.nodes[1].node_id, "node-1")
                test.eq(decision.payload.nodes[1].trigger_reason, "input_ready")
            end)

            it("should deadlock when node has partial required inputs", function()
                local state = scheduler.create_empty_state()
                state.nodes = {
                    ["node-1"] = {
                        status = consts.STATUS.PENDING,
                        type = "processor",
                        parent_node_id = "some-parent",
                    },
                }
                state.input_tracker = {
                    requirements = {
                        ["node-1"] = { required = { "data", "config" }, optional = {} },
                    },
                    available = {
                        ["node-1"] = { data = true },
                    },
                }

                local decision = scheduler.find_next_work(state)

                test.eq(decision.type, scheduler.DECISION_TYPE.COMPLETE_WORKFLOW)
                test.is_false(decision.payload.success)
                test.contains(decision.payload.message, "deadlocked")
            end)

            it("should skip yield children in input-ready search", function()
                local state = scheduler.create_empty_state()
                state.nodes = {
                    ["yield-child"] = {
                        status = consts.STATUS.PENDING,
                        type = "processor",
                        parent_node_id = "parent-1",
                    },
                    ["normal-node"] = {
                        status = consts.STATUS.PENDING,
                        type = "processor",
                        parent_node_id = "other-parent",
                    },
                }
                state.active_yields = {
                    ["parent-1"] = {
                        pending_children = {
                            ["yield-child"] = consts.STATUS.PENDING,
                        },
                    },
                }
                state.input_tracker = {
                    requirements = {
                        ["yield-child"] = { required = { "input" }, optional = {} },
                        ["normal-node"] = { required = { "input" }, optional = {} },
                    },
                    available = {
                        ["yield-child"] = { input = true },
                        ["normal-node"] = { input = true },
                    },
                }

                local decision = scheduler.find_next_work(state)

                test.eq(decision.type, scheduler.DECISION_TYPE.EXECUTE_NODES)
                test.eq(#decision.payload.nodes, 1)
                test.eq(decision.payload.nodes[1].node_id, "yield-child")
                test.eq(decision.payload.nodes[1].trigger_reason, "yield_driven")
            end)
        end)

        describe("Priority Ordering", function()
            it("should prioritize yield satisfaction over new execution", function()
                local state = scheduler.create_empty_state()
                state.nodes = {
                    ["ready-node"] = {
                        status = consts.STATUS.PENDING,
                        type = "processor",
                    },
                }
                state.active_yields = {
                    ["parent-1"] = {
                        yield_id = "yield-123",
                        reply_to = "reply-topic",
                        pending_children = {
                            ["child-1"] = consts.STATUS.COMPLETED_SUCCESS,
                        },
                        results = { ["child-1"] = "result" },
                    },
                }
                state.input_tracker = {
                    requirements = {
                        ["ready-node"] = { required = { "input" }, optional = {} },
                    },
                    available = {
                        ["ready-node"] = { input = true },
                    },
                }

                local decision = scheduler.find_next_work(state)

                test.eq(decision.type, scheduler.DECISION_TYPE.SATISFY_YIELD)
                test.eq(decision.payload.parent_id, "parent-1")
            end)

            it("should prioritize yield children over input-ready nodes", function()
                local state = scheduler.create_empty_state()
                state.nodes = {
                    ["yield-child"] = {
                        status = consts.STATUS.PENDING,
                        type = "child_processor",
                        parent_node_id = "parent-1",
                    },
                    ["normal-node"] = {
                        status = consts.STATUS.PENDING,
                        type = "processor",
                    },
                }
                state.active_yields = {
                    ["parent-1"] = {
                        pending_children = {
                            ["yield-child"] = consts.STATUS.PENDING,
                        },
                    },
                }
                state.input_tracker = {
                    requirements = {
                        ["yield-child"] = { required = { "input" }, optional = {} },
                        ["normal-node"] = { required = { "input" }, optional = {} },
                    },
                    available = {
                        ["yield-child"] = { input = true },
                        ["normal-node"] = { input = true },
                    },
                }

                local decision = scheduler.find_next_work(state)

                test.eq(decision.type, scheduler.DECISION_TYPE.EXECUTE_NODES)
                test.eq(#decision.payload.nodes, 1)
                test.eq(decision.payload.nodes[1].node_id, "yield-child")
                test.eq(decision.payload.nodes[1].trigger_reason, "yield_driven")
            end)
        end)

        describe("Completion Detection", function()
            it("should complete workflow successfully with all nodes complete", function()
                local state = scheduler.create_empty_state()
                state.nodes = {
                    ["node-1"] = {
                        status = consts.STATUS.COMPLETED_SUCCESS,
                        type = "processor",
                    },
                    ["node-2"] = {
                        status = consts.STATUS.COMPLETED_SUCCESS,
                        type = "processor",
                    },
                }
                state.has_workflow_output = true

                local decision = scheduler.find_next_work(state)

                test.eq(decision.type, scheduler.DECISION_TYPE.COMPLETE_WORKFLOW)
                test.is_true(decision.payload.success)
                test.contains(decision.payload.message, "successfully")
            end)

            it("should complete workflow with failure when nodes failed", function()
                local state = scheduler.create_empty_state()
                state.nodes = {
                    ["node-1"] = {
                        status = consts.STATUS.COMPLETED_SUCCESS,
                        type = "processor",
                    },
                    ["node-2"] = {
                        status = consts.STATUS.COMPLETED_FAILURE,
                        type = "processor",
                    },
                }

                local decision = scheduler.find_next_work(state)

                test.eq(decision.type, scheduler.DECISION_TYPE.COMPLETE_WORKFLOW)
                test.is_false(decision.payload.success)
                test.contains(decision.payload.message, "without producing output")
            end)

            it("should detect deadlock with pending nodes but no inputs", function()
                local state = scheduler.create_empty_state()
                state.nodes = {
                    ["node-1"] = {
                        status = consts.STATUS.PENDING,
                        type = "processor",
                        parent_node_id = nil,
                    },
                }
                state.input_tracker = {
                    requirements = {
                        ["node-1"] = { required = { "missing-input" }, optional = {} },
                    },
                    available = {
                        ["node-1"] = {},
                    },
                }

                local decision = scheduler.find_next_work(state)

                test.eq(decision.type, scheduler.DECISION_TYPE.COMPLETE_WORKFLOW)
                test.is_false(decision.payload.success)
                test.contains(decision.payload.message, "deadlocked")
            end)

            it("should not complete while processes are active", function()
                local state = scheduler.create_empty_state()
                state.nodes = {
                    ["node-1"] = {
                        status = consts.STATUS.RUNNING,
                        type = "processor",
                    },
                }
                state.active_processes = {
                    ["node-1"] = true,
                }

                local decision = scheduler.find_next_work(state)

                test.eq(decision.type, scheduler.DECISION_TYPE.NO_WORK)
            end)

            it("should not complete while yields are active", function()
                local state = scheduler.create_empty_state()
                state.nodes = {
                    ["node-1"] = {
                        status = consts.STATUS.COMPLETED_SUCCESS,
                        type = "processor",
                    },
                }
                state.active_yields = {
                    ["parent-1"] = {
                        pending_children = {
                            ["child-1"] = consts.STATUS.PENDING,
                        },
                    },
                }

                local decision = scheduler.find_next_work(state)

                test.eq(decision.type, scheduler.DECISION_TYPE.NO_WORK)
            end)
        end)

        describe("Input Requirements Behavior", function()
            it("should execute node with inputs but no requirements", function()
                local state = scheduler.create_empty_state()
                state.nodes = {
                    ["no-reqs-node"] = {
                        status = consts.STATUS.PENDING,
                        type = "processor",
                        parent_node_id = nil,
                    },
                }
                state.input_tracker = {
                    requirements = {},
                    available = {
                        ["no-reqs-node"] = {
                            config = true,
                            data = true,
                        },
                    },
                }

                local decision = scheduler.find_next_work(state)

                test.eq(decision.type, scheduler.DECISION_TYPE.EXECUTE_NODES)
                test.eq(#decision.payload.nodes, 1)
                test.eq(decision.payload.nodes[1].node_id, "no-reqs-node")
                test.eq(decision.payload.nodes[1].trigger_reason, "root_ready")
            end)

            it("should immediately fail for nodes with no requirements and no inputs", function()
                local state = scheduler.create_empty_state()
                state.nodes = {
                    ["no-reqs-no-inputs"] = {
                        status = consts.STATUS.PENDING,
                        type = "processor",
                        parent_node_id = nil,
                    },
                }
                state.input_tracker = {
                    requirements = {},
                    available = {
                        ["no-reqs-no-inputs"] = {},
                    },
                }

                local decision = scheduler.find_next_work(state)

                test.eq(decision.type, scheduler.DECISION_TYPE.COMPLETE_WORKFLOW)
                test.is_false(decision.payload.success)
                test.contains(decision.payload.message, "No input data provided")
            end)

            it("should execute yield child with inputs but no requirements", function()
                local state = scheduler.create_empty_state()
                state.nodes = {
                    ["yield-child-no-reqs"] = {
                        status = consts.STATUS.PENDING,
                        type = "child_processor",
                        parent_node_id = "parent-1",
                    },
                }
                state.active_yields = {
                    ["parent-1"] = {
                        yield_id = "yield-123",
                        pending_children = {
                            ["yield-child-no-reqs"] = consts.STATUS.PENDING,
                        },
                    },
                }
                state.input_tracker = {
                    requirements = {},
                    available = {
                        ["yield-child-no-reqs"] = {
                            input_data = true,
                        },
                    },
                }

                local decision = scheduler.find_next_work(state)

                test.eq(decision.type, scheduler.DECISION_TYPE.EXECUTE_NODES)
                test.eq(#decision.payload.nodes, 1)
                test.eq(decision.payload.nodes[1].node_id, "yield-child-no-reqs")
                test.eq(decision.payload.nodes[1].trigger_reason, "yield_driven")
            end)

            it("should execute root node with inputs but no requirements", function()
                local state = scheduler.create_empty_state()
                state.nodes = {
                    ["root-no-reqs"] = {
                        status = consts.STATUS.PENDING,
                        type = "processor",
                        parent_node_id = nil,
                    },
                }
                state.input_tracker = {
                    requirements = {},
                    available = {
                        ["root-no-reqs"] = {
                            some_data = true,
                            config = true,
                        },
                    },
                }

                local decision = scheduler.find_next_work(state)

                test.eq(decision.type, scheduler.DECISION_TYPE.EXECUTE_NODES)
                test.eq(#decision.payload.nodes, 1)
                test.eq(decision.payload.nodes[1].node_id, "root-no-reqs")
                test.eq(decision.payload.nodes[1].trigger_reason, "root_ready")
            end)

            it("should still deadlock when required inputs are missing", function()
                local state = scheduler.create_empty_state()
                state.nodes = {
                    ["missing-required"] = {
                        status = consts.STATUS.PENDING,
                        type = "processor",
                        parent_node_id = nil,
                    },
                }
                state.input_tracker = {
                    requirements = {
                        ["missing-required"] = {
                            required = { "essential-input" },
                            optional = {},
                        },
                    },
                    available = {
                        ["missing-required"] = {
                            optional_data = true,
                        },
                    },
                }

                local decision = scheduler.find_next_work(state)

                test.eq(decision.type, scheduler.DECISION_TYPE.COMPLETE_WORKFLOW)
                test.is_false(decision.payload.success)
                test.contains(decision.payload.message, "deadlocked")
            end)
        end)

        describe("Flexible Input Handling", function()
            it("should execute node with input data but no requirements", function()
                local state = scheduler.create_empty_state()
                state.nodes = {
                    ["no-reqs-with-data"] = {
                        status = consts.STATUS.PENDING,
                        type = "processor",
                        parent_node_id = nil,
                    },
                }
                state.input_tracker = {
                    requirements = {},
                    available = {
                        ["no-reqs-with-data"] = {
                            config = true,
                            data = true,
                        },
                    },
                }

                local decision = scheduler.find_next_work(state)

                test.eq(decision.type, scheduler.DECISION_TYPE.EXECUTE_NODES)
                test.eq(#decision.payload.nodes, 1)
                test.eq(decision.payload.nodes[1].node_id, "no-reqs-with-data")
                test.eq(decision.payload.nodes[1].trigger_reason, "root_ready")
            end)

            it("should execute multiple nodes without requirements when they have inputs", function()
                local state = scheduler.create_empty_state()
                state.nodes = {
                    ["flexible-node-1"] = {
                        status = consts.STATUS.PENDING,
                        type = "processor",
                        parent_node_id = nil,
                    },
                    ["flexible-node-2"] = {
                        status = consts.STATUS.PENDING,
                        type = "processor",
                        parent_node_id = nil,
                    },
                    ["good-node"] = {
                        status = consts.STATUS.PENDING,
                        type = "processor",
                        parent_node_id = nil,
                    },
                }
                state.input_tracker = {
                    requirements = {
                        ["good-node"] = { required = { "input" }, optional = {} },
                    },
                    available = {
                        ["flexible-node-1"] = { data = true },
                        ["flexible-node-2"] = { config = true },
                        ["good-node"] = {},
                    },
                }

                local decision = scheduler.find_next_work(state)

                test.eq(decision.type, scheduler.DECISION_TYPE.EXECUTE_NODES)
                test.eq(#decision.payload.nodes, 2)

                local executed_nodes: { [string]: boolean } = {}
                for _, node in ipairs(decision.payload.nodes) do
                    executed_nodes[node.node_id] = true
                    test.eq(node.trigger_reason, "root_ready")
                end

                test.is_true(executed_nodes["flexible-node-1"])
                test.is_true(executed_nodes["flexible-node-2"])
                test.is_nil(executed_nodes["good-node"])
            end)

            it("should immediately fail nodes without input data", function()
                local state = scheduler.create_empty_state()
                state.nodes = {
                    ["no-inputs-node"] = {
                        status = consts.STATUS.PENDING,
                        type = "processor",
                    },
                }
                state.input_tracker = {
                    requirements = {},
                    available = {
                        ["no-inputs-node"] = {},
                    },
                }

                local decision = scheduler.find_next_work(state)

                test.eq(decision.type, scheduler.DECISION_TYPE.COMPLETE_WORKFLOW)
                test.is_false(decision.payload.success)
                test.contains(decision.payload.message, "No input data provided")
            end)

            it("should execute flexible nodes over deadlocked nodes", function()
                local state = scheduler.create_empty_state()
                state.nodes = {
                    ["flexible-node"] = {
                        status = consts.STATUS.PENDING,
                        type = "processor",
                        parent_node_id = nil,
                    },
                    ["deadlocked"] = {
                        status = consts.STATUS.PENDING,
                        type = "processor",
                        parent_node_id = nil,
                    },
                }
                state.input_tracker = {
                    requirements = {
                        ["deadlocked"] = { required = { "missing-input" }, optional = {} },
                    },
                    available = {
                        ["flexible-node"] = { data = true },
                        ["deadlocked"] = {},
                    },
                }

                local decision = scheduler.find_next_work(state)

                test.eq(decision.type, scheduler.DECISION_TYPE.EXECUTE_NODES)
                test.eq(#decision.payload.nodes, 1)
                test.eq(decision.payload.nodes[1].node_id, "flexible-node")
                test.eq(decision.payload.nodes[1].trigger_reason, "root_ready")
            end)

            it("should immediately fail when node has no inputs and no requirements", function()
                local state = scheduler.create_empty_state()
                state.nodes = {
                    ["no-input-node"] = {
                        status = consts.STATUS.PENDING,
                        type = "processor",
                        parent_node_id = nil,
                    },
                }
                state.input_tracker = {
                    requirements = {},
                    available = {
                        ["no-input-node"] = {},
                    },
                }

                local decision = scheduler.find_next_work(state)

                test.eq(decision.type, scheduler.DECISION_TYPE.COMPLETE_WORKFLOW)
                test.is_false(decision.payload.success)
                test.contains(decision.payload.message, "No input data provided")
            end)
        end)

        describe("Edge Cases", function()
            it("should immediately fail when node has no input requirements", function()
                local state = scheduler.create_empty_state()
                state.nodes = {
                    ["node-1"] = {
                        status = consts.STATUS.PENDING,
                        type = "processor",
                    },
                }
                state.input_tracker = {
                    requirements = {},
                    available = {},
                }

                local decision = scheduler.find_next_work(state)

                test.eq(decision.type, scheduler.DECISION_TYPE.COMPLETE_WORKFLOW)
                test.is_false(decision.payload.success)
                test.contains(decision.payload.message, "No input data provided")
            end)

            it("should handle empty yield children list", function()
                local state = scheduler.create_empty_state()
                state.active_yields = {
                    ["parent-1"] = {
                        yield_id = "yield-123",
                        reply_to = "reply-topic",
                        pending_children = {},
                        results = {},
                    },
                }

                local decision = scheduler.find_next_work(state)

                test.eq(decision.type, scheduler.DECISION_TYPE.SATISFY_YIELD)
                test.eq(decision.payload.parent_id, "parent-1")
            end)

            it("should handle optional inputs correctly", function()
                local state = scheduler.create_empty_state()
                state.nodes = {
                    ["node-1"] = {
                        status = consts.STATUS.PENDING,
                        type = "processor",
                    },
                }
                state.input_tracker = {
                    requirements = {
                        ["node-1"] = {
                            required = { "essential" },
                            optional = { "nice-to-have", "metadata" },
                        },
                    },
                    available = {
                        ["node-1"] = {
                            essential = true,
                            metadata = true,
                        },
                    },
                }

                local decision = scheduler.find_next_work(state)

                test.eq(decision.type, scheduler.DECISION_TYPE.EXECUTE_NODES)
                test.eq(#decision.payload.nodes, 1)
                test.eq(decision.payload.nodes[1].node_id, "node-1")
            end)
        end)

        describe("Complex Workflow Scenarios", function()
            it("should handle multi-level yield hierarchy", function()
                local state = scheduler.create_empty_state()
                state.nodes = {
                    ["grandchild"] = {
                        status = consts.STATUS.PENDING,
                        type = "processor",
                        parent_node_id = "child",
                    },
                }
                state.active_yields = {
                    ["parent"] = {
                        pending_children = {
                            ["child"] = consts.STATUS.PENDING,
                        },
                    },
                    ["child"] = {
                        pending_children = {
                            ["grandchild"] = consts.STATUS.PENDING,
                        },
                        child_path = { "root", "parent", "child" },
                    },
                }
                state.input_tracker = {
                    requirements = {
                        ["grandchild"] = { required = { "input" }, optional = {} },
                    },
                    available = {
                        ["grandchild"] = { input = true },
                    },
                }

                local decision = scheduler.find_next_work(state)

                test.eq(decision.type, scheduler.DECISION_TYPE.EXECUTE_NODES)
                test.eq(#decision.payload.nodes, 1)
                test.eq(decision.payload.nodes[1].node_id, "grandchild")
                test.eq(decision.payload.nodes[1].trigger_reason, "yield_driven")
            end)

            it("should handle mixed root and yield scenarios", function()
                local state = scheduler.create_empty_state()
                state.nodes = {
                    ["root-node"] = {
                        status = consts.STATUS.PENDING,
                        type = "root_processor",
                        parent_node_id = nil,
                    },
                    ["yield-child"] = {
                        status = consts.STATUS.PENDING,
                        type = "child_processor",
                        parent_node_id = "parent",
                    },
                }
                state.active_yields = {
                    ["parent"] = {
                        pending_children = {
                            ["yield-child"] = consts.STATUS.PENDING,
                        },
                    },
                }
                state.input_tracker = {
                    requirements = {
                        ["root-node"] = { required = { "config" }, optional = {} },
                        ["yield-child"] = { required = { "data" }, optional = {} },
                    },
                    available = {
                        ["root-node"] = { config = true },
                        ["yield-child"] = { data = true },
                    },
                }

                local decision = scheduler.find_next_work(state)

                test.eq(decision.type, scheduler.DECISION_TYPE.EXECUTE_NODES)
                test.eq(#decision.payload.nodes, 1)
                test.eq(decision.payload.nodes[1].node_id, "yield-child")
                test.eq(decision.payload.nodes[1].trigger_reason, "yield_driven")
            end)
        end)

        describe("Concurrent Input-Ready Execution", function()
            it("should find multiple input-ready nodes concurrently when dependencies are satisfied", function()
                local state = scheduler.create_empty_state()

                state.nodes = {
                    ["node-a"] = {
                        status = consts.STATUS.COMPLETED_SUCCESS,
                        type = "processor",
                        parent_node_id = nil,
                    },
                    ["node-b"] = {
                        status = consts.STATUS.PENDING,
                        type = "processor",
                        parent_node_id = nil,
                    },
                    ["node-c"] = {
                        status = consts.STATUS.PENDING,
                        type = "processor",
                        parent_node_id = nil,
                    },
                    ["node-d"] = {
                        status = consts.STATUS.PENDING,
                        type = "processor",
                        parent_node_id = nil,
                    },
                }

                state.input_tracker = {
                    requirements = {
                        ["node-b"] = { required = { "from_a" }, optional = {} },
                        ["node-c"] = { required = { "from_a" }, optional = {} },
                        ["node-d"] = { required = { "from_b", "from_c" }, optional = {} },
                    },
                    available = {
                        ["node-b"] = { from_a = true },
                        ["node-c"] = { from_a = true },
                        ["node-d"] = {},
                    },
                }

                local decision = scheduler.find_next_work(state)

                test.eq(decision.type, scheduler.DECISION_TYPE.EXECUTE_NODES)
                test.eq(#decision.payload.nodes, 2)

                local executed_nodes: { [string]: boolean } = {}
                for _, node in ipairs(decision.payload.nodes) do
                    executed_nodes[node.node_id] = true
                    test.eq(node.trigger_reason, "input_ready")
                end

                test.is_true(executed_nodes["node-b"])
                test.is_true(executed_nodes["node-c"])
                test.is_nil(executed_nodes["node-d"])
            end)

            it("should NOT classify input-dependent nodes as root_ready", function()
                local state = scheduler.create_empty_state()

                state.nodes = {
                    ["dependency-node"] = {
                        status = consts.STATUS.PENDING,
                        type = "processor",
                        parent_node_id = nil,
                    },
                }

                state.input_tracker = {
                    requirements = {
                        ["dependency-node"] = { required = { "upstream_data" }, optional = {} },
                    },
                    available = {
                        ["dependency-node"] = { upstream_data = true },
                    },
                }

                local decision = scheduler.find_next_work(state)

                test.eq(decision.type, scheduler.DECISION_TYPE.EXECUTE_NODES)
                test.eq(#decision.payload.nodes, 1)
                test.eq(decision.payload.nodes[1].node_id, "dependency-node")
                test.eq(decision.payload.nodes[1].trigger_reason, "input_ready")
            end)

            it("should classify true root nodes as root_ready", function()
                local state = scheduler.create_empty_state()

                state.nodes = {
                    ["true-root"] = {
                        status = consts.STATUS.PENDING,
                        type = "processor",
                        parent_node_id = nil,
                    },
                }

                state.input_tracker = {
                    requirements = {},
                    available = {
                        ["true-root"] = { config = true, workflow_input = true },
                    },
                }

                local decision = scheduler.find_next_work(state)

                test.eq(decision.type, scheduler.DECISION_TYPE.EXECUTE_NODES)
                test.eq(#decision.payload.nodes, 1)
                test.eq(decision.payload.nodes[1].node_id, "true-root")
                test.eq(decision.payload.nodes[1].trigger_reason, "root_ready")
            end)
        end)

        describe("Parent-Child Error Handling", function()
            it("should succeed when parent handles child failures successfully", function()
                local state = scheduler.create_empty_state()
                state.nodes = {
                    ["map-reduce-parent"] = {
                        status = consts.STATUS.COMPLETED_SUCCESS,
                        type = "parallel_processor",
                        parent_node_id = nil,
                    },
                    ["child-1"] = {
                        status = consts.STATUS.COMPLETED_SUCCESS,
                        type = "child_processor",
                        parent_node_id = "map-reduce-parent",
                    },
                    ["child-2"] = {
                        status = consts.STATUS.COMPLETED_FAILURE,
                        type = "child_processor",
                        parent_node_id = "map-reduce-parent",
                    },
                    ["child-3"] = {
                        status = consts.STATUS.COMPLETED_FAILURE,
                        type = "child_processor",
                        parent_node_id = "map-reduce-parent",
                    },
                }
                state.has_workflow_output = true

                local decision = scheduler.find_next_work(state)

                test.eq(decision.type, scheduler.DECISION_TYPE.COMPLETE_WORKFLOW)
                test.is_true(decision.payload.success)
                test.contains(decision.payload.message, "successfully")
            end)

            it("should fail when parent fails even if some children succeeded", function()
                local state = scheduler.create_empty_state()
                state.nodes = {
                    ["map-reduce-parent"] = {
                        status = consts.STATUS.COMPLETED_FAILURE,
                        type = "parallel_processor",
                        parent_node_id = nil,
                    },
                    ["child-1"] = {
                        status = consts.STATUS.COMPLETED_SUCCESS,
                        type = "child_processor",
                        parent_node_id = "map-reduce-parent",
                    },
                    ["child-2"] = {
                        status = consts.STATUS.COMPLETED_FAILURE,
                        type = "child_processor",
                        parent_node_id = "map-reduce-parent",
                    },
                }

                local decision = scheduler.find_next_work(state)

                test.eq(decision.type, scheduler.DECISION_TYPE.COMPLETE_WORKFLOW)
                test.is_false(decision.payload.success)
                test.contains(decision.payload.message, "without producing output")
            end)

            it("should fail when orphaned nodes fail", function()
                local state = scheduler.create_empty_state()
                state.nodes = {
                    ["root-node"] = {
                        status = consts.STATUS.COMPLETED_SUCCESS,
                        type = "processor",
                        parent_node_id = nil,
                    },
                    ["orphan-failed"] = {
                        status = consts.STATUS.COMPLETED_FAILURE,
                        type = "processor",
                        parent_node_id = nil,
                    },
                }

                local decision = scheduler.find_next_work(state)

                test.eq(decision.type, scheduler.DECISION_TYPE.COMPLETE_WORKFLOW)
                test.is_false(decision.payload.success)
                test.contains(decision.payload.message, "without producing output")
            end)

            it("should succeed with nested parent-child success despite grandchild failures", function()
                local state = scheduler.create_empty_state()
                state.nodes = {
                    ["root-parent"] = {
                        status = consts.STATUS.COMPLETED_SUCCESS,
                        type = "root_processor",
                        parent_node_id = nil,
                    },
                    ["middle-parent"] = {
                        status = consts.STATUS.COMPLETED_SUCCESS,
                        type = "middle_processor",
                        parent_node_id = "root-parent",
                    },
                    ["grandchild-1"] = {
                        status = consts.STATUS.COMPLETED_FAILURE,
                        type = "child_processor",
                        parent_node_id = "middle-parent",
                    },
                    ["grandchild-2"] = {
                        status = consts.STATUS.COMPLETED_FAILURE,
                        type = "child_processor",
                        parent_node_id = "middle-parent",
                    },
                }
                state.has_workflow_output = true

                local decision = scheduler.find_next_work(state)

                test.eq(decision.type, scheduler.DECISION_TYPE.COMPLETE_WORKFLOW)
                test.is_true(decision.payload.success)
                test.contains(decision.payload.message, "successfully")
            end)

            it("should fail when intermediate parent fails even if root succeeds", function()
                local state = scheduler.create_empty_state()
                state.nodes = {
                    ["root-parent"] = {
                        status = consts.STATUS.COMPLETED_SUCCESS,
                        type = "root_processor",
                        parent_node_id = nil,
                    },
                    ["middle-parent"] = {
                        status = consts.STATUS.COMPLETED_FAILURE,
                        type = "middle_processor",
                        parent_node_id = "root-parent",
                    },
                    ["grandchild"] = {
                        status = consts.STATUS.COMPLETED_SUCCESS,
                        type = "child_processor",
                        parent_node_id = "middle-parent",
                    },
                }

                local decision = scheduler.find_next_work(state)

                test.eq(decision.type, scheduler.DECISION_TYPE.COMPLETE_WORKFLOW)
                test.is_false(decision.payload.success)
                test.contains(decision.payload.message, "without producing output")
            end)
        end)

        describe("Selective Node Loading Optimization", function()
            it("should handle scheduler snapshot with only active nodes", function()
                local state = scheduler.create_empty_state()
                state.nodes = {
                    ["pending-node-1"] = {
                        status = consts.STATUS.PENDING,
                        type = "processor",
                    },
                    ["running-node-1"] = {
                        status = consts.STATUS.RUNNING,
                        type = "processor",
                    },
                    ["pending-node-2"] = {
                        status = consts.STATUS.PENDING,
                        type = "processor",
                    },
                }
                state.input_tracker = {
                    requirements = {
                        ["pending-node-1"] = { required = { "input" }, optional = {} },
                        ["pending-node-2"] = { required = { "input" }, optional = {} },
                    },
                    available = {
                        ["pending-node-1"] = { input = true },
                        ["pending-node-2"] = {},
                    },
                }
                state.active_processes = {
                    ["running-node-1"] = true,
                }

                local decision = scheduler.find_next_work(state)

                test.eq(decision.type, scheduler.DECISION_TYPE.EXECUTE_NODES)
                test.eq(#decision.payload.nodes, 1)
                test.eq(decision.payload.nodes[1].node_id, "pending-node-1")
                test.eq(decision.payload.nodes[1].trigger_reason, "input_ready")
            end)

            it("should efficiently complete workflow when has_workflow_output is true", function()
                local state = scheduler.create_empty_state()
                state.nodes = {
                    ["completed-node"] = {
                        status = consts.STATUS.COMPLETED_SUCCESS,
                        type = "processor",
                    },
                }
                state.active_processes = {}
                state.active_yields = {}
                state.has_workflow_output = true

                local decision = scheduler.find_next_work(state)

                test.eq(decision.type, scheduler.DECISION_TYPE.COMPLETE_WORKFLOW)
                test.is_true(decision.payload.success)
                test.contains(decision.payload.message, "successfully")
            end)

            it("should handle large workflow simulation with concurrent execution", function()
                local state = scheduler.create_empty_state()

                state.nodes = {
                    ["active-1"] = { status = consts.STATUS.PENDING, type = "processor" },
                    ["active-2"] = { status = consts.STATUS.PENDING, type = "processor" },
                    ["active-3"] = { status = consts.STATUS.RUNNING, type = "processor" },
                    ["active-4"] = { status = consts.STATUS.PENDING, type = "processor" },
                }

                state.input_tracker = {
                    requirements = {},
                    available = {
                        ["active-1"] = { config = true },
                        ["active-2"] = { config = true },
                        ["active-4"] = { config = true },
                    },
                }

                state.active_processes = {
                    ["active-3"] = true,
                }

                local decision = scheduler.find_next_work(state)

                test.eq(decision.type, scheduler.DECISION_TYPE.EXECUTE_NODES)
                test.eq(#decision.payload.nodes, 3)

                local executed_nodes: { [string]: boolean } = {}
                for _, node in ipairs(decision.payload.nodes) do
                    executed_nodes[node.node_id] = true
                    test.eq(node.trigger_reason, "root_ready")
                end

                test.is_true(executed_nodes["active-1"])
                test.is_true(executed_nodes["active-2"])
                test.is_true(executed_nodes["active-4"])
                test.is_nil(executed_nodes["active-3"])
            end)
        end)

        describe("Concurrent Execution", function()
            it("should return multiple nodes for concurrent execution in diamond pattern", function()
                local state = scheduler.create_empty_state()

                state.nodes = {
                    ["node-b"] = {
                        status = consts.STATUS.PENDING,
                        type = "processor",
                    },
                    ["node-c"] = {
                        status = consts.STATUS.PENDING,
                        type = "processor",
                    },
                    ["node-d"] = {
                        status = consts.STATUS.PENDING,
                        type = "processor",
                    },
                }

                state.input_tracker = {
                    requirements = {
                        ["node-b"] = { required = { "from_a" }, optional = {} },
                        ["node-c"] = { required = { "from_a" }, optional = {} },
                        ["node-d"] = { required = { "from_b", "from_c" }, optional = {} },
                    },
                    available = {
                        ["node-b"] = { from_a = true },
                        ["node-c"] = { from_a = true },
                        ["node-d"] = {},
                    },
                }

                local decision = scheduler.find_next_work(state)

                test.eq(decision.type, scheduler.DECISION_TYPE.EXECUTE_NODES)
                test.eq(#decision.payload.nodes, 2)

                local node_ids: { string } = {}
                for _, node_info in ipairs(decision.payload.nodes) do
                    table.insert(node_ids, node_info.node_id)
                    test.eq(node_info.trigger_reason, "input_ready")
                end

                test.is_true(node_ids[1] == "node-b" or node_ids[1] == "node-c")
                test.is_true(node_ids[2] == "node-b" or node_ids[2] == "node-c")
                test.neq(node_ids[1], node_ids[2])
            end)

            it("should batch independent root nodes for concurrent execution", function()
                local state = scheduler.create_empty_state()

                state.nodes = {
                    ["root-1"] = {
                        status = consts.STATUS.PENDING,
                        type = "processor",
                    },
                    ["root-2"] = {
                        status = consts.STATUS.PENDING,
                        type = "processor",
                    },
                    ["root-3"] = {
                        status = consts.STATUS.PENDING,
                        type = "processor",
                    },
                }

                state.input_tracker = {
                    requirements = {},
                    available = {
                        ["root-1"] = { config = true },
                        ["root-2"] = { config = true },
                        ["root-3"] = { config = true },
                    },
                }

                local decision = scheduler.find_next_work(state)

                test.eq(decision.type, scheduler.DECISION_TYPE.EXECUTE_NODES)
                test.eq(#decision.payload.nodes, 3)

                local scheduled_ids: { [string]: boolean } = {}
                for _, node_info in ipairs(decision.payload.nodes) do
                    scheduled_ids[node_info.node_id] = true
                    test.eq(node_info.trigger_reason, "root_ready")
                end

                test.is_true(scheduled_ids["root-1"])
                test.is_true(scheduled_ids["root-2"])
                test.is_true(scheduled_ids["root-3"])
            end)

            it("should handle single execution when only one node is ready", function()
                local state = scheduler.create_empty_state()

                state.nodes = {
                    ["single-node"] = {
                        status = consts.STATUS.PENDING,
                        type = "processor",
                    },
                }

                state.input_tracker = {
                    requirements = {},
                    available = {
                        ["single-node"] = { config = true },
                    },
                }

                local decision = scheduler.find_next_work(state)

                test.eq(decision.type, scheduler.DECISION_TYPE.EXECUTE_NODES)
                test.eq(#decision.payload.nodes, 1)
                test.eq(decision.payload.nodes[1].node_id, "single-node")
                test.eq(decision.payload.nodes[1].trigger_reason, "root_ready")
            end)
        end)
    end)
end

return test.run_cases(define_tests)
