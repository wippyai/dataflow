local test = require("test")
local uuid = require("uuid")
local consts = require("consts")
local template_graph = require("template_graph")

local function define_tests()
    describe("Template Graph Tests", function()
        describe("TemplateGraph Class Methods", function()
            it("should create empty template graph", function()
                local graph: any = template_graph.new()
                test.is_true(graph:is_empty())
                test.eq(#graph:get_roots(), 0)
                test.eq(#graph:get_all_nodes(), 0)
            end)

            it("should detect non-empty template graph", function()
                local graph: any = template_graph.new()
                local node_id = uuid.v7() :: string
                graph.nodes[node_id] = { node_id = node_id }

                test.is_false(graph:is_empty())
                test.eq(#graph:get_all_nodes(), 1)
                test.eq(graph:get_all_nodes()[1], node_id)
            end)

            it("should get node data", function()
                local graph: any = template_graph.new()
                local node_id = uuid.v7() :: string
                local node_data = { node_id = node_id, type = "test" }
                graph.nodes[node_id] = node_data

                test.eq(graph:get_node(node_id), node_data)
                test.is_nil(graph:get_node("nonexistent"))
            end)

            it("should get edges", function()
                local graph: any = template_graph.new()
                local node_id = uuid.v7() :: string
                local target_id = uuid.v7() :: string
                graph.edges[node_id] = { target_id }

                local edges = graph:get_edges(node_id)
                test.eq(#edges, 1)
                test.eq(edges[1], target_id)

                test.eq(#graph:get_edges("nonexistent"), 0)
            end)

            it("should get root nodes", function()
                local graph: any = template_graph.new()
                local root1 = uuid.v7() :: string
                local root2 = uuid.v7() :: string
                graph.roots = { root1, root2 }

                local roots = graph:get_roots()
                test.eq(#roots, 2)
                test.eq(roots[1], root1)
                test.eq(roots[2], root2)
            end)
        end)

        describe("Cycle Detection", function()
            it("should detect no cycles in empty graph", function()
                local graph: any = template_graph.new()
                local has_cycles, description = graph:has_cycles()
                test.is_false(has_cycles)
                test.is_nil(description)
            end)

            it("should detect no cycles in simple chain", function()
                local graph: any = template_graph.new()
                local node1 = uuid.v7() :: string
                local node2: string = uuid.v7()
                local node3: string = uuid.v7()

                graph.nodes[node1] = { node_id = node1 }
                graph.nodes[node2] = { node_id = node2 }
                graph.nodes[node3] = { node_id = node3 }

                graph.edges[node1] = { node2 }
                graph.edges[node2] = { node3 }
                graph.edges[node3] = {}

                local has_cycles, description = graph:has_cycles()
                test.is_false(has_cycles)
                test.is_nil(description)
            end)

            it("should detect cycles in circular graph", function()
                local graph: any = template_graph.new()
                local node1 = uuid.v7() :: string
                local node2: string = uuid.v7()
                local node3: string = uuid.v7()

                graph.nodes[node1] = { node_id = node1 }
                graph.nodes[node2] = { node_id = node2 }
                graph.nodes[node3] = { node_id = node3 }

                graph.edges[node1] = { node2 }
                graph.edges[node2] = { node3 }
                graph.edges[node3] = { node1 }

                local has_cycles, description = graph:has_cycles()
                test.is_true(has_cycles)
                test.not_nil(description)
                test.contains(description :: string, "Circular dependency")
            end)

            it("should detect self-referencing cycle", function()
                local graph: any = template_graph.new()
                local node1 = uuid.v7() :: string

                graph.nodes[node1] = { node_id = node1 }
                graph.edges[node1] = { node1 }

                local has_cycles, description = graph:has_cycles()
                test.is_true(has_cycles)
                test.not_nil(description)
                test.contains(description :: string, "Circular dependency")
            end)

            it("should handle disconnected components", function()
                local graph: any = template_graph.new()
                local node1 = uuid.v7() :: string
                local node2: string = uuid.v7()
                local node3: string = uuid.v7()
                local node4: string = uuid.v7()

                graph.nodes[node1] = { node_id = node1 }
                graph.nodes[node2] = { node_id = node2 }
                graph.nodes[node3] = { node_id = node3 }
                graph.nodes[node4] = { node_id = node4 }

                graph.edges[node1] = { node2 }
                graph.edges[node2] = {}
                graph.edges[node3] = { node4 }
                graph.edges[node4] = {}

                local has_cycles, description = graph:has_cycles()
                test.is_false(has_cycles)
                test.is_nil(description)
            end)
        end)

        describe("Template Discovery", function()
            it("should handle invalid parent node", function()
                local graph, build_err = template_graph.build_for_node(nil)
                test.is_nil(graph)
                test.not_nil(build_err)
                test.contains(build_err :: string, "Invalid parent node")

                local invalid_node = { dataflow_id = "test" }
                graph, build_err = template_graph.build_for_node(invalid_node)
                test.is_nil(graph)
                test.not_nil(build_err)
                test.contains(build_err :: string, "Invalid parent node")
            end)

            it("should handle node reader creation failure", function()
                local mock_deps = {
                    node_reader = {
                        with_dataflow = function(_dataflow_id: string)
                            return nil, "Node reader creation failed"
                        end
                    }
                }

                local parent_node = {
                    dataflow_id = "test_dataflow",
                    node_id = "test_node"
                }

                local graph, build_err = template_graph.build_for_node(parent_node, mock_deps)
                test.is_nil(graph)
                test.not_nil(build_err)
                test.contains(build_err :: string, "Failed to create node reader")
            end)

            it("should handle template query failure", function()
                local mock_deps = {
                    node_reader = {
                        with_dataflow = function(_dataflow_id: string)
                            return {
                                with_parent_nodes = function(self, _parent_id: string)
                                    return self
                                end,
                                with_statuses = function(self, _status: string)
                                    return self
                                end,
                                all = function(_self)
                                    return nil, "Query failed"
                                end
                            }
                        end
                    }
                }

                local parent_node = {
                    dataflow_id = "test_dataflow",
                    node_id = "test_node"
                }

                local graph, build_err = template_graph.build_for_node(parent_node, mock_deps)
                test.is_nil(graph)
                test.not_nil(build_err)
                test.contains(build_err :: string, "Failed to query template nodes")
            end)

            it("should return empty graph when no templates found", function()
                local mock_deps = {
                    node_reader = {
                        with_dataflow = function(_dataflow_id: string)
                            return {
                                with_parent_nodes = function(self, _parent_id: string)
                                    return self
                                end,
                                with_statuses = function(self, _status: string)
                                    return self
                                end,
                                all = function(_self)
                                    return {}
                                end
                            }
                        end
                    }
                }

                local parent_node = {
                    dataflow_id = "test_dataflow",
                    node_id = "test_node"
                }

                local graph, build_err = template_graph.build_for_node(parent_node, mock_deps)
                test.is_nil(build_err)
                test.not_nil(graph)
                test.is_true((graph :: any):is_empty())
            end)
        end)

        describe("Graph Building", function()
            it("should build simple template graph", function()
                local node1_id: string = uuid.v7()
                local node2_id: string = uuid.v7()

                local templates = {
                    {
                        node_id = node1_id,
                        type = "func",
                        config = {
                            data_targets = {
                                { node_id = node2_id, data_type = "node.input" }
                            }
                        }
                    },
                    {
                        node_id = node2_id,
                        type = "func",
                        config = {}
                    }
                }

                local mock_deps = {
                    node_reader = {
                        with_dataflow = function(_dataflow_id: string)
                            return {
                                with_parent_nodes = function(self, _parent_id: string)
                                    return self
                                end,
                                with_statuses = function(self, _status: string)
                                    return self
                                end,
                                all = function(_self)
                                    return templates
                                end
                            }
                        end
                    }
                }

                local parent_node = {
                    dataflow_id = "test_dataflow",
                    node_id = "test_node"
                }

                local graph, build_err = template_graph.build_for_node(parent_node, mock_deps)
                test.is_nil(build_err)
                test.not_nil(graph)
                test.is_false((graph :: any):is_empty())

                -- Check nodes
                test.not_nil((graph :: any):get_node(node1_id))
                test.not_nil((graph :: any):get_node(node2_id))

                -- Check edges
                local edges = (graph :: any):get_edges(node1_id)
                test.eq(#edges, 1)
                test.eq(edges[1], node2_id)

                -- Check roots
                local roots = (graph :: any):get_roots()
                test.eq(#roots, 1)
                test.eq(roots[1], node1_id)

                -- Check no cycles
                local has_cycles = (graph :: any):has_cycles()
                test.is_false(has_cycles)
            end)

            it("should handle error targets in addition to data targets", function()
                local node1_id: string = uuid.v7()
                local node2_id: string = uuid.v7()
                local node3_id: string = uuid.v7()

                local templates = {
                    {
                        node_id = node1_id,
                        type = "func",
                        config = {
                            data_targets = {
                                { node_id = node2_id, data_type = "node.input" }
                            },
                            error_targets = {
                                { node_id = node3_id, data_type = "node.input" }
                            }
                        }
                    },
                    {
                        node_id = node2_id,
                        type = "func",
                        config = {}
                    },
                    {
                        node_id = node3_id,
                        type = "func",
                        config = {}
                    }
                }

                local mock_deps = {
                    node_reader = {
                        with_dataflow = function(_dataflow_id: string)
                            return {
                                with_parent_nodes = function(self, _parent_id: string)
                                    return self
                                end,
                                with_statuses = function(self, _status: string)
                                    return self
                                end,
                                all = function(_self)
                                    return templates
                                end
                            }
                        end
                    }
                }

                local parent_node = {
                    dataflow_id = "test_dataflow",
                    node_id = "test_node"
                }

                local graph, build_err = template_graph.build_for_node(parent_node, mock_deps)
                test.is_nil(build_err)

                -- Check that node1 has edges to both node2 and node3
                local edges = (graph :: any):get_edges(node1_id)
                test.eq(#edges, 2)

                local edge_set: { [string]: boolean } = {}
                for _, edge in ipairs(edges) do
                    edge_set[edge] = true
                end
                test.is_true(edge_set[node2_id])
                test.is_true(edge_set[node3_id])

                -- Root should still be node1
                local roots = (graph :: any):get_roots()
                test.eq(#roots, 1)
                test.eq(roots[1], node1_id)
            end)

            it("should ignore external node references", function()
                local node1_id: string = uuid.v7()
                local node2_id: string = uuid.v7()
                local external_id: string = uuid.v7()

                local templates = {
                    {
                        node_id = node1_id,
                        type = "func",
                        config = {
                            data_targets = {
                                { node_id = node2_id, data_type = "node.input" },
                                { node_id = external_id, data_type = "node.input" }
                            }
                        }
                    },
                    {
                        node_id = node2_id,
                        type = "func",
                        config = {}
                    }
                }

                local mock_deps = {
                    node_reader = {
                        with_dataflow = function(_dataflow_id: string)
                            return {
                                with_parent_nodes = function(self, _parent_id: string)
                                    return self
                                end,
                                with_statuses = function(self, _status: string)
                                    return self
                                end,
                                all = function(_self)
                                    return templates
                                end
                            }
                        end
                    }
                }

                local parent_node = {
                    dataflow_id = "test_dataflow",
                    node_id = "test_node"
                }

                local graph, build_err = template_graph.build_for_node(parent_node, mock_deps)
                test.is_nil(build_err)

                -- Should only have edge to node2, not external
                local edges = (graph :: any):get_edges(node1_id)
                test.eq(#edges, 1)
                test.eq(edges[1], node2_id)

                -- External node should not be in graph
                test.is_nil((graph :: any):get_node(external_id))
            end)

            it("should detect circular dependencies", function()
                local node1_id: string = uuid.v7()
                local node2_id: string = uuid.v7()
                local node3_id: string = uuid.v7()

                local templates = {
                    {
                        node_id = node1_id,
                        type = "func",
                        config = {
                            data_targets = {
                                { node_id = node2_id, data_type = "node.input" }
                            }
                        }
                    },
                    {
                        node_id = node2_id,
                        type = "func",
                        config = {
                            data_targets = {
                                { node_id = node3_id, data_type = "node.input" }
                            }
                        }
                    },
                    {
                        node_id = node3_id,
                        type = "func",
                        config = {
                            data_targets = {
                                { node_id = node1_id, data_type = "node.input" }
                            }
                        }
                    }
                }

                local mock_deps = {
                    node_reader = {
                        with_dataflow = function(_dataflow_id: string)
                            return {
                                with_parent_nodes = function(self, _parent_id: string)
                                    return self
                                end,
                                with_statuses = function(self, _status: string)
                                    return self
                                end,
                                all = function(_self)
                                    return templates
                                end
                            }
                        end
                    }
                }

                local parent_node = {
                    dataflow_id = "test_dataflow",
                    node_id = "test_node"
                }

                local graph, build_err = template_graph.build_for_node(parent_node, mock_deps)
                test.is_nil(graph)
                test.not_nil(build_err)
                test.contains(build_err :: string, "circular dependencies")
            end)

            it("should detect when all nodes have dependencies (no roots)", function()
                local node1_id: string = uuid.v7()
                local node2_id: string = uuid.v7()

                local templates = {
                    {
                        node_id = node1_id,
                        type = "func",
                        config = {
                            data_targets = {
                                { node_id = node2_id, data_type = "node.input" }
                            }
                        }
                    },
                    {
                        node_id = node2_id,
                        type = "func",
                        config = {
                            data_targets = {
                                { node_id = node1_id, data_type = "node.input" }
                            }
                        }
                    }
                }

                local mock_deps = {
                    node_reader = {
                        with_dataflow = function(_dataflow_id: string)
                            return {
                                with_parent_nodes = function(self, _parent_id: string)
                                    return self
                                end,
                                with_statuses = function(self, _status: string)
                                    return self
                                end,
                                all = function(_self)
                                    return templates
                                end
                            }
                        end
                    }
                }

                local parent_node = {
                    dataflow_id = "test_dataflow",
                    node_id = "test_node"
                }

                local graph, build_err = template_graph.build_for_node(parent_node, mock_deps)
                test.is_nil(graph)
                test.not_nil(build_err)
                test.contains(build_err :: string, "circular dependencies")
            end)

            it("should handle multiple roots", function()
                local node1_id: string = uuid.v7()
                local node2_id: string = uuid.v7()
                local node3_id: string = uuid.v7()

                local templates = {
                    {
                        node_id = node1_id,
                        type = "func",
                        config = {
                            data_targets = {
                                { node_id = node3_id, data_type = "node.input" }
                            }
                        }
                    },
                    {
                        node_id = node2_id,
                        type = "func",
                        config = {
                            data_targets = {
                                { node_id = node3_id, data_type = "node.input" }
                            }
                        }
                    },
                    {
                        node_id = node3_id,
                        type = "func",
                        config = {}
                    }
                }

                local mock_deps = {
                    node_reader = {
                        with_dataflow = function(_dataflow_id: string)
                            return {
                                with_parent_nodes = function(self, _parent_id: string)
                                    return self
                                end,
                                with_statuses = function(self, _status: string)
                                    return self
                                end,
                                all = function(_self)
                                    return templates
                                end
                            }
                        end
                    }
                }

                local parent_node = {
                    dataflow_id = "test_dataflow",
                    node_id = "test_node"
                }

                local graph, build_err = template_graph.build_for_node(parent_node, mock_deps)
                test.is_nil(build_err)

                local roots = (graph :: any):get_roots()
                test.eq(#roots, 2)

                local root_set: { [string]: boolean } = {}
                for _, root in ipairs(roots) do
                    root_set[root] = true
                end
                test.is_true(root_set[node1_id])
                test.is_true(root_set[node2_id])
                test.is_nil(root_set[node3_id])
            end)

            it("should handle templates with no config", function()
                local node1_id: string = uuid.v7()

                local templates = {
                    {
                        node_id = node1_id,
                        type = "func"
                    }
                }

                local mock_deps = {
                    node_reader = {
                        with_dataflow = function(_dataflow_id: string)
                            return {
                                with_parent_nodes = function(self, _parent_id: string)
                                    return self
                                end,
                                with_statuses = function(self, _status: string)
                                    return self
                                end,
                                all = function(_self)
                                    return templates
                                end
                            }
                        end
                    }
                }

                local parent_node = {
                    dataflow_id = "test_dataflow",
                    node_id = "test_node"
                }

                local graph, build_err = template_graph.build_for_node(parent_node, mock_deps)
                test.is_nil(build_err)

                test.not_nil((graph :: any):get_node(node1_id))
                test.eq(#(graph :: any):get_edges(node1_id), 0)

                local roots = (graph :: any):get_roots()
                test.eq(#roots, 1)
                test.eq(roots[1], node1_id)
            end)

            it("should provide consistent root ordering", function()
                local node1_id: string = "aaaa"
                local node2_id: string = "bbbb"
                local node3_id: string = "cccc"

                local templates = {
                    {
                        node_id = node3_id,
                        type = "func",
                        config = {}
                    },
                    {
                        node_id = node1_id,
                        type = "func",
                        config = {}
                    },
                    {
                        node_id = node2_id,
                        type = "func",
                        config = {}
                    }
                }

                local mock_deps = {
                    node_reader = {
                        with_dataflow = function(_dataflow_id: string)
                            return {
                                with_parent_nodes = function(self, _parent_id: string)
                                    return self
                                end,
                                with_statuses = function(self, _status: string)
                                    return self
                                end,
                                all = function(_self)
                                    return templates
                                end
                            }
                        end
                    }
                }

                local parent_node = {
                    dataflow_id = "test_dataflow",
                    node_id = "test_node"
                }

                local graph, build_err = template_graph.build_for_node(parent_node, mock_deps)
                test.is_nil(build_err)

                local roots = (graph :: any):get_roots()
                test.eq(#roots, 3)

                test.eq(roots[1], node1_id)
                test.eq(roots[2], node2_id)
                test.eq(roots[3], node3_id)
            end)
        end)
    end)
end

return test.run_cases(define_tests)
