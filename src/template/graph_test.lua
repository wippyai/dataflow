local test = require("test")
local uuid = require("uuid")
local consts = require("consts")
local template_graph = require("template_graph")

local function define_tests()
    describe("Template Graph Tests", function()
        describe("TemplateGraph Class Methods", function()
            it("should create empty template graph", function()
                local graph = template_graph.new()
                expect(graph:is_empty()).to_be_true()
                expect(#graph:get_roots()).to_equal(0)
                expect(#graph:get_all_nodes()).to_equal(0)
            end)

            it("should detect non-empty template graph", function()
                local graph = template_graph.new()
                local node_id = uuid.v7()
                graph.nodes[node_id] = { node_id = node_id }

                expect(graph:is_empty()).to_be_false()
                expect(#graph:get_all_nodes()).to_equal(1)
                expect(graph:get_all_nodes()[1]).to_equal(node_id)
            end)

            it("should get node data", function()
                local graph = template_graph.new()
                local node_id = uuid.v7()
                local node_data = { node_id = node_id, type = "test" }
                graph.nodes[node_id] = node_data

                expect(graph:get_node(node_id)).to_equal(node_data)
                expect(graph:get_node("nonexistent")).to_be_nil()
            end)

            it("should get edges", function()
                local graph = template_graph.new()
                local node_id = uuid.v7()
                local target_id = uuid.v7()
                graph.edges[node_id] = { target_id }

                local edges = graph:get_edges(node_id)
                expect(#edges).to_equal(1)
                expect(edges[1]).to_equal(target_id)

                expect(#graph:get_edges("nonexistent")).to_equal(0)
            end)

            it("should get root nodes", function()
                local graph = template_graph.new()
                local root1 = uuid.v7()
                local root2 = uuid.v7()
                graph.roots = { root1, root2 }

                local roots = graph:get_roots()
                expect(#roots).to_equal(2)
                expect(roots[1]).to_equal(root1)
                expect(roots[2]).to_equal(root2)
            end)
        end)

        describe("Cycle Detection", function()
            it("should detect no cycles in empty graph", function()
                local graph = template_graph.new()
                local has_cycles, description = graph:has_cycles()
                expect(has_cycles).to_be_false()
                expect(description).to_be_nil()
            end)

            it("should detect no cycles in simple chain", function()
                local graph = template_graph.new()
                local node1 = uuid.v7()
                local node2 = uuid.v7()
                local node3 = uuid.v7()

                graph.nodes[node1] = { node_id = node1 }
                graph.nodes[node2] = { node_id = node2 }
                graph.nodes[node3] = { node_id = node3 }

                graph.edges[node1] = { node2 }
                graph.edges[node2] = { node3 }
                graph.edges[node3] = {}

                local has_cycles, description = graph:has_cycles()
                expect(has_cycles).to_be_false()
                expect(description).to_be_nil()
            end)

            it("should detect cycles in circular graph", function()
                local graph = template_graph.new()
                local node1 = uuid.v7()
                local node2 = uuid.v7()
                local node3 = uuid.v7()

                graph.nodes[node1] = { node_id = node1 }
                graph.nodes[node2] = { node_id = node2 }
                graph.nodes[node3] = { node_id = node3 }

                -- Create cycle: node1 -> node2 -> node3 -> node1
                graph.edges[node1] = { node2 }
                graph.edges[node2] = { node3 }
                graph.edges[node3] = { node1 }

                local has_cycles, description = graph:has_cycles()
                expect(has_cycles).to_be_true()
                expect(description).to_contain("Circular dependency")
            end)

            it("should detect self-referencing cycle", function()
                local graph = template_graph.new()
                local node1 = uuid.v7()

                graph.nodes[node1] = { node_id = node1 }
                graph.edges[node1] = { node1 } -- Self-cycle

                local has_cycles, description = graph:has_cycles()
                expect(has_cycles).to_be_true()
                expect(description).to_contain("Circular dependency")
            end)

            it("should handle disconnected components", function()
                local graph = template_graph.new()
                local node1 = uuid.v7()
                local node2 = uuid.v7()
                local node3 = uuid.v7()
                local node4 = uuid.v7()

                graph.nodes[node1] = { node_id = node1 }
                graph.nodes[node2] = { node_id = node2 }
                graph.nodes[node3] = { node_id = node3 }
                graph.nodes[node4] = { node_id = node4 }

                -- Two separate chains
                graph.edges[node1] = { node2 }
                graph.edges[node2] = {}
                graph.edges[node3] = { node4 }
                graph.edges[node4] = {}

                local has_cycles, description = graph:has_cycles()
                expect(has_cycles).to_be_false()
                expect(description).to_be_nil()
            end)
        end)

        describe("Template Discovery", function()
            it("should handle invalid parent node", function()
                local graph, error = template_graph.build_for_node(nil)
                expect(graph).to_be_nil()
                expect(error).to_contain("Invalid parent node")

                local invalid_node = { dataflow_id = "test" } -- Missing node_id
                graph, error = template_graph.build_for_node(invalid_node)
                expect(graph).to_be_nil()
                expect(error).to_contain("Invalid parent node")
            end)

            it("should handle node reader creation failure", function()
                local mock_deps = {
                    node_reader = {
                        with_dataflow = function(dataflow_id)
                            return nil, "Node reader creation failed"
                        end
                    }
                }

                local parent_node = {
                    dataflow_id = "test_dataflow",
                    node_id = "test_node"
                }

                local graph, error = template_graph.build_for_node(parent_node, mock_deps)
                expect(graph).to_be_nil()
                expect(error).to_contain("Failed to create node reader")
            end)

            it("should handle template query failure", function()
                local mock_deps = {
                    node_reader = {
                        with_dataflow = function(dataflow_id)
                            return {
                                with_parent_nodes = function(self, parent_id)
                                    return self
                                end,
                                with_statuses = function(self, status)
                                    return self
                                end,
                                all = function(self)
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

                local graph, error = template_graph.build_for_node(parent_node, mock_deps)
                expect(graph).to_be_nil()
                expect(error).to_contain("Failed to query template nodes")
            end)

            it("should return empty graph when no templates found", function()
                local mock_deps = {
                    node_reader = {
                        with_dataflow = function(dataflow_id)
                            return {
                                with_parent_nodes = function(self, parent_id)
                                    return self
                                end,
                                with_statuses = function(self, status)
                                    return self
                                end,
                                all = function(self)
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

                local graph, error = template_graph.build_for_node(parent_node, mock_deps)
                expect(error).to_be_nil()
                expect(graph).not_to_be_nil()
                expect(graph:is_empty()).to_be_true()
            end)
        end)

        describe("Graph Building", function()
            it("should build simple template graph", function()
                local node1_id = uuid.v7()
                local node2_id = uuid.v7()

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
                        with_dataflow = function(dataflow_id)
                            return {
                                with_parent_nodes = function(self, parent_id)
                                    return self
                                end,
                                with_statuses = function(self, status)
                                    return self
                                end,
                                all = function(self)
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

                local graph, error = template_graph.build_for_node(parent_node, mock_deps)
                expect(error).to_be_nil()
                expect(graph).not_to_be_nil()
                expect(graph:is_empty()).to_be_false()

                -- Check nodes
                expect(graph:get_node(node1_id)).not_to_be_nil()
                expect(graph:get_node(node2_id)).not_to_be_nil()

                -- Check edges
                local edges = graph:get_edges(node1_id)
                expect(#edges).to_equal(1)
                expect(edges[1]).to_equal(node2_id)

                -- Check roots
                local roots = graph:get_roots()
                expect(#roots).to_equal(1)
                expect(roots[1]).to_equal(node1_id)

                -- Check no cycles
                local has_cycles = graph:has_cycles()
                expect(has_cycles).to_be_false()
            end)

            it("should handle error targets in addition to data targets", function()
                local node1_id = uuid.v7()
                local node2_id = uuid.v7()
                local node3_id = uuid.v7()

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
                        with_dataflow = function(dataflow_id)
                            return {
                                with_parent_nodes = function(self, parent_id)
                                    return self
                                end,
                                with_statuses = function(self, status)
                                    return self
                                end,
                                all = function(self)
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

                local graph, error = template_graph.build_for_node(parent_node, mock_deps)
                expect(error).to_be_nil()

                -- Check that node1 has edges to both node2 and node3
                local edges = graph:get_edges(node1_id)
                expect(#edges).to_equal(2)

                local edge_set = {}
                for _, edge in ipairs(edges) do
                    edge_set[edge] = true
                end
                expect(edge_set[node2_id]).to_be_true()
                expect(edge_set[node3_id]).to_be_true()

                -- Root should still be node1
                local roots = graph:get_roots()
                expect(#roots).to_equal(1)
                expect(roots[1]).to_equal(node1_id)
            end)

            it("should ignore external node references", function()
                local node1_id = uuid.v7()
                local node2_id = uuid.v7()
                local external_id = uuid.v7()

                local templates = {
                    {
                        node_id = node1_id,
                        type = "func",
                        config = {
                            data_targets = {
                                { node_id = node2_id, data_type = "node.input" },
                                { node_id = external_id, data_type = "node.input" } -- External reference
                            }
                        }
                    },
                    {
                        node_id = node2_id,
                        type = "func",
                        config = {}
                    }
                    -- Note: external_id is not in templates
                }

                local mock_deps = {
                    node_reader = {
                        with_dataflow = function(dataflow_id)
                            return {
                                with_parent_nodes = function(self, parent_id)
                                    return self
                                end,
                                with_statuses = function(self, status)
                                    return self
                                end,
                                all = function(self)
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

                local graph, error = template_graph.build_for_node(parent_node, mock_deps)
                expect(error).to_be_nil()

                -- Should only have edge to node2, not external
                local edges = graph:get_edges(node1_id)
                expect(#edges).to_equal(1)
                expect(edges[1]).to_equal(node2_id)

                -- External node should not be in graph
                expect(graph:get_node(external_id)).to_be_nil()
            end)

            it("should detect circular dependencies", function()
                local node1_id = uuid.v7()
                local node2_id = uuid.v7()
                local node3_id = uuid.v7()

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
                                { node_id = node1_id, data_type = "node.input" } -- Creates cycle
                            }
                        }
                    }
                }

                local mock_deps = {
                    node_reader = {
                        with_dataflow = function(dataflow_id)
                            return {
                                with_parent_nodes = function(self, parent_id)
                                    return self
                                end,
                                with_statuses = function(self, status)
                                    return self
                                end,
                                all = function(self)
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

                local graph, error = template_graph.build_for_node(parent_node, mock_deps)
                expect(graph).to_be_nil()
                expect(error).to_contain("circular dependencies")
            end)

            it("should detect when all nodes have dependencies (no roots)", function()
                local node1_id = uuid.v7()
                local node2_id = uuid.v7()

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
                                { node_id = node1_id, data_type = "node.input" } -- Both depend on each other
                            }
                        }
                    }
                }

                local mock_deps = {
                    node_reader = {
                        with_dataflow = function(dataflow_id)
                            return {
                                with_parent_nodes = function(self, parent_id)
                                    return self
                                end,
                                with_statuses = function(self, status)
                                    return self
                                end,
                                all = function(self)
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

                local graph, error = template_graph.build_for_node(parent_node, mock_deps)
                expect(graph).to_be_nil()
                expect(error).to_contain("circular dependencies")
            end)

            it("should handle multiple roots", function()
                local node1_id = uuid.v7()
                local node2_id = uuid.v7()
                local node3_id = uuid.v7()

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
                        with_dataflow = function(dataflow_id)
                            return {
                                with_parent_nodes = function(self, parent_id)
                                    return self
                                end,
                                with_statuses = function(self, status)
                                    return self
                                end,
                                all = function(self)
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

                local graph, error = template_graph.build_for_node(parent_node, mock_deps)
                expect(error).to_be_nil()

                local roots = graph:get_roots()
                expect(#roots).to_equal(2)

                -- Convert to set for easier checking
                local root_set = {}
                for _, root in ipairs(roots) do
                    root_set[root] = true
                end
                expect(root_set[node1_id]).to_be_true()
                expect(root_set[node2_id]).to_be_true()
                expect(root_set[node3_id]).to_be_nil()
            end)

            it("should handle templates with no config", function()
                local node1_id = uuid.v7()

                local templates = {
                    {
                        node_id = node1_id,
                        type = "func"
                        -- No config
                    }
                }

                local mock_deps = {
                    node_reader = {
                        with_dataflow = function(dataflow_id)
                            return {
                                with_parent_nodes = function(self, parent_id)
                                    return self
                                end,
                                with_statuses = function(self, status)
                                    return self
                                end,
                                all = function(self)
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

                local graph, error = template_graph.build_for_node(parent_node, mock_deps)
                expect(error).to_be_nil()

                expect(graph:get_node(node1_id)).not_to_be_nil()
                expect(#graph:get_edges(node1_id)).to_equal(0)

                local roots = graph:get_roots()
                expect(#roots).to_equal(1)
                expect(roots[1]).to_equal(node1_id)
            end)

            it("should provide consistent root ordering", function()
                local node1_id = "aaaa" -- Lexicographically first
                local node2_id = "bbbb"
                local node3_id = "cccc"

                local templates = {
                    {
                        node_id = node3_id, -- Add in different order
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
                        with_dataflow = function(dataflow_id)
                            return {
                                with_parent_nodes = function(self, parent_id)
                                    return self
                                end,
                                with_statuses = function(self, status)
                                    return self
                                end,
                                all = function(self)
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

                local graph, error = template_graph.build_for_node(parent_node, mock_deps)
                expect(error).to_be_nil()

                local roots = graph:get_roots()
                expect(#roots).to_equal(3)

                -- Should be in sorted order
                expect(roots[1]).to_equal(node1_id)
                expect(roots[2]).to_equal(node2_id)
                expect(roots[3]).to_equal(node3_id)
            end)
        end)
    end)
end

return test.run_cases(define_tests)