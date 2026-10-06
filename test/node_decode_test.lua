local test = require("test")
local decoder = require("decoder")

local function define_tests()
    describe("Node row decoding", function()
        it("should handle invalid JSON gracefully", function()
            local nodes, err = decoder.get_nodes_for_dataflow("fixture")
            test.is_nil(err)

            local invalid_node = nil
            for _, node in ipairs(nodes) do
                if node.type == "invalid_json_config_node" then
                    invalid_node = node
                    break
                end
            end

            test.not_nil(invalid_node)
            -- Invalid JSON should default to empty table
            test.is_table(invalid_node.config)
            test.is_nil(next(invalid_node.config))
            test.is_table(invalid_node.metadata)
            test.is_nil(next(invalid_node.metadata))
        end)

        it("should handle empty string config", function()
            local nodes, err = decoder.get_nodes_for_dataflow("fixture")
            test.is_nil(err)

            local empty_node = nil
            for _, node in ipairs(nodes) do
                if node.type == "empty_string_config_node" then
                    empty_node = node
                    break
                end
            end

            test.not_nil(empty_node)
            test.is_table(empty_node.config)
            test.is_nil(next(empty_node.config))
            test.is_table(empty_node.metadata)
            test.is_nil(next(empty_node.metadata))
        end)
    end)
end
return { run = test.run_cases(define_tests) }
