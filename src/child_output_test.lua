local test = require("test")
local consts = require("consts")
local child_output = require("child_output")

local function make_node(rows)
    local state: any = {}
    local reader
    reader = {
        with_data = function(self, data_ids)
            state.queried_data_ids = data_ids
            state.queried_data_type = nil
            return self
        end,
        with_nodes = function(self, node_ids)
            state.queried_node_ids = node_ids
            state.queried_data_ids = nil
            state.queried_data_type = nil
            return self
        end,
        with_data_types = function(self, data_type)
            state.queried_data_type = data_type
            return self
        end,
        fetch_options = function(self, options)
            state.fetch_options = options
            return self
        end,
        all = function()
            local selected = {}
            for _, row in ipairs(rows) do
                local type_ok = state.queried_data_type == nil or row.data_type == state.queried_data_type
                local id_ok = state.queried_data_ids == nil
                for _, data_id in ipairs(state.queried_data_ids or {}) do
                    if data_id == row.data_id then id_ok = true end
                end
                if type_ok and id_ok then
                    selected[#selected + 1] = row
                end
            end
            return selected, nil
        end,
    }
    return { query = function() return reader end }, state
end

local function envelope(data_ids)
    return {
        data_id = "child-result-1",
        data_type = consts.DATA_TYPE.NODE_RESULT,
        node_id = "child-1",
        content = { success = true, data_ids = data_ids },
    }
end

local YIELD_RESULTS = { ["child-1"] = "child-result-1" }

local function define_tests()
    describe("collecting a child's output", function()
        it("collects the output rows the child pinned", function()
            local answer = {
                data_id = "child-answer-1",
                data_type = consts.DATA_TYPE.NODE_OUTPUT,
                node_id = "child-1",
                content = "the delegate's answer",
            }
            local n, state = make_node({ envelope({ "child-answer-1" }), answer })

            local rows, err = child_output.outputs_from_yield_results(n, YIELD_RESULTS)

            test.is_nil(err)
            test.eq(#rows, 1)
            test.eq(rows[1].content, "the delegate's answer")
            test.is_true(state.fetch_options.replace_references)
        end)

        it("drops a pinned node.input row that the child routed to a sibling", function()
            local sibling_input = {
                data_id = "sibling-input-1",
                data_type = consts.DATA_TYPE.NODE_INPUT,
                node_id = "child-2",
                content = "routed to the next node",
            }
            local answer = {
                data_id = "child-answer-1",
                data_type = consts.DATA_TYPE.NODE_OUTPUT,
                node_id = "child-1",
                content = "the delegate's answer",
            }
            local n = make_node({ envelope({ "sibling-input-1", "child-answer-1" }), sibling_input, answer })

            local rows, err = child_output.outputs_from_yield_results(n, YIELD_RESULTS)

            test.is_nil(err)
            test.eq(#rows, 1)
            test.eq(rows[1].data_id, "child-answer-1")
        end)

        it("falls back to a node scan when the envelope pins nothing", function()
            local opaque = {
                data_id = "child-result-1",
                data_type = consts.DATA_TYPE.NODE_RESULT,
                node_id = "child-1",
                content = "opaque",
            }
            local answer = {
                data_id = "child-answer-1",
                data_type = consts.DATA_TYPE.NODE_OUTPUT,
                node_id = "child-1",
                content = "scanned",
            }
            local n, state = make_node({ opaque, answer })

            local rows, err = child_output.outputs_from_yield_results(n, YIELD_RESULTS)

            test.is_nil(err)
            test.eq(state.queried_node_ids[1], "child-1")
            test.eq(#rows, 1)
            test.eq(rows[1].content, "scanned")
        end)
    end)
end

local run_cases = test.run_cases(define_tests)

return { run_tests = function(options) return run_cases(options) end }
