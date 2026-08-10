local test = require("test")
local agent_node = require("agent_node")
local agent_consts = require("agent_consts")

-- A minimal node_sdk double: records every n:data() call so tests can assert
-- on which observations process_tool_results actually wrote.
local function make_recording_node()
    local recorded = {}
    local n
    n = {
        node_id = "test-node",
        data = function(_self, data_type, content, opts)
            table.insert(recorded, {
                data_type = data_type,
                content = content,
                opts = opts
            })
        end
    }
    return n, recorded
end

local function find_by_tool_call_id(recorded, tool_call_id)
    for _, row in ipairs(recorded) do
        local meta = row.opts and row.opts.metadata or {}
        if meta.tool_call_id == tool_call_id then
            return row
        end
    end
    return nil
end

local function define_tests()
    describe("process_tool_results: exit validator rejection", function()
        it("still records observations for sibling tool calls in the same turn", function()
            local process_tool_results = agent_node._test.process_tool_results
            test.not_nil(process_tool_results, "process_tool_results exported for testing")

            local n, recorded = make_recording_node()

            local agent_result = {
                tool_calls = {
                    { id = "call_finish", name = "finish", arguments = { answer = "done" } },
                    { id = "call_search_1", name = "kb_search", arguments = { query = "a" } },
                    { id = "call_search_2", name = "kb_search", arguments = { query = "b" } },
                }
            }

            -- Only the sibling calls were actually executed by execute_tools; the
            -- exit call never appears in tool_results (matches production: split_exit_tool_calls
            -- routes it away from the executable set).
            local tool_results = {
                call_search_1 = { result = { hits = 1 } },
                call_search_2 = { result = { hits = 2 } },
            }

            local arena_config = {
                exit_func_id = "userspace.dataflow.node.agent.stub:exit_reject_validator"
            }

            local control_responses, control_delegations, task_complete, final_result = process_tool_results(
                n,
                tool_results,
                1,
                "finish",
                agent_result,
                arena_config,
                {},
                {}
            )

            test.eq(task_complete, false, "rejected finish does not complete the task")
            test.is_nil(final_result, "no final result on rejection")
            test.eq(#control_responses, 0, "no control responses for plain tool results")
            test.eq(#control_delegations, 0, "no delegations for plain tool results")

            local rejection = find_by_tool_call_id(recorded, "call_finish")
            test.not_nil(rejection, "rejection observation recorded for the finish call")
            test.eq(rejection.data_type, agent_consts.DATA_TYPE.AGENT_OBSERVATION, "rejection is an observation")
            test.eq((rejection.opts.metadata or {}).is_error, true, "rejection observation flagged as error")
            test.eq((rejection.opts.metadata or {}).exit_validation, true, "rejection observation flagged as exit validation")

            local sibling_1 = find_by_tool_call_id(recorded, "call_search_1")
            test.not_nil(sibling_1, "sibling call_search_1 got a recorded observation")
            test.eq((sibling_1.opts.metadata or {}).tool_name, "kb_search", "sibling observation carries tool name")
            test.eq((sibling_1.opts.metadata or {}).is_error, false, "sibling observation is not an error")

            local sibling_2 = find_by_tool_call_id(recorded, "call_search_2")
            test.not_nil(sibling_2, "sibling call_search_2 got a recorded observation")

            test.eq(#recorded, 3, "exactly one observation per tool_use id: rejection plus both siblings")
        end)

        it("a genuine completion still returns early without touching sibling results", function()
            local process_tool_results = agent_node._test.process_tool_results

            local n, recorded = make_recording_node()

            local agent_result = {
                tool_calls = {
                    { id = "call_finish", name = "finish", arguments = { answer = "done" } },
                    { id = "call_search_1", name = "kb_search", arguments = { query = "a" } },
                }
            }

            local tool_results = {
                call_search_1 = { result = { hits = 1 } },
            }

            -- No exit_func_id: the exit tool call is accepted unconditionally.
            local arena_config = {}

            local _control_responses, _control_delegations, task_complete, final_result = process_tool_results(
                n,
                tool_results,
                1,
                "finish",
                agent_result,
                arena_config,
                {},
                {}
            )

            test.eq(task_complete, true, "unconditional finish completes the task")
            test.eq((final_result :: any).answer, "done", "final result carries the finish arguments")
            test.eq(#recorded, 0, "success path does not record sibling tool observations")
        end)
    end)
end

return { run_tests = test.run_cases(define_tests) }
