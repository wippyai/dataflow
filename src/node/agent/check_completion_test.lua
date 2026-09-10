local test = require("test")
local agent_node = require("agent_node")
local agent_consts = require("agent_consts")

-- A minimal node_sdk double: records every n:data() call so tests can assert
-- on the observation check_completion writes back to the conversation.
local function make_recording_node()
    local recorded = {}
    local n = {
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

local function check(tool_calling, agent_result, opts)
    opts = opts or {}
    local check_completion = agent_node._test.check_completion
    local n, recorded = make_recording_node()
    local complete, final_result, feedback_recorded, unproductive = check_completion(
        tool_calling,
        agent_result,
        opts.iteration or 3,
        opts.min_iterations or 1,
        opts.exit_tool_name,
        n
    )
    test.eq(feedback_recorded, #recorded > 0, "feedback_recorded mirrors the queued observation")
    return complete, final_result, recorded, unproductive
end

local function define_tests()
    describe("check_completion: unstructured text results", function()
        it("exports check_completion for testing", function()
            test.not_nil(agent_node._test.check_completion, "check_completion exported for testing")
        end)

        it("completes tool_calling=none on non-empty text", function()
            local complete, final_result, recorded = check(agent_consts.TOOL_CALLING.NONE, { result = "final answer" })
            test.is_true(complete)
            test.eq(final_result, "final answer")
            test.eq(#recorded, 0)
        end)

        it("does not complete tool_calling=none on an empty string", function()
            local complete, final_result = check(agent_consts.TOOL_CALLING.NONE, { result = "" })
            test.is_false(complete)
            test.is_nil(final_result)
        end)

        it("does not complete tool_calling=none on whitespace", function()
            local complete = check(agent_consts.TOOL_CALLING.NONE, { result = " \n\t " })
            test.is_false(complete)
        end)

        it("does not complete tool_calling=none on a nil result", function()
            local complete = check(agent_consts.TOOL_CALLING.NONE, { result = nil })
            test.is_false(complete)
        end)

        it("completes tool_calling=none on a structured result", function()
            local complete, final_result = check(agent_consts.TOOL_CALLING.NONE, { result = { answer = 42 } })
            test.is_true(complete)
            test.eq((final_result :: any).answer, 42)
        end)

        it("does not complete tool_calling=none on an empty table", function()
            local complete, final_result = check(agent_consts.TOOL_CALLING.NONE, { result = {} })
            test.is_false(complete)
            test.is_nil(final_result)
        end)

        it("does not complete tool_calling=none on boolean false", function()
            local complete = check(agent_consts.TOOL_CALLING.NONE, { result = false })
            test.is_false(complete)
        end)

        it("completes tool_calling=none on a numeric result", function()
            local complete, final_result = check(agent_consts.TOOL_CALLING.NONE, { result = 0 })
            test.is_true(complete)
            test.eq(final_result, 0)
        end)
    end)

    describe("check_completion: tool_calling=auto", function()
        it("completes on non-empty text without tool calls", function()
            local complete, final_result, recorded = check(agent_consts.TOOL_CALLING.AUTO, { result = "final answer" })
            test.is_true(complete)
            test.eq(final_result, "final answer")
            test.eq(#recorded, 0)
        end)

        it("does not complete on an empty string and asks for a real answer", function()
            local complete, final_result, recorded = check(agent_consts.TOOL_CALLING.AUTO, { result = "" })
            test.is_false(complete)
            test.is_nil(final_result)
            test.eq(#recorded, 1)
            local row = recorded[1] :: any
            test.eq(row.data_type, agent_consts.DATA_TYPE.AGENT_OBSERVATION)
            test.eq(row.content, agent_consts.FEEDBACK.EMPTY_RESULT)
            test.eq(row.opts.key, "3_empty_result")
            test.eq(row.opts.metadata.iteration, 3)
        end)

        it("does not complete on whitespace and asks for a real answer", function()
            local complete, _, recorded = check(agent_consts.TOOL_CALLING.AUTO, { result = "  \n " })
            test.is_false(complete)
            test.eq(#recorded, 1)
            test.eq((recorded[1] :: any).content, agent_consts.FEEDBACK.EMPTY_RESULT)
        end)

        it("does not complete on a nil result and asks for tool use", function()
            local complete, _, recorded = check(agent_consts.TOOL_CALLING.AUTO, { result = nil })
            test.is_false(complete)
            test.eq(#recorded, 1)
            local row = recorded[1] :: any
            test.eq(row.content, agent_consts.FEEDBACK.NO_TOOLS_CALLED)
            test.eq(row.opts.key, "3_no_tools_called")
        end)

        it("does not complete on an empty structured result and asks for a real answer", function()
            local complete, _, recorded = check(agent_consts.TOOL_CALLING.AUTO, { result = {} })
            test.is_false(complete)
            test.eq(#recorded, 1)
            test.eq((recorded[1] :: any).content, agent_consts.FEEDBACK.EMPTY_RESULT)
        end)

        it("completes on a populated structured result", function()
            local complete, final_result = check(agent_consts.TOOL_CALLING.AUTO, { result = { summary = "done" } })
            test.is_true(complete)
            test.eq((final_result :: any).summary, "done")
        end)

        it("does not complete while tool calls are pending, without feedback", function()
            local complete, _, recorded = check(agent_consts.TOOL_CALLING.AUTO, {
                result = "",
                tool_calls = { { id = "call_1", name = "CreateArtifact", arguments = {} } }
            })
            test.is_false(complete)
            test.eq(#recorded, 0)
        end)

        it("does not complete on text while tool calls are pending", function()
            local complete = check(agent_consts.TOOL_CALLING.AUTO, {
                result = "partial",
                tool_calls = { { id = "call_1", name = "Search", arguments = {} } }
            })
            test.is_false(complete)
        end)

        it("does not complete below min_iterations", function()
            local complete, _, recorded = check(agent_consts.TOOL_CALLING.AUTO, { result = "final answer" },
                { iteration = 1, min_iterations = 2 })
            test.is_false(complete)
            test.eq(#recorded, 0)
        end)
    end)

    describe("check_completion: tool_calling=any", function()
        it("never completes on text alone and points at the exit tool", function()
            local complete, _, recorded = check(agent_consts.TOOL_CALLING.ANY, { result = "final answer" },
                { exit_tool_name = "Finish" })
            test.is_false(complete)
            test.eq(#recorded, 1)
            test.eq((recorded[1] :: any).content, agent_consts.FEEDBACK.NO_TOOLS_CALLED .. " " ..
                string.format(agent_consts.FEEDBACK.EXIT_AVAILABLE, "Finish"))
        end)
    end)

    describe("check_completion: unproductive turn reporting", function()
        it("reports an unusable tool_calling=none turn as unproductive", function()
            local _complete, _result, recorded, unproductive = check(agent_consts.TOOL_CALLING.NONE, { result = "" })
            test.eq(#recorded, 0, "none writes no feedback observation")
            test.is_true(unproductive)
        end)

        it("does not report a completed tool_calling=none turn as unproductive", function()
            local complete, _result, _recorded, unproductive = check(agent_consts.TOOL_CALLING.NONE,
                { result = "final answer" })
            test.is_true(complete)
            test.is_false(unproductive)
        end)

        it("reports an empty tool_calling=auto turn as unproductive", function()
            local _complete, _result, recorded, unproductive = check(agent_consts.TOOL_CALLING.AUTO, { result = "" })
            test.eq(#recorded, 1)
            test.is_true(unproductive)
        end)

        it("does not report a tool-calling turn as unproductive", function()
            local _complete, _result, _recorded, unproductive = check(agent_consts.TOOL_CALLING.AUTO, {
                result = "",
                tool_calls = { { id = "1", name = "some_tool" } },
            })
            test.is_false(unproductive)
        end)

        it("does not report a warm-up turn below min_iterations as unproductive", function()
            local _complete, _result, _recorded, unproductive = check(agent_consts.TOOL_CALLING.AUTO,
                { result = "" }, { iteration = 1, min_iterations = 3 })
            test.is_false(unproductive)
        end)
    end)
end

return { run_tests = test.run_cases(define_tests) }
