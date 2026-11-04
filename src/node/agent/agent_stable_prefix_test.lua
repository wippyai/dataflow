local test = require("test")
local json = require("json")
local uuid = require("uuid")
local time = require("time")
local client = require("client")
local consts = require("consts")
local data_reader = require("data_reader")
local node_reader = require("node_reader")
local agent_consts = require("agent_consts")
local recovery_helpers = require("recovery_helpers")
local run_context = require("run_context")
local funcs = require("funcs")
local claude_mapper = require("claude_mapper")

local STUB = "userspace.dataflow.node.agent.stub:"
local AGENT = STUB .. "recovery_test_agent"
local TIME_AGENT = STUB .. "stable_prefix_time_agent"
local TARGET_AGENT = STUB .. "stable_prefix_switch_target_agent"
local TURN_NOTE_PREFIX = "The current date and time is "

local function wait_terminal(c, dataflow_id)
    for _ = 1, 250 do
        local status = c:get_status(dataflow_id)
        if status == consts.STATUS.COMPLETED_SUCCESS or status == consts.STATUS.COMPLETED_FAILURE then
            return status
        end
        time.sleep("100ms")
    end
    return nil
end

type RunOptions = {
    agent: string?,
    traits: {string}?,
    input: table?,
    extra_inputs: {string}?,
    extra_commands: ((string, string) -> {table})?,
    arena: table?,
    checkpoint: table?,
    node_metadata: table?,
    max_iterations: number?,
    no_input: boolean?,
    named_inputs: {[string]: any}?,
    config: table?,
}

-- Runs an agent node to its end. Every request the model receives is recorded.
local function run_node(options: RunOptions)
    local c = client.new()
    local node_id = uuid.v7()
    local input_id = uuid.v7()
    local scenario_id = "stable-prefix-" .. uuid.v7()
    local input = { scenario_id = scenario_id, capture_requests = true, mode = "thinking_rounds", rounds = 2 }
    for key, value in pairs(options.input or {}) do
        input[key] = value
    end

    local commands = {
        {
            type = consts.COMMAND_TYPES.CREATE_NODE,
            payload = {
                node_id = node_id,
                node_type = "userspace.dataflow.node.agent:node",
                status = consts.STATUS.PENDING,
                metadata = options.node_metadata,
                config = {
                    agent = options.agent or AGENT,
                    active_traits = options.traits,
                    checkpoint = options.checkpoint,
                    model = options.config and options.config.model,
                    show_tool_calls = false,
                    data_targets = {
                        { data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "result",
                            content_type = consts.CONTENT_TYPE.TEXT },
                    },
                    arena = {
                        prompt = "Run the stable prefix scenario.",
                        max_iterations = options.max_iterations or 8,
                        tool_calling = "auto",
                        tools = { STUB .. "recovery_tool", STUB .. "recovery_control_tool" },
                        exit_schema = options.arena and options.arena.exit_schema,
                        exit_func_id = options.arena and options.arena.exit_func_id,
                    },
                },
            },
        },
        {
            type = consts.COMMAND_TYPES.CREATE_DATA,
            payload = {
                data_id = input_id,
                data_type = consts.DATA_TYPE.WORKFLOW_INPUT,
                content = input,
                content_type = consts.CONTENT_TYPE.JSON,
            },
        },
        {
            type = consts.COMMAND_TYPES.CREATE_DATA,
            payload = {
                data_id = uuid.v7(),
                data_type = consts.DATA_TYPE.NODE_INPUT,
                node_id = node_id,
                key = input_id,
                content = "",
                content_type = consts.CONTENT_TYPE.REFERENCE,
            },
        },
    }
    if options.no_input then
        table.remove(commands)
    end
    for _, key in ipairs(options.extra_inputs or {}) do
        table.insert(commands, {
            type = consts.COMMAND_TYPES.CREATE_DATA,
            payload = {
                data_id = uuid.v7(),
                data_type = consts.DATA_TYPE.NODE_INPUT,
                node_id = node_id,
                key = key,
                content = input,
                content_type = consts.CONTENT_TYPE.JSON,
            },
        })
    end
    for key, content in pairs(options.named_inputs or {}) do
        table.insert(commands, {
            type = consts.COMMAND_TYPES.CREATE_DATA,
            payload = {
                data_id = uuid.v7(),
                data_type = consts.DATA_TYPE.NODE_INPUT,
                node_id = node_id,
                key = key,
                content = content,
                content_type = consts.CONTENT_TYPE.TEXT,
            },
        })
    end
    if options.extra_commands then
        for _, command in ipairs(options.extra_commands(node_id, scenario_id)) do
            table.insert(commands, command)
        end
    end

    local dataflow_id, create_err = c:create_workflow(commands)
    test.is_nil(create_err, "create")
    local _, start_err = c:start(dataflow_id)
    test.is_nil(start_err, "start")
    local status = wait_terminal(c, dataflow_id)

    return {
        status = status,
        dataflow_id = dataflow_id,
        node_id = node_id,
        scenario_id = scenario_id,
    }
end

local function requests_of(run)
    local requests = {}
    while true do
        local request = recovery_helpers.get_request(run.scenario_id, #requests + 1)
        if not request then
            return requests
        end
        requests[#requests + 1] = request
    end
end

local function text_of(message)
    local parts = {}
    for _, part in ipairs(message.content or {}) do
        if part.text then parts[#parts + 1] = part.text end
    end
    return table.concat(parts, "")
end

local without_markers = recovery_helpers.conversation
local signature_of = recovery_helpers.signature

local function assert_extends(earlier, later, label)
    test.is_nil(recovery_helpers.extension_error(earlier, later), label)
end

local function assert_paired(requests)
    for index, request in ipairs(requests) do
        test.is_nil(recovery_helpers.pairing_error(request), "request " .. tostring(index))
    end
end

local function assert_each_request_extends_the_last(requests)
    for index = 2, #requests do
        assert_extends(requests[index - 1], requests[index], "request " .. tostring(index))
    end
end

local function rows_of(run, data_type)
    local rows = data_reader.with_dataflow(run.dataflow_id)
        :with_nodes(run.node_id)
        :with_data_types(data_type)
        :all()
    table.sort(rows, function(a, b) return tostring(a.data_id) < tostring(b.data_id) end)
    return rows
end

local function find_message(request, predicate)
    for index, message in ipairs(without_markers(request)) do
        if predicate(message) then
            return index, message
        end
    end
    return nil, nil
end

local function with_text(needle)
    return function(message)
        return string.find(text_of(message), needle, 1, true) ~= nil
    end
end

local function user_index(request)
    local index = find_message(request, function(message) return message.role == "user" end)
    return index or 0
end

local function block_texts(request)
    local seen, order = {}, {}
    for _, message in ipairs(request) do
        local metadata = message.metadata or {}
        for _, block in ipairs(metadata.thinking_blocks or {}) do
            if not seen[block.thinking] then
                seen[block.thinking] = true
                order[#order + 1] = block.thinking
            end
        end
    end
    table.sort(order)
    return table.concat(order, ",")
end

local function node_row(run)
    return node_reader.with_dataflow(run.dataflow_id):with_nodes(run.node_id):one()
end

local function data_row(node_id, data_type, key, content, metadata)
    return {
        type = consts.COMMAND_TYPES.CREATE_DATA,
        payload = {
            data_id = uuid.v7(),
            data_type = data_type,
            node_id = node_id,
            key = key,
            content = content,
            content_type = type(content) == "table" and consts.CONTENT_TYPE.JSON or consts.CONTENT_TYPE.TEXT,
            metadata = metadata,
        },
    }
end

local function node_error(run)
    local node = node_reader.with_dataflow(run.dataflow_id):with_nodes(run.node_id):one()
    local encoded = json.encode(node and node.metadata and node.metadata.error or {})
    return encoded
end

local function define_tests()
    describe("recalled memory notes", function()
        it("stores each note before its action and replays it byte-identically", function()
            local run = run_node({ traits = { STUB .. "recall_note_trait" }, input = { rounds = 1 } })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS)

            local notes = rows_of(run, agent_consts.DATA_TYPE.AGENT_MEMORY)
            local actions = rows_of(run, agent_consts.DATA_TYPE.AGENT_ACTION)
            test.eq(#actions, 2)
            test.eq(#notes, 2, "one note per step")
            for index, note in ipairs(notes) do
                test.is_true(tostring(note.data_id) < tostring(actions[index].data_id), "note precedes its action")
                test.eq(note.metadata.llm_meta.preserve_position, true)
                test.eq(note.metadata.llm_meta.memory_ids[1], "m" .. tostring(index))
            end

            local requests = requests_of(run)
            test.eq(#requests, 2)
            assert_each_request_extends_the_last(requests)

            local first = without_markers(requests[1])
            local sent = first[#first]
            test.eq(sent.role, "developer")
            test.is_true(string.find(text_of(sent), "recalled m1", 1, true) ~= nil, "the first step sends m1")

            local index, replayed = find_message(requests[2], with_text("recalled m1"))
            test.eq(index, #first, "the note stays where it was sent")
            test.eq(signature_of(replayed), signature_of(sent))
            test.eq(replayed.metadata.preserve_position, true)
            test.eq(replayed.metadata.memory_ids[1], "m1")

            local second = without_markers(requests[2])
            test.is_true(string.find(text_of(second[#second]), "recalled m2", 1, true) ~= nil,
                "the stored ids reach the next recall")
        end)

        it("keeps the note of a truncated step", function()
            local run = run_node({ traits = { STUB .. "recall_note_trait" }, input = { mode = "truncated_then_final" } })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS)

            local notes = rows_of(run, agent_consts.DATA_TYPE.AGENT_MEMORY)
            local actions = rows_of(run, agent_consts.DATA_TYPE.AGENT_ACTION)
            test.eq(#notes, 2)
            test.is_true(tostring(notes[1].data_id) < tostring(actions[1].data_id))
            assert_each_request_extends_the_last(requests_of(run))
        end)
    end)

    describe("turn context", function()
        it("stores one positional note for the input, none per iteration", function()
            local run = run_node({ agent = TIME_AGENT, input = { rounds = 3 } })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS)

            local notes = rows_of(run, agent_consts.DATA_TYPE.AGENT_MEMORY)
            test.eq(#notes, 1, "one note over four iterations")
            test.eq(notes[1].metadata.llm_meta.preserve_position, true)
            local actions = rows_of(run, agent_consts.DATA_TYPE.AGENT_ACTION)
            test.is_true(tostring(notes[1].data_id) < tostring(actions[1].data_id))

            local requests = requests_of(run)
            test.eq(#requests, 4)
            assert_each_request_extends_the_last(requests)
            for _, request in ipairs(requests) do
                local index, note = find_message(request, with_text(TURN_NOTE_PREFIX))
                test.eq(index, user_index(request) + 1, "the note follows the input")
                test.eq(note.role, "system")
                test.eq(note.metadata.preserve_position, true)
                test.eq(text_of(note), notes[1].content)
            end
        end)

        it("stores one note for a user message folded from several inputs", function()
            local run = run_node({ agent = TIME_AGENT, input = { rounds = 1 }, extra_inputs = { "second" } })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS)
            test.eq(#rows_of(run, agent_consts.DATA_TYPE.AGENT_MEMORY), 1)
        end)

        it("stores the note for a node whose inputs hold no conversation", function()
            local run = run_node({ agent = TIME_AGENT, no_input = true, extra_inputs = { "context" } })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS)
            test.eq(#rows_of(run, agent_consts.DATA_TYPE.AGENT_MEMORY), 1)
        end)

        it("fails the node when the note cannot render", function()
            local run = run_node({ agent = STUB .. "stable_prefix_bad_time_agent" })
            test.eq(run.status, consts.STATUS.COMPLETED_FAILURE)
            test.is_true(string.find(node_error(run), "turn context", 1, true) ~= nil, node_error(run))
            test.is_true(string.find(node_error(run), "Not/AZone", 1, true) ~= nil, node_error(run))
            test.eq(#requests_of(run), 0, "no request goes out without the note")
        end)

        it("replays a stored note instead of rendering another", function()
            local stored = TURN_NOTE_PREFIX .. "a stored minute."
            local run = run_node({
                agent = TIME_AGENT,
                input = { rounds = 1 },
                extra_commands = function(node_id)
                    return { {
                        type = consts.COMMAND_TYPES.CREATE_DATA,
                        payload = {
                            data_id = uuid.v7(),
                            data_type = agent_consts.DATA_TYPE.AGENT_MEMORY,
                            node_id = node_id,
                            key = "turn_context",
                            content = stored,
                            content_type = consts.CONTENT_TYPE.TEXT,
                            metadata = { turn_context = true, role = "system", llm_meta = { preserve_position = true } },
                        },
                    } }
                end,
            })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS)
            test.eq(#rows_of(run, agent_consts.DATA_TYPE.AGENT_MEMORY), 1)
            local requests = requests_of(run)
            local _, note = find_message(requests[1], with_text(TURN_NOTE_PREFIX))
            test.eq(text_of(note), stored)
        end)

        it("adds no note to a run that sent its input before the note existed", function()
            local run = run_node({
                agent = TIME_AGENT,
                input = { rounds = 1 },
                node_metadata = { state = { current_iteration = 1 } },
            })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS)
            test.eq(#rows_of(run, agent_consts.DATA_TYPE.AGENT_MEMORY), 0)
        end)

        it("keeps the note after the input across a checkpoint", function()
            local run = run_node({
                agent = TIME_AGENT,
                input = { rounds = 3, prompt_tokens = 500 },
                checkpoint = { token_threshold = 100, function_id = STUB .. "checkpoint_summarizer" },
            })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS)

            local markers = 0
            for _, row in ipairs(rows_of(run, agent_consts.DATA_TYPE.AGENT_MEMORY)) do
                if row.metadata.checkpoint_marker == true then markers = markers + 1 end
            end
            test.gt(markers, 0, "the scenario checkpoints")
            test.eq(recovery_helpers.get_metric(run.scenario_id, "checkpoint_saw_turn_context", 0), 0,
                "the summarizer receives only rows the checkpoint removes")

            for _, request in ipairs(requests_of(run)) do
                local index = find_message(request, with_text(TURN_NOTE_PREFIX))
                test.eq(index, user_index(request) + 1, "the note follows the input")
            end

            local history, history_err = run_context.get_history({
                host = { dataflow_id = run.dataflow_id, node_id = run.node_id },
            })
            test.is_nil(history_err)
            local found = false
            for _, event in ipairs(history.events or {}) do
                if string.find(tostring(event.raw_content), TURN_NOTE_PREFIX, 1, true) then found = true end
            end
            test.is_true(found, "the run context keeps the note too")
        end)
    end)

    describe("thinking blocks", function()
        it("stores the fingerprint next to the blocks", function()
            local run = run_node({ input = { rounds = 1 } })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS)
            for _, action in ipairs(rows_of(run, agent_consts.DATA_TYPE.AGENT_ACTION)) do
                test.not_nil(action.metadata.llm_meta.thinking_blocks)
                test.eq(type(action.metadata.llm_meta.prefix_fingerprint), "string")
                test.gt(#action.metadata.llm_meta.prefix_fingerprint, 0)
            end
        end)

        it("keeps earlier blocks while the fingerprint is unchanged", function()
            local run = run_node({ input = { rounds = 3 } })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS)
            local requests = requests_of(run)
            test.eq(block_texts(requests[2]), "block-1")
            test.eq(block_texts(requests[3]), "block-1,block-2")
            test.eq(block_texts(requests[4]), "block-1,block-2,block-3")
        end)

        it("strips earlier blocks after an agent switch and keeps the current tool round", function()
            local run = run_node({ input = { rounds = 2, switch_at = 1, switch_agent = TARGET_AGENT } })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS)

            local actions = rows_of(run, agent_consts.DATA_TYPE.AGENT_ACTION)
            test.eq(#actions, 3)
            test.is_true(actions[1].metadata.llm_meta.prefix_fingerprint ~= actions[2].metadata.llm_meta.prefix_fingerprint,
                "the switch changes the fingerprint")
            test.eq(actions[2].metadata.llm_meta.prefix_fingerprint, actions[3].metadata.llm_meta.prefix_fingerprint)

            local requests = requests_of(run)
            test.eq(block_texts(requests[2]), "block-1", "the round in progress keeps its blocks")
            test.eq(block_texts(requests[3]), "block-2", "the earlier agent's blocks are gone")
            test.not_nil(actions[1].metadata.llm_meta.thinking_blocks, "the stored row keeps its blocks")
        end)
    end)

    describe("checkpoints", function()
        local CHECKPOINT = { token_threshold = 100, function_id = STUB .. "checkpoint_summarizer" }

        it("sends no result without its call after a cut and summarizes the results", function()
            local run = run_node({ input = { rounds = 4, checkpoint_at = 2 }, checkpoint = CHECKPOINT })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS)
            local requests = requests_of(run)
            test.eq(#requests, 5)
            assert_paired(requests)
            test.is_nil(find_message(requests[3], function(message) return message.role == "function_result" end),
                "the first request after the cut carries no earlier result")
            test.eq(tonumber(recovery_helpers.get_metric(run.scenario_id, "checkpoint_result_rows", 0)), 2)
        end)

        it("extends and keeps thinking in the requests after a cut", function()
            local run = run_node({ input = { rounds = 4, checkpoint_at = 2 }, checkpoint = CHECKPOINT })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS)
            local requests = requests_of(run)
            test.eq(#requests, 5)
            test.is_nil(recovery_helpers.prefix_error(requests[1], requests[2]))
            assert_extends(requests[3], requests[4], "the second request after the cut")
            assert_extends(requests[4], requests[5], "the third request after the cut")
            test.eq(block_texts(requests[3]), "")
            test.eq(block_texts(requests[4]), "block-3")
            test.eq(block_texts(requests[5]), "block-3,block-4")
        end)

        it("summarizes the previous summary and the rows after its cut, nothing older", function()
            local run = run_node({ input = { rounds = 5, checkpoint_at = { 2, 4 } }, checkpoint = CHECKPOINT })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS, node_error(run))

            local markers = {}
            for _, row in ipairs(rows_of(run, agent_consts.DATA_TYPE.AGENT_MEMORY)) do
                if row.metadata.checkpoint_marker == true then markers[#markers + 1] = row end
            end
            test.eq(#markers, 2)

            local first = recovery_helpers.get_checkpoint_history(run.scenario_id, 1)
            test.eq(#first, 4, "the first summary replaces two rounds")

            local second = recovery_helpers.get_checkpoint_history(run.scenario_id, 2)
            local cut = markers[1].metadata.checkpoint_before_data_id
            test.eq(second[1].data_id, markers[1].data_id, "the first summary comes first")
            test.eq(second[1].content, markers[1].content, "with its summary text")
            local results = 0
            for index = 2, #second do
                test.is_true(second[index].data_id > cut, "no row before the first cut: " .. second[index].type)
                if type((second[index].metadata or {}).tool_call_id) == "string" then results = results + 1 end
            end
            test.eq(#second, 5, "the summary plus the two rounds after its cut")
            test.eq(results, 2, "the results after the cut are summarized")
            test.eq(markers[2].metadata.checkpoint_history_count, 5)
            test.eq(markers[2].metadata.checkpoint_first_data_id, markers[1].data_id)
        end)

        it("reaches the default summarizer's model with the whole earlier summary", function()
            local node_id = uuid.v7()
            local summary = string.rep("summary ", 800) .. "SUMMARY-TAIL"
            local result_text = string.rep("result ", 800) .. "RESULT-TAIL"
            local result, call_err = funcs.new():call("userspace.dataflow.node.agent:checkpoint", {
                dataflow_id = uuid.v7(),
                node_id = node_id,
                options = { model = "recovery-test-model" },
                history = {
                    { data_id = "1", type = agent_consts.DATA_TYPE.AGENT_MEMORY, content = summary,
                        metadata = { checkpoint_marker = true } },
                    { data_id = "2", type = agent_consts.DATA_TYPE.AGENT_OBSERVATION, content = result_text,
                        metadata = { tool_call_id = "call", tool_name = "tool" } },
                },
            })
            test.is_nil(call_err)
            test.not_nil(result)

            local request = recovery_helpers.get_request(node_id, 1)
            test.not_nil(request, "the model received the summarizer's request")
            local _, user = find_message(request, function(message) return message.role == "user" end)
            test.is_true(string.find(text_of(user), summary, 1, true) ~= nil, "the summary arrives whole, tail included")
            test.is_true(string.find(text_of(user), "RESULT-TAIL", 1, true) == nil,
                "the row clamp still applies to other rows")
            local _, system = find_message(request, function(message) return message.role == "system" end)
            test.is_true(string.find(text_of(system), "Merge", 1, true) ~= nil, "the prompt asks to merge")
        end)

        it("does not cut through a finish that has no result yet", function()
            local finish_key = "2_final"
            local run = run_node({
                input = { rounds = 0 },
                arena = { exit_schema = { type = "object", properties = { answer = { type = "string" } } } },
                checkpoint = CHECKPOINT,
                node_metadata = { checkpoint_requested = true,
                    state = { current_iteration = 2, agent_id = AGENT } },
                extra_commands = function(node_id, scenario_id)
                    return {
                        data_row(node_id, agent_consts.DATA_TYPE.AGENT_ACTION, "1_action", {
                            result = "working",
                            tool_calls = { { id = "stored-call", name = "recovery_tool",
                                arguments = { scenario_id = scenario_id, step = 1 } } },
                        }, { iteration = 1, finish_reason = "tool_call" }),
                        data_row(node_id, agent_consts.DATA_TYPE.AGENT_OBSERVATION, "1_recovery_tool", { ok = true }, {
                            iteration = 1, tool_call_id = "stored-call", tool_name = "recovery_tool",
                        }),
                        data_row(node_id, agent_consts.DATA_TYPE.AGENT_ACTION, finish_key, {
                            result = "",
                            tool_calls = { { id = "stored-finish", name = "finish", arguments = { answer = "done" } } },
                        }, { iteration = 2, finish_reason = "tool_call" }),
                    }
                end,
            })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS, node_error(run))

            local marker
            for _, row in ipairs(rows_of(run, agent_consts.DATA_TYPE.AGENT_MEMORY)) do
                if row.metadata.checkpoint_marker == true then marker = row end
            end
            test.not_nil(marker, "the requested compaction ran")
            local finish
            for _, row in ipairs(rows_of(run, agent_consts.DATA_TYPE.AGENT_ACTION)) do
                if row.key == finish_key then finish = row end
            end
            test.is_true(marker.metadata.checkpoint_before_data_id < finish.data_id,
                "the cut stops before the open finish")
            for _, row in ipairs(recovery_helpers.get_checkpoint_history(run.scenario_id, 1)) do
                test.is_true(row.data_id ~= finish.data_id, "the summary does not replace the open finish")
            end
        end)

        it("sends a call and its result when a recovered finish is rejected after a cut", function()
            local run = run_node({
                input = { rounds = 0 },
                arena = {
                    exit_schema = { type = "object", properties = { answer = { type = "string" } } },
                    exit_func_id = STUB .. "exit_reject_validator",
                },
                node_metadata = { state = { current_iteration = 2, agent_id = AGENT } },
                extra_commands = function(node_id)
                    local earlier = data_row(node_id, agent_consts.DATA_TYPE.AGENT_OBSERVATION, "1_note", "earlier", {})
                    return {
                        earlier,
                        data_row(node_id, agent_consts.DATA_TYPE.AGENT_ACTION, "2_final", {
                            result = "",
                            tool_calls = { { id = "stored-finish", name = "finish", arguments = { answer = "done" } } },
                        }, { iteration = 2, finish_reason = "tool_call" }),
                        data_row(node_id, agent_consts.DATA_TYPE.AGENT_MEMORY, "1_checkpoint", "the summary", {
                            iteration = 1, checkpoint_marker = true,
                            checkpoint_before_data_id = earlier.payload.data_id,
                        }),
                    }
                end,
            })
            local requests = requests_of(run)
            test.gt(#requests, 0, node_error(run))
            assert_paired(requests)
            local seen_use, seen_result = false, false
            for _, turn in ipairs(claude_mapper.map_messages(requests[1]).messages) do
                for _, block in ipairs(turn.content) do
                    if block.type == "tool_use" then seen_use = true end
                    if block.type == "tool_result" then
                        test.is_true(seen_use, "tool_result follows its tool_use on the wire")
                        seen_result = true
                    end
                end
            end
            test.is_true(seen_result, "the rejection reaches the model")
        end)

        it("gives the run context the same rows after the cut", function()
            local run = run_node({ input = { rounds = 5, checkpoint_at = { 2, 4 } }, checkpoint = CHECKPOINT })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS, node_error(run))
            local markers = {}
            for _, row in ipairs(rows_of(run, agent_consts.DATA_TYPE.AGENT_MEMORY)) do
                if row.metadata.checkpoint_marker == true then markers[#markers + 1] = row end
            end
            local history = run_context.get_history({
                host = { dataflow_id = run.dataflow_id, node_id = run.node_id },
            })
            local cut = markers[2].metadata.checkpoint_before_data_id
            test.eq(history.events[1].role, "user", "the selector starts with the pinned input")
            test.eq(history.events[2].id, markers[2].data_id, "then the latest summary")
            for index = 3, #history.events do
                test.is_true(history.events[index].id > cut, "the selector holds no row before the cut")
            end
        end)

        it("leaves a fresh summary alone when nothing followed it", function()
            local run = run_node({
                traits = { STUB .. "behavior_compaction_trait" },
                input = { rounds = 1, prompt_tokens = 500 },
                checkpoint = CHECKPOINT,
            })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS, node_error(run))
            local calls = tonumber(recovery_helpers.get_metric(run.scenario_id, "checkpoint_calls", 0)) or 0
            test.gt(calls, 0, "the scenario checkpoints")
            for number = 1, calls do
                local history = recovery_helpers.get_checkpoint_history(run.scenario_id, number)
                test.gt(#history, 1, "checkpoint " .. tostring(number) .. " has rows to replace")
            end
        end)

        it("closes a round of parallel calls before it cuts", function()
            local run = run_node({
                input = { mode = "two_tools_one_turn", prompt_tokens = 500 },
                checkpoint = CHECKPOINT,
            })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS, node_error(run))
            local markers = 0
            for _, row in ipairs(rows_of(run, agent_consts.DATA_TYPE.AGENT_MEMORY)) do
                if row.metadata.checkpoint_marker == true then markers = markers + 1 end
            end
            test.gt(markers, 0, "the scenario checkpoints")
            assert_paired(requests_of(run))
        end)

        it("closes a delegation before it cuts and summarizes the delegate's answer", function()
            local run = run_node({
                input = { rounds = 2, delegate_at = 1, delegate_to = AGENT, checkpoint_at = 1 },
                checkpoint = CHECKPOINT,
            })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS, node_error(run))
            assert_paired(requests_of(run))
            test.eq(tonumber(recovery_helpers.get_metric(run.scenario_id, "checkpoint_result_rows", 0)), 1)
            local content = tostring(recovery_helpers.get_metric(run.scenario_id, "checkpoint_last_result_content", ""))
            test.is_true(string.find(content, "final:text_final", 1, true) ~= nil, content)
        end)

        it("keeps a skipped checkpoint's audit row out of the conversation", function()
            local run = run_node({
                input = { rounds = 2, prompt_tokens = 500 },
                checkpoint = { token_threshold = 100, function_id = STUB .. "no_such_summarizer" },
            })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS)

            local skipped = 0
            for _, row in ipairs(rows_of(run, agent_consts.DATA_TYPE.AGENT_OBSERVATION)) do
                if row.metadata.checkpoint_skipped == true then skipped = skipped + 1 end
            end
            test.gt(skipped, 0, "the audit row exists")

            local requests = requests_of(run)
            test.eq(#requests, 3)
            assert_each_request_extends_the_last(requests)

            local history = run_context.get_history({
                host = { dataflow_id = run.dataflow_id, node_id = run.node_id },
            })
            for _, event in ipairs(history.events or {}) do
                test.not_nil(event.role, "each history event is a conversation message")
            end
        end)
    end)

    describe("agent selection", function()
        local TARGET_MODEL = "recovery-auto-test-model"
        local INPUT_MODEL = "recovery-test-model"

        it("applies input overrides on the first start and persists them", function()
            local run = run_node({
                input = { rounds = 1 },
                named_inputs = { agent_id = TIME_AGENT, model = INPUT_MODEL },
            })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS, node_error(run))
            local node = node_row(run)
            test.eq(node.config.agent, TIME_AGENT)
            test.eq(node.config.model, INPUT_MODEL)
        end)

        it("keeps a persisted selection over the inputs on a restart", function()
            local run = run_node({
                agent = TARGET_AGENT,
                config = { model = TARGET_MODEL },
                input = { rounds = 1 },
                node_metadata = { state = { current_iteration = 1, agent_id = TARGET_AGENT, model = TARGET_MODEL } },
                named_inputs = { agent_id = AGENT, model = INPUT_MODEL },
                extra_commands = function(node_id, scenario_id)
                    return { data_row(node_id, agent_consts.DATA_TYPE.AGENT_INPUT, "conversation_input",
                        json.encode({ scenario_id = scenario_id, capture_requests = true,
                            mode = "thinking_rounds", rounds = 1 })) }
                end,
            })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS, node_error(run))
            local action = rows_of(run, agent_consts.DATA_TYPE.AGENT_ACTION)[1]
            test.eq(action.metadata.agent_id, TARGET_AGENT)
            test.eq(action.metadata.model, TARGET_MODEL)
        end)

        it("keeps the input model of a run stored before the selection was persisted", function()
            local run = run_node({
                agent = TARGET_AGENT,
                config = { model = INPUT_MODEL },
                input = { rounds = 1 },
                node_metadata = { state = { current_iteration = 1, agent_id = TARGET_AGENT, model = TARGET_MODEL } },
                named_inputs = { model = TARGET_MODEL },
            })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS, node_error(run))
            local action = rows_of(run, agent_consts.DATA_TYPE.AGENT_ACTION)[1]
            test.eq(action.metadata.model, TARGET_MODEL, "the template model does not take over")
            test.eq(node_row(run).config.model, TARGET_MODEL, "the next start finds the same model")
        end)
    end)

    describe("iteration warnings", function()
        it("stays out of the space between a call and a delegate's result", function()
            local run = run_node({
                input = { rounds = 1, delegate_at = 1, delegate_to = AGENT },
                max_iterations = 3,
            })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS, node_error(run))
            assert_paired(requests_of(run))
            local warned = false
            for _, row in ipairs(rows_of(run, agent_consts.DATA_TYPE.AGENT_OBSERVATION)) do
                if row.metadata.remaining_iterations == 2 then warned = true end
            end
            test.is_true(warned, "the warning is written")
        end)
    end)

    describe("conversation input", function()
        it("stores the fold once", function()
            local run = run_node({ input = { rounds = 2 } })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS)
            local rows = rows_of(run, agent_consts.DATA_TYPE.AGENT_INPUT)
            test.eq(#rows, 1)
            local _, user = find_message(requests_of(run)[1], function(message) return message.role == "user" end)
            test.eq(rows[1].content, text_of(user))
        end)

        it("replays the stored fold when an input arrives later", function()
            local scenario = { scenario_id = "stable-prefix-fold-" .. uuid.v7(), capture_requests = true,
                mode = "thinking_rounds", rounds = 1 }
            local stored = '<input key="default">\n' .. json.encode(scenario) .. '\n</input>'
            local run = run_node({
                input = scenario,
                extra_inputs = { "arrived_later" },
                node_metadata = { state = { current_iteration = 1 } },
                extra_commands = function(node_id)
                    return { data_row(node_id, agent_consts.DATA_TYPE.AGENT_INPUT, "conversation_input", stored) }
                end,
            })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS, node_error(run))
            run.scenario_id = scenario.scenario_id
            local _, user = find_message(requests_of(run)[1], function(message) return message.role == "user" end)
            test.eq(text_of(user), stored)
            test.eq(#rows_of(run, agent_consts.DATA_TYPE.AGENT_INPUT), 1)
        end)

        it("shows the run context the pinned input, not a later one", function()
            local scenario = { scenario_id = "stable-prefix-pin-" .. uuid.v7(), capture_requests = true,
                mode = "thinking_rounds", rounds = 1 }
            local stored = '<input key="default">\n' .. json.encode(scenario) .. '\n</input>'
            local run = run_node({
                input = scenario,
                extra_inputs = { "arrived_later" },
                node_metadata = { state = { current_iteration = 1 } },
                extra_commands = function(node_id)
                    return { data_row(node_id, agent_consts.DATA_TYPE.AGENT_INPUT, "conversation_input", stored) }
                end,
            })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS, node_error(run))

            local host = { dataflow_id = run.dataflow_id, node_id = run.node_id }
            local history = run_context.get_history({ host = host })
            local users = {}
            for _, event in ipairs(history.events) do
                if event.role == "user" then users[#users + 1] = event end
            end
            test.eq(#users, 1, "one user message, as in the request")
            test.eq(users[1].raw_content, stored)

            local built = run_context.get_prompt({ host = host })
            local _, prompt_user = find_message(built.messages, function(message) return message.role == "user" end)
            test.eq(text_of(prompt_user), stored)
        end)

        it("resumes a run stored before the fold and before fingerprints", function()
            local scenario_action = { result = "Requesting tool step 0", tool_calls = {
                { id = "stored-call", name = "recovery_tool", arguments = { scenario_id = "stored", step = 0 } },
            } }
            local run = run_node({
                agent = TIME_AGENT,
                input = { rounds = 1 },
                node_metadata = { state = { current_iteration = 1 } },
                extra_commands = function(node_id)
                    return {
                        data_row(node_id, agent_consts.DATA_TYPE.AGENT_ACTION, "1_action", scenario_action, {
                            iteration = 1,
                            finish_reason = "tool_call",
                            llm_meta = { thinking_blocks = {
                                { type = "thinking", thinking = "stored-block", signature = "signature" },
                            } },
                        }),
                        data_row(node_id, agent_consts.DATA_TYPE.AGENT_OBSERVATION, "1_recovery_tool", { ok = true }, {
                            iteration = 1, tool_call_id = "stored-call", tool_name = "recovery_tool",
                        }),
                    }
                end,
            })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS, node_error(run))
            test.eq(#rows_of(run, agent_consts.DATA_TYPE.AGENT_MEMORY), 0, "no turn note is added after the fact")
            test.eq(#rows_of(run, agent_consts.DATA_TYPE.AGENT_INPUT), 1, "the fold is pinned from here on")
            local requests = requests_of(run)
            test.eq(#requests, 2)
            test.eq(block_texts(requests[1]), "stored-block", "the round in progress keeps its blocks")
            test.eq(block_texts(requests[2]), "block-1", "blocks with no fingerprint are stale after it")
            assert_extends(requests[1], requests[2], "the resumed request")
        end)
    end)

    describe("feedback rows", function()
        it("stay separate messages across an empty turn", function()
            local run = run_node({ input = { mode = "empty_until_limit" }, max_iterations = 8 })
            local requests = requests_of(run)
            test.gt(#requests, 2, "the scenario repeats empty turns")
            for index = 2, #requests do
                test.is_nil(recovery_helpers.prefix_error(requests[index - 1], requests[index]),
                    "request " .. tostring(index))
            end
        end)
    end)

    describe("memory directives", function()
        it("adds memory after the round's results", function()
            local run = run_node({ input = { rounds = 2, memory_at = 1 } })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS, node_error(run))
            local requests = requests_of(run)
            assert_paired(requests)
            test.not_nil(find_message(requests[2], with_text("added at 1")), "the model reads the added memory")
            assert_each_request_extends_the_last(requests)
        end)
    end)

    describe("lifecycle notes", function()
        it("keeps a note at the position it was sent", function()
            local run = run_node({ traits = { STUB .. "lifecycle_notes_trait" }, input = { rounds = 2 } })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS)

            local requests = requests_of(run)
            test.eq(#requests, 3)
            assert_each_request_extends_the_last(requests)

            local first = without_markers(requests[1])
            local index, note = find_message(requests[1], with_text("note:before_step:1:system"))
            test.eq(index, #first, "sent after the history")
            test.eq(note.role, "system")
            test.eq(note.metadata.preserve_position, true, "a system note is positional, not hoisted")
            local later_index = find_message(requests[3], with_text("note:before_step:1:system"))
            test.eq(later_index, index)

            for _, request in ipairs(requests) do
                local activation = 0
                for _, message in ipairs(request) do
                    if string.find(text_of(message), "note:activate:", 1, true) then activation = activation + 1 end
                end
                test.eq(activation, 1, "the activation note is stored once")
            end

            local stored = rows_of(run, agent_consts.DATA_TYPE.AGENT_MEMORY)
            test.eq(#stored, 1 + 2 * 3, "one activation note and two notes per step")
            test.eq(stored[1].metadata.lifecycle_phase, "activate")
            test.eq(stored[2].metadata.lifecycle_phase, "before_step")
            test.eq(stored[3].metadata.role, "system")
        end)

        for _, case in ipairs({
            { trait = "lifecycle_user_note_trait", phase = "before_step", role = "user" },
            { trait = "lifecycle_assistant_note_trait", phase = "activate", role = "assistant" },
        }) do
            it("fails the node on a " .. case.role .. " message in " .. case.phase, function()
                local run = run_node({ traits = { STUB .. case.trait } })
                test.eq(run.status, consts.STATUS.COMPLETED_FAILURE)
                local message = node_error(run)
                test.is_true(string.find(message, case.phase, 1, true) ~= nil, message)
                test.is_true(string.find(message, AGENT, 1, true) ~= nil, message)
                test.is_true(string.find(message, case.role, 1, true) ~= nil, message)
                test.eq(#requests_of(run), 0, "no request carries the message")
            end)
        end
    end)
end

return { run_tests = test.run_cases(define_tests) }
