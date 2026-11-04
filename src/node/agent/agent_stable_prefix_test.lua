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
local bedrock_mapper = require("bedrock_mapper")
local openai_mapper = require("openai_mapper")
local compat_mapper = require("compat_mapper")
local agent_node = require("agent_node")
local prompt_builder = require("prompt_builder")
local node_sdk = require("node_sdk")
local agent_context = require("agent_context")

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
    agent: any,
    traits: {string}?,
    input: table?,
    extra_inputs: {string}?,
    extra_commands: ((string, string) -> {table})?,
    arena: table?,
    checkpoint: table?,
    node_metadata: table?,
    max_iterations: number?,
    no_input: boolean?,
    no_prompt: boolean?,
    min_iterations: number?,
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
                        prompt = not options.no_prompt and "Run the stable prefix scenario." or nil,
                        max_iterations = options.max_iterations or 8,
                        min_iterations = options.min_iterations,
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
        for _, command in ipairs(options.extra_commands(node_id, input.scenario_id)) do
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
            local run = run_node({ agent = TIME_AGENT, no_input = true, no_prompt = true, extra_inputs = { "context" } })
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

        local function seeded_finish(node_id, scenario_id)
            return {
                data_row(node_id, agent_consts.DATA_TYPE.AGENT_ACTION, "1_action", {
                    result = "working",
                    tool_calls = { { id = "stored-call", name = "recovery_tool",
                        arguments = { scenario_id = scenario_id, step = 1 } } },
                }, { iteration = 1, finish_reason = "tool_call" }),
                data_row(node_id, agent_consts.DATA_TYPE.AGENT_OBSERVATION, "1_recovery_tool", { ok = true }, {
                    iteration = 1, tool_call_id = "stored-call", tool_name = "recovery_tool",
                }),
                data_row(node_id, agent_consts.DATA_TYPE.AGENT_ACTION, "2_final", {
                    result = "",
                    tool_calls = { { id = "stored-finish", name = "finish", arguments = { answer = "done" } } },
                }, { iteration = 2, finish_reason = "tool_call" }),
            }
        end

        it("compacts with an accepted finish", function()
            local run = run_node({
                input = { rounds = 0 },
                arena = { exit_schema = { type = "object", properties = { answer = { type = "string" } } } },
                checkpoint = CHECKPOINT,
                node_metadata = { checkpoint_requested = true,
                    state = { current_iteration = 2, agent_id = AGENT } },
                extra_commands = seeded_finish,
            })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS, node_error(run))
            local marker
            for _, row in ipairs(rows_of(run, agent_consts.DATA_TYPE.AGENT_MEMORY)) do
                if row.metadata.checkpoint_marker == true then marker = row end
            end
            test.not_nil(marker, "the requested compaction is not dropped")
            test.eq(node_row(run).metadata.checkpoint_requested, false, "the request is served")
        end)

        it("keeps an accepted finish when a strict compaction fails", function()
            local run = run_node({
                input = { rounds = 0 },
                arena = { exit_schema = { type = "object", properties = { answer = { type = "string" } } } },
                checkpoint = { function_id = STUB .. "no_such_summarizer", strict = true },
                node_metadata = { checkpoint_requested = true, state = { current_iteration = 2, agent_id = AGENT } },
                extra_commands = seeded_finish,
            })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS, node_error(run))
            local skipped = 0
            for _, row in ipairs(rows_of(run, agent_consts.DATA_TYPE.AGENT_OBSERVATION)) do
                if row.metadata.checkpoint_skipped == true then skipped = skipped + 1 end
            end
            test.eq(skipped, 1, "the failure is recorded")
        end)

        it("keeps a text completion when a strict compaction fails", function()
            local run = run_node({
                input = { mode = "text_final" },
                traits = { STUB .. "behavior_compaction_trait" },
                checkpoint = { function_id = STUB .. "no_such_summarizer", strict = true },
            })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS, node_error(run))
        end)

        it("compacts the whole round once a finish is rejected, each row once", function()
            local run = run_node({
                input = { rounds = 0 },
                arena = {
                    exit_schema = { type = "object", properties = { answer = { type = "string" } } },
                    exit_func_id = STUB .. "exit_reject_validator",
                },
                checkpoint = CHECKPOINT,
                node_metadata = { checkpoint_requested = true,
                    state = { current_iteration = 2, agent_id = AGENT } },
                extra_commands = seeded_finish,
            })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS, node_error(run))

            local history = recovery_helpers.get_checkpoint_history(run.scenario_id, 1)
            test.not_nil(history, "the requested compaction ran")
            local seen, finish_result = {}, false
            for _, row in ipairs(history) do
                test.is_nil(seen[row.data_id], "row summarized once: " .. row.data_id)
                seen[row.data_id] = true
                if (row.metadata or {}).tool_call_id == "stored-finish" then finish_result = true end
            end
            test.eq(#history, 4, "both rounds with their results")
            test.is_true(finish_result, "the rejection is summarized with its finish")

            local requests = requests_of(run)
            test.gt(#requests, 0)
            assert_paired(requests)
            for _, turn in ipairs(claude_mapper.map_messages(requests[1]).messages) do
                for _, block in ipairs(turn.content) do
                    test.is_true(block.type ~= "tool_result" and block.type ~= "tool_use",
                        "the request after the cut carries none of the round")
                end
            end
        end)

        describe("open rounds", function()
            local function checkpoint_over(rows, completing)
                local flags = {}
                local query = {}
                for _, method in ipairs({ "fetch_options", "with_nodes", "with_data_types" }) do
                    query[method] = function(self) return self end
                end
                query.all = function() return rows end
                local written = {}
                local n = {
                    node_id = "node", dataflow_id = "flow",
                    query = function() return query end,
                    metadata = function() return { checkpoint_requested = true } end,
                    update_metadata = function(_, updates) table.insert(flags, updates) end,
                    data = function(_, _, content) table.insert(written, content) end,
                }
                local marker = agent_node._test.maybe_checkpoint_history(n,
                    { checkpoint = { function_id = STUB .. "checkpoint_summarizer" } }, {}, {}, "agent", "model", 3, completing)
                return marker, flags
            end

            local function action(id, iteration, content)
                return { data_id = id, type = agent_consts.DATA_TYPE.AGENT_ACTION, content = content,
                    content_type = consts.CONTENT_TYPE.JSON, metadata = { iteration = iteration } }
            end

            local function result(id, iteration, call_id)
                return { data_id = id, type = agent_consts.DATA_TYPE.AGENT_OBSERVATION, content = "ok",
                    content_type = consts.CONTENT_TYPE.TEXT,
                    metadata = { iteration = iteration, tool_call_id = call_id, tool_name = "tool" } }
            end

            it("cuts a closed round", function()
                local marker = checkpoint_over({
                    action("01", 1, { tool_calls = { { id = "c", name = "tool", arguments = {} } } }),
                    result("02", 1, "c"),
                })
                test.not_nil(marker)
            end)

            it("takes no cut while a finish has no result on a node that keeps running", function()
                local marker, flags = checkpoint_over({
                    action("01", 1, { tool_calls = { { id = "f", name = "finish", arguments = {} } } }),
                })
                test.is_nil(marker)
                test.eq(#flags, 0, "the request flag stays")
            end)

            it("cuts an open finish when the node completes with it", function()
                local marker = checkpoint_over({
                    action("01", 1, { tool_calls = { { id = "f", name = "finish", arguments = {} } } }),
                }, true)
                test.not_nil(marker)
            end)

            it("counts an unanswered delegate call as open and keeps the request", function()
                local marker, flags = checkpoint_over({
                    action("01", 1, { delegate_calls = { { id = "d", name = "delegate", arguments = {} } } }),
                })
                test.is_nil(marker)
                test.eq(#flags, 0, "the request flag stays")
            end)

            it("summarizes an older unanswered call instead of stopping every checkpoint", function()
                local marker = checkpoint_over({
                    action("01", 1, { tool_calls = { { id = "stale", name = "tool", arguments = {} } } }),
                    action("02", 2, { tool_calls = { { id = "c", name = "tool", arguments = {} } } }),
                    result("03", 2, "c"),
                })
                test.not_nil(marker, "only the latest action can be open")
            end)

            it("matches a result to the call of its own iteration", function()
                local marker = checkpoint_over({
                    action("01", 1, { tool_calls = { { id = "same", name = "tool", arguments = {} } } }),
                    result("02", 1, "same"),
                    action("03", 2, { tool_calls = { { id = "same", name = "tool", arguments = {} } } }),
                })
                test.is_nil(marker, "the second turn reused the id and has no result")
            end)
        end)

        it("fails a summary the model cut off, with no cut", function()
            local scenario_id = "TRUNCATE-SUMMARY-" .. uuid.v7()
            local run = run_node({
                input = { scenario_id = scenario_id, rounds = 2, prompt_tokens = 500 },
                checkpoint = { token_threshold = 100, function_id = "userspace.dataflow.node.agent:checkpoint",
                    model = "recovery-test-model" },
            })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS, node_error(run))
            local skipped = 0
            for _, row in ipairs(rows_of(run, agent_consts.DATA_TYPE.AGENT_OBSERVATION)) do
                if row.metadata.checkpoint_skipped == true then skipped = skipped + 1 end
            end
            test.gt(skipped, 1, "a skip row per attempt: it retries")
            for _, row in ipairs(rows_of(run, agent_consts.DATA_TYPE.AGENT_MEMORY)) do
                test.is_true(row.metadata.checkpoint_marker ~= true, "no cut")
            end
        end)

        it("fails the node on a summary the model cut off when strict", function()
            local scenario_id = "TRUNCATE-SUMMARY-" .. uuid.v7()
            local run = run_node({
                input = { scenario_id = scenario_id, rounds = 2, prompt_tokens = 500 },
                checkpoint = { token_threshold = 100, function_id = "userspace.dataflow.node.agent:checkpoint",
                    model = "recovery-test-model", strict = true },
            })
            test.eq(run.status, consts.STATUS.COMPLETED_FAILURE)
            test.is_true(string.find(node_error(run), "CHECKPOINT_FAILED", 1, true) ~= nil, node_error(run))
        end)

        local function stated_budget(options, history)
            local node_id = uuid.v7()
            options.model = options.model or "recovery-test-model"
            local _, call_err = funcs.new():call("userspace.dataflow.node.agent:checkpoint", {
                dataflow_id = uuid.v7(), node_id = node_id, options = options, history = history,
            })
            test.is_nil(call_err)
            local _, system = find_message(recovery_helpers.get_request(node_id, 1),
                function(message) return message.role == "system" end)
            return text_of(system), recovery_helpers.get_metric(node_id, "summarizer_max_tokens", nil)
        end

        local EARLIER = { { data_id = "1", type = agent_consts.DATA_TYPE.AGENT_MEMORY, content = "earlier",
            metadata = { checkpoint_marker = true } } }
        local FIRST = { { data_id = "1", type = agent_consts.DATA_TYPE.AGENT_OBSERVATION, content = "event", metadata = {} } }

        describe("the node's commits", function()
            -- Runs the node against a recording double, for the order of its commits.
            local function run_recorded(options)
                local events, rows = {}, {}
                local query = {}
                local kinds = {}
                query.with_nodes = function(self) return self end
                query.fetch_options = function(self) return self end
                query.order_by = function(self) return self end
                query.with_data_keys = function(self) return self end
                query.with_data_types = function(self, ...)
                    kinds = { ... }
                    return self
                end
                local function matching()
                    local out = {}
                    for _, row in ipairs(rows) do
                        for _, kind in ipairs(kinds) do
                            if row.type == kind then out[#out + 1] = row end
                        end
                    end
                    return out
                end
                query.all = matching
                query.one = function() return matching()[1] end
                local metadata = options.metadata or {}
                local config = { agent = "agent:a", model = "m", checkpoint = options.checkpoint,
                    arena = { tool_calling = "auto", max_iterations = 2, exit_schema = options.exit_schema } }
                local n
                n = {
                    dataflow_id = uuid.v7(), node_id = uuid.v7(), path = { "node" },
                    config = function() return config end,
                    inputs = function() return { task = { content = "do it", metadata = {} } } end,
                    metadata = function() return metadata end,
                    query = function() return query end,
                    data = function(_, kind, content, data_options)
                        local row = { data_id = uuid.v7(), type = kind, content = content,
                            content_type = data_options.content_type, key = data_options.key,
                            metadata = data_options.metadata }
                        table.insert(rows, row)
                        local marker = type(row.metadata) == "table" and row.metadata.checkpoint_marker
                        table.insert(events, marker and "marker" or ("data:" .. tostring(kind)))
                    end,
                    update_config = function() end,
                    update_metadata = function(_, updates)
                        for key, value in pairs(updates) do metadata[key] = value end
                    end,
                    yield = function() table.insert(events, "yield"); return {}, nil end,
                    submit = function() table.insert(events, "submit"); return true end,
                    complete = function() table.insert(events, "complete"); return "completed" end,
                    fail = function(_, _, message) table.insert(events, "fail:" .. tostring(message)); return message end,
                }
                local instance = { id = "agent:a", model = "m", step = function()
                    table.insert(events, "step")
                    return options.step_result
                end }
                local ctx = {
                    configure_delegate_tools = function() end,
                    get_current_agent = function() return instance end,
                    load_agent = function() return instance end,
                    get_config = function() return { current_agent_id = "agent:a", current_model = "m" } end,
                    add_tools = function() end,
                }
                local original_node, original_context = node_sdk.new, agent_context.new
                node_sdk.new = function() return n end
                agent_context.new = function() return ctx end
                local ok, result = pcall(agent_node.run, { node = { metadata = metadata } })
                node_sdk.new, agent_context.new = original_node, original_context
                test.is_true(ok, tostring(result))
                return events
            end

            it("commits an accepted finish's marker with the completion", function()
                local events = run_recorded({
                    metadata = { checkpoint_requested = true },
                    checkpoint = { function_id = STUB .. "checkpoint_summarizer" },
                    exit_schema = { type = "object", properties = { answer = { type = "string" } } },
                    step_result = { result = "", finish_reason = "tool_call", tokens = {},
                        tool_calls = { { id = "f", name = "finish", arguments = { answer = "done" } } } },
                })
                local marker_at, complete_at
                for index, event in ipairs(events) do
                    if event == "marker" then marker_at = index end
                    if event == "complete" then complete_at = index end
                end
                test.not_nil(marker_at, table.concat(events, ","))
                test.not_nil(complete_at, table.concat(events, ","))
                for index = marker_at, complete_at do
                    test.is_true(events[index] ~= "yield" and events[index] ~= "submit",
                        "no commit separates them: " .. table.concat(events, ","))
                end
            end)

            it("commits the input pin before the first request of a resumed stored run", function()
                local events = run_recorded({
                    metadata = { state = { agent_id = "agent:a", model = "m", current_iteration = 1 } },
                    step_result = { result = "done", finish_reason = "stop", tokens = {} },
                })
                local pin_at, yield_at, step_at
                for index, event in ipairs(events) do
                    if event == "data:" .. agent_consts.DATA_TYPE.AGENT_INPUT then pin_at = index end
                    if event == "yield" and pin_at and not yield_at then yield_at = index end
                    if event == "step" and not step_at then step_at = index end
                end
                test.not_nil(pin_at, table.concat(events, ","))
                test.not_nil(yield_at, table.concat(events, ","))
                test.is_true(yield_at < step_at, table.concat(events, ","))
            end)
        end)

        it("states the memory budget in the merge instruction", function()
            local system, max_tokens = stated_budget({ max_memory_chars = 4321 }, EARLIER)
            test.is_true(string.find(system, "4321", 1, true) ~= nil, system)
            test.eq(max_tokens, 8642, "max_tokens follows the budget")
        end)

        it("states the memory budget on the first summary too", function()
            local system, max_tokens = stated_budget({}, FIRST)
            test.is_true(string.find(system, "8192", 1, true) ~= nil, system)
            test.eq(max_tokens, 16384)
        end)

        it("states what fits when an explicit max_tokens is smaller", function()
            local system, max_tokens = stated_budget({ max_tokens = 1000 }, EARLIER)
            test.eq(max_tokens, 1000, "explicit max_tokens is honoured")
            test.is_true(string.find(system, "fit in 1000 characters", 1, true) ~= nil, system)
        end)

        it("states what fits under the model's output limit", function()
            local system, max_tokens = stated_budget({ model = "recovery-small-output-model" }, FIRST)
            test.eq(max_tokens, 1000)
            test.is_true(string.find(system, "fit in 1000 characters", 1, true) ~= nil, system)
        end)

        it("reserves at most half of a small context window", function()
            local system, max_tokens = stated_budget({ model = "recovery-small-window-model" }, FIRST)
            test.eq(max_tokens, 500)
            test.is_true(string.find(system, "fit in 500 characters", 1, true) ~= nil, system)
        end)

        it("sends no unbounded default to a model with no limits", function()
            local system, max_tokens = stated_budget({ max_memory_chars = 100000 }, FIRST)
            test.eq(max_tokens, 32000)
            test.is_true(string.find(system, "fit in 32000 characters", 1, true) ~= nil, system)
        end)

        local DEFAULT_SUMMARIZER = "userspace.dataflow.node.agent:checkpoint"

        local function assert_no_repeated_summary(run)
            local calls = tonumber(recovery_helpers.get_metric(run.node_id, "summarizer_calls", 0)) or 0
            test.gt(calls, 0, "the scenario summarizes")
            local seen = {}
            for number = 1, calls do
                local request = json.encode(recovery_helpers.get_request(run.node_id, number))
                test.is_nil(seen[request], "summarizer request " .. tostring(number) .. " repeats an earlier one")
                seen[request] = true
            end
            return calls
        end

        it("does not repeat a failed summary on the threshold path", function()
            local run = run_node({
                input = { scenario_id = "TRUNCATE-SUMMARY-" .. uuid.v7(), rounds = 4, prompt_tokens = 500 },
                checkpoint = { token_threshold = 100, function_id = DEFAULT_SUMMARIZER, model = "recovery-test-model" },
            })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS, node_error(run))
            assert_no_repeated_summary(run)
        end)

        it("does not repeat a failed requested compaction for an unchanged history", function()
            local run = run_node({
                input = { scenario_id = "TRUNCATE-SUMMARY-" .. uuid.v7(), rounds = 2 },
                checkpoint = { function_id = DEFAULT_SUMMARIZER, model = "recovery-test-model" },
                node_metadata = { checkpoint_requested = true, state = { current_iteration = 1, agent_id = AGENT } },
                extra_commands = function(node_id, scenario_id)
                    return {
                        data_row(node_id, agent_consts.DATA_TYPE.AGENT_ACTION, "1_action", {
                            result = "working",
                            tool_calls = { { id = "stored", name = "recovery_tool",
                                arguments = { scenario_id = scenario_id, step = 0 } } },
                        }, { iteration = 1, finish_reason = "tool_call" }),
                        data_row(node_id, agent_consts.DATA_TYPE.AGENT_OBSERVATION, "1_recovery_tool", { ok = true }, {
                            iteration = 1, tool_call_id = "stored", tool_name = "recovery_tool" }),
                    }
                end,
            })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS, node_error(run))
            assert_no_repeated_summary(run)
        end)

        it("keeps compacting when the rolling summary reaches its budget", function()
            local run = run_node({
                input = { scenario_id = "GROW-SUMMARY-" .. uuid.v7(), rounds = 14, prompt_tokens = 500 },
                checkpoint = { token_threshold = 100, function_id = DEFAULT_SUMMARIZER, model = "recovery-test-model" },
                max_iterations = 20,
            })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS, node_error(run))
            for _, row in ipairs(rows_of(run, agent_consts.DATA_TYPE.AGENT_OBSERVATION)) do
                test.is_true(row.metadata.checkpoint_skipped ~= true, "no merge is cut off: " .. tostring(row.content))
            end
            local sizes = {}
            for _, row in ipairs(rows_of(run, agent_consts.DATA_TYPE.AGENT_MEMORY)) do
                if row.metadata.checkpoint_marker == true then sizes[#sizes + 1] = #row.content end
            end
            test.gt(#sizes, 8, "every step past the threshold compacts")
            test.is_true(sizes[#sizes] <= 8192, "stored size " .. tostring(sizes[#sizes]))
            test.eq(sizes[#sizes], sizes[#sizes - 1], "the size stabilizes at the cap")
        end)

        it("clamps a stored summary by characters, keeping each one whole", function()
            local run = run_node({
                input = { rounds = 2, prompt_tokens = 500 },
                checkpoint = { token_threshold = 100, function_id = STUB .. "multibyte_summarizer", max_memory_chars = 200 },
            })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS, node_error(run))
            local marker
            for _, row in ipairs(rows_of(run, agent_consts.DATA_TYPE.AGENT_MEMORY)) do
                if row.metadata.checkpoint_marker == true then marker = row end
            end
            test.not_nil(marker)
            test.eq(marker.metadata.checkpoint_memory_truncated, true)
            local chars = 0
            for _ in string.gmatch(marker.content, "[\1-\127\194-\244][\128-\191]*") do chars = chars + 1 end
            test.eq(chars, 200, "200 characters, not 200 bytes")
            local body = marker.content:gsub(" %.%.%. %[truncated%]$", "")
            test.eq(#body % 2, 1, "a leading a, then whole two-byte characters")
        end)

        it("holds no delegate result from before the cut in the run context", function()
            local run = run_node({
                input = { rounds = 2, delegate_at = 1, delegate_to = AGENT, checkpoint_at = 1 },
                checkpoint = CHECKPOINT,
            })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS, node_error(run))
            assert_paired(requests_of(run))
            local built = run_context.get_prompt({
                host = { dataflow_id = run.dataflow_id, node_id = run.node_id },
            })
            test.is_nil(recovery_helpers.pairing_error(built.messages))
        end)

        it("shows the run context what the request after a cut sends, in that order", function()
            local run = run_node({
                agent = TIME_AGENT, input = { rounds = 2, checkpoint_at = 1 }, checkpoint = CHECKPOINT,
            })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS, node_error(run))
            local requests = requests_of(run)
            local built = run_context.get_prompt({
                host = { dataflow_id = run.dataflow_id, node_id = run.node_id },
            })
            local shown = without_markers(built.messages)
            local sent = without_markers(requests[#requests])
            local shown_user = find_message(built.messages, function(m) return m.role == "user" end) or 0
            local sent_user = find_message(requests[#requests], function(m) return m.role == "user" end) or 0
            test.eq(text_of(shown[shown_user + 1]), text_of(sent[sent_user + 1]), "the turn note")
            test.eq(text_of(shown[shown_user + 2]), text_of(sent[sent_user + 2]), "the summary")
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

        it("resumes a run stored before the pin with the model it was using", function()
            local run = run_node({
                agent = TARGET_AGENT,
                config = { model = TARGET_MODEL },
                input = { rounds = 1 },
                node_metadata = { state = { current_iteration = 1, agent_id = TARGET_AGENT, model = TARGET_MODEL } },
                named_inputs = { model = INPUT_MODEL },
            })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS, node_error(run))
            local action = rows_of(run, agent_consts.DATA_TYPE.AGENT_ACTION)[1]
            test.eq(action.metadata.model, TARGET_MODEL, "a persisted switch stays over the input model")
        end)

        it("keeps a committed model switch over a stale state on a run stored before the pin", function()
            local run = run_node({
                agent = TARGET_AGENT,
                config = { model = TARGET_MODEL },
                input = { rounds = 1 },
                node_metadata = { state = { current_iteration = 1, agent_id = TARGET_AGENT, model = INPUT_MODEL } },
            })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS, node_error(run))
            test.eq(rows_of(run, agent_consts.DATA_TYPE.AGENT_ACTION)[1].metadata.model, TARGET_MODEL)
        end)

        it("keeps a committed agent switch's model on a run stored before the pin", function()
            local run = run_node({
                agent = TARGET_AGENT,
                config = { model = TARGET_MODEL },
                input = { rounds = 1 },
                node_metadata = { state = { current_iteration = 1, agent_id = AGENT, model = INPUT_MODEL } },
            })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS, node_error(run))
            local action = rows_of(run, agent_consts.DATA_TYPE.AGENT_ACTION)[1]
            test.eq(action.metadata.agent_id, TARGET_AGENT)
            test.eq(action.metadata.model, TARGET_MODEL)
        end)

        it("keeps an inline agent's model on a run stored before the pin", function()
            local inline = { id = "inline-agent", name = "inline-agent", model = INPUT_MODEL,
                prompt = "Follow the deterministic scenario encoded in the user input." }
            local run = run_node({
                agent = inline,
                input = { rounds = 1 },
                node_metadata = { state = { current_iteration = 1, agent_id = "inline-agent", model = TARGET_MODEL } },
                named_inputs = { model = TARGET_MODEL },
            })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS, node_error(run))
            test.eq(rows_of(run, agent_consts.DATA_TYPE.AGENT_ACTION)[1].metadata.model, TARGET_MODEL)
        end)

        it("ignores a model input after the first commit of a pinned run", function()
            local run = run_node({
                agent = TARGET_AGENT,
                input = { rounds = 1 },
                node_metadata = { state = { current_iteration = 1, agent_id = TARGET_AGENT, model = INPUT_MODEL } },
                named_inputs = { model = TARGET_MODEL },
                extra_commands = function(node_id, scenario_id)
                    return { data_row(node_id, agent_consts.DATA_TYPE.AGENT_INPUT, "conversation_input",
                        json.encode({ scenario_id = scenario_id, capture_requests = true,
                            mode = "thinking_rounds", rounds = 1 })) }
                end,
            })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS, node_error(run))
            local action = rows_of(run, agent_consts.DATA_TYPE.AGENT_ACTION)[1]
            test.eq(action.metadata.model, INPUT_MODEL, "the stored selection wins")
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

        local function chars_of(history)
            local total = 0
            for _, event in ipairs(history.events) do
                total = total + #tostring(event.raw_content or "")
                if event.role == "function" then total = total + #json.encode(event.metadata.result) end
            end
            return total
        end

        for _, mode in ipairs({ "since_checkpoint", "all", "window" }) do
            it("keeps the newest rows over a long input in " .. mode .. " under max_chars", function()
                local run = run_node({ input = { rounds = 3, padding = string.rep("p", 4000) } })
                test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS, node_error(run))
                local history = run_context.get_history({
                    host = { dataflow_id = run.dataflow_id, node_id = run.node_id },
                    selector = { mode = mode, last = 64, max_chars = 1000 },
                })
                test.gt(#history.events, 1, "the input does not empty the selection")
                test.is_true(history.truncated, "the dropped input is reported")
                test.is_true(chars_of(history) <= 1000, "max_chars is a hard bound: " .. tostring(chars_of(history)))
                test.is_true(history.events[1].role ~= "user", "the input is what is dropped")
                test.is_true(history.events[1].role ~= "function", "no result is left without its call")
                local actions = rows_of(run, agent_consts.DATA_TYPE.AGENT_ACTION)
                test.eq(history.events[#history.events].id, actions[#actions].data_id, "the newest row survives")
            end)
        end

        it("keeps the summary over older rows under max_chars", function()
            local run = run_node({
                input = { rounds = 4, checkpoint_at = 2, padding = string.rep("p", 4000) },
                checkpoint = { token_threshold = 100, function_id = STUB .. "checkpoint_summarizer" },
            })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS, node_error(run))
            local history = run_context.get_history({
                host = { dataflow_id = run.dataflow_id, node_id = run.node_id },
                selector = { mode = "since_checkpoint", max_chars = 300 },
            })
            local found = false
            for _, event in ipairs(history.events) do
                if string.find(tostring(event.raw_content), "checkpointed", 1, true) then found = true end
            end
            test.is_true(found, "the summary survives")
            test.is_true(history.truncated)
        end)

        it("shows no event for an empty pin", function()
            local run = run_node({ agent = TIME_AGENT, no_input = true, no_prompt = true, extra_inputs = { "context" } })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS, node_error(run))
            local history = run_context.get_history({ host = { dataflow_id = run.dataflow_id, node_id = run.node_id } })
            for _, event in ipairs(history.events) do
                test.is_true(event.role ~= nil and event.role ~= "user", "no empty input event")
            end
        end)

        it("pins the arena prompt when the fold is empty", function()
            local run = run_node({ no_input = true, extra_inputs = { "context" } })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS, node_error(run))
            local pins = rows_of(run, agent_consts.DATA_TYPE.AGENT_INPUT)
            test.eq(#pins, 1)
            test.eq(pins[1].content, "Run the stable prefix scenario.")
        end)

        it("shows the run context the input first on a run stored before the pin", function()
            local run = run_node({
                input = { rounds = 1 },
                node_metadata = { state = { current_iteration = 1, agent_id = AGENT } },
                extra_commands = function(node_id, scenario_id)
                    return {
                        data_row(node_id, agent_consts.DATA_TYPE.AGENT_ACTION, "1_action", {
                            result = "working", tool_calls = { { id = "stored", name = "recovery_tool",
                                arguments = { scenario_id = scenario_id, step = 1 } } },
                        }, { iteration = 1, finish_reason = "tool_call" }),
                        data_row(node_id, agent_consts.DATA_TYPE.AGENT_OBSERVATION, "1_recovery_tool", { ok = true }, {
                            iteration = 1, tool_call_id = "stored", tool_name = "recovery_tool" }),
                    }
                end,
            })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS, node_error(run))
            local built = run_context.get_prompt({ host = { dataflow_id = run.dataflow_id, node_id = run.node_id } })
            local user_at = find_message(built.messages, function(m) return m.role == "user" end) or 0
            local assistant_at = find_message(built.messages, function(m) return m.role == "assistant" end) or 0
            test.is_true(user_at < assistant_at, "user " .. tostring(user_at) .. ", assistant " .. tostring(assistant_at))
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

    describe("text_prefix", function()
        it("costs the same on a huge row as on a small one", function()
            local huge = string.rep("a", 20 * 1024 * 1024)
            local started = time.now():unix_nano()
            local kept, clipped = prompt_builder.text_prefix(huge, 200)
            local elapsed_ms = (time.now():unix_nano() - started) / 1000000
            test.eq(#kept, 200)
            test.is_true(clipped)
            test.is_true(elapsed_ms < 200, "work follows the cap, not the row: " .. tostring(elapsed_ms) .. "ms")
        end)

        it("caps bytes that start no character", function()
            local kept, clipped = prompt_builder.text_prefix(string.rep("\128", 50000), 200)
            test.is_true(clipped)
            test.is_true(#kept <= 800, "kept " .. tostring(#kept))
        end)

        it("keeps a text at exactly the cap, in four-byte characters", function()
            local text = string.rep("\240\159\152\128", 200)
            local kept, clipped = prompt_builder.text_prefix(text, 200)
            test.is_false(clipped)
            test.eq(kept, text)
            local cut, cut_clipped = prompt_builder.text_prefix(text .. "x", 200)
            test.is_true(cut_clipped)
            test.eq(cut, text)
        end)
    end)

    describe("command children", function()
        it("sends the outputs of two children as text and keeps the prefix", function()
            local run = run_node({ input = { rounds = 2, two_children_at = 1 } })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS, node_error(run))
            local requests = requests_of(run)
            test.eq(#requests, 3)
            assert_each_request_extends_the_last(requests)
            local _, output = find_message(requests[2], with_text("child-output-1"))
            test.not_nil(output, "the model reads the outputs")
            test.is_true(string.find(text_of(output), "child-output-2", 1, true) ~= nil, text_of(output))
            test.is_nil(find_message(requests[2], with_text("table: 0x")))
        end)

        it("renders a table in an error result as text", function()
            local builder = prompt_builder.new("flow", "node", "agent") :: any
            builder:with_history({
                { data_id = "01", type = agent_consts.DATA_TYPE.AGENT_ACTION,
                    content = { tool_calls = { { id = "c", name = "tool", arguments = {} } } }, metadata = {} },
                { data_id = "02", type = agent_consts.DATA_TYPE.AGENT_OBSERVATION,
                    content = { reason = "broke" }, metadata = { tool_call_id = "c", tool_name = "tool", is_error = true } },
            })
            local built = builder:build_prompt("system")
            local _, result = find_message(built:get_messages(), function(m) return m.role == "function_result" end)
            test.is_true(string.find(text_of(result), "broke", 1, true) ~= nil, text_of(result))
        end)
    end)

    describe("rounds with nothing to answer", function()
        it("answers a text turn below min_iterations, so the request stays valid", function()
            local run = run_node({ input = { mode = "text_final" }, min_iterations = 3, max_iterations = 5 })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS, node_error(run))
            local requests = requests_of(run)
            test.eq(#requests, 3)
            for index, request in ipairs(requests) do
                local messages = without_markers(request)
                test.is_true(messages[#messages].role ~= "assistant", "request " .. tostring(index) .. " ends on assistant")
            end
            test.is_nil(recovery_helpers.prefix_error(requests[1], requests[2]))
            test.is_nil(recovery_helpers.prefix_error(requests[2], requests[3]))
        end)

        it("sends no empty user message: the turn note is the only content", function()
            local run = run_node({ agent = TIME_AGENT, no_input = true, no_prompt = true, extra_inputs = { "context" } })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS, node_error(run))
        end)

        it("maps a note with no input to a request with content", function()
            local builder = prompt_builder.new("flow", "node", "agent") :: any
            builder:with_initial_input(""):with_history({
                { data_id = "01", type = agent_consts.DATA_TYPE.AGENT_MEMORY, content = "the turn note",
                    metadata = { turn_context = true, role = "system", llm_meta = { preserve_position = true } } },
            })
            local messages = builder:build_prompt("system"):get_messages()
            local claude = claude_mapper.map_messages(messages)
            test.eq(#claude.messages, 1)
            test.eq(claude.messages[1].content[1].text, "the turn note")
            local bedrock = bedrock_mapper.map_messages(messages)
            test.eq(#bedrock.messages, 1)
            test.eq(bedrock.messages[1].content[1].text, "the turn note")
            test.is_true(string.find(json.encode(openai_mapper.map_messages(messages)), "the turn note", 1, true) ~= nil,
                "the note reaches OpenAI")
            test.is_true(string.find(json.encode(compat_mapper.map_messages(messages)), "the turn note", 1, true) ~= nil,
                "the note reaches OpenAI-compatible drivers")

            local with_input = prompt_builder.new("flow", "node", "agent") :: any
            with_input:with_initial_input("hello"):with_history({
                { data_id = "01", type = agent_consts.DATA_TYPE.AGENT_MEMORY, content = "the turn note",
                    metadata = { turn_context = true, role = "system", llm_meta = { preserve_position = true } } },
            })
            local stayed = false
            for _, block in ipairs(bedrock_mapper.map_messages(with_input:build_prompt("system"):get_messages()).messages[1].content) do
                if block.text == "the turn note" then stayed = true end
            end
            test.is_true(stayed, "the note stays in the user turn on Bedrock")
        end)

        it("renders no text for a whitespace turn and answers it", function()
            local run = run_node({ input = { rounds = 2, whitespace_at = 1 } })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS, node_error(run))
            local requests = requests_of(run)
            test.gt(#requests, 1)
            for index, request in ipairs(requests) do
                for _, message in ipairs(without_markers(request)) do
                    test.is_true(message.role ~= "assistant" or string.find(text_of(message), "%S") ~= nil,
                        "request " .. tostring(index) .. " has a blank assistant text")
                end
                local messages = without_markers(request)
                test.is_true(messages[#messages].role ~= "assistant", "request " .. tostring(index) .. " ends on assistant")
            end
            test.is_nil(recovery_helpers.prefix_error(requests[1], requests[2]))
        end)

        it("fails a delegate with no input instead of waiting forever", function()
            local run = run_node({ input = { rounds = 1, delegate_without_input_at = 1, delegate_to = AGENT } })
            test.eq(run.status, consts.STATUS.COMPLETED_FAILURE)
            test.is_true(string.find(node_error(run), "input_data", 1, true) ~= nil, node_error(run))
        end)

        it("fails a node with nothing to send, naming why", function()
            local run = run_node({ no_input = true, no_prompt = true, extra_inputs = { "context" } })
            test.eq(run.status, consts.STATUS.COMPLETED_FAILURE)
            test.is_true(string.find(node_error(run), "nothing to send", 1, true) ~= nil, node_error(run))
        end)
    end)

    describe("settling a round on recovery", function()
        local function seeded_round(node_id, scenario_id)
            return {
                data_row(node_id, agent_consts.DATA_TYPE.AGENT_ACTION, "1_action", {
                    result = "working", tool_calls = { { id = "stored", name = "recovery_tool",
                        arguments = { scenario_id = scenario_id, step = 0 } } },
                }, { iteration = 1, finish_reason = "tool_call" }),
                data_row(node_id, agent_consts.DATA_TYPE.AGENT_OBSERVATION, "1_recovery_tool", { ok = true }, {
                    iteration = 1, tool_call_id = "stored", tool_name = "recovery_tool" }),
            }
        end

        local function warnings_of(run)
            local count = 0
            for _, row in ipairs(rows_of(run, agent_consts.DATA_TYPE.AGENT_OBSERVATION)) do
                if row.metadata.remaining_iterations == 2 then count = count + 1 end
            end
            return count
        end

        it("writes the warning a killed round did not", function()
            local run = run_node({
                input = { rounds = 0 }, max_iterations = 3,
                node_metadata = { state = { current_iteration = 1, agent_id = AGENT } },
                extra_commands = seeded_round,
            })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS, node_error(run))
            test.eq(warnings_of(run), 1)
        end)

        it("writes no second warning when one is stored", function()
            local run = run_node({
                input = { rounds = 0 }, max_iterations = 3,
                node_metadata = { state = { current_iteration = 1, agent_id = AGENT } },
                extra_commands = function(node_id, scenario_id)
                    local rows = seeded_round(node_id, scenario_id)
                    table.insert(rows, data_row(node_id, agent_consts.DATA_TYPE.AGENT_OBSERVATION, "1_iterations_warning",
                        "stored warning", { iteration = 1, remaining_iterations = 2 }))
                    return rows
                end,
            })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS, node_error(run))
            test.eq(warnings_of(run), 1)
        end)

        local function wrapper_payload(run, number)
            local encoded = recovery_helpers.get_metric(run.node_id, "wrapper_payload:" .. tostring(number), nil)
            return encoded and json.decode(encoded) or nil
        end

        it("gives after_execute the live payload on recovery", function()
            local run = run_node({
                input = { rounds = 0 }, max_iterations = 3,
                traits = { STUB .. "round_wrapper_trait" },
                arena = { exit_schema = { type = "object", properties = { answer = { type = "string" } } } },
                node_metadata = { state = { current_iteration = 1, agent_id = AGENT } },
                extra_commands = function(node_id, scenario_id)
                    return {
                        data_row(node_id, agent_consts.DATA_TYPE.AGENT_ACTION, "1_action", {
                            result = "working", tool_calls = {
                                { id = "stored", name = "recovery_tool", arguments = { scenario_id = scenario_id, step = 0 } },
                                { id = "rejected", name = "finish", arguments = {} },
                            },
                        }, { iteration = 1, finish_reason = "tool_call" }),
                        data_row(node_id, agent_consts.DATA_TYPE.AGENT_OBSERVATION, "1_recovery_tool", "123", {
                            iteration = 1, tool_call_id = "stored", tool_name = "recovery_tool" }),
                        data_row(node_id, agent_consts.DATA_TYPE.AGENT_OBSERVATION, "1_exit", "rejected", {
                            iteration = 1, tool_call_id = "rejected", tool_name = "finish", is_error = true }),
                    }
                end,
            })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS, node_error(run))
            local payload = wrapper_payload(run, 1)
            test.not_nil(payload, "after_execute ran on recovery")
            test.eq(#payload.tool_calls, 1, "the exit call is not a tool")
            test.eq(payload.tool_calls[1].id, "stored")
            test.eq(payload.tool_results.stored.result, "123", "text stays text")
            test.eq(payload.tool_results.stored.tool_call.id, "stored")
        end)

        it("gives after_execute no calls on a round without any", function()
            local run = run_node({ traits = { STUB .. "round_wrapper_trait" }, input = { rounds = 1 } })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS, node_error(run))
            local last = tonumber(recovery_helpers.get_metric(run.node_id, "wrapper_payloads", 0)) or 0
            test.gt(last, 1)
            test.eq(#wrapper_payload(run, last).tool_calls, 0, "the previous round's calls are gone")
        end)

        it("leaves a settled round alone", function()
            local run = run_node({
                input = { rounds = 0 }, max_iterations = 3,
                node_metadata = { round_settled = 1, state = { current_iteration = 1, agent_id = AGENT } },
                extra_commands = seeded_round,
            })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS, node_error(run))
            test.eq(warnings_of(run), 0)
        end)

        it("records a settled round with its commit", function()
            local run = run_node({ input = { rounds = 1 } })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS, node_error(run))
            test.gt(tonumber(node_row(run).metadata.round_settled) or 0, 0)
        end)
    end)

    describe("reference rows", function()
        it("sends two reference rows to one target, each with its call", function()
            local OBSERVATION = agent_consts.DATA_TYPE.AGENT_OBSERVATION
            local function reference(node_id, target, iteration)
                return {
                    type = consts.COMMAND_TYPES.CREATE_DATA,
                    payload = {
                        data_id = uuid.v7(), data_type = OBSERVATION, node_id = node_id, key = target.payload.data_id,
                        content = "", content_type = consts.CONTENT_TYPE.REFERENCE,
                        metadata = { iteration = iteration, tool_call_id = "reused", tool_name = "delegate_x" },
                    },
                }
            end
            local function delegating(node_id, iteration)
                return data_row(node_id, agent_consts.DATA_TYPE.AGENT_ACTION, iteration .. "_action", {
                    result = "delegating",
                    delegate_calls = { { id = "reused", name = "delegate_x", agent_id = AGENT, arguments = {} } },
                }, { iteration = iteration, finish_reason = "tool_call" })
            end
            local run = run_node({
                input = { rounds = 0 },
                node_metadata = { state = { current_iteration = 2, agent_id = AGENT } },
                extra_commands = function(node_id)
                    local target = data_row(nil, agent_consts.DATA_TYPE.AGENT_DELEGATION, "delegation_result_1", { answer = "child" })
                    return {
                        delegating(node_id, 1), target, reference(node_id, target, 1),
                        delegating(node_id, 2), reference(node_id, target, 2),
                    }
                end,
            })
            test.eq(run.status, consts.STATUS.COMPLETED_SUCCESS, node_error(run))
            assert_paired(requests_of(run))
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
        it("clears the pending additions with the delegate's result", function()
            local child = "stable-prefix-child-" .. uuid.v7()
            local run = run_node({
                input = { rounds = 1, delegate_with_memory_at = 1, delegate_to = AGENT,
                    delegate_input = { scenario_id = child, mode = "text_final" } },
                max_iterations = 1,
            })
            test.eq(node_row(run).metadata.memory_pending, false, "no later round clears it for this one")
            local added = 0
            for _, row in ipairs(rows_of(run, agent_consts.DATA_TYPE.AGENT_MEMORY)) do
                if row.discriminator == "fact" then added = added + 1 end
            end
            test.eq(added, 1)
        end)

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
