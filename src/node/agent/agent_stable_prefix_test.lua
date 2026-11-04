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
    extra_commands: ((string) -> {table})?,
    checkpoint: table?,
    node_metadata: table?,
    max_iterations: number?,
    no_input: boolean?,
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
    if options.extra_commands then
        for _, command in ipairs(options.extra_commands(node_id)) do
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

local function without_markers(request)
    local messages = {}
    for _, message in ipairs(request) do
        if message.role ~= "cache_marker" then
            messages[#messages + 1] = message
        end
    end
    return messages
end

-- What the provider prefix binds: everything but the metadata, which carries
-- thinking blocks the agent may strip from older turns.
local function signature_of(message)
    local copy = {}
    for key, value in pairs(message) do
        if key ~= "metadata" then copy[key] = value end
    end
    local encoded = json.encode(copy)
    return encoded
end

local function assert_extends(earlier, later, label)
    local before, after = without_markers(earlier), without_markers(later)
    test.gt(#after, #before, label .. ": the later request is longer")
    for index, message in ipairs(before) do
        test.eq(signature_of(after[index]), signature_of(message), label .. ": message " .. tostring(index))
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

            local stored = {}
            for _, row in ipairs(rows_of(run, agent_consts.DATA_TYPE.AGENT_MEMORY)) do
                stored[#stored + 1] = row
            end
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
