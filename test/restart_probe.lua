local flow = require("flow")
local funcs = require("funcs")
local uuid = require("uuid")
local client = require("client")
local consts = require("consts")
local sql = require("sql")
local time = require("time")
local recovery_helpers = require("recovery_helpers")

local M = {}

local function hold_runtime()
    local events = process.events()
    while true do
        local result = channel.select({ events:case_receive() })
        if not result.ok then return end
        local event = result.value
        if event and event.kind == process.event.CANCEL then return end
    end
end

-- One agent turn with a fast and a slow call. The runtime is killed after the
-- fast call's observation commits; the recovery metrics live in app:db so the
-- second runtime reports how often each call ran.
local function create_parallel_tools_workflow()
    local db = assert(sql.get("app:db"))
    local bytes = db:type() == "postgres" and "BYTEA" or "BLOB"
    local _, table_err = db:execute("CREATE TABLE IF NOT EXISTS restart_recovery_store ("
        .. "store_key TEXT PRIMARY KEY, store_value " .. bytes .. " NOT NULL, expires_at TIMESTAMP NULL)")
    db:release()
    if table_err then error(table_err) end

    local node_id, input_id = uuid.v7(), uuid.v7()
    local c = client.new()
    local id, err = c:create_workflow({
        { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
            node_id = node_id, node_type = "userspace.dataflow.node.agent:node", status = consts.STATUS.PENDING,
            config = {
                agent = "userspace.dataflow.node.agent.stub:recovery_test_agent", show_tool_calls = false,
                data_targets = {{ data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "result", content_type = consts.CONTENT_TYPE.TEXT }},
                arena = { prompt = "Complete the restart scenario.", max_iterations = 4, tool_calling = "auto",
                    tools = { "userspace.dataflow.node.agent.stub:recovery_tool" } },
            },
        } },
        { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
            data_id = input_id, data_type = consts.DATA_TYPE.WORKFLOW_INPUT,
            content = { scenario_id = "parallel-restart", mode = "fast_and_slow_one_turn",
                fast_delay_ms = 300, slow_delay_ms = 9000 },
            content_type = consts.CONTENT_TYPE.JSON,
        } },
        { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
            data_id = uuid.v7(), data_type = consts.DATA_TYPE.NODE_INPUT, node_id = node_id, key = input_id,
            content = "", content_type = consts.CONTENT_TYPE.REFERENCE,
        } },
    })
    if not id then error(err or "parallel tools workflow creation failed") end
    local started, start_err = c:start(id)
    if not started then error(start_err or "parallel tools workflow start failed") end
end

local function create_behavior_workflow()
    local node_id, input_id = uuid.v7(), uuid.v7()
    local c = client.new()
    local id, err = c:create_workflow({
        { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
            node_id = node_id, node_type = "userspace.dataflow.node.agent:node", status = consts.STATUS.PENDING,
            config = {
                agent = "userspace.dataflow.node.agent.stub:recovery_test_agent", show_tool_calls = false,
                data_targets = {{ data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "result", content_type = consts.CONTENT_TYPE.TEXT }},
                active_traits = { "userspace.dataflow.node.agent.stub:behavior_switch_compact_trait" },
                arena = { prompt = "Complete the restart scenario.", max_iterations = 4, tool_calling = "auto",
                    tools = { "userspace.dataflow.node.agent.stub:recovery_tool" } },
            },
        } },
        { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
            data_id = input_id, data_type = consts.DATA_TYPE.WORKFLOW_INPUT,
            content = { scenario_id = "behavior-restart", mode = "behavior_restart", tool_delay_ms = 9000 },
            content_type = consts.CONTENT_TYPE.JSON,
        } },
        { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
            data_id = uuid.v7(), data_type = consts.DATA_TYPE.NODE_INPUT, node_id = node_id, key = input_id,
            content = "", content_type = consts.CONTENT_TYPE.REFERENCE,
        } },
    })
    if not id then error(err or "behavior workflow creation failed") end
    local started, start_err = c:start(id)
    if not started then error(start_err or "behavior workflow start failed") end
end

local STUB = "userspace.dataflow.node.agent.stub:"
local PREFIX_SCENARIO = "prefix-restart"
local LATE_INPUT = "late-input-text"

-- An input-selected, time-aware agent switches agent in its first round. The second
-- request hangs and the runtime is killed there. Every checkpoint is skipped, and a
-- late node input arrives while no runtime is up.
local function create_stable_prefix_workflow()
    local db = assert(sql.get("app:db"))
    local bytes = db:type() == "postgres" and "BYTEA" or "BLOB"
    local _, table_err = db:execute("CREATE TABLE IF NOT EXISTS restart_recovery_store ("
        .. "store_key TEXT PRIMARY KEY, store_value " .. bytes .. " NOT NULL, expires_at TIMESTAMP NULL)")
    if table_err then error(table_err) end
    local _, verdict_err = db:execute("CREATE TABLE IF NOT EXISTS restart_prefix_verdict (verdict TEXT NOT NULL)")
    db:release()
    if verdict_err then error(verdict_err) end

    local node_id, input_id = uuid.v7(), uuid.v7()
    local c = client.new()
    local id, err = c:create_workflow({
        { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
            node_id = node_id, node_type = "userspace.dataflow.node.agent:node", status = consts.STATUS.PENDING,
            config = {
                agent = STUB .. "recovery_test_agent", show_tool_calls = false,
                checkpoint = { token_threshold = 1, function_id = STUB .. "no_such_summarizer" },
                data_targets = {{ data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "result", content_type = consts.CONTENT_TYPE.TEXT }},
                arena = { prompt = "Complete the restart scenario.", max_iterations = 8, tool_calling = "auto",
                    tools = { STUB .. "recovery_tool", STUB .. "recovery_control_tool" } },
            },
        } },
        { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
            data_id = input_id, data_type = consts.DATA_TYPE.WORKFLOW_INPUT,
            content = { scenario_id = PREFIX_SCENARIO, capture_requests = true, mode = "thinking_rounds", rounds = 4,
                switch_at = 1, switch_agent = STUB .. "stable_prefix_switch_target_agent",
                slow_request = 2, slow_ms = 60000 },
            content_type = consts.CONTENT_TYPE.JSON,
        } },
        { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
            data_id = uuid.v7(), data_type = consts.DATA_TYPE.NODE_INPUT, node_id = node_id, key = input_id,
            content = "", content_type = consts.CONTENT_TYPE.REFERENCE,
        } },
        { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
            data_id = uuid.v7(), data_type = consts.DATA_TYPE.NODE_INPUT, node_id = node_id, key = "agent_id",
            content = STUB .. "stable_prefix_time_agent", content_type = consts.CONTENT_TYPE.TEXT,
        } },
    })
    if not id then error(err or "stable prefix workflow creation failed") end
    local started, start_err = c:start(id)
    if not started then error(start_err or "stable prefix workflow start failed") end
end

-- The round's memory add waits behind slow work when the runtime is killed: a child
-- DAG, or a delegate of a sibling call.
local function create_round_text_workflow(scenario, active_traits)
    local db = assert(sql.get("app:db"))
    local bytes = db:type() == "postgres" and "BYTEA" or "BLOB"
    local _, table_err = db:execute("CREATE TABLE IF NOT EXISTS restart_recovery_store ("
        .. "store_key TEXT PRIMARY KEY, store_value " .. bytes .. " NOT NULL, expires_at TIMESTAMP NULL)")
    db:release()
    if table_err then error(table_err) end

    local node_id, input_id = uuid.v7(), uuid.v7()
    local c = client.new()
    local id, err = c:create_workflow({
        { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
            node_id = node_id, node_type = "userspace.dataflow.node.agent:node", status = consts.STATUS.PENDING,
            config = {
                agent = STUB .. "recovery_test_agent", show_tool_calls = false,
                active_traits = active_traits,
                data_targets = {{ data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "result", content_type = consts.CONTENT_TYPE.TEXT }},
                arena = { prompt = "Complete the restart scenario.", max_iterations = 3, tool_calling = "auto",
                    tools = { STUB .. "recovery_tool", STUB .. "recovery_control_tool" } },
            },
        } },
        { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
            data_id = input_id, data_type = consts.DATA_TYPE.WORKFLOW_INPUT,
            content = scenario, content_type = consts.CONTENT_TYPE.JSON,
        } },
        { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
            data_id = uuid.v7(), data_type = consts.DATA_TYPE.NODE_INPUT, node_id = node_id, key = input_id,
            content = "", content_type = consts.CONTENT_TYPE.REFERENCE,
        } },
    })
    if not id then error(err or "round text workflow creation failed") end
    local started, start_err = c:start(id)
    if not started then error(start_err or "round text workflow start failed") end
end

local function text_of(message)
    local parts = {}
    for _, part in ipairs(message.content or {}) do
        if part.text then parts[#parts + 1] = part.text end
    end
    return table.concat(parts, "")
end

local function block_list(request)
    local blocks = {}
    for _, message in ipairs(request) do
        if message.role == "assistant" then
            for _, block in ipairs((message.metadata or {}).thinking_blocks or {}) do
                blocks[#blocks + 1] = block.thinking
            end
        end
    end
    return table.concat(blocks, ",")
end

-- Request 2 died with the first runtime, so request 3 is the one that extends request 1.
local function stable_prefix_error()
    local requests: any = {}
    for number = 1, 5 do
        requests[number] = recovery_helpers.get_request(PREFIX_SCENARIO, number)
        if not requests[number] then return "request " .. number .. " was not captured" end
    end
    if recovery_helpers.get_request(PREFIX_SCENARIO, 6) then return "the run sent a request too many" end

    local chain_err = recovery_helpers.extension_error(requests[1], requests[3], { ignore_system = true })
        or recovery_helpers.extension_error(requests[3], requests[4])
        or recovery_helpers.extension_error(requests[4], requests[5])
    if chain_err then return chain_err end

    local turn_note
    for _, number in ipairs({ 1, 3, 4, 5 }) do
        local messages = recovery_helpers.conversation(requests[number])
        local pairing_err = recovery_helpers.pairing_error(requests[number])
        if pairing_err then return "request " .. number .. ": " .. pairing_err end
        for index, message in ipairs(messages) do
            if message.role == "user" then
                if text_of(message):find(LATE_INPUT, 1, true) then
                    return "request " .. number .. " folded the late input into the user message"
                end
                local note = text_of(messages[index + 1])
                if not note:find("The current date and time is ", 1, true) then
                    return "request " .. number .. " has no turn note after the input: " .. note
                end
                turn_note = turn_note or note
                if note ~= turn_note then return "request " .. number .. " rendered another turn note: " .. note end
            end
        end
    end

    local system = text_of(recovery_helpers.conversation(requests[3])[1])
    if not system:find("A different system prompt", 1, true) then
        return "the restart undid the persisted agent switch: " .. system
    end

    for number, expected in pairs({ [3] = "block-1", [4] = "block-3", [5] = "block-3,block-4" }) do
        if block_list(requests[number]) ~= expected then
            return "request " .. number .. " thinking: expected " .. expected .. ", got " .. block_list(requests[number])
        end
    end
    return nil
end

local function prepare_runtime()
    -- Services may start while the dependency bootloader is still migrating.
    -- Use the same canonical readiness check as the ordinary test harness.
    local ready, readiness_err = funcs.call("app:wait_for_boot")
    if readiness_err or not ready then error(readiness_err or "boot is not ready") end
    -- The standalone restart process bypasses the test runner's 00_setup suite.
    -- Seed the same authority fixture before capturing a workflow identity.
    local authority_ready, authority_err = funcs.call("app.await_authority:setup", { durable_tokens = true })
    if authority_err or not authority_ready then error(authority_err or "authority fixture is not ready") end
end

function M.create_behavior()
    prepare_runtime()
    create_behavior_workflow()
    return hold_runtime()
end

function M.create_parallel_tools()
    prepare_runtime()
    create_parallel_tools_workflow()
    return hold_runtime()
end

function M.create_stable_prefix()
    prepare_runtime()
    create_stable_prefix_workflow()
    return hold_runtime()
end

local function create_command_delegate(scenario_id, yield_commands)
    prepare_runtime()
    local db = assert(sql.get("app:db"))
    local _, err = db:execute("CREATE TABLE IF NOT EXISTS restart_command_verdict (verdict TEXT NOT NULL)")
    db:release()
    if err then error(err) end
    create_round_text_workflow({ scenario_id = scenario_id, mode = "thinking_rounds", rounds = 1,
        capture_requests = true, command_delegate_at = 1, yield_commands = yield_commands,
        child_delay_ms = 6000, delegate_to = STUB .. "recovery_test_agent",
        delegate_input = { scenario_id = scenario_id .. "-child", mode = "thinking_rounds", rounds = 0,
            slow_request = 1, slow_ms = 6000 } })
    return hold_runtime()
end

function M.create_round_command_delegate()
    return create_command_delegate("round-command-delegate", false)
end

function M.create_round_yield_delegate()
    return create_command_delegate("round-yield-delegate", true)
end

local function observe_command_outputs(scenario_id, expected)
    for _ = 1, 600 do
        local db = assert(sql.get("app:db"))
        local rows = db:query("SELECT COUNT(*) AS done FROM dataflows WHERE status = 'completed'")
        db:release()
        if rows and rows[1] and tonumber(rows[1].done) == 1 then
            local request = recovery_helpers.get_request(scenario_id, 2)
            local verdict = "next request is missing"
            if request then
                verdict = recovery_helpers.pairing_error(request)
                local count = 0
                for _, message in ipairs(request) do
                    if message.role == "developer" and string.find(text_of(message), expected, 1, true) then
                        count = count + 1
                    end
                end
                if count ~= 1 then verdict = "next request has " .. count .. " command outputs" end
                if recovery_helpers.get_request(scenario_id, 3) then verdict = "an extra request is sent" end
            end
            local writer = assert(sql.get("app:db"))
            local _, err = writer:execute("INSERT INTO restart_command_verdict (verdict) VALUES ('"
                .. (verdict or "ok"):gsub("'", "''") .. "')")
            writer:release()
            if err then error(err) end
            return hold_runtime()
        end
        time.sleep("100ms")
    end
    return hold_runtime()
end

function M.observe_round_command_delegate()
    return observe_command_outputs("round-command-delegate", "child-output-1")
end

function M.observe_round_yield_delegate()
    return observe_command_outputs("round-yield-delegate", "slow")
end

function M.create_round_children()
    prepare_runtime()
    create_round_text_workflow({ scenario_id = "round-children", mode = "thinking_rounds", rounds = 1,
        commands_memory_at = 1, child_delay_ms = 6000 })
    return hold_runtime()
end

function M.create_round_delegate()
    prepare_runtime()
    create_round_text_workflow({ scenario_id = "round-delegate", mode = "thinking_rounds", rounds = 1,
        delegate_with_memory_at = 1, delegate_to = STUB .. "recovery_test_agent",
        delegate_input = { scenario_id = "round-delegate-child", mode = "thinking_rounds", rounds = 0,
            slow_request = 1, slow_ms = 6000 } })
    return hold_runtime()
end

function M.create_round_both()
    prepare_runtime()
    create_round_text_workflow({ scenario_id = "round-both", mode = "thinking_rounds", rounds = 1,
        delegate_and_memory_at = 1, delegate_to = STUB .. "recovery_test_agent",
        delegate_input = { scenario_id = "round-both-child", mode = "thinking_rounds", rounds = 0,
            slow_request = 1, slow_ms = 6000 } })
    return hold_runtime()
end

function M.create_round_rerun()
    prepare_runtime()
    create_round_text_workflow({ scenario_id = "round-rerun", mode = "thinking_rounds", rounds = 1,
        delegate_once_at = 1, delegate_to = STUB .. "recovery_test_agent",
        delegate_input = { scenario_id = "round-rerun-child", mode = "thinking_rounds", rounds = 0,
            slow_request = 1, slow_ms = 6000 } })
    return hold_runtime()
end

function M.create_round_double_kill()
    prepare_runtime()
    create_round_text_workflow({ scenario_id = "round-double-kill", mode = "thinking_rounds", rounds = 1,
        capture_requests = true, delegate_once_at = 1, delegate_memory = "superseded",
        delegate_to = STUB .. "recovery_test_agent",
        delegate_input = { scenario_id = "round-double-kill-child", mode = "thinking_rounds", rounds = 0,
            slow_request = 1, slow_ms = 6000 } }, { STUB .. "round_wrapper_trait" })
    return hold_runtime()
end

function M.create_round_yield()
    prepare_runtime()
    create_round_text_workflow({ scenario_id = "round-yield", mode = "thinking_rounds", rounds = 1,
        yield_at = 1, switch_agent = STUB .. "stable_prefix_switch_target_agent" })
    return hold_runtime()
end

function M.create_round_settle()
    prepare_runtime()
    create_round_text_workflow({ scenario_id = "round-settle", mode = "thinking_rounds", rounds = 1 },
        { STUB .. "round_wrapper_trait" })
    return hold_runtime()
end

function M.create_round_yield_commands()
    prepare_runtime()
    create_round_text_workflow({ scenario_id = "round-yield-commands", mode = "thinking_rounds", rounds = 1,
        commands_yield_at = 1, child_delay_ms = 6000 })
    return hold_runtime()
end

function M.create_round_artifact()
    prepare_runtime()
    create_round_text_workflow({ scenario_id = "round-artifact", mode = "thinking_rounds", rounds = 1,
        artifact_delegate_at = 1, delegate_to = STUB .. "recovery_test_agent",
        delegate_input = { scenario_id = "round-artifact-child", mode = "thinking_rounds", rounds = 0,
            slow_request = 1, slow_ms = 6000 } })
    return hold_runtime()
end

function M.create()
    prepare_runtime()
    local template = (flow.template() :: any):func("userspace.dataflow.node.func:test_func")
    local dataflow_id, start_err = (flow.create() :: any)
        :with_input({
            items = {
                { message = "restart slow", delay_ms = 9000 },
                { message = "restart fast 1", delay_ms = 2000 },
                { message = "restart fast 2", delay_ms = 2000 },
                { message = "restart fast 3", delay_ms = 2000 },
            }
        })
        :parallel({
            source_array_key = "items",
            batch_size = 2,
            scheduling = "rolling",
            on_error = "continue",
            template = template,
        })
        :start()
    if start_err or not dataflow_id then error(start_err or "workflow start failed") end
    hold_runtime()
end

function M.observe()
    hold_runtime()
end

function M.observe_stable_prefix()
    for _ = 1, 600 do
        local db = assert(sql.get("app:db"))
        local rows = db:query("SELECT COUNT(*) AS done FROM dataflows WHERE status = 'completed'")
        db:release()
        if rows and rows[1] and tonumber(rows[1].done) == 1 then
            local db_verdict = assert(sql.get("app:db"))
            local _, write_err = db_verdict:execute("INSERT INTO restart_prefix_verdict (verdict) VALUES ('"
                .. (stable_prefix_error() or "ok"):gsub("'", "''") .. "')")
            db_verdict:release()
            if write_err then error(write_err) end
            return hold_runtime()
        end
        time.sleep("100ms")
    end
    return hold_runtime()
end

return M
