local test = require("test")
local orchestrator = require("orchestrator")
local consts = require("consts")

type HarnessOptions = {
    activation: any?,
    admission_error: string?,
    admission_refused: string?,
    durable_owner_token: string?,
    actor_id: string?,
    channel_events: { any }?,
    failed_node_errors: string?,
    identity_error: string?,
    load_error: string?,
    nodes: { [string]: any }?,
    pending_error: string?,
    pending_commits: { string }?,
    persist_results: { any }?,
    registry_owner: string?,
    scheduler_decisions: { any }?,
    signal_wake_keys: { string }?,
    trace: { string }?,
    state_error: string?,
    status: string?,
}

local function harness(options: HarnessOptions?): any
    local cfg = options or {}
    local persist_index = 0
    local scheduler_index = 0
    local event_index = 0
    local workflow_state: any = {}
    local trace: { string } = cfg.trace or {}
    local registered_owner: string? = cfg.registry_owner
    local admitted_token: string? = nil
    local function record(entry: string) table.insert(trace, entry) end

    workflow_state.load_state = function(self: any): (any?, string?)
        record("load_state")
        if cfg.load_error then return nil, cfg.load_error end
        return self, nil
    end
    workflow_state.get_nodes = function(): { [string]: any }
        if cfg.nodes ~= nil then return cfg.nodes end
        return { root = { type = "test_node", status = consts.STATUS.PENDING } }
    end
    workflow_state.get_dataflow_metadata = function(): { [string]: any } return { test = true } end
    workflow_state.get_dataflow_status = function(): string? return cfg.status or consts.STATUS.PENDING end
    workflow_state.get_actor_id = function(): string? return cfg.actor_id == nil and "test-actor" or cfg.actor_id end
    workflow_state.get_actor_context = function(): any return { kind = "test" } end
    workflow_state.get_scheduler_snapshot = function(): any
        return {
            nodes = workflow_state.get_nodes(),
            active_yields = {},
            active_processes = {},
            input_tracker = { requirements = {}, available = { root = { input = true } } },
            has_workflow_output = false,
        }
    end
    workflow_state.get_failed_node_errors = function(): string? return cfg.failed_node_errors end
    workflow_state.track_process = function(self: any): any return self end
    workflow_state.queue_commands = function(self: any, commands: any): any
        if type(commands) == "table" and commands.type == consts.COMMAND_TYPES.PASSIVATE_WORKFLOW then
            local owner = commands.payload.owner or {}
            record("queue_passivation:" .. table.concat(commands.payload.signal_wake_keys or {}, ",") ..
                ":" .. tostring(owner.token == admitted_token and owner.phase == "running"))
        end
        return self
    end
    workflow_state.unclaimed_signal_wake_keys = function(): { string }
        local keys: { string } = {}
        for _, key in ipairs(cfg.signal_wake_keys or {}) do table.insert(keys, key) end
        return keys
    end
    workflow_state.release_signal_wake_keys = function(self: any, keys: { string }): any
        record("release_wakes:" .. table.concat(keys, ","))
        return self
    end
    workflow_state.queue_completion = function(self: any): any return self end
    workflow_state.discard_queued_commands = function(self: any): any return self end
    workflow_state.get_node = function(): any return { type = "test_node", status = consts.STATUS.PENDING } end
    workflow_state.handle_process_exit = function(): string? return nil end
    workflow_state.process_commits = function(): (any?, string?) return { changes_made = true }, nil end
    workflow_state.track_yield = function(self: any): any return self end
    workflow_state.satisfy_yield = function(self: any): any return self end
    workflow_state.abandon_yield = function(self: any): any return self end
    workflow_state.prepare_passivation = function(): (boolean, string?) return true, nil end
    workflow_state.observe_signal_wake = function() end
    workflow_state.persist = function(): (any?, string?)
        persist_index = persist_index + 1
        local configured = cfg.persist_results and cfg.persist_results[persist_index]
        if configured and configured.error then
            record("persist_failed")
            return nil, tostring(configured.error)
        end
        record("commit")
        local value = configured and configured.value or
            { changes_made = true, results = { { completed = true, released = true } } }
        return value, nil
    end

    local inbox = { case_receive = function(): any return { channel = "inbox" } end }
    local events = { case_receive = function(): any return { channel = "events" } end }
    local runtime: any = {
        workflow_state = {
            new = function(_id: string, options: any?): (any?, string?)
                local token = options and options.owner_token or nil
                record("state:" .. tostring(token ~= nil and token == admitted_token))
                if cfg.state_error then return nil, cfg.state_error end
                return workflow_state, nil
            end,
        },
        scheduler = {
            DECISION_TYPE = {
                EXECUTE_NODES = "execute_nodes",
                SATISFY_YIELD = "satisfy_yield",
                COMPLETE_WORKFLOW = "complete_workflow",
                PASSIVATE = "passivate",
                NO_WORK = "no_work",
            },
            find_next_work = function(): any
                scheduler_index = scheduler_index + 1
                local decisions = cfg.scheduler_decisions
                if decisions and decisions[scheduler_index] then return decisions[scheduler_index] end
                return { type = "complete_workflow", payload = { success = true, message = "done" } }
            end,
        },
        process = {
            registry = {
                lookup = function(): (string?, any?)
                    record("lookup")
                    if registered_owner then return registered_owner, nil end
                    return nil, "not_found: name not registered"
                end,
                register = function(): (boolean, nil)
                    record("register")
                    registered_owner = "orchestrator-pid"
                    return true, nil
                end,
                unregister = function(): any
                    record("unregister")
                    registered_owner = nil
                    return true
                end,
            },
            pid = function(): string return "orchestrator-pid" end,
            set_options = function() end,
            send = function(): (boolean, nil) return true, nil end,
            terminate = function() end,
            with_context = function(): any
                local spawner: any = {}
                spawner.with_actor = function(self: any): any return self end
                spawner.with_scope = function(self: any): any return self end
                spawner.spawn_linked_monitored = function(): (string, nil) return "child-pid", nil end
                return spawner
            end,
            inbox = function(): any return inbox end,
            events = function(): any return events end,
            event = { EXIT = "pid.exit", LINK_DOWN = "pid.link.down", CANCEL = "pid.cancel" },
        },
        channel = {
            select = function(): any
                event_index = event_index + 1
                local configured = cfg.channel_events and cfg.channel_events[event_index]
                if configured then
                    return { ok = true, channel = events, value = configured }
                end
                return { ok = false }
            end,
        },
        commit = {
            get_pending_commits = function(): ({ string }?, string?)
                if cfg.pending_error then return nil, cfg.pending_error end
                return cfg.pending_commits or {}, nil
            end,
            disable_terminal_activation = function(): (any, nil) return { terminal = true }, nil end,
            admit_owner = function(_id: string, min_generation: number, owner: any): (any?, string?)
                local current = cfg.activation or { generation = 1, desired_active = true }
                local refused = cfg.admission_refused
                if not refused and current.desired_active ~= true then refused = "inactive" end
                if not refused and current.generation < min_generation then refused = "stale" end
                record("admit:" .. tostring(current.generation) .. ":" .. tostring(owner.runtime_epoch) ..
                    ":" .. tostring(refused or "admitted"))
                admitted_token = owner.token
                if cfg.admission_error then return nil, cfg.admission_error end
                local row: any = { admitted = refused == nil, refused = refused }
                for key, value in pairs(current) do row[key] = value end
                return row, nil
            end,
        },
        activation_repo = {
            get = function(): (any, nil)
                record("reread")
                local row: any = {}
                for key, value in pairs(cfg.activation or { generation = 1, desired_active = true }) do
                    row[key] = value
                end
                row.owner_token = cfg.durable_owner_token or admitted_token
                row.owner_phase = "running"
                return row, nil
            end,
        },
        execution_frame = {
            reconstruct = function(): (any?, any?, string?)
                if cfg.identity_error then return nil, nil, cfg.identity_error end
                return { id = function(): string return "test-actor" end }, "test-scope", nil
            end,
        },
        funcs = {
            new = function(): any
                local executor: any = {}
                executor.with_actor = function(self: any): any return self end
                executor.with_scope = function(self: any): any return self end
                executor.call = function(_self: any, id: string): (any, nil)
                    if id == consts.RUNTIME_EPOCH_READER then
                        record("epoch_reader")
                        return { epoch = "runtime-test" }, nil
                    end
                    return {}, nil
                end
                return executor
            end,
        },
        overseer = {
            notify = function(): (boolean, nil)
                record("notify")
                return true, nil
            end,
        },
    }
    return runtime
end

local function run(runtime: any, args: any?): any
    local call_args: any = {}
    for key, value in pairs(args or {}) do call_args[key] = value end
    call_args.dataflow_id = call_args.dataflow_id or "workflow-1"
    call_args.activation_generation = call_args.activation_generation or 1
    if call_args.runtime_epoch == false then
        call_args.runtime_epoch = nil
    else
        call_args.runtime_epoch = call_args.runtime_epoch or "runtime-spawn"
    end
    return orchestrator.run(call_args, runtime)
end

local function since(trace: { string }, marker: string): { string }
    local tail: { string } = {}
    local found = false
    for _, entry in ipairs(trace) do
        if entry == marker then found = true end
        if found then table.insert(tail, entry) end
    end
    return tail
end

local function contains(trace: { string }, entry: string): boolean
    for _, value in ipairs(trace) do
        if value == entry then return true end
    end
    return false
end

local function define_tests()
    describe("Orchestrator protocol", function()
        it("rejects a missing dataflow id", function()
            local result = orchestrator.run({}, harness())
            test.is_false(result.success)
            test.contains(result.error, "Missing required dataflow_id")
        end)

        it("rejects a missing activation generation", function()
            local result = orchestrator.run({ dataflow_id = "workflow-1" }, harness())
            test.is_false(result.success)
            test.contains(result.error, "Missing required activation_generation")
        end)

        it("treats a canonical registry owner as a benign duplicate", function()
            local result = run(harness({ registry_owner = "other-pid" }))
            test.is_true(result.success)
            test.is_true(result.pending)
            test.contains(result.message, "already running")
        end)

        it("reports workflow-state construction failure", function()
            local result = run(harness({ state_error = "repository unavailable" }))
            test.is_false(result.success)
            test.contains(result.error, "repository unavailable")
        end)

        it("reports workflow-state loading failure", function()
            local result = run(harness({ load_error = "snapshot corrupt" }))
            test.is_false(result.success)
            test.contains(result.error, "snapshot corrupt")
        end)

        it("rejects an inactive activation without executing work", function()
            local result = run(harness({ activation = { generation = 1, desired_active = false } }))
            test.is_true(result.success)
            test.is_true(result.pending)
            test.contains(result.message, "inactive")
        end)

        it("adopts a newer durable generation before executing work", function()
            local result = run(harness({ activation = { generation = 2, desired_active = true } }))
            test.is_true(result.success)
            test.eq(result.output.message, "done")
            test.is_false(result.pending == true)
        end)

        it("fails closed when the persisted actor is absent", function()
            local result = run(harness({ actor_id = "" }))
            test.is_false(result.success)
            test.contains(result.error, "has no execution actor")
        end)

        it("fails closed when the execution frame cannot be reconstructed", function()
            local result = run(harness({ identity_error = "policy removed" }))
            test.is_false(result.success)
            test.contains(result.error, "policy removed")
        end)

        it("fails durably when pending commits cannot be loaded", function()
            local result = run(harness({ pending_error = "commit database unavailable" }))
            test.is_false(result.success)
            test.contains(result.error, "commit database unavailable")
        end)

        it("completes an empty workflow through the terminal projection", function()
            local result = run(harness({ nodes = {} }))
            test.is_true(result.success)
            test.contains(result.output.message, "Empty workflow")
        end)

        it("returns a successful completion projection", function()
            local result = run(harness({
                scheduler_decisions = {
                    { type = "complete_workflow", payload = { success = true, message = "all done" } },
                },
            }))
            test.is_true(result.success)
            test.eq(result.output.message, "all done")
        end)

        it("preserves detailed node failure text", function()
            local result = run(harness({
                failed_node_errors = "Node [root] failed: invalid output",
                scheduler_decisions = {
                    { type = "complete_workflow", payload = { success = false, message = "failed" } },
                },
            }))
            test.is_false(result.success)
            test.eq(result.error, "Node [root] failed: invalid output")
        end)

        it("reschedules immediately after losing a completion generation fence", function()
            local result = run(harness({
                scheduler_decisions = {
                    { type = "complete_workflow", payload = { success = true, message = "old" } },
                    { type = "complete_workflow", payload = { success = true, message = "new" } },
                },
                persist_results = {
                    { value = { results = { { completed = false, current_generation = 2 } } } },
                    { value = { results = { { completed = true, generation = 2 } } } },
                },
            }))
            test.is_true(result.success)
            test.eq(result.output.message, "new")
        end)

        it("passivates only after the durable generation is released", function()
            local result = run(harness({
                scheduler_decisions = {
                    { type = "passivate", payload = {} },
                },
                persist_results = {
                    { value = { results = { { released = true, generation = 1 } } } },
                },
            }))
            test.is_true(result.success)
            test.is_true(result.pending)
            test.is_true(result.passivated)
        end)

        it("admits itself under the canonical name before loading or mutating state", function()
            local trace: { string } = {}
            local result = run(harness({
                trace = trace,
                activation = { generation = 3, desired_active = true },
            }), { activation_generation = 2 })
            test.is_true(result.success)
            test.eq(table.concat(since(trace, "register"), " ", 1, 4),
                "register admit:3:runtime-spawn:admitted state:true load_state")
        end)

        it("reads the runtime epoch through the module reader when started synchronously", function()
            local trace: { string } = {}
            local result = run(harness({ trace = trace }), { runtime_epoch = false })
            test.is_true(result.success)
            local admitted = since(trace, "epoch_reader")
            test.eq(admitted[2], "admit:1:runtime-test:admitted")
        end)

        it("leaves before loading state when admission is refused", function()
            for _, case in ipairs({
                { refused = "owned", message = "already running" },
                { refused = "stale", message = "Stale" },
                { refused = "inactive", message = "Stale" },
            }) do
                local trace: { string } = {}
                local result = run(harness({ trace = trace, admission_refused = case.refused }))
                test.is_true(result.pending)
                test.contains(result.message, case.message)
                test.is_false(contains(trace, "load_state"))
                test.is_true(contains(trace, "unregister"))
            end
            local terminal_trace: { string } = {}
            local terminal = run(harness({ trace = terminal_trace, admission_refused = "terminal" }))
            test.is_true(terminal.success)
            test.contains(terminal.message, "terminal")
            test.is_false(contains(terminal_trace, "load_state"))
        end)

        it("resolves an unacknowledged admission from its durable token", function()
            local trace: { string } = {}
            local result = run(harness({ trace = trace, admission_error = "connection reset" }))
            test.is_true(result.success)
            test.eq(table.concat(since(trace, "admit:1:runtime-spawn:admitted"), " ", 1, 4),
                "admit:1:runtime-spawn:admitted reread state:true load_state")

            local lost: { string } = {}
            local refused = run(harness({
                trace = lost, admission_error = "connection reset", durable_owner_token = "other-token",
            }))
            test.is_false(refused.success)
            test.contains(refused.error, "admission")
            test.is_false(contains(lost, "load_state"))
        end)

        it("releases ownership, then gives up the name and exits", function()
            local trace: { string } = {}
            local result = run(harness({
                trace = trace,
                signal_wake_keys = { "signal:a", "signal:b" },
                scheduler_decisions = { { type = "passivate", payload = {} } },
                persist_results = {
                    { value = { results = { { released = true, generation = 1 } } } },
                },
            }))
            test.is_true(result.success)
            test.is_true(result.passivated)
            test.eq(table.concat(since(trace, "queue_passivation:signal:a,signal:b:true"), " "),
                "queue_passivation:signal:a,signal:b:true commit " ..
                "release_wakes:signal:a,signal:b unregister notify")
        end)

        it("keeps the name and wakes and reschedules when passivation loses the generation", function()
            local trace: { string } = {}
            local result = run(harness({
                trace = trace,
                signal_wake_keys = { "signal:a" },
                scheduler_decisions = {
                    { type = "passivate", payload = {} },
                    { type = "complete_workflow", payload = { success = true, message = "rescheduled" } },
                },
                persist_results = {
                    { value = { results = { { released = false, current_generation = 2 } } } },
                    { value = { results = { { completed = true, generation = 2 } } } },
                },
            }))
            test.is_true(result.success)
            test.eq(result.output.message, "rescheduled")
            test.is_false(contains(trace, "unregister"))
            test.is_false(contains(trace, "release_wakes:signal:a"))
        end)

        it("stops without further work when its ownership was lost or the release is uncertain", function()
            for _, persisted in ipairs({
                { value = { results = { { released = false, owner_changed = true, current_generation = 1 } } } },
                { error = "Failed to persist commands: connection reset" },
            }) do
                local trace: { string } = {}
                local result = run(harness({
                    trace = trace,
                    signal_wake_keys = { "signal:a" },
                    scheduler_decisions = {
                        { type = "passivate", payload = {} },
                        { type = "complete_workflow", payload = { success = true, message = "unreachable" } },
                    },
                    persist_results = { persisted },
                }))
                test.is_false(result.success)
                test.is_nil(result.passivated)
                test.is_false(contains(trace, "release_wakes:signal:a"))
                test.is_false(contains(trace, "notify"))
                local after = since(trace, "queue_passivation:signal:a:true")
                test.eq(#after, 2)
            end
        end)

        it("runtime cancellation stops the life without inventing business cancellation", function()
            local result = run(harness({
                scheduler_decisions = { { type = "no_work", payload = {} } },
                channel_events = { { kind = "pid.cancel" } },
            }))
            test.is_true(result.success)
            test.is_true(result.pending)
            test.contains(result.message, "runtime cancellation")
        end)

        it("returns a structured parked-arm validation error", function()
            local result = orchestrator.arm_parked_yield({ runtime = harness(), actor = {}, scope = {} }, {})
            test.eq(result.code, "PARK_ARM_FAILED")
            test.contains(result.message, "arm.ref")
        end)
    end)
end

return test.run_cases(define_tests)
