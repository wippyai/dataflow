local test = require("test")
local orchestrator = require("orchestrator")
local consts = require("consts")

type HarnessOptions = {
    activation: any?,
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
    state_error: string?,
    status: string?,
}

local function harness(options: HarnessOptions?): any
    local cfg = options or {}
    local persist_index = 0
    local scheduler_index = 0
    local event_index = 0
    local workflow_state: any = {}

    workflow_state.load_state = function(self: any): (any?, string?)
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
    workflow_state.queue_commands = function(self: any): any return self end
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
        if configured then
            if configured.error then return nil, tostring(configured.error) end
            return configured.value, nil
        end
        return { changes_made = true, results = { { completed = true, released = true } } }, nil
    end

    local inbox = { case_receive = function(): any return { channel = "inbox" } end }
    local events = { case_receive = function(): any return { channel = "events" } end }
    local runtime: any = {
        workflow_state = {
            new = function(): (any?, string?)
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
                    if cfg.registry_owner then return cfg.registry_owner, nil end
                    return nil, "not_found: name not registered"
                end,
                register = function(): (boolean, nil) return true, nil end,
                unregister = function() end,
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
        },
        activation_repo = {
            get = function(): (any, nil)
                return cfg.activation or { generation = 1, desired_active = true }, nil
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
                executor.call = function(): (any, nil) return {}, nil end
                return executor
            end,
        },
        wake_repo = { remove = function(): (boolean, nil) return true, nil end },
        overseer = { notify = function(): (boolean, nil) return true, nil end },
    }
    return runtime
end

local function run(runtime: any, args: any?): any
    local call_args: any = {}
    for key, value in pairs(args or {}) do call_args[key] = value end
    call_args.dataflow_id = call_args.dataflow_id or "workflow-1"
    call_args.activation_generation = call_args.activation_generation or 1
    return orchestrator.run(call_args, runtime)
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
