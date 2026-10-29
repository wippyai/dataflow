local test = require("test")
local uuid = require("uuid")
local time = require("time")
local client = require("client")
local node = require("node")
local commit = require("commit")
local consts = require("consts")
local data_reader = require("data_reader")
local node_reader = require("node_reader")
local dataflow_repo = require("dataflow_repo")
local orchestrator = require("orchestrator")
local workflow_state = require("workflow_state")
local scheduler = require("scheduler")
local activation_repo = require("activation_repo")
local execution_frame = require("execution_frame")

local function data_command(node_id, data_type, key, content, data_id)
    return { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
        data_id = data_id or uuid.v7(), node_id = node_id, data_type = data_type,
        key = key, content = content, content_type = consts.CONTENT_TYPE.JSON,
    } }
end

local function create_workflow()
    local c, client_err = client.new()
    test.is_nil(client_err)
    local root, downstream, leaf = uuid.v7(), uuid.v7(), uuid.v7()
    local leaf_input = uuid.v7()
    local id, create_err = (c :: any):create_workflow({
        { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
            node_id = root, node_type = "test_node", status = consts.STATUS.RUNNING,
            config = {
                data_targets = {
                    { data_type = consts.DATA_TYPE.NODE_INPUT, node_id = downstream, key = "default" },
                    { data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "result", discriminator = "result" },
                },
                error_targets = {
                    { data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "error", discriminator = "error" },
                },
            },
        } },
        { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
            node_id = downstream, node_type = "test_node", status = consts.STATUS.COMPLETED_SUCCESS,
            config = { data_targets = {{ data_type = consts.DATA_TYPE.NODE_INPUT,
                node_id = leaf, key = "default" }} },
        } },
        { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
            node_id = leaf, node_type = "test_node", status = consts.STATUS.COMPLETED_SUCCESS,
            config = {},
        } },
        data_command(root, consts.DATA_TYPE.NODE_INPUT, "default", { request = true }),
        data_command(leaf, consts.DATA_TYPE.NODE_INPUT, "default", { value = "durable" }, leaf_input),
    })
    test.is_nil(create_err)
    return { id = id, root = root, downstream = downstream, leaf = leaf, leaf_input = leaf_input, client = c }
end

-- Only process transport is scripted; startup, scheduling, commits and reads use
-- the production modules. No node process survives the simulated restart.
local function runtime_for(probe, inspect_snapshot)
    local runtime: any = {
        workflow_state = workflow_state, commit = commit, activation_repo = activation_repo,
        execution_frame = execution_frame,
        wake_repo = { remove = function(): (boolean, nil) return true, nil end },
        overseer = { notify = function(): (boolean, nil) return true, nil end },
        scheduler = {
            DECISION_TYPE = scheduler.DECISION_TYPE,
            find_next_work = function(snapshot: any): any
                if not probe.inspected then
                    probe.inspected = true
                    inspect_snapshot(snapshot)
                end
                return scheduler.find_next_work(snapshot)
            end,
        },
    }
    runtime.funcs = { new = function(): any
        local executor: any = {}
        executor.with_actor = function(self: any): any return self end
        executor.with_scope = function(self: any): any return self end
        executor.call = function(_self: any, id: string, args: any): (boolean, nil)
            probe.hook_id, probe.hook_args = id, args
            return true, nil
        end
        return executor
    end }
    local inbox = { case_receive = function(): any return {} end }
    local events = { case_receive = function(): any return {} end }
    runtime.process = {
        registry = {
            lookup = function(): (nil, string) return nil, "not_found: name not registered" end,
            register = function(): (boolean, nil) return true, nil end,
            unregister = function(name: string) probe.unregistered = name end,
        },
        pid = function(): string return "recovery-orchestrator" end,
        set_options = function() end,
        send = function(): (boolean, nil) return true, nil end,
        terminate = function() end,
        with_context = function(): any
            local spawner: any = {}
            spawner.with_actor = function(self: any): any return self end
            spawner.with_scope = function(self: any): any return self end
            spawner.spawn_linked_monitored = function(_self: any, _entry: any, _host: any, args: any): (string, nil)
                table.insert(probe.spawned, args.node_id)
                return "recovery-node", nil
            end
            return spawner
        end,
        inbox = function(): any return inbox end,
        events = function(): any return events end,
        event = { EXIT = "pid.exit", LINK_DOWN = "pid.link.down", CANCEL = "pid.cancel" },
    }
    runtime.channel = { select = function(): any return { ok = false } end }
    return runtime
end

local function restart(fixture, inspect_snapshot)
    local activation, activation_err = commit.request_activation(fixture.id, {}, { notify = false })
    test.is_nil(activation_err)
    local probe: any = { spawned = {} }
    local result = orchestrator.run({ dataflow_id = fixture.id,
        activation_generation = (activation :: any).generation,
    }, runtime_for(probe, inspect_snapshot)) :: any
    test.is_true(probe.inspected, "startup reaches the scheduler")
    return result, probe
end

local function assert_preserved(fixture, snapshot)
    local nodes = (node_reader.with_dataflow(fixture.id) :: any):with_nodes(fixture.root):all()
    test.is_nil(nodes[1].metadata.orchestrator_restarted_at,
        "a committed outcome is not classified as interrupted")
    test.eq(snapshot.nodes[fixture.downstream].status, consts.STATUS.COMPLETED_SUCCESS,
        "committed outcomes do not reset completed dependents")
    test.eq(snapshot.nodes[fixture.leaf].status, consts.STATUS.COMPLETED_SUCCESS)
    local inputs = (data_reader.with_dataflow(fixture.id) :: any)
        :with_nodes(fixture.leaf):with_data_types(consts.DATA_TYPE.NODE_INPUT):all()
    test.eq(#inputs, 1, "downstream routed data survives recovery")
    test.eq(inputs[1].data_id, fixture.leaf_input, "recovery preserves the existing data row")
end

local function submit(fixture, commands)
    local commit_id = uuid.v7()
    local submitted, submit_err = commit.submit(fixture.id, commit_id, commands)
    test.is_nil(submit_err)
    test.not_nil(submitted)
    return commit_id
end

local function assert_backlog_applied(fixture)
    local pending, pending_err = commit.get_pending_commits(fixture.id)
    test.is_nil(pending_err)
    test.eq(#(pending :: any), 0, "startup applies every persisted commit")
end

local function park_commands(fixture, yield_id)
    return {
        { type = consts.COMMAND_TYPES.UPDATE_NODE, payload = {
            node_id = fixture.root, status = consts.STATUS.WAITING,
        } },
        data_command(fixture.root, consts.DATA_TYPE.NODE_YIELD, yield_id, {
            node_id = fixture.root, yield_id = yield_id, reply_to = "previous-process",
            yield_context = { wait_for_signal = true, signal_id = "continue", park_ack = true },
        }),
    }
end

local function define_tests()
    describe("Orchestrator recovery from unapplied durable commits", function()
        it("reconstructs a timer and its retry outcome from the pending backlog before scheduling", function()
            local fixture = create_workflow()
            local deadline = time.now():add(time.HOUR):format(time.RFC3339NANO)
            local yield_id = fixture.root .. ":retry:2"
            submit(fixture, {
                {type = consts.COMMAND_TYPES.UPDATE_NODE, payload = {node_id = fixture.root,
                    status = consts.STATUS.WAITING, metadata = {func_retry = {
                        attempt = 2, first_attempt_at = 123, in_flight = false, wait_until = deadline,
                        last_error = {kind = "Unavailable", message = "Interrupted call"},
                    }}}},
                data_command(fixture.root, consts.DATA_TYPE.NODE_YIELD, yield_id, {
                    yield_id = yield_id, reply_to = "previous-process", yield_context = {
                        wait_for_timer = true, timeout_deadline = deadline, park_ack = true,
                    },
                }),
            })
            local result, probe = restart(fixture, function(snapshot)
                local wait = test.not_nil(snapshot.active_yields[fixture.root]) :: any
                test.is_true(wait.wait_for_timer)
                test.is_true(wait.detached)
                test.eq(wait.timeout_deadline, deadline)
                local row = (node_reader.with_dataflow(fixture.id) :: any):with_nodes(fixture.root):one()
                test.eq(row.metadata.func_retry.attempt, 2)
                test.is_false(row.metadata.func_retry.in_flight)
                test.eq(row.metadata.func_retry.wait_until, deadline)
                test.eq(scheduler.find_next_work(snapshot).type, scheduler.DECISION_TYPE.PASSIVATE)
            end)
            test.is_true(result.passivated)
            test.eq(#probe.spawned, 0)
            assert_backlog_applied(fixture)
        end)

        it("reconstructs a completed timer separately from an outstanding timer", function()
            local fixture = create_workflow()
            local deadline = time.now():add(time.HOUR):format(time.RFC3339NANO)
            local yield_id = uuid.v7()
            submit(fixture, {
                {type = consts.COMMAND_TYPES.UPDATE_NODE, payload = {node_id = fixture.root,
                    status = consts.STATUS.WAITING}},
                data_command(fixture.root, consts.DATA_TYPE.NODE_YIELD, yield_id, {
                    yield_id = yield_id, reply_to = "previous-process", yield_context = {
                        wait_for_timer = true, timeout_deadline = deadline, park_ack = true,
                    },
                }),
                data_command(fixture.root, consts.DATA_TYPE.NODE_YIELD_RESULT, yield_id, {timer_completed = true}),
            })
            restart(fixture, function(snapshot)
                local wait = test.not_nil(snapshot.active_yields[fixture.root]) :: any
                test.is_true(wait.wait_for_timer)
                test.is_true(wait.signal_data.timer_completed)
                test.eq(wait.timeout_deadline, deadline)
                test.eq(#wait.wake_keys, 0)
                test.eq(scheduler.find_next_work(snapshot).type, scheduler.DECISION_TYPE.EXECUTE_NODES)
            end)
            assert_backlog_applied(fixture)
        end)

        for _, in_flight in ipairs({false, true}) do
            it("ignores earlier timer episodes for attempt two in_flight=" .. tostring(in_flight), function()
                local fixture = create_workflow()
                local old_deadline = time.now():add(-time.SECOND):format(time.RFC3339NANO)
                local deadline = time.now():add(time.HOUR):format(time.RFC3339NANO)
                local old_id, current_id = fixture.root .. ":retry:1", fixture.root .. ":retry:2"
                local commands = {
                    {type = consts.COMMAND_TYPES.UPDATE_NODE, payload = {node_id = fixture.root,
                        status = in_flight and consts.STATUS.RUNNING or consts.STATUS.WAITING,
                        metadata = {func_retry = {attempt = 2, first_attempt_at = 123,
                            in_flight = in_flight, wait_until = not in_flight and deadline or nil}}}},
                    data_command(fixture.root, consts.DATA_TYPE.NODE_YIELD, old_id, {
                        yield_id = old_id, yield_context = {wait_for_timer = true, timeout_deadline = old_deadline}}),
                    data_command(fixture.root, consts.DATA_TYPE.NODE_YIELD_RESULT, old_id, {timer_completed = true}),
                }
                if not in_flight then
                    table.insert(commands, data_command(fixture.root, consts.DATA_TYPE.NODE_YIELD, current_id, {
                        yield_id = current_id, yield_context = {wait_for_timer = true, timeout_deadline = deadline}}))
                end
                -- A stale replay may be newer than the current reservation's yield.
                table.insert(commands, data_command(fixture.root, consts.DATA_TYPE.NODE_YIELD, old_id, {
                    yield_id = old_id, yield_context = {wait_for_timer = true, timeout_deadline = old_deadline}}))
                submit(fixture, commands)
                local result = restart(fixture, function(snapshot)
                    local wait = snapshot.active_yields[fixture.root]
                    if in_flight then
                        test.is_nil(wait, "an in-flight attempt has no backoff to reconstruct")
                    else
                        test.not_nil(wait)
                        test.eq(wait.yield_id, current_id)
                        test.eq(wait.timeout_deadline, deadline)
                        test.is_nil(wait.signal_data)
                    end
                end)
                if not in_flight then test.is_true(result.passivated) end
            end)
        end

        it("reattaches a completed timer without reintroducing its consumed wake", function()
            local fixture = create_workflow()
            local ws = workflow_state.new(fixture.id) :: any
            test.is_nil(select(2, ws:load_state()))
            local deadline = time.now():add(time.HOUR):format(time.RFC3339NANO)
            ws.active_yields[fixture.root] = {yield_id = "completed", wait_for_timer = true,
                timeout_deadline = deadline, wake_keys = {}, signal_data = {timer_completed = true}, detached = true}
            test.eq(type(orchestrator.track_wait_yield), "function")
            local tracking_state: any = {workflow_state = ws}
            orchestrator.track_wait_yield(tracking_state, fixture.root,
                {yield_id = "completed", wait_for_timer = true, timeout_deadline = deadline}, "node-process")
            test.eq(#ws.active_yields[fixture.root].wake_keys, 0)
            test.is_true(ws.active_yields[fixture.root].signal_data.timer_completed)
        end)

        it("loads an in-flight dispatch reservation from the backlog before recovery", function()
            local fixture = create_workflow()
            submit(fixture, {{type = consts.COMMAND_TYPES.UPDATE_NODE, payload = {
                node_id = fixture.root, metadata = {func_retry = {
                    attempt = 3, first_attempt_at = 123, in_flight = true,
                }},
            }}})
            restart(fixture, function(snapshot)
                local row = (node_reader.with_dataflow(fixture.id) :: any):with_nodes(fixture.root):one()
                test.eq(row.metadata.func_retry.attempt, 3)
                test.is_true(row.metadata.func_retry.in_flight)
                test.eq(snapshot.nodes[fixture.root].status, consts.STATUS.PENDING)
            end)
            assert_backlog_applied(fixture)
        end)

        local transport: any = {}
        before_all(function()
            transport.send = commit._send_process_message
            transport.pending = commit.get_pending_commits
            transport.publish = commit.publish_updates
            commit._send_process_message = function() end
        end)
        after_each(function()
            commit.get_pending_commits = transport.pending
            commit.publish_updates = transport.publish
        end)
        after_all(function()
            commit._send_process_message = transport.send
        end)

        it("loads durable state without applying commits or classifying running nodes", function()
            local fixture = create_workflow()
            submit(fixture, { data_command(fixture.root, consts.DATA_TYPE.NODE_RESULT,
                "result", { success = true }) })
            local published = 0
            commit.publish_updates = function() published = published + 1 end
            local ws = workflow_state.new(fixture.id) :: any
            local _, load_err = ws:load_state()
            test.is_nil(load_err)
            test.eq(ws:get_nodes()[fixture.root].status, consts.STATUS.RUNNING)
            test.eq(#(commit.get_pending_commits(fixture.id) :: any), 1)
            test.eq(published, 0)
            local nodes = (node_reader.with_dataflow(fixture.id) :: any):with_nodes(fixture.root):all()
            test.is_nil(nodes[1].metadata.orchestrator_restarted_at)
        end)

        for _, eligibility in ipairs({ "a superseded activation", "an inactive activation", "a terminal workflow" }) do
            it("does not apply or publish backlog for " .. eligibility, function()
                local terminal = eligibility == "a terminal workflow"
                local fixture = create_workflow()
                local activation, activation_err = commit.request_activation(fixture.id, {}, { notify = false })
                test.is_nil(activation_err)
                if eligibility == "an inactive activation" then
                    local newer, newer_err = commit.request_activation(fixture.id, {}, { notify = false })
                    test.is_nil(newer_err)
                    test.is_true((newer :: any).generation > (activation :: any).generation)
                    local _, release_err = commit.execute(fixture.id, uuid.v7(), {{
                        type = consts.COMMAND_TYPES.PASSIVATE_WORKFLOW, payload = {
                            activation_generation = (newer :: any).generation,
                        },
                    }}, { publish = false })
                    test.is_nil(release_err)
                elseif terminal then
                    local _, complete_err = commit.execute(fixture.id, uuid.v7(), {{
                        type = consts.COMMAND_TYPES.COMPLETE_WORKFLOW, payload = {
                            activation_generation = (activation :: any).generation,
                            status = consts.STATUS.COMPLETED_SUCCESS,
                        },
                    }}, { publish = false })
                    test.is_nil(complete_err)
                end
                submit(fixture, { data_command(fixture.root, consts.DATA_TYPE.NODE_RESULT,
                    "result", { success = true }) })
                local published = 0
                commit.publish_updates = function() published = published + 1 end
                local probe: any = { spawned = {} }
                local result = orchestrator.run({ dataflow_id = fixture.id,
                    activation_generation = (activation :: any).generation + (eligibility == "a superseded activation" and 1 or 0),
                }, runtime_for(probe, function() error("ineligible startup reaches the scheduler") end)) :: any
                test.is_true(result.success)
                test.eq(#(commit.get_pending_commits(fixture.id) :: any), 1,
                    "an ineligible activation leaves the backlog unapplied")
                test.eq(published, 0, "an ineligible activation publishes no commit events")
                test.eq(#probe.spawned, 0)
                test.eq(probe.unregistered, "dataflow." .. fixture.id)
                local nodes = (node_reader.with_dataflow(fixture.id) :: any):with_nodes(fixture.root):all()
                test.eq(nodes[1].status, consts.STATUS.RUNNING)
                test.is_nil(nodes[1].metadata.orchestrator_restarted_at)
            end)
        end

        for _, failure in ipairs({ "load", "apply" }) do
            it("finishes a startup backlog " .. failure .. " failure through durable fenced completion", function()
                local fixture = create_workflow()
                local activation, activation_err = commit.request_activation(fixture.id, {}, { notify = false })
                test.is_nil(activation_err)
                if failure == "load" then
                    commit.get_pending_commits = function(): (nil, string) return nil, "pending storage unavailable" end
                else
                    submit(fixture, {{ type = "invalid.command", payload = {} }})
                end
                local probe: any = { spawned = {} }
                local result = orchestrator.run({ dataflow_id = fixture.id,
                    activation_generation = (activation :: any).generation, on_complete = "test:completed",
                }, runtime_for(probe, function() error("failed startup reaches the scheduler") end)) :: any
                test.is_false(result.success)
                local stored, stored_err = dataflow_repo.get(fixture.id)
                test.is_nil(stored_err)
                test.eq((stored :: any).status, consts.STATUS.COMPLETED_FAILURE,
                    "backlog failures persist terminal state with the activation fence")
                test.eq(probe.hook_id, "test:completed")
                test.eq(probe.hook_args.status, consts.STATUS.COMPLETED_FAILURE)
                test.eq(probe.hook_args.error, result.error)
                test.eq(probe.unregistered, "dataflow." .. fixture.id)
                test.eq(#probe.spawned, 0)
                local durable_activation = activation_repo.get(fixture.id)
                test.is_false((durable_activation :: any).desired_active)
            end)
        end

        it("preserves input requirements configured before loading", function()
            local fixture = create_workflow()
            local ws = workflow_state.new(fixture.id) :: any
            local requirements = { required = { "approval" }, optional = { "context" } }
            ws:set_input_requirements(fixture.root, requirements)
            ws:set_input_requirements(fixture.leaf, { required = { "obsolete" } })
            submit(fixture, {{ type = consts.COMMAND_TYPES.DELETE_NODE, payload = { node_id = fixture.leaf } }})
            local _, load_err = ws:load_state()
            test.is_nil(load_err)
            test.eq(ws:get_scheduler_snapshot().input_tracker.requirements[fixture.root], requirements)
            local pending = commit.get_pending_commits(fixture.id)
            local _, apply_err = ws:process_commits(pending)
            test.is_nil(apply_err)
            local _, recovery_err = ws:recover()
            test.is_nil(recovery_err)
            test.eq(ws:get_scheduler_snapshot().input_tracker.requirements[fixture.root], requirements)
            test.is_nil(ws:get_scheduler_snapshot().input_tracker.requirements[fixture.leaf])
            test.is_nil(ws:get_scheduler_snapshot().input_tracker.available[fixture.leaf])
        end)

        it("resolves execution identity before publishing the recovered backlog", function()
            local fixture = create_workflow()
            submit(fixture, park_commands(fixture, uuid.v7()))
            local probe: any = { spawned = {} }
            local runtime = runtime_for(probe, function() end)
            local resolved, published = false, 0
            runtime.execution_frame = { resolve = function(actor_id: string, actor_context: any): (any?, any?, string?)
                local actor, scope, err = execution_frame.resolve(actor_id, actor_context)
                test.is_nil(err)
                resolved = true
                return actor, scope, err
            end }
            commit.publish_updates = function()
                test.is_true(resolved, "commit publication uses an established execution identity")
                published = published + 1
            end
            local activation = commit.request_activation(fixture.id, {}, { notify = false })
            local result = orchestrator.run({ dataflow_id = fixture.id,
                activation_generation = (activation :: any).generation,
            }, runtime) :: any
            test.is_true(result.passivated)
            test.is_true(published > 0)
            assert_backlog_applied(fixture)
        end)

        for _, outcome in ipairs({
            { status = consts.STATUS.COMPLETED_SUCCESS, discriminator = "result.success", success = true },
            { status = consts.STATUS.COMPLETED_FAILURE, discriminator = "result.error", success = false },
        }) do
            it("applies " .. outcome.status .. " before classifying interrupted nodes", function()
                local fixture = create_workflow()
                local result_id = uuid.v7()
                local node_result = data_command(fixture.root, consts.DATA_TYPE.NODE_RESULT,
                    "result", { success = outcome.success, message = "committed outcome" }, result_id)
                node_result.payload.discriminator = outcome.discriminator
                local output = data_command(fixture.root, consts.DATA_TYPE.WORKFLOW_OUTPUT,
                    "result", { success = outcome.success })
                output.payload.discriminator = outcome.success and "result" or "error"
                submit(fixture, { node_result, output, {
                    type = consts.COMMAND_TYPES.UPDATE_NODE,
                    payload = { node_id = fixture.root, status = outcome.status },
                } })
                local result, probe = restart(fixture, function(snapshot)
                    assert_preserved(fixture, snapshot)
                    test.eq(snapshot.nodes[fixture.root].status, outcome.status)
                end)
                test.eq(result.success, outcome.success)
                test.eq(#probe.spawned, 0, "committed nodes and their dependents do not execute again")
                assert_backlog_applied(fixture)
                local rows = (data_reader.with_dataflow(fixture.id) :: any)
                    :with_nodes(fixture.root):with_data_types(consts.DATA_TYPE.NODE_RESULT):all()
                test.eq(#rows, 1, "the committed node outcome appears exactly once")
                test.eq(rows[1].data_id, result_id)
                local nodes = (node_reader.with_dataflow(fixture.id) :: any):with_nodes(fixture.root):all()
                test.eq(nodes[1].status, outcome.status)
            end)
        end

        for _, success in ipairs({ true, false }) do
            it("recovers a node API final submission with success=" .. tostring(success), function()
                local fixture = create_workflow()
                local nodes = (node_reader.with_dataflow(fixture.id) :: any):with_nodes(fixture.root):all()
                local ctx, node_err = node.new({ dataflow_id = fixture.id, node_id = fixture.root, node = nodes[1] })
                test.is_nil(node_err)
                local submitted, submit_err
                if success then
                    submitted, submit_err = (ctx :: any):complete({ value = "once" })
                else
                    submitted, submit_err = (ctx :: any):fail("committed failure")
                end
                test.is_nil(submit_err)
                test.eq(submitted.success, success)
                local before = (node_reader.with_dataflow(fixture.id) :: any):with_nodes(fixture.root):all()
                test.eq(before[1].status, consts.STATUS.RUNNING, "the final submission is not applied yet")
                local pending = commit.get_pending_commits(fixture.id)
                test.eq(#(pending :: any), 1, "the real node API leaves its final commit in the backlog")

                local result, probe = restart(fixture, function(snapshot) fixture.snapshot = snapshot end)
                test.eq(#probe.spawned, 0, "a node with committed final output does not execute again")
                assert_preserved(fixture, fixture.snapshot)
                test.eq(result.success, success)
                local after = (node_reader.with_dataflow(fixture.id) :: any):with_nodes(fixture.root):all()
                test.eq(after[1].status, success and consts.STATUS.COMPLETED_SUCCESS or consts.STATUS.COMPLETED_FAILURE)
                local results = (data_reader.with_dataflow(fixture.id) :: any)
                    :with_nodes(fixture.root):with_data_types(consts.DATA_TYPE.NODE_RESULT):all()
                test.eq(#results, 1, "recovery creates exactly one terminal result")
                local outputs = (data_reader.with_dataflow(fixture.id) :: any)
                    :with_nodes(fixture.root):with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT):all()
                test.eq(#outputs, 1, "the final output appears exactly once")
                assert_backlog_applied(fixture)
            end)
        end

        it("reconstructs a parked yield from the backlog before recovery", function()
            local fixture = create_workflow()
            local yield_id = uuid.v7()
            submit(fixture, park_commands(fixture, yield_id))
            local result, probe = restart(fixture, function(snapshot)
                assert_preserved(fixture, snapshot)
                test.eq(snapshot.nodes[fixture.root].status, consts.STATUS.WAITING)
                local parked = snapshot.active_yields[fixture.root]
                test.not_nil(parked, "the pending yield participates in startup reconstruction")
                test.eq(parked.yield_id, yield_id)
                test.is_nil(parked.signal_data)
            end)
            test.is_true(result.passivated, "a committed park remains parked")
            test.eq(#probe.spawned, 0, "a parked node does not execute again")
            assert_backlog_applied(fixture)
        end)

        it("reconstructs a pending park together with its pending signal", function()
            local fixture = create_workflow()
            local yield_id = uuid.v7()
            submit(fixture, park_commands(fixture, yield_id))
            submit(fixture, { data_command(fixture.root, consts.DATA_TYPE.NODE_SIGNAL,
                "continue", { answer = "ready" }) })
            local _, probe = restart(fixture, function(snapshot)
                assert_preserved(fixture, snapshot)
                local parked = snapshot.active_yields[fixture.root]
                test.not_nil(parked)
                test.eq(parked.signal_data.answer, "ready", "the committed signal satisfies the committed park")
            end)
            test.eq(#probe.spawned, 1, "the signalled continuation executes once")
            test.eq(probe.spawned[1], fixture.root)
            assert_backlog_applied(fixture)
        end)

        for _, wait_for_signal in ipairs({ false, true }) do
            it("reconstructs an unapplied yield with wait_for_signal=" .. tostring(wait_for_signal), function()
                local fixture = create_workflow()
                local yield_id = uuid.v7()
                submit(fixture, { data_command(fixture.root, consts.DATA_TYPE.NODE_YIELD, yield_id, {
                    node_id = fixture.root, yield_id = yield_id, reply_to = "previous-process",
                    yield_context = { wait_for_signal = wait_for_signal, signal_id = "continue",
                        park_ack = wait_for_signal },
                }) })
                local result, probe = restart(fixture, function(snapshot)
                    test.eq(snapshot.nodes[fixture.root].status, consts.STATUS.PENDING,
                        "an interrupted yielding process still resets")
                    local yielded = snapshot.active_yields[fixture.root]
                    test.not_nil(yielded, "a committed yield is reconstructed before scheduling")
                    test.eq(yielded.yield_id, yield_id)
                    test.is_true(yielded.detached, "the previous reply mailbox has no owner")
                end)
                if wait_for_signal then
                    test.is_true(result.passivated)
                    test.eq(#probe.spawned, 0, "an unsatisfied signal wait stays parked")
                else
                    test.eq(#probe.spawned, 1, "an interrupted continuation resumes once")
                    test.eq(probe.spawned[1], fixture.root)
                end
                assert_backlog_applied(fixture)
            end)
        end

        it("rebuilds input availability from the final applied backlog", function()
            local fixture = create_workflow()
            local transient_input, pending_node = uuid.v7(), uuid.v7()
            submit(fixture, {
                { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                    node_id = pending_node, node_type = "test_node", status = consts.STATUS.PENDING, config = {},
                } },
                data_command(pending_node, consts.DATA_TYPE.NODE_INPUT, "default", { temporary = true }, transient_input),
                { type = consts.COMMAND_TYPES.DELETE_DATA, payload = { data_id = transient_input } },
            })
            restart(fixture, function(snapshot)
                local available = snapshot.input_tracker.available[pending_node] or {}
                test.is_nil(available.default, "deleted backlog input does not make a node runnable")
            end)
        end)

        it("rebuilds pending signal wakes from the final applied backlog", function()
            local fixture = create_workflow()
            local signal_id = uuid.v7()
            submit(fixture, {
                data_command(fixture.root, consts.DATA_TYPE.NODE_SIGNAL, "continue", { answer = "ready" }, signal_id),
                data_command(fixture.root, consts.DATA_TYPE.NODE_SIGNAL_CONSUMED, signal_id, { consumed = true }),
            })
            local ws = workflow_state.new(fixture.id) :: any
            local _, load_err = ws:load_state()
            test.is_nil(load_err)
            local pending, pending_err = commit.get_pending_commits(fixture.id)
            test.is_nil(pending_err)
            local _, apply_err = ws:process_commits(pending)
            test.is_nil(apply_err)
            local _, recovery_err = ws:recover()
            test.is_nil(recovery_err)
            test.eq(#ws:take_unclaimed_signal_wake_keys(), 0,
                "a consumed backlog signal does not leave a pending wake")
        end)

        it("resets genuinely interrupted nodes and invalidates downstream data", function()
            local fixture = create_workflow()
            local _, probe = restart(fixture, function(snapshot)
                test.eq(snapshot.nodes[fixture.root].status, consts.STATUS.PENDING)
                local nodes = (node_reader.with_dataflow(fixture.id) :: any):with_nodes(fixture.root):all()
                test.not_nil(nodes[1].metadata.orchestrator_restarted_at)
                test.eq(snapshot.nodes[fixture.downstream].status, consts.STATUS.PENDING)
                test.eq(snapshot.nodes[fixture.leaf].status, consts.STATUS.PENDING)
                local inputs = (data_reader.with_dataflow(fixture.id) :: any)
                    :with_nodes(fixture.leaf):with_data_types(consts.DATA_TYPE.NODE_INPUT):all()
                test.eq(#inputs, 0, "interrupted producers invalidate downstream routed data")
            end)
            test.eq(#probe.spawned, 1, "the interrupted node executes again")
            test.eq(probe.spawned[1], fixture.root)
        end)
    end)
end

return test.run_cases(define_tests)
