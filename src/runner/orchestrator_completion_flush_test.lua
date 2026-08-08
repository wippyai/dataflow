local test = require("test")
local uuid = require("uuid")
local orchestrator = require("orchestrator")
local workflow_state = require("workflow_state")
local scheduler = require("scheduler")
local commit = require("commit")
local client = require("client")
local consts = require("consts")
local node_reader = require("node_reader")
local execution_frame = require("execution_frame")

-- Drives the real orchestrator loop with the real workflow state, scheduler and
-- persistence layer. Only the process/channel seams are scripted, and a single
-- transaction abort is simulated for the exit batch: per the workflow-state
-- contract a failed transaction leaves its batch queued for retry, and the
-- completion path must still form a valid generation-fenced batch.
local function define_tests()
    describe("Orchestrator completion after a failed exit-batch transaction", function()
        local function create_probe_workflow(c)
            local node_id = uuid.v7()
            local input_id = uuid.v7()
            local dataflow_id, create_err = (c :: any):create_workflow({
                {
                    type = consts.COMMAND_TYPES.CREATE_NODE,
                    payload = {
                        node_id = node_id,
                        node_type = "test_node",
                        status = consts.STATUS.PENDING,
                        config = {},
                        metadata = { title = "Completion flush probe" }
                    }
                },
                {
                    type = consts.COMMAND_TYPES.CREATE_DATA,
                    payload = {
                        data_id = input_id,
                        data_type = consts.DATA_TYPE.NODE_INPUT,
                        node_id = node_id,
                        key = "default",
                        content = { probe = true },
                        content_type = consts.CONTENT_TYPE.JSON
                    }
                }
            })
            test.is_nil(create_err, "workflow created")
            return dataflow_id, node_id
        end

        -- probes is a shared table: closure upvalue reassignment is not
        -- observable across the runtime boundary here; table mutation is.
        local function build_runtime(dataflow_id, node_id, probes, on_first_event: any)
            local runtime: any = {
                workflow_state = {
                    new = function(id: string): (any?, string?)
                        local ws, ws_err = workflow_state.new(id)
                        if not ws then return nil, ws_err end
                        local ws_any = (ws :: any) :: { [string]: any }
                        local real_persist = ws_any["persist"]
                        -- One transaction abort for the first batch carrying the
                        -- node's terminal update. The batch stays queued exactly
                        -- as a rolled-back transaction leaves it.
                        ws_any["persist"] = function(self: any): (any?, string?)
                            if not probes.aborted then
                                for _, cmd in ipairs(self.queued_commands) do
                                    if cmd.type == consts.COMMAND_TYPES.UPDATE_NODE and
                                        (cmd.payload or {}).node_id == node_id and
                                        (cmd.payload or {}).status == consts.STATUS.COMPLETED_SUCCESS then
                                        probes.aborted = true
                                        return nil, "Failed to persist commands: simulated transaction abort"
                                    end
                                end
                            end
                            return real_persist(self)
                        end
                        return ws, nil
                    end,
                },
                scheduler = scheduler,
                commit = commit,
                activation_repo = {
                    get = function(): (any, nil)
                        return { generation = probes.generation, desired_active = true }, nil
                    end,
                },
                execution_frame = execution_frame,
                wake_repo = { remove = function(): (boolean, nil) return true, nil end },
                overseer = { notify = function(): (boolean, nil) return true, nil end },
                funcs = {
                    new = function(): any
                        local executor: any = {}
                        executor.with_actor = function(self: any): any return self end
                        executor.with_scope = function(self: any): any return self end
                        executor.call = function(): (any, nil) return {}, nil end
                        return executor
                    end,
                },
            }

            local inbox = { case_receive = function(): any return { channel = "inbox" } end }
            local events = { case_receive = function(): any return { channel = "events" } end }
            runtime.process = {
                registry = {
                    lookup = function(): (string?, any?) return nil, "not_found: name not registered" end,
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
            }

            local function child_exit_event(): any
                -- The node process routes its workflow output durably and then
                -- exits, the production interleaving for a clean run.
                local _, submit_err = commit.submit(dataflow_id, uuid.v7(), {
                    {
                        type = consts.COMMAND_TYPES.CREATE_DATA,
                        payload = {
                            data_id = uuid.v7(),
                            data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                            content = { done = true },
                            content_type = consts.CONTENT_TYPE.JSON,
                            discriminator = "result",
                            node_id = node_id
                        }
                    }
                })
                test.is_nil(submit_err, "node output submitted")
                return {
                    ok = true,
                    channel = events,
                    value = {
                        kind = "pid.exit",
                        from = "child-pid",
                        result = { value = { success = true, message = "done", data_ids = {} } },
                    },
                }
            end

            runtime.channel = {
                select = function(): any
                    probes.selects = (probes.selects or 0) + 1
                    if probes.selects == 1 then
                        if on_first_event then on_first_event() end
                        return child_exit_event()
                    end
                    if probes.selects == 2 and probes.redeliver_exit then
                        return child_exit_event()
                    end
                    return { ok = false }
                end,
            }

            return runtime
        end

        local function request_generation(dataflow_id)
            local activation, activation_err = commit.request_activation(dataflow_id, {}, { notify = false })
            test.is_nil(activation_err, "activation requested")
            local generation = tonumber((activation :: any).generation)
            test.not_nil(generation, "activation generation available")
            return generation
        end

        local function assert_completed(c, dataflow_id, node_id)
            local status, status_err = (c :: any):get_status(dataflow_id)
            test.is_nil(status_err, "status readable")
            test.eq(status, consts.STATUS.COMPLETED_SUCCESS, "workflow terminal status persisted")

            local rows = (node_reader.with_dataflow(dataflow_id) :: any)
                :with_nodes(node_id)
                :all() or {}
            test.eq(#rows, 1, "node row present")
            test.eq((rows[1] :: any).status, consts.STATUS.COMPLETED_SUCCESS,
                "node terminal status persisted")
        end

        it("retries the retained batch and persists completion as its own batch", function()
            local c, client_err = client.new()
            test.is_nil(client_err, "client created")

            local dataflow_id, node_id = create_probe_workflow(c)
            local probes: any = { aborted = false }
            probes.generation = request_generation(dataflow_id)

            local runtime = build_runtime(dataflow_id, node_id, probes, nil)
            local result = orchestrator.run({
                dataflow_id = dataflow_id,
                activation_generation = probes.generation,
            }, runtime) :: any

            test.is_true(probes.aborted, "exit-batch transaction abort was exercised")
            test.is_true(result.success,
                "a retained batch does not break completion: " .. tostring(result.error))
            assert_completed(c, dataflow_id, node_id)
        end)

        it("rebuilds from durable state when the retained batch loses the completion fence", function()
            local c, client_err = client.new()
            test.is_nil(client_err, "client created")

            local dataflow_id, node_id = create_probe_workflow(c)
            local probes: any = { aborted = false, redeliver_exit = true }
            probes.generation = request_generation(dataflow_id)
            local stale_generation = probes.generation

            -- The durable generation advances between the exit and the
            -- completion attempt, so the fenced batch is dropped and the run
            -- must re-derive the node outcome from durable state.
            local runtime = build_runtime(dataflow_id, node_id, probes, function()
                probes.generation = request_generation(dataflow_id)
            end)

            local result = orchestrator.run({
                dataflow_id = dataflow_id,
                activation_generation = stale_generation,
            }, runtime) :: any

            test.is_true(probes.aborted, "exit-batch transaction abort was exercised")
            test.is_true(probes.generation > stale_generation, "durable generation advanced mid-run")
            test.is_true(result.success,
                "a dropped fenced batch does not terminalize unpersisted state: " .. tostring(result.error))
            assert_completed(c, dataflow_id, node_id)
        end)
    end)
end

return test.run_cases(define_tests)
