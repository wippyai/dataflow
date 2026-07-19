local test = require("test")
local uuid = require("uuid")
local time = require("time")
local sql = require("sql")
local json = require("json")
local commit = require("commit")
local workflow_state = require("workflow_state")
local consts = require("consts")
local data_reader = require("data_reader")

local function rebind(query, db_type)
    if db_type ~= "postgres" then return query end
    local i = 0
    return (query:gsub("%?", function()
        i = i + 1
        return "$" .. i
    end))
end

type TestDataRecord = {
    data_id: string?,
    node_id: string?,
    type: string,
    discriminator: string?,
    key: string?,
    content: table | string | number | boolean | nil,
    content_type: string?,
    metadata: string?,
    created_at: string?,
}

local function define_tests()
    describe("WorkflowState", function()
        local test_ctx: {
            db: any?,
            tx: any?,
            dataflow_id: string?,
            actor_id: string?,
        } = {
            db = nil,
            tx = nil,
            dataflow_id = nil,
            actor_id = nil
        }

        before_each(function()
            local db, err_db = sql.get("app:db")
            if err_db then error("Failed to connect to database: " .. err_db) end
            test_ctx.db = db

            local tx, err_tx = db:begin()
            if err_tx then
                db:release()
                error("Failed to begin transaction: " .. err_tx)
            end
            test_ctx.tx = tx

            test_ctx.dataflow_id = uuid.v7()
            test_ctx.actor_id = uuid.v7()
            local now_ts: string = time.now():format(time.RFC3339NANO)

            local _, err_create = tx:execute(rebind([[
                INSERT INTO dataflows (
                    dataflow_id, actor_id, type, status, metadata, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ]], tx:db_type()), {
                test_ctx.dataflow_id,
                test_ctx.actor_id,
                "test_workflow",
                "active",
                "{}",
                now_ts,
                now_ts
            })

            if err_create then
                tx:rollback()
                db:release()
                error("Failed to create test dataflow: " .. err_create)
            end
        end)

        after_each(function()
            if test_ctx.tx then
                test_ctx.tx:rollback()
                test_ctx.tx = nil
            end

            if test_ctx.db then
                test_ctx.db:release()
                test_ctx.db = nil
            end

            test_ctx.dataflow_id = nil
            test_ctx.actor_id = nil
        end)

        local function create_test_nodes(tx: any, dataflow_id: string, nodes: {{[string]: any}})
            for _, node in ipairs(nodes) do
                local now_ts: string = time.now():format(time.RFC3339NANO)
                local parent_id = (node :: any).parent_node_id
                if parent_id == "" then
                    parent_id = nil
                end

                local config = (node :: any).config or "{}"
                if type(config) == "table" then
                    local encoded_config, config_err = json.encode(config)
                    if config_err then
                        error("Failed to encode test node config: " .. config_err)
                    end
                    config = encoded_config
                end

                local metadata = (node :: any).metadata or "{}"
                if type(metadata) == "table" then
                    local encoded_metadata, metadata_err = json.encode(metadata)
                    if metadata_err then
                        error("Failed to encode test node metadata: " .. metadata_err)
                    end
                    metadata = encoded_metadata
                end

                local _, err = tx:execute(rebind([[
                    INSERT INTO dataflow_nodes (
                        node_id, dataflow_id, parent_node_id, type, status, config, metadata, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ]], tx:db_type()), {
                    node.node_id,
                    dataflow_id,
                    parent_id,
                    node.type,
                    node.status or consts.STATUS.PENDING,
                    config,
                    metadata,
                    now_ts,
                    now_ts
                })
                if err then
                    error("Failed to create test node: " .. err)
                end
            end
        end

        local function create_test_data(tx: any, dataflow_id: string, data_records: { TestDataRecord })
            for _, data in ipairs(data_records) do
                local created_at: string = data.created_at or time.now():format(time.RFC3339NANO)
                local _, err = tx:execute(rebind([[
                    INSERT INTO dataflow_data (
                        data_id, dataflow_id, node_id, type, discriminator, key, content, content_type, metadata, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ]], tx:db_type()), {
                    data.data_id or uuid.v7(),
                    dataflow_id,
                    data.node_id,
                    data.type,
                    data.discriminator,
                    data.key,
                    data.content or "",
                    data.content_type or "application/json",
                    data.metadata or "{}",
                    created_at
                })
                if err then
                    error("Failed to create test data: " .. err)
                end
            end
        end

        describe("Constructor", function()
            it("should create a new workflow state instance", function()
                local ws = workflow_state.new(test_ctx.dataflow_id) :: any

                test.not_nil(ws)
                test.eq(ws.dataflow_id, test_ctx.dataflow_id)
                test.is_false(ws.loaded)
                test.eq(type(ws.nodes), "table")
                test.eq(type(ws.active_processes), "table")
                test.eq(type(ws.active_yields), "table")
                test.is_false(ws.has_workflow_output)
                test.eq(type(ws.queued_commands), "table")
                test.eq(#ws.queued_commands, 0)
            end)

            it("should fail with missing dataflow_id", function()
                local ws, err = workflow_state.new(nil)

                test.is_nil(ws)
                test.not_nil(err)
                test.contains(err :: string, "Dataflow ID is required")
            end)

            it("should fail with empty dataflow_id", function()
                local ws, err = workflow_state.new("")

                test.is_nil(ws)
                test.not_nil(err)
                test.contains(err :: string, "Dataflow ID is required")
            end)
        end)

        describe("State Loading", function()
            it("should load dataflow and nodes from database", function()
                local node1_id: string = uuid.v7()
                local node2_id: string = uuid.v7()

                create_test_nodes(test_ctx.tx, test_ctx.dataflow_id :: string, {
                    {
                        node_id = node1_id,
                        type = "test_node",
                        parent_node_id = nil
                    },
                    {
                        node_id = node2_id,
                        type = "child_node",
                        parent_node_id = node1_id
                    }
                })

                test_ctx.tx:commit()
                test_ctx.db:release()
                test_ctx.tx = nil
                test_ctx.db = nil

                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                test.not_nil(ws)

                local result, load_err = ws:load_state()
                test.is_nil(load_err)
                test.not_nil(result)

                test.is_true(ws.loaded)
                test.not_nil(ws.nodes[node1_id])
                test.eq(ws.nodes[node1_id].type, "test_node")
                test.not_nil(ws.nodes[node2_id])
                test.eq(ws.nodes[node2_id].parent_node_id, node1_id)
            end)

            it("should detect existing workflow output", function()
                create_test_data(test_ctx.tx, test_ctx.dataflow_id :: string, {
                    {
                        type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                        content = '{"result": "test"}'
                    }
                })

                test_ctx.tx:commit()
                test_ctx.db:release()
                test_ctx.tx = nil
                test_ctx.db = nil

                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                test.not_nil(ws)

                local result, load_err = ws:load_state()
                test.is_nil(load_err)
                test.not_nil(result)

                test.is_true(ws.has_workflow_output)
            end)

            it("should reset RUNNING nodes to PENDING on recovery", function()
                local running_node_id: string = uuid.v7()
                create_test_nodes(test_ctx.tx, test_ctx.dataflow_id :: string, {
                    {
                        node_id = running_node_id,
                        type = "test_node",
                        status = consts.STATUS.RUNNING
                    }
                })

                test_ctx.tx:commit()
                test_ctx.db:release()
                test_ctx.tx = nil
                test_ctx.db = nil

                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                test.not_nil(ws)

                local result, load_err = ws:load_state()
                test.is_nil(load_err)
                test.not_nil(result)

                test.eq(ws.nodes[running_node_id].status, consts.STATUS.PENDING)
            end)

            it("recovers RUNNING nodes that already wrote final output", function()
                local running_node_id: string = uuid.v7()

                create_test_nodes(test_ctx.tx, test_ctx.dataflow_id :: string, {
                    {
                        node_id = running_node_id,
                        type = "test_node",
                        status = consts.STATUS.RUNNING,
                        config = {
                            data_targets = {
                                {
                                    data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                    key = "result",
                                    content_type = consts.CONTENT_TYPE.JSON
                                }
                            }
                        }
                    }
                })

                create_test_data(test_ctx.tx, test_ctx.dataflow_id :: string, {
                    {
                        node_id = running_node_id,
                        type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                        key = "result",
                        content = '{"ok":true}',
                        content_type = consts.CONTENT_TYPE.JSON
                    }
                })

                test_ctx.tx:commit()
                test_ctx.db:release()
                test_ctx.tx = nil
                test_ctx.db = nil

                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                local _result, load_err = ws:load_state()

                test.is_nil(load_err)
                test.eq(ws.nodes[running_node_id].status, consts.STATUS.COMPLETED_SUCCESS)

                local node_result = data_reader.with_dataflow(test_ctx.dataflow_id :: string)
                    :with_nodes(running_node_id)
                    :with_data_types(consts.DATA_TYPE.NODE_RESULT)
                    :one()
                test.not_nil(node_result)
                test.eq(node_result.discriminator, "result.success")
            end)

            it("resets downstream completed nodes and removes stale routed data when an upstream producer restarts", function()
                local upstream_id: string = uuid.v7()
                local downstream_id: string = uuid.v7()

                create_test_nodes(test_ctx.tx, test_ctx.dataflow_id :: string, {
                    {
                        node_id = upstream_id,
                        type = "root_node",
                        status = consts.STATUS.COMPLETED_SUCCESS,
                        config = {
                            data_targets = {
                                {
                                    data_type = consts.DATA_TYPE.NODE_INPUT,
                                    node_id = downstream_id,
                                    discriminator = "default"
                                }
                            }
                        }
                    },
                    {
                        node_id = downstream_id,
                        type = "leaf_node",
                        status = consts.STATUS.COMPLETED_SUCCESS,
                        config = {
                            data_targets = {
                                {
                                    data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                    key = "result",
                                    content_type = consts.CONTENT_TYPE.JSON
                                }
                            },
                            inputs = {
                                required = { "default" }
                            }
                        }
                    }
                })

                create_test_data(test_ctx.tx, test_ctx.dataflow_id :: string, {
                    {
                        node_id = downstream_id,
                        type = consts.DATA_TYPE.NODE_INPUT,
                        discriminator = "default",
                        content = '{"stale":true}',
                        content_type = consts.CONTENT_TYPE.JSON
                    },
                    {
                        node_id = downstream_id,
                        type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                        key = "result",
                        content = '{"stale_output":true}',
                        content_type = consts.CONTENT_TYPE.JSON
                    }
                })

                test_ctx.tx:commit()
                test_ctx.db:release()
                test_ctx.tx = nil
                test_ctx.db = nil

                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                local _result, load_err = ws:load_state()

                test.is_nil(load_err)

                ws._reset_node_ids = { [upstream_id] = true }
                local recovery_commands = {}
                ws:_propagate_reset_to_dependents(recovery_commands)

                local commit_result, commit_err = commit.execute(
                    test_ctx.dataflow_id :: string,
                    uuid.v7(),
                    recovery_commands,
                    { publish = false }
                )

                test.is_nil(commit_err)
                test.not_nil(commit_result)

                ws:_load_existing_data()

                test.eq(ws.nodes[downstream_id].status, consts.STATUS.PENDING)
                test.is_nil(ws.input_tracker.available[downstream_id]["default"])
                test.is_false(ws.has_workflow_output)

                local stale_input = data_reader.with_dataflow(test_ctx.dataflow_id :: string)
                    :with_nodes(downstream_id)
                    :with_data_types(consts.DATA_TYPE.NODE_INPUT)
                    :with_data_discriminators("default")
                    :one()
                local stale_output = data_reader.with_dataflow(test_ctx.dataflow_id :: string)
                    :with_nodes(downstream_id)
                    :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                    :with_data_keys("result")
                    :one()

                test.is_nil(stale_input)
                test.is_nil(stale_output)
            end)

            it("should load existing node inputs", function()
                local node_id: string = uuid.v7()
                create_test_nodes(test_ctx.tx, test_ctx.dataflow_id :: string, {
                    {
                        node_id = node_id,
                        type = "test_node"
                    }
                })

                create_test_data(test_ctx.tx, test_ctx.dataflow_id :: string, {
                    {
                        node_id = node_id,
                        type = consts.DATA_TYPE.NODE_INPUT,
                        key = "config",
                        content = '{"value": "test"}'
                    }
                })

                test_ctx.tx:commit()
                test_ctx.db:release()
                test_ctx.tx = nil
                test_ctx.db = nil

                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                test.not_nil(ws)

                ws:set_input_requirements(node_id, {
                    required = { "config" },
                    optional = {}
                })

                local result, load_err = ws:load_state()
                test.is_nil(load_err)
                test.not_nil(result)

                test.not_nil(ws.input_tracker.available[node_id])
                test.is_true(ws.input_tracker.available[node_id]["config"])
            end)
        end)

        describe("Error Query", function()
            it("should return nil when no nodes have failed", function()
                test_ctx.tx:commit()
                test_ctx.db:release()
                test_ctx.tx = nil
                test_ctx.db = nil

                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                test.not_nil(ws)

                local _result, load_err = ws:load_state()
                test.is_nil(load_err)

                local error_summary = ws:get_failed_node_errors()
                test.is_nil(error_summary)
            end)

            it("should return formatted error details for failed nodes with simple string content", function()
                local failed_node_id: string = uuid.v7()

                create_test_nodes(test_ctx.tx, test_ctx.dataflow_id :: string, {
                    {
                        node_id = failed_node_id,
                        type = "test_node",
                        status = consts.STATUS.COMPLETED_FAILURE
                    }
                })

                create_test_data(test_ctx.tx, test_ctx.dataflow_id :: string, {
                    {
                        node_id = failed_node_id,
                        type = consts.DATA_TYPE.NODE_RESULT,
                        discriminator = "result.error",
                        content = "Function 'test_func' not found",
                        content_type = "text/plain"
                    }
                })

                test_ctx.tx:commit()
                test_ctx.db:release()
                test_ctx.tx = nil
                test_ctx.db = nil

                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                test.not_nil(ws)

                local _result, load_err = ws:load_state()
                test.is_nil(load_err)

                local error_summary = ws:get_failed_node_errors() :: string
                test.not_nil(error_summary)
                test.contains(error_summary, "Node [" .. failed_node_id .. "] failed")
                test.contains(error_summary, "Function 'test_func' not found")
                test.is_nil(string.find(error_summary, "{", 1, true))
            end)

            it("should extract error.message from JSON error content", function()
                local failed_node_id: string = uuid.v7()

                create_test_nodes(test_ctx.tx, test_ctx.dataflow_id :: string, {
                    {
                        node_id = failed_node_id,
                        type = "test_node",
                        status = consts.STATUS.COMPLETED_FAILURE
                    }
                })

                local error_json: string = '{"success":false,"message":"Missing func_id in node config","error":{"code":"MISSING_FUNC_ID","message":"Function ID not specified in node configuration"},"data_ids":[]}'
                create_test_data(test_ctx.tx, test_ctx.dataflow_id :: string, {
                    {
                        node_id = failed_node_id,
                        type = consts.DATA_TYPE.NODE_RESULT,
                        discriminator = "result.error",
                        content = error_json,
                        content_type = "application/json"
                    }
                })

                test_ctx.tx:commit()
                test_ctx.db:release()
                test_ctx.tx = nil
                test_ctx.db = nil

                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                test.not_nil(ws)

                local _result, load_err = ws:load_state()
                test.is_nil(load_err)

                local error_summary = ws:get_failed_node_errors() :: string
                test.not_nil(error_summary)
                test.contains(error_summary, "Node [" .. failed_node_id .. "] failed")
                test.contains(error_summary, "Function ID not specified in node configuration")
                test.is_nil(string.find(error_summary, '{"success"', 1, true))
                test.is_nil(string.find(error_summary, '"data_ids"', 1, true))
            end)

            it("should extract top-level message from JSON when error.message not available", function()
                local failed_node_id: string = uuid.v7()

                create_test_nodes(test_ctx.tx, test_ctx.dataflow_id :: string, {
                    {
                        node_id = failed_node_id,
                        type = "test_node",
                        status = consts.STATUS.COMPLETED_FAILURE
                    }
                })

                local error_json: string = '{"success":false,"message":"Configuration validation failed","details":"Missing required field"}'
                create_test_data(test_ctx.tx, test_ctx.dataflow_id :: string, {
                    {
                        node_id = failed_node_id,
                        type = consts.DATA_TYPE.NODE_RESULT,
                        discriminator = "result.error",
                        content = error_json,
                        content_type = "application/json"
                    }
                })

                test_ctx.tx:commit()
                test_ctx.db:release()
                test_ctx.tx = nil
                test_ctx.db = nil

                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                test.not_nil(ws)

                local _result, load_err = ws:load_state()
                test.is_nil(load_err)

                local error_summary = ws:get_failed_node_errors() :: string
                test.not_nil(error_summary)
                test.contains(error_summary, "Node [" .. failed_node_id .. "] failed")
                test.contains(error_summary, "Configuration validation failed")
                test.is_nil(string.find(error_summary, '{"success"', 1, true))
            end)

            it("should fall back to raw content for JSON without meaningful message", function()
                local failed_node_id: string = uuid.v7()

                create_test_nodes(test_ctx.tx, test_ctx.dataflow_id :: string, {
                    {
                        node_id = failed_node_id,
                        type = "test_node",
                        status = consts.STATUS.COMPLETED_FAILURE
                    }
                })

                local error_json: string = '{"code":500,"details":["field1","field2"]}'
                create_test_data(test_ctx.tx, test_ctx.dataflow_id :: string, {
                    {
                        node_id = failed_node_id,
                        type = consts.DATA_TYPE.NODE_RESULT,
                        discriminator = "result.error",
                        content = error_json,
                        content_type = "application/json"
                    }
                })

                test_ctx.tx:commit()
                test_ctx.db:release()
                test_ctx.tx = nil
                test_ctx.db = nil

                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                test.not_nil(ws)

                local _result, load_err = ws:load_state()
                test.is_nil(load_err)

                local error_summary = ws:get_failed_node_errors() :: string
                test.not_nil(error_summary)
                test.contains(error_summary, "Node [" .. failed_node_id .. "] failed")
                test.contains(error_summary, error_json)
            end)

            it("should handle malformed JSON gracefully", function()
                local failed_node_id: string = uuid.v7()

                create_test_nodes(test_ctx.tx, test_ctx.dataflow_id :: string, {
                    {
                        node_id = failed_node_id,
                        type = "test_node",
                        status = consts.STATUS.COMPLETED_FAILURE
                    }
                })

                local malformed_json: string = '{"success":false,"message":"Incomplete JSON'
                create_test_data(test_ctx.tx, test_ctx.dataflow_id :: string, {
                    {
                        node_id = failed_node_id,
                        type = consts.DATA_TYPE.NODE_RESULT,
                        discriminator = "result.error",
                        content = malformed_json,
                        content_type = "application/json"
                    }
                })

                test_ctx.tx:commit()
                test_ctx.db:release()
                test_ctx.tx = nil
                test_ctx.db = nil

                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                test.not_nil(ws)

                local _result, load_err = ws:load_state()
                test.is_nil(load_err)

                local error_summary = ws:get_failed_node_errors() :: string
                test.not_nil(error_summary)
                test.contains(error_summary, "Node [" .. failed_node_id .. "] failed")
                test.contains(error_summary, malformed_json)
            end)

            it("should handle multiple failed nodes", function()
                local failed_node1_id: string = uuid.v7()
                local failed_node2_id: string = uuid.v7()

                create_test_nodes(test_ctx.tx, test_ctx.dataflow_id :: string, {
                    {
                        node_id = failed_node1_id,
                        type = "test_node",
                        status = consts.STATUS.COMPLETED_FAILURE
                    },
                    {
                        node_id = failed_node2_id,
                        type = "test_node",
                        status = consts.STATUS.COMPLETED_FAILURE
                    }
                })

                create_test_data(test_ctx.tx, test_ctx.dataflow_id :: string, {
                    {
                        node_id = failed_node1_id,
                        type = consts.DATA_TYPE.NODE_RESULT,
                        discriminator = "result.error",
                        content = '{"error":{"message":"First JSON error"}}',
                        content_type = "application/json"
                    },
                    {
                        node_id = failed_node2_id,
                        type = consts.DATA_TYPE.NODE_RESULT,
                        discriminator = "result.error",
                        content = "Second string error",
                        content_type = "text/plain"
                    }
                })

                test_ctx.tx:commit()
                test_ctx.db:release()
                test_ctx.tx = nil
                test_ctx.db = nil

                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                test.not_nil(ws)

                local _result, load_err = ws:load_state()
                test.is_nil(load_err)

                local error_summary = ws:get_failed_node_errors() :: string
                test.not_nil(error_summary)
                test.contains(error_summary, "Node [" .. failed_node1_id .. "] failed")
                test.contains(error_summary, "Node [" .. failed_node2_id .. "] failed")
                test.contains(error_summary, "First JSON error")
                test.contains(error_summary, "Second string error")
                test.contains(error_summary, ";")
            end)

            it("should handle failed nodes without error data", function()
                local failed_node_id: string = uuid.v7()

                create_test_nodes(test_ctx.tx, test_ctx.dataflow_id :: string, {
                    {
                        node_id = failed_node_id,
                        type = "test_node",
                        status = consts.STATUS.COMPLETED_FAILURE
                    }
                })

                test_ctx.tx:commit()
                test_ctx.db:release()
                test_ctx.tx = nil
                test_ctx.db = nil

                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                test.not_nil(ws)

                local _result, load_err = ws:load_state()
                test.is_nil(load_err)

                local error_summary = ws:get_failed_node_errors() :: string
                test.not_nil(error_summary)
                test.contains(error_summary, "Node [" .. failed_node_id .. "] failed")
                test.contains(error_summary, "Unknown error")
            end)
        end)

        describe("Scheduler Snapshot", function()
            it("should provide consistent state snapshot for scheduler", function()
                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                test.not_nil(ws)

                ws.nodes["node-1"] = {
                    status = consts.STATUS.PENDING,
                    type = "test_node"
                }
                ws:track_process("node-1", "pid-123")
                ws:set_input_requirements("node-1", {
                    required = { "config" },
                    optional = {}
                })

                local snapshot = ws:get_scheduler_snapshot() :: any

                test.not_nil(snapshot.nodes["node-1"])
                test.is_true(snapshot.active_processes["node-1"])
                test.not_nil(snapshot.input_tracker.requirements["node-1"])
                test.is_false(snapshot.has_workflow_output)
            end)
        end)

        describe("Process Tracking", function()
            it("should track and untrack processes", function()
                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                test.not_nil(ws)

                ws:track_process("node-1", "pid-123")

                test.eq(ws.active_processes["node-1"], "pid-123")
                test.is_true(ws:is_node_active("node-1"))
            end)

            it("should handle process exits", function()
                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                test.not_nil(ws)

                ws.nodes["node-1"] = {
                    status = consts.STATUS.RUNNING,
                    type = "test_node"
                }
                ws:track_process("node-1", "pid-123")

                local exit_info = ws:handle_process_exit("pid-123", true, "success result") :: any

                test.not_nil(exit_info)
                test.eq(exit_info.node_id, "node-1")
                test.is_true(exit_info.success)
                test.is_nil(ws.active_processes["node-1"])
                test.eq(ws.nodes["node-1"].status, consts.STATUS.COMPLETED_SUCCESS)
                test.gt(#ws.queued_commands, 0)
            end)

            it("should handle process exit for unknown PID", function()
                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                test.not_nil(ws)

                local exit_info = ws:handle_process_exit("unknown-pid", true, "result")

                test.is_nil(exit_info)
            end)
        end)

        describe("Yield Tracking", function()
            it("should track and satisfy yields", function()
                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                test.not_nil(ws)

                local yield_info = {
                    yield_id = "yield-123",
                    reply_to = "test-topic",
                    pending_children = {
                        ["child-1"] = consts.STATUS.PENDING
                    },
                    results = {}
                }

                ws:track_yield("parent-1", yield_info)
                test.not_nil(ws.active_yields["parent-1"])
                test.is_true(ws:is_node_active("parent-1"))

                ws:satisfy_yield("parent-1", { result = "test" })
                test.is_nil(ws.active_yields["parent-1"])
                test.gt(#ws.queued_commands, 0)
            end)

            it("should handle yield child completion", function()
                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                test.not_nil(ws)

                ws.nodes["parent-1"] = { status = consts.STATUS.RUNNING, type = "parent" }
                ws.nodes["child-1"] = {
                    status = consts.STATUS.RUNNING,
                    type = "child",
                    parent_node_id = "parent-1"
                }

                local yield_info = {
                    yield_id = "yield-123",
                    pending_children = {
                        ["child-1"] = consts.STATUS.PENDING
                    },
                    results = {}
                }
                ws:track_yield("parent-1", yield_info)
                ws:track_process("child-1", "child-pid")

                local exit_info = ws:handle_process_exit("child-pid", true, "child result") :: any

                test.not_nil(exit_info)
                test.not_nil(exit_info.yield_complete)
                test.eq(exit_info.yield_complete.parent_id, "parent-1")
            end)
        end)

        describe("Input Tracking", function()
            it("should manage input requirements and availability", function()
                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                test.not_nil(ws)

                ws:set_input_requirements("node-1", {
                    required = { "config", "data" },
                    optional = { "metadata" }
                })

                test.not_nil(ws.input_tracker.requirements["node-1"])
                test.eq(type(ws.input_tracker.requirements["node-1"].required), "table")
                test.eq(#ws.input_tracker.requirements["node-1"].required, 2)
                test.eq(type(ws.input_tracker.available["node-1"]), "table")
            end)

            it("should update input availability from data operations", function()
                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                test.not_nil(ws)

                ws:set_input_requirements("node-1", {
                    required = { "config" },
                    optional = {}
                })

                local results = {
                    results = {
                        {
                            input = {
                                type = consts.COMMAND_TYPES.CREATE_DATA,
                                payload = {
                                    data_type = consts.DATA_TYPE.NODE_INPUT,
                                    node_id = "node-1",
                                    key = "config"
                                }
                            }
                        }
                    }
                }

                ws:_update_state_from_results(results)

                test.is_true(ws.input_tracker.available["node-1"]["config"])
            end)
        end)

        describe("Command Queuing and Persistence", function()
            it("should queue and persist commands", function()
                test_ctx.tx:commit()
                test_ctx.db:release()
                test_ctx.tx = nil
                test_ctx.db = nil

                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                test.not_nil(ws)

                local unique_node_id: string = uuid.v7()
                ws:queue_commands({
                    type = consts.COMMAND_TYPES.CREATE_NODE,
                    payload = {
                        node_id = unique_node_id,
                        node_type = "test_node"
                    }
                })

                test.eq(#ws.queued_commands, 1)

                local result = ws:persist() :: any
                test.not_nil(result)
                test.is_true(result.changes_made)
                test.eq(#ws.queued_commands, 0)
            end)

            it("should handle empty command queue", function()
                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                test.not_nil(ws)

                local result = ws:persist() :: any
                test.not_nil(result)
                test.is_false(result.changes_made)
            end)

            it("should queue arrays of commands", function()
                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                test.not_nil(ws)

                ws:queue_commands({
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = { node_id = "node-1", node_type = "test" }
                    },
                    {
                        type = consts.COMMAND_TYPES.CREATE_NODE,
                        payload = { node_id = "node-2", node_type = "test" }
                    }
                })

                test.eq(#ws.queued_commands, 2)
            end)
        end)

        describe("State Updates from Results", function()
            it("does not clean a signal wake before its durable commit is applied", function()
                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                local signal_data_id = uuid.v7()
                local wake_key = "signal:" .. signal_data_id

                ws:observe_signal_wake(wake_key)
                test.eq(#ws:take_unclaimed_signal_wake_keys(), 0,
                    "mailbox observation alone cannot acknowledge the durable wake")

                ws:_update_state_from_results({
                    results = { {
                        input = {
                            type = consts.COMMAND_TYPES.CREATE_DATA,
                            payload = {
                                data_id = signal_data_id,
                                data_type = consts.DATA_TYPE.NODE_SIGNAL,
                                key = "unmatched-signal",
                                content = { decision = "approve" },
                            },
                        },
                    } },
                })
                local applied = ws:take_unclaimed_signal_wake_keys()
                test.eq(#applied, 1)
                test.eq(applied[1], wake_key,
                    "only an applied, unmatched signal can become a cleanup candidate")
            end)

            it("replays a consumed signal when its replacement re-yields in the same owner", function()
                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                ws:track_yield("signal-node", {
                    yield_id = "old-yield",
                    wait_for_signal = true,
                    signal_id = "approval",
                    signal_data = { decision = "approve" },
                    signal_wake_key = "signal:signal-data",
                })
                ws:satisfy_yield("signal-node", { decision = "approve" })

                ws:track_yield("signal-node", {
                    yield_id = "replacement-yield",
                    wait_for_signal = true,
                    signal_id = "approval",
                })
                local replacement = test.not_nil(ws.active_yields["signal-node"]) :: any
                test.eq(replacement.signal_data.decision, "approve")
                test.is_nil(replacement.signal_wake_key,
                    "already-consumed signal must not be consumed a second time")

                ws:satisfy_yield("signal-node", replacement.signal_data)
                local replacement_results = 0
                local consumed_markers = 0
                for _, command in ipairs(ws.queued_commands) do
                    local payload = command.payload or {}
                    if payload.data_type == consts.DATA_TYPE.NODE_YIELD_RESULT and
                        payload.key == "replacement-yield" then
                        replacement_results = replacement_results + 1
                    elseif payload.data_type == consts.DATA_TYPE.NODE_SIGNAL_CONSUMED then
                        consumed_markers = consumed_markers + 1
                    end
                end
                test.eq(replacement_results, 1)
                test.eq(consumed_markers, 1,
                    "only the original satisfaction consumes the signal wake")
            end)

            it("should update node state from CREATE_NODE results", function()
                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                test.not_nil(ws)

                local results = {
                    results = {
                        {
                            node_id = "new-node",
                            input = {
                                type = consts.COMMAND_TYPES.CREATE_NODE,
                                payload = {
                                    node_type = "test_node",
                                    status = consts.STATUS.PENDING
                                }
                            }
                        }
                    }
                }

                ws:_update_state_from_results(results)

                test.not_nil(ws.nodes["new-node"])
                test.eq(ws.nodes["new-node"].type, "test_node")
                test.eq(ws.nodes["new-node"].status, consts.STATUS.PENDING)
            end)

            it("should update node state from UPDATE_NODE results", function()
                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                test.not_nil(ws)

                ws.nodes["existing-node"] = {
                    status = consts.STATUS.PENDING,
                    type = "test_node"
                }

                local results = {
                    results = {
                        {
                            input = {
                                type = consts.COMMAND_TYPES.UPDATE_NODE,
                                payload = {
                                    node_id = "existing-node",
                                    status = consts.STATUS.RUNNING
                                }
                            }
                        }
                    }
                }

                ws:_update_state_from_results(results)

                test.eq(ws.nodes["existing-node"].status, consts.STATUS.RUNNING)
            end)

            it("should detect workflow output creation", function()
                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                test.not_nil(ws)

                local results = {
                    results = {
                        {
                            input = {
                                type = consts.COMMAND_TYPES.CREATE_DATA,
                                payload = {
                                    data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                    content = "test output"
                                }
                            }
                        }
                    }
                }

                ws:_update_state_from_results(results)

                test.is_true(ws.has_workflow_output)
            end)
        end)

        describe("Commit Processing", function()
            it("should process commit IDs and update state", function()
                test_ctx.tx:commit()
                test_ctx.db:release()
                test_ctx.tx = nil
                test_ctx.db = nil

                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                test.not_nil(ws)

                local result = ws:process_commits({}) :: any
                test.not_nil(result)
                test.is_false(result.changes_made)
            end)

            it("projects persisted commit results exactly once", function()
                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                local projections = 0
                local persisted_result = { changes_made = true, results = {} }
                ws._update_state_from_results = function(_self: any, result: any)
                    test.eq(result, persisted_result)
                    projections = projections + 1
                end
                ws.persist = function(self: any)
                    self:_update_state_from_results(persisted_result)
                    self.queued_commands = {}
                    return persisted_result, nil
                end

                local result, err = ws:process_commits({ "commit-1" })
                test.is_nil(err)
                test.eq(result, persisted_result)
                test.eq(projections, 1, "one commit result produces one in-memory projection")
            end)
        end)

        describe("Activity Tracking", function()
            it("should correctly identify active nodes", function()
                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                test.not_nil(ws)

                ws:track_process("node-1", "pid-1")
                test.is_true(ws:is_node_active("node-1"))

                ws:track_yield("node-2", { yield_id = "y1" })
                test.is_true(ws:is_node_active("node-2"))

                ws:track_yield("parent", {
                    yield_id = "y2",
                    pending_children = { ["child"] = consts.STATUS.PENDING }
                })
                test.is_true(ws:is_node_active("child"))

                test.is_false(ws:is_node_active("inactive-node"))
            end)
        end)

        describe("Edge Cases", function()
            it("should handle loading non-existent dataflow", function()
                test_ctx.tx:rollback()
                test_ctx.db:release()
                test_ctx.tx = nil
                test_ctx.db = nil

                local fake_id: string = uuid.v7()
                local ws = workflow_state.new(fake_id) :: any
                test.not_nil(ws)

                local result, load_err = ws:load_state()

                test.is_nil(result)
                test.not_nil(load_err)
                test.contains(load_err :: string, "not found")
            end)

            it("should handle multiple load_state calls", function()
                test_ctx.tx:commit()
                test_ctx.db:release()
                test_ctx.tx = nil
                test_ctx.db = nil

                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                test.not_nil(ws)

                local _result1, load_err1 = ws:load_state()
                test.is_nil(load_err1)
                test.is_true(ws.loaded)

                local _result2, load_err2 = ws:load_state()
                test.is_nil(load_err2)
                test.is_true(ws.loaded)
            end)

            it("should handle yield completion with no children", function()
                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                test.not_nil(ws)

                ws:track_yield("parent", {
                    yield_id = "empty-yield",
                    pending_children = {}
                })

                ws:satisfy_yield("parent", {})
                test.is_nil(ws.active_yields["parent"])
            end)
        end)

        describe("Yield Recovery", function()
            it("abandons a failed parked yield so later signals cannot revive it", function()
                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                ws.active_yields["await-1"] = {
                    yield_id = "yield-1",
                    signal_id = "approval-1",
                    wait_for_signal = true,
                }
                ws:abandon_yield("await-1")
                test.is_nil(ws.active_yields["await-1"])
            end)

            it("retains a declarative park arm across restart recovery", function()
                local parent_id: string = uuid.v7()
                local yield_id: string = uuid.v7()
                create_test_nodes(test_ctx.tx, test_ctx.dataflow_id :: string, {
                    { node_id = parent_id, type = "await_node", status = consts.STATUS.RUNNING },
                })
                create_test_data(test_ctx.tx, test_ctx.dataflow_id :: string, {
                    {
                        node_id = parent_id,
                        type = consts.DATA_TYPE.NODE_YIELD,
                        key = yield_id,
                        content = string.format([[{
                            "node_id":"%s","yield_id":"%s","reply_to":"park.reply",
                            "yield_context":{"run_nodes":[],"wait_for_signal":true,
                            "signal_id":"approval-1","park_ack":true,
                            "arm":{"ref":"inbox:arm","args":{"decision_id":"decision-1"}}}
                        }]], parent_id, yield_id),
                        content_type = "application/json",
                    },
                })
                test_ctx.tx:commit()
                test_ctx.db:release()
                test_ctx.tx = nil
                test_ctx.db = nil

                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                local _result, load_err = ws:load_state()
                test.is_nil(load_err)
                local recovered = test.not_nil(ws.active_yields[parent_id]) :: any
                test.eq(recovered.park_ack, true)
                test.eq(recovered.arm.ref, "inbox:arm")
                test.eq(recovered.arm.args.decision_id, "decision-1")
                test.eq(recovered.signal_id, "approval-1")
                test.eq(recovered.detached, true, "restarted node reattaches with a fresh reply mailbox")
            end)

            it("replays a durably satisfied signal yield after a crash before node completion", function()
                local parent_id = uuid.v7()
                local yield_id = uuid.v7()
                local persisted_retry_yield_id = uuid.v7()
                local signal_data_id = uuid.v7()
                local signal_id = "approval-1"
                create_test_nodes(test_ctx.tx, test_ctx.dataflow_id :: string, {
                    { node_id = parent_id, type = "await_node", status = consts.STATUS.WAITING },
                })
                create_test_data(test_ctx.tx, test_ctx.dataflow_id :: string, {
                    {
                        data_id = signal_data_id,
                        node_id = parent_id,
                        type = consts.DATA_TYPE.NODE_SIGNAL,
                        key = signal_id,
                        content = '{"decision":"approve"}',
                    },
                    {
                        node_id = parent_id,
                        type = consts.DATA_TYPE.NODE_YIELD,
                        key = yield_id,
                        created_at = "2026-07-12T20:00:00.000000000Z",
                        content = string.format([[{
                            "node_id":"%s","yield_id":"%s","reply_to":"old.reply",
                            "yield_context":{"run_nodes":[],"wait_for_signal":true,"signal_id":"%s","park_ack":true}
                        }]], parent_id, yield_id, signal_id),
                    },
                    {
                        node_id = parent_id,
                        type = consts.DATA_TYPE.NODE_YIELD_RESULT,
                        key = yield_id,
                        content = '{"decision":"approve"}',
                    },
                    {
                        node_id = parent_id,
                        type = consts.DATA_TYPE.NODE_SIGNAL_CONSUMED,
                        key = signal_data_id,
                        content = '{"consumed":true}',
                    },
                    {
                        node_id = parent_id,
                        type = consts.DATA_TYPE.NODE_YIELD,
                        key = persisted_retry_yield_id,
                        created_at = "2026-07-12T20:00:01.000000000Z",
                        content = string.format([[{
                            "node_id":"%s","yield_id":"%s","reply_to":"dead.retry.reply",
                            "yield_context":{"run_nodes":[],"wait_for_signal":true,"signal_id":"%s","park_ack":true}
                        }]], parent_id, persisted_retry_yield_id, signal_id),
                    },
                })
                test_ctx.tx:commit()
                test_ctx.db:release()
                test_ctx.tx = nil
                test_ctx.db = nil

                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                local _, load_err = ws:load_state()
                test.is_nil(load_err)

                local recovered = test.not_nil(ws.active_yields[parent_id]) :: any
                test.is_true(recovered.detached)
                test.eq(recovered.yield_id, persisted_retry_yield_id,
                    "newest unsatisfied retry remains the reattachment point")
                test.eq(recovered.signal_data.decision, "approve")
                test.eq(#recovered.wake_keys, 1)
                test.eq(recovered.wake_keys[1], "yield:" .. persisted_retry_yield_id)
                test.is_nil(recovered.signal_wake_key, "consumed signal wake is not re-armed")

                local fresh_yield_id = uuid.v7()
                ws:track_yield(parent_id, {
                    yield_id = fresh_yield_id,
                    wait_for_signal = true,
                    signal_id = signal_id,
                    wake_keys = {
                        recovered.wake_keys[1],
                        "yield:" .. fresh_yield_id,
                    },
                    reply_to = "fresh.reply",
                })
                local attached = test.not_nil(ws.active_yields[parent_id]) :: any
                test.eq(attached.signal_data.decision, "approve")

                ws:satisfy_yield(parent_id, attached.signal_data)
                local yield_results = 0
                local consumed_markers = 0
                local result_keys = {}
                for _, command in ipairs(ws.queued_commands) do
                    local payload = command.payload or {}
                    if payload.data_type == consts.DATA_TYPE.NODE_YIELD_RESULT then
                        yield_results = yield_results + 1
                        result_keys[payload.key] = true
                    elseif payload.data_type == consts.DATA_TYPE.NODE_SIGNAL_CONSUMED then
                        consumed_markers = consumed_markers + 1
                    end
                end
                test.eq(yield_results, 2, "fresh and persisted retry mailboxes are satisfied")
                test.is_true(result_keys[fresh_yield_id])
                test.is_true(result_keys[persisted_retry_yield_id])
                test.eq(consumed_markers, 0, "replay does not consume the signal twice")
            end)

            it("should reconstruct simple yield with pending children", function()
                local parent_id: string = uuid.v7()
                local child1_id: string = uuid.v7()
                local child2_id: string = uuid.v7()
                local yield_id: string = uuid.v7()

                create_test_nodes(test_ctx.tx, test_ctx.dataflow_id :: string, {
                    {
                        node_id = parent_id,
                        type = "parent_node",
                        status = consts.STATUS.RUNNING
                    },
                    {
                        node_id = child1_id,
                        type = "child_node",
                        parent_node_id = parent_id,
                        status = consts.STATUS.PENDING
                    },
                    {
                        node_id = child2_id,
                        type = "child_node",
                        parent_node_id = parent_id,
                        status = consts.STATUS.COMPLETED_SUCCESS
                    }
                })

                create_test_data(test_ctx.tx, test_ctx.dataflow_id :: string, {
                    {
                        node_id = parent_id,
                        type = consts.DATA_TYPE.NODE_YIELD,
                        key = yield_id,
                        content = string.format([[{
                                    "node_id": "%s",
                                    "yield_id": "%s",
                                    "reply_to": "test.yield_reply.%s",
                                    "yield_context": {
                                        "run_nodes": ["%s", "%s"]
                                    },
                                    "timestamp": "2023-01-01T12:00:00Z"
                                }]], parent_id, yield_id, parent_id, child1_id, child2_id),
                        content_type = "application/json"
                    }
                })

                test_ctx.tx:commit()
                test_ctx.db:release()
                test_ctx.tx = nil
                test_ctx.db = nil

                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                test.not_nil(ws)

                local _result, load_err = ws:load_state()
                test.is_nil(load_err)

                test.eq(ws.nodes[parent_id].status, consts.STATUS.PENDING)

                test.not_nil(ws.active_yields[parent_id])
                local yield_info = ws.active_yields[parent_id] :: any
                test.eq(yield_info.yield_id, yield_id)
                test.eq(yield_info.reply_to, "test.yield_reply." .. parent_id)

                test.eq(yield_info.pending_children[child1_id], consts.STATUS.PENDING)
                test.eq(yield_info.pending_children[child2_id], consts.STATUS.COMPLETED_SUCCESS)

                test.is_true(ws:is_node_active(parent_id))
            end)

            it("should reconstruct yield that's ready to be satisfied", function()
                local parent_id: string = uuid.v7()
                local child1_id: string = uuid.v7()
                local child2_id: string = uuid.v7()
                local yield_id: string = uuid.v7()

                create_test_nodes(test_ctx.tx, test_ctx.dataflow_id :: string, {
                    {
                        node_id = parent_id,
                        type = "parent_node",
                        status = consts.STATUS.RUNNING
                    },
                    {
                        node_id = child1_id,
                        type = "child_node",
                        parent_node_id = parent_id,
                        status = consts.STATUS.COMPLETED_SUCCESS
                    },
                    {
                        node_id = child2_id,
                        type = "child_node",
                        parent_node_id = parent_id,
                        status = consts.STATUS.COMPLETED_SUCCESS
                    }
                })

                create_test_data(test_ctx.tx, test_ctx.dataflow_id :: string, {
                    {
                        node_id = parent_id,
                        type = consts.DATA_TYPE.NODE_YIELD,
                        key = yield_id,
                        content = string.format([[{
                                    "node_id": "%s",
                                    "yield_id": "%s",
                                    "reply_to": "test.yield_reply.%s",
                                    "yield_context": {
                                        "run_nodes": ["%s", "%s"]
                                    }
                                }]], parent_id, yield_id, parent_id, child1_id, child2_id)
                    },
                    {
                        node_id = child1_id,
                        type = consts.DATA_TYPE.NODE_RESULT,
                        discriminator = "result.success",
                        content = '{"result": "child1 success"}'
                    },
                    {
                        node_id = child2_id,
                        type = consts.DATA_TYPE.NODE_RESULT,
                        discriminator = "result.success",
                        content = '{"result": "child2 success"}'
                    }
                })

                test_ctx.tx:commit()
                test_ctx.db:release()
                test_ctx.tx = nil
                test_ctx.db = nil

                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                test.not_nil(ws)

                local _result, load_err = ws:load_state()
                test.is_nil(load_err)

                test.not_nil(ws.active_yields[parent_id])
                local yield_info = ws.active_yields[parent_id] :: any

                test.eq(yield_info.pending_children[child1_id], consts.STATUS.COMPLETED_SUCCESS)
                test.eq(yield_info.pending_children[child2_id], consts.STATUS.COMPLETED_SUCCESS)

                test.not_nil(yield_info.results[child1_id])
                test.not_nil(yield_info.results[child2_id])
            end)

            it("picks most recent unsatisfied yield on reconstruct", function()
                local parent_id: string = uuid.v7()
                local older_unsatisfied_yield_id: string = uuid.v7()
                local newer_satisfied_yield_id: string = uuid.v7()
                local older_yield_data_id: string = uuid.v7()
                local newer_yield_data_id: string = uuid.v7()
                local shared_created_at = "2026-04-11T00:00:00.000000000Z"

                create_test_nodes(test_ctx.tx, test_ctx.dataflow_id :: string, {
                    {
                        node_id = parent_id,
                        type = "parent_node",
                        status = consts.STATUS.RUNNING
                    }
                })

                create_test_data(test_ctx.tx, test_ctx.dataflow_id :: string, {
                    {
                        data_id = older_yield_data_id,
                        node_id = parent_id,
                        type = consts.DATA_TYPE.NODE_YIELD,
                        key = older_unsatisfied_yield_id,
                        created_at = shared_created_at,
                        content = string.format([[{
                                    "node_id": "%s",
                                    "yield_id": "%s",
                                    "reply_to": "test.yield_reply.%s",
                                    "yield_context": {"run_nodes": []}
                                }]], parent_id, older_unsatisfied_yield_id, parent_id)
                    }
                })

                create_test_data(test_ctx.tx, test_ctx.dataflow_id :: string, {
                    {
                        data_id = newer_yield_data_id,
                        node_id = parent_id,
                        type = consts.DATA_TYPE.NODE_YIELD,
                        key = newer_satisfied_yield_id,
                        created_at = shared_created_at,
                        content = string.format([[{
                                    "node_id": "%s",
                                    "yield_id": "%s",
                                    "reply_to": "test.yield_reply.%s",
                                    "yield_context": {"run_nodes": []}
                                }]], parent_id, newer_satisfied_yield_id, parent_id)
                    },
                    {
                        node_id = parent_id,
                        type = consts.DATA_TYPE.NODE_YIELD_RESULT,
                        key = newer_satisfied_yield_id,
                        content = "{}"
                    }
                })

                test_ctx.tx:commit()
                test_ctx.db:release()
                test_ctx.tx = nil
                test_ctx.db = nil

                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                test.not_nil(ws)

                local _result, load_err = ws:load_state()
                test.is_nil(load_err)

                test.not_nil(ws.active_yields[parent_id])
                test.eq((ws.active_yields[parent_id] :: any).yield_id, older_unsatisfied_yield_id)
            end)

            it("prefers the highest v7 data_id when yield rows share created_at", function()
                local parent_id: string = uuid.v7()
                local first_data_id: string = uuid.v7()
                local second_data_id: string = uuid.v7()
                local third_data_id: string = uuid.v7()
                local shared_created_at = "2026-04-11T00:00:00.000000000Z"
                local first_yield_id = "yield-first"
                local second_yield_id = "yield-second"
                local third_yield_id = "yield-third"

                test.is_true(first_data_id < second_data_id)
                test.is_true(second_data_id < third_data_id)

                create_test_nodes(test_ctx.tx, test_ctx.dataflow_id :: string, {
                    {
                        node_id = parent_id,
                        type = "parent_node",
                        status = consts.STATUS.PENDING
                    }
                })

                create_test_data(test_ctx.tx, test_ctx.dataflow_id :: string, {
                    {
                        data_id = first_data_id,
                        node_id = parent_id,
                        type = consts.DATA_TYPE.NODE_YIELD,
                        key = first_yield_id,
                        created_at = shared_created_at,
                        content = string.format([[{
                                    "node_id": "%s",
                                    "yield_id": "%s",
                                    "reply_to": "test.yield_reply.%s",
                                    "yield_context": {"run_nodes": []}
                                }]], parent_id, first_yield_id, parent_id)
                    },
                    {
                        data_id = second_data_id,
                        node_id = parent_id,
                        type = consts.DATA_TYPE.NODE_YIELD,
                        key = second_yield_id,
                        created_at = shared_created_at,
                        content = string.format([[{
                                    "node_id": "%s",
                                    "yield_id": "%s",
                                    "reply_to": "test.yield_reply.%s",
                                    "yield_context": {"run_nodes": []}
                                }]], parent_id, second_yield_id, parent_id)
                    },
                    {
                        data_id = third_data_id,
                        node_id = parent_id,
                        type = consts.DATA_TYPE.NODE_YIELD,
                        key = third_yield_id,
                        created_at = shared_created_at,
                        content = string.format([[{
                                    "node_id": "%s",
                                    "yield_id": "%s",
                                    "reply_to": "test.yield_reply.%s",
                                    "yield_context": {"run_nodes": []}
                                }]], parent_id, third_yield_id, parent_id)
                    }
                })

                test_ctx.tx:commit()
                test_ctx.db:release()
                test_ctx.tx = nil
                test_ctx.db = nil

                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                test.not_nil(ws)

                local _result, load_err = ws:load_state()
                test.is_nil(load_err)
                test.not_nil(ws.active_yields[parent_id])
                test.eq((ws.active_yields[parent_id] :: any).yield_id, third_yield_id)
            end)

            it("should handle multiple yields to reconstruct", function()
                local parent1_id: string = uuid.v7()
                local parent2_id: string = uuid.v7()
                local child1_id: string = uuid.v7()
                local child2_id: string = uuid.v7()

                create_test_nodes(test_ctx.tx, test_ctx.dataflow_id :: string, {
                    {
                        node_id = parent1_id,
                        type = "parent_node",
                        status = consts.STATUS.RUNNING
                    },
                    {
                        node_id = parent2_id,
                        type = "parent_node",
                        status = consts.STATUS.RUNNING
                    },
                    {
                        node_id = child1_id,
                        type = "child_node",
                        parent_node_id = parent1_id,
                        status = consts.STATUS.PENDING
                    },
                    {
                        node_id = child2_id,
                        type = "child_node",
                        parent_node_id = parent2_id,
                        status = consts.STATUS.PENDING
                    }
                })

                create_test_data(test_ctx.tx, test_ctx.dataflow_id :: string, {
                    {
                        node_id = parent1_id,
                        type = consts.DATA_TYPE.NODE_YIELD,
                        key = uuid.v7(),
                        content = string.format([[{
                                    "node_id": "%s",
                                    "yield_id": "yield1",
                                    "yield_context": {"run_nodes": ["%s"]}
                                }]], parent1_id, child1_id)
                    },
                    {
                        node_id = parent2_id,
                        type = consts.DATA_TYPE.NODE_YIELD,
                        key = uuid.v7(),
                        content = string.format([[{
                                    "node_id": "%s",
                                    "yield_id": "yield2",
                                    "yield_context": {"run_nodes": ["%s"]}
                                }]], parent2_id, child2_id)
                    }
                })

                test_ctx.tx:commit()
                test_ctx.db:release()
                test_ctx.tx = nil
                test_ctx.db = nil

                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                test.not_nil(ws)

                local _result, load_err = ws:load_state()
                test.is_nil(load_err)

                test.not_nil(ws.active_yields[parent1_id])
                test.not_nil(ws.active_yields[parent2_id])
                test.eq((ws.active_yields[parent1_id] :: any).yield_id, "yield1")
                test.eq((ws.active_yields[parent2_id] :: any).yield_id, "yield2")
            end)

            it("should ignore yields for nodes that are no longer PENDING", function()
                local completed_parent_id: string = uuid.v7()
                local child_id: string = uuid.v7()

                create_test_nodes(test_ctx.tx, test_ctx.dataflow_id :: string, {
                    {
                        node_id = completed_parent_id,
                        type = "parent_node",
                        status = consts.STATUS.COMPLETED_SUCCESS
                    },
                    {
                        node_id = child_id,
                        type = "child_node",
                        parent_node_id = completed_parent_id,
                        status = consts.STATUS.PENDING
                    }
                })

                create_test_data(test_ctx.tx, test_ctx.dataflow_id :: string, {
                    {
                        node_id = completed_parent_id,
                        type = consts.DATA_TYPE.NODE_YIELD,
                        key = uuid.v7(),
                        content = string.format([[{
                                    "node_id": "%s",
                                    "yield_id": "stale-yield",
                                    "yield_context": {"run_nodes": ["%s"]}
                                }]], completed_parent_id, child_id)
                    }
                })

                test_ctx.tx:commit()
                test_ctx.db:release()
                test_ctx.tx = nil
                test_ctx.db = nil

                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                test.not_nil(ws)

                local _result, load_err = ws:load_state()
                test.is_nil(load_err)

                test.is_nil(ws.active_yields[completed_parent_id])
                test.is_false(ws:is_node_active(completed_parent_id))
            end)

            it("should handle yields with missing child nodes", function()
                local parent_id: string = uuid.v7()
                local missing_child_id: string = uuid.v7()

                create_test_nodes(test_ctx.tx, test_ctx.dataflow_id :: string, {
                    {
                        node_id = parent_id,
                        type = "parent_node",
                        status = consts.STATUS.RUNNING
                    }
                })

                create_test_data(test_ctx.tx, test_ctx.dataflow_id :: string, {
                    {
                        node_id = parent_id,
                        type = consts.DATA_TYPE.NODE_YIELD,
                        key = uuid.v7(),
                        content = string.format([[{
                                    "node_id": "%s",
                                    "yield_id": "orphan-yield",
                                    "yield_context": {"run_nodes": ["%s"]}
                                }]], parent_id, missing_child_id)
                    }
                })

                test_ctx.tx:commit()
                test_ctx.db:release()
                test_ctx.tx = nil
                test_ctx.db = nil

                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                test.not_nil(ws)

                local _result, load_err = ws:load_state()
                test.is_nil(load_err)

                test.not_nil(ws.active_yields[parent_id])
                local yield_info = ws.active_yields[parent_id] :: any
                test.is_nil(yield_info.pending_children[missing_child_id])
            end)

            it("should handle empty yields (no run_nodes)", function()
                local parent_id: string = uuid.v7()

                create_test_nodes(test_ctx.tx, test_ctx.dataflow_id :: string, {
                    {
                        node_id = parent_id,
                        type = "parent_node",
                        status = consts.STATUS.RUNNING
                    }
                })

                create_test_data(test_ctx.tx, test_ctx.dataflow_id :: string, {
                    {
                        node_id = parent_id,
                        type = consts.DATA_TYPE.NODE_YIELD,
                        key = uuid.v7(),
                        content = string.format([[{
                                    "node_id": "%s",
                                    "yield_id": "empty-yield",
                                    "yield_context": {"run_nodes": []}
                                }]], parent_id)
                    }
                })

                test_ctx.tx:commit()
                test_ctx.db:release()
                test_ctx.tx = nil
                test_ctx.db = nil

                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                test.not_nil(ws)

                local _result, load_err = ws:load_state()
                test.is_nil(load_err)

                test.not_nil(ws.active_yields[parent_id])
                local yield_info = ws.active_yields[parent_id] :: any
                test.is_nil(next(yield_info.pending_children))
            end)

            it("should reconstruct child path correctly", function()
                local grandparent_id: string = uuid.v7()
                local parent_id: string = uuid.v7()
                local child_id: string = uuid.v7()

                create_test_nodes(test_ctx.tx, test_ctx.dataflow_id :: string, {
                    {
                        node_id = grandparent_id,
                        type = "grandparent_node",
                        status = consts.STATUS.RUNNING
                    },
                    {
                        node_id = parent_id,
                        type = "parent_node",
                        parent_node_id = grandparent_id,
                        status = consts.STATUS.RUNNING
                    },
                    {
                        node_id = child_id,
                        type = "child_node",
                        parent_node_id = parent_id,
                        status = consts.STATUS.PENDING
                    }
                })

                create_test_data(test_ctx.tx, test_ctx.dataflow_id :: string, {
                    {
                        node_id = grandparent_id,
                        type = consts.DATA_TYPE.NODE_YIELD,
                        key = uuid.v7(),
                        content = string.format([[{
                                    "node_id": "%s",
                                    "yield_id": "gp-yield",
                                    "yield_context": {"run_nodes": ["%s"]},
                                    "child_path": []
                                }]], grandparent_id, parent_id)
                    },
                    {
                        node_id = parent_id,
                        type = consts.DATA_TYPE.NODE_YIELD,
                        key = uuid.v7(),
                        content = string.format([[{
                                    "node_id": "%s",
                                    "yield_id": "p-yield",
                                    "yield_context": {"run_nodes": ["%s"]},
                                    "child_path": ["%s"]
                                }]], parent_id, child_id, grandparent_id)
                    }
                })

                test_ctx.tx:commit()
                test_ctx.db:release()
                test_ctx.tx = nil
                test_ctx.db = nil

                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                test.not_nil(ws)

                local _result, load_err = ws:load_state()
                test.is_nil(load_err)

                test.not_nil(ws.active_yields[grandparent_id])
                test.not_nil(ws.active_yields[parent_id])

                local parent_yield = ws.active_yields[parent_id] :: any
                test.eq(type(parent_yield.child_path), "table")
                test.eq(parent_yield.child_path[1], grandparent_id)
            end)

            it("should reconstruct chain of active yields (grandparent->parent->child)", function()
                local grandparent_id: string = uuid.v7()
                local parent_id: string = uuid.v7()
                local child_id: string = uuid.v7()
                local grandchild_id: string = uuid.v7()

                create_test_nodes(test_ctx.tx, test_ctx.dataflow_id :: string, {
                    {
                        node_id = grandparent_id,
                        type = "chain_grandparent",
                        status = consts.STATUS.RUNNING
                    },
                    {
                        node_id = parent_id,
                        type = "chain_parent",
                        parent_node_id = grandparent_id,
                        status = consts.STATUS.RUNNING
                    },
                    {
                        node_id = child_id,
                        type = "chain_child",
                        parent_node_id = parent_id,
                        status = consts.STATUS.RUNNING
                    },
                    {
                        node_id = grandchild_id,
                        type = "chain_grandchild",
                        parent_node_id = child_id,
                        status = consts.STATUS.PENDING
                    }
                })

                create_test_data(test_ctx.tx, test_ctx.dataflow_id :: string, {
                    {
                        node_id = grandparent_id,
                        type = consts.DATA_TYPE.NODE_YIELD,
                        key = uuid.v7(),
                        content = string.format([[{
                                        "node_id": "%s",
                                        "yield_id": "gp-chain-yield",
                                        "reply_to": "test.yield_reply.%s",
                                        "yield_context": {"run_nodes": ["%s"]},
                                        "child_path": []
                                    }]], grandparent_id, grandparent_id, parent_id)
                    },
                    {
                        node_id = parent_id,
                        type = consts.DATA_TYPE.NODE_YIELD,
                        key = uuid.v7(),
                        content = string.format([[{
                                        "node_id": "%s",
                                        "yield_id": "p-chain-yield",
                                        "reply_to": "test.yield_reply.%s",
                                        "yield_context": {"run_nodes": ["%s"]},
                                        "child_path": ["%s"]
                                    }]], parent_id, parent_id, child_id, grandparent_id)
                    },
                    {
                        node_id = child_id,
                        type = consts.DATA_TYPE.NODE_YIELD,
                        key = uuid.v7(),
                        content = string.format([[{
                                        "node_id": "%s",
                                        "yield_id": "c-chain-yield",
                                        "reply_to": "test.yield_reply.%s",
                                        "yield_context": {"run_nodes": ["%s"]},
                                        "child_path": ["%s", "%s"]
                                    }]], child_id, child_id, grandchild_id, grandparent_id, parent_id)
                    }
                })

                test_ctx.tx:commit()
                test_ctx.db:release()
                test_ctx.tx = nil
                test_ctx.db = nil

                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                test.not_nil(ws)

                local _result, load_err = ws:load_state()
                test.is_nil(load_err)

                test.eq(ws.nodes[grandparent_id].status, consts.STATUS.PENDING)
                test.eq(ws.nodes[parent_id].status, consts.STATUS.PENDING)
                test.eq(ws.nodes[child_id].status, consts.STATUS.PENDING)
                test.eq(ws.nodes[grandchild_id].status, consts.STATUS.PENDING)

                test.not_nil(ws.active_yields[grandparent_id])
                test.not_nil(ws.active_yields[parent_id])
                test.not_nil(ws.active_yields[child_id])

                local gp_yield = ws.active_yields[grandparent_id] :: any
                local p_yield = ws.active_yields[parent_id] :: any
                local c_yield = ws.active_yields[child_id] :: any

                test.eq(gp_yield.yield_id, "gp-chain-yield")
                test.eq(gp_yield.pending_children[parent_id], consts.STATUS.PENDING)

                test.eq(p_yield.yield_id, "p-chain-yield")
                test.eq(p_yield.pending_children[child_id], consts.STATUS.PENDING)
                test.eq(#p_yield.child_path, 1)
                test.eq(p_yield.child_path[1], grandparent_id)

                test.eq(c_yield.yield_id, "c-chain-yield")
                test.eq(c_yield.pending_children[grandchild_id], consts.STATUS.PENDING)
                test.eq(#c_yield.child_path, 2)
                test.eq(c_yield.child_path[1], grandparent_id)
                test.eq(c_yield.child_path[2], parent_id)

                test.is_true(ws:is_node_active(grandparent_id))
                test.is_true(ws:is_node_active(parent_id))
                test.is_true(ws:is_node_active(child_id))
                test.is_true(ws:is_node_active(grandchild_id))

                local snapshot = ws:get_scheduler_snapshot() :: any
                test.not_nil(next(snapshot.active_yields))
                test.not_nil(snapshot.active_yields[grandparent_id])
                test.not_nil(snapshot.active_yields[parent_id])
                test.not_nil(snapshot.active_yields[child_id])
            end)
        end)

        describe("Node Input Configuration", function()
            local config_test_nodes: {string} = {}
            local function has_value(values: {any}?, expected: string): boolean
                if type(values) ~= "table" then
                    return false
                end

                for _, value in ipairs(values) do
                    if value == expected then
                        return true
                    end
                end

                return false
            end

            after_all(function()
                if #config_test_nodes > 0 then
                    local cleanup_db, err_db = sql.get("app:db")
                    if not err_db then
                        local dt = cleanup_db:type()
                        for _, node_id in ipairs(config_test_nodes) do
                            cleanup_db:execute(rebind("DELETE FROM dataflow_nodes WHERE node_id = ?", dt), { node_id })
                            cleanup_db:execute(rebind("DELETE FROM dataflow_data WHERE node_id = ?", dt), { node_id })
                        end
                        cleanup_db:release()
                    end
                    config_test_nodes = {}
                end
            end)

            it("should set input requirements when node config specifies inputs", function()
                test_ctx.tx:commit()
                test_ctx.db:release()
                test_ctx.tx = nil
                test_ctx.db = nil

                local node_id: string = uuid.v7()
                table.insert(config_test_nodes, node_id)

                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                test.not_nil(ws)

                ws:queue_commands({
                    type = consts.COMMAND_TYPES.CREATE_NODE,
                    payload = {
                        node_id = node_id,
                        node_type = "test_node_type",
                        status = consts.STATUS.PENDING,
                        config = {
                            inputs = {
                                required = { "data", "config" },
                                optional = { "metadata" }
                            }
                        }
                    }
                })

                local _result, err = ws:persist()
                test.is_nil(err)

                test.not_nil(ws.input_tracker.requirements[node_id])
                test.is_true(has_value(ws.input_tracker.requirements[node_id].required, "data"))
                test.is_true(has_value(ws.input_tracker.requirements[node_id].required, "config"))
                test.is_true(has_value(ws.input_tracker.requirements[node_id].optional, "metadata"))
            end)

            it("should load input requirements from node config during load_state", function()
                test_ctx.tx:commit()
                test_ctx.db:release()
                test_ctx.tx = nil
                test_ctx.db = nil

                local node_id: string = uuid.v7()
                table.insert(config_test_nodes, node_id)

                local ws1 = workflow_state.new(test_ctx.dataflow_id) :: any
                test.not_nil(ws1)

                ws1:queue_commands({
                    type = consts.COMMAND_TYPES.CREATE_NODE,
                    payload = {
                        node_id = node_id,
                        node_type = "test_node",
                        status = consts.STATUS.PENDING,
                        config = {
                            inputs = {
                                required = { "input_data" },
                                optional = { "extra_params" }
                            }
                        }
                    }
                })

                local _persist_result, persist_err = ws1:persist()
                test.is_nil(persist_err)

                local ws2 = workflow_state.new(test_ctx.dataflow_id) :: any
                test.not_nil(ws2)

                local _load_result, load_err = ws2:load_state()
                test.is_nil(load_err)

                test.not_nil(ws2.input_tracker.requirements[node_id])
                test.is_true(has_value(ws2.input_tracker.requirements[node_id].required, "input_data"))
                test.is_true(has_value(ws2.input_tracker.requirements[node_id].optional, "extra_params"))
            end)

            it("should handle nodes without input configuration gracefully", function()
                test_ctx.tx:commit()
                test_ctx.db:release()
                test_ctx.tx = nil
                test_ctx.db = nil

                local node_id: string = uuid.v7()
                table.insert(config_test_nodes, node_id)

                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                test.not_nil(ws)

                ws:queue_commands({
                    type = consts.COMMAND_TYPES.CREATE_NODE,
                    payload = {
                        node_id = node_id,
                        node_type = "simple_node",
                        status = consts.STATUS.PENDING
                    }
                })

                local _persist_result, persist_err = ws:persist()
                test.is_nil(persist_err)

                test.not_nil(ws.nodes[node_id])
                test.is_nil(ws.input_tracker.requirements[node_id])
            end)

            it("should provide scheduler snapshot with loaded input requirements", function()
                test_ctx.tx:commit()
                test_ctx.db:release()
                test_ctx.tx = nil
                test_ctx.db = nil

                local node_id: string = uuid.v7()
                table.insert(config_test_nodes, node_id)

                local ws = workflow_state.new(test_ctx.dataflow_id) :: any
                test.not_nil(ws)

                ws:queue_commands({
                    type = consts.COMMAND_TYPES.CREATE_NODE,
                    payload = {
                        node_id = node_id,
                        node_type = "test_node",
                        status = consts.STATUS.PENDING,
                        config = {
                            inputs = {
                                required = { "essential_data" },
                                optional = { "nice_to_have" }
                            }
                        }
                    }
                })

                local _persist_result, persist_err = ws:persist()
                test.is_nil(persist_err)

                local snapshot = ws:get_scheduler_snapshot() :: any

                test.not_nil(snapshot.input_tracker.requirements[node_id])
                test.is_true(has_value(snapshot.input_tracker.requirements[node_id].required, "essential_data"))
                test.is_true(has_value(snapshot.input_tracker.requirements[node_id].optional, "nice_to_have"))
            end)
        end)

    end)
end

return test.run_cases(define_tests)
