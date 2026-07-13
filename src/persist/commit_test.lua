local test = require("test")
local uuid = require("uuid")
local json = require("json")
local time = require("time")
local sql = require("sql")

local commit = require("commit")
local ops = require("ops")
local consts = require("dataflow_consts")

local function define_tests()
    describe("Commit Module", function()
        local test_ctx = {
            db = nil,
            tx = nil,
            resources = nil,
            isolated_dataflows = {},
            mocks = {}
        }

        -- Mock storage for tracking calls
        local mock_calls = {}

        -- Helper function to setup mocks
        local function setup_mocks()
            mock_calls = {
                process_messages = {},
                user_id_calls = 0,
                timestamp_calls = 0,
            }

            -- Mock process message sending
            test_ctx.mocks.original_send_process_message = commit._send_process_message
            commit._send_process_message = function(_target_process, _topic, _payload)
                table.insert(mock_calls.process_messages, {
                    target_process = _target_process,
                    topic = _topic,
                    payload = _payload
                })
            end

            -- Mock user ID retrieval
            test_ctx.mocks.original_get_current_user_id = commit._get_current_user_id
            commit._get_current_user_id = function()
                mock_calls.user_id_calls = mock_calls.user_id_calls + 1
                return "test-user-123"
            end

            -- Mock timestamp retrieval
            test_ctx.mocks.original_get_current_timestamp = commit._get_current_timestamp
            commit._get_current_timestamp = function()
                mock_calls.timestamp_calls = mock_calls.timestamp_calls + 1
                return "2023-01-01T12:00:00.123456789Z"
            end
        end

        -- Helper function to restore mocks
        local function restore_mocks()
            if test_ctx.mocks.original_send_process_message then
                commit._send_process_message = test_ctx.mocks.original_send_process_message
            end
            if test_ctx.mocks.original_get_current_user_id then
                commit._get_current_user_id = test_ctx.mocks.original_get_current_user_id
            end
            if test_ctx.mocks.original_get_current_timestamp then
                commit._get_current_timestamp = test_ctx.mocks.original_get_current_timestamp
            end
            test_ctx.mocks = {}
        end

        before_each(function()
            setup_mocks()

            local db, err_db = sql.get("app:db")
            if err_db then error("Failed to connect to database: " .. err_db) end
            test_ctx.db = db

            local tx, err_tx = db:begin()
            if err_tx then
                db:release()
                test_ctx.db = nil
                error("Failed to begin transaction: " .. err_tx)
            end
            test_ctx.tx = tx
        end)

        after_each(function()
            restore_mocks()

            if test_ctx.tx then
                test_ctx.tx:rollback()
                test_ctx.tx = nil
            end

            if test_ctx.db then
                test_ctx.db:release()
                test_ctx.db = nil
            end

            if #test_ctx.isolated_dataflows > 0 then
                local cleanup_db, cleanup_db_err = sql.get("app:db")
                if cleanup_db_err then
                    error("Failed to connect for isolated dataflow cleanup: " .. cleanup_db_err)
                end
                for _, dataflow_id in ipairs(test_ctx.isolated_dataflows) do
                    local _, cleanup_err = cleanup_db:execute(
                        "DELETE FROM dataflows WHERE dataflow_id = ?",
                        { dataflow_id }
                    )
                    if cleanup_err then
                        cleanup_db:release()
                        error("Failed to clean isolated dataflow " .. dataflow_id .. ": " .. cleanup_err)
                    end
                end
                cleanup_db:release()
                test_ctx.isolated_dataflows = {}
            end

            test_ctx.resources = nil
        end)

        local function get_test_transaction()
            return test_ctx.tx
        end

        local function setup_test_resources()
            local test_actor_id = "test-user-" .. uuid.v7()
            local tx = get_test_transaction()

            local dataflow_id = uuid.v7()
            local now_ts = time.now():format(time.RFC3339NANO)

            local insert_query = sql.builder.insert("dataflows")
                :set_map({
                    dataflow_id = dataflow_id,
                    actor_id = test_actor_id,
                    type = "test_dataflow",
                    status = "active",
                    metadata = "{}",
                    created_at = now_ts,
                    updated_at = now_ts
                })

            local insert_exec = insert_query:run_with(tx)
            local _, err_insert = insert_exec:exec()

            if err_insert then
                error("Failed to create test dataflow: " .. err_insert)
            end

            test_ctx.resources = {
                actor_id = test_actor_id,
                dataflow_id = dataflow_id
            }

            return test_ctx.resources
        end

        -- Helper function to create isolated dataflow for tests that need their own transactions
        local function create_isolated_dataflow()
            local dataflow_id = uuid.v7()
            local actor_id = "test-actor-" .. uuid.v7()
            local now_ts = time.now():format(time.RFC3339NANO)

            local db, err_db = sql.get("app:db")
            if err_db then
                error("Failed to connect to database: " .. err_db)
            end

            local tx, err_tx = db:begin()
            if err_tx then
                db:release()
                error("Failed to begin transaction: " .. err_tx)
            end

            local insert_query = sql.builder.insert("dataflows")
                :set_map({
                    dataflow_id = dataflow_id,
                    actor_id = actor_id,
                    type = "test_dataflow",
                    status = "active",
                    metadata = "{}",
                    created_at = now_ts,
                    updated_at = now_ts
                })

            local insert_exec = insert_query:run_with(tx)
            local _, err_create = insert_exec:exec()

            if err_create then
                tx:rollback()
                db:release()
                error("Failed to create test dataflow: " .. err_create)
            end

            local _, err_commit = tx:commit()
            if err_commit then
                tx:rollback()
                db:release()
                error("Failed to commit dataflow creation: " .. err_commit)
            end

            db:release()
            table.insert(test_ctx.isolated_dataflows, dataflow_id)
            return dataflow_id
        end

        local function count_rows(tx, table_name, filters)
            local query = sql.builder.select("COUNT(*) AS row_count")
                :from(table_name)

            for _, filter in ipairs(filters or {}) do
                query = query:where(filter[1], filter[2])
            end

            local executor = query:run_with(tx)
            local rows, err = executor:query()
            if err then
                error("Failed to count rows in " .. table_name .. ": " .. err)
            end

            return rows[1].row_count
        end

        local function count_process_messages(target_process, topic)
            local count = 0
            for _, message in ipairs(mock_calls.process_messages) do
                if message.target_process == target_process and message.topic == topic then
                    count = count + 1
                end
            end
            return count
        end

        local function get_commit_op_id(tx, commit_id)
            local query = sql.builder.select("op_id")
                :from("dataflow_commits")
                :where("commit_id = ?", commit_id)
                :limit(1)

            local executor = query:run_with(tx)
            local rows, err = executor:query()
            if err then
                error("Failed to load commit row: " .. err)
            end

            return rows[1] and rows[1].op_id or nil
        end

        describe("publish_updates", function()
            it("should publish to the current actor id", function()
                commit._get_current_user_id = function()
                    return "ambient-user-456"
                end

                local dataflow_id = "test-dataflow-id"
                local result = {
                    changes_made = true,
                    results = {
                        {
                            changes_made = true,
                            input = {
                                type = ops.COMMAND_TYPES.UPDATE_WORKFLOW,
                                payload = {
                                    status = "completed"
                                }
                            }
                        }
                    }
                }

                commit.publish_updates(dataflow_id, "test-op-id", result)

                test.eq(#mock_calls.process_messages, 1)
                local msg = (mock_calls.process_messages :: any)[1]
                test.eq(msg.target_process, "user.ambient-user-456")
                test.eq(msg.topic, "dataflow:" .. dataflow_id)
            end)

            it("should not publish without an ambient actor", function()
                commit._get_current_user_id = function()
                    return ""
                end

                local result = {
                    changes_made = true,
                    results = {
                        {
                            changes_made = true,
                            input = {
                                type = ops.COMMAND_TYPES.UPDATE_WORKFLOW,
                                payload = {
                                    status = "completed"
                                }
                            }
                        }
                    }
                }

                commit.publish_updates("test-dataflow-id", "test-op-id", result)

                test.eq(#mock_calls.process_messages, 0)
            end)

            it("should do nothing when result has no changes", function()
                local result = {
                    changes_made = false,
                    results = {}
                }

                commit.publish_updates("test-dataflow-id", "test-op-id", result)

                test.eq(#mock_calls.process_messages, 0)
                test.eq(mock_calls.user_id_calls, 0)
                test.eq(mock_calls.timestamp_calls, 0)
            end)

            it("should do nothing when result is nil", function()
                commit.publish_updates("test-dataflow-id", "test-op-id", nil)

                test.eq(#mock_calls.process_messages, 0)
            end)

            it("should send workflow updates for CREATE_WORKFLOW command", function()
                local dataflow_id = "test-dataflow-id"
                local op_id = "test-op-id"
                local result = {
                    changes_made = true,
                    results = {
                        {
                            changes_made = true,
                            input = {
                                type = ops.COMMAND_TYPES.CREATE_WORKFLOW,
                                payload = {
                                    type = "test_workflow",
                                    status = "active",
                                    metadata = { test = "data" }
                                }
                            }
                        }
                    }
                }

                commit.publish_updates(dataflow_id, op_id, result)

                test.eq(mock_calls.user_id_calls, 1)
                test.eq(mock_calls.timestamp_calls, 1)
                test.eq(#mock_calls.process_messages, 1)

                local msg = (mock_calls.process_messages :: any)[1]
                test.eq(msg.target_process, "user.test-user-123")
                test.eq(msg.topic, "dataflow:" .. dataflow_id)
                test.eq(msg.payload.dataflow_id, dataflow_id)
                test.not_nil(msg.payload.updated_at)
            end)

            it("should send only node update for CREATE_NODE command", function()
                local dataflow_id = "test-dataflow-id"
                local _op_id = "test-op-id"
                local node_id = "test-node-id"
                local result = {
                    changes_made = true,
                    results = {
                        {
                            changes_made = true,
                            node_id = node_id,
                            input = {
                                type = ops.COMMAND_TYPES.CREATE_NODE,
                                payload = {
                                    node_type = "test_node",
                                    status = "pending",
                                    metadata = { node = "data" }
                                }
                            }
                        }
                    }
                }

                commit.publish_updates(dataflow_id, _op_id, result)

                test.eq(#mock_calls.process_messages, 1)

                local msg = (mock_calls.process_messages :: any)[1]
                test.eq(msg.target_process, "user.test-user-123")
                test.eq(msg.topic, "dataflow:" .. dataflow_id)
                test.eq(msg.payload.node_id, node_id)
                test.eq(msg.payload.op_type, ops.COMMAND_TYPES.CREATE_NODE)
                test.eq(msg.payload.deleted, false)
            end)

            it("should mark deleted=true for DELETE_NODE command", function()
                local dataflow_id = "test-dataflow-id"
                local _op_id = "test-op-id"
                local result = {
                    changes_made = true,
                    results = {
                        {
                            changes_made = true,
                            node_id = "test-node-id",
                            input = {
                                type = ops.COMMAND_TYPES.DELETE_NODE,
                                payload = {}
                            }
                        }
                    }
                }

                commit.publish_updates(dataflow_id, _op_id, result)

                test.eq(#mock_calls.process_messages, 1)
                test.eq((mock_calls.process_messages :: any)[1].payload.deleted, true)
            end)

            it("should send workflow event when no node events present", function()
                local dataflow_id = "test-dataflow-id"
                local op_id = "test-op-id"
                local result = {
                    changes_made = true,
                    results = {
                        {
                            changes_made = true,
                            input = {
                                type = ops.COMMAND_TYPES.UPDATE_WORKFLOW,
                                payload = {
                                    status = "completed",
                                    metadata = { final = "state" }
                                }
                            }
                        }
                    }
                }

                commit.publish_updates(dataflow_id, op_id, result)

                test.eq(mock_calls.user_id_calls, 1)
                test.eq(mock_calls.timestamp_calls, 1)
                test.eq(#mock_calls.process_messages, 1)

                local msg = (mock_calls.process_messages :: any)[1]
                test.eq(msg.target_process, "user.test-user-123")
                test.eq(msg.topic, "dataflow:" .. dataflow_id)
                test.eq(msg.payload.dataflow_id, dataflow_id)
                test.not_nil(msg.payload.updated_at)
            end)

            it("should handle mixed operation types", function()
                local dataflow_id = "test-dataflow-id"
                local _op_id = "test-op-id"
                local result = {
                    changes_made = true,
                    results = {
                        {
                            changes_made = true,
                            input = {
                                type = ops.COMMAND_TYPES.CREATE_WORKFLOW,
                                payload = { type = "test_workflow" }
                            }
                        },
                        {
                            changes_made = true,
                            node_id = "test-node-id",
                            input = {
                                type = ops.COMMAND_TYPES.CREATE_NODE,
                                payload = { node_type = "test_node" }
                            }
                        }
                    }
                }

                commit.publish_updates(dataflow_id, _op_id, result)

                -- When there are node events, workflow events are suppressed
                test.eq(#mock_calls.process_messages, 1)

                local msg = (mock_calls.process_messages :: any)[1]
                test.eq(msg.topic, "dataflow:" .. dataflow_id)
                test.eq(msg.payload.node_id, "test-node-id")
                test.eq(msg.payload.op_type, ops.COMMAND_TYPES.CREATE_NODE)
            end)
        end)

        describe("tx_execute", function()
            it("should execute commands within provided transaction", function()
                local resources = setup_test_resources()
                local tx = get_test_transaction()

                local commands = {
                    {
                        type = ops.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = uuid.v7(),
                            node_type = "test_node"
                        }
                    }
                }

                local result, err = commit.tx_execute(tx, resources.dataflow_id, nil, commands, { publish = false })

                test.is_nil(err)
                test.not_nil(result)
                test.is_true(result.changes_made)
                test.eq(#result.results, 1)
                test.not_nil(result.results[1].node_id)

                test.eq(#mock_calls.process_messages, 0)
            end)

            it("should fail with missing transaction", function()
                local commands = {
                    {
                        type = ops.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = uuid.v7(),
                            node_type = "test_node"
                        }
                    }
                }

                local result, err = commit.tx_execute(nil, uuid.v7(), nil, commands)

                test.is_nil(result)
                test.contains(err, "Transaction is required")
            end)

            it("should expand APPLY_COMMIT commands", function()
                local resources = setup_test_resources()
                local tx = get_test_transaction()

                local commit_id = uuid.v7()
                local commit_payload = {
                    op_id = uuid.v7(),
                    commands = {
                        {
                            type = ops.COMMAND_TYPES.CREATE_NODE,
                            payload = {
                                node_id = uuid.v7(),
                                node_type = "commit_test_node"
                            }
                        }
                    }
                }

                local payload_json = json.encode(commit_payload)
                local now_ts = time.now():format(time.RFC3339NANO)

                local insert_query = sql.builder.insert("dataflow_commits")
                    :set_map({
                        commit_id = commit_id,
                        dataflow_id = resources.dataflow_id,
                        op_id = sql.as.null(),
                        payload = payload_json,
                        metadata = "{}",
                        created_at = now_ts
                    })

                local insert_exec = insert_query:run_with(tx)
                local _, insert_err = insert_exec:exec()
                test.is_nil(insert_err)

                local commands = {
                    {
                        type = "APPLY_COMMIT",
                        payload = {
                            commit_id = commit_id
                        }
                    }
                }

                local result, err = commit.tx_execute(tx, resources.dataflow_id, nil, commands, { publish = false })

                test.is_nil(err)
                test.not_nil(result)
                test.is_true(result.changes_made)
                test.not_nil(result.commit_ids)
                test.eq(#result.commit_ids, 1)
                test.eq(result.commit_ids[1], commit_id)

                test.eq(#result.results, 2)
            end)

            it("should skip replaying an already applied commit", function()
                local resources = setup_test_resources()
                local tx = get_test_transaction()
                local commit_id = uuid.v7()
                local op_id = uuid.v7()
                local node_type = "replay_guard_node"
                local data_type = "replay_guard_data"
                local data_key = "replay-guard-key"
                local commit_payload = {
                    op_id = op_id,
                    commands = {
                        {
                            type = ops.COMMAND_TYPES.CREATE_NODE,
                            payload = {
                                node_type = node_type
                            }
                        },
                        {
                            type = ops.COMMAND_TYPES.CREATE_DATA,
                            payload = {
                                data_type = data_type,
                                key = data_key,
                                content = "durable replay test"
                            }
                        }
                    }
                }

                local payload_json = json.encode(commit_payload)
                local now_ts = time.now():format(time.RFC3339NANO)

                local insert_query = sql.builder.insert("dataflow_commits")
                    :set_map({
                        commit_id = commit_id,
                        dataflow_id = resources.dataflow_id,
                        op_id = sql.as.null(),
                        payload = payload_json,
                        metadata = "{}",
                        created_at = now_ts
                    })

                local insert_exec = insert_query:run_with(tx)
                local _, insert_err = insert_exec:exec()
                test.is_nil(insert_err)

                local apply_commands = {
                    {
                        type = consts.COMMAND.APPLY_COMMIT,
                        payload = {
                            commit_id = commit_id
                        }
                    }
                }

                local result1, err1 = commit.tx_execute(tx, resources.dataflow_id, op_id, apply_commands, { publish = false })

                test.is_nil(err1)
                test.not_nil(result1)
                test.is_true(result1.changes_made)
                test.eq(count_rows(tx, "dataflow_nodes", {
                    { "dataflow_id = ?", resources.dataflow_id },
                    { "type = ?", node_type }
                }), 1)
                test.eq(count_rows(tx, "dataflow_data", {
                    { "dataflow_id = ?", resources.dataflow_id },
                    { "type = ?", data_type },
                    { "key = ?", data_key }
                }), 1)
                test.eq(get_commit_op_id(tx, commit_id), op_id)

                local result2, err2 = commit.tx_execute(tx, resources.dataflow_id, op_id, apply_commands, { publish = false })

                test.is_nil(err2)
                test.not_nil(result2)
                test.is_false(result2.changes_made)
                test.eq(#result2.results, 0)
                test.not_nil(result2.skipped_commit_ids)
                test.eq(#(result2.skipped_commit_ids :: any), 1)
                test.eq((result2.skipped_commit_ids :: any)[1], commit_id)

                test.eq(count_rows(tx, "dataflow_nodes", {
                    { "dataflow_id = ?", resources.dataflow_id },
                    { "type = ?", node_type }
                }), 1)
                test.eq(count_rows(tx, "dataflow_data", {
                    { "dataflow_id = ?", resources.dataflow_id },
                    { "type = ?", data_type },
                    { "key = ?", data_key }
                }), 1)
                test.eq(get_commit_op_id(tx, commit_id), op_id)
            end)
        end)

        describe("get_pending_commits - No Transaction Conflicts", function()
            it("should return empty array when no commits exist", function()
                if test_ctx.tx then
                    test_ctx.tx:rollback()
                    test_ctx.tx = nil
                end
                if test_ctx.db then
                    test_ctx.db:release()
                    test_ctx.db = nil
                end

                local dataflow_id = create_isolated_dataflow()

                local commits, err = commit.get_pending_commits(dataflow_id)

                test.is_nil(err)
                test.not_nil(commits)
                test.eq(#commits, 0)
            end)

            it("should return all commits when no last_commit_id using submit", function()
                if test_ctx.tx then
                    test_ctx.tx:rollback()
                    test_ctx.tx = nil
                end
                if test_ctx.db then
                    test_ctx.db:release()
                    test_ctx.db = nil
                end

                local dataflow_id = create_isolated_dataflow()

                local commands = {
                    {
                        type = ops.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = uuid.v7(),
                            node_type = "pending_test_node"
                        }
                    }
                }

                local submit_result, submit_err = commit.submit(dataflow_id, nil, commands)
                test.is_nil(submit_err)
                test.not_nil(submit_result.commit_id)

                local commits, err = commit.get_pending_commits(dataflow_id)

                test.is_nil(err)
                test.not_nil(commits)
                test.eq(#commits, 1)
                test.eq((commits :: any)[1], submit_result.commit_id)
            end)

            it("should fail with missing dataflow_id", function()
                local commits, err = commit.get_pending_commits(nil)

                test.is_nil(commits)
                test.contains(err, "Dataflow ID is required")
            end)
        end)

        describe("Integration Tests - No Transaction Conflicts", function()
            it("should handle full workflow from submit to execute", function()
                if test_ctx.tx then
                    test_ctx.tx:rollback()
                    test_ctx.tx = nil
                end
                if test_ctx.db then
                    test_ctx.db:release()
                    test_ctx.db = nil
                end

                local dataflow_id = create_isolated_dataflow()

                -- Step 1: Submit a commit
                local commands = {
                    {
                        type = ops.COMMAND_TYPES.CREATE_NODE,
                        payload = {
                            node_id = uuid.v7(),
                            node_type = "integration_test_node"
                        }
                    }
                }

                local submit_result, submit_err = commit.submit(dataflow_id, nil, commands)
                test.is_nil(submit_err)
                test.not_nil(submit_result.commit_id)

                -- Step 2: Get pending commits
                local pending_commits, pending_err = commit.get_pending_commits(dataflow_id)
                test.is_nil(pending_err)
                test.eq(#pending_commits, 1)
                test.eq((pending_commits :: any)[1], submit_result.commit_id)

                -- Step 3: Execute the commit
                local apply_commands = {
                    {
                        type = "APPLY_COMMIT",
                        payload = {
                            commit_id = submit_result.commit_id
                        }
                    }
                }

                local execute_result, execute_err = commit.execute(dataflow_id, nil, apply_commands, { publish = false })
                test.is_nil(execute_err)
                test.is_true(execute_result.changes_made)
                test.eq((execute_result :: any).commit_ids[1], submit_result.commit_id)

                -- Step 4: Verify no more pending commits
                local final_pending, final_err = commit.get_pending_commits(dataflow_id)
                test.is_nil(final_err)
                test.eq(#final_pending, 0)
            end)
        end)

        describe("wake index notification", function()
            it("notifies once after a committed wake-index transition", function()
                if test_ctx.tx then test_ctx.tx:rollback(); test_ctx.tx = nil end
                if test_ctx.db then test_ctx.db:release(); test_ctx.db = nil end
                local dataflow_id = create_isolated_dataflow()
                local wake_db, wake_db_err = sql.get("app:db")
                test.is_nil(wake_db_err)
                test.not_nil(wake_db)
                local _, wake_insert_err = wake_db:execute([[
                    INSERT INTO dataflow_wakes(dataflow_id, wake_key, wake_at)
                    VALUES (?, ?, ?)
                ]], { dataflow_id, "yield:terminal-test", "2099-01-01T00:00:00Z" })
                wake_db:release()
                test.is_nil(wake_insert_err)

                local result, err = commit.execute(dataflow_id, nil, { {
                    type = ops.COMMAND_TYPES.UPDATE_WORKFLOW,
                    payload = { dataflow_id = dataflow_id, status = consts.STATUS.COMPLETED_SUCCESS },
                } }, { publish = false })

                test.is_nil(err)
                test.not_nil(result)
                test.eq(count_process_messages("dataflow.wakes", "dataflow.wake.changed"), 1)
                local message = test.not_nil(mock_calls.process_messages[1]) :: any
                test.eq(message.target_process, "dataflow.wakes")
                test.eq(message.topic, "dataflow.wake.changed")
            end)

            it("does not notify for unrelated commits or failed transactions", function()
                if test_ctx.tx then test_ctx.tx:rollback(); test_ctx.tx = nil end
                if test_ctx.db then test_ctx.db:release(); test_ctx.db = nil end
                local dataflow_id = create_isolated_dataflow()

                local result, err = commit.execute(dataflow_id, nil, { {
                    type = ops.COMMAND_TYPES.CREATE_NODE,
                    payload = { node_id = uuid.v7(), node_type = "notification_test" },
                } }, { publish = false })
                test.is_nil(err)
                test.not_nil(result)
                test.eq(#mock_calls.process_messages, 0)

                result, err = commit.execute(dataflow_id, nil, { {
                    type = ops.COMMAND_TYPES.UPDATE_WORKFLOW,
                    payload = { dataflow_id = dataflow_id, status = consts.STATUS.COMPLETED_SUCCESS },
                } }, { publish = false })
                test.is_nil(err)
                test.not_nil(result)
                test.eq(#mock_calls.process_messages, 0)

                result, err = commit.execute(dataflow_id, nil, { {
                    type = consts.COMMAND.APPLY_COMMIT,
                    payload = { commit_id = uuid.v7() },
                } }, { publish = false })
                test.is_nil(result)
                test.not_nil(err)
                test.eq(#mock_calls.process_messages, 0)
            end)

            it("notifies once after a timed-yield wake is committed to the outbox", function()
                if test_ctx.tx then test_ctx.tx:rollback(); test_ctx.tx = nil end
                if test_ctx.db then test_ctx.db:release(); test_ctx.db = nil end
                local dataflow_id = create_isolated_dataflow()
                local yield_id = uuid.v7()

                local result, err = commit.submit(dataflow_id, nil, { {
                    type = ops.COMMAND_TYPES.CREATE_DATA,
                    payload = {
                        data_id = uuid.v7(),
                        data_type = consts.DATA_TYPE.NODE_YIELD,
                        content = {
                            yield_id = yield_id,
                            yield_context = {
                                timeout_deadline = "2099-01-01T00:00:00Z",
                            },
                        },
                    },
                } })

                test.is_nil(err)
                test.not_nil(result)
                test.eq(count_process_messages("dataflow.wakes", "dataflow.wake.changed"), 1)
            end)
        end)

        describe("Edge Cases and Error Handling", function()
            it("should handle get_pending_commits with non-existent dataflow", function()
                if test_ctx.tx then
                    test_ctx.tx:rollback()
                    test_ctx.tx = nil
                end
                if test_ctx.db then
                    test_ctx.db:release()
                    test_ctx.db = nil
                end

                local fake_dataflow_id = uuid.v7()
                local commits, err = commit.get_pending_commits(fake_dataflow_id)

                test.is_nil(err)
                test.not_nil(commits)
                test.eq(#commits, 0)
            end)

            it("should handle execute with invalid commit ID in APPLY_COMMIT", function()
                if test_ctx.tx then
                    test_ctx.tx:rollback()
                    test_ctx.tx = nil
                end
                if test_ctx.db then
                    test_ctx.db:release()
                    test_ctx.db = nil
                end

                local dataflow_id = create_isolated_dataflow()

                local apply_commands = {
                    {
                        type = "APPLY_COMMIT",
                        payload = {
                            commit_id = uuid.v7()
                        }
                    }
                }

                local result, err = commit.execute(dataflow_id, nil, apply_commands)

                test.is_nil(result)
                test.contains(err, "Commit not found")
            end)

            it("should preserve commit order with rapid submissions", function()
                if test_ctx.tx then
                    test_ctx.tx:rollback()
                    test_ctx.tx = nil
                end
                if test_ctx.db then
                    test_ctx.db:release()
                    test_ctx.db = nil
                end

                local dataflow_id = create_isolated_dataflow()

                local commit_ids = {}
                for i = 1, 5 do
                    local commands = {
                        {
                            type = ops.COMMAND_TYPES.CREATE_NODE,
                            payload = {
                                node_id = uuid.v7(),
                                node_type = "rapid_test_node_" .. i
                            }
                        }
                    }

                    local result, err = commit.submit(dataflow_id, nil, commands)
                    test.is_nil(err)
                    table.insert(commit_ids, result.commit_id)
                end

                local pending_commits, pending_err = commit.get_pending_commits(dataflow_id)
                test.is_nil(pending_err)
                test.eq(#pending_commits, 5)

                -- Verify commit IDs are in ascending order (UUID v7 time-based)
                for i = 2, #pending_commits do
                    test.is_true((pending_commits :: any)[i] > (pending_commits :: any)[i-1])
                end

                -- Verify they match submitted order
                for i = 1, #commit_ids do
                    test.eq((pending_commits :: any)[i], commit_ids[i])
                end
            end)

            it("should handle submit with invalid parameters", function()
                if test_ctx.tx then
                    test_ctx.tx:rollback()
                    test_ctx.tx = nil
                end
                if test_ctx.db then
                    test_ctx.db:release()
                    test_ctx.db = nil
                end

                local _result1, err1 = commit.submit("", nil, {})
                test.is_nil(_result1)
                test.contains(err1, "Dataflow ID is required")

                local _result2, err2 = commit.submit(uuid.v7(), nil, nil)
                test.is_nil(_result2)
                test.contains(err2, "Commands must be a table or array of commands")
            end)
        end)


    end)
end

return test.run_cases(define_tests)
