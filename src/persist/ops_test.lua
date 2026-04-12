local test = require("test")
local uuid = require("uuid")
local json = require("json")
local time = require("time")
local sql = require("sql")

local ops = require("ops")

-- rebind ? placeholders to $1,$2 for postgres
local function rebind(query, db_type)
    if db_type ~= "postgres" then return query end
    local i = 0
    return (query:gsub("%?", function()
        i = i + 1
        return "$" .. i
    end))
end

-- tx:query wrapper that auto-rebinds placeholders per db dialect
local function txq(tx, query, params)
    return tx:query(rebind(query, tx:db_type()), params)
end

local function define_tests()
    describe("Operations Module", function()
        local test_ctx = {
            db = nil,
            tx = nil,
            resources = nil
        }

        before_each(function()
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
            if test_ctx.tx then
                test_ctx.tx:rollback()
                test_ctx.tx = nil
            end

            if test_ctx.db then
                test_ctx.db:release()
                test_ctx.db = nil
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
            local now_ts = time.now():format(time.RFC3339)

            local db_type = tx:db_type()
            local _success, err_insert = tx:execute(rebind([[
                INSERT INTO dataflows (
                    dataflow_id, actor_id,  type, status, metadata, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ]], db_type), {
                dataflow_id,
                test_actor_id,
                "test_dataflow",
                "active",
                "{}",
                now_ts,
                now_ts
            })

            if err_insert then
                error("Failed to create test dataflow: " .. err_insert)
            end

            local node_id = uuid.v7()

            _success, err_insert = tx:execute(rebind([[
                INSERT INTO dataflow_nodes (
                    node_id, dataflow_id, type, status, config, metadata, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ]], db_type), {
                node_id,
                dataflow_id,
                "test_node",
                "pending",
                "{}",
                "{}",
                now_ts,
                now_ts
            })

            if err_insert then
                error("Failed to create test node: " .. err_insert)
            end

            test_ctx.resources = {
                actor_id = test_actor_id,
                dataflow_id = dataflow_id,
                node_id = node_id
            }

            return test_ctx.resources
        end

        local function assert_unique_violation(err_message)
            local lowered = string.lower(tostring(err_message or ""))
            local has_unique = string.find(lowered, "unique", 1, true) ~= nil
            local has_duplicate = string.find(lowered, "duplicate", 1, true) ~= nil
            test.is_true(has_unique or has_duplicate,
                "expected unique constraint violation, got: " .. tostring(err_message))
        end

        describe("Basic Operation Execution", function()
            it("should execute a single command successfully", function()
                local resources = setup_test_resources()
                local tx = get_test_transaction()

                local data_id = uuid.v7()
                local command = {
                    type = ops.COMMAND_TYPES.CREATE_DATA,
                    payload = {
                        data_id = data_id,
                        node_id = resources.node_id,
                        key = "test_key",
                        discriminator = "test",
                        data_type = "test_data",
                        content = { value = "test content" },
                        content_type = "application/json",
                        metadata = { source = "ops_test" }
                    }
                }

                local result, err = ops.execute(tx, resources.dataflow_id, nil, command)

                test.is_nil(err)
                test.not_nil(result)
                test.not_nil(result.op_id)
                test.is_true(result.changes_made)
                test.eq(#result.results, 1)
                test.eq(result.results[1].data_id, data_id)

                local query = "SELECT * FROM dataflow_data WHERE data_id = ?"
                local rows, err_query = txq(tx, query, { data_id })

                test.is_nil(err_query)
                test.eq(#rows, 1)
                test.eq(rows[1].data_id, data_id)
            end)

            it("should execute multiple commands in a batch", function()
                local resources = setup_test_resources()
                local tx = get_test_transaction()

                local data_id_1 = uuid.v7()
                local data_id_2 = uuid.v7()

                local commands = {
                    {
                        type = "CREATE_DATA",
                        payload = {
                            data_id = data_id_1,
                            node_id = resources.node_id,
                            key = "batch_key_1",
                            discriminator = "test",
                            data_type = "test_data",
                            content = { value = "batch content 1" },
                            content_type = "application/json"
                        }
                    },
                    {
                        type = "CREATE_DATA",
                        payload = {
                            data_id = data_id_2,
                            key = "batch_key_2",
                            discriminator = "test",
                            data_type = "test_data",
                            content = { value = "batch content 2" },
                            content_type = "application/json"
                        }
                    }
                }

                local result, err = ops.execute(tx, resources.dataflow_id, nil, commands)

                test.is_nil(err)
                test.not_nil(result)
                test.not_nil(result.op_id)
                test.is_true(result.changes_made)
                test.eq(#result.results, 2)
                test.eq(result.results[1].data_id, data_id_1)
                test.eq(result.results[2].data_id, data_id_2)

                local query = "SELECT * FROM dataflow_data WHERE data_id IN (?, ?) ORDER BY key ASC"
                local rows, err_query = txq(tx, query, { data_id_1, data_id_2 })

                test.is_nil(err_query)
                test.eq(#rows, 2)
                test.eq(rows[1].key, "batch_key_1")
                test.eq(rows[2].key, "batch_key_2")
                test.eq(rows[1].node_id, resources.node_id)
                test.is_nil(rows[2].node_id)
            end)

            it("should fail with missing dataflow ID", function()
                local tx = get_test_transaction()

                local command = {
                    type = ops.COMMAND_TYPES.CREATE_DATA,
                    payload = {
                        data_id = uuid.v7(),
                        key = "error_test",
                        discriminator = "test",
                        data_type = "test_data",
                        content = "test"
                    }
                }

                local result, err = ops.execute(tx, nil, nil, command)

                test.is_nil(result)
                test.contains(err, "Workflow ID is required")
            end)

            it("should fail with unknown command type", function()
                local resources = setup_test_resources()
                local tx = get_test_transaction()

                local command = {
                    type = "unknown_command",
                    payload = {
                        some_field = "value"
                    }
                }

                local result, err = ops.execute(tx, resources.dataflow_id, nil, command)

                test.is_nil(result)
                test.contains(err, "Unknown command type")
            end)

            it("should fail if a command in a batch fails", function()
                local resources = setup_test_resources()
                local tx = get_test_transaction()

                local data_id_1 = uuid.v7()
                local data_id_2 = uuid.v7()

                local commands = {
                    {
                        type = ops.COMMAND_TYPES.CREATE_DATA,
                        payload = {
                            data_id = data_id_1,
                            key = "batch_error_key_1",
                            discriminator = "test",
                            data_type = "test_data",
                            content = "batch content 1"
                        }
                    },
                    {
                        type = ops.COMMAND_TYPES.CREATE_DATA,
                        payload = {
                            data_id = data_id_2,
                            key = "batch_error_key_2",
                            discriminator = "test"
                            -- data_type is missing (required field)
                            -- content is missing (required field)
                        }
                    }
                }

                local result, err = ops.execute(tx, resources.dataflow_id, nil, commands)

                test.is_nil(result)
                test.contains(err, "Data type is required")

                -- Verify first command was executed but will be rolled back
                local query = "SELECT * FROM dataflow_data WHERE data_id = ?"
                local rows, err_query = txq(tx, query, { data_id_1 })

                test.is_nil(err_query)
                test.eq(#rows, 1) -- Record exists in transaction but will rollback
            end)
        end)

        describe("Durable Uniqueness Guards", function()
            it("should reject duplicate workflow output rows at the database level", function()
                local resources = setup_test_resources()
                local tx = get_test_transaction()
                local now_ts = time.now():format(time.RFC3339NANO)
                local db_type = tx:db_type()

                local insert_sql = rebind([[
                    INSERT INTO dataflow_data (
                        data_id, dataflow_id, node_id, type, discriminator, key, content, content_type, metadata, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ]], db_type)

                local _, first_err = tx:execute(insert_sql, {
                    uuid.v7(),
                    resources.dataflow_id,
                    resources.node_id,
                    "dataflow.output",
                    nil,
                    "result",
                    "first-output",
                    "text/plain",
                    "{}",
                    now_ts
                })
                test.is_nil(first_err)

                local _, second_err = tx:execute(insert_sql, {
                    uuid.v7(),
                    resources.dataflow_id,
                    resources.node_id,
                    "dataflow.output",
                    nil,
                    "result",
                    "duplicate-output",
                    "text/plain",
                    "{}",
                    now_ts
                })

                test.not_nil(second_err)
                assert_unique_violation(second_err)
            end)

            it("should reject duplicate successful node results at the database level", function()
                local resources = setup_test_resources()
                local tx = get_test_transaction()
                local now_ts = time.now():format(time.RFC3339NANO)
                local db_type = tx:db_type()

                local insert_sql = rebind([[
                    INSERT INTO dataflow_data (
                        data_id, dataflow_id, node_id, type, discriminator, key, content, content_type, metadata, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ]], db_type)

                local _, first_err = tx:execute(insert_sql, {
                    uuid.v7(),
                    resources.dataflow_id,
                    resources.node_id,
                    "node.result",
                    "result.success",
                    nil,
                    "Completed",
                    "text/plain",
                    "{}",
                    now_ts
                })
                test.is_nil(first_err)

                local _, second_err = tx:execute(insert_sql, {
                    uuid.v7(),
                    resources.dataflow_id,
                    resources.node_id,
                    "node.result",
                    "result.success",
                    nil,
                    "Completed again",
                    "text/plain",
                    "{}",
                    now_ts
                })

                test.not_nil(second_err)
                assert_unique_violation(second_err)
            end)

            it("should treat duplicate workflow output creates as idempotent", function()
                local resources = setup_test_resources()
                local tx = get_test_transaction()

                local first_command = {
                    type = ops.COMMAND_TYPES.CREATE_DATA,
                    payload = {
                        data_id = uuid.v7(),
                        node_id = resources.node_id,
                        data_type = "dataflow.output",
                        key = "result",
                        content = "first-output",
                        content_type = "text/plain"
                    }
                }

                local first_result, first_err = ops.execute(tx, resources.dataflow_id, nil, first_command)
                test.is_nil(first_err)
                test.is_true(first_result.changes_made)

                local duplicate_command = {
                    type = ops.COMMAND_TYPES.CREATE_DATA,
                    payload = {
                        data_id = uuid.v7(),
                        node_id = resources.node_id,
                        data_type = "dataflow.output",
                        key = "result",
                        content = "second-output",
                        content_type = "text/plain"
                    }
                }

                local duplicate_result, duplicate_err = ops.execute(tx, resources.dataflow_id, nil, duplicate_command)
                test.is_nil(duplicate_err)
                test.is_false(duplicate_result.changes_made)
                test.eq(duplicate_result.results[1].data_id, first_result.results[1].data_id)
                test.is_true(duplicate_result.results[1].deduplicated)
            end)
        end)

        describe("Workflow Operations", function()
            it("should create a dataflow using CREATE_WORKFLOW command with minimal fields", function()
                local tx = get_test_transaction()

                local dataflow_id = uuid.v7()
                local actor_id = "test-actor-" .. uuid.v7()
                local command = {
                    type = ops.COMMAND_TYPES.CREATE_WORKFLOW,
                    payload = {
                        dataflow_id = dataflow_id,
                        actor_id = actor_id,
                        type = "minimal_dataflow_type"
                    }
                }

                local result, err = ops.execute(tx, dataflow_id, nil, command)

                test.is_nil(err)
                test.not_nil(result)
                test.not_nil(result.op_id)
                test.is_true(result.changes_made)
                test.eq(#result.results, 1)
                test.eq(result.results[1].dataflow_id, dataflow_id)

                local query = "SELECT * FROM dataflows WHERE dataflow_id = ?"
                local rows, err_query = txq(tx, query, { dataflow_id })

                test.is_nil(err_query)
                test.eq(#rows, 1)
                test.eq(rows[1].dataflow_id, dataflow_id)
                test.eq(rows[1].actor_id, actor_id)
                test.eq(rows[1].type, "minimal_dataflow_type")
                test.eq(rows[1].status, "pending")
                test.is_nil(rows[1].parent_dataflow_id)

                local metadata = json.decode(rows[1].metadata :: string)
                test.is_table(metadata)
                test.is_nil(next(metadata))

                test.not_nil(rows[1].created_at)
                test.not_nil(rows[1].updated_at)
                test.eq(rows[1].created_at, rows[1].updated_at)
            end)

            it("should create a dataflow using CREATE_WORKFLOW command with all optional fields", function()
                local tx = get_test_transaction()

                local parent_dataflow_id = uuid.v7()
                local actor_id = "test-actor-" .. uuid.v7()

                local parent_command = {
                    type = ops.COMMAND_TYPES.CREATE_WORKFLOW,
                    payload = {
                        dataflow_id = parent_dataflow_id,
                        actor_id = actor_id,
                        type = "parent_dataflow_type"
                    }
                }

                local _parent_result, parent_err = ops.execute(tx, parent_dataflow_id, nil, parent_command)
                test.is_nil(parent_err)

                local dataflow_id = uuid.v7()
                local command = {
                    type = ops.COMMAND_TYPES.CREATE_WORKFLOW,
                    payload = {
                        dataflow_id = dataflow_id,
                        actor_id = actor_id,
                        type = "full_dataflow_type",
                        status = "running",
                        parent_dataflow_id = parent_dataflow_id,
                        metadata = {
                            source = "ops_test",
                            purpose = "testing",
                            nested = { key = "value", num = 42 }
                        }
                    }
                }

                local result, err = ops.execute(tx, dataflow_id, nil, command)

                test.is_nil(err)
                test.not_nil(result)
                test.not_nil(result.op_id)
                test.is_true(result.changes_made)
                test.eq(#result.results, 1)
                test.eq(result.results[1].dataflow_id, dataflow_id)

                local query = "SELECT * FROM dataflows WHERE dataflow_id = ?"
                local rows, err_query = txq(tx, query, { dataflow_id })

                test.is_nil(err_query)
                test.eq(#rows, 1)
                test.eq(rows[1].dataflow_id, dataflow_id)
                test.eq(rows[1].actor_id, actor_id)
                test.eq(rows[1].type, "full_dataflow_type")
                test.eq(rows[1].status, "running")
                test.eq(rows[1].parent_dataflow_id, parent_dataflow_id)

                local metadata = json.decode(rows[1].metadata :: string)
                test.is_table(metadata)
                test.eq(metadata.source, "ops_test")
                test.eq(metadata.purpose, "testing")
                test.eq(metadata.nested.key, "value")
                test.eq(metadata.nested.num, 42)
            end)

            it("should fail to create a dataflow without required fields", function()
                local tx = get_test_transaction()

                local dataflow_id = uuid.v7()
                local actor_id = "test-actor-" .. uuid.v7()

                local command1 = {
                    type = ops.COMMAND_TYPES.CREATE_WORKFLOW,
                    payload = {
                        dataflow_id = dataflow_id,
                        type = "test_dataflow"
                    }
                }

                local command2 = {
                    type = ops.COMMAND_TYPES.CREATE_WORKFLOW,
                    payload = {
                        dataflow_id = dataflow_id,
                        actor_id = actor_id
                    }
                }

                local result1, err1 = ops.execute(tx, dataflow_id, nil, command1)
                test.is_nil(result1)
                test.contains(err1, "User ID is required")

                local result2, err2 = ops.execute(tx, dataflow_id, nil, command2)
                test.is_nil(result2)
                test.contains(err2, "Workflow type is required")
            end)

            it("should accept metadata as pre-encoded JSON string", function()
                local tx = get_test_transaction()

                local dataflow_id = uuid.v7()
                local actor_id = "test-actor-" .. uuid.v7()
                local metadata_json_string = '{"json_key":"json_value","nested":{"num":123}}'

                local command = {
                    type = ops.COMMAND_TYPES.CREATE_WORKFLOW,
                    payload = {
                        dataflow_id = dataflow_id,
                        actor_id = actor_id,
                        type = "json_string_meta",
                        metadata = metadata_json_string
                    }
                }

                local _result, err = ops.execute(tx, dataflow_id, nil, command)
                test.is_nil(err)

                local query = "SELECT metadata FROM dataflows WHERE dataflow_id = ?"
                local rows, err_query = txq(tx, query, { dataflow_id })

                test.is_nil(err_query)
                test.eq(#rows, 1)

                local metadata = json.decode(rows[1].metadata :: string)
                test.eq(metadata.json_key, "json_value")
                test.eq(metadata.nested.num, 123)
            end)

            it("should update a dataflow with UPDATE_WORKFLOW command", function()
                local tx = get_test_transaction()

                local dataflow_id = uuid.v7()
                local actor_id = "test-actor-" .. uuid.v7()
                local create_command = {
                    type = ops.COMMAND_TYPES.CREATE_WORKFLOW,
                    payload = {
                        dataflow_id = dataflow_id,
                        actor_id = actor_id,
                        type = "update_test_type",
                        metadata = { version = 1 }
                    }
                }

                local create_result, create_err = ops.execute(tx, dataflow_id, nil, create_command)
                test.is_nil(create_err)
                test.is_true(create_result.changes_made)

                local check_query = "SELECT created_at FROM dataflows WHERE dataflow_id = ?"
                local check_rows, check_err = txq(tx, check_query, { dataflow_id })
                test.is_nil(check_err)
                local created_at = check_rows[1].created_at

                local update_command = {
                    type = ops.COMMAND_TYPES.UPDATE_WORKFLOW,
                    payload = {
                        status = "completed",
                        metadata = { version = 2, updated = true }
                    }
                }

                local result, err = ops.execute(tx, dataflow_id, nil, update_command)

                test.is_nil(err)
                test.not_nil(result)
                test.is_true(result.changes_made)
                test.eq(#result.results, 1)
                test.eq(result.results[1].dataflow_id, dataflow_id)

                local query = "SELECT * FROM dataflows WHERE dataflow_id = ?"
                local rows, err_query = txq(tx, query, { dataflow_id })

                test.is_nil(err_query)
                test.eq(#rows, 1)
                test.eq(rows[1].status, "completed")

                local metadata = json.decode(rows[1].metadata :: string)
                test.is_table(metadata)
                test.eq(metadata.version, 2)
                test.is_true(metadata.updated)

                test.is_true(rows[1].updated_at >= created_at)
            end)

            it("should handle empty updates with UPDATE_WORKFLOW command", function()
                local tx = get_test_transaction()

                local dataflow_id = uuid.v7()
                local actor_id = "test-actor-" .. uuid.v7()
                local create_command = {
                    type = ops.COMMAND_TYPES.CREATE_WORKFLOW,
                    payload = {
                        dataflow_id = dataflow_id,
                        actor_id = actor_id,
                        type = "empty_update_test_type",
                    }
                }

                local _create_result, create_err = ops.execute(tx, dataflow_id, nil, create_command)
                test.is_nil(create_err)

                local update_command = {
                    type = ops.COMMAND_TYPES.UPDATE_WORKFLOW,
                    payload = {}
                }

                local result, err = ops.execute(tx, dataflow_id, nil, update_command)

                test.is_nil(err)
                test.not_nil(result)
                test.is_false(result.changes_made)
                test.eq(#result.results, 1)
                test.eq(result.results[1].dataflow_id, dataflow_id)
                test.contains(result.results[1].message, "No valid fields provided for update")
            end)

            it("should return error for non-existent dataflow ID on update", function()
                local tx = get_test_transaction()

                local fake_dataflow_id = uuid.v7()

                local update_command = {
                    type = ops.COMMAND_TYPES.UPDATE_WORKFLOW,
                    payload = {
                        status = "failed"
                    }
                }

                local result, err = ops.execute(tx, fake_dataflow_id, nil, update_command)

                test.is_nil(result)
                test.contains(err, "Workflow not found or no changes applied")
            end)

            it("should update dataflow status with UPDATE_WORKFLOW command", function()
                local tx = get_test_transaction()

                local dataflow_id = uuid.v7()
                local actor_id = "test-actor-" .. uuid.v7()
                local create_command = {
                    type = ops.COMMAND_TYPES.CREATE_WORKFLOW,
                    payload = {
                        dataflow_id = dataflow_id,
                        actor_id = actor_id,
                        type = "status_update_test_type"
                    }
                }

                local _create_result, create_err = ops.execute(tx, dataflow_id, nil, create_command)
                test.is_nil(create_err)

                local check_query = "SELECT status FROM dataflows WHERE dataflow_id = ?"
                local check_rows, check_err = txq(tx, check_query, { dataflow_id })
                test.is_nil(check_err)
                test.eq(check_rows[1].status, "pending")

                local update_command = {
                    type = ops.COMMAND_TYPES.UPDATE_WORKFLOW,
                    payload = {
                        status = "running"
                    }
                }

                local result, err = ops.execute(tx, dataflow_id, nil, update_command)

                test.is_nil(err)
                test.not_nil(result)
                test.is_true(result.changes_made)
                test.eq(#result.results, 1)
                test.eq(result.results[1].dataflow_id, dataflow_id)

                local query = "SELECT status FROM dataflows WHERE dataflow_id = ?"
                local rows, err_query = txq(tx, query, { dataflow_id })

                test.is_nil(err_query)
                test.eq(#rows, 1)
                test.eq(rows[1].status, "running")
            end)

            it("should delete a dataflow with DELETE_WORKFLOW command", function()
                local tx = get_test_transaction()

                local dataflow_id = uuid.v7()
                local actor_id = "test-actor-" .. uuid.v7()
                local create_command = {
                    type = ops.COMMAND_TYPES.CREATE_WORKFLOW,
                    payload = {
                        dataflow_id = dataflow_id,
                        actor_id = actor_id,
                        type = "delete_test_type"
                    }
                }

                local _create_result, create_err = ops.execute(tx, dataflow_id, nil, create_command)
                test.is_nil(create_err)

                local check_query = "SELECT COUNT(*) AS wf_count FROM dataflows WHERE dataflow_id = ?"
                local check_rows, check_err = txq(tx, check_query, { dataflow_id })
                test.is_nil(check_err)
                test.eq(check_rows[1].wf_count, 1)

                local delete_command = {
                    type = ops.COMMAND_TYPES.DELETE_WORKFLOW,
                    payload = {}
                }

                local result, err = ops.execute(tx, dataflow_id, nil, delete_command)

                test.is_nil(err)
                test.not_nil(result)
                test.is_true(result.changes_made)
                test.eq(#result.results, 1)
                test.eq(result.results[1].dataflow_id, dataflow_id)
                test.is_true(result.results[1].deleted)

                local count_query = "SELECT COUNT(*) AS wf_count FROM dataflows WHERE dataflow_id = ?"
                local count_rows, count_err = txq(tx, count_query, { dataflow_id })
                test.is_nil(count_err)
                test.eq(count_rows[1].wf_count, 0)
            end)

            it("should return error when deleting non-existent dataflow", function()
                local tx = get_test_transaction()

                local fake_dataflow_id = uuid.v7()

                local delete_command = {
                    type = ops.COMMAND_TYPES.DELETE_WORKFLOW,
                    payload = {}
                }

                local result, err = ops.execute(tx, fake_dataflow_id, nil, delete_command)

                test.is_nil(result)
                test.contains(err, "Workflow not found")
            end)

            it("should delete a specific dataflow when provided in command", function()
                local tx = get_test_transaction()

                local context_dataflow_id = uuid.v7()
                local delete_dataflow_id = uuid.v7()
                local actor_id = "test-actor-" .. uuid.v7()

                local context_command = {
                    type = ops.COMMAND_TYPES.CREATE_WORKFLOW,
                    payload = {
                        dataflow_id = context_dataflow_id,
                        actor_id = actor_id,
                        type = "context_dataflow_type"
                    }
                }

                local _context_result, context_err = ops.execute(tx, context_dataflow_id, nil, context_command)
                test.is_nil(context_err)

                local target_command = {
                    type = ops.COMMAND_TYPES.CREATE_WORKFLOW,
                    payload = {
                        dataflow_id = delete_dataflow_id,
                        actor_id = actor_id,
                        type = "target_delete_type"
                    }
                }

                local _target_result, target_err = ops.execute(tx, delete_dataflow_id, nil, target_command)
                test.is_nil(target_err)

                local delete_command = {
                    type = ops.COMMAND_TYPES.DELETE_WORKFLOW,
                    payload = {
                        dataflow_id = delete_dataflow_id
                    }
                }

                local result, err = ops.execute(tx, delete_dataflow_id, nil, delete_command)

                test.is_nil(err)
                test.not_nil(result)
                test.is_true(result.changes_made)
                test.eq(#result.results, 1)
                test.eq(result.results[1].dataflow_id, delete_dataflow_id)
                test.is_true(result.results[1].deleted)

                local count_query = "SELECT COUNT(*) AS wf_count FROM dataflows WHERE dataflow_id = ?"

                local deleted_rows, deleted_err = txq(tx, count_query, { delete_dataflow_id })
                test.is_nil(deleted_err)
                test.eq(deleted_rows[1].wf_count, 0)

                local context_rows, context_count_err = txq(tx, count_query, { context_dataflow_id })
                test.is_nil(context_count_err)
                test.eq(context_rows[1].wf_count, 1)
            end)
        end)

        describe("Node Operations", function()
            it("should add a node using CREATE_NODE command with minimal fields", function()
                local resources = setup_test_resources()
                local tx = get_test_transaction()

                local node_id = uuid.v7()
                local command = {
                    type = ops.COMMAND_TYPES.CREATE_NODE,
                    payload = {
                        node_id = node_id,
                        node_type = "minimal_node_type"
                    }
                }

                local result, err = ops.execute(tx, resources.dataflow_id, nil, command)

                test.is_nil(err)
                test.not_nil(result)
                test.not_nil(result.op_id)
                test.is_true(result.changes_made)
                test.eq(#result.results, 1)
                test.eq(result.results[1].node_id, node_id)

                local query = "SELECT * FROM dataflow_nodes WHERE node_id = ?"
                local rows, err_query = txq(tx, query, { node_id })

                test.is_nil(err_query)
                test.eq(#rows, 1)
                test.eq(rows[1].node_id, node_id)
                test.eq(rows[1].type, "minimal_node_type")
                test.eq(rows[1].status, "pending")
                test.is_nil(rows[1].parent_node_id)

                local metadata = json.decode(rows[1].metadata :: string)
                test.is_table(metadata)
                test.is_nil(next(metadata))

                local config = json.decode(rows[1].config :: string)
                test.is_table(config)
                test.is_nil(next(config))
            end)

            it("should add a node using CREATE_NODE command with all optional fields", function()
                local resources = setup_test_resources()
                local tx = get_test_transaction()

                local node_id = uuid.v7()
                local command = {
                    type = ops.COMMAND_TYPES.CREATE_NODE,
                    payload = {
                        node_id = node_id,
                        parent_node_id = resources.node_id,
                        node_type = "full_node_type",
                        status = "ready",
                        config = { timeout = 30, retries = 3, mode = "strict" },
                        metadata = { source = "ops_test", purpose = "testing", count = 42 }
                    }
                }

                local result, err = ops.execute(tx, resources.dataflow_id, nil, command)

                test.is_nil(err)
                test.not_nil(result)
                test.not_nil(result.op_id)
                test.is_true(result.changes_made)
                test.eq(#result.results, 1)
                test.eq(result.results[1].node_id, node_id)

                local query = "SELECT * FROM dataflow_nodes WHERE node_id = ?"
                local rows, err_query = txq(tx, query, { node_id })

                test.is_nil(err_query)
                test.eq(#rows, 1)
                test.eq(rows[1].node_id, node_id)
                test.eq(rows[1].parent_node_id, resources.node_id)
                test.eq(rows[1].type, "full_node_type")
                test.eq(rows[1].status, "ready")

                local config = json.decode(rows[1].config :: string)
                test.is_table(config)
                test.eq(config.timeout, 30)
                test.eq(config.retries, 3)
                test.eq(config.mode, "strict")

                local metadata = json.decode(rows[1].metadata :: string)
                test.is_table(metadata)
                test.eq(metadata.source, "ops_test")
                test.eq(metadata.purpose, "testing")
                test.eq(metadata.count, 42)
            end)

            it("should add a node with config as pre-encoded JSON string", function()
                local resources = setup_test_resources()
                local tx = get_test_transaction()

                local node_id = uuid.v7()
                local config_json_string = '{"batch_size":100,"parallel":true,"settings":{"debug":false}}'
                local command = {
                    type = ops.COMMAND_TYPES.CREATE_NODE,
                    payload = {
                        node_id = node_id,
                        node_type = "json_config_node",
                        config = config_json_string
                    }
                }

                local result, err = ops.execute(tx, resources.dataflow_id, nil, command)

                test.is_nil(err)
                test.not_nil(result)
                test.is_true(result.changes_made)
                test.eq(#result.results, 1)
                test.eq(result.results[1].node_id, node_id)

                local query = "SELECT config FROM dataflow_nodes WHERE node_id = ?"
                local rows, err_query = txq(tx, query, { node_id })

                test.is_nil(err_query)
                test.eq(#rows, 1)

                local config = json.decode(rows[1].config :: string)
                test.is_table(config)
                test.eq(config.batch_size, 100)
                test.is_true(config.parallel)
                test.is_table(config.settings)
                test.is_false(config.settings.debug)
            end)

            it("should fail to add a node without required node_type", function()
                local resources = setup_test_resources()
                local tx = get_test_transaction()

                local node_id = uuid.v7()
                local command = {
                    type = ops.COMMAND_TYPES.CREATE_NODE,
                    payload = {
                        node_id = node_id
                    }
                }

                local result, err = ops.execute(tx, resources.dataflow_id, nil, command)

                test.is_nil(result)
                test.not_nil(err)
                test.contains(err, "Node type is required")

                local query = "SELECT COUNT(*) AS node_count FROM dataflow_nodes WHERE node_id = ?"
                local rows, err_query = txq(tx, query, { node_id })

                test.is_nil(err_query)
                test.eq(rows[1].node_count, 0)
            end)

            it("should update node config with UPDATE_NODE command", function()
                local resources = setup_test_resources()
                local tx = get_test_transaction()

                local initial_query = "SELECT config FROM dataflow_nodes WHERE node_id = ?"
                local initial_rows, err_initial = txq(tx, initial_query, { resources.node_id })

                test.is_nil(err_initial)
                test.eq(#initial_rows, 1)

                local initial_config = json.decode(initial_rows[1].config :: string)
                test.is_table(initial_config)
                test.is_nil(next(initial_config))

                local command = {
                    type = ops.COMMAND_TYPES.UPDATE_NODE,
                    payload = {
                        node_id = resources.node_id,
                        config = {
                            max_workers = 4,
                            timeout = 60,
                            features = { "logging", "metrics" }
                        }
                    }
                }

                local result, err = ops.execute(tx, resources.dataflow_id, nil, command)

                test.is_nil(err)
                test.not_nil(result)
                test.is_true(result.changes_made)
                test.eq(#result.results, 1)
                test.eq(result.results[1].node_id, resources.node_id)
                test.is_true(result.results[1].changes_made)

                local query = "SELECT config FROM dataflow_nodes WHERE node_id = ?"
                local rows, err_query = txq(tx, query, { resources.node_id })

                test.is_nil(err_query)
                test.eq(#rows, 1)

                local config = json.decode(rows[1].config :: string)
                test.is_table(config)
                test.eq(config.max_workers, 4)
                test.eq(config.timeout, 60)
                test.is_table(config.features)
                test.eq(#config.features, 2)
                test.eq(config.features[1], "logging")
                test.eq(config.features[2], "metrics")
            end)

            it("should update node metadata with UPDATE_NODE command", function()
                local resources = setup_test_resources()
                local tx = get_test_transaction()

                local initial_query = "SELECT metadata FROM dataflow_nodes WHERE node_id = ?"
                local initial_rows, err_initial = txq(tx, initial_query, { resources.node_id })

                test.is_nil(err_initial)
                test.eq(#initial_rows, 1)

                local initial_metadata = json.decode(initial_rows[1].metadata :: string)
                test.is_table(initial_metadata)
                test.is_nil(next(initial_metadata))

                local command = {
                    type = ops.COMMAND_TYPES.UPDATE_NODE,
                    payload = {
                        node_id = resources.node_id,
                        metadata = {
                            updated = true,
                            version = 2,
                            tags = { "test", "update" }
                        }
                    }
                }

                local result, err = ops.execute(tx, resources.dataflow_id, nil, command)

                test.is_nil(err)
                test.not_nil(result)
                test.is_true(result.changes_made)
                test.eq(#result.results, 1)
                test.eq(result.results[1].node_id, resources.node_id)
                test.is_true(result.results[1].changes_made)

                local query = "SELECT metadata FROM dataflow_nodes WHERE node_id = ?"
                local rows, err_query = txq(tx, query, { resources.node_id })

                test.is_nil(err_query)
                test.eq(#rows, 1)

                local metadata = json.decode(rows[1].metadata :: string)
                test.is_table(metadata)
                test.is_true(metadata.updated)
                test.eq(metadata.version, 2)
                test.is_table(metadata.tags)
                test.eq(#metadata.tags, 2)
                test.eq(metadata.tags[1], "test")
                test.eq(metadata.tags[2], "update")
            end)

            it("should update node status with UPDATE_NODE command", function()
                local resources = setup_test_resources()
                local tx = get_test_transaction()

                local initial_query = "SELECT status FROM dataflow_nodes WHERE node_id = ?"
                local initial_rows, err_initial = txq(tx, initial_query, { resources.node_id })

                test.is_nil(err_initial)
                test.eq(#initial_rows, 1)
                test.eq(initial_rows[1].status, "pending")

                local command = {
                    type = ops.COMMAND_TYPES.UPDATE_NODE,
                    payload = {
                        node_id = resources.node_id,
                        status = "running"
                    }
                }

                local result, err = ops.execute(tx, resources.dataflow_id, nil, command)

                test.is_nil(err)
                test.not_nil(result)
                test.is_true(result.changes_made)
                test.eq(#result.results, 1)
                test.eq(result.results[1].node_id, resources.node_id)
                test.is_true(result.results[1].changes_made)

                local query = "SELECT status FROM dataflow_nodes WHERE node_id = ?"
                local rows, err_query = txq(tx, query, { resources.node_id })

                test.is_nil(err_query)
                test.eq(#rows, 1)
                test.eq(rows[1].status, "running")
            end)

            it("should update node type with UPDATE_NODE command", function()
                local resources = setup_test_resources()
                local tx = get_test_transaction()

                local initial_query = "SELECT type FROM dataflow_nodes WHERE node_id = ?"
                local initial_rows, err_initial = txq(tx, initial_query, { resources.node_id })

                test.is_nil(err_initial)
                test.eq(#initial_rows, 1)
                test.eq(initial_rows[1].type, "test_node")

                local command = {
                    type = ops.COMMAND_TYPES.UPDATE_NODE,
                    payload = {
                        node_id = resources.node_id,
                        node_type = "updated_node_type"
                    }
                }

                local result, err = ops.execute(tx, resources.dataflow_id, nil, command)

                test.is_nil(err)
                test.not_nil(result)
                test.is_true(result.changes_made)
                test.eq(#result.results, 1)
                test.eq(result.results[1].node_id, resources.node_id)
                test.is_true(result.results[1].changes_made)

                local query = "SELECT type FROM dataflow_nodes WHERE node_id = ?"
                local rows, err_query = txq(tx, query, { resources.node_id })

                test.is_nil(err_query)
                test.eq(#rows, 1)
                test.eq(rows[1].type, "updated_node_type")
            end)

            it("should update multiple node fields including config with a single UPDATE_NODE command", function()
                local resources = setup_test_resources()
                local tx = get_test_transaction()

                local command = {
                    type = ops.COMMAND_TYPES.UPDATE_NODE,
                    payload = {
                        node_id = resources.node_id,
                        node_type = "multi_update_type",
                        status = "running",
                        config = { workers = 8, enabled = true },
                        metadata = { updated = true, multi = "field_update" }
                    }
                }

                local result, err = ops.execute(tx, resources.dataflow_id, nil, command)

                test.is_nil(err)
                test.not_nil(result)
                test.is_true(result.changes_made)
                test.eq(#result.results, 1)
                test.eq(result.results[1].node_id, resources.node_id)
                test.is_true(result.results[1].changes_made)

                local query = "SELECT * FROM dataflow_nodes WHERE node_id = ?"
                local rows, err_query = txq(tx, query, { resources.node_id })

                test.is_nil(err_query)
                test.eq(#rows, 1)
                test.eq(rows[1].type, "multi_update_type")
                test.eq(rows[1].status, "running")

                local config = json.decode(rows[1].config :: string)
                test.is_table(config)
                test.eq(config.workers, 8)
                test.is_true(config.enabled)

                local metadata = json.decode(rows[1].metadata :: string)
                test.is_table(metadata)
                test.is_true(metadata.updated)
                test.eq(metadata.multi, "field_update")
            end)

            it("should update config as pre-encoded JSON string with UPDATE_NODE command", function()
                local resources = setup_test_resources()
                local tx = get_test_transaction()

                local config_json_string = '{"api_key":"secret123","endpoints":["api.example.com"],"retry_count":5}'
                local command = {
                    type = ops.COMMAND_TYPES.UPDATE_NODE,
                    payload = {
                        node_id = resources.node_id,
                        config = config_json_string
                    }
                }

                local result, err = ops.execute(tx, resources.dataflow_id, nil, command)

                test.is_nil(err)
                test.not_nil(result)
                test.is_true(result.changes_made)
                test.eq(#result.results, 1)
                test.eq(result.results[1].node_id, resources.node_id)
                test.is_true(result.results[1].changes_made)

                local query = "SELECT config FROM dataflow_nodes WHERE node_id = ?"
                local rows, err_query = txq(tx, query, { resources.node_id })

                test.is_nil(err_query)
                test.eq(#rows, 1)

                local config = json.decode(rows[1].config :: string)
                test.is_table(config)
                test.eq(config.api_key, "secret123")
                test.is_table(config.endpoints)
                test.eq(#config.endpoints, 1)
                test.eq(config.endpoints[1], "api.example.com")
                test.eq(config.retry_count, 5)
            end)

            it("should handle empty updates with UPDATE_NODE command", function()
                local resources = setup_test_resources()
                local tx = get_test_transaction()

                local command = {
                    type = ops.COMMAND_TYPES.UPDATE_NODE,
                    payload = {
                        node_id = resources.node_id
                    }
                }

                local result, err = ops.execute(tx, resources.dataflow_id, nil, command)

                test.is_nil(err)
                test.not_nil(result)
                test.is_false(result.changes_made)
                test.eq(#result.results, 1)
                test.eq(result.results[1].node_id, resources.node_id)
                test.is_false(result.results[1].changes_made)
                test.contains(result.results[1].message, "No fields provided for update")
            end)

            it("should delete a node with DELETE_NODE command", function()
                local resources = setup_test_resources()
                local tx = get_test_transaction()

                local initial_query = "SELECT 1 AS exists_flag FROM dataflow_nodes WHERE node_id = ?"
                local initial_rows, err_initial = txq(tx, initial_query, { resources.node_id })

                test.is_nil(err_initial)
                test.eq(#initial_rows, 1)
                test.eq(initial_rows[1].exists_flag, 1)

                local command = {
                    type = ops.COMMAND_TYPES.DELETE_NODE,
                    payload = {
                        node_id = resources.node_id
                    }
                }

                local result, err = ops.execute(tx, resources.dataflow_id, nil, command)

                test.is_nil(err)
                test.not_nil(result)
                test.is_true(result.changes_made)
                test.eq(#result.results, 1)
                test.eq(result.results[1].node_id, resources.node_id)
                test.is_true(result.results[1].changes_made)

                local query = "SELECT COUNT(*) AS node_count FROM dataflow_nodes WHERE node_id = ?"
                local rows, err_query = txq(tx, query, { resources.node_id })

                test.is_nil(err_query)
                test.eq(rows[1].node_count, 0)
            end)
        end)

        describe("Data Operations", function()
            it("should create data using CREATE_DATA command with minimal fields", function()
                local resources = setup_test_resources()
                local tx = get_test_transaction()

                local data_id = uuid.v7()
                local command = {
                    type = ops.COMMAND_TYPES.CREATE_DATA,
                    payload = {
                        data_id = data_id,
                        data_type = "test_data",
                        key = "minimal_test_key",
                        content = "Simple string content"
                    }
                }

                local result, err = ops.execute(tx, resources.dataflow_id, nil, command)

                test.is_nil(err)
                test.not_nil(result)
                test.not_nil(result.op_id)
                test.is_true(result.changes_made)
                test.eq(#result.results, 1)
                test.eq(result.results[1].data_id, data_id)

                local query = "SELECT * FROM dataflow_data WHERE data_id = ?"
                local rows, err_query = txq(tx, query, { data_id })

                test.is_nil(err_query)
                test.eq(#rows, 1)
                test.eq(rows[1].data_id, data_id)
                test.eq(rows[1].dataflow_id, resources.dataflow_id)
                test.is_nil(rows[1].node_id)
                test.eq(rows[1].type, "test_data")
                test.eq(rows[1].discriminator, nil)
                test.eq(rows[1].key, "minimal_test_key")
                test.eq(rows[1].content, "Simple string content")
                test.eq(rows[1].content_type, "application/json")
            end)

            it("should create data with complex content and metadata", function()
                local resources = setup_test_resources()
                local tx = get_test_transaction()

                local data_id = uuid.v7()
                local content_obj = {
                    items = {
                        { id = 1, name = "First item" },
                        { id = 2, name = "Second item" }
                    },
                    count = 2,
                    valid = true
                }
                local metadata_obj = {
                    source = "test",
                    created_by = "ops_test",
                    tags = { "complex", "data" }
                }

                local command = {
                    type = ops.COMMAND_TYPES.CREATE_DATA,
                    payload = {
                        data_id = data_id,
                        data_type = "complex_data",
                        key = "complex_test_key",
                        discriminator = "test_discriminator",
                        content = content_obj,
                        content_type = "application/json",
                        metadata = metadata_obj,
                        node_id = resources.node_id
                    }
                }

                local result, err = ops.execute(tx, resources.dataflow_id, nil, command)

                test.is_nil(err)
                test.not_nil(result)
                test.is_true(result.changes_made)
                test.eq(#result.results, 1)
                test.eq(result.results[1].data_id, data_id)

                local query = "SELECT * FROM dataflow_data WHERE data_id = ?"
                local rows, err_query = txq(tx, query, { data_id })

                test.is_nil(err_query)
                test.eq(#rows, 1)
                test.eq(rows[1].data_id, data_id)
                test.eq(rows[1].dataflow_id, resources.dataflow_id)
                test.eq(rows[1].node_id, resources.node_id)
                test.eq(rows[1].type, "complex_data")
                test.eq(rows[1].discriminator, "test_discriminator")
                test.eq(rows[1].key, "complex_test_key")
                test.eq(rows[1].content_type, "application/json")

                local content = json.decode(rows[1].content :: string)
                test.is_table(content)
                test.eq(content.count, 2)
                test.is_true(content.valid)
                test.eq(#content.items, 2)
                test.eq(content.items[1].id, 1)
                test.eq(content.items[2].name, "Second item")

                local metadata = json.decode(rows[1].metadata :: string)
                test.is_table(metadata)
                test.eq(metadata.source, "test")
                test.eq(metadata.created_by, "ops_test")
                test.eq(#metadata.tags, 2)
            end)

            it("should update data content with UPDATE_DATA command", function()
                local resources = setup_test_resources()
                local tx = get_test_transaction()

                local data_id = uuid.v7()
                local create_command = {
                    type = ops.COMMAND_TYPES.CREATE_DATA,
                    payload = {
                        data_id = data_id,
                        data_type = "updatable_data",
                        key = "update_test_key",
                        content = { value = "original value", count = 1 }
                    }
                }

                local _create_result, err_create = ops.execute(tx, resources.dataflow_id, nil, create_command)
                test.is_nil(err_create)

                local update_command = {
                    type = ops.COMMAND_TYPES.UPDATE_DATA,
                    payload = {
                        data_id = data_id,
                        content = { value = "updated value", count = 2, new_field = true }
                    }
                }

                local result, err = ops.execute(tx, resources.dataflow_id, nil, update_command)

                test.is_nil(err)
                test.not_nil(result)
                test.is_true(result.changes_made)
                test.eq(#result.results, 1)
                test.eq(result.results[1].data_id, data_id)
                test.is_true(result.results[1].changes_made)

                local query = "SELECT content FROM dataflow_data WHERE data_id = ?"
                local rows, err_query = txq(tx, query, { data_id })

                test.is_nil(err_query)
                test.eq(#rows, 1)

                local content = json.decode(rows[1].content :: string)
                test.is_table(content)
                test.eq(content.value, "updated value")
                test.eq(content.count, 2)
                test.is_true(content.new_field)
            end)

            it("should update data metadata with UPDATE_DATA command", function()
                local resources = setup_test_resources()
                local tx = get_test_transaction()

                local data_id = uuid.v7()
                local create_command = {
                    type = ops.COMMAND_TYPES.CREATE_DATA,
                    payload = {
                        data_id = data_id,
                        data_type = "updatable_data",
                        key = "metadata_update_test",
                        content = "Test content",
                        metadata = { version = 1 }
                    }
                }

                local _create_result, err_create = ops.execute(tx, resources.dataflow_id, nil, create_command)
                test.is_nil(err_create)

                local update_command = {
                    type = ops.COMMAND_TYPES.UPDATE_DATA,
                    payload = {
                        data_id = data_id,
                        metadata = { version = 2, updated = true, tags = { "modified" } }
                    }
                }

                local result, err = ops.execute(tx, resources.dataflow_id, nil, update_command)

                test.is_nil(err)
                test.not_nil(result)
                test.is_true(result.changes_made)
                test.eq(#result.results, 1)
                test.eq(result.results[1].data_id, data_id)
                test.is_true(result.results[1].changes_made)

                local query = "SELECT metadata FROM dataflow_data WHERE data_id = ?"
                local rows, err_query = txq(tx, query, { data_id })

                test.is_nil(err_query)
                test.eq(#rows, 1)

                local metadata = json.decode(rows[1].metadata :: string)
                test.is_table(metadata)
                test.eq(metadata.version, 2)
                test.is_true(metadata.updated)
                test.is_table(metadata.tags)
                test.eq(#metadata.tags, 1)
                test.eq(metadata.tags[1], "modified")
            end)

            it("should delete data with DELETE_DATA command", function()
                local resources = setup_test_resources()
                local tx = get_test_transaction()

                local data_id = uuid.v7()
                local create_command = {
                    type = ops.COMMAND_TYPES.CREATE_DATA,
                    payload = {
                        data_id = data_id,
                        data_type = "deletable_data",
                        key = "delete_test_key",
                        content = "Content to be deleted"
                    }
                }

                local _create_result, err_create = ops.execute(tx, resources.dataflow_id, nil, create_command)
                test.is_nil(err_create)

                local exists_query = "SELECT COUNT(*) AS data_count FROM dataflow_data WHERE data_id = ?"
                local exists_rows, err_exists = txq(tx, exists_query, { data_id })
                test.is_nil(err_exists)
                test.eq(exists_rows[1].data_count, 1)

                local delete_command = {
                    type = ops.COMMAND_TYPES.DELETE_DATA,
                    payload = {
                        data_id = data_id
                    }
                }

                local result, err = ops.execute(tx, resources.dataflow_id, nil, delete_command)

                test.is_nil(err)
                test.not_nil(result)
                test.is_true(result.changes_made)
                test.eq(#result.results, 1)
                test.eq(result.results[1].data_id, data_id)
                test.is_true(result.results[1].changes_made)

                local count_query = "SELECT COUNT(*) AS data_count FROM dataflow_data WHERE data_id = ?"
                local count_rows, count_err = txq(tx, count_query, { data_id })
                test.is_nil(count_err)
                test.eq(count_rows[1].data_count, 0)
            end)
        end)

        describe("Metadata Handling", function()
            it("should merge metadata by default when updating workflow", function()
                local tx = get_test_transaction()

                local dataflow_id = uuid.v7()
                local actor_id = "test-actor-" .. uuid.v7()

                -- Create workflow with initial metadata
                local create_command = {
                    type = ops.COMMAND_TYPES.CREATE_WORKFLOW,
                    payload = {
                        dataflow_id = dataflow_id,
                        actor_id = actor_id,
                        type = "merge_test_type",
                        metadata = {
                            title = "Original Title",
                            version = 1,
                            tags = { "initial", "test" }
                        }
                    }
                }

                local _create_result, create_err = ops.execute(tx, dataflow_id, nil, create_command)
                test.is_nil(create_err)

                -- Update with new metadata (should merge by default)
                local update_command = {
                    type = ops.COMMAND_TYPES.UPDATE_WORKFLOW,
                    payload = {
                        metadata = {
                            status = "running",
                            version = 2,
                            started_at = "2024-01-01T12:00:00Z"
                        }
                        -- merge_metadata not specified, should default to true
                    }
                }

                local result, err = ops.execute(tx, dataflow_id, nil, update_command)

                test.is_nil(err)
                test.is_true(result.changes_made)
                test.is_true(result.results[1].metadata_merged)

                local query = "SELECT metadata FROM dataflows WHERE dataflow_id = ?"
                local rows, err_query = txq(tx, query, { dataflow_id })

                test.is_nil(err_query)
                test.eq(#rows, 1)

                local metadata = json.decode(rows[1].metadata :: string)
                -- Original metadata should be preserved
                test.eq(metadata.title, "Original Title")
                test.is_table(metadata.tags)
                test.eq(#metadata.tags, 2)
                test.eq(metadata.tags[1], "initial")
                -- New metadata should be added
                test.eq(metadata.status, "running")
                test.eq(metadata.started_at, "2024-01-01T12:00:00Z")
                -- Overlapping key should be updated
                test.eq(metadata.version, 2)
            end)

            it("should merge metadata when explicitly requested", function()
                local tx = get_test_transaction()

                local dataflow_id = uuid.v7()
                local actor_id = "test-actor-" .. uuid.v7()

                local create_command = {
                    type = ops.COMMAND_TYPES.CREATE_WORKFLOW,
                    payload = {
                        dataflow_id = dataflow_id,
                        actor_id = actor_id,
                        type = "explicit_merge_test",
                        metadata = {
                            priority = "high",
                            department = "engineering",
                            config = { timeout = 30 }
                        }
                    }
                }

                local _create_result, create_err = ops.execute(tx, dataflow_id, nil, create_command)
                test.is_nil(create_err)

                local update_command = {
                    type = ops.COMMAND_TYPES.UPDATE_WORKFLOW,
                    payload = {
                        metadata = {
                            priority = "urgent",     -- Should overwrite
                            assignee = "john.doe",   -- Should add
                            config = { retries = 3 } -- Should overwrite completely
                        },
                        merge_metadata = true        -- Explicit merge
                    }
                }

                local result, err = ops.execute(tx, dataflow_id, nil, update_command)

                test.is_nil(err)
                test.is_true(result.changes_made)
                test.is_true(result.results[1].metadata_merged)

                local query = "SELECT metadata FROM dataflows WHERE dataflow_id = ?"
                local rows, err_query = txq(tx, query, { dataflow_id })

                test.is_nil(err_query)
                local metadata = json.decode(rows[1].metadata :: string)

                test.eq(metadata.priority, "urgent")        -- Overwritten
                test.eq(metadata.department, "engineering") -- Preserved
                test.eq(metadata.assignee, "john.doe")      -- Added
                test.eq(metadata.config.retries, 3)         -- New config
                test.is_nil(metadata.config.timeout)         -- Original config overwritten
            end)

            it("should replace metadata when explicitly disabled", function()
                local tx = get_test_transaction()

                local dataflow_id = uuid.v7()
                local actor_id = "test-actor-" .. uuid.v7()

                local create_command = {
                    type = ops.COMMAND_TYPES.CREATE_WORKFLOW,
                    payload = {
                        dataflow_id = dataflow_id,
                        actor_id = actor_id,
                        type = "replace_test",
                        metadata = {
                            original_field = "original_value",
                            preserve_me = "should_be_lost",
                            nested = { keep = "nope" }
                        }
                    }
                }

                local _create_result, create_err = ops.execute(tx, dataflow_id, nil, create_command)
                test.is_nil(create_err)

                local update_command = {
                    type = ops.COMMAND_TYPES.UPDATE_WORKFLOW,
                    payload = {
                        metadata = {
                            new_field = "new_value",
                            replacement = true
                        },
                        merge_metadata = false -- Explicit replacement
                    }
                }

                local result, err = ops.execute(tx, dataflow_id, nil, update_command)

                test.is_nil(err)
                test.is_true(result.changes_made)
                test.is_false(result.results[1].metadata_merged)

                local query = "SELECT metadata FROM dataflows WHERE dataflow_id = ?"
                local rows, err_query = txq(tx, query, { dataflow_id })

                test.is_nil(err_query)
                local metadata = json.decode(rows[1].metadata :: string)

                -- Only new metadata should exist
                test.eq(metadata.new_field, "new_value")
                test.is_true(metadata.replacement)
                -- Original metadata should be gone
                test.is_nil(metadata.original_field)
                test.is_nil(metadata.preserve_me)
                test.is_nil(metadata.nested)
            end)

            it("should handle empty existing metadata during merge", function()
                local tx = get_test_transaction()

                local dataflow_id = uuid.v7()
                local actor_id = "test-actor-" .. uuid.v7()

                -- Create workflow with empty metadata
                local create_command = {
                    type = ops.COMMAND_TYPES.CREATE_WORKFLOW,
                    payload = {
                        dataflow_id = dataflow_id,
                        actor_id = actor_id,
                        type = "empty_meta_test"
                        -- No metadata provided
                    }
                }

                local _create_result, create_err = ops.execute(tx, dataflow_id, nil, create_command)
                test.is_nil(create_err)

                local update_command = {
                    type = ops.COMMAND_TYPES.UPDATE_WORKFLOW,
                    payload = {
                        metadata = {
                            first_addition = "value1",
                            nested = { key = "value" }
                        },
                        merge_metadata = true
                    }
                }

                local result, err = ops.execute(tx, dataflow_id, nil, update_command)

                test.is_nil(err)
                test.is_true(result.changes_made)

                local query = "SELECT metadata FROM dataflows WHERE dataflow_id = ?"
                local rows, err_query = txq(tx, query, { dataflow_id })

                test.is_nil(err_query)
                local metadata = json.decode(rows[1].metadata :: string)

                test.eq(metadata.first_addition, "value1")
                test.eq(metadata.nested.key, "value")
            end)

            it("should handle empty update metadata during merge", function()
                local tx = get_test_transaction()

                local dataflow_id = uuid.v7()
                local actor_id = "test-actor-" .. uuid.v7()

                local create_command = {
                    type = ops.COMMAND_TYPES.CREATE_WORKFLOW,
                    payload = {
                        dataflow_id = dataflow_id,
                        actor_id = actor_id,
                        type = "empty_update_test",
                        metadata = {
                            preserved_field = "should_remain",
                            count = 42
                        }
                    }
                }

                local _create_result, create_err = ops.execute(tx, dataflow_id, nil, create_command)
                test.is_nil(create_err)

                local update_command = {
                    type = ops.COMMAND_TYPES.UPDATE_WORKFLOW,
                    payload = {
                        metadata = {}, -- Empty metadata update
                        merge_metadata = true
                    }
                }

                local result, err = ops.execute(tx, dataflow_id, nil, update_command)

                test.is_nil(err)
                test.is_true(result.changes_made)

                local query = "SELECT metadata FROM dataflows WHERE dataflow_id = ?"
                local rows, err_query = txq(tx, query, { dataflow_id })

                test.is_nil(err_query)
                local metadata = json.decode(rows[1].metadata :: string)

                -- Original metadata should be preserved
                test.eq(metadata.preserved_field, "should_remain")
                test.eq(metadata.count, 42)
            end)

            it("should handle JSON string metadata during merge", function()
                local tx = get_test_transaction()

                local dataflow_id = uuid.v7()
                local actor_id = "test-actor-" .. uuid.v7()

                local create_command = {
                    type = ops.COMMAND_TYPES.CREATE_WORKFLOW,
                    payload = {
                        dataflow_id = dataflow_id,
                        actor_id = actor_id,
                        type = "json_string_test",
                        metadata = '{"string_created":"true","num":123}'
                    }
                }

                local _create_result, create_err = ops.execute(tx, dataflow_id, nil, create_command)
                test.is_nil(create_err)

                local update_command = {
                    type = ops.COMMAND_TYPES.UPDATE_WORKFLOW,
                    payload = {
                        metadata = '{"string_updated":"true","num":456,"new_field":"added"}',
                        merge_metadata = true
                    }
                }

                local result, err = ops.execute(tx, dataflow_id, nil, update_command)

                test.is_nil(err)
                test.is_true(result.changes_made)

                local query = "SELECT metadata FROM dataflows WHERE dataflow_id = ?"
                local rows, err_query = txq(tx, query, { dataflow_id })

                test.is_nil(err_query)
                local metadata = json.decode(rows[1].metadata :: string)

                test.eq(metadata.string_created, "true") -- Preserved
                test.eq(metadata.string_updated, "true") -- Added
                test.eq(metadata.num, 456)               -- Overwritten
                test.eq(metadata.new_field, "added")     -- Added
            end)

            it("should fail gracefully with malformed JSON during merge", function()
                local tx = get_test_transaction()

                local dataflow_id = uuid.v7()
                local actor_id = "test-actor-" .. uuid.v7()

                local create_command = {
                    type = ops.COMMAND_TYPES.CREATE_WORKFLOW,
                    payload = {
                        dataflow_id = dataflow_id,
                        actor_id = actor_id,
                        type = "malformed_json_test",
                        metadata = { valid = "metadata" }
                    }
                }

                local _create_result, create_err = ops.execute(tx, dataflow_id, nil, create_command)
                test.is_nil(create_err)

                local update_command = {
                    type = ops.COMMAND_TYPES.UPDATE_WORKFLOW,
                    payload = {
                        metadata = '{"malformed":json}', -- Invalid JSON
                        merge_metadata = true
                    }
                }

                local result, err = ops.execute(tx, dataflow_id, nil, update_command)

                test.is_nil(result)
                test.contains(err, "Failed to decode new metadata JSON")
            end)

            it("should preserve complex nested structures during merge", function()
                local tx = get_test_transaction()

                local dataflow_id = uuid.v7()
                local actor_id = "test-actor-" .. uuid.v7()

                local create_command = {
                    type = ops.COMMAND_TYPES.CREATE_WORKFLOW,
                    payload = {
                        dataflow_id = dataflow_id,
                        actor_id = actor_id,
                        type = "complex_merge_test",
                        metadata = {
                            config = {
                                database = { host = "localhost", port = 5432 },
                                features = { "logging", "metrics" }
                            },
                            owner = "team-alpha"
                        }
                    }
                }

                local _create_result, create_err = ops.execute(tx, dataflow_id, nil, create_command)
                test.is_nil(create_err)

                local update_command = {
                    type = ops.COMMAND_TYPES.UPDATE_WORKFLOW,
                    payload = {
                        metadata = {
                            config = {
                                cache = { ttl = 300 },
                                features = { "caching" } -- Will overwrite arrays completely
                            },
                            status = "active"
                        },
                        merge_metadata = true
                    }
                }

                local result, err = ops.execute(tx, dataflow_id, nil, update_command)

                test.is_nil(err)
                test.is_true(result.changes_made)

                local query = "SELECT metadata FROM dataflows WHERE dataflow_id = ?"
                local rows, err_query = txq(tx, query, { dataflow_id })

                test.is_nil(err_query)
                local metadata = json.decode(rows[1].metadata :: string)

                -- Top-level merge behavior
                test.eq(metadata.owner, "team-alpha") -- Preserved
                test.eq(metadata.status, "active")    -- Added

                -- Nested objects are replaced entirely (Lua table merge behavior)
                test.eq(metadata.config.cache.ttl, 300) -- From update
                test.is_nil(metadata.config.database)    -- Lost in merge
                test.eq(#metadata.config.features, 1)   -- Replaced
                test.eq(metadata.config.features[1], "caching")
            end)

            it("should clear metadata with empty object replacement", function()
                local tx = get_test_transaction()

                local dataflow_id = uuid.v7()
                local actor_id = "test-actor-" .. uuid.v7()

                local create_command = {
                    type = ops.COMMAND_TYPES.CREATE_WORKFLOW,
                    payload = {
                        dataflow_id = dataflow_id,
                        actor_id = actor_id,
                        type = "clear_test",
                        metadata = { should_be_cleared = "value", count = 42 }
                    }
                }

                local _create_result, create_err = ops.execute(tx, dataflow_id, nil, create_command)
                test.is_nil(create_err)

                local update_command = {
                    type = ops.COMMAND_TYPES.UPDATE_WORKFLOW,
                    payload = {
                        metadata = {}, -- Clear with empty object
                        merge_metadata = false -- Use replacement
                    }
                }

                local result, err = ops.execute(tx, dataflow_id, nil, update_command)

                test.is_nil(err)
                test.is_true(result.changes_made)

                local query = "SELECT metadata FROM dataflows WHERE dataflow_id = ?"
                local rows, err_query = txq(tx, query, { dataflow_id })

                test.is_nil(err_query)

                local metadata = json.decode(rows[1].metadata :: string)
                test.is_table(metadata)
                test.is_nil(next(metadata)) -- Should be empty table
            end)

            it("should maintain backward compatibility with existing UPDATE_WORKFLOW usage", function()
                local tx = get_test_transaction()

                local dataflow_id = uuid.v7()
                local actor_id = "test-actor-" .. uuid.v7()

                local create_command = {
                    type = ops.COMMAND_TYPES.CREATE_WORKFLOW,
                    payload = {
                        dataflow_id = dataflow_id,
                        actor_id = actor_id,
                        type = "backward_compat_test",
                        metadata = { original = "data" }
                    }
                }

                local _create_result, create_err = ops.execute(tx, dataflow_id, nil, create_command)
                test.is_nil(create_err)

                -- Old-style update without merge_metadata flag
                local update_command = {
                    type = ops.COMMAND_TYPES.UPDATE_WORKFLOW,
                    payload = {
                        status = "completed",
                        metadata = { completion_time = "2024-01-01" }
                        -- No merge_metadata specified - should default to merge=true
                    }
                }

                local result, err = ops.execute(tx, dataflow_id, nil, update_command)

                test.is_nil(err)
                test.is_true(result.changes_made)
                test.is_true(result.results[1].metadata_merged) -- Default behavior

                local query = "SELECT metadata, status FROM dataflows WHERE dataflow_id = ?"
                local rows, err_query = txq(tx, query, { dataflow_id })

                test.is_nil(err_query)
                test.eq(rows[1].status, "completed")

                local metadata = json.decode(rows[1].metadata :: string)
                test.eq(metadata.original, "data") -- Should be preserved with new default
                test.eq(metadata.completion_time, "2024-01-01")
            end)
        end)
    end)
end

return test.run_cases(define_tests)
