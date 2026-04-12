local test = require("test")
local uuid = require("uuid")
local time = require("time")
local sql = require("sql")

local migration_01 = require("migration_01")
local migration_02 = require("migration_02")
local migration_03 = require("migration_03")
local migration_04 = require("migration_04")

local function rebind(query, db_type)
    if db_type ~= "postgres" then return query end
    local i = 0
    return (query:gsub("%?", function()
        i = i + 1
        return "$" .. i
    end))
end

local function define_tests()
    describe("Migration 04 Regression", function()
        local test_ctx = {
            db = nil,
            db_type = nil,
            cleanup_dataflow_ids = {},
            cleanup_migration_ids = {}
        }

        local function query_rows(query, params)
            local db = test_ctx.db :: any
            local rows, err = db:query(rebind(query, test_ctx.db_type), params)
            if err then
                error("Query failed: " .. err)
            end
            return rows
        end

        local function execute(query, params)
            local db = test_ctx.db :: any
            local _, err = db:execute(rebind(query, test_ctx.db_type), params)
            if err then
                error("Execute failed: " .. err)
            end
        end

        local function execute_in_tx(query, params)
            local db = test_ctx.db :: any
            local tx, err = db:begin()
            if err then
                error("Failed to begin transaction: " .. err)
            end

            local _, exec_err = tx:execute(rebind(query, test_ctx.db_type), params)
            if exec_err then
                tx:rollback()
                error("Transactional execute failed: " .. exec_err)
            end

            local success, commit_err = tx:commit()
            if not success then
                tx:rollback()
                error("Failed to commit transaction: " .. commit_err)
            end
        end

        local function run_migration(direction, suffix)
            local migration_id = "BC_REGRESSION_S1_migration_04_" .. suffix .. "_" .. uuid.v7()
            table.insert(test_ctx.cleanup_migration_ids, migration_id)

            local result = migration_04({
                db = test_ctx.db,
                direction = direction,
                id = migration_id
            })

            test.not_nil(result)
            test.neq(result.status, "error")
            test.eq(result.failed or 0, 0)

            return result
        end

        local function run_bootstrap_migration(migration_fn, suffix)
            local result = migration_fn({
                db = test_ctx.db,
                direction = "up",
                id = "BC_REGRESSION_S1_bootstrap_" .. suffix
            })

            test.not_nil(result)
            test.neq(result.status, "error")
            test.eq(result.failed or 0, 0)

            return result
        end

        local function table_exists(table_name)
            if test_ctx.db_type == "postgres" then
                local rows = query_rows([[
                    SELECT COUNT(*) AS count
                    FROM pg_tables
                    WHERE schemaname = 'public'
                      AND tablename = ?
                ]], { table_name })
                return rows[1].count > 0
            end

            local rows = query_rows([[
                SELECT COUNT(*) AS count
                FROM sqlite_master
                WHERE type = 'table'
                  AND name = ?
            ]], { table_name })
            return rows[1].count > 0
        end

        local function ensure_base_schema()
            if table_exists("dataflow_data") then
                return
            end

            run_bootstrap_migration(migration_01, "migration_01")
            run_bootstrap_migration(migration_02, "migration_02")
            run_bootstrap_migration(migration_03, "migration_03")
        end

        local function create_test_resources()
            local dataflow_id = uuid.v7()
            local node_id = uuid.v7()
            local now_ts = time.now():format(time.RFC3339NANO)

            local db = test_ctx.db :: any
            local tx, err = db:begin()
            if err then
                error("Failed to begin transaction: " .. err)
            end

            local _, dataflow_err = tx:execute(rebind([[
                INSERT INTO dataflows (
                    dataflow_id, actor_id, type, status, metadata, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ]], test_ctx.db_type), {
                dataflow_id,
                "migration-test-" .. uuid.v7(),
                "migration_regression",
                "active",
                "{}",
                now_ts,
                now_ts
            })

            if dataflow_err then
                tx:rollback()
                error("Failed to create test dataflow: " .. dataflow_err)
            end

            local _, node_err = tx:execute(rebind([[
                INSERT INTO dataflow_nodes (
                    node_id, dataflow_id, type, status, config, metadata, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ]], test_ctx.db_type), {
                node_id,
                dataflow_id,
                "migration_test_node",
                "completed",
                "{}",
                "{}",
                now_ts,
                now_ts
            })

            if node_err then
                tx:rollback()
                error("Failed to create test node: " .. node_err)
            end

            local success, commit_err = tx:commit()
            if not success then
                tx:rollback()
                error("Failed to commit test resources: " .. commit_err)
            end

            table.insert(test_ctx.cleanup_dataflow_ids, dataflow_id)

            return {
                dataflow_id = dataflow_id,
                node_id = node_id,
                now_ts = now_ts
            }
        end

        local function seed_duplicate_rows(resources)
            local db = test_ctx.db :: any
            local tx, err = db:begin()
            if err then
                error("Failed to begin transaction: " .. err)
            end

            local insert_sql = rebind([[
                INSERT INTO dataflow_data (
                    data_id, dataflow_id, node_id, type, discriminator, key, content, content_type, metadata, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ]], test_ctx.db_type)

            local output_ids = { uuid.v7(), uuid.v7(), uuid.v7() }
            local result_ids = { uuid.v7(), uuid.v7(), uuid.v7() }

            for i, data_id in ipairs(output_ids) do
                local _, insert_err = tx:execute(insert_sql, {
                    data_id,
                    resources.dataflow_id,
                    resources.node_id,
                    "dataflow.output",
                    nil,
                    "final_output",
                    "output-" .. i,
                    "text/plain",
                    "{}",
                    resources.now_ts
                })
                if insert_err then
                    tx:rollback()
                    error("Failed to seed output duplicate row: " .. insert_err)
                end
            end

            for i, data_id in ipairs(result_ids) do
                local _, insert_err = tx:execute(insert_sql, {
                    data_id,
                    resources.dataflow_id,
                    resources.node_id,
                    "node.result",
                    "result.success",
                    nil,
                    "result-" .. i,
                    "text/plain",
                    "{}",
                    resources.now_ts
                })
                if insert_err then
                    tx:rollback()
                    error("Failed to seed node result duplicate row: " .. insert_err)
                end
            end

            local success, commit_err = tx:commit()
            if not success then
                tx:rollback()
                error("Failed to commit duplicate seed rows: " .. commit_err)
            end

            return {
                latest_output_id = output_ids[#output_ids],
                latest_result_id = result_ids[#result_ids]
            }
        end

        local function index_exists(index_name)
            if test_ctx.db_type == "postgres" then
                local rows = query_rows([[
                    SELECT COUNT(*) AS count
                    FROM pg_indexes
                    WHERE schemaname = 'public'
                      AND indexname = ?
                ]], { index_name })
                return rows[1].count > 0
            end

            local rows = query_rows([[
                SELECT COUNT(*) AS count
                FROM sqlite_master
                WHERE type = 'index'
                  AND name = ?
            ]], { index_name })
            return rows[1].count > 0
        end

        before_each(function()
            local db, err = sql.get("app:db")
            if err then
                error("Failed to connect to database: " .. err)
            end

            test_ctx.db = db

            local db_type, type_err = db:type()
            if type_err then
                db:release()
                test_ctx.db = nil
                error("Failed to determine database type: " .. type_err)
            end

            test_ctx.db_type = db_type
            test_ctx.cleanup_dataflow_ids = {}
            test_ctx.cleanup_migration_ids = {}

            ensure_base_schema()
        end)

        after_each(function()
            if test_ctx.db then
                for _, dataflow_id in ipairs(test_ctx.cleanup_dataflow_ids) do
                    execute_in_tx("DELETE FROM dataflows WHERE dataflow_id = ?", { dataflow_id })
                end

                for _, migration_id in ipairs(test_ctx.cleanup_migration_ids) do
                    execute("DELETE FROM _migrations WHERE id = ?", { migration_id })
                end

                test_ctx.db:release()
            end

            test_ctx.db = nil
            test_ctx.db_type = nil
            test_ctx.cleanup_dataflow_ids = {}
            test_ctx.cleanup_migration_ids = {}
        end)

        it("BC_REGRESSION_S1_migration_04_idempotent", function()
            run_migration("up", "idempotent_first")
            run_migration("up", "idempotent_second")

            test.is_true(index_exists("idx_dataflow_output_unique_slot"))
            test.is_true(index_exists("idx_node_result_success_unique_node"))
        end)

        it("BC_REGRESSION_S1_migration_04_deduplicates", function()
            local resources = create_test_resources()

            run_migration("down", "deduplicate_reset")
            test.is_false(index_exists("idx_dataflow_output_unique_slot"))
            test.is_false(index_exists("idx_node_result_success_unique_node"))

            local latest_ids = seed_duplicate_rows(resources)

            local seeded_output_rows = query_rows([[
                SELECT data_id
                FROM dataflow_data
                WHERE dataflow_id = ?
                  AND type = 'dataflow.output'
                  AND COALESCE(key, '') = COALESCE(?, '')
                  AND COALESCE(discriminator, '') = COALESCE(?, '')
                ORDER BY data_id
            ]], { resources.dataflow_id, "final_output", "" })
            test.eq(#seeded_output_rows, 3)

            local seeded_result_rows = query_rows([[
                SELECT data_id
                FROM dataflow_data
                WHERE node_id = ?
                  AND type = 'node.result'
                  AND discriminator = 'result.success'
                ORDER BY data_id
            ]], { resources.node_id })
            test.eq(#seeded_result_rows, 3)

            run_migration("up", "deduplicate_apply")

            local output_rows = query_rows([[
                SELECT data_id
                FROM dataflow_data
                WHERE dataflow_id = ?
                  AND type = 'dataflow.output'
                  AND COALESCE(key, '') = COALESCE(?, '')
                  AND COALESCE(discriminator, '') = COALESCE(?, '')
            ]], { resources.dataflow_id, "final_output", "" })
            test.eq(#output_rows, 1)
            test.eq(output_rows[1].data_id, latest_ids.latest_output_id)

            local result_rows = query_rows([[
                SELECT data_id
                FROM dataflow_data
                WHERE node_id = ?
                  AND type = 'node.result'
                  AND discriminator = 'result.success'
            ]], { resources.node_id })
            test.eq(#result_rows, 1)
            test.eq(result_rows[1].data_id, latest_ids.latest_result_id)

            test.is_true(index_exists("idx_dataflow_output_unique_slot"))
            test.is_true(index_exists("idx_node_result_success_unique_node"))
        end)
    end)
end

return test.run_cases(define_tests)
