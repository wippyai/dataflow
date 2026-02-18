local test = require("test")
local uuid = require("uuid")
local json = require("json")
local time = require("time")
local sql = require("sql")

local node_reader = require("node_reader")

local function define_tests()
    local test_dataflow_id
    local test_node_ids = {}
    local test_parent_node_id
    describe("Node Reader", function()
        local function get_test_db()
            local db, err = sql.get("app:db")
            if err then error("Failed to connect to database: " .. err) end
            return db
        end

        before_all(function()
            local db = get_test_db()
            local tx, err_tx = db:begin()
            if err_tx then
                db:release(); error("Failed to begin transaction: " .. err_tx)
            end

            local now_ts = time.now():format(time.RFC3339)

            test_dataflow_id = uuid.v7()
            local test_actor_id = "test-actor-" .. uuid.v7()

            local dataflow_insert = sql.builder.insert("dataflows")
                :set_map({
                    dataflow_id = test_dataflow_id,
                    actor_id = test_actor_id,
                    type = "node_reader_test",
                    status = "active",
                    metadata = "{}",
                    created_at = now_ts,
                    updated_at = now_ts
                })

            local dataflow_exec = dataflow_insert:run_with(tx)
            local _, wf_err = dataflow_exec:exec()

            if wf_err then
                tx:rollback()
                db:release()
                error("Failed to create test dataflow: " .. wf_err)
            end

            local test_nodes = {
                {
                    node_id = uuid.v7(),
                    parent_node_id = nil,
                    type = "root_node",
                    status = "pending",
                    config = {
                        timeout = 30,
                        retries = 3,
                        mode = "strict"
                    },
                    metadata = {
                        source = "test",
                        category = "root"
                    }
                },
                {
                    node_id = uuid.v7(),
                    parent_node_id = nil,
                    type = "template_node",
                    status = "template",
                    config = {
                        func_id = "test_function",
                        data_targets = {
                            {
                                data_type = "node.output",
                                key = "result"
                            }
                        }
                    },
                    metadata = {
                        template_type = "function_executor",
                        version = "1.0"
                    }
                },
                {
                    node_id = uuid.v7(),
                    parent_node_id = nil,
                    type = "func_node",
                    status = "running",
                    config = {
                        func_id = "active_function",
                        timeout = 60
                    },
                    metadata = {
                        started_at = now_ts,
                        worker_id = "worker_123"
                    }
                },
                {
                    node_id = uuid.v7(),
                    parent_node_id = nil,
                    type = "func_node",
                    status = "completed",
                    config = {
                        func_id = "completed_function"
                    },
                    metadata = {
                        completed_at = now_ts,
                        duration_ms = 1500
                    }
                },
                {
                    node_id = uuid.v7(),
                    parent_node_id = nil,
                    type = "func_node",
                    status = "failed",
                    config = {},
                    metadata = {
                        error = "Function execution failed",
                        failed_at = now_ts
                    }
                },
                {
                    node_id = uuid.v7(),
                    parent_node_id = nil,
                    type = "minimal_node",
                    status = "pending",
                    config = "",
                    metadata = ""
                },
                {
                    node_id = uuid.v7(),
                    parent_node_id = nil,
                    type = "invalid_json_node",
                    status = "pending",
                    config = '{"invalid":json}',
                    metadata = '{"invalid":metadata}'
                }
            }

            for i, node_spec in ipairs(test_nodes) do
                local node_id = node_spec.node_id
                test_node_ids[node_spec.type] = node_id

                local parent_id = nil
                if i > 1 and i <= 5 then
                    parent_id = test_node_ids["root_node"]
                    if i == 2 then
                        test_parent_node_id = test_node_ids["root_node"]
                    end
                end

                local config_json = "{}"
                if node_spec.config then
                    if type(node_spec.config) == "table" then
                        local encoded, err_encode = json.encode(node_spec.config)
                        if not err_encode then
                            config_json = encoded
                        end
                    elseif type(node_spec.config) == "string" then
                        config_json = node_spec.config
                    end
                end

                local metadata_json = "{}"
                if node_spec.metadata then
                    if type(node_spec.metadata) == "table" then
                        local encoded, err_encode = json.encode(node_spec.metadata)
                        if not err_encode then
                            metadata_json = encoded
                        end
                    elseif type(node_spec.metadata) == "string" then
                        metadata_json = node_spec.metadata
                    end
                end

                local node_insert = sql.builder.insert("dataflow_nodes")
                    :set_map({
                        node_id = node_id,
                        dataflow_id = test_dataflow_id,
                        parent_node_id = parent_id and parent_id or sql.as.null(),
                        type = node_spec.type,
                        status = node_spec.status,
                        config = config_json,
                        metadata = metadata_json,
                        created_at = now_ts,
                        updated_at = now_ts
                    })

                local node_exec = node_insert:run_with(tx)
                local _, node_err = node_exec:exec()

                if node_err then
                    tx:rollback()
                    db:release()
                    error("Failed to create test node: " .. node_err)
                end
            end

            local _, commit_err = tx:commit()
            if commit_err then
                tx:rollback()
                db:release()
                error("Failed to commit test data: " .. commit_err)
            end

            db:release()
        end)

        after_all(function()
            local db = get_test_db()
            local tx, err_tx = db:begin()
            if err_tx then
                db:release(); return
            end

            local delete_query = sql.builder.delete("dataflows")
                :where("dataflow_id = ?", test_dataflow_id)

            local delete_exec = delete_query:run_with(tx)
            local _, del_err = delete_exec:exec()

            if del_err then
                tx:rollback()
                db:release()
                return
            end

            local _, commit_err = tx:commit()
            if commit_err then
                tx:rollback()
                db:release()
                return
            end

            db:release()
        end)

        describe("Basic Operations", function()
            it("should initialize with a dataflow ID", function()
                local reader, err = node_reader.with_dataflow(test_dataflow_id)
                test.is_nil(err)
                test.not_nil(reader)
            end)

            it("should return error when initialized without a dataflow ID", function()
                local reader1, err1 = node_reader.with_dataflow(nil)
                test.is_nil(reader1)
                test.contains(err1, "Workflow ID is required")

                local reader2, err2 = node_reader.with_dataflow("")
                test.is_nil(reader2)
                test.contains(err2, "Workflow ID is required")
            end)

            it("should return all nodes for a dataflow", function()
                local reader, err = node_reader.with_dataflow(test_dataflow_id)
                test.is_nil(err)

                local results, query_err = (reader :: any):all()
                test.is_nil(query_err)
                test.eq(#results, 7)

                test.is_table(results[1].config)
                test.is_table(results[1].metadata)
            end)

            it("should count all nodes for a dataflow", function()
                local reader, err = node_reader.with_dataflow(test_dataflow_id)
                test.is_nil(err)

                local count, query_err = (reader :: any):count()
                test.is_nil(query_err)
                test.eq(count, 7)
            end)

            it("should check existence of nodes", function()
                local reader, err = node_reader.with_dataflow(test_dataflow_id)
                test.is_nil(err)

                local exists, query_err = (reader :: any):exists()
                test.is_nil(query_err)
                test.is_true(exists)

                local reader2, err2 = node_reader.with_dataflow(uuid.v7())
                test.is_nil(err2)

                local non_exists, query_err2 = (reader2 :: any):exists()
                test.is_nil(query_err2)
                test.is_false(non_exists)
            end)
        end)

        describe("Filtering", function()
            it("should filter by node ID", function()
                local root_node_id = test_node_ids["root_node"]
                local reader, err = node_reader.with_dataflow(test_dataflow_id)
                test.is_nil(err)

                local results, query_err = reader
                    :with_nodes(root_node_id)
                    :all()
                test.is_nil(query_err)

                test.eq(#results, 1)
                test.eq(results[1].node_id, root_node_id)
                test.eq(results[1].type, "root_node")
            end)

            it("should filter by multiple node IDs", function()
                local root_id = test_node_ids["root_node"]
                local template_id = test_node_ids["template_node"]

                local reader, err = node_reader.with_dataflow(test_dataflow_id)
                test.is_nil(err)

                local results, query_err = reader
                    :with_nodes(root_id, template_id)
                    :all()
                test.is_nil(query_err)

                test.eq(#results, 2)
                local found_types = {}
                for _, node in ipairs(results) do
                    found_types[node.type] = true
                end
                test.is_true(found_types["root_node"])
                test.is_true(found_types["template_node"])
            end)

            it("should filter by node type", function()
                local reader, err = node_reader.with_dataflow(test_dataflow_id)
                test.is_nil(err)

                local results, query_err = reader
                    :with_node_types("func_node")
                    :all()
                test.is_nil(query_err)

                test.eq(#results, 3)
                for _, node in ipairs(results) do
                    test.eq(node.type, "func_node")
                end
            end)

            it("should filter by multiple node types", function()
                local reader, err = node_reader.with_dataflow(test_dataflow_id)
                test.is_nil(err)

                local results, query_err = reader
                    :with_node_types("root_node", "template_node")
                    :all()
                test.is_nil(query_err)

                test.eq(#results, 2)
                for _, node in ipairs(results) do
                    test.is_true(node.type == "root_node" or node.type == "template_node")
                end
            end)

            it("should filter by status", function()
                local reader, err = node_reader.with_dataflow(test_dataflow_id)
                test.is_nil(err)

                local results, query_err = reader
                    :with_statuses("pending")
                    :all()
                test.is_nil(query_err)

                test.eq(#results, 3)
                for _, node in ipairs(results) do
                    test.eq(node.status, "pending")
                end
            end)

            it("should filter by multiple statuses", function()
                local reader, err = node_reader.with_dataflow(test_dataflow_id)
                test.is_nil(err)

                local results, query_err = reader
                    :with_statuses("running", "completed")
                    :all()
                test.is_nil(query_err)

                test.eq(#results, 2)
                for _, node in ipairs(results) do
                    test.is_true(node.status == "running" or node.status == "completed")
                end
            end)

            it("should filter by parent node ID", function()
                local reader, err = node_reader.with_dataflow(test_dataflow_id)
                test.is_nil(err)

                local results, query_err = reader
                    :with_parent_nodes(test_parent_node_id)
                    :all()
                test.is_nil(query_err)

                test.eq(#results, 4)
                for _, node in ipairs(results) do
                    test.eq(node.parent_node_id, test_parent_node_id)
                end
            end)

            it("should filter by multiple parent node IDs", function()
                local fake_parent_id = uuid.v7()
                local reader, err = node_reader.with_dataflow(test_dataflow_id)
                test.is_nil(err)

                local results, query_err = reader
                    :with_parent_nodes(test_parent_node_id, fake_parent_id)
                    :all()
                test.is_nil(query_err)

                test.eq(#results, 4)
                for _, node in ipairs(results) do
                    test.eq(node.parent_node_id, test_parent_node_id)
                end
            end)

            it("should combine multiple filters", function()
                local reader, err = node_reader.with_dataflow(test_dataflow_id)
                test.is_nil(err)

                local results, query_err = reader
                    :with_parent_nodes(test_parent_node_id)
                    :with_node_types("func_node")
                    :with_statuses("running", "completed")
                    :all()
                test.is_nil(query_err)

                test.eq(#results, 2)
                for _, node in ipairs(results) do
                    test.eq(node.type, "func_node")
                    test.eq(node.parent_node_id, test_parent_node_id)
                    test.is_true(node.status == "running" or node.status == "completed")
                end
            end)
        end)

        describe("Fetch Options", function()
            it("should exclude config when specified", function()
                local reader, err = node_reader.with_dataflow(test_dataflow_id)
                test.is_nil(err)

                local results, query_err = reader
                    :fetch_options({ config = false })
                    :all()
                test.is_nil(query_err)

                test.gt(#results, 0)
                for _, node in ipairs(results) do
                    test.is_nil(node.config)
                end
            end)

            it("should exclude metadata when specified", function()
                local reader, err = node_reader.with_dataflow(test_dataflow_id)
                test.is_nil(err)

                local results, query_err = reader
                    :fetch_options({ metadata = false })
                    :all()
                test.is_nil(query_err)

                test.gt(#results, 0)
                for _, node in ipairs(results) do
                    test.is_nil(node.metadata)
                end
            end)

            it("should fetch only basic fields when config and metadata excluded", function()
                local reader, err = node_reader.with_dataflow(test_dataflow_id)
                test.is_nil(err)

                local results, query_err = reader
                    :fetch_options({ config = false, metadata = false })
                    :all()
                test.is_nil(query_err)

                test.gt(#results, 0)
                for _, node in ipairs(results) do
                    test.not_nil(node.node_id)
                    test.not_nil(node.type)
                    test.not_nil(node.status)
                    test.is_nil(node.config)
                    test.is_nil(node.metadata)
                end
            end)
        end)

        describe("One Result", function()
            it("should fetch a single result", function()
                local root_node_id = test_node_ids["root_node"]
                local reader, err = node_reader.with_dataflow(test_dataflow_id)
                test.is_nil(err)

                local node, query_err = reader
                    :with_nodes(root_node_id)
                    :one()
                test.is_nil(query_err)

                test.not_nil(node)
                test.eq(node.node_id, root_node_id)
                test.eq(node.type, "root_node")
                test.is_table(node.config)
                test.is_table(node.metadata)
            end)

            it("should return nil for non-matching query", function()
                local fake_node_id = uuid.v7()
                local reader, err = node_reader.with_dataflow(test_dataflow_id)
                test.is_nil(err)

                local node, query_err = reader
                    :with_nodes(fake_node_id)
                    :one()
                test.is_nil(query_err)
                test.is_nil(node)
            end)

            it("should respect fetch options", function()
                local root_node_id = test_node_ids["root_node"]
                local reader, err = node_reader.with_dataflow(test_dataflow_id)
                test.is_nil(err)

                local node, query_err = reader
                    :with_nodes(root_node_id)
                    :fetch_options({ config = false })
                    :one()
                test.is_nil(query_err)

                test.not_nil(node)
                test.eq(node.node_id, root_node_id)
                test.is_nil(node.config)
                test.is_table(node.metadata)
            end)
        end)

        describe("JSON Parsing", function()
            it("should parse complex config correctly", function()
                local template_node_id = test_node_ids["template_node"]
                local reader, err = node_reader.with_dataflow(test_dataflow_id)
                test.is_nil(err)

                local node, query_err = reader
                    :with_nodes(template_node_id)
                    :one()
                test.is_nil(query_err)

                test.not_nil(node)
                test.is_table(node.config)
                test.eq(node.config.func_id, "test_function")
                test.is_table(node.config.data_targets)
                test.eq(#node.config.data_targets, 1)
                test.eq(node.config.data_targets[1].data_type, "node.output")
                test.eq(node.config.data_targets[1].key, "result")

                test.is_table(node.metadata)
                test.eq(node.metadata.template_type, "function_executor")
                test.eq(node.metadata.version, "1.0")
            end)

            it("should handle empty string config/metadata", function()
                local minimal_node_id = test_node_ids["minimal_node"]
                local reader, err = node_reader.with_dataflow(test_dataflow_id)
                test.is_nil(err)

                local node, query_err = reader
                    :with_nodes(minimal_node_id)
                    :one()
                test.is_nil(query_err)

                test.not_nil(node)
                test.is_table(node.config)
                test.is_nil(next(node.config))
                test.is_table(node.metadata)
                test.is_nil(next(node.metadata))
            end)

            it("should handle invalid JSON gracefully", function()
                local invalid_node_id = test_node_ids["invalid_json_node"]
                local reader, err = node_reader.with_dataflow(test_dataflow_id)
                test.is_nil(err)

                local node, query_err = reader
                    :with_nodes(invalid_node_id)
                    :one()
                test.is_nil(query_err)

                test.not_nil(node)
                test.is_table(node.config)
                test.is_nil(next(node.config))
                test.is_table(node.metadata)
                test.is_nil(next(node.metadata))
            end)
        end)

        describe("Template Node Discovery", function()
            it("should find template nodes by status", function()
                local reader, err = node_reader.with_dataflow(test_dataflow_id)
                test.is_nil(err)

                local templates, query_err = reader
                    :with_statuses("template")
                    :all()
                test.is_nil(query_err)

                test.eq(#templates, 1)
                test.eq(templates[1].status, "template")
                test.eq(templates[1].type, "template_node")
            end)

            it("should find template children of a specific parent", function()
                local reader, err = node_reader.with_dataflow(test_dataflow_id)
                test.is_nil(err)

                local templates, query_err = reader
                    :with_parent_nodes(test_parent_node_id)
                    :with_statuses("template")
                    :all()
                test.is_nil(query_err)

                test.eq(#templates, 1)
                test.eq(templates[1].status, "template")
                test.eq(templates[1].parent_node_id, test_parent_node_id)
                test.eq(templates[1].config.func_id, "test_function")
            end)

            it("should find all children of a parent node", function()
                local reader, err = node_reader.with_dataflow(test_dataflow_id)
                test.is_nil(err)

                local children, query_err = reader
                    :with_parent_nodes(test_parent_node_id)
                    :all()
                test.is_nil(query_err)

                test.eq(#children, 4)
                for _, child in ipairs(children) do
                    test.eq(child.parent_node_id, test_parent_node_id)
                end

                local statuses = {}
                for _, child in ipairs(children) do
                    statuses[child.status] = true
                end
                test.is_true(statuses["template"])
                test.is_true(statuses["running"])
                test.is_true(statuses["completed"])
                test.is_true(statuses["failed"])
            end)
        end)
    end)
end

return test.run_cases(define_tests)
