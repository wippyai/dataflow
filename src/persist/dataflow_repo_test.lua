local test = require("test")
local uuid = require("uuid")
local json = require("json")
local time = require("time")
local sql = require("sql")
local security = require("security")
local dataflow_repo = require("dataflow_repo")

local function define_tests()
    describe("Workflow Repository", function()
        local test_actor_id_global_scope = uuid.v7()
        local actor = security.actor()
        if actor then
            test_actor_id_global_scope = actor:id()
        end

        local created_dataflow_ids_for_global_cleanup = {}

        local function register_for_global_cleanup(id)
            if id then
                local found = false
                for _, existing_id in ipairs(created_dataflow_ids_for_global_cleanup) do
                    if existing_id == id then
                        found = true
                        break
                    end
                end
                if not found then
                    table.insert(created_dataflow_ids_for_global_cleanup, id)
                end
            end
        end

        after_all(function()
            if #created_dataflow_ids_for_global_cleanup == 0 then return end
            local db, err_db = sql.get("app:db")
            if err_db then return end
            local tx, err_tx = db:begin()
            if err_tx then db:release(); return end
            local success_all = true
            for i = #created_dataflow_ids_for_global_cleanup, 1, -1 do
                local id_to_delete = created_dataflow_ids_for_global_cleanup[i]
                local delete_query = sql.builder.delete("dataflows")
                    :where("dataflow_id = ?", id_to_delete)

                local delete_exec = delete_query:run_with(tx)
                local _, err_delete = delete_exec:exec()

                if err_delete then
                    success_all = false
                end
            end
            if success_all then
                local _, err_commit = tx:commit()
                if err_commit then tx:rollback() end
            else
                tx:rollback()
            end
            db:release()
            created_dataflow_ids_for_global_cleanup = {}
        end)

        -- Helper function to create dataflows for testing
        local function create_test_dataflow(dataflow_id, actor_id, type_val, params)
            params = params or {}
            local db, err_db = sql.get("app:db")
            if err_db then return nil, "DB connection failed: " .. err_db end

            local tx, err_tx = db:begin()
            if err_tx then
                db:release()
                return nil, "Transaction begin failed: " .. err_tx
            end

            local now_ts = time.now():format(time.RFC3339)
            local metadata_json = "{}"

            if params.metadata then
                if type(params.metadata) == "table" then
                    local encoded, err_json = json.encode(params.metadata)
                    if not err_json then
                        metadata_json = encoded
                    end
                elseif type(params.metadata) == "string" then
                    metadata_json = params.metadata
                end
            end

            local dataflow_insert = sql.builder.insert("dataflows")
                :set_map({
                    dataflow_id = dataflow_id,
                    parent_dataflow_id = params.parent_dataflow_id and params.parent_dataflow_id or sql.as.null(),
                    actor_id = actor_id,
                    type = type_val,
                    status = params.status or "pending",
                    metadata = metadata_json,
                    created_at = now_ts,
                    updated_at = now_ts
                })

            local dataflow_exec = dataflow_insert:run_with(tx)
            local _, err_insert = dataflow_exec:exec()

            if err_insert then
                tx:rollback()
                db:release()
                return nil, "Failed to insert dataflow: " .. err_insert
            end

            local _, commit_err = tx:commit()
            if commit_err then
                tx:rollback()
                db:release()
                return nil, "Commit failed: " .. commit_err
            end

            db:release()
            register_for_global_cleanup(dataflow_id)

            return dataflow_repo.get(dataflow_id)
        end

        -- Helper function to create test nodes
        local function create_test_node(node_id, dataflow_id, node_type, params)
            params = params or {}
            local db, err_db = sql.get("app:db")
            if err_db then return nil, "DB connection failed: " .. err_db end

            local tx, err_tx = db:begin()
            if err_tx then
                db:release()
                return nil, "Transaction begin failed: " .. err_tx
            end

            local now_ts = time.now():format(time.RFC3339)

            local metadata_json = "{}"
            if params.metadata then
                if type(params.metadata) == "table" then
                    local encoded, err_json = json.encode(params.metadata)
                    if not err_json then
                        metadata_json = encoded
                    end
                elseif type(params.metadata) == "string" then
                    metadata_json = params.metadata
                end
            end

            local config_json = "{}"
            if params.config then
                if type(params.config) == "table" then
                    local encoded, err_json = json.encode(params.config)
                    if not err_json then
                        config_json = encoded
                    end
                elseif type(params.config) == "string" then
                    config_json = params.config
                end
            end

            local node_insert = sql.builder.insert("dataflow_nodes")
                :set_map({
                    node_id = node_id,
                    dataflow_id = dataflow_id,
                    parent_node_id = params.parent_node_id and params.parent_node_id or sql.as.null(),
                    type = node_type,
                    status = params.status or "pending",
                    config = config_json,
                    metadata = metadata_json,
                    created_at = now_ts,
                    updated_at = now_ts
                })

            local node_exec = node_insert:run_with(tx)
            local _, err_insert = node_exec:exec()

            if err_insert then
                tx:rollback()
                db:release()
                return nil, "Failed to insert node: " .. err_insert
            end

            local _, commit_err = tx:commit()
            if commit_err then
                tx:rollback()
                db:release()
                return nil, "Commit failed: " .. commit_err
            end

            db:release()
            return true, nil
        end

        describe("Read (Get) Operations", function()
            local wf_id_get
            local get_metadata = { data = "to_get" }
            before_all(function()
                wf_id_get = uuid.v7()
                local wf, err = create_test_dataflow(wf_id_get, test_actor_id_global_scope, "get_test_type",
                    { metadata = get_metadata })
                test.is_nil(err)
                test.not_nil(wf)
            end)

            it("should get an existing dataflow by ID", function()
                local wf, err = dataflow_repo.get(wf_id_get)
                test.is_nil(err)
                test.not_nil(wf)
                test.eq(wf.dataflow_id, wf_id_get)
                test.eq(wf.metadata.data, get_metadata.data)
            end)

            it("should return error for non-existent dataflow ID", function()
                local _, err = dataflow_repo.get(uuid.v7())
                test.is_true(string.find(err :: string, "Workflow not found", 1, true) ~= nil)
            end)

            it("should return error for nil or empty dataflow ID", function()
                local _, err = dataflow_repo.get(nil)
                test.is_true(string.find(err :: string, "Workflow ID is required", 1, true) ~= nil)
                _, err = dataflow_repo.get("")
                test.is_true(string.find(err :: string, "Workflow ID is required", 1, true) ~= nil)
            end)
        end)

        describe("Node Loading Operations", function()
            local nodes_test_dataflow_id
            local node_ids = {}

            before_all(function()
                nodes_test_dataflow_id = uuid.v7()

                -- Create test dataflow
                local wf, err = create_test_dataflow(nodes_test_dataflow_id, test_actor_id_global_scope, "nodes_test_type")
                test.is_nil(err)
                test.not_nil(wf)

                -- Create test nodes with various config scenarios
                local test_nodes = {
                    {
                        id = uuid.v7(),
                        type = "minimal_node",
                        params = {}
                    },
                    {
                        id = uuid.v7(),
                        type = "complex_config_node",
                        params = {
                            config = {
                                timeout = 30,
                                retries = 3,
                                endpoints = {"api1.example.com", "api2.example.com"},
                                settings = {
                                    debug = true,
                                    verbose = false
                                }
                            },
                            metadata = {
                                created_by = "test",
                                version = "1.0"
                            }
                        }
                    },
                    {
                        id = uuid.v7(),
                        type = "json_string_config_node",
                        params = {
                            config = '{"batch_size":100,"parallel":true,"features":["logging","metrics"]}',
                            metadata = '{"source":"json_test","tags":["test","config"]}'
                        }
                    },
                    {
                        id = uuid.v7(),
                        type = "invalid_json_config_node",
                        params = {
                            config = '{"invalid":json}',
                            metadata = '{"invalid":metadata}'
                        }
                    },
                    {
                        id = uuid.v7(),
                        type = "empty_string_config_node",
                        params = {
                            config = "",
                            metadata = ""
                        }
                    }
                }

                for _, test_node in ipairs(test_nodes) do
                    local success, err_node = create_test_node(test_node.id, nodes_test_dataflow_id, test_node.type, test_node.params)
                    test.is_nil(err_node)
                    test.is_true(success)
                    table.insert(node_ids, test_node.id)
                end
            end)

            it("should get nodes for dataflow and parse config correctly", function()
                local nodes, err = dataflow_repo.get_nodes_for_dataflow(nodes_test_dataflow_id)

                test.is_nil(err)
                test.not_nil(nodes)
                test.eq(#nodes, 5)

                -- Sort nodes by type for predictable testing
                table.sort(nodes, function(a, b) return a.type < b.type end)

                -- Test complex config node
                local complex_node = nil
                for _, node in ipairs(nodes) do
                    if node.type == "complex_config_node" then
                        complex_node = node
                        break
                    end
                end

                test.not_nil(complex_node)
                test.is_table(complex_node.config)
                test.eq(complex_node.config.timeout, 30)
                test.eq(complex_node.config.retries, 3)
                test.is_table(complex_node.config.endpoints)
                test.eq(#complex_node.config.endpoints, 2)
                test.eq(complex_node.config.endpoints[1], "api1.example.com")
                test.is_table(complex_node.config.settings)
                test.is_true(complex_node.config.settings.debug)
                test.is_false(complex_node.config.settings.verbose)

                test.is_table(complex_node.metadata)
                test.eq(complex_node.metadata.created_by, "test")
                test.eq(complex_node.metadata.version, "1.0")
            end)

            it("should parse JSON string config correctly", function()
                local nodes, err = dataflow_repo.get_nodes_for_dataflow(nodes_test_dataflow_id)
                test.is_nil(err)

                local json_node = nil
                for _, node in ipairs(nodes) do
                    if node.type == "json_string_config_node" then
                        json_node = node
                        break
                    end
                end

                test.not_nil(json_node)
                test.is_table(json_node.config)
                test.eq(json_node.config.batch_size, 100)
                test.is_true(json_node.config.parallel)
                test.is_table(json_node.config.features)
                test.eq(#json_node.config.features, 2)
                test.eq(json_node.config.features[1], "logging")
                test.eq(json_node.config.features[2], "metrics")

                test.is_table(json_node.metadata)
                test.eq(json_node.metadata.source, "json_test")
                test.is_table(json_node.metadata.tags)
                test.eq(#json_node.metadata.tags, 2)
            end)

            it("should handle minimal node with empty config", function()
                local nodes, err = dataflow_repo.get_nodes_for_dataflow(nodes_test_dataflow_id)
                test.is_nil(err)

                local minimal_node = nil
                for _, node in ipairs(nodes) do
                    if node.type == "minimal_node" then
                        minimal_node = node
                        break
                    end
                end

                test.not_nil(minimal_node)
                test.is_table(minimal_node.config)
                test.is_nil(next(minimal_node.config))
                test.is_table(minimal_node.metadata)
                test.is_nil(next(minimal_node.metadata))
            end)

            it("should handle invalid JSON gracefully", function()
                local nodes, err = dataflow_repo.get_nodes_for_dataflow(nodes_test_dataflow_id)
                test.is_nil(err)

                local invalid_node = nil
                for _, node in ipairs(nodes) do
                    if node.type == "invalid_json_config_node" then
                        invalid_node = node
                        break
                    end
                end

                test.not_nil(invalid_node)
                -- Invalid JSON should default to empty table
                test.is_table(invalid_node.config)
                test.is_nil(next(invalid_node.config))
                test.is_table(invalid_node.metadata)
                test.is_nil(next(invalid_node.metadata))
            end)

            it("should handle empty string config", function()
                local nodes, err = dataflow_repo.get_nodes_for_dataflow(nodes_test_dataflow_id)
                test.is_nil(err)

                local empty_node = nil
                for _, node in ipairs(nodes) do
                    if node.type == "empty_string_config_node" then
                        empty_node = node
                        break
                    end
                end

                test.not_nil(empty_node)
                test.is_table(empty_node.config)
                test.is_nil(next(empty_node.config))
                test.is_table(empty_node.metadata)
                test.is_nil(next(empty_node.metadata))
            end)

            it("should return error for missing dataflow ID", function()
                local nodes, err = dataflow_repo.get_nodes_for_dataflow(nil)
                test.is_nil(nodes)
                test.is_true(string.find(err :: string, "Workflow ID is required", 1, true) ~= nil)

                nodes, err = dataflow_repo.get_nodes_for_dataflow("")
                test.is_nil(nodes)
                test.is_true(string.find(err :: string, "Workflow ID is required", 1, true) ~= nil)
            end)

            it("should return empty array for dataflow with no nodes", function()
                local empty_dataflow_id = uuid.v7()
                local _, err = create_test_dataflow(empty_dataflow_id, test_actor_id_global_scope, "empty_nodes_test")
                test.is_nil(err)

                local nodes, err_nodes = dataflow_repo.get_nodes_for_dataflow(empty_dataflow_id)
                test.is_nil(err_nodes)
                test.is_table(nodes)
                test.eq(#nodes, 0)
            end)

            it("should return error for non-existent dataflow", function()
                local fake_dataflow_id = uuid.v7()
                local nodes, err = dataflow_repo.get_nodes_for_dataflow(fake_dataflow_id)
                test.is_nil(err)
                test.is_table(nodes)
                test.eq(#nodes, 0)
            end)
        end)

        describe("List Operations", function()
            local list_operations_actor_id = uuid.v7()
            local user1_id = list_operations_actor_id
            local user2_id = uuid.v7()
            local list_parent_id = uuid.v7()
            local list_suite_temp_created_ids = {}

            before_all(function()
                -- Create parent dataflow
                local _, p_err = create_test_dataflow(list_parent_id, user1_id, "list_parent_type")
                test.is_nil(p_err)
                table.insert(list_suite_temp_created_ids, list_parent_id)

                -- Create test dataflows for listing tests
                local items = {
                    { id = uuid.v7(), actor_id = user1_id, type = "typeA", status = "pending" },
                    { id = uuid.v7(), actor_id = user1_id, type = "typeA", status = "completed" },
                    { id = uuid.v7(), actor_id = user1_id, type = "typeB", status = "pending" },
                    { id = uuid.v7(), actor_id = user1_id, type = "typeB", status = "running", parent_dataflow_id = list_parent_id },
                    { id = uuid.v7(), actor_id = user2_id, type = "typeA", status = "pending" },
                }

                for _, item_spec in ipairs(items) do
                    local _, err_create = create_test_dataflow(item_spec.id, item_spec.actor_id, item_spec.type, {
                        status = item_spec.status,
                        parent_dataflow_id = item_spec.parent_dataflow_id
                    })
                    test.is_nil(err_create)
                    table.insert(list_suite_temp_created_ids, item_spec.id)
                end
            end)

            it("should list dataflows by actor_id", function()
                local wfs, err = dataflow_repo.list_by_user(user1_id)
                test.is_nil(err)
                test.eq(#wfs, 5)
                for _, wf in ipairs(wfs) do test.eq(wf.actor_id, user1_id) end
            end)

            it("should list by actor_id with status filter", function()
                local wfs, err = dataflow_repo.list_by_user(user1_id, { status = "pending" })
                test.is_nil(err)
                test.eq(#wfs, 3)
                for _, wf in ipairs(wfs) do test.eq(wf.status, "pending") end
            end)

            it("should list by actor_id with type filter", function()
                local wfs, err = dataflow_repo.list_by_user(user1_id, { type = "typeA" })
                test.is_nil(err)
                test.eq(#wfs, 2)
                for _, wf in ipairs(wfs) do test.eq(wf.type, "typeA") end
            end)

            it("should list by actor_id with parent_dataflow_id filter (specific parent)", function()
                local wfs, err = dataflow_repo.list_by_user(user1_id, { parent_dataflow_id = list_parent_id })
                test.is_nil(err)
                test.eq(#wfs, 1)
            end)

            it("should list by actor_id with parent_dataflow_id filter (NULL for root)", function()
                local wfs, err = dataflow_repo.list_by_user(user1_id, { parent_dataflow_id = "NULL" })
                test.is_nil(err)
                test.eq(#wfs, 4)
                for _, wf in ipairs(wfs) do test.is_nil(wf.parent_dataflow_id) end
            end)

            it("should list by actor_id with limit and offset", function()
                local wfs_all_u1, _ = dataflow_repo.list_by_user(user1_id)
                local total_u1 = #wfs_all_u1
                local wfs_p1, _ = dataflow_repo.list_by_user(user1_id, { limit = 2 })
                test.eq(#wfs_p1, 2)
                if total_u1 > 2 then
                    local wfs_p2, _ = dataflow_repo.list_by_user(user1_id, { limit = 2, offset = 2 })
                    test.eq(#wfs_p2, math.min(2, total_u1 - 2))
                    test.is_true((wfs_p1 :: any)[1].dataflow_id ~= (wfs_p2 :: any)[1].dataflow_id)
                end
            end)

            it("should list children of a parent dataflow", function()
                local children, err = dataflow_repo.list_children(list_parent_id)
                test.is_nil(err)
                test.eq(#children, 1)
            end)

            it("should return empty list for user with no dataflows", function()
                local wfs, err = dataflow_repo.list_by_user(uuid.v7())
                test.is_nil(err)
                test.eq(#wfs, 0)
            end)
        end)
    end)
end

return test.run_cases(define_tests)
