local test = require("test")
local uuid = require("uuid")
local json = require("json")
local time = require("time")
local sql = require("sql")
local data_reader = require("data_reader")

local function define_tests()
    -- Test data setup
    local test_dataflow_id
    local test_node_id_1
    local test_node_id_2
    local test_data_ids = {}
    local test_reference_data_id
    local test_target_data_id

    describe("Data Reader", function()
        -- Helper to get a DB connection for setup/cleanup
        local function get_test_db()
            local db, err = sql.get("app:db")
            if err then error("Failed to connect to database: " .. err) end
            return db
        end

        -- Create test data fixtures
        before_all(function()
            local db = get_test_db()
            local tx, err_tx = db:begin()
            if err_tx then
                db:release(); error("Failed to begin transaction: " .. err_tx)
            end

            local now_ts = time.now():format(time.RFC3339)

            -- Create test dataflow
            test_dataflow_id = uuid.v7()
            local test_actor_id = "test-actor-" .. uuid.v7()

            local dataflow_insert = sql.builder.insert("dataflows")
                :set_map({
                    dataflow_id = test_dataflow_id,
                    actor_id = test_actor_id,
                    type = "data_reader_test",
                    status = "active",
                    metadata = "{}",
                    created_at = now_ts,
                    updated_at = now_ts
                })

            local dataflow_exec = dataflow_insert:run_with(tx)
            local dataflow_result, wf_err = dataflow_exec:exec()

            if wf_err then
                tx:rollback()
                db:release()
                error("Failed to create test dataflow: " .. wf_err)
            end

            -- Create test nodes
            test_node_id_1 = uuid.v7()
            test_node_id_2 = uuid.v7()

            local node1_insert = sql.builder.insert("dataflow_nodes")
                :set_map({
                    node_id = test_node_id_1,
                    dataflow_id = test_dataflow_id,
                    type = "test_node_type_1",
                    status = "active",
                    metadata = "{}",
                    created_at = now_ts,
                    updated_at = now_ts
                })

            local node1_exec = node1_insert:run_with(tx)
            local _, node1_err = node1_exec:exec()

            if node1_err then
                tx:rollback()
                db:release()
                error("Failed to create test node 1: " .. node1_err)
            end

            local node2_insert = sql.builder.insert("dataflow_nodes")
                :set_map({
                    node_id = test_node_id_2,
                    dataflow_id = test_dataflow_id,
                    type = "test_node_type_2",
                    status = "active",
                    metadata = "{}",
                    created_at = now_ts,
                    updated_at = now_ts
                })

            local node2_exec = node2_insert:run_with(tx)
            local node_result, node_err = node2_exec:exec()

            if node_err then
                tx:rollback()
                db:release()
                error("Failed to create test nodes: " .. node_err)
            end

            -- Create target data first to reference later
            test_target_data_id = uuid.v7()
            local target_insert = sql.builder.insert("dataflow_data")
                :set_map({
                    data_id = test_target_data_id,
                    dataflow_id = test_dataflow_id,
                    node_id = sql.as.null(),
                    type = "target_type",
                    discriminator = "test_target",
                    key = "target_key",
                    content = "Target content value",
                    content_type = "text/plain",
                    metadata = json.encode({ target_meta = "Target metadata value" }),
                    created_at = now_ts
                })

            local target_exec = target_insert:run_with(tx)
            local target_result, target_err = target_exec:exec()

            if target_err then
                tx:rollback()
                db:release()
                error("Failed to create target data: " .. target_err)
            end

            -- Create test data records
            local data_items = {
                -- Workflow-level data
                {
                    data_id = uuid.v7(),
                    node_id = nil,
                    type = "config",
                    discriminator = "default",
                    key = "global_settings",
                    content = json.encode({ theme = "dark", fontSize = 14 }),
                    content_type = "application/json",
                    metadata = json.encode({ source = "system" })
                },
                {
                    data_id = uuid.v7(),
                    node_id = nil,
                    type = "config",
                    discriminator = "user",
                    key = "user_preferences",
                    content = json.encode({ notifications = true, language = "en" }),
                    content_type = "application/json",
                    metadata = json.encode({ source = "user" })
                },
                -- Node 1 data
                {
                    data_id = uuid.v7(),
                    node_id = test_node_id_1,
                    type = "input",
                    discriminator = "default",
                    key = "text_input",
                    content = "This is a test input",
                    content_type = "text/plain",
                    metadata = json.encode({ source = "user", timestamp = now_ts })
                },
                {
                    data_id = uuid.v7(),
                    node_id = test_node_id_1,
                    type = "output",
                    discriminator = "default",
                    key = "processed_output",
                    content = json.encode({ result = "Processed test input", score = 0.95 }),
                    content_type = "application/json",
                    metadata = json.encode({ processed_at = now_ts })
                },
                -- Node 2 data
                {
                    data_id = uuid.v7(),
                    node_id = test_node_id_2,
                    type = "input",
                    discriminator = "default",
                    key = "numeric_input",
                    content = json.encode({ value = 42, unit = "meters" }),
                    content_type = "application/json",
                    metadata = json.encode({ source = "sensor" })
                },
                {
                    data_id = uuid.v7(),
                    node_id = test_node_id_2,
                    type = "output",
                    discriminator = "default",
                    key = "calculation_result",
                    content = json.encode({ result = 84, unit = "meters" }),
                    content_type = "application/json",
                    metadata = json.encode({ formula = "value * 2" })
                },
                -- Reference data item
                {
                    data_id = uuid.v7(),
                    node_id = nil,
                    type = "reference",
                    discriminator = "default",
                    key = test_target_data_id, -- Reference to the target
                    content = "Reference to target",
                    content_type = "dataflow/reference",
                    metadata = json.encode({ ref_created_at = now_ts })
                }
            }

            for _, item in ipairs(data_items) do
                test_data_ids[item.key] = item.data_id

                -- Save reference ID for tests
                if item.content_type == "dataflow/reference" then
                    test_reference_data_id = item.data_id
                end

                local data_insert = sql.builder.insert("dataflow_data")
                    :set_map({
                        data_id = item.data_id,
                        dataflow_id = test_dataflow_id,
                        node_id = item.node_id and item.node_id or sql.as.null(),
                        type = item.type,
                        discriminator = item.discriminator,
                        key = item.key,
                        content = item.content,
                        content_type = item.content_type,
                        metadata = item.metadata,
                        created_at = now_ts
                    })

                local data_exec = data_insert:run_with(tx)
                local data_result, data_err = data_exec:exec()

                if data_err then
                    tx:rollback()
                    db:release()
                    error("Failed to create test data: " .. data_err)
                end
            end

            local commit_result, commit_err = tx:commit()
            if commit_err then
                tx:rollback()
                db:release()
                error("Failed to commit test data: " .. commit_err)
            end

            db:release()
        end)

        -- Clean up test data
        after_all(function()
            local db = get_test_db()
            local tx, err_tx = db:begin()
            if err_tx then
                print("ERROR: Failed to begin cleanup transaction"); db:release(); return
            end

            -- Delete the dataflow (should cascade to nodes and data)
            local del_result, del_err = tx:execute("DELETE FROM dataflows WHERE dataflow_id = ?", { test_dataflow_id })
            if del_err then
                tx:rollback()
                db:release()
                print("ERROR: Failed to clean up test data: " .. del_err)
                return
            end

            local commit_result, commit_err = tx:commit()
            if commit_err then
                tx:rollback()
                db:release()
                print("ERROR: Failed to commit cleanup: " .. commit_err)
                return
            end

            db:release()
        end)

        describe("Basic Operations", function()
            it("should initialize with a dataflow ID", function()
                local reader = data_reader.with_dataflow(test_dataflow_id)
                expect(reader).not_to_be_nil()
            end)

            it("should error when initialized without a dataflow ID", function()
                local success1 = pcall(function() data_reader.with_dataflow(nil) end)
                expect(success1).to_be_false()

                local success2 = pcall(function() data_reader.with_dataflow("") end)
                expect(success2).to_be_false()
            end)

            it("should return all data for a dataflow", function()
                local results = data_reader.with_dataflow(test_dataflow_id):all()
                expect(#results).to_equal(8) -- 6 original items + 1 target + 1 reference

                -- Check that metadata is parsed automatically
                expect(results[1].metadata).to_be_type("table")
            end)

            it("should count all data for a dataflow", function()
                local count = data_reader.with_dataflow(test_dataflow_id):count()
                expect(count).to_equal(8) -- 6 original items + 1 target + 1 reference
            end)

            it("should check existence of data", function()
                local exists = data_reader.with_dataflow(test_dataflow_id):exists()
                expect(exists).to_be_true()

                -- Should not exist for non-existent dataflow
                local non_exists = data_reader.with_dataflow(uuid.v7()):exists()
                expect(non_exists).to_be_false()
            end)
        end)

        describe("Filtering", function()
            it("should filter by node ID", function()
                local results = data_reader.with_dataflow(test_dataflow_id)
                    :with_nodes(test_node_id_1)
                    :all()

                expect(#results).to_equal(2)
                for _, item in ipairs(results) do
                    expect(item.node_id).to_equal(test_node_id_1)
                end
            end)

            it("should filter by multiple node IDs", function()
                local results = data_reader.with_dataflow(test_dataflow_id)
                    :with_nodes(test_node_id_1, test_node_id_2)
                    :all()

                expect(#results).to_equal(4)
                for _, item in ipairs(results) do
                    expect(item.node_id == test_node_id_1 or item.node_id == test_node_id_2).to_be_true()
                end
            end)

            it("should filter by data type", function()
                local results = data_reader.with_dataflow(test_dataflow_id)
                    :with_data_types("config")
                    :all()

                expect(#results).to_equal(2)
                for _, item in ipairs(results) do
                    expect(item.type).to_equal("config")
                end
            end)

            it("should filter by multiple data types", function()
                local results = data_reader.with_dataflow(test_dataflow_id)
                    :with_data_types("input", "output")
                    :all()

                expect(#results).to_equal(4)
                for _, item in ipairs(results) do
                    expect(item.type == "input" or item.type == "output").to_be_true()
                end
            end)

            it("should filter by data key", function()
                local results = data_reader.with_dataflow(test_dataflow_id)
                    :with_data_keys("global_settings")
                    :all()

                expect(#results).to_equal(1)
                expect(results[1].key).to_equal("global_settings")
            end)

            it("should filter by multiple data keys", function()
                local results = data_reader.with_dataflow(test_dataflow_id)
                    :with_data_keys("global_settings", "user_preferences")
                    :all()

                expect(#results).to_equal(2)
                expect(results[1].key == "global_settings" or results[1].key == "user_preferences").to_be_true()
                expect(results[2].key == "global_settings" or results[2].key == "user_preferences").to_be_true()
            end)

            it("should filter by discriminator", function()
                local results = data_reader.with_dataflow(test_dataflow_id)
                    :with_data_discriminators("user")
                    :all()

                expect(#results).to_equal(1)
                expect(results[1].discriminator).to_equal("user")
            end)

            it("should combine multiple filters", function()
                local results = data_reader.with_dataflow(test_dataflow_id)
                    :with_nodes(test_node_id_1, test_node_id_2)
                    :with_data_types("input")
                    :all()

                expect(#results).to_equal(2)
                for _, item in ipairs(results) do
                    expect(item.type).to_equal("input")
                    expect(item.node_id == test_node_id_1 or item.node_id == test_node_id_2).to_be_true()
                end
            end)
        end)

        describe("Fetch Options", function()
            it("should exclude content when specified", function()
                local results = data_reader.with_dataflow(test_dataflow_id)
                    :fetch_options({ content = false })
                    :all()

                expect(#results).to_be_greater_than(0)
                for _, item in ipairs(results) do
                    expect(item.content).to_be_nil()
                    expect(item.content_type).to_be_nil()
                end
            end)

            it("should exclude metadata when specified", function()
                local results = data_reader.with_dataflow(test_dataflow_id)
                    :fetch_options({ metadata = false })
                    :all()

                expect(#results).to_be_greater_than(0)
                for _, item in ipairs(results) do
                    expect(item.metadata).to_be_nil()
                end
            end)

            it("should fetch only headers when content and metadata excluded", function()
                local results = data_reader.with_dataflow(test_dataflow_id)
                    :fetch_options({ content = false, metadata = false })
                    :all()

                expect(#results).to_be_greater_than(0)
                for _, item in ipairs(results) do
                    expect(item.data_id).not_to_be_nil()
                    expect(item.type).not_to_be_nil()
                    expect(item.key).not_to_be_nil()
                    expect(item.content).to_be_nil()
                    expect(item.metadata).to_be_nil()
                end
            end)
        end)

        describe("One Result", function()
            it("should fetch a single result", function()
                local item = data_reader.with_dataflow(test_dataflow_id)
                    :with_data_keys("global_settings")
                    :one()

                expect(item).not_to_be_nil()
                expect(item.key).to_equal("global_settings")
                expect(item.content).not_to_be_nil()
                expect(item.metadata).to_be_type("table")
            end)

            it("should return nil for non-matching query", function()
                local item = data_reader.with_dataflow(test_dataflow_id)
                    :with_data_keys("non_existent_key")
                    :one()

                expect(item).to_be_nil()
            end)

            it("should respect fetch options", function()
                local item = data_reader.with_dataflow(test_dataflow_id)
                    :with_data_keys("global_settings")
                    :fetch_options({ content = false })
                    :one()

                expect(item).not_to_be_nil()
                expect(item.key).to_equal("global_settings")
                expect(item.content).to_be_nil()
            end)
        end)

        describe("Reference Resolution", function()
            it("should fetch reference with referenced data", function()
                local item = data_reader.with_dataflow(test_dataflow_id)
                    :with_data(test_reference_data_id)
                    :one()

                expect(item).not_to_be_nil()
                expect(item.content_type).to_equal("dataflow/reference")
                expect(item.key).to_equal(test_target_data_id)

                -- Check reference fields
                expect(item.ref_content).to_equal("Target content value")
                expect(item.ref_content_type).to_equal("text/plain")
                expect(item.ref_type).to_equal("target_type")
                expect(item.ref_discriminator).to_equal("test_target")
                expect(item.ref_key).to_equal("target_key")

                -- Check reference metadata is parsed
                expect(item.ref_metadata).to_be_type("table")
                expect(item.ref_metadata.target_meta).to_equal("Target metadata value")
            end)

            it("should disable reference resolution when specified", function()
                local item = data_reader.with_dataflow(test_dataflow_id)
                    :with_data(test_reference_data_id)
                    :fetch_options({ resolve_references = false })
                    :one()

                expect(item).not_to_be_nil()
                expect(item.content_type).to_equal("dataflow/reference")
                expect(item.key).to_equal(test_target_data_id)

                -- Ref fields should not be present
                expect(item.ref_content).to_be_nil()
                expect(item.ref_content_type).to_be_nil()
                expect(item.ref_type).to_be_nil()
                expect(item.ref_discriminator).to_be_nil()
                expect(item.ref_key).to_be_nil()
                expect(item.ref_metadata).to_be_nil()
            end)

            it("should handle missing reference target gracefully", function()
                -- Create a reference to non-existent data
                local db = get_test_db()
                local non_existent_id = uuid.v7()
                local hanging_ref_id = uuid.v7()

                -- Insert dangling reference
                local now_ts = time.now():format(time.RFC3339)

                local dangling_insert = sql.builder.insert("dataflow_data")
                    :set_map({
                        data_id = hanging_ref_id,
                        dataflow_id = test_dataflow_id,
                        node_id = sql.as.null(),
                        type = "reference",
                        discriminator = "default",
                        key = non_existent_id,
                        content = "Reference to nothing",
                        content_type = "dataflow/reference",
                        metadata = "{}",
                        created_at = now_ts
                    })

                local dangling_exec = dangling_insert:run_with(db)
                local result, err = dangling_exec:exec()
                db:release()

                if err then
                    error("Failed to create test dangling reference: " .. err)
                end

                -- Test fetching the dangling reference
                local item = data_reader.with_dataflow(test_dataflow_id)
                    :with_data(hanging_ref_id)
                    :one()

                expect(item).not_to_be_nil()
                expect(item.content_type).to_equal("dataflow/reference")
                expect(item.key).to_equal(non_existent_id)

                -- All ref fields should be nil for a dangling reference
                expect(item.ref_content).to_be_nil()
                expect(item.ref_content_type).to_be_nil()
                expect(item.ref_type).to_be_nil()
                expect(item.ref_discriminator).to_be_nil()
                expect(item.ref_key).to_be_nil()
                expect(item.ref_metadata).to_be_nil()
            end)
        end)
    end)
end

return test.run_cases(define_tests)
