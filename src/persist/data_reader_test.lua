local test = require("test")
local uuid = require("uuid")
local json = require("json")
local time = require("time")
local sql = require("sql")
local data_reader = require("data_reader")

local function define_tests()
    local test_dataflow_id
    local test_node_id_1
    local test_node_id_2
    local test_data_ids = {}
    local test_reference_data_id
    local test_target_data_id
    describe("Data Reader", function()
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
                    type = "data_reader_test",
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
            local _, node_err = node2_exec:exec()

            if node_err then
                tx:rollback()
                db:release()
                error("Failed to create test nodes: " .. node_err)
            end

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
            local _, target_err = target_exec:exec()

            if target_err then
                tx:rollback()
                db:release()
                error("Failed to create target data: " .. target_err)
            end

            local data_items = {
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
                {
                    data_id = uuid.v7(),
                    node_id = nil,
                    type = "reference",
                    discriminator = "default",
                    key = test_target_data_id,
                    content = "Reference to target",
                    content_type = "dataflow/reference",
                    metadata = json.encode({ ref_created_at = now_ts })
                }
            }

            for _, item in ipairs(data_items) do
                test_data_ids[item.key] = item.data_id

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
                local _, data_err = data_exec:exec()

                if data_err then
                    tx:rollback()
                    db:release()
                    error("Failed to create test data: " .. data_err)
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

            local _, del_err = tx:execute("DELETE FROM dataflows WHERE dataflow_id = ?", { test_dataflow_id })
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
                local reader = data_reader.with_dataflow(test_dataflow_id)
                test.not_nil(reader)
            end)

            it("should error when initialized without a dataflow ID", function()
                local success1 = pcall(function() data_reader.with_dataflow(nil) end)
                test.is_false(success1)

                local success2 = pcall(function() data_reader.with_dataflow("") end)
                test.is_false(success2)
            end)

            it("should return all data for a dataflow", function()
                local reader = data_reader.with_dataflow(test_dataflow_id) :: any
                local results = reader:all()
                test.eq(#results, 8)
                test.is_table(results[1].metadata)
            end)

            it("should count all data for a dataflow", function()
                local reader = data_reader.with_dataflow(test_dataflow_id) :: any
                local count = reader:count()
                test.eq(count, 8)
            end)

            it("should check existence of data", function()
                local reader = data_reader.with_dataflow(test_dataflow_id) :: any
                local exists = reader:exists()
                test.is_true(exists)

                local reader2 = data_reader.with_dataflow(uuid.v7()) :: any
                local non_exists = reader2:exists()
                test.is_false(non_exists)
            end)
        end)

        describe("Filtering", function()
            it("should filter by node ID", function()
                local results = data_reader.with_dataflow(test_dataflow_id)
                    :with_nodes(test_node_id_1)
                    :all()

                test.eq(#results, 2)
                for _, item in ipairs(results) do
                    test.eq(item.node_id, test_node_id_1)
                end
            end)

            it("should filter by multiple node IDs", function()
                local results = data_reader.with_dataflow(test_dataflow_id)
                    :with_nodes(test_node_id_1, test_node_id_2)
                    :all()

                test.eq(#results, 4)
                for _, item in ipairs(results) do
                    test.is_true(item.node_id == test_node_id_1 or item.node_id == test_node_id_2)
                end
            end)

            it("should filter by data type", function()
                local results = data_reader.with_dataflow(test_dataflow_id)
                    :with_data_types("config")
                    :all()

                test.eq(#results, 2)
                for _, item in ipairs(results) do
                    test.eq(item.type, "config")
                end
            end)

            it("should filter by multiple data types", function()
                local results = data_reader.with_dataflow(test_dataflow_id)
                    :with_data_types("input", "output")
                    :all()

                test.eq(#results, 4)
                for _, item in ipairs(results) do
                    test.is_true(item.type == "input" or item.type == "output")
                end
            end)

            it("should filter by data key", function()
                local results = data_reader.with_dataflow(test_dataflow_id)
                    :with_data_keys("global_settings")
                    :all()

                test.eq(#results, 1)
                test.eq(results[1].key, "global_settings")
            end)

            it("should filter by multiple data keys", function()
                local results = data_reader.with_dataflow(test_dataflow_id)
                    :with_data_keys("global_settings", "user_preferences")
                    :all()

                test.eq(#results, 2)
                test.is_true(results[1].key == "global_settings" or results[1].key == "user_preferences")
                test.is_true(results[2].key == "global_settings" or results[2].key == "user_preferences")
            end)

            it("should filter by discriminator", function()
                local results = data_reader.with_dataflow(test_dataflow_id)
                    :with_data_discriminators("user")
                    :all()

                test.eq(#results, 1)
                test.eq(results[1].discriminator, "user")
            end)

            it("should combine multiple filters", function()
                local results = data_reader.with_dataflow(test_dataflow_id)
                    :with_nodes(test_node_id_1, test_node_id_2)
                    :with_data_types("input")
                    :all()

                test.eq(#results, 2)
                for _, item in ipairs(results) do
                    test.eq(item.type, "input")
                    test.is_true(item.node_id == test_node_id_1 or item.node_id == test_node_id_2)
                end
            end)
        end)

        describe("Fetch Options", function()
            it("should exclude content when specified", function()
                local results = data_reader.with_dataflow(test_dataflow_id)
                    :fetch_options({ content = false })
                    :all()

                test.gt(#results, 0)
                for _, item in ipairs(results) do
                    test.is_nil(item.content)
                    test.is_nil(item.content_type)
                end
            end)

            it("should exclude metadata when specified", function()
                local results = data_reader.with_dataflow(test_dataflow_id)
                    :fetch_options({ metadata = false })
                    :all()

                test.gt(#results, 0)
                for _, item in ipairs(results) do
                    test.is_nil(item.metadata)
                end
            end)

            it("should fetch only headers when content and metadata excluded", function()
                local results = data_reader.with_dataflow(test_dataflow_id)
                    :fetch_options({ content = false, metadata = false })
                    :all()

                test.gt(#results, 0)
                for _, item in ipairs(results) do
                    test.not_nil(item.data_id)
                    test.not_nil(item.type)
                    test.not_nil(item.key)
                    test.is_nil(item.content)
                    test.is_nil(item.metadata)
                end
            end)
        end)

        describe("One Result", function()
            it("should fetch a single result", function()
                local item = data_reader.with_dataflow(test_dataflow_id)
                    :with_data_keys("global_settings")
                    :one()

                test.not_nil(item)
                test.eq(item.key, "global_settings")
                test.not_nil(item.content)
                test.is_table(item.metadata)
            end)

            it("should return nil for non-matching query", function()
                local item = data_reader.with_dataflow(test_dataflow_id)
                    :with_data_keys("non_existent_key")
                    :one()

                test.is_nil(item)
            end)

            it("should respect fetch options", function()
                local item = data_reader.with_dataflow(test_dataflow_id)
                    :with_data_keys("global_settings")
                    :fetch_options({ content = false })
                    :one()

                test.not_nil(item)
                test.eq(item.key, "global_settings")
                test.is_nil(item.content)
            end)
        end)

        describe("Reference Resolution", function()
            it("should fetch reference with referenced data", function()
                local item = data_reader.with_dataflow(test_dataflow_id)
                    :with_data(test_reference_data_id)
                    :one()

                test.not_nil(item)
                test.eq(item.content_type, "dataflow/reference")
                test.eq(item.key, test_target_data_id)

                test.eq(item.ref_content, "Target content value")
                test.eq(item.ref_content_type, "text/plain")
                test.eq(item.ref_type, "target_type")
                test.eq(item.ref_discriminator, "test_target")
                test.eq(item.ref_key, "target_key")

                test.is_table(item.ref_metadata)
                test.eq(item.ref_metadata.target_meta, "Target metadata value")
            end)

            it("should disable reference resolution when specified", function()
                local item = data_reader.with_dataflow(test_dataflow_id)
                    :with_data(test_reference_data_id)
                    :fetch_options({ resolve_references = false })
                    :one()

                test.not_nil(item)
                test.eq(item.content_type, "dataflow/reference")
                test.eq(item.key, test_target_data_id)

                test.is_nil(item.ref_content)
                test.is_nil(item.ref_content_type)
                test.is_nil(item.ref_type)
                test.is_nil(item.ref_discriminator)
                test.is_nil(item.ref_key)
                test.is_nil(item.ref_metadata)
            end)

            it("should handle missing reference target gracefully", function()
                local db = get_test_db()
                local non_existent_id = uuid.v7()
                local hanging_ref_id = uuid.v7()

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
                local _, err = dangling_exec:exec()
                db:release()

                if err then
                    error("Failed to create test dangling reference: " .. err)
                end

                local item = data_reader.with_dataflow(test_dataflow_id)
                    :with_data(hanging_ref_id)
                    :one()

                test.not_nil(item)
                test.eq(item.content_type, "dataflow/reference")
                test.eq(item.key, non_existent_id)

                test.is_nil(item.ref_content)
                test.is_nil(item.ref_content_type)
                test.is_nil(item.ref_type)
                test.is_nil(item.ref_discriminator)
                test.is_nil(item.ref_key)
                test.is_nil(item.ref_metadata)
            end)
        end)
    end)
end

return test.run_cases(define_tests)
