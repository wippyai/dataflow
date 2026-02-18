local test = require("test")
local uuid = require("uuid")
local json = require("json")
local time = require("time")
local sql = require("sql")
local commit_repo = require("commit_repo")

local function define_tests()
    describe("Commit Repository", function()
        local test_ctx = {
            db = nil,
            tx = nil,
            dataflow_id = nil,
            cleanup_ids = {}
        }

        before_all(function()
            local db, err_db = sql.get("app:db")
            if err_db then error("Failed to connect to database: " .. err_db) end

            local tx, err_tx = db:begin()
            if err_tx then
                db:release()
                error("Failed to begin transaction: " .. err_tx)
            end

            local dataflow_id = uuid.v7()
            test_ctx.dataflow_id = dataflow_id

            local now_ts = time.now():format(time.RFC3339)

            local insert_query = sql.builder.insert("dataflows")
                :set_map({
                    dataflow_id = dataflow_id,
                    actor_id = "test-user",
                    type = "commit_test_type",
                    status = "active",
                    metadata = "{}",
                    created_at = now_ts,
                    updated_at = now_ts
                })

            local insert_exec = insert_query:run_with(tx)
            local _, err_insert = insert_exec:exec()

            if err_insert then
                tx:rollback()
                db:release()
                error("Failed to create test dataflow: " .. err_insert)
            end

            local _, err_commit = tx:commit()
            if err_commit then
                tx:rollback()
                db:release()
                error("Failed to commit transaction: " .. err_commit)
            end

            db:release()

            table.insert(test_ctx.cleanup_ids, dataflow_id)
        end)

        after_all(function()
            if #test_ctx.cleanup_ids == 0 then return end

            local db, err_db = sql.get("app:db")
            if err_db then return end

            local tx, err_tx = db:begin()
            if err_tx then
                db:release()
                return
            end

            for _, id in ipairs(test_ctx.cleanup_ids) do
                local delete_query = sql.builder.delete("dataflows"):where("dataflow_id = ?", id)
                local delete_exec = delete_query:run_with(tx)
                delete_exec:exec()
            end

            tx:commit()
            db:release()
        end)

        describe("Create operations", function()
            it("should create a commit with minimal fields", function()
                local commit_id = uuid.v7()
                local payload = { command = "test_command" }

                local commit, err = commit_repo.create(commit_id, test_ctx.dataflow_id, payload)

                test.is_nil(err)
                test.not_nil(commit)
                test.eq(commit.commit_id, commit_id)
                test.eq(commit.dataflow_id, test_ctx.dataflow_id)
                test.eq(commit.payload.command, "test_command")
                test.not_nil(commit.created_at)

                -- Verify in database
                local db, _ = sql.get("app:db")
                assert(db)
                local query = sql.builder.select("*"):from("dataflow_commits"):where("commit_id = ?", commit_id)
                local exec = query:run_with(db :: sql.DB)
                local rows, _ = exec:query()
                db:release()

                test.eq(#rows, 1)

                -- Check dataflow last_commit_id was updated
                db, _ = sql.get("app:db")
                assert(db)
                local dataflow_query = sql.builder.select("last_commit_id")
                    :from("dataflows")
                    :where("dataflow_id = ?", test_ctx.dataflow_id)
                local dataflow_exec = dataflow_query:run_with(db :: sql.DB)
                local dataflow_rows, _ = dataflow_exec:query()
                db:release()

                test.eq(dataflow_rows[1].last_commit_id, commit_id)
            end)

            it("should create a commit with metadata", function()
                local commit_id = uuid.v7()
                local payload = { command = "metadata_command" }
                local metadata = {
                    source = "test",
                    node_id = uuid.v7()
                }

                local commit, err = commit_repo.create(
                    commit_id,
                    test_ctx.dataflow_id,
                    payload,
                    metadata
                )

                test.is_nil(err)
                test.not_nil(commit)
                local meta = commit.metadata :: any
                test.eq(meta.source, "test")
                test.not_nil(meta.node_id)

                -- Verify metadata in database
                local db, _ = sql.get("app:db")
                assert(db)
                local metadata_query = sql.builder.select("metadata")
                    :from("dataflow_commits")
                    :where("commit_id = ?", commit_id)
                local metadata_exec = metadata_query:run_with(db :: sql.DB)
                local rows, _ = metadata_exec:query()
                db:release()

                local parsed_metadata = json.decode(rows[1].metadata :: string)
                test.eq(parsed_metadata.source, "test")
                test.eq(parsed_metadata.node_id, metadata.node_id)
            end)

            it("should create a commit with complex payload", function()
                local commit_id = uuid.v7()
                local payload = {
                    commands = {
                        { type = "CREATE_NODE", payload = { node_type = "test" } },
                        { type = "UPDATE_NODE", payload = { status = "running" } }
                    },
                    count = 2
                }

                local commit, err = commit_repo.create(
                    commit_id,
                    test_ctx.dataflow_id,
                    payload
                )

                test.is_nil(err)
                test.not_nil(commit)
                test.eq(commit.commit_id, commit_id)
                test.eq(#commit.payload.commands, 2)
                test.eq(commit.payload.count, 2)
            end)

            it("should fail to create a commit without required fields", function()
                local _, err1 = commit_repo.create(
                    nil,
                    test_ctx.dataflow_id,
                    { test = true }
                )
                test.not_nil(string.find(err1 :: string, "Commit ID is required", 1, true))

                local _, err2 = commit_repo.create(
                    uuid.v7(),
                    nil,
                    { test = true }
                )
                test.not_nil(string.find(err2 :: string, "Dataflow ID is required", 1, true))

                local _, err3 = commit_repo.create(
                    uuid.v7(),
                    test_ctx.dataflow_id,
                    nil
                )
                test.not_nil(string.find(err3 :: string, "Payload is required", 1, true))
            end)
        end)

        describe("Read operations", function()
            local test_commit_id
            before_all(function()
                test_commit_id = uuid.v7()
                local payload = {
                    test_value = "retrievable",
                    nested = { key = "value" }
                }
                local metadata = { purpose = "retrieval_test" }

                local _, err = commit_repo.create(
                    test_commit_id,
                    test_ctx.dataflow_id,
                    payload,
                    metadata
                )

                if err then
                    error("Failed to create test commit: " .. err)
                end
            end)

            it("should get a commit by ID", function()
                local commit, err = commit_repo.get(test_commit_id)

                test.is_nil(err)
                test.not_nil(commit)
                test.eq(commit.commit_id, test_commit_id)
                test.eq(commit.payload.test_value, "retrievable")
                test.eq(commit.payload.nested.key, "value")
                test.eq(commit.metadata.purpose, "retrieval_test")
            end)

            it("should return error for non-existent commit ID", function()
                local _, err = commit_repo.get(uuid.v7())
                test.not_nil(string.find(err :: string, "Commit not found", 1, true))
            end)

            it("should list commits for a dataflow", function()
                local commit_ids = {}
                for i = 1, 3 do
                    local cid = uuid.v7()
                    table.insert(commit_ids, cid)
                    commit_repo.create(
                        cid,
                        test_ctx.dataflow_id,
                        { index = i, test_list = true }
                    )
                end

                local commits, err = commit_repo.list_by_dataflow(test_ctx.dataflow_id)

                test.is_nil(err)
                test.not_nil(commits)
                assert(commits)
                test.is_true(#commits >= 4)

                for i = 2, #commits do
                    test.is_true(commits[i].commit_id > commits[i-1].commit_id)
                end

                local limited_commits, err_limit = commit_repo.list_by_dataflow(
                    test_ctx.dataflow_id,
                    { limit = 2 }
                )
                test.is_nil(err_limit)
                test.eq(#limited_commits, 2)
            end)

            it("should list all commits with list_after(nil)", function()
                local all_commits, err = commit_repo.list_after(test_ctx.dataflow_id, nil)

                test.is_nil(err)
                test.not_nil(all_commits)

                local direct_commits, _ = commit_repo.list_by_dataflow(test_ctx.dataflow_id)
                test.eq(#all_commits, #direct_commits)
            end)

            it("should list commits after a specific ID", function()
                local all_commits, _ = commit_repo.list_by_dataflow(test_ctx.dataflow_id)
                assert(all_commits)

                local mid_index = math.floor(#all_commits / 2)
                local mid_commit_id = all_commits[mid_index].commit_id

                local later_commits, err = commit_repo.list_after(
                    test_ctx.dataflow_id,
                    mid_commit_id
                )

                test.is_nil(err)
                test.not_nil(later_commits)
                test.eq(#later_commits, #all_commits - mid_index)

                for _, commit in ipairs(later_commits) do
                    test.is_true(commit.commit_id > mid_commit_id)
                end
            end)
        end)
    end)
end

return test.run_cases(define_tests)
