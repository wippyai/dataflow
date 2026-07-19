local test = require("test")
local sql = require("sql")
local uuid = require("uuid")
local time = require("time")
local wake_repo = require("wake_repo")

local function run_tests()
    test.describe("Dataflow wake repository", function()
        test.it("keeps independently addressable wake triggers per dataflow", function()
            local db = test.not_nil(select(1, sql.get("app:db"))) :: any
            local id = uuid.v7()
            local now = time.now():format(time.RFC3339NANO)
            local db_type, type_err = db:type()
            test.is_nil(type_err)
            test.is_true(db_type == "sqlite" or db_type == "postgres")
            local _, insert_err = sql.builder.insert("dataflows"):set_map({
                dataflow_id = id,
                actor_id = "wake-test",
                type = "test",
                status = "running",
                metadata = "{}",
                created_at = now,
                updated_at = now,
            }):run_with(db):exec()
            test.is_nil(insert_err)
            db:release()

            local insert_db = test.not_nil(select(1, sql.get("app:db"))) :: any
            for _, wake in ipairs({
                { key = "yield:a", at = "2026-07-12T20:02:00Z" },
                { key = "yield:b", at = "2026-07-12T20:01:00Z" },
                { key = "commit:c", at = "2026-07-12T20:03:00Z" },
            }) do
                test.is_nil(select(2, sql.builder.insert("dataflow_wakes"):set_map({
                    dataflow_id = id,
                    wake_key = wake.key,
                    wake_at = wake.at,
                }):run_with(insert_db):exec()))
            end
            insert_db:release()
            local row = test.not_nil(select(1, wake_repo.next())) :: any
            test.eq(row.dataflow_id, id)
            test.eq(row.wake_key, "yield:b")
            test.eq(tostring(row.wake_at), "2026-07-12T20:01:00Z")

            test.is_true(select(1, wake_repo.remove(id, "yield:b")))
            row = test.not_nil(select(1, wake_repo.next())) :: any
            test.eq(row.wake_key, "yield:a")
            test.is_true(select(1, wake_repo.clear(id)))
            test.is_nil(select(1, wake_repo.next()))
        end)

        -- due() is reached only by the wake service, and wake_process_test stubs the repo out, so
        -- until now no test ran this query against a database at all. That gap is why the query
        -- shipped with sqlite-only `?` placeholders and crashlooped the wake service on postgres
        -- with `syntax error at or near "ORDER"`. The fixture writes go through the builder so the
        -- only thing this exercises on either dialect is due() itself.
        test.it("returns wakes that are due, ordered and limited, on the live dialect", function()
            local db = test.not_nil(select(1, sql.get("app:db"))) :: any
            local id = uuid.v7()
            local now = time.now():format(time.RFC3339NANO)

            -- due() is deliberately global: it returns every wake that is due, for any dataflow.
            -- Start from an empty table so the counts below mean what they say.
            test.is_nil(select(2, sql.builder.delete("dataflow_wakes"):run_with(db):exec()))

            test.is_nil(select(2, sql.builder.insert("dataflows"):set_map({
                dataflow_id = id, actor_id = "due-test", type = "test",
                status = "running", metadata = "{}", created_at = now, updated_at = now,
            }):run_with(db):exec()))

            for _, w in ipairs({
                { key = "due:late",  at = "2020-01-01T00:02:00Z" },
                { key = "due:early", at = "2020-01-01T00:01:00Z" },
                { key = "not:due",   at = "2999-01-01T00:00:00Z" },
            }) do
                test.is_nil(select(2, sql.builder.insert("dataflow_wakes"):set_map({
                    dataflow_id = id, wake_key = w.key, wake_at = w.at,
                }):run_with(db):exec()))
            end
            db:release()

            local rows = test.not_nil(select(1, wake_repo.due("2020-06-01T00:00:00Z", 10))) :: any
            test.eq(#rows, 2)
            test.eq(rows[1].wake_key, "due:early")
            test.eq(rows[2].wake_key, "due:late")

            local limited = test.not_nil(select(1, wake_repo.due("2020-06-01T00:00:00Z", 1))) :: any
            test.eq(#limited, 1)
            test.eq(limited[1].wake_key, "due:early")

            test.is_true(select(1, wake_repo.clear(id)))
        end)
    end)
end

return { run_tests = test.run_cases(run_tests) }
