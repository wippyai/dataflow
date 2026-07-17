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
            local _, insert_err = db:execute([[
                INSERT INTO dataflows(dataflow_id, actor_id, type, status, metadata, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ]], { id, "wake-test", "test", "running", "{}", now, now })
            test.is_nil(insert_err)
            db:release()

            local insert_db = test.not_nil(select(1, sql.get("app:db"))) :: any
            test.is_nil(select(2, insert_db:execute([[
                INSERT INTO dataflow_wakes(dataflow_id, wake_key, wake_at) VALUES
                    (?, ?, ?), (?, ?, ?), (?, ?, ?)
            ]], {
                id, "yield:a", "2026-07-12T20:02:00Z",
                id, "yield:b", "2026-07-12T20:01:00Z",
                id, "commit:c", "2026-07-12T20:03:00Z",
            })))
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
