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

            test.is_true(select(1, wake_repo.register(id, "yield:a", "2026-07-12T20:02:00Z")))
            test.is_true(select(1, wake_repo.register(id, "yield:b", "2026-07-12T20:01:00Z")))
            test.is_true(select(1, wake_repo.register(id, "commit:c", "2026-07-12T20:03:00Z")))
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
    end)
end

return { run_tests = run_tests }
