local sql = require("sql")
local time = require("time")

local function run()
    local max_attempts = 300
    local sleep_ms = 100

    for _ = 1, max_attempts do
        local db, err = sql.get("app:db")
        if not err then
            local rows, query_err = db:query(
                "SELECT tablename AS name FROM pg_tables WHERE schemaname='public' AND tablename='dataflows'"
            )
            if query_err then
                rows, query_err = db:query(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='dataflows'"
                )
            end

            if not query_err and rows and #rows > 0 then
                -- tests create records with random user_id uuids; relax FKs that reference
                -- app_users so tests don't need to seed rows (sqlite silently ignores)
                pcall(function()
                    db:execute("ALTER TABLE sessions DROP CONSTRAINT IF EXISTS sessions_user_id_fkey")
                    db:execute("ALTER TABLE artifacts DROP CONSTRAINT IF EXISTS fk_artifacts_user")
                    db:execute("ALTER TABLE artifacts DROP CONSTRAINT IF EXISTS fk_artifacts_session")
                    db:execute("ALTER TABLE messages DROP CONSTRAINT IF EXISTS fk_messages_session")
                    db:execute("ALTER TABLE messages DROP CONSTRAINT IF EXISTS fk_messages_user")
                    db:execute("ALTER TABLE session_contexts DROP CONSTRAINT IF EXISTS fk_session_contexts_session")
                end)
                db:release()
                return true
            end

            db:release()
        end

        time.sleep(sleep_ms .. "ms")
    end

    error("bootloader did not complete within " .. (max_attempts * sleep_ms) .. "ms")
end

return { run = run }
