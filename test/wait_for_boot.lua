local sql = require("sql")
local time = require("time")
local env = require("env")

-- wippy/session owns foreign keys into the application's user/context tables.
-- SQLite permits those referenced tables to be absent during CREATE TABLE,
-- while PostgreSQL correctly rejects the migration. Dataflow's isolated test
-- app does not install an identity module, so provide only its referenced user
-- key before running dependency migrations; session creates its own contexts
-- table. The external constraints are removed below once migrations complete.
local function prepare_postgres_session_dependencies()
    local db, db_err = sql.get("app:db")
    if db_err then error("Failed to acquire setup database: " .. tostring(db_err)) end

    local db_type, type_err = db:type()
    if type_err then
        db:release()
        error("Failed to identify setup database: " .. tostring(type_err))
    end

    if db_type == "postgres" then
        local _, users_err = db:execute([[
            CREATE TABLE IF NOT EXISTS app_users (
                user_id TEXT PRIMARY KEY
            )
        ]])
        if users_err then
            db:release()
            error("Failed to create PostgreSQL app_users test stub: " .. tostring(users_err))
        end
    end

    db:release()
    return {
        status = "success",
        message = "PostgreSQL session dependency stubs are ready",
    }
end

local function run()
    local max_attempts = 300
    local sleep_ms = 100

    for _ = 1, max_attempts do
        -- The canonical hook sets the epoch only after migrations complete.
        -- Table existence alone does not prove indexes and constraints are ready.
        local epoch = env.get("userspace.dataflow.env:runtime_epoch")
        if type(epoch) ~= "string" or epoch == "" then
            time.sleep(sleep_ms .. "ms")
            goto continue
        end
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
        ::continue::
    end

    error("bootloader did not complete within " .. (max_attempts * sleep_ms) .. "ms")
end

return {
    prepare_postgres_session_dependencies = prepare_postgres_session_dependencies,
    run = run,
}
