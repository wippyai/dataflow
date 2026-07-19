local sql = require("sql")
local time = require("time")
local encryption_key_bootloader = require("encryption_key_bootloader")
local migration_bootloader = require("migration_bootloader")

local bootloaders_started = false

-- wippy/session owns foreign keys into the application's user/context tables.
-- SQLite permits those referenced tables to be absent during CREATE TABLE,
-- while PostgreSQL correctly rejects the migration. Dataflow's isolated test
-- app does not install an identity module, so provide only the two referenced
-- keys before running dependency migrations; the session constraints are
-- removed below once all migrations have completed.
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

        local _, contexts_err = db:execute([[
            CREATE TABLE IF NOT EXISTS contexts (
                context_id TEXT PRIMARY KEY
            )
        ]])
        if contexts_err then
            db:release()
            error("Failed to create PostgreSQL contexts test stub: " .. tostring(contexts_err))
        end
    end

    db:release()
end

local function call_bootloader(module, options)
    if type(module) == "function" then
        return module(options)
    end
    if type(module) == "table" and type(module.run) == "function" then
        return module.run(options)
    end
    error("bootloader import is not callable")
end

local function run_setup_bootloaders()
    if bootloaders_started then
        return
    end
    bootloaders_started = true

    prepare_postgres_session_dependencies()

    local key_result = call_bootloader(encryption_key_bootloader, {})
    if type(key_result) ~= "table" or key_result.status == "error" then
        error("encryption key bootloader failed: " .. tostring(key_result and key_result.message or key_result))
    end

    local migration_result = call_bootloader(migration_bootloader, {})
    if type(migration_result) ~= "table" or migration_result.status == "error" then
        error("migration bootloader failed: " .. tostring(migration_result and migration_result.message or migration_result))
    end
end

local function run()
    run_setup_bootloaders()

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
