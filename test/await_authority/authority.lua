local sql = require("sql")
local security = require("security")
local M = {}

function M.create_schema(db)
    return db:execute([[CREATE TABLE IF NOT EXISTS await_authority_subjects (
        subject_id TEXT PRIMARY KEY, groups_json TEXT NOT NULL, active INTEGER NOT NULL,
        effects INTEGER NOT NULL DEFAULT 0, credential_id TEXT UNIQUE, token TEXT
    )]])
end

local function credential(subject_id, credential_id)
    local db = assert(sql.get("app:db"))
    local rows, err = db:query([[SELECT credential_id, token FROM await_authority_subjects
        WHERE subject_id = $1 AND ($2 = '' OR credential_id = $2)]], {subject_id, credential_id or ""})
    db:release()
    if err then error(err) end
    if #rows ~= 1 or type(rows[1].token) ~= "string" then error("execution credential is unavailable") end
    return rows[1]
end

function M.capture(actor, scope)
    local stored = credential(actor:id(), nil)
    local tokens = assert(security.token_store("app.await_authority:tokens"))
    local live_actor, live_scope, err = tokens:validate(stored.token)
    if err then return nil, err end
    if live_actor:id() ~= actor:id() then return nil, "execution credential belongs to a different subject" end
    return {credential_id = stored.credential_id}, nil
end

function M.resolve(args)
    local saved = args.authority_context
    local stored = credential(args.subject_id, saved and saved.credential_id or nil)
    local tokens = assert(security.token_store("app.await_authority:tokens"))
    return tokens:validate(stored.token)
end

return M
