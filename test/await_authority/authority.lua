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
    local db, db_err = sql.get("app:db")
    if db_err then return nil, db_err end
    local rows, err = db:query([[SELECT credential_id, token FROM await_authority_subjects
        WHERE subject_id = $1 AND ($2 = '' OR credential_id = $2)]], {subject_id, credential_id or ""})
    db:release()
    if err then return nil, err end
    if #rows ~= 1 or type(rows[1].token) ~= "string" then
        return nil, errors.new({kind = errors.NOT_FOUND, message = "execution credential is unavailable", retryable = false})
    end
    return rows[1], nil
end

function M.capture(actor, scope)
    local stored, stored_err = credential(actor:id(), nil)
    if stored_err then return nil, stored_err end
    local tokens, tokens_err = security.token_store("app.await_authority:tokens")
    if tokens_err then return nil, tokens_err end
    local live_actor, live_scope, err = tokens:validate(stored.token)
    if err then return nil, err end
    if live_actor:id() ~= actor:id() then
        return nil, errors.new({kind = errors.INVALID, message = "execution credential belongs to a different subject"})
    end
    return {credential_id = stored.credential_id}, nil
end

function M.resolve(args)
    local saved = args.authority_context
    local stored, stored_err = credential(args.subject_id, saved and saved.credential_id or nil)
    if stored_err then return nil, nil, stored_err end
    local tokens, tokens_err = security.token_store("app.await_authority:tokens")
    if tokens_err then return nil, nil, tokens_err end
    return tokens:validate(stored.token)
end

return M
