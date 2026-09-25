local sql = require("sql")
local json = require("json")
local consts = require("dataflow_consts")

local activation_repo: any = {}

local function typed(kind, message)
    return errors.new({ kind = kind, message = tostring(message) })
end

local TERMINAL_STATUS = {
    [consts.STATUS.COMPLETED_SUCCESS] = true,
    [consts.STATUS.COMPLETED_FAILURE] = true,
    [consts.STATUS.CANCELLED] = true,
    [consts.STATUS.TERMINATED] = true,
}

local TERMINAL_VALUES = {
    consts.STATUS.COMPLETED_SUCCESS,
    consts.STATUS.COMPLETED_FAILURE,
    consts.STATUS.CANCELLED,
    consts.STATUS.TERMINATED,
}

local function rebind(query, db_type)
    if db_type ~= sql.type.POSTGRES and db_type ~= "postgres" then return query end
    local index = 0
    return (query:gsub("%?", function()
        index = index + 1
        return "$" .. index
    end))
end

local function tx_query(tx, query, params)
    local db_type, type_err = tx:db_type()
    if type_err then return nil, type_err end
    return tx:query(rebind(query, db_type), params or {})
end

local function tx_execute(tx, query, params)
    local db_type, type_err = tx:db_type()
    if type_err then return nil, type_err end
    return tx:execute(rebind(query, db_type), params or {})
end

local function db_query(db, query, params)
    local db_type, type_err = db:type()
    if type_err then return nil, type_err end
    return db:query(rebind(query, db_type), params or {})
end

local function validate_id(dataflow_id)
    if type(dataflow_id) ~= "string" or dataflow_id == "" then
        return nil, typed(errors.INVALID, "dataflow_id is required")
    end
    return true, nil
end

local function validate_timestamp(value, field)
    if type(value) ~= "string" or value == "" then
        return nil, typed(errors.INVALID, field .. " is required")
    end
    return true, nil
end

local function validate_json_value(value: any, seen: any, path: string)
    local kind = type(value)
    if kind == "nil" or kind == "string" or kind == "boolean" then return true, nil end
    if kind == "number" then
        if value ~= value or value == math.huge or value == -math.huge then
            return nil, typed(errors.INVALID, path .. " contains a non-finite number")
        end
        return true, nil
    end
    if kind ~= "table" then return nil, typed(errors.INVALID, path .. " contains unsupported " .. kind) end
    if getmetatable(value) ~= nil then return nil, typed(errors.INVALID, path .. " must not have a metatable") end
    if seen[value] then return nil, typed(errors.INVALID, path .. " contains a cycle") end
    seen[value] = true

    local key_kind = nil
    local count = 0
    local max_index = 0
    for key, item in pairs(value) do
        local current_kind = type(key)
        if current_kind == "number" then
            local array_index = tonumber(key) or 0
            if array_index < 1 or array_index % 1 ~= 0 then
                seen[value] = nil
                return nil, typed(errors.INVALID, path .. " contains an invalid array index")
            end
            max_index = math.max(max_index, array_index)
            current_kind = "array"
        elseif current_kind == "string" then
            current_kind = "object"
        else
            seen[value] = nil
            return nil, typed(errors.INVALID, path .. " contains an unsupported key")
        end
        if key_kind and key_kind ~= current_kind then
            seen[value] = nil
            return nil, typed(errors.INVALID, path .. " mixes object and array keys")
        end
        key_kind = current_kind
        count = count + 1
        local ok, err = validate_json_value(item, seen, path .. "." .. tostring(key))
        if not ok then
            seen[value] = nil
            return nil, err
        end
    end
    seen[value] = nil
    if key_kind == "array" and max_index ~= count then
        return nil, typed(errors.INVALID, path .. " contains a sparse array")
    end
    return true, nil
end

local function encode_launch_args(launch_args: any)
    if launch_args == nil then return nil, nil end
    if type(launch_args) ~= "table" or getmetatable(launch_args) ~= nil then
        return nil, typed(errors.INVALID, "launch_args must be a plain object")
    end
    for key in pairs(launch_args) do
        if type(key) ~= "string" then return nil, typed(errors.INVALID, "launch_args must be a plain object") end
    end
    local valid, validation_err = validate_json_value(launch_args, {}, "launch_args")
    if not valid then return nil, validation_err end
    local encoded, encode_err = json.encode(launch_args)
    if encode_err then return nil, typed(errors.UNAVAILABLE, "failed to encode launch_args: " .. tostring(encode_err)) end
    return encoded, nil
end

local function decode_launch_args(value: any)
    if value == nil then return nil, nil end
    local decoded = value
    if type(value) == "string" then
        local decode_err
        decoded, decode_err = json.decode(value)
        if decode_err then return nil, typed(errors.UNAVAILABLE, "failed to decode launch_args: " .. tostring(decode_err)) end
    end
    if type(decoded) ~= "table" then return nil, typed(errors.INVALID, "launch_args is not an object") end
    for key in pairs(decoded) do
        if type(key) ~= "string" then return nil, typed(errors.INVALID, "launch_args is not an object") end
    end
    return decoded, nil
end

local function normalize_row(row: any)
    if not row then return nil, nil end
    local launch_args, decode_err = decode_launch_args(row.launch_args)
    if decode_err then return nil, decode_err end
    return {
        dataflow_id = tostring(row.dataflow_id),
        generation = tonumber(row.generation),
        desired_active = row.desired_active == true or tonumber(row.desired_active) == 1,
        owner_epoch = row.owner_epoch and tostring(row.owner_epoch) or nil,
        launch_args = launch_args,
        requested_at = tostring(row.requested_at),
        updated_at = tostring(row.updated_at),
        admission_key = row.admission_key and tostring(row.admission_key) or nil,
        ever_activated = row.ever_activated == true or tonumber(row.ever_activated) == 1,
        terminal_status = row.terminal_status and tostring(row.terminal_status) or nil,
        terminal_outcome_json = row.terminal_outcome_json,
        terminal_generation = row.terminal_generation and tonumber(row.terminal_generation) or nil,
        terminal_ack_at = row.terminal_ack_at and tostring(row.terminal_ack_at) or nil,
    }, nil
end

-- The workflow row is the first lock in every transaction that also mutates
-- activation or wake rows. PostgreSQL foreign-key checks can hold KEY SHARE on
-- this parent row, so acquiring a weaker UPDATE lock and upgrading it later can
-- deadlock with a concurrent commit. Callers that cross the workflow/lifecycle
-- boundary must establish this lock before either side is changed.
function activation_repo.lock_workflow_tx(tx, dataflow_id)
    local db_type, type_err = tx:db_type()
    if type_err then return nil, type_err end
    if db_type ~= sql.type.POSTGRES and db_type ~= "postgres" then
        -- SQLite has no row-level SELECT FOR UPDATE. Make the parent row the
        -- first write so concurrent activation/terminal transactions serialize
        -- before either can inspect lifecycle state or touch a wake.
        local lock_result, lock_err = tx:execute(
            "UPDATE dataflows SET updated_at = updated_at WHERE dataflow_id = ?",
            { dataflow_id })
        if lock_err then return nil, lock_err end
        if not lock_result or (lock_result.rows_affected or 0) == 0 then
            return nil, typed(errors.NOT_FOUND, "dataflow not found")
        end
    end
    local query = "SELECT status FROM dataflows WHERE dataflow_id = ? LIMIT 1"
    if db_type == sql.type.POSTGRES or db_type == "postgres" then
        query = query .. " FOR UPDATE"
    end
    local rows, query_err = tx:query(rebind(query, db_type), { dataflow_id })
    if query_err then return nil, query_err end
    if not rows or not rows[1] then return nil, typed(errors.NOT_FOUND, "dataflow not found") end
    return tostring(rows[1].status), nil
end

local function get_tx(tx, dataflow_id)
    local rows, query_err = tx_query(tx, [[
        SELECT dataflow_id, generation, desired_active, owner_epoch,
               launch_args, requested_at, updated_at, admission_key, ever_activated,
               terminal_status, terminal_outcome_json, terminal_generation, terminal_ack_at
        FROM dataflow_activations WHERE dataflow_id = ? LIMIT 1
    ]], { dataflow_id })
    if query_err then return nil, query_err end
    return normalize_row(rows and rows[1] or nil)
end

local function terminal_result_from_status(status)
    if TERMINAL_STATUS[status] then
        return { changed = false, terminal = true, status = status }
    end
    return nil
end

-- The workflow row is already lifecycle-locked by the caller. Terminal state
-- owns both durable activation intent and its wake index, so converge them in
-- the same transaction before returning the terminal observation.
local function cleanup_terminal_tx(tx, dataflow_id, status, now_value)
    local flow_rows, flow_err = tx_query(tx,
        "SELECT metadata FROM dataflows WHERE dataflow_id = ?", { dataflow_id })
    if flow_err then return nil, typed(errors.UNAVAILABLE, "failed to read terminal outcome: " .. tostring(flow_err)) end
    local outcome = flow_rows and flow_rows[1] and flow_rows[1].metadata or nil
    if type(outcome) == "table" then
        local encoded, encode_err = json.encode(outcome)
        if encode_err then return nil, typed(errors.UNAVAILABLE, "failed to encode terminal outcome: " .. tostring(encode_err)) end
        outcome = encoded
    end
    local activation_result, activation_err = tx_execute(tx, [[
        UPDATE dataflow_activations
        SET desired_active = ?, launch_args = NULL, updated_at = ?,
            terminal_status = CASE WHEN admission_key IS NOT NULL
                THEN COALESCE(terminal_status, ?) ELSE terminal_status END,
            terminal_outcome_json = CASE WHEN admission_key IS NOT NULL
                THEN COALESCE(terminal_outcome_json, ?) ELSE terminal_outcome_json END,
            terminal_generation = CASE WHEN admission_key IS NOT NULL
                THEN COALESCE(terminal_generation, generation) ELSE terminal_generation END
        WHERE dataflow_id = ? AND (desired_active = ? OR launch_args IS NOT NULL
            OR (admission_key IS NOT NULL AND terminal_status IS NULL))
    ]], { false, now_value, status, outcome or sql.as.null(), dataflow_id, true })
    if activation_err then return nil, typed(errors.UNAVAILABLE, "failed to disable terminal activation: " .. tostring(activation_err)) end

    local wake_result, wake_err = tx_execute(tx,
        "DELETE FROM dataflow_wakes WHERE dataflow_id = ?", { dataflow_id })
    if wake_err then return nil, typed(errors.UNAVAILABLE, "failed to clear terminal wakes: " .. tostring(wake_err)) end

    local activation_disabled = activation_result and (activation_result.rows_affected or 0) > 0
    local wake_index_changed = wake_result and (wake_result.rows_affected or 0) > 0
    return {
        changed = activation_disabled or wake_index_changed,
        terminal = true,
        status = status,
        activation_disabled = activation_disabled,
        wake_index_changed = wake_index_changed,
    }, nil
end

function activation_repo.disable_terminal_tx(tx, dataflow_id, now_value)
    if not tx then return nil, typed(errors.INVALID, "transaction is required") end
    local valid, validation_err = validate_id(dataflow_id)
    if not valid then return nil, validation_err end
    valid, validation_err = validate_timestamp(now_value, "updated_at")
    if not valid then return nil, validation_err end
    local status, status_err = activation_repo.lock_workflow_tx(tx, dataflow_id)
    if status_err then return nil, status_err end
    if not TERMINAL_STATUS[status] then return nil, typed(errors.CONFLICT, "dataflow is not terminal") end
    return cleanup_terminal_tx(tx, dataflow_id, status, now_value)
end

function activation_repo.get(dataflow_id)
    local valid, id_err = validate_id(dataflow_id)
    if not valid then return nil, id_err end
    local db, db_err = sql.get(consts.APP_DB)
    if db_err then return nil, db_err end
    local rows, query_err = db_query(db, [[
        SELECT dataflow_id, generation, desired_active, owner_epoch,
               launch_args, requested_at, updated_at, admission_key, ever_activated,
               terminal_status, terminal_outcome_json, terminal_generation, terminal_ack_at
        FROM dataflow_activations WHERE dataflow_id = ? LIMIT 1
    ]], { dataflow_id })
    db:release()
    if query_err then return nil, query_err end
    return normalize_row(rows and rows[1] or nil)
end

function activation_repo.list_active()
    local db, db_err = sql.get(consts.APP_DB)
    if db_err then return nil, db_err end
    local rows, query_err = db_query(db, [[
        SELECT a.dataflow_id, a.generation, a.desired_active, a.owner_epoch, a.launch_args,
               a.requested_at, a.updated_at
        FROM dataflow_activations a
        JOIN dataflows d ON d.dataflow_id = a.dataflow_id
        WHERE a.desired_active = ? AND d.status NOT IN (?, ?, ?, ?)
        ORDER BY a.updated_at ASC, a.dataflow_id ASC
    ]], {
        true,
        TERMINAL_VALUES[1], TERMINAL_VALUES[2], TERMINAL_VALUES[3], TERMINAL_VALUES[4],
    })
    db:release()
    if query_err then return nil, query_err end
    local result = {}
    for _, row in ipairs(rows or {}) do
        local normalized, normalize_err = normalize_row(row)
        if normalize_err then return nil, normalize_err end
        table.insert(result, normalized)
    end
    return result, nil
end

local shared = {
    sql = sql, json = json, consts = consts,
    TERMINAL_STATUS = TERMINAL_STATUS, TERMINAL_VALUES = TERMINAL_VALUES,
    tx_query = tx_query, tx_execute = tx_execute, db_query = db_query,
    validate_id = validate_id, validate_timestamp = validate_timestamp,
    normalize_row = normalize_row, get_tx = get_tx,
    cleanup_terminal_tx = cleanup_terminal_tx, encode_launch_args = encode_launch_args,
    terminal_result_from_status = terminal_result_from_status,
    rebind = rebind, typed = typed,
}
require("activation_operations")(activation_repo, shared)
require("activation_evidence")(activation_repo, shared)

return activation_repo :: any
