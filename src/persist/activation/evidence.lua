return function(activation_repo, shared)
    local sql = shared.sql
    local json = shared.json
    local consts = shared.consts
    local TERMINAL_STATUS = shared.TERMINAL_STATUS
    local TERMINAL_VALUES = shared.TERMINAL_VALUES
    local tx_query = shared.tx_query
    local tx_execute = shared.tx_execute
    local db_query = shared.db_query
    local validate_id = shared.validate_id
    local validate_timestamp = shared.validate_timestamp
    local normalize_row = shared.normalize_row
    local get_tx = shared.get_tx
    local cleanup_terminal_tx = shared.cleanup_terminal_tx
    local encode_launch_args = shared.encode_launch_args
    local rebind = shared.rebind
    local typed = shared.typed

local function admission_key_valid(key)
    return type(key) == "string" and key ~= ""
end

local function evidence_from_row(row, status, key)
    if not row then
        return { state = TERMINAL_STATUS[status] and "terminal" or "created",
            admission_key = key, ever_activated = false,
            terminal_status = TERMINAL_STATUS[status] and status or nil }, nil
    end
    local outcome = row.terminal_outcome_json
    if type(outcome) == "string" and outcome ~= "" then
        local decoded, err = json.decode(outcome)
        if err then return nil, typed(errors.INTERNAL, "invalid terminal outcome: " .. tostring(err)) end
        outcome = decoded
    end
    local state = "activated"
    if TERMINAL_STATUS[status] or row.terminal_status then
        state = row.terminal_status and "terminal" or "unknown"
    elseif status == consts.STATUS.RUNNING then state = "running" end
    return {
        state = state, admission_key = key, generation = row.generation,
        ever_activated = row.ever_activated, terminal_status = row.terminal_status,
        terminal_outcome = outcome, terminal_generation = row.terminal_generation,
        terminal_ack_at = row.terminal_ack_at,
    }, nil
end

local function transaction(fn)
    local db, db_err = sql.get(consts.APP_DB)
    if db_err then return nil, db_err end
    local tx, begin_err = db:begin()
    if begin_err then db:release(); return nil, begin_err end
    local value, operation_err = fn(tx)
    if operation_err then tx:rollback(); db:release(); return nil, operation_err end
    local committed, commit_err = tx:commit()
    if not committed or commit_err then
        tx:rollback(); db:release()
        return nil, commit_err or typed(errors.UNAVAILABLE, "transaction did not commit")
    end
    db:release()
    return value, nil
end

function activation_repo.ensure_activation(dataflow_id, admission_key, now_value)
    local valid, id_err = validate_id(dataflow_id)
    if not valid then return nil, id_err end
    if not admission_key_valid(admission_key) then return nil, typed(errors.INVALID, "admission_key is required") end
    valid, id_err = validate_timestamp(now_value, "requested_at")
    if not valid then return nil, id_err end
    return transaction(function(tx)
        local status, lock_err = activation_repo.lock_workflow_tx(tx, dataflow_id)
        if lock_err and errors.is(lock_err, errors.NOT_FOUND) then
            return { state = "absent", admission_key = admission_key,
                ever_activated = false }, nil
        end
        if lock_err then return nil, lock_err end
        local row, row_err = get_tx(tx, dataflow_id)
        if row_err then return nil, row_err end
        if row and row.admission_key and row.admission_key ~= admission_key then
            return nil, typed(errors.CONFLICT, "dataflow has another admission key")
        end
        if not row and TERMINAL_STATUS[status] then
            return evidence_from_row(nil, status, admission_key)
        end
        if not row then
            local result, insert_err = tx_execute(tx, [[
                INSERT INTO dataflow_activations(dataflow_id,generation,desired_active,
                    owner_epoch,launch_args,requested_at,updated_at,admission_key,ever_activated)
                VALUES (?,1,?,NULL,NULL,?,?,?,?)
            ]], { dataflow_id, true, now_value, now_value, admission_key, true })
            if insert_err then return nil, insert_err end
            if not result or (result.rows_affected or 0) ~= 1 then
                return nil, typed(errors.INTERNAL, "activation insert made no change")
            end
        elseif not row.admission_key then
            local _, update_err = tx_execute(tx, [[
                UPDATE dataflow_activations
                SET admission_key = ?, ever_activated = ?, updated_at = ?
                WHERE dataflow_id = ? AND admission_key IS NULL
            ]], { admission_key, true, now_value, dataflow_id })
            if update_err then return nil, update_err end
        end
        if TERMINAL_STATUS[status] then
            local _, cleanup_err = cleanup_terminal_tx(tx, dataflow_id, status, now_value)
            if cleanup_err then return nil, cleanup_err end
        end
        row, row_err = get_tx(tx, dataflow_id)
        if row_err then return nil, row_err end
        return evidence_from_row(row, status, admission_key)
    end)
end

function activation_repo.get_activation_evidence(dataflow_id, admission_key)
    local valid, id_err = validate_id(dataflow_id)
    if not valid then return nil, id_err end
    if not admission_key_valid(admission_key) then return nil, typed(errors.INVALID, "admission_key is required") end
    local db, db_err = sql.get(consts.APP_DB)
    if db_err then return nil, db_err end
    local rows, query_err = db_query(db, [[
        SELECT d.status, a.dataflow_id, a.generation, a.desired_active,
            a.owner_epoch, a.launch_args, a.requested_at, a.updated_at,
            a.admission_key, a.ever_activated, a.terminal_status,
            a.terminal_outcome_json, a.terminal_generation, a.terminal_ack_at
        FROM dataflows d LEFT JOIN dataflow_activations a ON a.dataflow_id = d.dataflow_id
        WHERE d.dataflow_id = ? LIMIT 1
    ]], { dataflow_id })
    db:release()
    if query_err then return nil, query_err end
    local joined = rows and rows[1] or nil
    if not joined then return { state = "absent", admission_key = admission_key,
        ever_activated = false }, nil end
    if joined.admission_key and tostring(joined.admission_key) ~= admission_key then
        return nil, typed(errors.CONFLICT, "dataflow has another admission key")
    end
    local row, row_err = normalize_row(joined.dataflow_id and joined or nil)
    if row_err then return nil, row_err end
    return evidence_from_row(row, tostring(joined.status), admission_key)
end

function activation_repo.ack_terminal(dataflow_id, admission_key, generation, now_value)
    local valid, id_err = validate_id(dataflow_id)
    if not valid then return nil, id_err end
    if not admission_key_valid(admission_key) then return nil, typed(errors.INVALID, "admission_key is required") end
    generation = tonumber(generation)
    if not generation or generation < 1 or generation % 1 ~= 0 then
        return nil, typed(errors.INVALID, "generation must be a positive integer")
    end
    valid, id_err = validate_timestamp(now_value, "terminal_ack_at")
    if not valid then return nil, id_err end
    return transaction(function(tx)
        local _, lock_err = activation_repo.lock_workflow_tx(tx, dataflow_id)
        if lock_err then return nil, lock_err end
        local row, row_err = get_tx(tx, dataflow_id)
        if row_err then return nil, row_err end
        if not row or row.admission_key ~= admission_key then
            return nil, typed(errors.CONFLICT, "terminal admission or generation differs")
        end
        if not row.terminal_status then return nil, typed(errors.UNAVAILABLE, "terminal evidence is unavailable") end
        if row.terminal_generation ~= generation then
            return nil, typed(errors.CONFLICT, "terminal admission or generation differs")
        end
        if not row.terminal_ack_at then
            local _, update_err = tx_execute(tx, [[
                UPDATE dataflow_activations SET terminal_ack_at = ?
                WHERE dataflow_id = ? AND admission_key = ?
                    AND terminal_generation = ? AND terminal_ack_at IS NULL
            ]], { now_value, dataflow_id, admission_key, generation })
            if update_err then return nil, update_err end
            row, row_err = get_tx(tx, dataflow_id)
            if row_err then return nil, row_err end
        end
        return { acknowledged = true, terminal_ack_at = row.terminal_ack_at }, nil
    end)
end

end
