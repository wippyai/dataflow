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
    local terminal_result_from_status = shared.terminal_result_from_status
    local encode_launch_args = shared.encode_launch_args
    local rebind = shared.rebind
    local typed = shared.typed

local function advance_activation_tx(tx, dataflow_id, launch_args: any, now_value, preserve_launch_args)
    local encoded_args, encode_err = encode_launch_args(launch_args)
    if encode_err then return nil, encode_err end

    local update_args = preserve_launch_args and "dataflow_activations.launch_args" or "excluded.launch_args"
    local result, write_err = tx_execute(tx, ([[
        INSERT INTO dataflow_activations(
            dataflow_id, generation, desired_active, owner_epoch,
            launch_args, requested_at, updated_at
        )
        SELECT ?, 1, ?, NULL, ?, ?, ? FROM dataflows
        WHERE dataflow_id = ? AND status NOT IN (?, ?, ?, ?)
        ON CONFLICT(dataflow_id) DO UPDATE SET
            generation = dataflow_activations.generation + 1,
            desired_active = excluded.desired_active,
            owner_epoch = NULL,
            launch_args = %s,
            requested_at = excluded.requested_at,
            updated_at = excluded.updated_at
        WHERE EXISTS (
            SELECT 1 FROM dataflows
            WHERE dataflow_id = excluded.dataflow_id AND status NOT IN (?, ?, ?, ?)
        )
    ]]):format(update_args), {
        dataflow_id, true, encoded_args or sql.as.null(), now_value, now_value, dataflow_id,
        TERMINAL_VALUES[1], TERMINAL_VALUES[2], TERMINAL_VALUES[3], TERMINAL_VALUES[4],
        TERMINAL_VALUES[1], TERMINAL_VALUES[2], TERMINAL_VALUES[3], TERMINAL_VALUES[4],
    })
    if write_err then return nil, typed(errors.UNAVAILABLE, "failed to advance activation: " .. tostring(write_err)) end
    if not result or (result.rows_affected or 0) == 0 then
        return nil, typed(errors.INTERNAL, "activation request made no change")
    end

    local row, row_err = get_tx(tx, dataflow_id)
    if row_err then return nil, row_err end
    if not row then return nil, typed(errors.INTERNAL, "activation row missing after advance") end
    row.changed = true
    row.terminal = false
    return row, nil
end

function activation_repo.request_activation_tx(tx, dataflow_id, launch_args, now_value)
    if not tx then return nil, typed(errors.INVALID, "transaction is required") end
    local valid, id_err = validate_id(dataflow_id)
    if not valid then return nil, id_err end
    valid, id_err = validate_timestamp(now_value, "requested_at")
    if not valid then return nil, id_err end
    local status, status_err = activation_repo.lock_workflow_tx(tx, dataflow_id)
    if status_err then return nil, status_err end
    local terminal = terminal_result_from_status(status)
    if terminal then return terminal, nil end
    return advance_activation_tx(tx, dataflow_id, launch_args, now_value, false)
end

function activation_repo.activate_for_signal_tx(tx, dataflow_id, wake_key, wake_at, now_value)
    if not tx then return nil, typed(errors.INVALID, "transaction is required") end
    local valid, validation_err = validate_id(dataflow_id)
    if not valid then return nil, validation_err end
    if type(wake_key) ~= "string" or not wake_key:match("^signal:.+") then
        return nil, typed(errors.INVALID, "signal wake_key is required")
    end
    valid, validation_err = validate_timestamp(wake_at, "wake_at")
    if not valid then return nil, validation_err end
    valid, validation_err = validate_timestamp(now_value, "requested_at")
    if not valid then return nil, validation_err end

    local status, status_err = activation_repo.lock_workflow_tx(tx, dataflow_id)
    if status_err then return nil, status_err end
    local terminal = terminal_result_from_status(status)
    if terminal then
        terminal.wake_inserted = false
        return terminal, nil
    end

    local insert_result, insert_err = tx_execute(tx, [[
        INSERT INTO dataflow_wakes(dataflow_id, wake_key, wake_at, activation_generation)
        SELECT ?, ?, ?, NULL FROM dataflows
        WHERE dataflow_id = ? AND status NOT IN (?, ?, ?, ?)
        ON CONFLICT(dataflow_id, wake_key) DO NOTHING
    ]], {
        dataflow_id, wake_key, wake_at, dataflow_id,
        TERMINAL_VALUES[1], TERMINAL_VALUES[2], TERMINAL_VALUES[3], TERMINAL_VALUES[4],
    })
    if insert_err then return nil, typed(errors.UNAVAILABLE, "failed to insert signal wake: " .. tostring(insert_err)) end

    if not insert_result or (insert_result.rows_affected or 0) == 0 then
        local rows, row_err = tx_query(tx, [[
            SELECT activation_generation FROM dataflow_wakes
            WHERE dataflow_id = ? AND wake_key = ? LIMIT 1
        ]], { dataflow_id, wake_key })
        if row_err then return nil, row_err end
        return {
            changed = false,
            terminal = false,
            wake_inserted = false,
            generation = rows and rows[1] and tonumber(rows[1].activation_generation) or nil,
        }, nil
    end

    local activation, activation_err = advance_activation_tx(tx, dataflow_id, nil, now_value, true)
    if activation_err then return nil, activation_err end
    if activation.terminal then return nil, typed(errors.CONFLICT, "signal wake inserted for terminal dataflow") end

    local stamp_result, stamp_err = tx_execute(tx, [[
        UPDATE dataflow_wakes SET activation_generation = ?
        WHERE dataflow_id = ? AND wake_key = ? AND activation_generation IS NULL
    ]], { activation.generation, dataflow_id, wake_key })
    if stamp_err then return nil, typed(errors.UNAVAILABLE, "failed to fence signal wake: " .. tostring(stamp_err)) end
    if not stamp_result or (stamp_result.rows_affected or 0) ~= 1 then
        return nil, typed(errors.INTERNAL, "signal wake generation fence was not written")
    end

    activation.wake_inserted = true
    return activation, nil
end

function activation_repo.activate_due_tx(tx, dataflow_id, wake_key, now_value)
    if not tx then return nil, typed(errors.INVALID, "transaction is required") end
    local valid, validation_err = validate_id(dataflow_id)
    if not valid then return nil, validation_err end
    if type(wake_key) ~= "string" or wake_key == "" then return nil, typed(errors.INVALID, "wake_key is required") end
    valid, validation_err = validate_timestamp(now_value, "now")
    if not valid then return nil, validation_err end

    local status, status_err = activation_repo.lock_workflow_tx(tx, dataflow_id)
    if status_err then return nil, status_err end
    local terminal = terminal_result_from_status(status)
    if terminal then
        local cleaned, cleanup_err = cleanup_terminal_tx(tx, dataflow_id, status, now_value)
        if cleanup_err then return nil, cleanup_err end
        cleaned.promoted = false
        return cleaned, nil
    end

    -- This conditional no-op update is the row lock/CAS. On PostgreSQL a
    -- concurrent scanner waits and then rechecks activation_generation; on
    -- SQLite it acquires the database writer lock before generation advances.
    local lock_result, lock_err = tx_execute(tx, [[
        UPDATE dataflow_wakes SET wake_at = wake_at
        WHERE dataflow_id = ? AND wake_key = ? AND wake_at <= ?
          AND activation_generation IS NULL
          AND EXISTS (
              SELECT 1 FROM dataflows
              WHERE dataflow_id = ? AND status NOT IN (?, ?, ?, ?)
          )
    ]], {
        dataflow_id, wake_key, now_value, dataflow_id,
        TERMINAL_VALUES[1], TERMINAL_VALUES[2], TERMINAL_VALUES[3], TERMINAL_VALUES[4],
    })
    if lock_err then return nil, typed(errors.UNAVAILABLE, "failed to lock due wake: " .. tostring(lock_err)) end

    if lock_result and (lock_result.rows_affected or 0) > 0 then
        local activation, activation_err = advance_activation_tx(tx, dataflow_id, nil, now_value, true)
        if activation_err then return nil, activation_err end
        if activation.terminal then return nil, typed(errors.CONFLICT, "due wake promoted for terminal dataflow") end
        local stamp_result, stamp_err = tx_execute(tx, [[
            UPDATE dataflow_wakes SET activation_generation = ?
            WHERE dataflow_id = ? AND wake_key = ? AND activation_generation IS NULL
        ]], { activation.generation, dataflow_id, wake_key })
        if stamp_err then return nil, typed(errors.UNAVAILABLE, "failed to fence due wake: " .. tostring(stamp_err)) end
        if not stamp_result or (stamp_result.rows_affected or 0) ~= 1 then
            return nil, typed(errors.INTERNAL, "due wake generation fence was not written")
        end
        activation.promoted = true
        return activation, nil
    end

    local rows, row_err = tx_query(tx, [[
        SELECT wake_at, activation_generation FROM dataflow_wakes
        WHERE dataflow_id = ? AND wake_key = ? LIMIT 1
    ]], { dataflow_id, wake_key })
    if row_err then return nil, row_err end
    local row = rows and rows[1] or nil
    if not row then
        return { changed = false, terminal = false, promoted = false, missing = true }, nil
    end
    if row.activation_generation ~= nil then
        return {
            changed = false,
            terminal = false,
            promoted = false,
            already_promoted = true,
            generation = tonumber(row.activation_generation),
        }, nil
    end
    return { changed = false, terminal = false, promoted = false, due = false }, nil
end

function activation_repo.release_if_generation_tx(tx, dataflow_id, generation, now_value)
    if not tx then return nil, typed(errors.INVALID, "transaction is required") end
    local valid, validation_err = validate_id(dataflow_id)
    if not valid then return nil, validation_err end
    generation = tonumber(generation)
    if not generation or generation < 1 or generation % 1 ~= 0 then
        return nil, typed(errors.INVALID, "generation must be a positive integer")
    end
    valid, validation_err = validate_timestamp(now_value, "updated_at")
    if not valid then return nil, validation_err end

    local status, status_err = activation_repo.lock_workflow_tx(tx, dataflow_id)
    if status_err then return nil, status_err end
    local terminal = terminal_result_from_status(status)
    if terminal then
        terminal.released = false
        return terminal, nil
    end

    local result, update_err = tx_execute(tx, [[
        UPDATE dataflow_activations
        SET desired_active = ?, launch_args = NULL, updated_at = ?
        WHERE dataflow_id = ? AND generation = ? AND desired_active = ?
          AND EXISTS (
              SELECT 1 FROM dataflows
              WHERE dataflow_id = ? AND status NOT IN (?, ?, ?, ?)
          )
    ]], {
        false, now_value, dataflow_id, generation, true, dataflow_id,
        TERMINAL_VALUES[1], TERMINAL_VALUES[2], TERMINAL_VALUES[3], TERMINAL_VALUES[4],
    })
    if update_err then return nil, typed(errors.UNAVAILABLE, "failed to release activation: " .. tostring(update_err)) end
    if result and (result.rows_affected or 0) > 0 then
        return { changed = true, released = true, generation = generation, terminal = false }, nil
    end

    local current, current_err = get_tx(tx, dataflow_id)
    if current_err then return nil, current_err end
    return {
        changed = false,
        released = false,
        terminal = false,
        generation = current and current.generation or nil,
    }, nil
end

-- Fence process ownership before spawn. A generation can be claimed only from
-- the exact epoch observed by the overseer. The write happens before process
-- creation, so an overseer crash between claim and spawn is classified as a
-- same-runtime loss rather than retried into a process flood.
function activation_repo.claim_epoch_tx(
    tx, dataflow_id, generation, observed_epoch, runtime_epoch, now_value)
    if not tx then return nil, typed(errors.INVALID, "transaction is required") end
    local valid, validation_err = validate_id(dataflow_id)
    if not valid then return nil, validation_err end
    generation = tonumber(generation)
    if not generation or generation < 1 or generation % 1 ~= 0 then
        return nil, typed(errors.INVALID, "generation must be a positive integer")
    end
    if type(runtime_epoch) ~= "string" or runtime_epoch == "" then
        return nil, typed(errors.INVALID, "runtime_epoch is required")
    end
    valid, validation_err = validate_timestamp(now_value, "updated_at")
    if not valid then return nil, validation_err end

    local status, status_err = activation_repo.lock_workflow_tx(tx, dataflow_id)
    if status_err then return nil, status_err end
    local terminal = terminal_result_from_status(status)
    if terminal then
        terminal.claimed = false
        return terminal, nil
    end

    local epoch_predicate = "owner_epoch IS NULL"
    local params = { runtime_epoch, now_value, dataflow_id, generation, true }
    if observed_epoch ~= nil then
        if type(observed_epoch) ~= "string" or observed_epoch == "" then
            return nil, typed(errors.INVALID, "observed_epoch must be nil or a non-empty string")
        end
        epoch_predicate = "owner_epoch = ?"
        table.insert(params, observed_epoch)
    end
    local result, update_err = tx_execute(tx, [[
        UPDATE dataflow_activations
        SET owner_epoch = ?, updated_at = ?
        WHERE dataflow_id = ? AND generation = ? AND desired_active = ?
          AND ]] .. epoch_predicate, params)
    if update_err then return nil, typed(errors.UNAVAILABLE, "failed to claim activation epoch: " .. tostring(update_err)) end

    local current, current_err = get_tx(tx, dataflow_id)
    if current_err then return nil, current_err end
    if not current then return nil, typed(errors.INTERNAL, "activation row missing after epoch claim") end
    current.claimed = result ~= nil and (result.rows_affected or 0) == 1
    current.terminal = false
    return current, nil
end

function activation_repo.consume_wake_tx(tx, dataflow_id, wake_key, generation)
    if not tx then return nil, typed(errors.INVALID, "transaction is required") end
    local valid, validation_err = validate_id(dataflow_id)
    if not valid then return nil, validation_err end
    if type(wake_key) ~= "string" or wake_key == "" then return nil, typed(errors.INVALID, "wake_key is required") end

    local status, status_err = activation_repo.lock_workflow_tx(tx, dataflow_id)
    if status_err then return nil, status_err end
    local terminal = terminal_result_from_status(status)
    if terminal then
        terminal.consumed = false
        return terminal, nil
    end

    local query = "DELETE FROM dataflow_wakes WHERE dataflow_id = ? AND wake_key = ?"
    local params = { dataflow_id, wake_key }
    if generation ~= nil then
        generation = tonumber(generation)
        if not generation or generation < 1 or generation % 1 ~= 0 then
            return nil, typed(errors.INVALID, "generation must be a positive integer")
        end
        query = query .. " AND activation_generation = ?"
        table.insert(params, generation)
    end
    local result, delete_err = tx_execute(tx, query, params)
    if delete_err then return nil, typed(errors.UNAVAILABLE, "failed to consume wake: " .. tostring(delete_err)) end
    return { changed = result and (result.rows_affected or 0) > 0, consumed = result and (result.rows_affected or 0) > 0 }, nil
end

-- Register or re-arm a durable yield deadline. Reusing the same logical yield
-- is a new wait episode, so any activation fence left by the previous episode
-- must be cleared atomically with the new deadline.
function activation_repo.register_yield_wake_tx(tx, dataflow_id, yield_id, wake_at)
    if not tx then return nil, typed(errors.INVALID, "transaction is required") end
    local valid, validation_err = validate_id(dataflow_id)
    if not valid then return nil, validation_err end
    if type(yield_id) ~= "string" or yield_id == "" then return nil, typed(errors.INVALID, "yield_id is required") end
    valid, validation_err = validate_timestamp(wake_at, "wake_at")
    if not valid then return nil, validation_err end

    local result, write_err = tx_execute(tx, [[
        INSERT INTO dataflow_wakes(dataflow_id, wake_key, wake_at, activation_generation)
        VALUES (?, ?, ?, NULL)
        ON CONFLICT(dataflow_id, wake_key) DO UPDATE SET
            wake_at = excluded.wake_at,
            activation_generation = NULL
    ]], { dataflow_id, "yield:" .. yield_id, wake_at })
    if write_err then return nil, typed(errors.UNAVAILABLE, "failed to register yield wake: " .. tostring(write_err)) end
    return {
        changed = result ~= nil and (result.rows_affected or 0) > 0,
    }, nil
end

end
