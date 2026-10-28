-- SPDX-License-Identifier: Apache-2.0
-- Trusted storage inspection. Authorization remains with the calling owner;
-- these reads retain its database authority and never reconstruct an identity.
local sql = require("sql")
local json = require("json")
local consts = require("consts")

local M = {}
type Map = { [string]: any }

local function failure(err: any, message: string, code: string, kind: string, retryable: boolean): error
    local details: Map = {code = code}
    if type(err) == "userdata" then
        local native = err :: error
        for key, value in pairs(native:details() or {}) do details[key] = value end
        return errors.new({message = message .. ": " .. native:message(),
            kind = native:kind(), retryable = native:retryable(), details = details})
    end
    details.cause = err
    return errors.new({message = message, kind = kind, retryable = retryable, details = details})
end

local function query(statement: string, args: { any }, code: string): (Map[]?, error?)
    local db, db_err = sql.get(consts.APP_DB)
    if not db then return nil, failure(db_err, "Dataflow database unavailable", code, errors.UNAVAILABLE, true) end
    local rows, read_err = db:query(statement, args)
    db:release()
    if read_err then return nil, failure(read_err, "Dataflow inspection failed", code, errors.UNAVAILABLE, true) end
    if type(rows) ~= "table" then
        return nil, failure(nil, "Dataflow inspection response invalid", "DATAFLOW_READ_RESPONSE_INVALID", errors.INTERNAL, false)
    end
    return rows, nil
end

local function object(value: any, code: string): (Map?, error?)
    if type(value) == "table" then return value :: Map, nil end
    if type(value) == "string" and value:match("^%s*{") then
        local decoded, err = json.decode(value)
        if not err and type(decoded) == "table" then return decoded :: Map, nil end
    end
    return nil, failure(nil, "Persisted Dataflow state invalid", code, errors.INTERNAL, false)
end

function M.flow(dataflow_id: string): (Map?, error?)
    if type(dataflow_id) ~= "string" or dataflow_id == "" then
        return nil, failure(nil, "dataflow_id is required", "DATAFLOW_ID_REQUIRED", errors.INVALID, false)
    end
    local rows, err = query([[SELECT dataflow_id, actor_id, actor_context, status, metadata
        FROM dataflows WHERE dataflow_id=$1 LIMIT 1]], {dataflow_id}, "DATAFLOW_FLOW_LOOKUP_FAILED")
    if err or not rows then return nil, err end
    if not rows[1] then
        return nil, failure(nil, "Flow not found", "DATAFLOW_FLOW_NOT_FOUND", errors.NOT_FOUND, false)
    end
    local row = rows[1]
    local metadata = row.metadata
    -- Legacy constructors encode an absent metadata value as JSON null or an
    -- empty array. Neither carries a resume route; retain their empty meaning.
    if metadata == nil or metadata == "" or metadata == "null" or metadata == "[]" then metadata = {} end
    local decoded, decode_err = object(metadata, "DATAFLOW_FLOW_METADATA_INVALID")
    if decode_err then return nil, decode_err end
    row.metadata = decoded
    return row, nil
end

-- The caller selects a node type implementing the signal-wait protocol. Reads
-- only that type's active nodes; declarations alone never prove a parked wait.
function M.pending_signal(dataflow_id: string, signal_id: string, node_type: string): (Map?, error?)
    for _, arg in ipairs({{name = "dataflow_id", value = dataflow_id},
        {name = "signal_id", value = signal_id}, {name = "node_type", value = node_type}}) do
        local field, value = arg.name, arg.value
        if type(value) ~= "string" or value == "" then
            return nil, failure(nil, field .. " is required", "DATAFLOW_SIGNAL_ARGUMENT_INVALID", errors.INVALID, false)
        end
    end
    local rows, err = query([[SELECT d.data_id, d.node_id, d.type, d.key, d.discriminator, d.content
        FROM dataflow_data d JOIN dataflow_nodes n
          ON n.node_id=d.node_id AND n.dataflow_id=d.dataflow_id
        WHERE d.dataflow_id=$1 AND n.type=$2 AND n.status IN ($3,$4,$5)
          AND d.type IN ($6,$7)
        ORDER BY d.data_id DESC]], {dataflow_id, node_type, consts.STATUS.PENDING, consts.STATUS.RUNNING,
            consts.STATUS.WAITING, consts.DATA_TYPE.NODE_YIELD, consts.DATA_TYPE.NODE_YIELD_RESULT},
        "DATAFLOW_SIGNAL_LOOKUP_FAILED")
    if err or not rows then return nil, err end
    local satisfied: Map = {}
    for _, row in ipairs(rows) do
        if row.type == consts.DATA_TYPE.NODE_YIELD_RESULT then
            local id = row.key or row.discriminator
            if type(id) ~= "string" or id == "" then
                return nil, failure(nil, "Signal result identifier invalid", "DATAFLOW_SIGNAL_STATE_INVALID", errors.INTERNAL, false)
            end
            satisfied[id] = true
        end
    end
    local seen: Map = {}
    for _, row in ipairs(rows) do
        if row.type == consts.DATA_TYPE.NODE_YIELD and not seen[row.node_id] then
            -- Match recovery's durable UUID-v7 ordering. An older wait must not
            -- reopen after the latest episode has received its durable result.
            seen[row.node_id] = true
            local value, decode_err = object(row.content, "DATAFLOW_SIGNAL_STATE_INVALID")
            if decode_err then return nil, decode_err end
            local wait = value.yield_context
            if type(value.yield_id) ~= "string" or value.yield_id == ""
                or type(wait) ~= "table" or wait.wait_for_signal ~= true
                or type(wait.signal_id) ~= "string" or wait.signal_id == "" then
                return nil, failure(nil, "Signal wait context invalid", "DATAFLOW_SIGNAL_STATE_INVALID", errors.INTERNAL, false)
            end
            if wait.signal_id == signal_id and not satisfied[value.yield_id] then
                return {node_id = row.node_id, type = node_type, yield_id = value.yield_id}, nil
            end
        end
    end
    return nil, failure(nil, "Pending signal not found", "DATAFLOW_SIGNAL_NOT_FOUND", errors.NOT_FOUND, false)
end

return M
