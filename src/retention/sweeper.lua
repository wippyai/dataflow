local time = require("time")

local M = {}

-- Keep this allowlist aligned with the partial index in migration 10.
local DIAGNOSTICS = "('cycle.state','cycle.function_result','node.input','node.yield','node.yield.result','parallel.progress')"
local TERMINAL = "('completed','failed','cancelled','terminated')"

function M.validate(options)
    options = options or {}
    local days = tonumber(options.days or 0)
    local batch = tonumber(options.batch_size or 500)
    if not days or days < 0 or days > 36500 or days % 1 ~= 0 then
        return nil, "retention days must be an integer from 0 (disabled) to 36500"
    end
    if not batch or batch < 1 or batch > 1000 or batch % 1 ~= 0 then
        return nil, "retention batch_size must be an integer from 1 to 1000"
    end
    return { days = days, batch_size = batch, dry_run = options.dry_run == true }
end

local function rebind(query, postgres)
    if not postgres then return query end
    local i = 0
    return (query:gsub("%?", function() i = i + 1; return "$" .. i end))
end

local function read_page(query, table_name, key, predicate, index, postgres, cutoff, limit, after)
    local age = postgres and "created_at" or "julianday(created_at)"
    local cursor_age = postgres and "CAST(created_at AS text)" or age
    local boundary = postgres and "CAST(? AS timestamp)" or "julianday(?)"
    local source = postgres and table_name or (table_name .. " INDEXED BY " .. index)
    local select = "SELECT " .. key .. " AS id, dataflow_id, " .. cursor_age .. " AS age FROM " .. source
        .. " WHERE " .. predicate .. " AND "
    if not after then
        return query(select .. age .. " < " .. boundary .. " ORDER BY " .. age .. "," .. key .. " LIMIT ?", { cutoff, limit })
    end

    -- Separate the tied and later age seeks for SQLite expression indexes.
    local cursor_bound = postgres and "CAST(? AS timestamp)" or "?"
    local rows = query(select .. age .. " = " .. cursor_bound .. " AND " .. key .. " > ? ORDER BY " .. key .. " LIMIT ?",
        { after.age, after.id, limit })
    if #rows == limit then return rows end
    local later = query(select .. age .. " > " .. cursor_bound .. " AND " .. age .. " < " .. boundary
        .. " ORDER BY " .. age .. "," .. key .. " LIMIT ?", { after.age, cutoff, limit - #rows })
    for _, row in ipairs(later) do rows[#rows + 1] = row end
    return rows
end

-- Use only the candidates' ancestry and rooted families. Branches beyond an
-- already disqualifying member cannot change eligibility. The parent relation
-- has a single edge per row, so only the upward walk needs cycle detection.
local function eligible_flows(query, flow_ids, postgres, cutoff)
    if #flow_ids == 0 then return {} end
    local values, params = {}, {}
    for _, id in ipairs(flow_ids) do
        values[#values + 1] = postgres and "(?::uuid)" or "(?)"
        params[#params + 1] = id
    end
    params[#params + 1] = cutoff
    params[#params + 1] = cutoff

    local point_join = postgres and " JOIN " or " CROSS JOIN "
    local parent_join = postgres and [[ CROSS JOIN LATERAL (SELECT dataflow_id,parent_dataflow_id
        FROM dataflows WHERE dataflow_id=up.parent_id OFFSET 0) f]]
        or " CROSS JOIN dataflows f ON f.dataflow_id=up.parent_id"
    local child_join = postgres and [[ CROSS JOIN LATERAL (SELECT dataflow_id,status,updated_at
        FROM dataflows WHERE parent_dataflow_id=walk.flow_id OFFSET 0) f]]
        or " CROSS JOIN dataflows f INDEXED BY idx_dataflows_parent ON f.parent_dataflow_id=walk.flow_id"
    local old = postgres and "updated_at < CAST(? AS timestamp)" or "julianday(updated_at) < julianday(?)"
    local blocked = "CASE WHEN status IN " .. TERMINAL .. " AND " .. old .. " THEN 0 ELSE 1 END"
    local rows = query("WITH RECURSIVE candidates(leaf) AS (VALUES " .. table.concat(values, ",") .. [[),
        up(leaf,flow_id,parent_id) AS (
            SELECT leaf,f.dataflow_id,f.parent_dataflow_id FROM candidates c]] .. point_join .. [[dataflows f ON f.dataflow_id=c.leaf
            UNION
            SELECT up.leaf,f.dataflow_id,f.parent_dataflow_id FROM up]] .. parent_join .. [[
        ), mapping(leaf,root) AS MATERIALIZED (SELECT leaf,flow_id FROM up WHERE parent_id IS NULL),
        roots(root) AS (SELECT DISTINCT root FROM mapping),
        walk(root,flow_id,blocked) AS (
            SELECT root,dataflow_id,]] .. blocked .. " FROM roots" .. point_join .. [[dataflows ON dataflow_id=root
            UNION ALL
            SELECT walk.root,f.dataflow_id,]] .. blocked .. " FROM walk" .. child_join .. [[ WHERE walk.blocked=0
        ), protected(root) AS (SELECT DISTINCT root FROM walk WHERE blocked=1)
        SELECT m.leaf FROM mapping m WHERE NOT EXISTS (SELECT 1 FROM protected p WHERE p.root=m.root)
    ]], params)
    local eligible = {}
    for _, row in ipairs(rows) do eligible[row.leaf] = true end
    return eligible
end

-- Each pass reuses a cutoff across pages, with no DB transaction between pages.
-- Caller owns the database handle. A yield may return false to end the pass.
function M.run(db, options, yield_batch)
    local config, err = M.validate(options)
    if not config then return nil, err end
    local stats: { [string]: any } = { enabled = config.days > 0, days = config.days, data = 0, commits = 0,
        data_candidates = 0, commit_candidates = 0, dry_run = config.dry_run }
    if not stats.enabled then return stats end
    local dialect, type_err = db:type()
    if type_err then return nil, tostring(type_err) end
    local postgres = tostring(dialect) == "postgres"
    if not postgres and tostring(dialect) ~= "sqlite" then return nil, "unsupported retention database" end
    local cutoff = time.now():add(-config.days * 24 * time.HOUR):format(time.RFC3339)
    local delay = postgres and "1ms" or "20ms"

    local function transaction(fn)
        local tx, begin_err = db:begin()
        if begin_err then error(tostring(begin_err)) end
        local function execute(query, params)
            local result, execute_err = tx:execute(rebind(query, postgres), params or {})
            if execute_err then error(tostring(execute_err)) end
            return result
        end
        local function query(statement, params)
            local rows, query_err = tx:query(rebind(statement, postgres), params or {})
            if query_err then error(tostring(query_err)) end
            return rows
        end
        local ok, result = pcall(function()
            if postgres then
                execute("SET LOCAL lock_timeout = '2s'")
                execute("SET LOCAL statement_timeout = '30s'")
                execute("SET LOCAL jit = off")
                -- Released writers, including child creation, already take a conflicting lock.
                execute("LOCK TABLE dataflows IN SHARE ROW EXCLUSIVE MODE")
            else
                execute("UPDATE dataflows SET status = status WHERE 1 = 0")
            end
            return fn(query, execute)
        end)
        if not ok then tx:rollback(); error(tostring(result)) end
        local _, commit_err = tx:commit()
        if commit_err then tx:rollback(); error(tostring(commit_err)) end
        return result
    end

    local function scan(table_name, key, predicate, index, field, candidate_field)
        local after
        while true do
            local rows = transaction(function(query, execute)
                local page = read_page(query, table_name, key, predicate, index, postgres, cutoff, config.batch_size, after)
                local flow_ids, seen, marks = {}, {}, {}
                for _, row in ipairs(page) do
                    if not seen[row.dataflow_id] then
                        flow_ids[#flow_ids + 1] = row.dataflow_id
                        marks[#marks + 1] = "?"
                        seen[row.dataflow_id] = true
                    end
                end
                local last = {}
                if field == "commits" and #flow_ids > 0 then
                    for _, row in ipairs(query("SELECT dataflow_id,last_commit_id FROM dataflows WHERE dataflow_id IN ("
                        .. table.concat(marks, ",") .. ")", flow_ids)) do
                        last[row.dataflow_id] = row.last_commit_id
                    end
                    flow_ids, seen = {}, {}
                    for _, row in ipairs(page) do
                        if row.id ~= last[row.dataflow_id] and not seen[row.dataflow_id] then
                            flow_ids[#flow_ids + 1] = row.dataflow_id
                            seen[row.dataflow_id] = true
                        end
                    end
                end
                local eligible = eligible_flows(query, flow_ids, postgres, cutoff)
                local ids, placeholders = {}, {}
                for _, row in ipairs(page) do
                    if eligible[row.dataflow_id] and row.id ~= last[row.dataflow_id] then
                        ids[#ids + 1] = row.id
                        placeholders[#placeholders + 1] = "?"
                    end
                end
                stats[candidate_field] = stats[candidate_field] + #ids
                if #ids > 0 and not config.dry_run then
                    local result = execute("DELETE FROM " .. table_name .. " WHERE " .. key .. " IN ("
                        .. table.concat(placeholders, ",") .. ")", ids)
                    stats[field] = stats[field] + (tonumber(result.rows_affected) or 0)
                end
                return page
            end)
            if #rows < config.batch_size then return true end
            after = rows[#rows]
            if yield_batch then
                if yield_batch(delay) == false then return false end
            else
                time.sleep(delay)
            end
        end
    end

    local ok, failure = pcall(function()
        if not scan("dataflow_data", "data_id", "type IN " .. DIAGNOSTICS, "idx_diagnostic_data_age", "data", "data_candidates") then return end
        scan("dataflow_commits", "commit_id", "op_id IS NOT NULL", "idx_diagnostic_commit_age", "commits", "commit_candidates")
    end)
    if not ok then return nil, tostring(failure) end
    return stats
end

return M
