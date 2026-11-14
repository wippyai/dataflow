local time = require("time")

local M = {}
local TERMINAL = "'completed','failed','cancelled','terminated'"
local DIAGNOSTICS = "'cycle.state','cycle.function_result','node.input','node.yield','node.yield.result','parallel.progress'"

function M.validate(options)
    options = options or {}
    local days = tonumber(options.days or 0)
    local batch = tonumber(options.batch_size or 500)
    if not days or days < 0 or days > 36500 or days % 1 ~= 0 then
        return nil, "retention days must be an integer from 0 (disabled) to 36500"
    end
    local flow_days = tonumber(options.flow_days)
    if options.flow_days == nil or options.flow_days == "" then
        flow_days = days == 0 and 0 or math.max(30, days)
    end
    if not flow_days or (flow_days ~= 0 and flow_days < 30) or flow_days > 36500 or flow_days % 1 ~= 0 then
        return nil, "flow retention days must be 0 (disabled) or an integer from 30 to 36500"
    end
    if not batch or batch < 1 or batch > 1000 or batch % 1 ~= 0 then
        return nil, "retention batch_size must be an integer from 1 to 1000"
    end
    return { days = days, flow_days = flow_days, batch_size = batch, dry_run = options.dry_run == true }
end

local function rebind(query, postgres)
    if not postgres then return query end
    local i = 0
    return (query:gsub("%?", function() i = i + 1; return "$" .. i end))
end

-- A pass drains all pages visible before its fixed cutoff. Cursors live only for
-- this pass; protected candidates advance the cursor too, so they cannot hide
-- later work. Every SQL selection/deletion is bounded; no recursive runtime SQL.
function M.run(db, options, yield_batch)
    local config, err = M.validate(options)
    if err then return nil, err end
    local stats: { [string]: any } = { enabled = config.days > 0 or config.flow_days > 0, days = config.days,
        flow_days = config.flow_days, data = 0, commits = 0, flows = 0, nodes = 0,
        activations = 0, wakes = 0, families = 0, data_candidates = 0, commit_candidates = 0,
        dry_run = config.dry_run, transactions = 0 }
    if not stats.enabled then return stats end
    local dialect, type_err = db:type()
    if type_err then return nil, tostring(type_err) end
    local postgres = tostring(dialect) == "postgres"
    if not postgres and tostring(dialect) ~= "sqlite" then return nil, "unsupported retention database" end
    local function age(column_name)
        return postgres and ("EXTRACT(EPOCH FROM " .. column_name .. ")") or ("julianday(" .. column_name .. ")")
    end
    local function cutoff(days)
        return time.now():add(-days * 24 * time.HOUR):format(time.RFC3339NANO)
    end
    local diagnostic_cutoff, flow_cutoff = cutoff(config.days), cutoff(config.flow_days)
    local cutoff_expr = postgres and "EXTRACT(EPOCH FROM CAST(? AS timestamp))" or "julianday(?)"
    local function call(executor, method, query, params)
        local result, call_err = executor[method](executor, rebind(query, postgres), params or {})
        if call_err then error(tostring(call_err)) end
        return result
    end
    local function transaction(fn: any): any
        local tx, begin_err = db:begin()
        if begin_err then error(tostring(begin_err)) end
        local function query(q, p) return call(tx, "query", q, p) end
        local function execute(q, p) return call(tx, "execute", q, p) end
        local ok, result = pcall(function()
            if postgres then
                execute("SET LOCAL lock_timeout = '2s'")
                execute("SET LOCAL statement_timeout = '30s'")
            else
                execute("UPDATE dataflows SET status = status WHERE 1 = 0")
            end
            return fn(query, execute)
        end)
        if not ok then tx:rollback(); error(tostring(result)) end
        local _, commit_err = tx:commit()
        if commit_err then tx:rollback(); error(tostring(commit_err)) end
        stats.transactions = stats.transactions + 1
        -- The service supplies a cancellation-aware yield. Library callers also
        -- yield between transactions rather than monopolizing the SQL pool.
        if yield_batch then
            if yield_batch() == false then error("retention cancelled") end
        else
            time.sleep("1ms")
        end
        return result
    end
    local function remove(execute, table_name, key, rows, field, flow)
        if #rows == 0 or config.dry_run then return end
        local marks, ids = {}, {}
        for _, row in ipairs(rows) do marks[#marks + 1] = "?"; ids[#ids + 1] = row[key] end
        local scope = ""
        if flow then scope = " AND dataflow_id = ?"; ids[#ids + 1] = flow end
        local result = execute("DELETE FROM " .. table_name .. " WHERE " .. key ..
            " IN (" .. table.concat(marks, ",") .. ")" .. scope, ids)
        stats[field] = stats[field] + (tonumber(result.rows_affected) or 0)
    end
    local function indexed(table_name, index)
        return table_name .. (postgres and "" or (" INDEXED BY " .. index))
    end
    local function candidate_page(query, table_name, key, column_name, predicate, index, before, cursor, extra)
        local value, rows = age(column_name), {}
        local function select_page(condition, params, limit, tied)
            params[#params + 1] = before
            params[#params + 1] = limit
            return query("SELECT " .. key .. (extra or "") .. ", " .. value .. " AS age FROM " ..
                indexed(table_name, index) .. " WHERE " .. predicate .. " AND " .. condition ..
                value .. " < " .. cutoff_expr .. " ORDER BY " .. (tied and key or (value .. ", " .. key)) .. " LIMIT ?", params)
        end
        if cursor then
            -- SQLite cannot seek a row-value comparison over an expression
            -- index. Split tied ages from later ages: both are true index seeks,
            -- even when millions of records share the same timestamp.
            rows = select_page(value .. " = ? AND " .. key .. " > ? AND ",
                { cursor.age, cursor[key] }, config.batch_size, true)
            if #rows == config.batch_size then return rows end
        end
        local later = select_page(cursor and (value .. " > ? AND ") or "",
            cursor and { cursor.age } or {}, config.batch_size - #rows)
        for _, row in ipairs(later) do rows[#rows + 1] = row end
        return rows
    end
    local function safe(query, root, before)
        if not root then return false end
        if #query("SELECT dataflow_id FROM " .. indexed("dataflows", "idx_retention_family_active") .. " WHERE retention_root_id = ? AND " ..
            "(status IS NULL OR status NOT IN (" .. TERMINAL .. ")) LIMIT 1", { root }) > 0 then return false end
        if not postgres and #query("SELECT dataflow_id FROM " .. indexed("dataflows", "idx_retention_invalid_age") ..
            " WHERE retention_root_id = ? AND julianday(updated_at) IS NULL LIMIT 1", { root }) > 0 then return false end
        return #query("SELECT dataflow_id FROM " .. indexed("dataflows", "idx_retention_family_age") .. " WHERE retention_root_id = ? AND " ..
            age("updated_at") .. " >= " .. cutoff_expr .. " LIMIT 1", { root, before }) == 0
    end
    local lock = postgres and " FOR UPDATE" or ""
    local function root_row(query, root)
        return query("SELECT dataflow_id, retention_started, retention_held FROM dataflows WHERE dataflow_id = ?" .. lock, { root })[1]
    end
    local function diagnostics(table_name, key, predicate, field, candidate_field)
        local cursor
        while true do
            local rows = transaction(function(query, execute)
                local index = field == "data" and "idx_retention_data_age" or "idx_retention_commit_age"
                local page = candidate_page(query, table_name, key, "created_at", predicate,
                    index, diagnostic_cutoff, cursor, ", dataflow_id")
                local candidates, roots, flows = {}, {}, {}
                for _, row in ipairs(page) do
                    local f = query("SELECT retention_root_id, last_commit_id FROM dataflows WHERE dataflow_id = ?", { row.dataflow_id })[1]
                    flows[row.dataflow_id] = f
                    if f and f.retention_root_id then roots[f.retention_root_id] = true end
                end
                local ordered = {}
                for root in pairs(roots) do ordered[#ordered + 1] = root end
                table.sort(ordered)
                for _, root in ipairs(ordered) do
                    local f = root_row(query, root)
                    roots[root] = f and not f.retention_started and safe(query, root, diagnostic_cutoff) or false
                end
                for _, row in ipairs(page) do
                    local f = flows[row.dataflow_id]
                    -- Read last_commit_id after the root lock, just like eligibility.
                    if f and roots[f.retention_root_id] and field == "commits" then
                        f = query("SELECT retention_root_id, last_commit_id FROM dataflows WHERE dataflow_id = ?", { row.dataflow_id })[1]
                    end
                    if f and roots[f.retention_root_id] and (field ~= "commits" or row[key] ~= f.last_commit_id) then
                        candidates[#candidates + 1] = row
                    end
                end
                stats[candidate_field] = stats[candidate_field] + #candidates
                remove(execute, table_name, key, candidates, field)
                return page
            end)
            if #rows < config.batch_size then return end
            cursor = rows[#rows]
        end
    end
    local function drain_rows(table_name, key, flow, field)
        while true do
            local n = transaction(function(query, execute)
                local rows = query("SELECT " .. key .. " FROM " .. table_name ..
                    " WHERE dataflow_id = ? ORDER BY " .. key .. " LIMIT ?", { flow, config.batch_size })
                remove(execute, table_name, key, rows, field, flow)
                return #rows
            end)
            if n < config.batch_size then return end
        end
    end
    -- Delete nodes/flows bottom-up, after their payload pages are empty. A
    -- parent is removed only after an indexed child probe returns empty; FK
    -- cascades therefore have no unbounded descendants or payload left to visit.
    local function drain_nodes(flow)
        while true do
            local nodes = call(db, "query", "SELECT node_id FROM dataflow_nodes WHERE dataflow_id = ? ORDER BY node_id LIMIT 1", { flow })
            if #nodes == 0 then return end
            local stack = { nodes[1].node_id }
            while #stack > 0 do
                local id = stack[#stack]
                local deleted = transaction(function(query, execute)
                    local child = query("SELECT node_id, dataflow_id FROM dataflow_nodes WHERE parent_node_id = ? ORDER BY node_id LIMIT 1", { id })[1]
                    if child then
                        if child.dataflow_id ~= flow then error("cross-flow node parent prevents bounded retention") end
                        stack[#stack + 1] = child.node_id
                        return false
                    end
                    if #query("SELECT data_id FROM dataflow_data WHERE node_id = ? LIMIT 1", { id }) > 0 then
                        error("cross-flow node data prevents bounded retention")
                    end
                    remove(execute, "dataflow_nodes", "node_id", { { node_id = id } }, "nodes")
                    return true
                end)
                if deleted then stack[#stack] = nil end
            end
        end
    end
    local function drain_family(root)
        local stack = { root }
        while #stack > 0 do
            local flow = stack[#stack]
            local child = transaction(function(query)
                return query("SELECT dataflow_id FROM dataflows WHERE parent_dataflow_id = ? ORDER BY dataflow_id LIMIT 1", { flow })[1]
            end)
            if child then
                stack[#stack + 1] = child.dataflow_id
            else
                drain_rows("dataflow_data", "data_id", flow, "data")
                drain_rows("dataflow_commits", "commit_id", flow, "commits")
                drain_rows("dataflow_wakes", "wake_key", flow, "wakes")
                drain_rows("dataflow_activations", "dataflow_id", flow, "activations")
                drain_nodes(flow)
                transaction(function(_, execute)
                    remove(execute, "dataflows", "dataflow_id", { { dataflow_id = flow } }, "flows")
                end)
                stack[#stack] = nil
            end
        end
        stats.families = stats.families + 1
    end
    local ok, failure = pcall(function()
        if config.days > 0 then
            diagnostics("dataflow_data", "data_id", "type IN (" .. DIAGNOSTICS .. ")", "data", "data_candidates")
            diagnostics("dataflow_commits", "commit_id", "op_id IS NOT NULL", "commits", "commit_candidates")
        end
        if config.flow_days == 0 then return end
        if not config.dry_run then
            while true do
                local pending = call(db, "query", "SELECT dataflow_id FROM dataflows WHERE retention_started IS NOT NULL ORDER BY dataflow_id LIMIT 1")
                if #pending == 0 then break end
                drain_family(pending[1].dataflow_id)
            end
        end
        local cursor
        while true do
            local page = transaction(function(query)
                return candidate_page(query, "dataflows", "dataflow_id", "updated_at",
                    "parent_dataflow_id IS NULL AND retention_root_id IS NOT NULL",
                    "idx_retention_root_age", flow_cutoff, cursor)
            end)
            for _, row in ipairs(page) do
                local claimed = transaction(function(query, execute)
                    local f = root_row(query, row.dataflow_id)
                    if not f or f.retention_started or tonumber(f.retention_held) ~= 0 or not safe(query, row.dataflow_id, flow_cutoff) then return false end
                    if config.dry_run then return false end
                    execute("UPDATE dataflows SET retention_started = ? WHERE dataflow_id = ?", {
                        time.now():format(time.RFC3339NANO), row.dataflow_id })
                    return true
                end)
                if claimed then drain_family(row.dataflow_id) end
            end
            if #page < config.batch_size then break end
            cursor = page[#page]
        end
    end)
    if not ok then return nil, tostring(failure) end
    return stats
end

return M
