local TERMINAL = "'completed','failed','cancelled','terminated'"
local DIAGNOSTICS = "'cycle.state','cycle.function_result','node.input','node.yield','node.yield.result','parallel.progress'"

local function execute(db, query)
    local _, err = db:execute(query)
    if err then error(tostring(err)) end
end

local function install(db, postgres)
    local columns, err
    if not postgres then
        columns, err = db:query("PRAGMA table_info(dataflows)")
        if err then error(tostring(err)) end
    end
    local function column(name, kind)
        if postgres then
            execute(db, "ALTER TABLE dataflows ADD COLUMN IF NOT EXISTS " .. name .. " " .. kind)
        else
            for _, c in ipairs(columns) do if c.name == name then return end end
            execute(db, "ALTER TABLE dataflows ADD COLUMN " .. name .. " " .. kind)
        end
    end
    column("retention_root_id", postgres and "UUID" or "TEXT")
    column("retention_started", "TEXT")
    -- No engine-side evidence proves a legacy owner has finished reading.
    -- Default to held for every preexisting family; new engine inserts opt out.
    column("retention_held", "INTEGER NOT NULL DEFAULT 1")
    -- The only recursive operation is this one-time populated-install backfill.
    -- Cycles/unrooted legacy rows remain NULL and cannot qualify for retention.
    execute(db, [[WITH RECURSIVE family(id, root) AS MATERIALIZED (
        SELECT dataflow_id, dataflow_id FROM dataflows WHERE parent_dataflow_id IS NULL
        UNION ALL SELECT f.dataflow_id, p.root FROM dataflows f JOIN family p
            ON f.parent_dataflow_id = p.id
    ) UPDATE dataflows SET retention_root_id = family.root FROM family
      WHERE family.id = dataflows.dataflow_id AND dataflows.retention_root_id IS NULL]])
    local function age(column_name)
        return postgres and ("EXTRACT(EPOCH FROM " .. column_name .. ")") or ("julianday(" .. column_name .. ")")
    end
    local indexes = {
        { "idx_retention_data_age", "dataflow_data(" .. age("created_at") .. ", data_id) WHERE type IN (" .. DIAGNOSTICS .. ")" },
        { "idx_retention_commit_age", "dataflow_commits(" .. age("created_at") .. ", commit_id) WHERE op_id IS NOT NULL" },
        { "idx_retention_root_age", "dataflows(" .. age("updated_at") .. ", dataflow_id) WHERE parent_dataflow_id IS NULL AND retention_root_id IS NOT NULL" },
        { "idx_retention_family_age", "dataflows(retention_root_id, " .. age("updated_at") .. ")" },
        { "idx_retention_family_active", "dataflows(retention_root_id) WHERE status IS NULL OR status NOT IN (" .. TERMINAL .. ")" },
        { "idx_retention_started", "dataflows(dataflow_id) WHERE retention_started IS NOT NULL" },
        { "idx_retention_flow_children", "dataflows(parent_dataflow_id, dataflow_id)" },
        { "idx_retention_nodes_page", "dataflow_nodes(dataflow_id, node_id)" },
        { "idx_retention_node_children", "dataflow_nodes(parent_node_id, node_id)" },
        { "idx_retention_data_page", "dataflow_data(dataflow_id, data_id)" },
        { "idx_retention_commit_page", "dataflow_commits(dataflow_id, commit_id)" },
    }
    for _, index in ipairs(indexes) do
        execute(db, "CREATE INDEX IF NOT EXISTS " .. index[1] .. " ON " .. index[2])
    end
    if not postgres then
        execute(db, "CREATE INDEX IF NOT EXISTS idx_retention_invalid_age ON dataflows(retention_root_id) WHERE julianday(updated_at) IS NULL")
    end
end

return require("migration").define(function()
    migration("Index and fence bounded family retention", function()
        database("postgres", function() up(function(db) install(db, true) end) end)
        database("sqlite", function() up(function(db) install(db, false) end) end)
    end)
end)
