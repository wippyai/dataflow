local sql = require("sql")
local json = require("json")

local APP_DB = "app:db"
local REFERENCE_CONTENT_TYPE = "dataflow/reference"

local data_reader = {}
local methods = {}
local reader_mt = { __index = methods }

function methods:_copy()
    local new_instance = {}
    for k, v in pairs(self) do
        new_instance[k] = v
    end
    return setmetatable(new_instance, reader_mt)
end

local function normalize_args(args, arg_count)
    if arg_count == 1 and type(args[1]) == "table" and not getmetatable(args[1]) then
        return args[1]
    else
        return args
    end
end

function data_reader.with_dataflow(dataflow_id)
    if not dataflow_id or dataflow_id == "" then
        error("Workflow ID is required")
    end

    local instance = {
        _dataflow_id = dataflow_id,
        _node_ids = nil,
        _data_ids = nil,
        _data_types = nil,
        _data_keys = nil,
        _data_discriminators = nil,
        _fetch_content = true,
        _fetch_metadata = true,
        _resolve_references = true,
        _replace_references = false,
        _order_by_field = nil,
        _order_by_direction = nil,
    }
    return setmetatable(instance, reader_mt)
end

function methods:with_nodes(...)
    local args = { ... }
    local count = select("#", ...)
    local new_instance = self:_copy()
    new_instance._node_ids = normalize_args(args, count)
    return new_instance
end

function methods:with_data(...)
    local args = { ... }
    local count = select("#", ...)
    local new_instance = self:_copy()
    new_instance._data_ids = normalize_args(args, count)
    return new_instance
end

function methods:with_data_types(...)
    local args = { ... }
    local count = select("#", ...)
    local new_instance = self:_copy()
    new_instance._data_types = normalize_args(args, count)
    return new_instance
end

function methods:with_data_keys(...)
    local args = { ... }
    local count = select("#", ...)
    local new_instance = self:_copy()
    new_instance._data_keys = normalize_args(args, count)
    return new_instance
end

function methods:with_data_discriminators(...)
    local args = { ... }
    local count = select("#", ...)
    local new_instance = self:_copy()
    new_instance._data_discriminators = normalize_args(args, count)
    return new_instance
end

function methods:fetch_options(options)
    if not options or type(options) ~= "table" then
        return self
    end

    local new_instance = self:_copy()

    if options.content ~= nil then
        new_instance._fetch_content = options.content
    end

    if options.metadata ~= nil then
        new_instance._fetch_metadata = options.metadata
    end

    if options.resolve_references ~= nil then
        new_instance._resolve_references = options.resolve_references
    end

    if options.replace_references ~= nil then
        new_instance._replace_references = options.replace_references
    end

    return new_instance
end

function methods:order_by(field, direction)
    local new_instance = self:_copy()
    new_instance._order_by_field = "d." .. field
    new_instance._order_by_direction = direction or "ASC"
    return new_instance
end

local function create_in_clause(field, values)
    if not values or #values == 0 then
        return nil
    end

    if #values == 1 then
        return { field .. " = ?", values[1] }
    end

    local placeholders = {}
    for i = 1, #values do
        table.insert(placeholders, "?")
    end

    return { field .. " IN (" .. table.concat(placeholders, ", ") .. ")", unpack(values) }
end

function methods:_build_query(db_type)
    local select_fields = { "d.data_id", "d.dataflow_id", "d.node_id", "d.type", "d.discriminator", "d.key",
        "d.created_at" }

    if self._fetch_content then
        table.insert(select_fields, "d.content")
        table.insert(select_fields, "d.content_type")
    end

    if self._fetch_metadata then
        table.insert(select_fields, "d.metadata")
    end

    if self._resolve_references then
        table.insert(select_fields, "ref.content as ref_content")
        table.insert(select_fields, "ref.content_type as ref_content_type")
        table.insert(select_fields, "ref.type as ref_type")
        table.insert(select_fields, "ref.discriminator as ref_discriminator")
        table.insert(select_fields, "ref.key as ref_key")
        if self._fetch_metadata then
            table.insert(select_fields, "ref.metadata as ref_metadata")
        end
    end

    local query_builder = sql.builder.select(unpack(select_fields))
        :from("dataflow_data d")

    if self._resolve_references then
        query_builder = query_builder:left_join(
            "dataflow_data ref ON d.key = " ..
            (sql.type.POSTGRES == db_type and "ref.data_id::text" or "ref.data_id") ..
            " AND d.dataflow_id = ref.dataflow_id AND d.content_type = '" ..
            REFERENCE_CONTENT_TYPE .. "'"
        )
    end

    query_builder = query_builder:where("d.dataflow_id = ?", self._dataflow_id)

    if self._node_ids and #self._node_ids > 0 then
        local node_clause = create_in_clause("d.node_id", self._node_ids)
        if node_clause then
            query_builder = query_builder:where(sql.builder.expr(unpack(node_clause)))
        end
    end

    if self._data_ids and #self._data_ids > 0 then
        local id_clause = create_in_clause("d.data_id", self._data_ids)
        if id_clause then
            query_builder = query_builder:where(sql.builder.expr(unpack(id_clause)))
        end
    end

    if self._data_types and #self._data_types > 0 then
        local type_clause = create_in_clause("d.type", self._data_types)
        if type_clause then
            query_builder = query_builder:where(sql.builder.expr(unpack(type_clause)))
        end
    end

    if self._data_keys and #self._data_keys > 0 then
        local key_clause = create_in_clause("d.key", self._data_keys)
        if key_clause then
            query_builder = query_builder:where(sql.builder.expr(unpack(key_clause)))
        end
    end

    if self._data_discriminators and #self._data_discriminators > 0 then
        local disc_clause = create_in_clause("d.discriminator", self._data_discriminators)
        if disc_clause then
            query_builder = query_builder:where(sql.builder.expr(unpack(disc_clause)))
        end
    end

    if self._order_by_field then
        local direction = self._order_by_direction or "ASC"
        query_builder = query_builder:order_by(self._order_by_field .. " " .. direction)
    else
        query_builder = query_builder:order_by("d.created_at ASC")
    end

    return query_builder
end

local function parse_json_metadata(metadata_str)
    if not metadata_str or type(metadata_str) ~= "string" then
        return {}
    end

    local success, parsed = pcall(json.decode, metadata_str)
    if success then
        return parsed
    else
        return {}
    end
end

local function parse_metadata(rows)
    for i, row in ipairs(rows) do
        if row.metadata then
            row.metadata = parse_json_metadata(row.metadata)
        else
            row.metadata = {}
        end

        if row.ref_metadata then
            row.ref_metadata = parse_json_metadata(row.ref_metadata)
        end
    end
    return rows
end

local function process_rows(rows, replace_references)
    if not replace_references then
        return rows
    end

    for i, row in ipairs(rows) do
        if row.content_type == REFERENCE_CONTENT_TYPE and row.key ~= nil then
            row.data_id = row.key
            row.content = row.ref_content
            row.content_type = row.ref_content_type

            if row.ref_key then
                row.key = row.ref_key
            end

            row.ref_content = nil
            row.ref_content_type = nil
            row.ref_discriminator = nil
            row.ref_key = nil
        end
    end

    return rows
end

local function get_db()
    local db, err = sql.get(APP_DB)
    if err then
        error("Failed to connect to database: " .. err)
    end
    return db
end

function methods:all()
    local db = get_db()
    local query_builder = self:_build_query(db:type())

    local executor = query_builder:run_with(db)
    local results, err = executor:query()
    db:release()

    if err then
        error("Failed to fetch data: " .. err)
    end

    if self._fetch_metadata then
        results = parse_metadata(results)
    end

    if self._resolve_references and self._replace_references then
        results = process_rows(results, self._replace_references)
    end

    return results
end

function methods:one()
    local db = get_db()
    local query_builder = self:_build_query(db:type()):limit(1)

    local executor = query_builder:run_with(db)
    local results, err = executor:query()
    db:release()

    if err then
        error("Failed to fetch data: " .. err)
    end

    if #results == 0 then
        return nil
    end

    if self._fetch_metadata then
        results = parse_metadata(results)
    end

    if self._resolve_references and self._replace_references then
        results = process_rows(results, self._replace_references)
    end

    return results[1]
end

function methods:count()
    local query_builder = sql.builder.select("COUNT(*) as count")
        :from("dataflow_data d")
        :where("d.dataflow_id = ?", self._dataflow_id)

    if self._node_ids and #self._node_ids > 0 then
        local node_clause = create_in_clause("d.node_id", self._node_ids)
        if node_clause then
            query_builder = query_builder:where(sql.builder.expr(unpack(node_clause)))
        end
    end

    if self._data_ids and #self._data_ids > 0 then
        local id_clause = create_in_clause("d.data_id", self._data_ids)
        if id_clause then
            query_builder = query_builder:where(sql.builder.expr(unpack(id_clause)))
        end
    end

    if self._data_types and #self._data_types > 0 then
        local type_clause = create_in_clause("d.type", self._data_types)
        if type_clause then
            query_builder = query_builder:where(sql.builder.expr(unpack(type_clause)))
        end
    end

    if self._data_keys and #self._data_keys > 0 then
        local key_clause = create_in_clause("d.key", self._data_keys)
        if key_clause then
            query_builder = query_builder:where(sql.builder.expr(unpack(key_clause)))
        end
    end

    if self._data_discriminators and #self._data_discriminators > 0 then
        local disc_clause = create_in_clause("d.discriminator", self._data_discriminators)
        if disc_clause then
            query_builder = query_builder:where(sql.builder.expr(unpack(disc_clause)))
        end
    end

    local db = get_db()
    local executor = query_builder:run_with(db)
    local results, err = executor:query()
    db:release()

    if err then
        error("Failed to count data: " .. err)
    end

    return results[1].count
end

function methods:exists()
    return self:count() > 0
end

return data_reader
