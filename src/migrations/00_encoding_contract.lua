-- The engine stores text; a Postgres database that is not UTF8 rejects every
-- non-ASCII byte at write time, poisoning whole commit batches long after
-- install. The contract is asserted first and loudly: a misprovisioned
-- database refuses to migrate. SQLite stores byte sequences verbatim.

return require("migration").define(function()
    migration("database encoding is UTF8", function()
        database("postgres", function()
            up(function(db)
                local rows = db:query(
                    "SELECT pg_encoding_to_char(encoding) AS enc FROM pg_database WHERE datname = current_database()")
                local enc = type(rows) == "table" and rows[1] and tostring(rows[1].enc) or "unknown"
                if enc ~= "UTF8" then
                    error("database encoding is " .. enc .. "; dataflow requires UTF8 — " ..
                        "recreate the database with ENCODING 'UTF8' (TEMPLATE template0) before installing")
                end
            end)
            down(function(_db) end)
        end)

        database("sqlite", function()
            up(function(_db) end)
            down(function(_db) end)
        end)
    end)
end)
