local registry = require("registry")
local test = require("test")

local function assert_missing(id)
    local entry = registry.get(id)
    test.is_nil(entry)
end

local function run()
    local entry, err = registry.get("userspace.dataflow.runner:wake_process.service")
    test.is_nil(err)
    test.not_nil(entry)
    test.not_nil(entry.data)
    test.not_nil(entry.data.lifecycle)
    test.not_nil(entry.data.lifecycle.security)

    local groups = entry.data.lifecycle.security.groups or {}
    test.eq(#groups, 1)
    test.eq(tostring(groups[1]), "userspace.dataflow.security:root")

    local dependencies = entry.data.lifecycle.requires or entry.data.lifecycle.depends_on or {}
    test.eq(#dependencies, 2)
    test.eq(tostring(dependencies[1]), "app:processes")
    test.eq(tostring(dependencies[2]), "app:db")

    assert_missing("userspace.dataflow.security:runtime_process_spawn")
    assert_missing("userspace.dataflow.security:runtime_process_registry")
    return true
end

return { run = run }
