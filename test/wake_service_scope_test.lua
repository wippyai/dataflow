local registry = require("registry")
local test = require("test")

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
    return true
end

return { run = run }
