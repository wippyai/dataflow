local registry = require("registry")
local test = require("test")

local ROOT_GROUP = "userspace.dataflow.security:root"

local function list(value)
    if type(value) == "table" then return value end
    return { value }
end

local function assert_values(actual, expected)
    local values = list(actual)
    test.eq(#values, #expected)
    for index, value in ipairs(expected) do test.eq(tostring(values[index]), value) end
end

local function assert_policy(name, resources, actions)
    local entry, err = registry.get("userspace.dataflow.security:" .. name)
    test.is_nil(err)
    local found = test.not_nil(entry) :: any
    local data = test.not_nil(found.data) :: any
    assert_values(data.groups, { ROOT_GROUP })
    local policy = test.not_nil(data.policy) :: any
    assert_values(policy.resources, resources)
    assert_values(policy.actions, actions)
    for _, action in ipairs(list(policy.actions)) do
        test.neq(tostring(action), "*", name .. " must never grant wildcard actions")
    end
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
    test.eq(tostring(groups[1]), ROOT_GROUP)

    local dependencies = entry.data.lifecycle.requires or entry.data.lifecycle.depends_on or {}
    test.eq(#dependencies, 2)
    test.eq(tostring(dependencies[1]), "app:processes")
    test.eq(tostring(dependencies[2]), "app:db")

    assert_policy("root.database", { "app:db" }, { "db.get" })
    assert_policy("root.orchestrator_spawn", {
        "userspace.dataflow.runner:orchestrator",
    }, { "process.spawn", "process.spawn.monitored" })
    assert_policy("root.process_host", { "app:processes" }, { "process.host" })
    assert_policy("root.actor_reconstruction", { "*" }, { "security.actor.create" })
    assert_policy("root.policy_reconstruction", { "*" }, {
        "security.policy.get",
        "security.policy_group.get",
    })
    assert_policy("root.scope_reconstruction", { "custom" }, { "security.scope.create" })
    assert_policy("root.process_context", { "context" }, { "process.context" })
    assert_policy("root.process_security", { "security" }, { "process.security" })
    assert_policy("root.process_delivery", { "*" }, {
        "process.send",
        "process.monitor",
        "process.unmonitor",
    })
    assert_policy("root.service_name", { "dataflow.wakes", "dataflow.overseer" }, {
        "process.registry.register",
    })

    local legacy = registry.get("userspace.dataflow.security:root.policy")
    test.is_nil(legacy)
    return true
end

return { run = run }
