local test = require("test")
local agent_ref = require("agent_ref")

local function define_tests()
    describe("agent_ref", function()
        after_each(function()
            agent_ref._resolver = nil
            agent_ref._registry = nil
        end)

        it("uses wippy.agent:resolver display metadata first", function()
            agent_ref._resolver = {
                resolve = function(_self, args)
                    return { id = args.agent_id, title = "Resolved title", name = "Resolved name" }
                end
            }
            agent_ref._registry = {
                get = function(_) return { meta = { title = "Registry title" } } end
            }

            local resolved = agent_ref.resolve("user_agent:abc")
            test.eq(resolved.title, "Resolved title")
            test.eq(resolved.name, "Resolved name")
        end)

        it("falls back to registry metadata when the resolver misses", function()
            agent_ref._resolver = { resolve = function(_) return nil end }
            agent_ref._registry = {
                get = function(_) return { id = "system.agent", meta = { name = "System agent" } } end
            }

            local resolved = agent_ref.resolve("system.agent")
            test.eq(resolved.title, "System agent")
            test.eq(resolved.name, "System agent")
        end)

        it("does not leak user-agent UUIDs when no resolver is available", function()
            agent_ref._resolver = { resolve = function(_) return nil end }
            agent_ref._registry = { get = function(_) return nil end }

            local resolved = agent_ref.resolve("user_agent:019eccf3-81ae-7e91-9bf8-750d4a685492")
            test.eq(resolved.title, "Custom agent")
            test.eq(resolved.name, "Custom agent")
        end)

        it("keeps unknown namespaced refs readable", function()
            agent_ref._resolver = { resolve = function(_) return nil end }
            agent_ref._registry = { get = function(_) return nil end }

            local resolved = agent_ref.resolve("vendor.package:agent_name")
            test.eq(resolved.title, "agent_name")
        end)
    end)
end

return test.run_cases(define_tests)
