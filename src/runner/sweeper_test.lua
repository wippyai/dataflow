local test = require("test")
local sweeper = require("sweeper")
local consts = require("consts")

local function define_tests()
    describe("Revival Sweeper", function()
        local original_process: any
        local original_repo: any
        local original_client: any
        local original_env: any

        before_each(function()
            original_process = sweeper.process
            original_repo = sweeper.dataflow_repo
            original_client = sweeper.client
            original_env = sweeper.env
        end)

        after_each(function()
            sweeper.process = original_process
            sweeper.dataflow_repo = original_repo
            sweeper.client = original_client
            sweeper.env = original_env
        end)

        describe("resolve_interval_seconds", function()
            it("returns the default when env is unset", function()
                sweeper.env = {
                    get = function(_id: string): (any, any)
                        return nil, nil
                    end
                }

                test.eq(sweeper.resolve_interval_seconds(), 120)
            end)

            it("parses a configured value", function()
                sweeper.env = {
                    get = function(_id: string): (any, any)
                        return "120", nil
                    end
                }

                test.eq(sweeper.resolve_interval_seconds(), 120)
            end)

            it("clamps below the minimum", function()
                sweeper.env = {
                    get = function(_id: string): (any, any)
                        return "1", nil
                    end
                }

                test.eq(sweeper.resolve_interval_seconds(), 5)
            end)

            it("clamps above the maximum", function()
                sweeper.env = {
                    get = function(_id: string): (any, any)
                        return "99999", nil
                    end
                }

                test.eq(sweeper.resolve_interval_seconds(), 3600)
            end)

            it("falls back to default on non-numeric input", function()
                sweeper.env = {
                    get = function(_id: string): (any, any)
                        return "not-a-number", nil
                    end
                }

                test.eq(sweeper.resolve_interval_seconds(), 120)
            end)
        end)

        describe("sweep_once", function()
            local lookups: { string } = {}
            local revived: { string } = {}

            local function install(active: { any }, live: { [string]: boolean })
                lookups = {}
                revived = {}

                sweeper.dataflow_repo = {
                    list_non_terminal = function(_limit: number): (any, any)
                        return active, nil
                    end
                }

                sweeper.process = {
                    registry = {
                        lookup = function(name: string): any
                            table.insert(lookups, name)
                            local id = name:gsub("^dataflow%.", "")
                            if live[id] then
                                return "pid-" .. id
                            end
                            return nil
                        end
                    }
                }

                sweeper.client = {
                    new = function(): (any, any)
                        return {
                            revive = function(_self: any, dataflow_id: string): (any, any, any)
                                table.insert(revived, dataflow_id)
                                return "pid-" .. dataflow_id, nil, { spawned = true }
                            end
                        }, nil
                    end
                }
            end

            it("respawns dead orchestrators for non-terminal dataflows", function()
                install({ { dataflow_id = "a" }, { dataflow_id = "b" } }, {})

                local count = sweeper.sweep_once()

                test.eq(count, 2)
                test.eq(#revived, 2)
                test.eq(revived[1], "a")
                test.eq(revived[2], "b")
            end)

            it("skips dataflows that already have a live process", function()
                install({ { dataflow_id = "a" }, { dataflow_id = "b" } }, { a = true })

                local count = sweeper.sweep_once()

                test.eq(count, 1)
                test.eq(#revived, 1)
                test.eq(revived[1], "b")
            end)

            it("returns zero when there are no non-terminal dataflows", function()
                install({}, {})

                local count = sweeper.sweep_once()

                test.eq(count, 0)
                test.eq(#revived, 0)
            end)

            it("returns zero and does not throw when listing fails", function()
                sweeper.dataflow_repo = {
                    list_non_terminal = function(_limit: number): (any, any)
                        return nil, "database unavailable"
                    end
                }
                sweeper.client = {
                    new = function(): (any, any)
                        error("client should not be built when listing fails")
                    end
                }

                local count = sweeper.sweep_once()

                test.eq(count, 0)
            end)
        end)
    end)
end

return test.run_cases(define_tests)
