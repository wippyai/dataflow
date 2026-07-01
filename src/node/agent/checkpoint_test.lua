local checkpoint = require("checkpoint")

local function define_tests()
    describe("dataflow agent checkpoint", function()
        it("uses stable defaults and option overrides", function()
            local defaults = checkpoint.config_from_args({})
            test.eq(defaults.model, "class:fast")
            test.eq(defaults.max_tokens, 2000)

            local cfg = checkpoint.config_from_args({
                options = {
                    model = "class:large",
                    max_tokens = 1234,
                    max_row_chars = 12,
                }
            })

            test.eq(cfg.model, "class:large")
            test.eq(cfg.max_tokens, 1234)
            test.eq(cfg.max_row_chars, 12)
        end)

        it("formats bounded history rows for the summarizer", function()
            local text = checkpoint.history_text({
                {
                    data_id = "d1",
                    type = "agent.action",
                    content = "abcdefghijklmnopqrstuvwxyz",
                    metadata = {
                        iteration = 3,
                        tool_name = "search"
                    }
                }
            }, 8)

            test.contains(text, "agent.action")
            test.contains(text, "abcdefgh...")
            test.contains(text, "metadata:")
            test.contains(text, "search")
        end)
    end)
end

return { run_tests = test.run_cases(define_tests) }
