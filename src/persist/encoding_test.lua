local test = require("test")
local encoding = require("encoding")

local function define_tests()
    describe("persist encoding boundary", function()
        it("passes valid UTF-8 through untouched", function()
            local samples = {
                "plain ascii",
                "кириллица и ümlaut",
                "emoji \240\159\154\128 and CJK \228\184\173\230\150\135",
                "",
            }
            for _, s in ipairs(samples) do
                test.eq(encoding.ensure_utf8(s), s)
            end
        end)

        it("replaces invalid bytes with U+FFFD instead of failing", function()
            local dirty = "scraped \255\254 bytes \192\128 inside"
            local clean = encoding.ensure_utf8(dirty)
            test.is_true(clean:find("\255", 1, true) == nil)
            test.is_true(clean:find("\192", 1, true) == nil)
            test.is_true(clean:find("scraped ", 1, true) ~= nil)
            test.is_true(clean:find(" inside", 1, true) ~= nil)
            test.is_true(clean:find("\239\191\189", 1, true) ~= nil)
        end)

        it("replaces truncated multi-byte sequences at end of string", function()
            local truncated = "tail \226\130" -- first two bytes of a 3-byte sequence
            local clean = encoding.ensure_utf8(truncated)
            test.eq(clean, "tail \239\191\189\239\191\189")
        end)

        it("rejects UTF-16 surrogates encoded as UTF-8", function()
            local surrogate = "x\237\160\128y" -- U+D800
            local clean = encoding.ensure_utf8(surrogate)
            test.is_true(clean:find("\237", 1, true) == nil)
            test.eq(clean:sub(1, 1), "x")
            test.eq(clean:sub(-1), "y")
        end)

        it("leaves non-strings alone", function()
            test.eq(encoding.ensure_utf8(nil), nil)
            test.eq(encoding.ensure_utf8(42), 42)
        end)
    end)
end

return { run_tests = test.run_cases(define_tests) }
