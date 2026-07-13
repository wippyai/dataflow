local test = require("test")
local uuid = require("uuid")
local json = require("json")
local time = require("time")
local client = require("client")
local consts = require("consts")
local data_reader = require("data_reader")
local sql = require("sql")

local WAIT = "3s"

local function define_tests()
    describe("Signal Edge Cases", function()
        local c

        before_all(function()
            c = client.new()
            test.not_nil(c, "client created")
        end)

        local function make_signal_wf(signal_id, extra_config)
            local nid = uuid.v7()
            local did = uuid.v7()
            extra_config = extra_config or {}
            return c:create_workflow({
                { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                    node_id = nid,
                    node_type = "userspace.dataflow.node.signal:node",
                    status = consts.STATUS.PENDING,
                    config = {
                        signal_id = signal_id,
                        timeout = extra_config.timeout,
                        data_targets = extra_config.data_targets or {
                            { data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "result", content_type = consts.CONTENT_TYPE.JSON }
                        },
                    },
                    metadata = { title = "Signal: " .. (signal_id or "auto") },
                }},
                { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                    data_id = did,
                    data_type = consts.DATA_TYPE.NODE_INPUT,
                    node_id = nid,
                    content = extra_config.input or { task = "test" },
                    content_type = consts.CONTENT_TYPE.JSON,
                    key = "default",
                }},
            })
        end

        local function wait_complete(df_id, timeout_ms)
            timeout_ms = timeout_ms or 5000
            local iterations = math.ceil(timeout_ms / 200)
            for _ = 1, iterations do
                local status = c:get_status(df_id)
                if status == consts.STATUS.COMPLETED_SUCCESS then return true end
                if status == consts.STATUS.COMPLETED_FAILURE then return false end
                time.sleep("200ms")
            end
            return false
        end

        local function kill_orchestrator(df_id)
            local pid = process.registry.lookup("dataflow." .. df_id)
            if pid then
                process.terminate(pid)
                time.sleep("200ms")
            end
            return pid
        end

        local function timeout_branch_targets()
            return {
                {
                    data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                    key = "timeout",
                    content_type = consts.CONTENT_TYPE.JSON,
                    condition = "output.timeout",
                },
                {
                    data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                    key = "signal",
                    content_type = consts.CONTENT_TYPE.JSON,
                    condition = "output.approved == true",
                },
            }
        end

        -- ==========================================
        -- SIGNAL DATA CONTENT TYPES
        -- ==========================================

        describe("signal data content types", function()
            it("handles boolean true", function()
                local sid = "bool-true-" .. uuid.v7()
                local df_id = make_signal_wf(sid)
                c:start(df_id)
                time.sleep("300ms")
                c:signal(df_id, sid, { approved = true })
                test.is_true(wait_complete(df_id), "boolean true")
            end)

            it("handles boolean false in data", function()
                local sid = "bool-false-" .. uuid.v7()
                local df_id = make_signal_wf(sid)
                c:start(df_id)
                time.sleep("300ms")
                c:signal(df_id, sid, { approved = false })
                test.is_true(wait_complete(df_id), "boolean false")
            end)

            it("handles numeric values", function()
                local sid = "nums-" .. uuid.v7()
                local df_id = make_signal_wf(sid)
                c:start(df_id)
                time.sleep("300ms")
                c:signal(df_id, sid, { score = 42, pi = 3.14159, negative = -100 })
                test.is_true(wait_complete(df_id), "numeric values")
            end)

            it("handles string values", function()
                local sid = "str-" .. uuid.v7()
                local df_id = make_signal_wf(sid)
                c:start(df_id)
                time.sleep("300ms")
                c:signal(df_id, sid, { message = "hello world", empty = "" })
                test.is_true(wait_complete(df_id), "string values")
            end)

            it("handles nested objects", function()
                local sid = "nested-" .. uuid.v7()
                local df_id = make_signal_wf(sid)
                c:start(df_id)
                time.sleep("300ms")
                c:signal(df_id, sid, {
                    user = {
                        name = "alice",
                        roles = { "admin", "reviewer" },
                        settings = { theme = "dark", notifications = true }
                    }
                })
                test.is_true(wait_complete(df_id), "nested objects")
            end)

            it("handles arrays", function()
                local sid = "arr-" .. uuid.v7()
                local df_id = make_signal_wf(sid)
                c:start(df_id)
                time.sleep("300ms")
                c:signal(df_id, sid, { items = { 1, 2, 3, "four", { five = 5 } } })
                test.is_true(wait_complete(df_id), "arrays")
            end)

            it("handles empty object", function()
                local sid = "empty-obj-" .. uuid.v7()
                local df_id = make_signal_wf(sid)
                c:start(df_id)
                time.sleep("300ms")
                c:signal(df_id, sid, {})
                test.is_true(wait_complete(df_id), "empty object")
            end)

            it("handles large payload", function()
                local sid = "large-" .. uuid.v7()
                local df_id = make_signal_wf(sid)
                c:start(df_id)
                time.sleep("300ms")
                local large_data = {}
                for i = 1, 100 do
                    large_data["key_" .. i] = string.rep("x", 100)
                end
                c:signal(df_id, sid, large_data)
                test.is_true(wait_complete(df_id), "large payload")
            end)
        end)

        -- ==========================================
        -- SIGNAL ID EDGE CASES
        -- ==========================================

        describe("signal_id edge cases", function()
            it("long signal_id works", function()
                local sid = string.rep("a", 200)
                local df_id = make_signal_wf(sid)
                c:start(df_id)
                time.sleep("300ms")
                c:signal(df_id, sid, { ok = true })
                test.is_true(wait_complete(df_id), "long signal_id")
            end)

            it("signal_id with special characters works", function()
                local sid = "approval:user/alice@domain.com#review"
                local df_id = make_signal_wf(sid)
                c:start(df_id)
                time.sleep("300ms")
                c:signal(df_id, sid, { ok = true })
                test.is_true(wait_complete(df_id), "special chars in signal_id")
            end)

            it("signal_id with unicode works", function()
                local sid = "signal-test-" .. uuid.v7()
                local df_id = make_signal_wf(sid)
                c:start(df_id)
                time.sleep("300ms")
                c:signal(df_id, sid, { ok = true })
                test.is_true(wait_complete(df_id), "unicode signal_id")
            end)

            it("two workflows with same signal_id are independent", function()
                local sid = "shared-" .. uuid.v7()
                local df1 = make_signal_wf(sid)
                local df2 = make_signal_wf(sid)
                c:start(df1)
                c:start(df2)
                time.sleep("300ms")

                c:signal(df1, sid, { wf = 1 })
                time.sleep("500ms")
                test.eq(c:get_status(df1), consts.STATUS.COMPLETED_SUCCESS, "wf1 completed")
                test.eq(c:get_status(df2), consts.STATUS.WAITING, "wf2 still waiting")

                c:signal(df2, sid, { wf = 2 })
                test.is_true(wait_complete(df2), "wf2 completed independently")
            end)
        end)

        -- ==========================================
        -- DUPLICATE / MULTIPLE SIGNALS
        -- ==========================================

        describe("duplicate signals", function()
            it("first signal satisfies, second is ignored", function()
                local sid = "dup-" .. uuid.v7()
                local df_id = make_signal_wf(sid)
                c:start(df_id)
                time.sleep("300ms")

                c:signal(df_id, sid, { version = 1 })
                c:signal(df_id, sid, { version = 2 })
                test.is_true(wait_complete(df_id), "completes despite duplicate")
                test.eq(c:get_status(df_id), consts.STATUS.COMPLETED_SUCCESS, "success")
            end)

            it("rapid-fire signals do not cause errors", function()
                local sid = "rapid-" .. uuid.v7()
                local df_id = make_signal_wf(sid)
                c:start(df_id)
                time.sleep("300ms")

                for i = 1, 5 do
                    c:signal(df_id, sid, { burst = i })
                end
                test.is_true(wait_complete(df_id), "handles rapid signals")
            end)
        end)

        -- ==========================================
        -- SIGNAL TIMING
        -- ==========================================

        describe("signal timing", function()
            it("signal sent before workflow reaches signal node (pre-queued)", function()
                local sid = "preq-" .. uuid.v7()
                local df_id = make_signal_wf(sid)

                -- signal before start
                c:signal(df_id, sid, { early = true })
                time.sleep("100ms")
                c:start(df_id)

                test.is_true(wait_complete(df_id), "pre-queued signal processed")
            end)

            it("signal sent exactly at start", function()
                local sid = "exact-" .. uuid.v7()
                local df_id = make_signal_wf(sid)

                c:start(df_id)
                c:signal(df_id, sid, { immediate = true })

                test.is_true(wait_complete(df_id), "immediate signal works")
            end)

            it("delayed signal after long wait", function()
                local sid = "delayed-" .. uuid.v7()
                local df_id = make_signal_wf(sid)
                c:start(df_id)
                time.sleep("300ms")
                test.eq(c:get_status(df_id), consts.STATUS.WAITING, "still waiting")

                time.sleep("1s")
                test.eq(c:get_status(df_id), consts.STATUS.WAITING, "still waiting after 1s")

                c:signal(df_id, sid, { finally = true })
                test.is_true(wait_complete(df_id), "completed after delay")
            end)
        end)

        -- ==========================================
        -- WRONG SIGNAL SCENARIOS
        -- ==========================================

        describe("wrong signal scenarios", function()
            it("wrong signal_id does not satisfy", function()
                local sid = "correct-" .. uuid.v7()
                local df_id = make_signal_wf(sid)
                c:start(df_id)
                time.sleep("300ms")

                c:signal(df_id, "wrong-" .. uuid.v7(), { nope = true })
                time.sleep("500ms")
                test.eq(c:get_status(df_id), consts.STATUS.WAITING, "not satisfied by wrong id")

                c:signal(df_id, sid, { correct = true })
                test.is_true(wait_complete(df_id), "satisfied by correct id")
            end)

            it("multiple wrong signals followed by correct one", function()
                local sid = "multi-wrong-" .. uuid.v7()
                local df_id = make_signal_wf(sid)
                c:start(df_id)
                time.sleep("300ms")

                for i = 1, 5 do
                    c:signal(df_id, "wrong-" .. i .. "-" .. uuid.v7(), { attempt = i })
                end
                time.sleep("500ms")
                test.eq(c:get_status(df_id), consts.STATUS.WAITING, "still waiting after 5 wrong signals")

                c:signal(df_id, sid, { correct = true })
                test.is_true(wait_complete(df_id), "correct signal works after wrong ones")
            end)

            it("wrong signals become quiescent after durable passivation", function()
                local sid = "quiescent-" .. uuid.v7()
                local df_id = make_signal_wf(sid)
                c:start(df_id)
                time.sleep("300ms")
                c:signal(df_id, "wrong-" .. sid, { timeout_deadline = "2099-01-01T00:00:00Z" })
                time.sleep("1200ms")

                test.eq(c:get_status(df_id), consts.STATUS.WAITING, "wrong signal leaves wait parked")
                local db = test.not_nil(select(1, sql.get("app:db"))) :: any
                local rows, query_err = db:query(
                    "SELECT wake_key FROM dataflow_wakes WHERE dataflow_id = ? AND wake_key LIKE 'signal:%'",
                    { df_id })
                db:release()
                test.is_nil(query_err)
                test.eq(#(rows or {}), 0, "applied wrong signal has no retry wake")

                c:signal(df_id, sid, { approved = true })
                test.is_true(wait_complete(df_id), "correct signal still completes")
            end)
        end)

        -- ==========================================
        -- CLIENT VALIDATION
        -- ==========================================

        describe("client validation", function()
            it("signal rejects nil dataflow_id", function()
                local _, err = c:signal(nil, "sig", {})
                test.not_nil(err, "nil dataflow_id rejected")
            end)

            it("signal rejects empty string dataflow_id", function()
                local _, err = c:signal("", "sig", {})
                test.not_nil(err, "empty dataflow_id rejected")
            end)

            it("signal rejects nil signal_id", function()
                local _, err = c:signal("df-1", nil, {})
                test.not_nil(err, "nil signal_id rejected")
            end)

            it("signal rejects empty string signal_id", function()
                local _, err = c:signal("df-1", "", {})
                test.not_nil(err, "empty signal_id rejected")
            end)
        end)

        -- ==========================================
        -- SIGNAL AFTER WORKFLOW TERMINAL STATE
        -- ==========================================

        describe("signal after completion", function()
            it("signal to completed workflow is refused with a structured terminal error", function()
                local sid = "post-complete-" .. uuid.v7()
                local df_id = make_signal_wf(sid)
                c:start(df_id)
                time.sleep("300ms")

                c:signal(df_id, sid, { first = true })
                test.is_true(wait_complete(df_id), "completed")

                local result, err = c:signal(df_id, sid, { second = true })
                test.is_nil(result, "no result for a finished run")
                test.not_nil(err, "structured refusal returned")
                test.contains(tostring(err), "terminal state", "refusal names terminality")
                test.contains(tostring(err), consts.STATUS.COMPLETED_SUCCESS, "refusal carries the terminal status")

                test.eq(c:get_status(df_id), consts.STATUS.COMPLETED_SUCCESS, "status unchanged")
            end)
        end)

        -- ==========================================
        -- CONCURRENT WORKFLOW OPERATIONS
        -- ==========================================

        describe("concurrent operations", function()
            it("start and signal at same time", function()
                local sid = "concurrent-" .. uuid.v7()
                local df_id = make_signal_wf(sid)

                -- fire both at once
                c:start(df_id)
                c:signal(df_id, sid, { concurrent = true })

                test.is_true(wait_complete(df_id, 6000), "handles concurrent start + signal")
            end)

            it("two workflows started simultaneously with signals", function()
                local sid1 = "sim1-" .. uuid.v7()
                local sid2 = "sim2-" .. uuid.v7()
                local df1 = make_signal_wf(sid1)
                local df2 = make_signal_wf(sid2)

                c:start(df1)
                c:start(df2)
                time.sleep("300ms")

                c:signal(df1, sid1, { wf = 1 })
                c:signal(df2, sid2, { wf = 2 })

                test.is_true(wait_complete(df1), "wf1 completed")
                test.is_true(wait_complete(df2), "wf2 completed")
            end)

            it("five workflows with signals in random order", function()
                local count = 5
                local sids = {}
                local df_ids = {}

                for i = 1, count do
                    local sid = "batch-" .. i .. "-" .. uuid.v7()
                    sids[i] = sid
                    df_ids[i] = make_signal_wf(sid)
                    c:start(df_ids[i])
                end

                time.sleep("500ms")
                for i = 1, count do
                    test.eq(c:get_status(df_ids[i]), consts.STATUS.WAITING, "wf" .. i .. " waiting")
                end

                -- signal in reverse
                for i = count, 1, -1 do
                    c:signal(df_ids[i], sids[i], { index = i })
                end

                for i = 1, count do
                    test.is_true(wait_complete(df_ids[i]), "wf" .. i .. " completed")
                end
            end)
        end)

        describe("signal timeout", function()
            it("fires timeout branch when signal does not arrive", function()
                local sid = "timeout-" .. uuid.v7()
                local df_id = make_signal_wf(sid, {
                    timeout = "400ms",
                    data_targets = timeout_branch_targets(),
                })

                c:start(df_id)
                test.is_true(wait_complete(df_id, 4000), "workflow completes via timeout")

                local output, output_err = c:output(df_id)
                test.is_nil(output_err, "output read succeeds")
                test.not_nil(output.timeout, "timeout output exists")
                test.eq(output.timeout.timeout, true, "timeout flag routed")
                test.eq(output.timeout.code, "SIGNAL_TIMEOUT", "timeout code routed")
                test.is_nil(output.signal, "signal branch not routed")
            end)

            it("signal before deadline cancels timeout branch", function()
                local sid = "signal-before-timeout-" .. uuid.v7()
                local df_id = make_signal_wf(sid, {
                    timeout = "2s",
                    data_targets = timeout_branch_targets(),
                })

                c:start(df_id)
                time.sleep("200ms")
                c:signal(df_id, sid, { approved = true })
                test.is_true(wait_complete(df_id, 4000), "workflow completes from signal")

                local output, output_err = c:output(df_id)
                test.is_nil(output_err, "output read succeeds")
                test.not_nil(output.signal, "signal output exists")
                test.eq(output.signal.approved, true, "signal data routed")
                test.is_nil(output.timeout, "timeout branch not routed")
            end)

            it("timeout deadline survives orchestrator restart", function()
                local sid = "timeout-restart-" .. uuid.v7()
                local df_id = make_signal_wf(sid, {
                    timeout = "800ms",
                    data_targets = timeout_branch_targets(),
                })

                c:start(df_id)
                time.sleep("200ms")
                test.is_nil(process.registry.lookup("dataflow." .. df_id), "parked wait has no resident orchestrator")
                c:start(df_id)

                test.is_true(wait_complete(df_id, 5000), "workflow completes via persisted timeout after restart")
                local output, output_err = c:output(df_id)
                test.is_nil(output_err, "output read succeeds")
                test.not_nil(output.timeout, "timeout output exists after restart")
                test.eq(output.timeout.timeout, true, "timeout flag routed after restart")
            end)
        end)
    end)
end

return { run_tests = test.run_cases(define_tests) }
