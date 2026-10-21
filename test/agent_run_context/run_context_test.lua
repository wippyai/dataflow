local contract = require("contract")
local json = require("json")
local sql = require("sql")
local test = require("test")
local time = require("time")
local uuid = require("uuid")
local agent_consts = require("agent_consts")
local consts = require("consts")
local wait_for_boot = require("wait_for_boot")

local CONTRACT_ID = "wippy.agent:run_context"
local BINDING_ID = "userspace.dataflow.node.agent.run_context:binding"

local function get_db()
    local db, err = sql.get("app:db")
    if err then
        error("db: " .. tostring(err))
    end
    return db
end

local function insert(tx, table_name, values)
    local stmt = sql.builder.insert(table_name):set_map(values):run_with(tx)
    local _, err = stmt:exec()
    if err then
        error("insert " .. table_name .. ": " .. tostring(err))
    end
end

local function open_binding()
    local def, def_err = contract.get(CONTRACT_ID)
    test.is_nil(def_err, "contract.get: " .. tostring(def_err))
    test.not_nil(def)

    local instance, open_err = def:open(BINDING_ID)
    test.is_nil(open_err, "contract.open: " .. tostring(open_err))
    test.not_nil(instance)
    return instance
end

local function define_tests()
    test.describe("dataflow agent run_context binding", function()
        local dataflow_id
        local node_id
        local action_id
        local observation_id
        local memory_id

        test.before_all(function()
            wait_for_boot.run()

            local db = get_db()
            local tx, tx_err = db:begin()
            if tx_err then
                db:release()
                error("begin: " .. tostring(tx_err))
            end

            local now = time.now():format(time.RFC3339)
            dataflow_id = uuid.v7()
            node_id = uuid.v7()
            action_id = uuid.v7()
            observation_id = uuid.v7()
            memory_id = uuid.v7()

            insert(tx, "dataflows", {
                dataflow_id = dataflow_id,
                actor_id = "run-context-test",
                type = "run_context_test",
                status = "active",
                metadata = "{}",
                created_at = now,
                updated_at = now
            })

            insert(tx, "dataflow_nodes", {
                node_id = node_id,
                dataflow_id = dataflow_id,
                type = "userspace.dataflow.node.agent:node",
                status = "active",
                config = json.encode({
                    arena = {
                        prompt = "Run context system prompt",
                        context = {
                            arena_value = "arena"
                        }
                    }
                }),
                metadata = json.encode({
                    session_context = {
                        session_value = "session"
                    }
                }),
                created_at = now,
                updated_at = now
            })

            insert(tx, "dataflow_data", {
                data_id = action_id,
                dataflow_id = dataflow_id,
                node_id = node_id,
                type = agent_consts.DATA_TYPE.AGENT_ACTION,
                key = "1_action",
                content = json.encode({
                    result = "assistant text from action"
                }),
                content_type = consts.CONTENT_TYPE.JSON,
                metadata = json.encode({
                    iteration = 1
                }),
                created_at = now
            })

            insert(tx, "dataflow_data", {
                data_id = observation_id,
                dataflow_id = dataflow_id,
                node_id = node_id,
                type = agent_consts.DATA_TYPE.AGENT_OBSERVATION,
                key = "1_observation",
                content = "developer observation",
                content_type = consts.CONTENT_TYPE.TEXT,
                metadata = "{}",
                created_at = now
            })

            insert(tx, "dataflow_data", {
                data_id = memory_id,
                dataflow_id = dataflow_id,
                node_id = node_id,
                type = agent_consts.DATA_TYPE.AGENT_MEMORY,
                key = "1_memory",
                content = "memory text",
                content_type = consts.CONTENT_TYPE.TEXT,
                metadata = "{}",
                created_at = now
            })

            local ok, commit_err = tx:commit()
            if not ok or commit_err then
                tx:rollback()
                db:release()
                error("commit: " .. tostring(commit_err))
            end
            db:release()
        end)

        test.after_all(function()
            if not dataflow_id then
                return
            end
            local db = get_db()
            db:execute("DELETE FROM dataflow_data WHERE dataflow_id = ?", { dataflow_id })
            db:execute("DELETE FROM dataflow_nodes WHERE dataflow_id = ?", { dataflow_id })
            db:execute("DELETE FROM dataflows WHERE dataflow_id = ?", { dataflow_id })
            db:release()
        end)

        test.it("reads context through contract dispatch", function()
            local result, err = open_binding():get_context({
                host = {
                    kind = "dataflow",
                    dataflow_id = dataflow_id,
                    node_id = node_id
                },
                agent = {
                    id = "agent:test",
                    model = "model:test"
                }
            })

            test.is_nil(err, tostring(err))
            test.eq(result.context.dataflow_id, dataflow_id)
            test.eq(result.context.node_id, node_id)
            test.eq(result.context.arena_value, "arena")
            test.eq(result.context.session_value, "session")
            test.eq(result.agent.id, "agent:test")
        end)

        test.it("reads a bounded history slice through contract dispatch", function()
            local result, err = open_binding():get_history({
                host = {
                    kind = "dataflow",
                    dataflow_id = dataflow_id,
                    node_id = node_id
                },
                selector = {
                    mode = "window",
                    last = 2
                }
            })

            test.is_nil(err, tostring(err))
            test.eq(#result.events, 2)
            test.eq(result.events[1].id, observation_id)
            test.eq(result.events[2].id, memory_id)
            test.eq(result.range.from_id, observation_id)
            test.eq(result.range.to_id, memory_id)
        end)

        test.it("reads all history in chronological order", function()
            local result, err = open_binding():get_history({
                host = {
                    kind = "dataflow",
                    dataflow_id = dataflow_id,
                    node_id = node_id
                },
                selector = { mode = "all" }
            })

            test.is_nil(err, tostring(err))
            test.eq(#result.events, 3)
            test.eq(result.events[1].id, action_id)
            test.eq(result.events[2].id, observation_id)
            test.eq(result.events[3].id, memory_id)
            test.eq(result.range.from_id, action_id)
            test.eq(result.range.to_id, memory_id)
        end)

        test.it("uses the latest checkpoint cut by default", function()
            local result, err = open_binding():get_history({
                host = {
                    kind = "dataflow",
                    dataflow_id = dataflow_id,
                    node_id = node_id
                }
            })

            test.is_nil(err, tostring(err))
            test.eq(#result.events, 3)
            test.eq(result.events[1].id, action_id)
            test.eq(result.events[3].id, memory_id)
        end)

        test.it("uses a default window size when last is omitted", function()
            local result, err = open_binding():get_history({
                host = {
                    kind = "dataflow",
                    dataflow_id = dataflow_id,
                    node_id = node_id
                },
                selector = { mode = "window" }
            })

            test.is_nil(err, tostring(err))
            test.eq(#result.events, 3)
            test.eq(result.events[1].id, action_id)
            test.eq(result.events[3].id, memory_id)
        end)

        test.it("rejects non-positive window sizes", function()
            local result, err = open_binding():get_history({
                host = {
                    kind = "dataflow",
                    dataflow_id = dataflow_id,
                    node_id = node_id
                },
                selector = { mode = "window", last = 0 }
            })

            test.is_nil(result)
            test.contains(tostring(err), "selector.last must be positive")
        end)

        test.it("rejects non-numeric window sizes", function()
            local result, err = open_binding():get_history({
                host = {
                    kind = "dataflow",
                    dataflow_id = dataflow_id,
                    node_id = node_id
                },
                selector = { mode = "window", last = "later" }
            })

            test.is_nil(result)
            test.contains(tostring(err), "selector.last must be positive")
        end)

        test.it("requires from_id for since_id", function()
            local result, err = open_binding():get_history({
                host = {
                    kind = "dataflow",
                    dataflow_id = dataflow_id,
                    node_id = node_id
                },
                selector = { mode = "since_id" }
            })

            test.is_nil(result)
            test.contains(tostring(err), "selector.from_id is required")
        end)

        test.it("treats since_id as exclusive", function()
            local result, err = open_binding():get_history({
                host = {
                    kind = "dataflow",
                    dataflow_id = dataflow_id,
                    node_id = node_id
                },
                selector = {
                    mode = "since_id",
                    from_id = action_id
                }
            })

            test.is_nil(err, tostring(err))
            test.eq(#result.events, 2)
            test.eq(result.events[1].id, observation_id)
            test.eq(result.events[2].id, memory_id)
            test.eq(result.range.from_id, observation_id)
            test.eq(result.range.to_id, memory_id)
        end)

        test.it("treats range without from_id as beginning of history", function()
            local result, err = open_binding():get_history({
                host = {
                    kind = "dataflow",
                    dataflow_id = dataflow_id,
                    node_id = node_id
                },
                selector = {
                    mode = "range",
                    to_id = observation_id
                }
            })

            test.is_nil(err, tostring(err))
            test.eq(#result.events, 2)
            test.eq(result.events[1].id, action_id)
            test.eq(result.events[2].id, observation_id)
            test.eq(result.range.from_id, action_id)
            test.eq(result.range.to_id, observation_id)
        end)

        test.it("treats range as exclusive from_id and inclusive to_id", function()
            local result, err = open_binding():get_history({
                host = {
                    kind = "dataflow",
                    dataflow_id = dataflow_id,
                    node_id = node_id
                },
                selector = {
                    mode = "range",
                    from_id = action_id,
                    to_id = observation_id
                }
            })

            test.is_nil(err, tostring(err))
            test.eq(#result.events, 1)
            test.eq(result.events[1].id, observation_id)
            test.eq(result.range.from_id, observation_id)
            test.eq(result.range.to_id, observation_id)
        end)

        test.it("returns an empty range when from_id and to_id are the same", function()
            local result, err = open_binding():get_history({
                host = {
                    kind = "dataflow",
                    dataflow_id = dataflow_id,
                    node_id = node_id
                },
                selector = {
                    mode = "range",
                    from_id = action_id,
                    to_id = action_id
                }
            })

            test.is_nil(err, tostring(err))
            test.eq(#result.events, 0)
            test.is_nil(result.range.from_id)
            test.is_nil(result.range.to_id)
        end)

        test.it("treats range without to_id as open ended", function()
            local result, err = open_binding():get_history({
                host = {
                    kind = "dataflow",
                    dataflow_id = dataflow_id,
                    node_id = node_id
                },
                selector = {
                    mode = "range",
                    from_id = action_id
                }
            })

            test.is_nil(err, tostring(err))
            test.eq(#result.events, 2)
            test.eq(result.events[1].id, observation_id)
            test.eq(result.events[2].id, memory_id)
        end)

        test.it("requires checkpoint_id for explicit checkpoint mode", function()
            local result, err = open_binding():get_history({
                host = {
                    kind = "dataflow",
                    dataflow_id = dataflow_id,
                    node_id = node_id
                },
                selector = { mode = "checkpoint" }
            })

            test.is_nil(result)
            test.contains(tostring(err), "selector.checkpoint_id is required")
        end)

        test.it("treats explicit checkpoints as exclusive anchors", function()
            local result, err = open_binding():get_history({
                host = {
                    kind = "dataflow",
                    dataflow_id = dataflow_id,
                    node_id = node_id
                },
                selector = {
                    mode = "checkpoint",
                    checkpoint_id = action_id
                }
            })

            test.is_nil(err, tostring(err))
            test.eq(#result.events, 2)
            test.eq(result.events[1].id, observation_id)
            test.eq(result.events[2].id, memory_id)
            test.eq(result.range.checkpoint_id, action_id)
        end)

        test.it("rejects unknown selector modes", function()
            local result, err = open_binding():get_history({
                host = {
                    kind = "dataflow",
                    dataflow_id = dataflow_id,
                    node_id = node_id
                },
                selector = { mode = "laterish" }
            })

            test.is_nil(result)
            test.contains(tostring(err), "unknown selector mode")
        end)

        test.it("ignores root max_chars for history slices", function()
            local result, err = open_binding():get_history({
                host = {
                    kind = "dataflow",
                    dataflow_id = dataflow_id,
                    node_id = node_id
                },
                selector = { mode = "all" },
                max_chars = 1
            })

            test.is_nil(err, tostring(err))
            test.eq(#result.events, 3)
            test.is_false(result.truncated)
        end)

        test.it("applies max_chars after slicing and reports truncation", function()
            local result, err = open_binding():get_history({
                host = {
                    kind = "dataflow",
                    dataflow_id = dataflow_id,
                    node_id = node_id
                },
                selector = {
                    mode = "all",
                    max_chars = 1
                }
            })

            test.is_nil(err, tostring(err))
            test.eq(#result.events, 0)
            test.is_true(result.truncated)
            test.is_nil(result.range.from_id)
            test.is_nil(result.range.to_id)
        end)

        test.it("builds prompt text through contract dispatch", function()
            local result, err = open_binding():get_prompt({
                host = {
                    kind = "dataflow",
                    dataflow_id = dataflow_id,
                    node_id = node_id
                },
                selector = {
                    mode = "all"
                },
                format = "text"
            })

            test.is_nil(err, tostring(err))
            test.is_string(result.text)
            test.contains(result.text, "Run context system prompt")
            test.contains(result.text, "assistant text from action")
            test.contains(result.text, "developer observation")
            test.contains(result.text, "memory text")
        end)

        test.it("applies selector max_chars to prompt slices", function()
            local result, err = open_binding():get_prompt({
                host = {
                    kind = "dataflow",
                    dataflow_id = dataflow_id,
                    node_id = node_id
                },
                selector = {
                    mode = "all",
                    max_chars = 1
                },
                format = "text"
            })

            test.is_nil(err, tostring(err))
            test.is_true(result.truncated)
            test.is_nil(result.range.from_id)
            test.is_nil(result.range.to_id)
            test.contains(result.text, "Run context system prompt")
            test.is_true(string.find(result.text, "assistant text from action", 1, true) == nil)
        end)

        test.it("builds prompt from the selected slice only", function()
            local result, err = open_binding():get_prompt({
                host = {
                    kind = "dataflow",
                    dataflow_id = dataflow_id,
                    node_id = node_id
                },
                selector = {
                    mode = "window",
                    last = 1
                },
                format = "text"
            })

            test.is_nil(err, tostring(err))
            test.contains(result.text, "Run context system prompt")
            test.contains(result.text, "memory text")
            test.is_true(string.find(result.text, "assistant text from action", 1, true) == nil)
            test.is_true(string.find(result.text, "developer observation", 1, true) == nil)
            test.is_nil(result.messages)
        end)
    end)
end

return { run_tests = test.run_cases(define_tests) }
