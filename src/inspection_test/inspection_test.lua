-- SPDX-License-Identifier: Apache-2.0
local test = require("test")
local sql = require("sql")
local json = require("json")
local uuid = require("uuid")
local security = require("security")
local funcs = require("funcs")
local consts = require("consts")
local inspection = require("inspection")
local subject = require("subject")
local fixture = require("fixture")
local dataflow_repo = require("dataflow_repo")

local function yield_row(id, node_id, signal, key)
    return {data_id = id, node_id = node_id, type = consts.DATA_TYPE.NODE_YIELD,
        content = {yield_id = key, yield_context = {wait_for_signal = true, signal_id = signal}}}
end
local function result_row(id, node_id, key)
    return {data_id = id, node_id = node_id, type = consts.DATA_TYPE.NODE_YIELD_RESULT, key = key}
end
local function define_tests()
    test.describe("Dataflow typed inspection", function()
        test.before_each(function() fixture.state = {} end)
        test.it("reads an actual flow and only its active parked node of the requested type", function()
            local flow_id, node_id, yield_id = uuid.v7(), uuid.v7(), uuid.v7()
            local db = assert(sql.get(consts.APP_DB))
            local now = "2026-10-27T00:00:00Z"
            assert(db:execute([[INSERT INTO dataflows
                (dataflow_id,actor_id,type,status,metadata,created_at,updated_at)
                VALUES ($1,'inspection-test','workflow','pending','{}',$2,$2)]], {flow_id, now}))
            assert(db:execute([[INSERT INTO dataflow_nodes
                (node_id,dataflow_id,type,status,config,metadata,created_at,updated_at)
                VALUES ($1,$2,'inspection:wait',$3,'{}','{}',$4,$4)]],
                {node_id, flow_id, consts.STATUS.WAITING, now}))
            assert(db:execute([[INSERT INTO dataflow_data
                (data_id,dataflow_id,node_id,type,content,content_type,metadata,created_at)
                VALUES ($1,$2,$3,$4,$5,$6,'{}',$7)]], {uuid.v7(), flow_id, node_id,
                consts.DATA_TYPE.NODE_YIELD, assert(json.encode({yield_id = yield_id,
                    yield_context = {wait_for_signal = true, signal_id = "signal"}})), consts.CONTENT_TYPE.JSON, now}))
            db:release()
            local flow, flow_err = inspection.flow(flow_id)
            test.is_nil(flow_err)
            test.eq(flow.dataflow_id, flow_id)
            local node, err = inspection.pending_signal(flow_id, "signal", "inspection:wait")
            test.is_nil(err)
            test.eq(node.node_id, node_id)
            for _, args in ipairs({{flow_id, "other", "inspection:wait"}, {flow_id, "signal", "inspection:other"},
                {uuid.v7(), "signal", "inspection:wait"}}) do
                local absent, missing_err = inspection.pending_signal(args[1], args[2], args[3])
                test.is_nil(absent)
                test.is_true(errors.is(missing_err, errors.NOT_FOUND))
                test.eq(missing_err:details().code, "DATAFLOW_SIGNAL_NOT_FOUND")
            end
            db = assert(sql.get(consts.APP_DB))
            assert(db:execute([[INSERT INTO dataflow_data
                (data_id,dataflow_id,node_id,type,key,content,content_type,metadata,created_at)
                VALUES ($1,$2,$3,$4,$5,'{}',$6,'{}',$7)]],
                {uuid.v7(), flow_id, node_id, consts.DATA_TYPE.NODE_YIELD_RESULT, yield_id, consts.CONTENT_TYPE.JSON, now}))
            db:release()
            local resolved, resolved_err = inspection.pending_signal(flow_id, "signal", "inspection:wait")
            test.is_nil(resolved)
            test.is_true(errors.is(resolved_err, errors.NOT_FOUND))
            db = assert(sql.get(consts.APP_DB))
            assert(db:execute("UPDATE dataflow_nodes SET status=$1 WHERE node_id=$2", {consts.STATUS.COMPLETED_SUCCESS, node_id}))
            db:release()
            local terminal, terminal_err = inspection.pending_signal(flow_id, "signal", "inspection:wait")
            test.is_nil(terminal)
            test.is_true(errors.is(terminal_err, errors.NOT_FOUND))
        end)
        test.it("returns typed absence and preserves legacy repository behavior", function()
            local missing_id = uuid.v7()
            local flow, err = inspection.flow(missing_id)
            test.is_nil(flow)
            test.is_true(errors.is(err, errors.NOT_FOUND))
            test.eq(err:details().code, "DATAFLOW_FLOW_NOT_FOUND")
            local legacy, legacy_err = dataflow_repo.get(missing_id)
            test.is_nil(legacy)
            test.eq(legacy_err, "Workflow not found")
            fixture.state.rows = {}
            local absent, absent_err = subject.flow("flow")
            test.is_nil(absent)
            test.is_true(errors.is(absent_err, errors.NOT_FOUND))
        end)
        for _, field in ipairs({"open_error", "query_error"}) do
            for _, case in ipairs({
                {kind = errors.NOT_FOUND, retryable = false, code = "db_missing"},
                {kind = errors.PERMISSION_DENIED, retryable = false, code = "db_denied"},
                {kind = errors.UNAVAILABLE, retryable = true, code = "db_unavailable"},
            }) do
                test.it("preserves " .. field .. " " .. case.code, function()
                    fixture.state[field] = errors.new({message = "fixture database failure", kind = case.kind,
                        retryable = case.retryable, details = {code = case.code, provider = "fixture"}})
                    for _, call in ipairs({function() return subject.flow("flow") end,
                        function() return subject.pending_signal("flow", "signal", "inspection:wait") end}) do
                        local value, err = call()
                        test.is_nil(value)
                        test.is_true(errors.is(err, case.kind))
                        test.eq(err:retryable(), case.retryable)
                        test.eq(err:details().code, case.code)
                        test.eq(err:details().provider, "fixture")
                    end
                    test.eq(fixture.state.released, field == "query_error" and true or nil)
                end)
            end
        end
        test.it("retains the caller's database authority", function()
            local policy = assert(security.policy("userspace.dataflow.inspection_test:probe_policy"))
            local actor = security.new_actor("inspection-denied", {})
            local value, err = funcs.new():with_actor(actor):with_scope(security.new_scope({policy}))
                :call("userspace.dataflow.inspection_test:probe", uuid.v7())
            test.is_nil(value)
            test.is_true(errors.is(err, errors.PERMISSION_DENIED), tostring(err))
            test.eq(err:details().code, "DATAFLOW_FLOW_LOOKUP_FAILED")
        end)
        test.it("does not classify an invalid query response as absence", function()
            local value, err = subject.flow("flow")
            test.is_nil(value)
            test.is_true(errors.is(err, errors.INTERNAL))
            test.eq(err:details().code, "DATAFLOW_READ_RESPONSE_INVALID")
        end)
        for _, metadata in ipairs({'42', '[1]', '{broken', false}) do
            test.it("rejects malformed stored metadata " .. tostring(metadata), function()
                fixture.state.rows = {{metadata = metadata}}
                local value, err = subject.flow("flow")
                test.is_nil(value)
                test.is_true(errors.is(err, errors.INTERNAL))
                test.eq(err:details().code, "DATAFLOW_FLOW_METADATA_INVALID")
            end)
        end
        test.it("preserves legacy empty metadata encodings", function()
            for _, metadata in ipairs({'', 'null', '[]', '{}'}) do
                fixture.state.rows = {{metadata = metadata}}
                local flow, err = subject.flow("flow")
                test.is_nil(err)
                test.eq(next(flow.metadata), nil)
            end
        end)
        test.it("uses only the latest episode and its own durable result", function()
            local old = yield_row("01", "node", "signal", "old")
            local current = yield_row("03", "node", "signal", "current")
            fixture.state.rows = {current, result_row("02", "node", "old"), old}
            local pending, err = subject.pending_signal("flow", "signal", "inspection:wait")
            test.is_nil(err)
            test.eq(pending.yield_id, "current")
            fixture.state.rows = {result_row("04", "node", "current"), current, old}
            local closed, closed_err = subject.pending_signal("flow", "signal", "inspection:wait")
            test.is_nil(closed)
            test.is_true(errors.is(closed_err, errors.NOT_FOUND))
        end)
        test.it("rejects malformed wait state rather than confirming absence", function()
            for _, content in ipairs({'{broken', '{}', '42', {yield_id = "id", yield_context = {signal_id = "signal"}},
                {yield_id = "id", yield_context = {wait_for_signal = true, signal_id = false}}}) do
                fixture.state.rows = {{node_id = "node", type = consts.DATA_TYPE.NODE_YIELD, content = content}}
                local node, err = subject.pending_signal("flow", "signal", "inspection:wait")
                test.is_nil(node)
                test.is_true(errors.is(err, errors.INTERNAL))
                test.eq(err:details().code, "DATAFLOW_SIGNAL_STATE_INVALID")
            end
        end)
        test.it("validates identifiers before touching storage", function()
            for _, args in ipairs({{}, {"flow"}, {"flow", "signal"}, {"flow", "signal", false}}) do
                local node, err = (subject :: any).pending_signal(args[1], args[2], args[3])
                test.is_nil(node)
                test.is_true(errors.is(err, errors.INVALID))
            end
            test.eq(fixture.state.released, nil)
        end)
    end)
end
return {run_tests = test.run_cases(define_tests)}
