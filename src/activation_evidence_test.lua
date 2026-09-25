local test = require("test")
local sql = require("sql")
local uuid = require("uuid")
local time = require("time")
local activation_repo = require("activation_repo")
local consts = require("dataflow_consts")
local client = require("client")

local function now()
    return time.now():format(time.RFC3339NANO)
end

local function with_tx(fn)
    local db, db_err = sql.get("app:db")
    test.is_nil(db_err)
    local tx, begin_err = db:begin()
    test.is_nil(begin_err)
    local value, err = fn(tx)
    if err then tx:rollback() else
        local committed, commit_err = tx:commit()
        test.is_true(committed)
        test.is_nil(commit_err)
    end
    db:release()
    return value, err
end

local function create_flow()
    local id = uuid.v7()
    local db, db_err = sql.get("app:db")
    test.is_nil(db_err)
    local _, insert_err = sql.builder.insert("dataflows"):set_map({
        dataflow_id = id, actor_id = "activation-evidence-test", type = "test",
        status = "pending", metadata = "{}", created_at = now(), updated_at = now(),
    }):run_with(db):exec()
    db:release()
    test.is_nil(insert_err)
    return id
end

local function delete_flow(id)
    local db, db_err = sql.get("app:db")
    test.is_nil(db_err)
    local _, delete_err = sql.builder.delete("dataflows"):
        where("dataflow_id = ?", id):run_with(db):exec()
    db:release()
    test.is_nil(delete_err)
end

local function define_tests()
    test.describe("Dataflow admission evidence", function()
        test.it("exposes absent and durable activation through the public client", function()
            local actor_id = "activation-evidence-test"
            local api, new_err = client.new({
                security = {
                    actor = function() return { id = function() return actor_id end } end,
                    scope = function() return {} end,
                },
                dataflow_repo = {
                    get_by_user = function(id, owner)
                        if owner ~= actor_id then return nil, "access denied" end
                        return { dataflow_id = id, actor_id = owner,
                            actor_context = "test-context", status = consts.STATUS.PENDING }, nil
                    end,
                },
                activation_repo = activation_repo,
                commit = { notify_activation = function() return true, nil end },
            })
            test.is_nil(new_err)
            local missing_id = uuid.v7()
            local missing, missing_err = api:get_activation_evidence(missing_id, "run:missing")
            test.is_nil(missing_err)
            test.eq(missing.state, "absent")
            local absent_activation, absent_err = api:ensure_activation(missing_id, "run:missing")
            test.is_nil(absent_err)
            test.eq(absent_activation.state, "absent")
            local id = create_flow()
            local key = "run:" .. uuid.v7()
            local created, created_err = api:get_activation_evidence(id, key)
            test.is_nil(created_err)
            test.eq(created.state, "created")
            local first, first_err = api:ensure_activation(id, key)
            test.is_nil(first_err)
            test.eq(first.state, "activated")
            local second, second_err = api:ensure_activation(id, key)
            test.is_nil(second_err)
            test.eq(second.generation, first.generation)
            delete_flow(id)
        end)

        test.it("ensures the same admission without advancing generation and rejects another key", function()
            local id = create_flow()
            local key = "run:" .. uuid.v7()
            local first, first_err = activation_repo.ensure_activation(id, key, now())
            test.is_nil(first_err)
            test.eq(first.generation, 1)
            local second, second_err = activation_repo.ensure_activation(id, key, now())
            test.is_nil(second_err)
            test.eq(second.generation, first.generation)
            local other, conflict = activation_repo.ensure_activation(id, key .. ":other", now())
            test.is_nil(other)
            test.contains(conflict, "CONFLICT")
            local wrong_read, read_conflict = activation_repo.get_activation_evidence(
                id, key .. ":other")
            test.is_nil(wrong_read)
            test.contains(read_conflict, "CONFLICT")
            local evidence, evidence_err = activation_repo.get_activation_evidence(id, key)
            test.is_nil(evidence_err)
            test.eq(evidence.generation, 1)
            test.is_true(evidence.ever_activated)
            delete_flow(id)
        end)

        test.it("retains terminal status and outcome until matching acknowledgment", function()
            local id = create_flow()
            local key = "run:" .. uuid.v7()
            local activation, activation_err = activation_repo.ensure_activation(id, key, now())
            test.is_nil(activation_err)
            local terminal, terminal_err = with_tx(function(tx)
                local db_type, type_err = tx:db_type()
                if type_err then return nil, type_err end
                local query = "UPDATE dataflows SET status = ?, metadata = ? WHERE dataflow_id = ?"
                if db_type == "postgres" then query = "UPDATE dataflows SET status = $1, metadata = $2 WHERE dataflow_id = $3" end
                local _, err = tx:execute(query, { consts.STATUS.COMPLETED_SUCCESS, '{"value":42}', id })
                if err then return nil, err end
                return activation_repo.disable_terminal_tx(tx, id, now())
            end)
            test.is_nil(terminal_err)
            test.is_true(terminal.terminal)
            local evidence, evidence_err = activation_repo.get_activation_evidence(id, key)
            test.is_nil(evidence_err)
            test.eq(evidence.state, "terminal")
            test.eq(evidence.terminal_generation, activation.generation)
            test.eq(evidence.terminal_outcome.value, 42)
            test.is_nil(evidence.terminal_ack_at)
            local wrong, wrong_err = activation_repo.ack_terminal(id, key, activation.generation + 1, now())
            test.is_nil(wrong)
            test.contains(wrong_err, "CONFLICT")
            local wrong_key, wrong_key_err = activation_repo.ack_terminal(
                id, key .. ":other", activation.generation, now())
            test.is_nil(wrong_key)
            test.contains(wrong_key_err, "CONFLICT")
            local ack, ack_err = activation_repo.ack_terminal(id, key, activation.generation, now())
            test.is_nil(ack_err)
            test.is_true(ack.acknowledged)
            local repeat_ack, repeat_err = activation_repo.ack_terminal(id, key, activation.generation, now())
            test.is_nil(repeat_err)
            test.eq(repeat_ack.terminal_ack_at, ack.terminal_ack_at)
            delete_flow(id)
        end)
    end)
end

return { run_tests = test.run_cases(define_tests) }
