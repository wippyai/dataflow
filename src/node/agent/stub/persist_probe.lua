local contract = require("contract")
local ctx = require("ctx")
local helpers = require("helpers")

local RECORDS = "after_persist_records"

-- Reads the committed action named by refs.event_id through the supplied
-- run_context and records what it found under the dataflow id.
local function handler(payload)
    if payload.phase ~= "after_persist" then
        return {}
    end
    local host = payload.host or {}
    local event_id = payload.refs and payload.refs.event_id
    local run_context = payload.run_context or {}

    local definition, definition_err = contract.get(run_context.contract)
    if not definition then
        return nil, "run_context contract: " .. tostring(definition_err)
    end
    local reader, open_err = definition:open(run_context.binding)
    if not reader then
        return nil, "run_context open: " .. tostring(open_err)
    end
    local history, history_err = reader:get_history({
        host = run_context.host,
        agent = run_context.agent,
        selector = { mode = "range", to_id = event_id },
    })
    local found = nil
    for _, event in ipairs(history and history.events or {}) do
        if event.id == event_id then
            found = event
        end
    end

    local records = helpers.get_metric(host.dataflow_id, RECORDS, {})
    records[#records + 1] = {
        event_id = event_id,
        iteration = payload.refs and payload.refs.iteration,
        history_error = history_err and tostring(history_err) or nil,
        found_role = found and found.role or nil,
        found_key = found and found.key or nil,
        outcome_state = payload.outcome and payload.outcome.state,
        memory_recall_error = payload.outcome and payload.outcome.memory_recall_error,
    }
    helpers.set_metric(host.dataflow_id, RECORDS, records)

    local options = ctx.get("options") or {}
    if options.fail == true then
        return nil, "after_persist probe failure"
    end
    return {}
end

local function records(input)
    if type(input) ~= "table" or not input.scenario_id then
        return nil, "scenario_id is required"
    end
    return helpers.get_metric(tostring(input.scenario_id), RECORDS, {})
end

return { handler = handler, records = records }
