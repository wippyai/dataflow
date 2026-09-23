-- Pure decisions for the Dataflow overseer.
--
-- The activation row carries the ownership record, written only by the
-- orchestrator: owner_token (one orchestrator incarnation), owner_epoch (its
-- runtime) and owner_phase (running or released). The canonical name
-- dataflow.<id> is held by at most one process. From one locked observation of
-- both, the overseer decides without remembering earlier decisions:
--   - terminal or inactive: stop whoever holds the name;
--   - a name holder: monitor it;
--   - a running owner of this runtime without the name: it died, so fail the
--     activation fenced by its token, which covers every newer request;
--   - otherwise (never owned, released, or owned in an earlier runtime): spawn.
local M = {}

M.ACTION = {
    NONE = "none",
    STOP = "stop",
    MONITOR = "monitor",
    FAIL = "fail",
    SPAWN = "spawn",
}

local OWNER_RUNNING = "running"

local TERMINAL_STATUS = {
    completed = true,
    failed = true,
    cancelled = true,
    terminated = true,
}

type Observation = {
    dataflow_id: string,
    status: string?,
    desired_active: boolean,
    generation: number?,
    owner_token: string?,
    owner_phase: string?,
    owner_epoch: string?,
    registered_pid: string?,
    runtime_epoch: string,
}

type Fence = {
    token: string?,
    phase: string?,
    generation: number?,
}

type Decision = {
    kind: string,
    reason: string,
    dataflow_id: string,
    generation: number?,
    pid: string?,
    fence: Fence?,
}

type State = {
    by_pid: { [string]: string },
    by_dataflow: { [string]: string },
}

local function is_terminal(status: string?): boolean
    return TERMINAL_STATUS[string.lower(tostring(status or ""))] == true
end

function M.decide(observation: Observation): Decision
    local id = observation.dataflow_id
    local pid = observation.registered_pid
    if pid == "" then pid = nil end

    if is_terminal(observation.status) or observation.desired_active ~= true then
        if pid then
            return {
                kind = M.ACTION.STOP,
                reason = is_terminal(observation.status) and "terminal_owner_stop" or "inactive_owner_stop",
                dataflow_id = id,
                pid = pid,
            }
        end
        return { kind = M.ACTION.NONE, reason = "not_active", dataflow_id = id }
    end

    if pid then
        return { kind = M.ACTION.MONITOR, reason = "registered_owner", dataflow_id = id, pid = pid }
    end

    if observation.owner_phase == OWNER_RUNNING and observation.owner_token ~= nil and
        observation.owner_epoch == observation.runtime_epoch then
        return {
            kind = M.ACTION.FAIL,
            reason = "runtime_owner_lost",
            dataflow_id = id,
            generation = observation.generation,
            fence = { token = observation.owner_token, phase = OWNER_RUNNING },
        }
    end

    return {
        kind = M.ACTION.SPAWN,
        reason = "activation_unowned",
        dataflow_id = id,
        generation = observation.generation,
        fence = {
            token = observation.owner_token,
            phase = observation.owner_phase,
            generation = observation.generation,
        },
    }
end

function M.new(): State
    return { by_pid = {}, by_dataflow = {} }
end

-- Record the monitored process of a dataflow; an earlier process stays routable
-- until its EXIT arrives.
function M.track(state: State, dataflow_id: string, pid: string)
    state.by_pid[pid] = dataflow_id
    state.by_dataflow[dataflow_id] = pid
end

function M.forget_pid(state: State, pid: string): string?
    local dataflow_id = state.by_pid[pid]
    if not dataflow_id then return nil end
    state.by_pid[pid] = nil
    if state.by_dataflow[dataflow_id] == pid then state.by_dataflow[dataflow_id] = nil end
    return dataflow_id
end

function M.forget_dataflow(state: State, dataflow_id: string)
    local pid = state.by_dataflow[dataflow_id]
    if pid then state.by_pid[pid] = nil end
    state.by_dataflow[dataflow_id] = nil
end

function M.pid_for(state: State, dataflow_id: string): string?
    return state.by_dataflow[dataflow_id]
end

function M.tracked(state: State): { string }
    local ids = {}
    for dataflow_id in pairs(state.by_dataflow) do table.insert(ids, dataflow_id) end
    return ids
end

return M
