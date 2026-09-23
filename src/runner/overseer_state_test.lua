local test = require("test")
local overseer: any = require("overseer_state")
local CURRENT_EPOCH = "runtime-current"

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

local function observe(fields: any): Observation
    local observation: Observation = {
        dataflow_id = "df",
        status = "running",
        desired_active = true,
        generation = 3,
        runtime_epoch = CURRENT_EPOCH,
    }
    local values = fields or {}
    if values.status ~= nil then observation.status = values.status end
    if values.desired_active ~= nil then observation.desired_active = values.desired_active == true end
    observation.owner_token = values.owner_token
    observation.owner_phase = values.owner_phase
    observation.owner_epoch = values.owner_epoch
    observation.registered_pid = values.registered_pid
    return observation
end

local function decide(observation: Observation): any
    return overseer.decide(observation)
end

local function run_tests()
    test.describe("Pure Dataflow overseer decisions", function()
        test.it("monitors whichever process holds the canonical name", function()
            local decision = decide(observe({
                registered_pid = "pid-live",
                owner_token = "t1", owner_phase = "running", owner_epoch = CURRENT_EPOCH,
            }))
            test.eq(decision.kind, overseer.ACTION.MONITOR)
            test.eq(decision.pid, "pid-live")
        end)

        test.it("fails a running owner of this runtime that lost its name, fenced by its token", function()
            local decision = decide(observe({
                owner_token = "t1", owner_phase = "running", owner_epoch = CURRENT_EPOCH,
            }))
            test.eq(decision.kind, overseer.ACTION.FAIL)
            test.eq(decision.reason, "runtime_owner_lost")
            test.eq(decision.fence.token, "t1")
            test.eq(decision.fence.phase, "running")
            test.is_nil(decision.fence.generation, "the owner's token covers every newer request")
        end)

        test.it("spawns for an unowned, released or earlier-runtime activation", function()
            for _, owner in ipairs({
                {},
                { owner_token = "t1", owner_phase = "released", owner_epoch = CURRENT_EPOCH },
                { owner_token = "t1", owner_phase = "running", owner_epoch = "runtime-before" },
            }) do
                local decision = decide(observe(owner))
                test.eq(decision.kind, overseer.ACTION.SPAWN)
                test.eq(decision.generation, 3)
                test.eq(decision.fence.token, owner.owner_token)
                test.eq(decision.fence.phase, owner.owner_phase)
                test.eq(decision.fence.generation, 3)
            end
        end)

        test.it("stops a name holder of a terminal or inactive activation and otherwise does nothing", function()
            for _, state in ipairs({
                { status = "failed" },
                { desired_active = false, status = "waiting" },
            }) do
                local named = observe(state)
                named.registered_pid = "pid-old"
                local stop = decide(named)
                test.eq(stop.kind, overseer.ACTION.STOP)
                test.eq(stop.pid, "pid-old")
                test.eq(decide(observe(state)).kind, overseer.ACTION.NONE)
            end
        end)

        test.it("tracks one monitored process per dataflow and routes its EXIT", function()
            local state = overseer.new()
            overseer.track(state, "df", "pid-1")
            overseer.track(state, "df", "pid-2")
            test.eq(overseer.pid_for(state, "df"), "pid-2")
            test.eq(overseer.forget_pid(state, "pid-1"), "df")
            test.eq(overseer.pid_for(state, "df"), "pid-2", "a late EXIT leaves the current owner tracked")
            test.eq(overseer.forget_pid(state, "pid-2"), "df")
            test.is_nil(overseer.pid_for(state, "df"))
            test.is_nil(overseer.forget_pid(state, "pid-unknown"))
        end)
    end)
end

return { run_tests = test.run_cases(run_tests) }
