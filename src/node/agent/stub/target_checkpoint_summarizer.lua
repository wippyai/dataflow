local ctx = require("ctx")

local function handler(args)
    local run = ctx.get("agent_run") or {}
    assert(run.agent and run.agent.id == "userspace.dataflow.node.agent.stub:compact_target_agent",
        "checkpoint must receive the committed target identity")
    assert(args.options and args.options.max_tokens == 731,
        "checkpoint must receive the target's options")
    return { memory = "post-control target checkpoint" }
end

return { handler = handler }
