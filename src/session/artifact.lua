-- create_view.lua
local json = require("json")
local ctx = require("ctx")
local artifact_repo = require("artifact_repo")

local function handle(args)
    args = args or {}

    local dataflow_id = args.dataflow_id
    local metadata = args.metadata or {}

    if not dataflow_id then
        return {
            success = false,
            error = "dataflow_id is required"
        }
    end

    -- Get session_id from context
    local session_id, _ = ctx.get("session_id")
    if not session_id then
        return {
            success = false,
            error = "session_id not found in context"
        }
    end

    -- Create title from metadata
    local title = metadata.title or "Workflow Execution"

    -- Parameters for the dataflow state view
    local view_params = {
        id = dataflow_id
    }

    local params_json, json_err = json.encode(view_params)
    if json_err then
        return {
            success = false,
            error = "Failed to encode view parameters: " .. json_err
        }
    end

    -- Create the artifact
    local artifact, err = artifact_repo.create(
        dataflow_id,
        session_id,
        "view_ref",
        title,
        params_json,
        {
            content_type = "text/html",
            description = "Workflow execution state",
            status = "active",
            page_id = "userspace.dataflow.session.views:state",
            display_type = "inline-interactive"
        }
    )

    if err then
        return {
            success = false,
            error = "Failed to create artifact: " .. err
        }
    end

    -- Announce to session
    process.send("session." .. session_id, "command", {
        command = "artifact",
        artifact_id = dataflow_id
    })

    return {
        success = true,
        artifact_id = dataflow_id
    }
end

return { handle = handle }
