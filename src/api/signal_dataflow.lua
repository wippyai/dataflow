local http = require("http")
local security = require("security")
local client = require("client")
local dataflow_repo = require("dataflow_repo")

local function handler()
    local res = http.response()
    local req = http.request()

    if not res or not req then
        return nil, "Failed to get HTTP context"
    end

    -- Security check - ensure user is authenticated
    local actor = security.actor()
    if not actor then
        res:set_status(http.STATUS.UNAUTHORIZED)
        res:set_content_type(http.CONTENT.JSON)
        res:write_json({
            success = false,
            error = "Authentication required"
        })
        return
    end

    local user_id = actor:id()

    -- Get dataflow ID from URL path
    local dataflow_id = req:param("id")
    if not dataflow_id or dataflow_id == "" then
        res:set_status(http.STATUS.BAD_REQUEST)
        res:set_content_type(http.CONTENT.JSON)
        res:write_json({
            success = false,
            error = "Missing dataflow ID in path"
        })
        return
    end

    -- Parse request body for signal_id and data
    local body, body_err = req:body_json()
    if body_err or type(body) ~= "table" then
        res:set_status(http.STATUS.BAD_REQUEST)
        res:set_content_type(http.CONTENT.JSON)
        res:write_json({
            success = false,
            error = "Invalid JSON body"
        })
        return
    end

    local signal_id = body.signal_id
    if type(signal_id) ~= "string" or signal_id == "" then
        res:set_status(http.STATUS.BAD_REQUEST)
        res:set_content_type(http.CONTENT.JSON)
        res:write_json({
            success = false,
            error = "Missing signal_id in body"
        })
        return
    end

    -- Verify access the same way cancel/terminate do: client:signal itself performs
    -- no access check, so the endpoint must authorize the caller against the dataflow.
    local workflow, access_err = dataflow_repo.get_by_user(dataflow_id, user_id)
    if access_err or not workflow then
        local message = access_err or "Workflow not found"
        local status = http.STATUS.INTERNAL_ERROR
        if message:match("not found") or message:match("access denied") then
            status = http.STATUS.NOT_FOUND
            message = "Dataflow not found"
        end
        res:set_status(status)
        res:set_content_type(http.CONTENT.JSON)
        res:write_json({
            success = false,
            error = message
        })
        return
    end

    -- Create client instance
    local workflow_client, client_err = client.new()
    if client_err then
        res:set_status(http.STATUS.INTERNAL_ERROR)
        res:set_content_type(http.CONTENT.JSON)
        res:write_json({
            success = false,
            error = "Failed to initialize workflow client: " .. client_err
        })
        return
    end

    -- Deliver the signal (durable NODE_SIGNAL commit + respawn on dead orchestrator)
    local result, signal_err = (workflow_client :: any):signal(dataflow_id, signal_id, body.data)
    if signal_err then
        local message = tostring(signal_err)
        local status = http.STATUS.INTERNAL_ERROR
        if message:match("terminal state") then
            status = http.STATUS.BAD_REQUEST
        elseif message:match("not found") then
            status = http.STATUS.NOT_FOUND
        end
        res:set_status(status)
        res:set_content_type(http.CONTENT.JSON)
        res:write_json({
            success = false,
            error = message
        })
        return
    end

    -- Success response
    res:set_content_type(http.CONTENT.JSON)
    res:set_status(http.STATUS.OK)
    res:write_json({
        success = true,
        message = "Signal delivered to workflow",
        dataflow_id = dataflow_id,
        signal_id = signal_id,
        commit_id = (type(result) == "table" and result.commit_id) or nil
    })
end

return {
    handler = handler
}
