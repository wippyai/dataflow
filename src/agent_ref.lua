local contract = require("contract")
local registry = require("registry")
local security = require("security")

local M = {}

M._resolver = nil
M._registry = nil

local AGENT_RESOLVER_CONTRACT = "wippy.agent:resolver"
local USER_AGENT_PREFIX = "user_agent:"
local UUID_RE = "^%x%x%x%x%x%x%x%x%-%x%x%x%x%-%x%x%x%x%-%x%x%x%x%-%x%x%x%x%x%x%x%x%x%x%x%x$"

local function trim(value: any): string
    if type(value) ~= "string" then return "" end
    return value:match("^%s*(.-)%s*$") or ""
end

local function first_text(first: any, second: any?, third: any?): string
    local text = trim(first)
    if text ~= "" then return text end
    text = trim(second)
    if text ~= "" then return text end
    return trim(third)
end

local function bare_user_agent_id(value)
    local ref = trim(value)
    if ref:sub(1, #USER_AGENT_PREFIX) == USER_AGENT_PREFIX then
        return ref:sub(#USER_AGENT_PREFIX + 1)
    end
    return ref
end

local function fallback_title(value)
    local ref = trim(value)
    if ref == "" then return "" end
    if ref:sub(1, #USER_AGENT_PREFIX) == USER_AGENT_PREFIX then return "Custom agent" end

    local bare = bare_user_agent_id(ref)
    if bare:match(UUID_RE) then return "Custom agent" end

    if ref:find(":", 1, true) then
        local tail = ref:match("([^:]+)$")
        return trim(tail) ~= "" and tail or ref
    end
    return ref
end

local function open_resolver()
    if M._resolver ~= nil then return M._resolver end

    local def, get_err = contract.get(AGENT_RESOLVER_CONTRACT)
    if get_err or not def then return nil end

    local actor = security.actor()
    local scope = security.scope()
    if not actor or not scope then return nil end

    local opened, open_err = def:with_actor(actor):with_scope(scope):open()
    if open_err or not opened then return nil end
    return opened
end

local function title_from_resolver(agent_id)
    local resolver = open_resolver()
    if not resolver or type(resolver.resolve) ~= "function" then return nil end

    local ok, spec_or_err = pcall(function()
        return resolver:resolve({ agent_id = agent_id })
    end)
    if not ok or type(spec_or_err) ~= "table" then return nil end

    local title = first_text(spec_or_err.title, spec_or_err.display_title, spec_or_err.name)
    if title == "" then return nil end
    return {
        id = first_text(spec_or_err.id, agent_id),
        title = title,
        name = first_text(spec_or_err.name, title),
    }
end

local function title_from_registry(agent_id)
    local entry = nil

    if M._registry ~= nil then
        local injected = M._registry :: any
        if type(injected.get) == "function" then
            entry = injected.get(agent_id)
        end
    else
        local ok, result = pcall(function()
            return registry.get(agent_id)
        end)
        if ok then entry = result end
    end

    local row = entry :: any
    local meta = row and (row.meta or row)
    if type(meta) ~= "table" then return nil end

    local title = first_text(meta.title, meta.display_title, meta.name)
    if title == "" then return nil end
    return {
        id = first_text(meta.id, entry.id, agent_id),
        title = title,
        name = first_text(meta.name, title),
    }
end

function M.lookup(agent_id)
    local ref = trim(agent_id)
    if ref == "" then return nil end

    return title_from_resolver(ref)
        or title_from_registry(ref)
end

function M.resolve(agent_id)
    local ref = trim(agent_id)
    if ref == "" then
        return { id = "", title = "", name = "" }
    end

    return M.lookup(ref) or {
        id = ref,
        title = fallback_title(ref),
        name = fallback_title(ref),
    }
end

function M.title(agent_id)
    local resolved = M.lookup(agent_id)
    return resolved and resolved.title or nil
end

return M
