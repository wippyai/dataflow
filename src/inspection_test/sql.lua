-- SPDX-License-Identifier: Apache-2.0
local M = {}
type Map = { [string]: any }
M.state = {} :: Map
function M.get(): (any?, error?)
    if M.state.open_error then return nil, M.state.open_error end
    local db: any = {}
    function db:query(): (any?, error?) return M.state.rows, M.state.query_error end
    function db:release(): (boolean?, error?)
        M.state.released = true
        if M.state.release_error then return nil, M.state.release_error end
        return true, nil
    end
    return db, nil
end
return M
