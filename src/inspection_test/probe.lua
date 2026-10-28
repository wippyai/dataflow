-- SPDX-License-Identifier: Apache-2.0
local inspection = require("inspection")
local function run(id: string): (any?, error?)
    return inspection.flow(id)
end
return {run = run}
