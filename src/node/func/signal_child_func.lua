local uuid = require("uuid")

-- Test function for func-node recovery: emits a control directive that creates a
-- signal child node and (via the func node) yields for it. Because the child id is a
-- fresh uuid, a naive re-run on recovery would create a duplicate; the func node's
-- checkpoint/resume must prevent that.
local function run(input_data)
    local signal_id = (input_data or {}).signal_id or ("func-child-signal-" .. uuid.v7())
    local child_node_id = uuid.v7()
    local output_data_id = uuid.v7()

    return {
        result = "awaiting signal",
        _control = {
            commands = {
                {
                    type = "CREATE_NODE",
                    payload = {
                        node_id = child_node_id,
                        node_type = "userspace.dataflow.node.signal:node",
                        status = "pending",
                        config = {
                            signal_id = signal_id,
                            data_targets = {
                                {
                                    data_type = "node.output",
                                    key = "signal_result",
                                    content_type = "application/json",
                                    data_id = output_data_id
                                }
                            }
                        },
                        metadata = { title = "Func Signal Child" }
                    }
                },
                {
                    type = "CREATE_DATA",
                    payload = {
                        data_id = uuid.v7(),
                        data_type = "node.input",
                        node_id = child_node_id,
                        key = "default",
                        content = { signal_id = signal_id },
                        content_type = "application/json"
                    }
                }
            }
        }
    }
end

return { run = run }
