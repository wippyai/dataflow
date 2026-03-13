local call_count = 0

local function handler(input, context)
    call_count = call_count + 1

    if call_count == 1 then
        return {
            success = true,
            result = {
                content = "I will try to call a tool with a very long argument...",
                tool_calls = {
                    {
                        id = "call_truncated_001",
                        name = "SomeToolCall",
                        arguments = { partial = true }
                    }
                }
            },
            finish_reason = "length",
            tokens = {
                prompt_tokens = 100,
                completion_tokens = 500,
                total_tokens = 600,
                thinking_tokens = 0
            },
            metadata = {}
        }
    end

    return {
        success = true,
        result = {
            content = "Task completed successfully after truncation recovery.",
            tool_calls = {}
        },
        finish_reason = "stop",
        tokens = {
            prompt_tokens = 200,
            completion_tokens = 100,
            total_tokens = 300,
            thinking_tokens = 0
        },
        metadata = {}
    }
end

local function reset()
    call_count = 0
end

return { handler = handler, reset = reset }
