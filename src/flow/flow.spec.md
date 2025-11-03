# Dataflow Flow Builder SDK - Specification

## Overview

Flow Builder SDK provides a fluent interface for composing acyclic dataflows. Iteration exists only as encapsulated subgraphs via cycle nodes.

**Core Principles:**
- Acyclic Graph: Forward-only routing with explicit edges
- Input-First: `:with_input()` establishes workflow input
- Static Data: `:with_data()` creates reusable static data sources
- Transform-on-Route: Transforms apply at routing points
- Explicit Routing: `:to()` for success, `:error_to()` for errors
- Terminal Nodes: `@success` and `@fail` for explicit workflow completion
- Conditional Logic: `:when()` applies to preceding route
- Optional Naming: `:as()` only when references needed
- Template Reuse: `:use()` inlines, `flow.template()` defines

**CRITICAL FOR NESTED WORKFLOWS:**
Functions that use Flow Builder must always return `flow.create()...:run()`. This enables workflow chaining and allows functions to spawn child workflows through the `{_control = {commands = [...]}}` mechanism.

```lua
function my_processor(input)
    return flow.create()
        :with_input(input)
        :func("process")
        :run()
end
```

## Core API

```lua
local flow = require("userspace.dataflow.flow")

flow.create()
    :with_title(title)
    :with_metadata(metadata)
    :with_input(data)
    :with_data(data)
    :[operation](config)
    :as(name)
    :to(target, input_key, transform)
    :error_to(target, input_key, transform)
    :when(condition)
    :run()
    :start()

flow.template()
    :[operations]...
```

## Expression Syntax

Flow Builder uses the `expr` module for dynamic expressions in two contexts: conditional routing (`:when()`) and input transformation (`input_transform`).

### Available in Conditionals (`:when()`)

Conditionals evaluate node outputs to determine routing paths.

**Basic Operations:**
```lua
:to("success"):when("output.is_valid")
:to("retry"):when("!output.success")
:to("process"):when("output.score > 0.8")
:to("handle"):when("output.status == 'ready'")
```

**Logical Operators:**
```lua
:to("next"):when("output.valid && output.score > 0.5")
:to("alt"):when("output.error || output.timeout")
:to("skip"):when("!(output.required)")
```

**Array Operations:**
```lua
:to("process"):when("len(output.items) > 0")
:to("skip"):when("len(output.assignments) == 0")
:to("check"):when("output.count >= 10")
```

**Array Functions:**
```lua
:to("next"):when("all(output.items, {.valid})")
:to("filter"):when("any(output.errors, {.critical})")
:to("handle"):when("none(output.tasks, {.completed})")
```

**Property Access:**
```lua
:to("deep"):when("output.user.profile.active")
:to("safe"):when("output.data?.nested?.value > 0")
```

**String Operations:**
```lua
:to("match"):when("output.status contains 'success'")
:to("check"):when("output.name startsWith 'test_'")
```

**Comparisons:**
```lua
:to("high"):when("output.priority >= 5")
:to("match"):when("output.type != 'internal'")
:to("member"):when("output.role in ['admin', 'moderator']")
```

**Ternary:**
```lua
:to("route"):when("output.age >= 18 ? output.verified : false")
```

### Available in Transforms (`input_transform`)

Transforms extract and reshape data when routing between nodes.

**Context Variables:**
- `input` - Workflow input
- `inputs` - All incoming node inputs
- `output` - Current node's output (when routing from node)

**Direct References:**
```lua
input_transform = {
    task = "input.prompt",
    config = "inputs.settings",
    result = "output.data"
}
```

**Nested Access:**
```lua
input_transform = {
    user_id = "input.user.id",
    settings = "inputs.config.database.settings",
    score = "output.analysis.metrics.score"
}
```

**Array Literals:**
```lua
input_transform = {
    items = "[1, 2, 3]",
    names = '["alice", "bob", "charlie"]',
    mixed = '[output.id, input.value, "constant"]'
}
```

**Object Literals:**
```lua
input_transform = {
    result = "{passed: true, feedback: nil}",
    user = '{id: output.user_id, name: output.name, active: true}',
    config = '{endpoint: "/api", timeout: 5000, retry: false}'
}
```

**Array Operations:**
```lua
input_transform = {
    first_item = "output.items[0]",
    last_item = "output.items[-1]",
    subset = "output.data[1:5]"
}
```

**Function Calls:**
```lua
input_transform = {
    item_count = "len(output.items)",
    names = "map(output.users, {.name})",
    valid_items = "filter(output.items, {.valid})",
    total = "output.values[0] + output.values[1]"
}
```

**Conditional Values:**
```lua
input_transform = {
    status = "output.error ? 'failed' : 'success'",
    priority = "output.score > 0.8 ? 'high' : 'normal'"
}
```

**String Literals:**
```lua
input_transform = {
    endpoint = '"/api/v1/process"',
    message = '"Processing complete"',
    template = '"User: " + input.name'
}
```

**Numeric Literals:**
```lua
input_transform = {
    timeout = "5000",
    retries = "3",
    threshold = "0.85"
}
```

**Boolean Literals:**
```lua
input_transform = {
    enabled = "true",
    debug = "false",
    required = "output.critical"
}
```

**Null Literal:**
```lua
input_transform = {
    feedback = "nil",
    optional = "output.value ?? nil"
}
```

**Complex Expressions:**
```lua
input_transform = {
    agent_id = "inputs.routing.agent_id",
    task = "inputs.task",
    priority = "output.score > 0.8 ? 'high' : 'normal'",
    item_count = "len(filter(output.items, {.valid}))",
    config = '{endpoint: input.url, enabled: true, timeout: 5000}'
}
```

**Combining Literals and Variables:**
```lua
input_transform = {
    response = '{status: output.code, message: output.msg, timestamp: now()}',
    metadata = '{user_id: input.user.id, action: "process", completed: true}',
    result = '{data: output.result, errors: output.errors ?? []}'
}
```

### Transform Expression on Route (Third Parameter)

The third parameter to `:to()` and `:error_to()` accepts an expression that transforms data inline:

```lua
:func("source"):as("source")
    :to("target", nil, "output.data")                    -- Extract field
    :to("other", nil, "{passed: true, value: output.x}") -- Build object
    :to("list", nil, "[output.a, output.b, output.c]")   -- Build array
```

This is equivalent to using `input_transform` with a single field:
```lua
:func("target", {
    input_transform = {
        default = "output.data"  -- Third param creates "default" key
    }
})
```

**Examples:**
```lua
-- Extract nested field
:to("processor", nil, "output.user.profile.id")

-- Build response object
:to("@success", nil, "{passed: true, feedback: nil}")

-- Transform array
:to("handler", nil, "map(output.items, {.id})")

-- Conditional transform
:to("router", nil, "output.valid ? output.data : nil")
```

### Expression Reference

For complete expression syntax including all operators, functions, and type conversions, see the `expr` module specification. Key capabilities:

**Operators:** Arithmetic (`+`, `-`, `*`, `/`, `%`, `**`), Comparison (`==`, `!=`, `<`, `<=`, `>`, `>=`), Logical (`&&`, `||`, `!`), Bitwise (`&`, `|`, `^`, `<<`, `>>`), String (`contains`, `startsWith`, `endsWith`)

**Array Functions:** `all()`, `any()`, `none()`, `one()`, `filter()`, `map()`, `count()`, `len()`, `first()`, `last()`

**Math Functions:** `max()`, `min()`, `abs()`, `ceil()`, `floor()`, `round()`, `sqrt()`, `pow()`

**String Functions:** `len()`, `upper()`, `lower()`, `trim()`, `split()`, `join()`

**Type Functions:** `type()`, `int()`, `float()`, `string()`

**Literals:** Numbers (`42`, `3.14`, `0x1A`), Strings (`"hello"`, `'world'`), Booleans (`true`, `false`), Null (`nil`), Arrays (`[1, 2, 3]`), Objects (`{key: value}`)

## Workflow Configuration

### Title

```lua
flow.create()
    :with_title("Data Processing Pipeline")
    :with_input(data)
    :func("process")
    :run()
```

Sets the workflow title. Defaults to "Flow Builder Workflow" if not provided.

### Metadata

```lua
flow.create()
    :with_title("Analytics Job")
    :with_metadata({
        project = "analytics",
        version = "1.0",
        owner = "data-team",
        priority = "high"
    })
    :with_input(data)
    :func("process")
    :run()
```

Sets custom workflow metadata. Merged with default fields (`title`, `created_by`).

## Input and Static Data

### Workflow Input (`:with_input()`)

Represents the single primary input that the workflow receives from outside.

**Semantics:**
- Creates `WORKFLOW_INPUT` at top-level
- Creates `NODE_INPUT` in nested contexts (cycles, parallel)
- Use once per workflow
- Represents external data coming into the workflow

```lua
flow.create()
    :with_input({ task = "implement X", priority = "high" })
    :to("processor")
    :func("processor")
    :run()
```

**Routing workflow input:**

```lua
flow.create()
    :with_input(data)
    :to("nodeA")
    :to("nodeB", "input_b")
    :to("nodeC", nil, "input.field")
```

**With transforms:**

```lua
flow.create()
    :with_input({prompt = "task", data = [1,2,3]})
    :to("agent", nil, "input.prompt")
    :to("processor", nil, "input.data")
```

### Static Data Sources (`:with_data()`)

Creates independent static data sources that act like constant nodes. Always creates `NODE_INPUT`, never `WORKFLOW_INPUT`.

**Semantics:**
- Multiple `:with_data()` calls allowed
- Acts like a node: can be named with `:as()`, routes with `:to()`
- Always creates `NODE_INPUT` (even at top-level)
- Reference optimization: first route creates actual data, subsequent routes create references

```lua
flow.create()
    :with_data(config):as("config_data")
        :to("api", "config")
        :to("logger", "config")
    
    :with_data(valid_options):as("options")
        :to("validator", "options")
```

**Reference optimization:**
```lua
:with_data(large_config):as("cfg")
    :to("nodeA", "config")
    :to("nodeB", "config")
    :to("nodeC", "config")
```

First route to nodeA creates actual `NODE_INPUT` with content. Subsequent routes to nodeB and nodeC create `NODE_INPUT` with `content_type: "dataflow/reference"` pointing to nodeA's data_id.

**When to use each:**

Use `:with_input()` for:
- External input to the workflow
- Single logical input from outside
- Data that represents "what the workflow receives"

Use `:with_data()` for:
- Config, constants, reference data
- Multiple independent static sources
- Clean routing without transforms
- Sharing same data across multiple nodes

**Comparison:**

```lua
flow.create()
    :with_input({ task = task, config = config, branch = branch })
    :to("router", nil, "input.task")
    :to("api", nil, "input.config")
    :to("logger", nil, "input.config")
    :to("checker", nil, "input.branch")
```

becomes:

```lua
flow.create()
    :with_input(task)
        :to("router")
    
    :with_data(config):as("cfg")
        :to("api", "config")
        :to("logger", "config")
    
    :with_data(branch):as("branch_data")
        :to("checker", "branch")
```

## Routing

### Routing Mechanics

Routing determines how data flows between nodes. Each route creates a `NODE_INPUT` at the target node with a **discriminator** (input key).

#### Discriminator Rules

**When routing to a node:**

```lua
:func("source")
    :to("target")                    -- discriminator: "default"
    :to("other", "config")           -- discriminator: "config"
    :to("another", nil, "expr")      -- discriminator: "default"
```

The discriminator determines the input key at the receiving node.

#### Input Merging at Function Nodes

Function nodes merge inputs based on discriminators. The behavior depends on whether `args` is configured:

**Without args (no base_args):**

1. Single input with discriminator `"default"` or `""`:
   ```lua
   -- Source
   :func("source"):to("target")
   
   -- Target receives
   input = "raw content"  -- unwrapped
   ```

2. Single input with named discriminator:
   ```lua
   -- Source
   :func("source"):to("target", "task")
   
   -- Target receives
   input = {task = "content"}  -- wrapped
   ```

3. Multiple inputs:
   ```lua
   -- Sources
   :func("source1"):to("target", "data")
   :func("source2"):to("target", "config")
   
   -- Target receives
   input = {
       data = "content1",
       config = "content2"
   }
   ```

**With args (base_args present):**

```lua
:func("target", {
    args = {
        endpoint = "https://api.com",
        timeout = 5000
    }
})
```

All inputs are merged into args structure:

```lua
-- Source
:func("source"):to("target", "request")

-- Target receives
input = {
    endpoint = "https://api.com",  -- from args
    timeout = 5000,                -- from args
    request = "content"            -- from routed input
}
```

**Key Insight:** Single named input without args becomes `{name = value}`. With args, all inputs merge into args base.

#### Common Patterns

**Pattern 1: Simple pass-through**
```lua
:with_input(task)
    :to("processor")  -- processor gets raw task string
```

**Pattern 2: Multiple inputs to one node**
```lua
:with_input(task):as("task_data")
    :to("processor", "task")

:with_data(config):as("cfg")
    :to("processor", "config")

-- processor gets {task = ..., config = ...}
```

**Pattern 3: Args with dynamic input**
```lua
:func("api_client", {
    args = {
        base_url = "https://api.com",
        headers = {...}
    }
})

:with_input(request)
    :to("api_client", "body")

-- api_client gets {base_url = ..., headers = ..., body = ...}
```

**Pattern 4: Avoid wrapper with default**
```lua
-- DON'T: Creates {task = value}
:with_data(task):to("router", "task")

-- DO: Passes raw value
:with_data(task):to("router")
```

### Automatic Sequential

Without explicit `:to()`, outputs auto-chain to next node. Using `:to()` disables auto-chain for that node.

### Terminal Nodes

```lua
:func("process")
    :to("@success")
    :error_to("@fail")

:func("validate")
    :to("process"):when("output.valid")
    :error_to("@fail")
```

Terminal routes (`@success`, `@fail`) adapt to context:
- Top-level: creates `WORKFLOW_OUTPUT`
- Nested (cycles, parallel): creates `NODE_OUTPUT`

### Conditional Routing (`:when()`)

```lua
:func("validator"):as("validator")
    :to("processor"):when("output.is_valid")
    :to("error_handler"):when("!output.is_valid")
    :error_to("@fail")
```

Conditionals apply to preceding route. Uses full expr syntax for dynamic routing based on node output.

**Examples:**
```lua
:to("next"):when("output.score > 0.8")
:to("skip"):when("len(output.items) == 0")
:to("branch"):when("output.status == 'ready' && output.count > 5")
:to("filter"):when("any(output.errors, {.critical})")
```

## Operations

### Function (`:func()`)

```lua
:func("namespace:function_name", {
    args = {...},
    inputs = {required = ["input1"]},
    context = {...},
    metadata = {title = "Process Data"},
    input_transform = {field = "inputs.source"}
})
```

Invokes a registered function. If function returns `{_control = {commands = [...]}}`, spawns a child workflow.

### Agent (`:agent()`)

```lua
:agent("namespace:agent_name", {
    model = "gpt-5-mini",
    arena = {
        prompt = "System instructions",
        max_iterations = 8,
        min_iterations = 1,
        tool_calling = "auto",
        exit_schema = {...},
        exit_func_id = "namespace:validate_exit",
        tools = [...],
        context = {...}
    },
    inputs = {required = ["task"]},
    show_tool_calls = true,
    metadata = {title = "Specialist Agent"},
    input_transform = {task = "inputs.prompt"}
})
```

Creates an agent execution node with arena configuration.

**`exit_func_id` (optional):** Validates finish tool output before completion. Only works with `tool_calling = "any"` or `"auto"` with `exit_schema`.

```lua
function validate_exit(input)
    if validation_passes then
        return result  -- becomes final_result
    else
        return nil, "Error message for agent"
    end
end
```

On validation failure, agent receives error as observation and continues. Respects `max_iterations`.

**Dynamic agent:**
```lua
:agent("", {
    inputs = {required = {"routing"}},
    input_transform = {
        agent_id = "inputs.routing.agent_id",
        task = "inputs.task"
    },
    arena = {...}
})
```

Agent ID can be selected dynamically via `input_transform.agent_id`.

> Reserved inputs: `agent_id`, `context`, `tools`, `tool_calls`.

### Cycle (`:cycle()`)

```lua
:cycle({
    func_id = "namespace:iteration_func",
    max_iterations = 5,
    initial_state = {count = 0},
    continue_condition = "output.continue",
    inputs = {required = ["data"]},
    metadata = {title = "Iterate Until Valid"}
})
```

Iterative execution with state accumulation. Function receives:

```lua
{
    input = <workflow_input>,
    state = <accumulated_state>,
    last_result = <previous_iteration_output>,
    iteration = <current_iteration_number>
}
```

Function can return:
- Nested workflow via `:run()`
- Direct result with continue flag: `{state = {...}, result = {...}, continue = bool}`

**Template-based cycle:**
```lua
:cycle({
    template = flow.template()
        :agent("worker")
        :func("validator"),
    max_iterations = 5
})
```

### Parallel (`:parallel()`)

Processes array items in parallel batches.

```lua
:parallel({
    inputs = {required = ["items"]},
    source_array_key = "items",
    iteration_input_key = "item",
    passthrough_keys = {"config"},
    batch_size = 10,
    on_error = "continue",
    filter = "successes",
    unwrap = true,
    template = flow.template()
        :func("process_item")
        :to("@success"),
    metadata = {title = "Process Items"}
})
```

#### Required Configuration

**`source_array_key` (required):** Key from inputs containing array to iterate over.

**`template` (required):** Flow template defining iteration logic. Must route to `@success`.

#### Core Configuration

**`iteration_input_key` (optional, default: "default"):** Key name for current item in template's input scope.

```lua
-- With default key
:parallel({
    source_array_key = "items",
    template = flow.template()
        :func("process")  -- receives raw item
})

-- With named key
:parallel({
    source_array_key = "items",
    iteration_input_key = "current_item",
    template = flow.template()
        :func("process", {
            inputs = {required = ["current_item"]}
        })
})
```

**`batch_size` (optional, default: 1):** Number of items to process in parallel. `batch_size = 1` means sequential processing.

**`on_error` (optional, default: "continue"):** Execution strategy when iterations fail.
- `"continue"`: Process all items regardless of errors, node completes
- `"fail_fast"`: Stop on first error, node fails with partial results

**`filter` (optional, default: "all"):** Which iterations to include in output.
- `"all"`: Include both successful and failed iterations
- `"successes"`: Only successful iterations
- `"failures"`: Only failed iterations

**`unwrap` (optional, default: false):** Output format control.
- `false`: Wrap each result with metadata: `{iteration = N, result/error = ...}`
- `true`: Just the raw content from each iteration

#### Passthrough Keys

**`passthrough_keys` (optional):** Array of input keys to pass to each iteration alongside the item.

**Purpose:** Provides shared context (config, task description, constants) to every iteration without duplicating data in the source array.

**Important:** Passthrough keys must reference top-level input keys to the parallel node, not nested paths. Use `input_transform` to extract nested data into separate inputs first if needed.

**Example:**
```lua
:with_data(["file1.txt", "file2.txt"]):as("items")
    :to("processor", "items")

:with_data("secret"):as("api_key")
    :to("processor", "api_key")

:parallel({
    inputs = {required = ["items", "api_key"]},
    source_array_key = "items",
    iteration_input_key = "filename",
    passthrough_keys = {"api_key"},
    template = flow.template()
        :func("upload_file", {
            inputs = {required = ["filename", "api_key"]}
        })
})
```

Each iteration receives:
```lua
{
    filename = "file1.txt",  -- current item
    api_key = "secret"       -- from passthrough_keys
}
```

#### Output Formats

**With `on_error = "continue"`, `filter = "all"`, `unwrap = false`:**
```lua
[
    {iteration = 1, result = {processed: true}},
    {iteration = 2, error = {error: "Failed", code: "ITERATION_FAILED"}},
    {iteration = 3, result = {processed: true}}
]
```

**With `on_error = "continue"`, `filter = "successes"`, `unwrap = true`:**
```lua
[
    {processed: true},
    {processed: true}
]
```

**With `on_error = "fail_fast"`:**
Node fails with error containing partial results:
```lua
{
    code = "ITERATION_FAILED",
    message = "Iteration failed",
    partial_results = [
        {iteration = 1, result = {processed: true}},
        {iteration = 2, error = {error: "Failed", code: "ITERATION_FAILED"}}
    ]
}
```

#### Complete Example

```lua
flow.create()
    :with_input(task_description)
        :to("processor", "task")
    
    :with_data(agent_specs):as("specs")
        :to("processor", "specs")
    
    :parallel({
        inputs = {required = ["specs", "task"]},
        source_array_key = "specs",
        iteration_input_key = "spec",
        passthrough_keys = {"task"},
        batch_size = 10,
        on_error = "continue",
        filter = "successes",
        unwrap = true,
        template = flow.template()
            :agent("", {
                inputs = {required = ["spec", "task"]},
                input_transform = {
                    agent_id = "inputs.spec.agent_id",
                    task = "inputs.task"
                },
                arena = {
                    prompt = "Process according to spec",
                    max_iterations = 25
                }
            })
            :to("@success"),
        metadata = {
            title = "Process Specs",
            icon = "tabler:list"
        }
    })
    :as("processor")
        :to("@success")
    
    :run()
```

#### Defaults

```lua
{
    batch_size = 1,
    iteration_input_key = "default",
    on_error = "continue",
    filter = "all",
    unwrap = false
}
```

### Join
```lua
:join({
    inputs = {required = ["source1", "source2"]},
    input_transform = {data = "inputs.source1", ctx = "inputs.source2"},
    output_mode = "object",
    ignored_keys = {"triggered", "internal_state"},
    metadata = {title = "Merge Data"}
})
```

Waits for all required inputs before proceeding.

**Configuration:**

**`ignored_keys` (optional):** Array of input key names to exclude from output. Useful for filtering out trigger inputs or internal routing keys that shouldn't appear in the final result.
```lua
:state({
    inputs = {required = ["data", "config", "triggered"]},
    ignored_keys = {"triggered"},
    output_mode = "object"
})

-- Output: {data = ..., config = ...}
-- "triggered" input is processed but not included in output
```

**Output structure:**
- Single input or single default key: `<content from source>`
- Multiple named inputs: `{input1 = <content1>, input2 = <content2>}`
- Keys in `ignored_keys` are excluded from output

### Template Usage

```lua
local preprocessor = flow.template()
    :func("namespace:clean")
    :func("namespace:tokenize")

flow.create()
    :with_input(data)
    :use(preprocessor)
    :run()
```

## Complete Examples

### State Node Configuration

State nodes collect multiple inputs and structure them for further processing. By default, state nodes return inputs as an object keyed by discriminator, but can be configured to return inputs as an array in arrival order.

**Default behavior (object mode):**
```lua
:state({
    inputs = {required = ["input_a", "input_b"]},
    output_mode = "object"  -- default
})

-- Output: {input_a = ..., input_b = ...}
```

**Array mode (ordered by arrival):**
```lua
:state({
    inputs = {required = ["result1", "result2", "result3"]},
    output_mode = "array"
})

-- Output: [..., ..., ...]  (in order of arrival time)
```

**Use cases for array mode:**
- When order of arrival matters more than input keys
- Collecting results from parallel operations where ordering is significant
- Aggregating time-series or sequential data
- Simplifying downstream processing that expects arrays

**Example with parallel operations:**
```lua
flow.create()
    :with_input(["task1", "task2", "task3"])
        :to("processor", "tasks")
    
    :parallel({
        inputs = {required = ["tasks"]},
        source_array_key = "tasks",
        iteration_input_key = "task",
        template = flow.template()
            :func("process_task", {
                inputs = {required = ["task"]}
            })
            :to("@success")
    })
    :as("processor")
        :to("collector")
    
    :state({
        inputs = {required = ["default"]},
        output_mode = "array"
    })
    :as("collector")
        :to("summarize")
    
    :func("summarize"):to("@success")
    :run()
```

### Static Data with Multiple Routes

```lua
flow.create()
    :with_title("Multi-Source Processing")
    :with_input(user_task)
        :to("analyzer")
    
    :with_data(api_config):as("config")
        :to("api_client", "config")
        :to("logger", "config")
        :to("retry_handler", "config")
    
    :with_data(valid_categories):as("categories")
        :to("validator", "allowed")
    
    :func("analyzer"):as("analyzer")
        :to("api_client", "request")
    
    :func("api_client"):as("api_client")
        :to("@success")
        :error_to("retry_handler", "failed_request")
    
    :run()
```

### Clean Routing Pattern

```lua
flow.create()
    :with_input(task)
        :to("dev", "task")
    
    :with_data(branch):as("branch_info")
        :to("checker")
    
    :with_data(config):as("cfg")
        :to("dev", "config")
        :to("logger", "config")
    
    :func("checker"):as("checker")
        :to("dev", "check_result")
    
    :func("dev"):as("dev")
        :to("@success")
        :error_to("@fail")
    
    :run()
```

### Router Pattern (Single Input)

```lua
flow.create()
    :with_input(task)
        :to("router")  -- router gets raw string
    
    :func("keeper.agents.keeper_v2.develop:router"):as("router")
        :to("context", "routing")
        :to("dev", "routing")
    
    :agent("context_agent"):as("context")
        :to("dev", "gathered_context")
    
    :agent("dev_agent"):as("dev")
        :to("@success")
    
    :run()
```

Router handler must handle both raw string and table:
```lua
local function handler(input)
    local task = type(input) == "table" and input.task or input
    -- process task
end
```

### Using Args for Configuration

```lua
flow.create()
    :with_input(request_data)
        :to("client", "body")
    
    :func("api_client", {
        args = {
            base_url = "https://api.example.com",
            timeout = 5000,
            headers = {
                ["Authorization"] = "Bearer token",
                ["Content-Type"] = "application/json"
            }
        },
        inputs = {required = ["body"]},
        input_transform = {
            endpoint = '"/v1/process"',
            data = "inputs.body"
        }
    })
    :as("client")
    :to("@success")
    :error_to("@fail")
    
    :run()
```

### Parallel with Passthrough Keys

```lua
flow.create()
    :with_title("Process Files with Shared Config")
    
    :with_data(["doc1.pdf", "doc2.pdf", "doc3.pdf"]):as("files")
        :to("processor", "files")
    
    :with_data("https://api.example.com"):as("endpoint")
        :to("processor", "endpoint")
    
    :with_data("secret_key"):as("api_key")
        :to("processor", "api_key")
    
    :parallel({
        inputs = {required = ["files", "endpoint", "api_key"]},
        source_array_key = "files",
        iteration_input_key = "filename",
        passthrough_keys = {"endpoint", "api_key"},
        batch_size = 3,
        on_error = "continue",
        filter = "all",
        unwrap = false,
        template = flow.template()
            :func("parse_document", {
                inputs = {required = ["filename", "endpoint", "api_key"]},
                input_transform = {
                    file = "inputs.filename",
                    url = "inputs.endpoint",
                    key = "inputs.api_key"
                }
            })
            :to("@success")
    })
    :as("processor")
        :to("@success")
    
    :run()
```

### Conditional Routing with Array Checks

```lua
flow.create()
    :with_input(data)
        :to("decompose")
    
    :func("decompose"):as("decompose")
        :to("@success", nil, "{passed: true, feedback: nil}"):when("len(output.items) == 0")
        :to("processor", "items", "output.items")
    
    :parallel({
        inputs = {required = ["items"]},
        source_array_key = "items",
        template = flow.template()
            :func("process")
            :to("@success")
    })
    :as("processor")
        :to("@success")
    
    :run()
```

### Nested Workflow with Function

```lua
function qa_cycle(cycle_context)
    if cycle_context.last_result and cycle_context.last_result.approved then
        return {
            state = cycle_context.state,
            result = cycle_context.last_result,
            continue = false
        }
    end
    
    return flow.create()
        :with_input({
            task = cycle_context.input.task,
            feedback = cycle_context.state.feedback_history
        })
        :to("worker", "work_input")
        :to("qa", "context")
        
        :agent("namespace:worker", {
            inputs = {required = {"work_input"}},
            arena = {prompt = "Do work", exit_schema = {...}}
        })
        :as("worker")
        :to("qa", "work")
        :error_to("@fail")
        
        :agent("namespace:qa", {
            inputs = {required = {"work", "context"}},
            arena = {prompt = "Review work", exit_schema = {...}}
        })
        :as("qa")
        :to("@success")
        :error_to("@fail")
        
        :run()
end

flow.create()
    :with_input({task = "Complex task"})
    :cycle({
        func_id = "namespace:qa_cycle",
        max_iterations = 4,
        initial_state = {feedback_history = {}}
    })
    :to("@success")
    :error_to("@fail")
    :run()
```

### Async with Status Polling

```lua
local client = require("client")

local dataflow_id, err = flow.create()
    :with_title("Background Analytics")
    :with_metadata({
        project = "analytics",
        priority = "low"
    })
    :with_input(large_dataset)
    :func("namespace:process")
    :start()

if err then
    return nil, err
end

local c = client.new()
while true do
    local status = c:get_status(dataflow_id)
    if status == "completed" or status == "failed" then
        break
    end
    time.sleep(2000)
end

local outputs, err = c:output(dataflow_id)
```

## Validation Rules

The compiler validates workflows at compile-time and fails with clear error messages.

**Graph structure:**
- All `:as(name)` names must be unique
- All `:to()` and `:error_to()` targets must exist (except `@success`, `@fail`)
- Graph must be acyclic
- All nodes must have incoming routes (from another node, workflow input, or static data)

**Node requirements:**
- `:cycle()` needs `func_id` OR `template` (not both)
- `:parallel()` requires `source_array_key` and `template`
- Terminal routes required: at least one path must lead to `@success` or have auto-output

**Routing constraints:**
- `:when()` only follows `:to()` or `:error_to()` from nodes
- `:when()` cannot be used with static data routes
- `:start()` cannot be used in nested contexts

**Configuration:**
- `:with_title()` requires non-empty string
- `:with_metadata()` requires table
- `:with_data()` can be called multiple times
- `:with_input()` should be called at most once

## Compile-Time Error Messages

**Dead nodes (no incoming routes):**
```
Dead nodes detected (no incoming routes): Processor (a4d8527eec7c), Logger (79f0052e0783). 
All nodes must either receive data from another node, workflow input, or static data.
```

**Missing success path:**
```
Workflow has no success termination path. Node(s) with :error_to() but no :to() route: Validator (cd687a929f19). 
Add :to("@success") to at least one node to complete the workflow on success.
```

**Conditional on static data:**
```
Cannot use :when() with static data routes. Static data is constant and conditions would always evaluate the same way.
```

**Undefined reference:**
```
Undefined node reference: processor_node
```

**Cycle detected:**
```
Cycle detected: node_a -> node_b -> node_c -> node_a
```

## Error Handling

Both `:run()` and `:start()` follow standard Lua error conventions:

**Success:**
- `:run()` → `data, nil`
- `:start()` → `dataflow_id, nil`

**Failure:**
- `:run()` → `nil, error_message`
- `:start()` → `nil, error_message`

**Error categories:**
- Compilation errors: "Compilation failed: ..."
- Client errors: "Failed to create dataflow client: ..."
- Workflow creation errors: "Failed to create workflow: ..."
- Execution errors: "Failed to execute workflow: ..."
- Workflow failures: Returns workflow error message directly

## Critical Requirements for Functions

Functions that use Flow Builder must return `flow.create()...:run()`:

```lua
function my_processor(input)
    return flow.create()
        :with_input(input)
        :func("process")
        :run()
end
```

This enables:
- Workflow chaining through `{_control = {commands = [...]}}`
- Nested workflow execution in cycles and parallel
- Child workflow spawning from parent workflows

Functions returning anything else cannot participate in dataflow composition.