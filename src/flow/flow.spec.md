# Dataflow Flow Builder SDK - Specification

## Overview

Flow Builder SDK provides a fluent interface for composing acyclic dataflows. Iteration exists only as encapsulated subgraphs via cycle nodes.

**Core Principles:**
- **Acyclic Graph**: Forward-only routing with explicit edges
- **Input-First**: `:with_input()` establishes flow direction
- **Transform-on-Route**: Transforms apply at routing points
- **Explicit Routing**: `:to()` for success, `:error_to()` for errors
- **Terminal Nodes**: `@success` and `@fail` for explicit workflow completion
- **Conditional Logic**: `:when()` applies to preceding route
- **Optional Naming**: `:as()` only when references needed
- **Template Reuse**: `:use()` inlines, `flow.template()` defines

## Core API

```lua
local flow = require("userspace.dataflow.flow")

flow.create()
    :with_title(title)
    :with_metadata(metadata)
    :with_input(data)
    :[operation](config)
    :as(name)
    :to(target, input_key, transform)
    :error_to(target, input_key, transform)
    :when(condition)
    :run()  -- or :start()

flow.template()
    :[operations]...
```

## Workflow Configuration

### Title
```lua
flow.create()
    :with_title("Data Processing Pipeline")
```

Sets workflow title (defaults to "Flow Builder Workflow").

### Metadata
```lua
flow.create()
    :with_metadata({ project = "analytics", version = "1.0" })
```

Sets custom workflow metadata.

### Execution Modes

**Synchronous (`:run()`)** - Blocks, returns `data, nil` or `nil, error`

**Asynchronous (`:start()`)** - Returns `dataflow_id, nil` immediately, workflow runs in background

## Routing

### Automatic Sequential
Without explicit `:to()`, outputs auto-chain to next node. Using `:to()` disables auto-chain for that node only.

### Terminal Nodes
Explicit workflow completion:

```lua
:func("process")
    :to("@success")              -- Terminates workflow successfully
    :error_to("@fail")            -- Terminates workflow with error
```

**Aliases:** `@end` is context-sensitive (`@success` in `:to()`, `@fail` in `:error_to()`).

**Required:** Workflows must have at least one success path. If a node has `:error_to()` but no `:to()`, you must add `:to("@success")` or let it auto-chain. Compiler validates this.

In nested contexts (cycles, map-reduce templates), terminal routes create `NODE_OUTPUT` instead of `WORKFLOW_OUTPUT`, properly returning to parent.

### Input Key Routing
```lua
:func("router")
    :to("nodeA", "primary")
    :to("nodeB", "fallback")
```

### Conditional
```lua
:func("analyzer")
    :to("high"):when("output.score > 0.8")
    :to("medium"):when("output.score > 0.5")
    :to("low")
    :error_to("failed")
```

### Transform on Routes
Apply transforms when routing:

```lua
:func("source")
    :to("target"):transform("output.data.field")
    :error_to("@fail"):transform("error.message")
```

Workflow input routing with transforms:

```lua
flow.create()
    :with_input({prompt = "task", data = [1,2,3]})
    :to("agent"):transform("input.prompt")
    :to("processor"):transform("input.data")
```

**Transform context variables:**
- For data routes from nodes: `output` (node's output)
- For error routes: `error` (error object)
- For workflow input routes: `input` (workflow input data)

## Expressions

Transform and condition expressions use the `expr` module.

**Transform context:**
- `output`: Previous node output (for `:to()`)
- `error`: Error object (for `:error_to()`)
- `input`: Workflow input (for routing from `:with_input()`)

**Condition context:**
- `output`: Node output (for `:to().when()`)
- `error`: Error object (for `:error_to().when()`)

**Operators:** `+`, `-`, `*`, `/`, `%`, `**`, `==`, `!=`, `<`, `<=`, `>`, `>=`, `&&`, `||`, `!`, `?:`

**Functions:** `contains()`, `startsWith()`, `endsWith()`, `upper()`, `lower()`, `trim()`, `filter()`, `map()`, `all()`, `any()`, `len()`, `first()`, `last()`, `max()`, `min()`, `abs()`, `ceil()`, `floor()`, `round()`, `sqrt()`, `pow()`

**Features:** Optional chaining `?.`, null coalescing `??`, array access `[0]`

## Node Configuration

### Input Transform
Nodes can declare `input_transform` to transform inputs on read:

```lua
:func("processor", {
    inputs = {required = ["data", "context"]},
    input_transform = {
        processed_data = "inputs.data.items",
        ctx_summary = "inputs.context.summary"
    }
})
```

Single expression transforms all inputs:
```lua
:func("processor", {
    input_transform = "input.nested.value"
})
```

Multi-key transform creates named variables for function access.

## Node Types

### Function
```lua
:func("namespace:func_id", {
    inputs = {required = ["key1", "key2"]},
    context = {key = value},
    input_transform = "input.data" or {k1 = "expr1", k2 = "expr2"},
    metadata = {title = "Custom"}
})
```

Functions can return `{_control = {commands = [...]}}` to spawn child graphs.

### Agent
```lua
:agent("namespace:agent_id", {
    inputs = {required = ["input1"]},
    model = "custom-model",
    input_transform = {field = "inputs.input1.data"},
    arena = {
        prompt = "System prompt",
        max_iterations = 10,
        min_iterations = 1,
        tool_calling = "auto",
        exit_schema = {...},
        tools = [...],
        context = {...}
    },
    show_tool_calls = true,
    metadata = {title = "Agent"}
})
```

**Reserved input keys:**
- `agent_id`: Overrides configured agent (must be string)
- `context`: Extracted and merged into agent's execution context (must be table, not included in XML-formatted inputs)
- `model`: Dynamic model overwrite (must be string)

**Tool calling modes:**
- `auto`: Optional tools, exit on text response or exit tool
- `any`: Must call tool (requires exit_schema for exit tool)
- `none`: No tools (cannot have exit_schema)

**Exit tool:** Automatically generated as `finish` when exit_schema provided

**Multiple inputs formatted as XML:**
```xml
<input key="task">Process this</input>
<input key="data">Sample data</input>
```

**Defaults:**
- `max_iterations`: 32
- `min_iterations`: 1
- `tool_calling`: "auto"
- `show_tool_calls`: true

### Cycle

Cycles iterate work until completion. Choose **either** `func_id` **or** `template` (not both).

**With function:**
```lua
:cycle({
    func_id = "namespace:cycle_func",
    max_iterations = 5,
    initial_state = {quality = 0.3},
    inputs = {required = ["data"]},
    input_transform = "input.nested",
    context = {key = value}
})
```

**With template:**
```lua
:cycle({
    template = flow.template()
        :func("namespace:step1")
        :func("namespace:step2")
        :func("namespace:collector"),
    max_iterations = 3,
    initial_state = {score = 0}
})
```

**Cycle function receives:**
```lua
{
    iteration = 1,
    input = original_input,
    state = {...},
    last_result = {...}
}
```

**Cycle function must return:**
```lua
{
    state = {...},
    result = {...},
    continue = bool,
    _control = {
        commands = {...}
    },
    _metadata = {...}
}
```

**Template cycle requirement:**
When using `template`, the template's **leaf node** must return the cycle structure. Template roots receive the cycle context `{iteration, input, state, last_result}` as their input.

**Continuation logic:**
1. If return value has `continue` field, use that value
2. Otherwise, continue if `state` changed from previous iteration
3. Always stop at `max_iterations` and return the final iteration's `result`

**Defaults:**
- `max_iterations`: 100
- `initial_state`: {}

### Map-Reduce
```lua
:map_reduce({
    source_array_key = "items",
    iteration_input_key = "default",
    batch_size = 4,
    failure_strategy = "collect_errors",
    inputs = {required = ["items", "config"]},
    input_transform = {arr = "inputs.items", cfg = "inputs.config"},
    template = flow.template()
        :func("namespace:process"),
    item_steps = {
        {type = "map", func_id = "namespace:transform"},
        {type = "filter", func_id = "namespace:keep"}
    },
    reduction_extract = "successes",
    reduction_steps = {
        {type = "map", func_id = "namespace:transform"},
        {type = "filter", func_id = "namespace:keep"},
        {type = "group", key_func_id = "namespace:key"},
        {type = "reduce_groups", func_id = "namespace:reduce"},
        {type = "aggregate", func_id = "namespace:combine"},
        {type = "flatten", func_id = "namespace:flatten"}
    }
})
```

**Without reduction:**
```lua
{
    successes = {{iteration, item, result}, ...},
    failures = {{iteration, item, error}, ...},
    total_iterations = N,
    success_count = N,
    failure_count = N
}
```

**With `reduction_extract = "successes"`:** `[result1, result2, ...]`

**Item pipeline steps:**
- `map`: Transform each result
- `filter`: Keep/reject results (returning nil removes item)

**Reduction pipeline steps:**
- `map`: Transform array items
- `filter`: Keep array items
- `group`: Group array by key into object
- `reduce_groups`: Reduce each group
- `aggregate`: Combine data into single result
- `flatten`: Custom flattening logic

**Pipeline data type flow:**
- Extract: `array`
- After `group`: `grouped_object`
- After `reduce_groups`: `object`
- After `aggregate`/`flatten`: `any`

**Validation:** `map`, `filter`, `group` require array input. Use `aggregate` for object inputs.

**Defaults:**
- `batch_size`: 1
- `iteration_input_key`: "default"
- `failure_strategy`: "fail_fast"

### Join
```lua
:join({
    inputs = {required = ["source1", "source2"]},
    input_transform = {data = "inputs.source1", ctx = "inputs.source2"},
    metadata = {title = "Merge Data"}
})
```

Join collects and merges multiple inputs into a single output. The output structure depends on the inputs received:

**Single input or single default key:**
```lua
:func("source"):to("merger", "default")
:join():as("merger")
-- Output: <content from source>
```

**Multiple named inputs:**
```lua
:func("source1"):to("merger", "data")
:func("source2"):to("merger", "config")
:join():as("merger")
-- Output: {data = <from source1>, config = <from source2>}
```

**Mixed default and named inputs:**
```lua
:func("source1"):to("merger", "default")
:func("source2"):to("merger", "config")
:join():as("merger")
-- Output: {default = <from source1>, config = <from source2>}
```

Join waits for all required inputs before proceeding. Use `input_transform` to reshape the merged data.

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

### Sequential Pipeline
```lua
flow.create()
    :with_input(doc)
    :func("namespace:clean")
    :func("namespace:analyze")
    :func("namespace:summarize")
    :run()
```

### Terminal Routing with Error Details
```lua
flow.create()
    :with_input(data)
    :func("namespace:validate")
        :to("process"):when("output.valid")
        :error_to("@fail")
    :func("namespace:process"):as("process")
        :to("@success")
        :error_to("@fail")
    :run()
```

### Transform on Routes
```lua
flow.create()
    :with_input({prompt = "task", context = {...}})
    :to("agent"):transform("input.prompt")
    :to("logger"):transform("input.context")
    
    :agent("namespace:agent"):as("agent")
        :to("processor"):transform("output.result.data")
        :error_to("@fail"):transform("error.message")
    
    :func("namespace:processor"):as("processor")
        :to("@success")
    
    :func("namespace:logger"):as("logger")
    :run()
```

### Input Transform in Node Config
```lua
flow.create()
    :with_input({prompt = "x", data = "y", context = "z"})
    :to("fetch", "ctx")
    :to("processor", "original")
    
    :func("namespace:fetch"):as("fetch")
        :to("processor", "fetched")
    
    :func("namespace:processor", {
        inputs = {required = ["original", "fetched", "ctx"]},
        input_transform = {
            task = "inputs.original.prompt",
            data = "inputs.fetched.results",
            context = "inputs.ctx"
        }
    })
    :as("processor")
    :to("@success")
    :run()
```

### Join Pattern for Parallel Processing
```lua
flow.create()
    :with_input({document = "..."})
    :to("analyze", "doc")
    :to("extract", "doc")
    
    :func("namespace:analyze"):as("analyze")
        :to("merger", "analysis")
    
    :func("namespace:extract"):as("extract")
        :to("merger", "entities")
    
    :join({
        inputs = {required = ["analysis", "entities"]},
        input_transform = {
            summary = "inputs.analysis.summary",
            entities = "inputs.entities.list"
        }
    })
    :as("merger")
    :to("@success")
    :run()
```

### Async Execution
```lua
local dataflow_id, err = flow.create()
    :with_title("Long Running Task")
    :with_input(large_dataset)
    :func("namespace:process")
    :to("@success")
    :start()

-- Poll status
local client = require("client")
local c = client.new()
local status = c:get_status(dataflow_id)
local outputs = c:output(dataflow_id)
```

## Validation Rules

- `:as(name)` names must be unique
- All `:to()` and `:error_to()` targets must exist (except `@success`, `@fail`, `@end`)
- Graph must be acyclic
- `:cycle()` needs `func_id` OR `template` (not both)
- `:map_reduce()` requires `source_array_key`
- `:when()` only follows `:to()` or `:error_to()`
- Map-reduce pipelines: validate data type compatibility
- Always return `flow()...:run()` from functions
- Terminal routes (`@success`, `@fail`) automatically adapt to context (top-level vs nested)
- **Workflows must have at least one success termination path** - compiler validates this at build time
- `:start()` cannot be used in nested contexts (cycles, map-reduce)
- `:with_title()` requires non-empty string
- `:with_metadata()` requires table

## Error Handling

Both `:run()` and `:start()` follow standard Lua error conventions:

**Success:**
- `:run()` → `data, nil`
- `:start()` → `dataflow_id, nil`

**Failure:**
- `:run()` → `nil, error_message`
- `:start()` → `nil, error_message`

**Error types:**
- Compilation errors: "Compilation failed: ..."
- Validation errors: "Workflow has no success termination path..."
- Client errors: "Failed to create dataflow client: ..."
- Workflow creation errors: "Failed to create workflow: ..."
- Execution errors (run): "Failed to execute workflow: ..."
- Startup errors (start): "Failed to start workflow: ..."
- Workflow failures (run): Returns workflow error message directly
