#!/usr/bin/env python3
"""Assert failure records from the native workflow reporter's console log."""
import json
import os
from pathlib import Path
import subprocess
import sys

result = subprocess.run(
    [os.environ.get("WIPPY", "wippy"), "test", "-v", "test", "app:wait_for_boot", "app.await_authority:setup", "app:failure_log_test", *sys.argv[1:]],
    cwd=Path(__file__).resolve().parents[1] / "test",
    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
)
print(result.stdout, end="")
assert result.returncode == 0, "native failure reporter test fails"
records = []
for line in result.stdout.splitlines():
    start = line.find("{")
    if start < 0:
        continue
    try:
        fields = json.loads(line[start:])
    except json.JSONDecodeError:
        continue
    records.append((line[:start], fields))
probes = [fields for prefix, fields in records if "failure log probe" in prefix]
assert len(probes) == 4, "native failure reporter emits its identifiers"
for probe in probes:
    failures = [(prefix, fields) for prefix, fields in records
                if fields.get("dataflow_id") == probe["dataflow_id"] and ("WARN" in prefix or "ERROR" in prefix)]
    node_failures = [(prefix, fields) for prefix, fields in failures if fields.get("node_id") == probe["node_id"]]
    assert len(node_failures) == 1, f"expected one node failure record, got {node_failures}"
    prefix, fields = node_failures[0]
    assert "WARN" in prefix
    assert fields["node_type"] == probe["node_type"]
    assert fields["error_kind"] == probe["error_kind"]
    assert fields["error_message"] == probe.get("node_message", "Service unavailable")
    workflow_failures = [(prefix, fields) for prefix, fields in failures if "node_id" not in fields]
    assert len(workflow_failures) == int(probe["workflow_failed"]), f"unexpected workflow failure records: {workflow_failures}"
    for prefix, fields in workflow_failures:
        assert "ERROR" in prefix
        assert fields["error_kind"] == "WorkflowFailed"
        assert probe.get("workflow_message", "Service unavailable") in fields["error_message"]
    assert "private-error-detail" not in "\n".join(prefix + json.dumps(fields) for prefix, fields in failures)
print("Failure log proof passes")
