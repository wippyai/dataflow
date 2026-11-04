#!/bin/sh
set -eu

repo_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
test_dir="$repo_dir/test"
output_dir="${DATAFLOW_RESTART_DIR:-$test_dir/.wippy}"
db_file="$output_dir/restart-proof.db"
phase1_log="$output_dir/restart-phase1.log"
phase2_log="$output_dir/restart-phase2.log"
phase3_log="$output_dir/restart-phase3.log"
runtime_pid=""
restart_wippy="${WIPPY:-wippy}"
dialect="${DATAFLOW_RESTART_DIALECT:-sqlite}"
scenario="${DATAFLOW_RESTART_SCENARIO:-rolling}"
case "$scenario" in rolling|behavior|parallel_tools|stable_prefix|round_children|round_delegate|round_both|round_rerun|round_double_kill|round_yield|round_settle|round_yield_commands|round_artifact|round_command_delegate|round_yield_delegate|round_staggered_children|round_output_only) ;; *) echo "unsupported restart scenario" >&2; exit 1 ;; esac
pg_host="${DATAFLOW_PG_HOST:-127.0.0.1}"
pg_port="${DATAFLOW_PG_PORT:-5432}"
pg_database="${DATAFLOW_PG_DATABASE:-dataflow_restart_test}"
pg_username="${DATAFLOW_PG_USERNAME:-dataflow}"
pg_password="${DATAFLOW_PG_PASSWORD:-dataflow}"

case "$dialect" in
    sqlite|postgres) ;;
    *) echo "unsupported restart proof dialect: $dialect" >&2; exit 1 ;;
esac
case "$pg_database" in
    *[!A-Za-z0-9_]*) echo "unsafe PostgreSQL restart database name" >&2; exit 1 ;;
esac

stop_runtime() {
    pid="$1"
    if ! kill -0 "$pid" 2>/dev/null; then
        wait "$pid" 2>/dev/null || true
        return
    fi
    kill -TERM "$pid" 2>/dev/null || true
    attempts=0
    while kill -0 "$pid" 2>/dev/null && [ "$attempts" -lt 100 ]; do
        sleep 0.1
        attempts=$((attempts + 1))
    done
    if kill -0 "$pid" 2>/dev/null; then
        kill -KILL "$pid" 2>/dev/null || true
    fi
    wait "$pid" 2>/dev/null || true
}

cleanup() {
    if [ -n "$runtime_pid" ]; then stop_runtime "$runtime_pid"; fi
}
trap cleanup EXIT INT TERM

query() {
    statement="$1"
    if [ "$dialect" = "postgres" ]; then
        PGPASSWORD="$pg_password" psql -X -qAt \
            -h "$pg_host" -p "$pg_port" -U "$pg_username" -d "$pg_database" \
            -c "$statement"
    else
        sqlite3 "$db_file" "$statement"
    fi
}

start_runtime() {
    restart_profile="$1"
    runtime_log="$2"
    case "$scenario" in
        behavior) set -- --profile restart_behavior ;;
        parallel_tools) set -- --profile restart_parallel_tools ;;
        stable_prefix) set -- --profile restart_stable_prefix ;;
        round_double_kill)
            set -- --profile restart_round_rerun \
                --override "app:restart_probe.create:method=create_round_double_kill" ;;
        round_command_delegate|round_yield_delegate|round_staggered_children|round_output_only)
            set -- --profile restart_round_children \
                --override "app:restart_probe.create:method=create_$scenario" \
                --override "app:restart_probe.observe:method=observe_$scenario" ;;
        round_*) set -- --profile "restart_$scenario" ;;
        *) set -- ;;
    esac
    if [ "$dialect" = "postgres" ]; then
        "$restart_wippy" run -s --config "${DATAFLOW_RESTART_CONFIG:-.wippy.yaml}" --profile postgres --profile "$restart_profile" "$@" \
            --set "vars.postgres_host=$pg_host" \
            --set "vars.postgres_port=$pg_port" \
            --set "vars.postgres_database=$pg_database" \
            --set "vars.postgres_username=$pg_username" \
            --set "vars.postgres_password=$pg_password" >"$runtime_log" 2>&1 &
    else
        "$restart_wippy" run -s --config "${DATAFLOW_RESTART_CONFIG:-.wippy.yaml}" --profile sqlite --profile "$restart_profile" "$@" \
            --set "vars.sqlite_file=$db_file" >"$runtime_log" 2>&1 &
    fi
    runtime_pid=$!
}

rm -f "$db_file" "$db_file-wal" "$db_file-shm" "$phase1_log" "$phase2_log" "$phase3_log"

if [ "$dialect" = "postgres" ]; then
    PGPASSWORD="$pg_password" dropdb --if-exists --force \
        -h "$pg_host" -p "$pg_port" -U "$pg_username" "$pg_database"
    PGPASSWORD="$pg_password" createdb \
        -h "$pg_host" -p "$pg_port" -U "$pg_username" "$pg_database"
fi

cd "$test_dir"
start_runtime restart_create "$phase1_log"

phase1=""
attempts=0
while [ "$attempts" -lt 200 ]; do
    if [ "$scenario" = "parallel_tools" ]; then
        # The fast call's observation is committed while the slow call of the
        # same turn has no observation yet.
        if [ "$dialect" = "postgres" ]; then
            fast_call="d.metadata->>'tool_call_id' = 'call-parallel-restart-1'"
            slow_call="d.metadata->>'tool_call_id' = 'call-parallel-restart-2'"
        else
            fast_call="json_extract(d.metadata, '$.tool_call_id') = 'call-parallel-restart-1'"
            slow_call="json_extract(d.metadata, '$.tool_call_id') = 'call-parallel-restart-2'"
        fi
        phase1=$(query "SELECT a.dataflow_id || '|' || a.owner_epoch
            FROM dataflow_activations a
            WHERE a.desired_active
              AND EXISTS (SELECT 1 FROM dataflow_data d WHERE d.dataflow_id = a.dataflow_id
                          AND d.type = 'agent.observation' AND $fast_call)
              AND NOT EXISTS (SELECT 1 FROM dataflow_data d WHERE d.dataflow_id = a.dataflow_id
                              AND d.type = 'agent.observation' AND $slow_call)
            LIMIT 1;" 2>/dev/null || true)
    elif [ "$scenario" = "stable_prefix" ]; then
        # The switch is committed and the first request of the new agent is in flight,
        # with no action stored for it.
        if [ "$dialect" = "postgres" ]; then
            target_predicate="n.config->>'agent' = 'userspace.dataflow.node.agent.stub:stable_prefix_switch_target_agent'"
        else
            target_predicate="json_extract(n.config, '$.agent') = 'userspace.dataflow.node.agent.stub:stable_prefix_switch_target_agent'"
        fi
        phase1=$(query "SELECT a.dataflow_id || '|' || a.owner_epoch
            FROM dataflow_activations a JOIN dataflow_nodes n ON n.dataflow_id = a.dataflow_id
            WHERE a.desired_active AND n.status = 'running' AND $target_predicate
              AND (SELECT COUNT(*) FROM dataflow_data d WHERE d.node_id = n.node_id AND d.type = 'agent.action') = 1
              AND EXISTS (SELECT 1 FROM restart_recovery_store s WHERE s.store_key = 'agent_recovery:prefix-restart:request:2')
            LIMIT 1;" 2>/dev/null || true)
    elif [ "$scenario" = "round_settle" ]; then
        # The call's result is committed and after_execute holds the round open.
        phase1=$(query "SELECT a.dataflow_id || '|' || a.owner_epoch
            FROM dataflow_activations a
            WHERE a.desired_active
              AND EXISTS (SELECT 1 FROM dataflow_data d WHERE d.dataflow_id = a.dataflow_id AND d.type = 'agent.observation')
              AND EXISTS (SELECT 1 FROM restart_recovery_store s WHERE s.store_key = 'agent_recovery:round-settle:wrapper_attempts')
            LIMIT 1;" 2>/dev/null || true)
    elif [ "$scenario" = "round_command_delegate" ] || [ "$scenario" = "round_staggered_children" ] \
        || [ "$scenario" = "round_output_only" ]; then
        completed_child=""
        if [ "$scenario" = "round_staggered_children" ] || [ "$scenario" = "round_output_only" ]; then
            completed_child="AND EXISTS (SELECT 1 FROM dataflow_data r JOIN dataflow_nodes c ON c.node_id = r.node_id
                WHERE c.dataflow_id = a.dataflow_id AND c.parent_node_id IS NOT NULL
                  AND c.type = 'userspace.dataflow.node.func:node' AND c.status = 'completed'
                  AND r.type = 'node.result')"
        fi
        phase1=$(query "SELECT a.dataflow_id || '|' || a.owner_epoch
            FROM dataflow_activations a WHERE a.desired_active
              $completed_child
              AND EXISTS (SELECT 1 FROM dataflow_nodes c WHERE c.dataflow_id = a.dataflow_id
                          AND c.parent_node_id IS NOT NULL AND c.type = 'userspace.dataflow.node.func:node'
                          AND c.status = 'running')
              AND EXISTS (SELECT 1 FROM dataflow_nodes c WHERE c.dataflow_id = a.dataflow_id
                          AND c.parent_node_id IS NOT NULL AND c.type = 'userspace.dataflow.node.agent:node'
                          AND c.status = 'running')
            LIMIT 1;" 2>/dev/null || true)
    elif [ "${scenario#round_}" != "$scenario" ]; then
        # The round's commit is durable and its slow child is running.
        phase1=$(query "SELECT a.dataflow_id || '|' || a.owner_epoch
            FROM dataflow_activations a
            WHERE a.desired_active
              AND EXISTS (SELECT 1 FROM dataflow_nodes c WHERE c.dataflow_id = a.dataflow_id
                          AND c.parent_node_id IS NOT NULL AND c.status = 'running')
            LIMIT 1;" 2>/dev/null || true)
    elif [ "$scenario" = "behavior" ]; then
        if [ "$dialect" = "postgres" ]; then
            pending_predicate="n.metadata #>> '{behavior_pending,iteration}' = '1'"
        else
            pending_predicate="json_extract(n.metadata, '$.behavior_pending.iteration') = 1"
        fi
        phase1=$(query "SELECT a.dataflow_id || '|' || a.owner_epoch
            FROM dataflow_activations a JOIN dataflow_nodes n ON n.dataflow_id = a.dataflow_id
            WHERE a.desired_active AND n.status = 'running' AND $pending_predicate
              AND EXISTS (SELECT 1 FROM dataflow_data d WHERE d.node_id = n.node_id AND d.type = 'agent.action')
              AND NOT EXISTS (SELECT 1 FROM dataflow_data d WHERE d.node_id = n.node_id AND d.type = 'agent.observation')
            LIMIT 1;" 2>/dev/null || true)
    else
    phase1=$(query "
        SELECT a.dataflow_id || '|' || a.owner_epoch
        FROM dataflow_activations a
        JOIN dataflow_data y ON y.dataflow_id = a.dataflow_id AND y.type = 'node.yield'
        WHERE a.desired_active
          AND (SELECT COUNT(*) FROM dataflow_nodes c
               WHERE c.dataflow_id = a.dataflow_id
                 AND c.parent_node_id IS NOT NULL
                 AND c.status = 'running') = 2
        LIMIT 1;
    " 2>/dev/null || true)
    fi
    [ -n "$phase1" ] && break
    if ! kill -0 "$runtime_pid" 2>/dev/null; then
        echo "first runtime exited before creating an active workflow" >&2
        tail -80 "$phase1_log" >&2 || true
        exit 1
    fi
    attempts=$((attempts + 1))
    sleep 0.1
done
if [ -z "$phase1" ] && [ "$scenario" = "parallel_tools" ]; then
    echo "timed out waiting for the fast call's observation to commit while the slow call runs" >&2
    exit 1
fi
[ -n "$phase1" ] || { echo "timed out waiting for first runtime ownership" >&2; exit 1; }

dataflow_id=${phase1%%|*}
first_epoch=${phase1#*|}
[ -n "$first_epoch" ] || { echo "first runtime did not claim an epoch" >&2; exit 1; }

if [ "$scenario" = "rolling" ]; then
if [ "$dialect" = "postgres" ]; then
    rolling_yield=$(query "
        SELECT COUNT(*) FROM dataflow_data
        WHERE dataflow_id = '$dataflow_id' AND type = 'node.yield'
          AND convert_from(content, 'UTF8')::jsonb #>> '{yield_context,completion_policy}' = 'any_group'
          AND convert_from(content, 'UTF8')::jsonb #>> '{yield_context,concurrency_group_key}' = 'iteration'
          AND convert_from(content, 'UTF8')::jsonb #>> '{yield_context,max_concurrent_nodes}' = '2';
    ")
else
    rolling_yield=$(query "
        SELECT COUNT(*) FROM dataflow_data
        WHERE dataflow_id = '$dataflow_id' AND type = 'node.yield'
          AND json_extract(CAST(content AS TEXT), '$.yield_context.completion_policy') = 'any_group'
          AND json_extract(CAST(content AS TEXT), '$.yield_context.concurrency_group_key') = 'iteration'
          AND json_extract(CAST(content AS TEXT), '$.yield_context.max_concurrent_nodes') = 2;
    ")
fi
[ "$rolling_yield" -ge 1 ] || { echo "rolling concurrency contract was not persisted" >&2; exit 1; }

# The fast second iteration must release a slot while iteration one is still
# running. This distinguishes a rolling window from the legacy wave barrier and
# ensures the restart below interrupts a genuinely occupied rolling cursor.
rolling_refilled=""
attempts=0
while [ "$attempts" -lt 100 ]; do
    if [ "$dialect" = "postgres" ]; then
        rolling_refilled=$(query "
            SELECT CASE WHEN
              EXISTS (SELECT 1 FROM dataflow_nodes
                      WHERE dataflow_id = '$dataflow_id' AND status = 'running'
                        AND (metadata->>'iteration')::int = 1)
              AND EXISTS (SELECT 1 FROM dataflow_nodes
                          WHERE dataflow_id = '$dataflow_id' AND status IN ('running', 'completed')
                            AND (metadata->>'iteration')::int >= 3)
            THEN 1 ELSE 0 END;
        ")
    else
        rolling_refilled=$(query "
            SELECT CASE WHEN
              EXISTS (SELECT 1 FROM dataflow_nodes
                      WHERE dataflow_id = '$dataflow_id' AND status = 'running'
                        AND json_extract(metadata, '$.iteration') = 1)
              AND EXISTS (SELECT 1 FROM dataflow_nodes
                          WHERE dataflow_id = '$dataflow_id' AND status IN ('running', 'completed')
                            AND json_extract(metadata, '$.iteration') >= 3)
            THEN 1 ELSE 0 END;
        ")
    fi
    [ "$rolling_refilled" = "1" ] && break
    attempts=$((attempts + 1))
    sleep 0.1
done
[ "$rolling_refilled" = "1" ] || { echo "rolling window did not refill before the slow iteration completed" >&2; exit 1; }

fi
if [ "$scenario" = "behavior" ] || [ "$scenario" = "parallel_tools" ] || [ "$scenario" = "stable_prefix" ] \
    || [ "${scenario#round_}" != "$scenario" ]; then
    # Interrupt only this disposable runtime while its persisted round is pending.
    kill -KILL "$runtime_pid"
    wait "$runtime_pid" 2>/dev/null || true
else
    stop_runtime "$runtime_pid"
fi
runtime_pid=""

if [ "$scenario" = "round_output_only" ]; then
    # Seed the output-before-result window; the two commits have no deterministic pause point.
    child_id=$(query "SELECT node_id FROM dataflow_nodes WHERE dataflow_id = '$dataflow_id'
        AND parent_node_id IS NOT NULL AND type = 'userspace.dataflow.node.func:node' AND status = 'completed' LIMIT 1;")
    [ -n "$child_id" ] || { echo "no completed command child to seed" >&2; exit 1; }
    query "BEGIN;
        DELETE FROM dataflow_data WHERE node_id = '$child_id' AND type = 'node.result';
        UPDATE dataflow_nodes SET status = 'running' WHERE node_id = '$child_id';
        COMMIT;"
    output_only=$(query "SELECT COUNT(*) FROM dataflow_data WHERE node_id = '$child_id' AND type = 'node.output';")
    [ "$output_only" = "1" ] || { echo "seeded child has no durable output" >&2; exit 1; }
    echo "seeded output-before-result rows for command child $child_id"
fi

preserved=$(query "
    SELECT COUNT(*) FROM dataflow_activations a
    JOIN dataflows d ON d.dataflow_id = a.dataflow_id
    WHERE a.dataflow_id = '$dataflow_id'
      AND a.generation = 1
      AND a.desired_active
      AND d.status = 'running';
")
[ "$preserved" = "1" ] || { echo "graceful runtime shutdown destroyed active intent" >&2; exit 1; }

if [ "$scenario" = "stable_prefix" ]; then
    # An input that arrives after the first request must not change the user message.
    if [ "$dialect" = "postgres" ]; then
        late_content="convert_to('late-input-text', 'UTF8')"
        now_value="now()"
    else
        late_content="CAST('late-input-text' AS BLOB)"
        now_value="CAST(strftime('%s', 'now') AS INTEGER)"
    fi
    query "INSERT INTO dataflow_data (data_id, dataflow_id, node_id, type, key, content, content_type, created_at)
        SELECT '019a0000-0000-7000-8000-000000000001', n.dataflow_id, n.node_id, 'node.input', 'arrived_later',
               $late_content, 'text/plain', $now_value
        FROM dataflow_nodes n WHERE n.dataflow_id = '$dataflow_id';"
fi

start_runtime restart_observe "$phase2_log"

second_epoch=""
attempts=0
while [ "$attempts" -lt 200 ]; do
    second_epoch=$(query "
        SELECT owner_epoch FROM dataflow_activations
        WHERE dataflow_id = '$dataflow_id'
          AND owner_epoch IS NOT NULL
          AND owner_epoch <> '$first_epoch';
    " 2>/dev/null || true)
    [ -n "$second_epoch" ] && break
    if ! kill -0 "$runtime_pid" 2>/dev/null; then
        echo "second runtime exited before epoch takeover" >&2
        tail -80 "$phase2_log" >&2 || true
        exit 1
    fi
    attempts=$((attempts + 1))
    sleep 0.1
done
[ -n "$second_epoch" ] || { echo "new runtime epoch did not take ownership" >&2; exit 1; }

if [ "$scenario" = "round_double_kill" ]; then
    if [ "$dialect" = "postgres" ]; then
        result_call="d.metadata->>'tool_call_id'"
        metric_text="convert_from(s.store_value, 'UTF8')"
    else
        result_call="json_extract(d.metadata, '$.tool_call_id')"
        metric_text="CAST(s.store_value AS TEXT)"
    fi
    interrupted=""
    attempts=0
    while [ "$attempts" -lt 200 ]; do
        interrupted=$(query "SELECT COUNT(*) FROM dataflow_nodes n
            WHERE n.dataflow_id = '$dataflow_id' AND n.parent_node_id IS NULL
              AND EXISTS (SELECT 1 FROM dataflow_data d WHERE d.node_id = n.node_id
                  AND d.type = 'agent.observation' AND $result_call = 'call-round-double-kill-2')
              AND EXISTS (SELECT 1 FROM restart_recovery_store s
                  WHERE s.store_key = 'agent_recovery:round-double-kill:wrapper_attempts' AND $metric_text = '2');" 2>/dev/null || true)
        [ "$interrupted" = "1" ] && break
        attempts=$((attempts + 1))
        sleep 0.1
    done
    [ "$interrupted" = "1" ] || { echo "recovery did not reach the second crash boundary" >&2; exit 1; }
    kill -KILL "$runtime_pid"
    wait "$runtime_pid" 2>/dev/null || true
    runtime_pid=""
    killed_epoch="$second_epoch"
    start_runtime restart_observe "$phase3_log"
    second_epoch=""
    attempts=0
    while [ "$attempts" -lt 200 ]; do
        second_epoch=$(query "SELECT owner_epoch FROM dataflow_activations
            WHERE dataflow_id = '$dataflow_id' AND owner_epoch IS NOT NULL
              AND owner_epoch <> '$killed_epoch' AND owner_epoch <> '$first_epoch';" 2>/dev/null || true)
        [ -n "$second_epoch" ] && break
        attempts=$((attempts + 1))
        sleep 0.1
    done
    [ -n "$second_epoch" ] || { echo "third runtime did not take ownership" >&2; exit 1; }
    echo "double SIGKILL epochs: $first_epoch -> $killed_epoch -> $second_epoch"
fi

completed=""
attempts=0
while [ "$attempts" -lt 250 ]; do
    completed=$(query "
        SELECT COUNT(*) FROM dataflow_activations a
        JOIN dataflows d ON d.dataflow_id = a.dataflow_id
        WHERE a.dataflow_id = '$dataflow_id'
          AND a.generation = 1
          AND NOT a.desired_active
          AND a.owner_epoch = '$second_epoch'
          AND d.status = 'completed';
    " 2>/dev/null || true)
    [ "$completed" = "1" ] && break
    attempts=$((attempts + 1))
    sleep 0.1
done
[ "$completed" = "1" ] || {
    echo "recovered workflow did not complete exactly once" >&2
    tail -120 "$phase2_log" >&2 || true
    exit 1
}

if [ "$scenario" = "stable_prefix" ]; then
    verdict=""
    attempts=0
    while [ "$attempts" -lt 100 ]; do
        verdict=$(query "SELECT verdict FROM restart_prefix_verdict LIMIT 1;" 2>/dev/null || true)
        [ -n "$verdict" ] && break
        attempts=$((attempts + 1))
        sleep 0.1
    done
    [ "$verdict" = "ok" ] || {
        echo "requests across the restart do not extend each other: ${verdict:-no verdict}" >&2
        tail -40 "$phase2_log" >&2 || true
        exit 1
    }
    fold_rows=$(query "SELECT COUNT(*) FROM dataflow_data WHERE dataflow_id = '$dataflow_id' AND type = 'agent.input';")
    turn_notes=$(query "SELECT COUNT(*) FROM dataflow_data WHERE dataflow_id = '$dataflow_id' AND type = 'agent.memory' AND key = 'turn_context';")
    skipped=$(query "SELECT COUNT(*) FROM dataflow_data WHERE dataflow_id = '$dataflow_id' AND type = 'agent.observation' AND key LIKE '%_checkpoint_skipped';")
    workflows=$(query "SELECT COUNT(*) FROM dataflows;")
    [ "$fold_rows|$turn_notes|$workflows" = "1|1|1" ] && [ "$skipped" -gt 0 ] || {
        echo "stable prefix rows are wrong: $fold_rows|$turn_notes|$skipped|$workflows" >&2
        exit 1
    }
    echo "$dialect stable prefix restart proof passed: $dataflow_id $first_epoch -> $second_epoch"
    exit 0
fi

if [ "${scenario#round_}" != "$scenario" ]; then
    added=$(query "SELECT COUNT(*) FROM dataflow_data d JOIN dataflow_nodes n ON n.node_id = d.node_id
        WHERE d.dataflow_id = '$dataflow_id' AND d.type = 'agent.memory' AND d.discriminator = 'fact' AND n.parent_node_id IS NULL;")
    warnings=$(query "SELECT COUNT(*) FROM dataflow_data d JOIN dataflow_nodes n ON n.node_id = d.node_id
        WHERE d.dataflow_id = '$dataflow_id' AND d.type = 'agent.observation' AND d.key = '1_iterations_warning' AND n.parent_node_id IS NULL;")
    expected_added=1
    case "$scenario" in round_settle|round_yield_commands|round_artifact|round_command_delegate|round_yield_delegate|round_staggered_children|round_output_only) expected_added=0 ;; esac
    [ "$added|$warnings" = "$expected_added|1" ] || {
        echo "the round's text did not survive the restart: added=$added warnings=$warnings" >&2
        exit 1
    }
    if [ "$scenario" = "round_command_delegate" ] || [ "$scenario" = "round_yield_delegate" ] \
        || [ "$scenario" = "round_staggered_children" ] || [ "$scenario" = "round_output_only" ]; then
        outputs=$(query "SELECT COUNT(*) FROM dataflow_data WHERE dataflow_id = '$dataflow_id'
            AND type = 'agent.observation' AND key = '1_commands_output';")
        children=$(query "SELECT COUNT(*) FROM dataflow_nodes WHERE dataflow_id = '$dataflow_id'
            AND parent_node_id IS NOT NULL;")
        expected_children=2
        if [ "$scenario" = "round_staggered_children" ] || [ "$scenario" = "round_output_only" ]; then expected_children=3; fi
        [ "$outputs|$children" = "1|$expected_children" ] || {
            echo "delegating round lost command output or duplicated children: outputs=$outputs children=$children" >&2
            exit 1
        }
        if [ "$scenario" = "round_staggered_children" ] || [ "$scenario" = "round_output_only" ]; then
            if [ "$dialect" = "postgres" ]; then
                pending_expr="metadata->>'agent_pending'"
            else
                pending_expr="json_extract(metadata, '$.agent_pending')"
            fi
            pending=$(query "SELECT $pending_expr FROM dataflow_nodes WHERE dataflow_id = '$dataflow_id' AND parent_node_id IS NULL;")
            case "$pending" in false|0) ;; *) echo "pending commands were not cleared: $pending" >&2; exit 1 ;; esac
        fi
        verdict=""
        attempts=0
        while [ "$attempts" -lt 100 ]; do
            verdict=$(query "SELECT verdict FROM restart_command_verdict LIMIT 1;" 2>/dev/null || true)
            [ -n "$verdict" ] && break
            attempts=$((attempts + 1))
            sleep 0.1
        done
        [ "$verdict" = "ok" ] || {
            echo "recovered command output did not reach a paired request: ${verdict:-no verdict}" >&2
            exit 1
        }
    fi
    if [ "$scenario" = "round_double_kill" ]; then
        if [ "$dialect" = "postgres" ]; then
            pending_expr="metadata->>'memory_pending'"
            request_text="convert_from(store_value, 'UTF8')"
        else
            pending_expr="json_extract(metadata, '$.memory_pending')"
            request_text="CAST(store_value AS TEXT)"
        fi
        pending=$(query "SELECT $pending_expr FROM dataflow_nodes WHERE dataflow_id = '$dataflow_id' AND parent_node_id IS NULL;")
        case "$pending" in false|0) ;; *) echo "pending memory was not cleared: $pending" >&2; exit 1 ;; esac
        request=$(query "SELECT $request_text FROM restart_recovery_store
            WHERE store_key = 'agent_recovery:round-double-kill:request:2';")
        case "$request" in *'\"text\":\"added at 1\"'*) ;; *) echo "the next request lost surviving memory" >&2; exit 1 ;; esac
        case "$request" in *'\"text\":\"superseded\"'*) echo "the next request contains replaced memory" >&2; exit 1 ;; esac
    fi
    if [ "$scenario" = "round_settle" ]; then
        if [ "$dialect" = "postgres" ]; then
            settled_expr="metadata #>> '{session_context,settled_by}'"
        else
            settled_expr="json_extract(metadata, '$.session_context.settled_by')"
        fi
        settled=$(query "SELECT $settled_expr FROM dataflow_nodes WHERE dataflow_id = '$dataflow_id' AND parent_node_id IS NULL;")
        [ "$settled" = "wrapper" ] || {
            echo "the restart lost the after_execute proposal: ${settled:-none}" >&2
            exit 1
        }
    fi
    if [ "$scenario" = "round_yield_commands" ]; then
        outputs=$(query "SELECT COUNT(*) FROM dataflow_data WHERE dataflow_id = '$dataflow_id' AND type = 'agent.observation' AND key = '1_commands_output';")
        [ "$outputs" = "1" ] || {
            echo "recovery did not collect the output of the yielded command child: outputs=$outputs" >&2
            exit 1
        }
    fi
    if [ "$scenario" = "round_artifact" ]; then
        artifacts=$(query "SELECT COUNT(*) FROM dataflow_data WHERE dataflow_id = '$dataflow_id' AND type = 'artifact.data';")
        [ "$artifacts" = "1" ] || {
            echo "the re-run tool duplicated its artifact: artifacts=$artifacts" >&2
            exit 1
        }
    fi
    if [ "$scenario" = "round_yield" ]; then
        if [ "$dialect" = "postgres" ]; then
            agent_expr="config->>'agent'"
        else
            agent_expr="json_extract(config, '$.agent')"
        fi
        selected=$(query "SELECT $agent_expr FROM dataflow_nodes WHERE dataflow_id = '$dataflow_id' AND parent_node_id IS NULL;")
        [ "$selected" = "userspace.dataflow.node.agent.stub:stable_prefix_switch_target_agent" ] || {
            echo "the restart lost the switch committed before the yield: $selected" >&2
            exit 1
        }
    fi
    echo "$dialect $scenario restart proof passed: $dataflow_id $first_epoch -> $second_epoch"
    exit 0
fi

if [ "$scenario" = "parallel_tools" ]; then
    if [ "$dialect" = "postgres" ]; then
        call_id_expr="metadata->>'tool_call_id'"
        output_text="convert_from(content, 'UTF8')"
    else
        call_id_expr="json_extract(metadata, '$.tool_call_id')"
        output_text="CAST(content AS TEXT)"
    fi
    fast_observations=$(query "SELECT COUNT(*) FROM dataflow_data WHERE dataflow_id = '$dataflow_id' AND type = 'agent.observation' AND $call_id_expr = 'call-parallel-restart-1';")
    slow_observations=$(query "SELECT COUNT(*) FROM dataflow_data WHERE dataflow_id = '$dataflow_id' AND type = 'agent.observation' AND $call_id_expr = 'call-parallel-restart-2';")
    call_observations=$(query "SELECT COUNT(*) FROM dataflow_data WHERE dataflow_id = '$dataflow_id' AND type = 'agent.observation' AND $call_id_expr IS NOT NULL;")
    # The model's final answer reports how many times each original call id ran.
    attempts=$(query "SELECT COUNT(*) FROM dataflow_data WHERE dataflow_id = '$dataflow_id' AND type = 'dataflow.output' AND $output_text LIKE '%:fast=1:slow=2';")
    workflows=$(query "SELECT COUNT(*) FROM dataflows;")
    [ "$fast_observations|$slow_observations|$call_observations|$attempts|$workflows" = "1|1|2|1|1" ] || {
        echo "parallel tool recovery re-ran a finished call or duplicated an observation: $fast_observations|$slow_observations|$call_observations|$attempts|$workflows" >&2
        query "SELECT $output_text FROM dataflow_data WHERE dataflow_id = '$dataflow_id' AND type = 'dataflow.output';" >&2 || true
        exit 1
    }
    echo "$dialect parallel tools restart proof passed: $dataflow_id $first_epoch -> $second_epoch"
    exit 0
fi

if [ "$scenario" = "behavior" ]; then
    if [ "$dialect" = "postgres" ]; then
        selection_predicate="config->>'agent' = 'userspace.dataflow.node.agent.stub:compact_target_agent' AND metadata->>'behavior_pending' = 'false'"
        target_predicate="metadata->>'agent_id' = 'userspace.dataflow.node.agent.stub:compact_target_agent'"
        call_predicate="metadata->>'tool_call_id' = 'call-behavior-restart-1'"
    else
        selection_predicate="json_extract(config, '$.agent') = 'userspace.dataflow.node.agent.stub:compact_target_agent' AND json_extract(metadata, '$.behavior_pending') = 0"
        target_predicate="json_extract(metadata, '$.agent_id') = 'userspace.dataflow.node.agent.stub:compact_target_agent'"
        call_predicate="json_extract(metadata, '$.tool_call_id') = 'call-behavior-restart-1'"
    fi
    selected=$(query "SELECT COUNT(*) FROM dataflow_nodes WHERE dataflow_id = '$dataflow_id' AND $selection_predicate;")
    actions=$(query "SELECT COUNT(*) FROM dataflow_data WHERE dataflow_id = '$dataflow_id' AND type = 'agent.action';")
    target_actions=$(query "SELECT COUNT(*) FROM dataflow_data WHERE dataflow_id = '$dataflow_id' AND type = 'agent.action' AND $target_predicate;")
    observations=$(query "SELECT COUNT(*) FROM dataflow_data WHERE dataflow_id = '$dataflow_id' AND type = 'agent.observation' AND $call_predicate;")
    results=$(query "SELECT COUNT(*) FROM dataflow_data WHERE dataflow_id = '$dataflow_id' AND type = 'node.result';")
    checkpoints=$(query "SELECT COUNT(*) FROM dataflow_data WHERE dataflow_id = '$dataflow_id' AND type = 'agent.memory';")
    workflows=$(query "SELECT COUNT(*) FROM dataflows;")
    [ "$selected|$actions|$target_actions|$observations|$results|$checkpoints|$workflows" = "1|2|1|1|1|1|1" ] || {
        echo "behavior recovery used a stale selection or duplicated a round: $selected|$actions|$target_actions|$observations|$results" >&2
        exit 1
    }
    echo "$dialect behavior restart proof passed: $dataflow_id $first_epoch -> $second_epoch"
    exit 0
fi

iteration_results=$(query "
    SELECT COUNT(*) FROM dataflow_data
    WHERE dataflow_id = '$dataflow_id'
      AND type IN ('iteration.result', 'iteration.error');
")
[ "$iteration_results" = "4" ] || {
    echo "recovered rolling workflow did not persist exactly four terminal iteration rows" >&2
    exit 1
}

cursor_rows=$(query "
    SELECT COUNT(*) FROM dataflow_data
    WHERE dataflow_id = '$dataflow_id'
      AND type = 'parallel.progress'
      AND key = 'cursor';
")
[ "$cursor_rows" = "1" ] || {
    echo "rolling cursor was not compacted to one mutable row: $cursor_rows" >&2
    exit 1
}

yield_rows=$(query "
    SELECT COUNT(*) FROM dataflow_data
    WHERE dataflow_id = '$dataflow_id'
      AND type = 'node.yield';
")
[ "$yield_rows" = "1" ] || {
    echo "superseded rolling barriers were not compacted: $yield_rows" >&2
    exit 1
}

yield_result_rows=$(query "
    SELECT COUNT(*) FROM dataflow_data
    WHERE dataflow_id = '$dataflow_id'
      AND type = 'node.yield.result';
")
[ "$yield_result_rows" -le 1 ] || {
    echo "superseded rolling yield results were not compacted: $yield_result_rows" >&2
    exit 1
}

workflow_count=$(query "SELECT COUNT(*) FROM dataflows;")
[ "$workflow_count" = "1" ] || { echo "restart created duplicate workflows: $workflow_count" >&2; exit 1; }

echo "$dialect restart proof passed: $dataflow_id $first_epoch -> $second_epoch"
