#!/bin/sh
set -eu

repo_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
test_dir="$repo_dir/test"
db_file="$test_dir/.wippy/restart-proof.db"
phase1_log="$test_dir/.wippy/restart-phase1.log"
phase2_log="$test_dir/.wippy/restart-phase2.log"
runtime_pid=""
dialect="${DATAFLOW_RESTART_DIALECT:-sqlite}"
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
    if [ "$dialect" = "postgres" ]; then
        wippy run -s --profile postgres --profile "$restart_profile" \
            --set "vars.postgres_host=$pg_host" \
            --set "vars.postgres_port=$pg_port" \
            --set "vars.postgres_database=$pg_database" \
            --set "vars.postgres_username=$pg_username" \
            --set "vars.postgres_password=$pg_password" >"$runtime_log" 2>&1 &
    else
        wippy run -s --profile sqlite --profile "$restart_profile" \
            --set vars.sqlite_file=./.wippy/restart-proof.db >"$runtime_log" 2>&1 &
    fi
    runtime_pid=$!
}

rm -f "$db_file" "$db_file-wal" "$db_file-shm" "$phase1_log" "$phase2_log"

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
    [ -n "$phase1" ] && break
    if ! kill -0 "$runtime_pid" 2>/dev/null; then
        echo "first runtime exited before creating an active workflow" >&2
        tail -80 "$phase1_log" >&2 || true
        exit 1
    fi
    attempts=$((attempts + 1))
    sleep 0.1
done
[ -n "$phase1" ] || { echo "timed out waiting for first runtime ownership" >&2; exit 1; }

dataflow_id=${phase1%%|*}
first_epoch=${phase1#*|}
[ -n "$first_epoch" ] || { echo "first runtime did not claim an epoch" >&2; exit 1; }

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

stop_runtime "$runtime_pid"
runtime_pid=""

preserved=$(query "
    SELECT COUNT(*) FROM dataflow_activations a
    JOIN dataflows d ON d.dataflow_id = a.dataflow_id
    WHERE a.dataflow_id = '$dataflow_id'
      AND a.generation = 1
      AND a.desired_active
      AND d.status = 'running';
")
[ "$preserved" = "1" ] || { echo "graceful runtime shutdown destroyed active intent" >&2; exit 1; }

start_runtime restart_observe "$phase2_log"

second_epoch=""
attempts=0
while [ "$attempts" -lt 200 ]; do
    second_epoch=$(query "
        SELECT owner_epoch FROM dataflow_activations
        WHERE dataflow_id = '$dataflow_id'
          AND desired_active
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
