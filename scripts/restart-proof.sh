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
        JOIN dataflow_nodes n ON n.dataflow_id = a.dataflow_id
        WHERE a.desired_active AND n.status = 'running'
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

workflow_count=$(query "SELECT COUNT(*) FROM dataflows;")
[ "$workflow_count" = "1" ] || { echo "restart created duplicate workflows: $workflow_count" >&2; exit 1; }

echo "$dialect restart proof passed: $dataflow_id $first_epoch -> $second_epoch"
