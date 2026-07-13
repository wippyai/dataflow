# Dataflow module — test and lint helpers.
#
# Tests run from the test/ harness. app:db is file-backed rather than :memory:
# because a plain in-memory DB gives each pooled connection its own empty
# database, so the schema vanishes ("no such table: dataflow_data") under the
# parallel connections the recovery/signal suites open. `make test` recreates
# the DB each run for a clean slate.

TEST_DIR := test
TEST_DB  := .wippy/test.db

.PHONY: test test-static lint install clean

test: clean test-static
	cd $(TEST_DIR) && wippy run test

test-static:
	@if rg -n "keeper\\.views\\.dataflow|dataflow-link|Open full view" src/session/views/state.jet; then \
		echo "dataflow state view must not link to keeper-owned UI"; \
		exit 1; \
	fi
	@awk '''BEGIN { in_user=0; in_wake=0; user_has_wake=0; wake_has_service=0 } \
		/^  - name: user_security_scope$$/ { in_user=1; in_wake=0; next } \
		/^  - name: wake_security_scope$$/ { in_user=0; in_wake=1; next } \
		/^  - name: / { in_user=0; in_wake=0 } \
		in_user && /userspace\.dataflow\.runner:wake_process\.service/ { user_has_wake=1 } \
		in_wake && /userspace\.dataflow\.runner:wake_process\.service/ { wake_has_service=1 } \
		END { \
			if (user_has_wake) { print "user_security_scope must not grant the wake service"; exit 1 } \
			if (!wake_has_service) { print "wake_security_scope must grant the wake service"; exit 1 } \
		}''' src/_index.yaml

lint:
	cd $(TEST_DIR) && wippy lint

install:
	cd $(TEST_DIR) && wippy install

clean:
	cd $(TEST_DIR) && rm -f $(TEST_DB) $(TEST_DB)-wal $(TEST_DB)-shm
