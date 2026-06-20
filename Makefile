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

lint:
	cd $(TEST_DIR) && wippy lint

install:
	cd $(TEST_DIR) && wippy install

clean:
	cd $(TEST_DIR) && rm -f $(TEST_DB) $(TEST_DB)-wal $(TEST_DB)-shm
