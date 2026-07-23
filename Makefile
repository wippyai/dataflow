# Dataflow module — test and lint helpers.
#
# Tests run from the test/ harness. app:db is file-backed rather than :memory:
# because a plain in-memory DB gives each pooled connection its own empty
# database, so the schema vanishes ("no such table: dataflow_data") under the
# parallel connections the recovery/signal suites open. `make test` recreates
# the DB each run for a clean slate.

TEST_DIR := test
TEST_DB  := .wippy/test.db
DATAFLOW_PG_HOST ?= 127.0.0.1
DATAFLOW_PG_PORT ?= 5432
DATAFLOW_PG_DATABASE ?= dataflow_test
DATAFLOW_PG_USERNAME ?= dataflow
DATAFLOW_PG_PASSWORD ?= dataflow
WIPPY ?= wippy
PUBLISH_DRY_RUN_TOKEN ?= wpy_ci_dry_run_0123456789abcdef0123456789abcdef

.PHONY: test test-sqlite test-postgres test-static lint install verify-lock verify-package clean

test: test-sqlite

test-sqlite: clean test-static
	cd $(TEST_DIR) && $(WIPPY) run test --profile sqlite

test-postgres: test-static
	cd $(TEST_DIR) && $(WIPPY) run test --profile postgres \
		--set "vars.postgres_host=$(DATAFLOW_PG_HOST)" \
		--set "vars.postgres_port=$(DATAFLOW_PG_PORT)" \
		--set "vars.postgres_database=$(DATAFLOW_PG_DATABASE)" \
		--set "vars.postgres_username=$(DATAFLOW_PG_USERNAME)" \
		--set "vars.postgres_password=$(DATAFLOW_PG_PASSWORD)"

test-static:
	@command -v rg >/dev/null 2>&1 || { echo "test-static requires ripgrep (rg)"; exit 1; }
	@if rg -n "keeper\\.views\\.dataflow|dataflow-link|Open full view" src/session/views/state.jet; then \
		echo "dataflow state view must not link to keeper-owned UI"; \
		exit 1; \
	fi
	@if rg -n "user_security_scope|runtime_process_(spawn|registry)" src test/_index.yaml; then \
		echo "dataflow execution must inherit the exact caller actor and scope"; \
		exit 1; \
	fi
	@if rg --files-without-match "test\.run_cases\(" src --glob '*_test.lua' | rg -q .; then \
		echo "every source test must execute through test.run_cases"; \
		rg --files-without-match "test\.run_cases\(" src --glob '*_test.lua'; \
		exit 1; \
	fi
	@for test_file in $$(rg --files src -g '*_test.lua'); do \
		test_name="$${test_file##*/}"; \
		test_index="$${test_file%/*}/_index.yaml"; \
		if ! test -f "$$test_index" || ! rg -q --fixed-strings "source: file://$$test_name" "$$test_index"; then \
			echo "source test is not registered in its own index: $$test_file"; \
			exit 1; \
		fi; \
	done
	@if rg -n "name: (wake|sweeper)_security_scope" src/_index.yaml; then \
		echo "wake service authority must be module-owned, not consumer-injected"; \
		exit 1; \
	fi
	@awk 'BEGIN { in_service=0; has_root_scope=0 } \
		/^  - name: wake_process\.service$$/ { in_service=1; next } \
		/^  - name: / { in_service=0 } \
		in_service && /userspace\.dataflow\.security:root/ { has_root_scope=1 } \
		END { if (!has_root_scope) { print "wake service must use the canonical root process scope"; exit 1 } }' src/runner/_index.yaml

lint:
	cd $(TEST_DIR) && $(WIPPY) lint --profile sqlite --ns app --ns 'userspace.dataflow.**'

install:
	cd $(TEST_DIR) && $(WIPPY) install

verify-lock:
	git diff --exit-code -- test/wippy.lock

verify-package:
	@version="$$(awk '/^version:/ { print $$2; exit }' wippy.yaml)"; \
	test -n "$$version"; \
	WIPPY_TOKEN="$(PUBLISH_DRY_RUN_TOKEN)" $(WIPPY) publish --version "$$version" --dry-run

clean:
	cd $(TEST_DIR) && rm -f $(TEST_DB) $(TEST_DB)-wal $(TEST_DB)-shm
