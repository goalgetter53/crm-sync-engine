.PHONY: install lint typecheck test test-pg check bench fake demo plan apply clean

PY ?= python
MAPPING ?= examples/mapping.yaml

install:
	$(PY) -m pip install -e ".[dev,postgres]"

lint:
	ruff check src tests bench
	ruff format --check src tests bench

typecheck:
	mypy

test:
	pytest

# Run the integration suite against Postgres (needs a reachable database).
test-pg:
	CRM_SYNC_TEST_PG=$${CRM_SYNC_TEST_PG:-postgresql+psycopg://crm:crm@localhost:5432/warehouse} \
		pytest tests/test_sync_integration.py

check: lint typecheck test

bench:
	$(PY) bench/run_bench.py

fake:
	$(PY) -m fake_hubspot --port 8090

demo:
	WAREHOUSE_URL=sqlite:///warehouse.db crm-sync demo-seed --contacts 500

plan:
	crm-sync plan $(MAPPING)

apply:
	crm-sync apply $(MAPPING)

clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache .hypothesis *.db build dist
