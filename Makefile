# Local dev shortcuts. `make ci` mirrors the GitHub Actions pipeline 1:1.
# Windows: install `make` (e.g. via Chocolatey/Git Bash) or run the commands by hand.

# Single source of truth for the lint/type-check surface. ci.yml delegates to these
# targets (no duplicated file list in YAML). Add a refactored job here in ONE place;
# coverage auto-discovers it via glob, pre-commit via its regex. Keep this list to jobs
# that follow the pure-transform()/main() pattern (legacy compact jobs would fail format).
JOBS := \
	processing/spark_jobs/batch_silver_customers.py \
	processing/spark_jobs/batch_silver_category.py \
	processing/spark_jobs/batch_silver_products.py \
	processing/spark_jobs/batch_silver_branches.py \
	processing/spark_jobs/batch_silver_nou.py \
	processing/spark_jobs/batch_silver_transactions_cdc.py \
	processing/spark_jobs/batch_silver_transaction_details.py \
	processing/spark_jobs/batch_silver_customer_activity_monthly.py \
	processing/spark_jobs/batch_silver_events.py

.PHONY: install lint format typecheck audit test build ci

install:
	python -m pip install --upgrade pip
	pip install -r requirements-dev.txt

lint:
	ruff check tests/ $(JOBS)

format:
	ruff format --check tests/

typecheck:
	mypy tests/ $(JOBS)

# SCA: scan declared deps for known CVEs. Same command CI runs (no duplicated flags).
# Accepted-risk allowlist (gate still blocks any NEW advisory):
#   PYSEC-2025-184      pyspark 3.5.1 — fix is 3.5.2, but the whole cluster (Dockerfile.spark
#                       tarball + all jars) is pinned to 3.5.1; bump cluster + tests together
#                       in a dedicated PR, not here.
#   GHSA-6w46-j5rx-g56g pytest 8.3.4 — dev-only test tooling, low severity; fix is a major
#                       bump to 9.x deferred to avoid breaking the suite.
audit:
	pip-audit -r requirements.txt -r requirements-dev.txt \
	  --ignore-vuln PYSEC-2025-184 \
	  --ignore-vuln GHSA-6w46-j5rx-g56g

# Coverage gate (fail_under) lives in pyproject [tool.coverage.report] — shared with CI.
test:
	pytest --cov --cov-report=term-missing -v

# Sanity-build both runtime images so a broken Dockerfile fails CI, not production.
# Needs a local Docker daemon; CI does the same via buildx (with layer cache).
build:
	docker build -f Dockerfile -t lakehouse-app:ci .
	docker build -f Dockerfile.spark -t lakehouse-spark:ci .

# Mirrors the GitHub Actions quality+security+test gates (build is heavier → run on demand).
ci: lint format typecheck audit test
