.PHONY: dev test lint typecheck profile chaos bench mutate run worker audit openapi up down clean

UV ?= uv

dev:  ## install locked deps + git hooks
	$(UV) sync --frozen
	-$(UV) run pre-commit install --install-hooks
	-$(UV) run pre-commit install --hook-type commit-msg

test:  ## fast suite: unit + integration + property + e2e, with coverage gate
	$(UV) run pytest --cov --cov-report=term-missing

lint:
	$(UV) run ruff check .
	$(UV) run ruff format --check .

typecheck:
	$(UV) run mypy

profile:  ## flamegraph + where a worker's time goes (needs py-spy)
	$(UV) run py-spy record --idle --subprocesses --format raw -o bench/results/profile_raw.txt --rate 200 -- python -m bench.profile_processing
	$(UV) run py-spy record --idle --subprocesses --format flamegraph -o bench/results/flamegraph.svg --rate 200 -- python -m bench.profile_processing
	$(UV) run python -m bench.profile_summary

chaos:  ## 1000 generated delivery permutations; writes bench/results/chaos.csv
	$(UV) run python -m bench.chaos_run --runs 1000

bench:  ## throughput benchmark; writes bench/results/throughput.csv
	$(UV) run python -m bench.throughput

mutate:  ## mutation-test the pure decision modules; see docs/mutation.md
	$(UV) run python -m bench.mutation_run

audit:
	$(UV) run pip-audit

openapi:  ## regenerate docs/openapi.json
	$(UV) run python -m reconciler.cli openapi > docs/openapi.json

run:  ## embedded Postgres + API on :8000 (no Docker needed)
	$(UV) run python -m reconciler.devserver

worker:
	$(UV) run reconciler worker

up:  ## real Postgres via docker compose
	docker compose up --build

down:
	docker compose down -v

clean:
	rm -rf .venv .mypy_cache .ruff_cache .pytest_cache .hypothesis .coverage htmlcov mutants
