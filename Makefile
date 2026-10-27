# Convenience targets; the scripts under scripts/ are the source of truth.
ENGINE ?= both
CONCURRENCY ?= 5 10

.PHONY: help build up seed validate bench down purge lint catalog maintenance

help:           ## list targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*##' '{printf "  %-12s %s\n", $$1, $$2}'

build:          ## build the tools, spark and kyuubi images
	cd infrastructure && docker compose -f docker-compose.yml -f docker-compose.trino.yml -f docker-compose.kyuubi.yml build tools spark kyuubi

up:             ## start core + ENGINE (trino|kyuubi|both|core)
	scripts/start.sh --engine $(ENGINE) --no-build

seed:           ## deterministic seed: tables, bronze, silver (CDC), gold
	scripts/seed.sh

validate:       ## health + catalog + pytest for ENGINE
	scripts/validate.sh --engine $(ENGINE)

bench:          ## benchmark ENGINE at CONCURRENCY
	scripts/benchmark.sh --engine $(ENGINE) --concurrency "$(CONCURRENCY)"

catalog:        ## list tables, snapshots and current snapshot ids
	scripts/catalog-info.sh

maintenance:    ## expire snapshots + remove orphan files (dry run)
	spark/spark-submit.sh maintenance.py --dry-run

down:           ## stop, keep volumes
	scripts/stop.sh

purge:          ## stop and delete volumes
	scripts/stop.sh --purge

lint:           ## static checks (ruff, shellcheck, compose config, json)
	ruff check . && ruff format --check .
	shellcheck -x scripts/*.sh spark/spark-submit.sh infrastructure/catalog/init-postgres.sh
	cd infrastructure && cp -n .env.example .env && docker compose -f docker-compose.yml -f docker-compose.trino.yml -f docker-compose.kyuubi.yml config -q
	for f in $$(git ls-files '*.json'); do python3 -m json.tool "$$f" >/dev/null || exit 1; done
