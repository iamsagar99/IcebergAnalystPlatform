# Contributing

Issues and pull requests are welcome. Keep changes small and tie them to one of the problems P1-P10 or to a
documented gap.

## Ground rules

* Spark owns table metadata and writes. Trino stays read-only; Kyuubi engines stay read-only for analysts.
* Nothing is marked as verified without a run. If you change the generator, re-record `tests/expected.json`
  from a real seed and confirm a second seed produces the same numbers.
* Every configuration key you add gets a `[LOCAL]` / `[PROD]` note where the value differs in production.
* Keep docs/implementation-decisions.md current: version bumps need the documentation page you checked.
* Measured numbers go to `benchmarks/results/` only when they come from a real run.

## Workflow

```bash
pre-commit install            # ruff, shellcheck, json/yaml checks
make lint
scripts/start.sh --engine both
scripts/seed.sh
scripts/validate.sh --engine both
```

Pull requests should state which tests ran and on which Docker memory budget.
