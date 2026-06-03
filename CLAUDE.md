# sector-in-brief-api

AWS SAM serverless API that queries NCCS nonprofit data archives via AWS Athena
and emails the requester a download link.

## Status (as of 2026-06-03)

This repo replaces the archived **`nccs-dataexplorer-api`** (renamed for clarity).
History was started clean — the initial commit is seeded from the old repo's
working tree, minus a scratch notebook, build/cache dirs, and editor settings.

- GitHub: https://github.com/UrbanInstitute/sector-in-brief-api (public; org
  canonical name is `UI-Research`)
- Old repo `UI-Research/nccs-dataexplorer-api` is archived (read-only).

## How it works

1. `POST /data` (API Gateway) triggers the Lambda.
2. Lambda builds a SQL query from the request's `variables` + `filters`, runs it
   on a Parquet file in S3 via Athena.
3. Results are written to an NCCS S3 bucket.
4. An SES email with a download link is sent to the requester.

Request shape:
```json
{ "user": {"name": "...", "email": "..."}, "variables": ["col1"], "filters": {"col1": ["v1","v2"]} }
```

## Layout

- `template.yaml` — SAM template (API Gateway, Lambda `nccs-api-query-${Stage}`, IAM).
- `query/query.py` — **the deployed Lambda** (`CodeUri: ./query/`, handler `query.lambda_handler`); deps in `query/requirements.txt`.
- `src/` — dev/setup scripts (`athena_setup.py`, `where_statement.py`, `send_email.py`,
  `create_table_query.py`, `lambda_handler.py`, `s3_copy.py`, `table.py`). Some are
  scratch/WIP with known bugs (e.g. `where_statement.py` references an undefined
  `query`; `lambda_handler.py` uses `json`/`boto3`/`sender_email` without importing them).
- `data/core_schema.csv` — schema used to generate the Athena `CREATE TABLE`.
- `.github/workflows/sam_pipeline.yaml` — CI/CD (stg/prod).
- `tests/test_query.py`, `events/event.json` — tests + sample event.

## API overhaul (planned; not yet built)

This repo is the modernized API. The architecture is **decided and canonical in
`nccs-contracts`** — do not re-derive it here:

- **`nccs-contracts` ADR 0020** — *Realize the Modernized API as sector-in-brief-api*
  (finalizes names, host, buckets). Implements **ADR 0008** (modernize the API),
  per **ADR 0003** (DuckDB, not Athena) and **ADR 0016** (join the separate
  contracts at query time; no pre-merged table).

In short: replace the Athena handler with **DuckDB on parquet**, reading the
contracted CORE tiers + BMF-geocoded from `s3://nccsdata/...` **read-only** and
joining on `EIN` at query time; write results to a new
`sector-in-brief-api-results-{stg|prod}` bucket (30-day lifecycle); keep the
`sector-in-brief` dashboard's Data-Download payload backward-compatible.
Delete the old Athena setup and `src/` scratch. Runtime host (AWS App Runner vs
Lambda) is provisional pending a Phase-0 measurement of real result sizes.

When executing, leave `ADR 0020 step N` breadcrumbs in commit messages and
reconcile back into `nccs-contracts` (see its `CONTRIBUTING.md`).

## Conventions

- Conventional Commits for messages. Don't commit `.claude/settings.local.json`,
  secrets, or notebooks with embedded credentials (see `.gitignore`).
