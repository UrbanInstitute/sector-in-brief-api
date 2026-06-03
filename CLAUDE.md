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

## Planned next phase: API overhaul (not yet started)

Scope still to be defined with the user. Candidate work:
- **Rename internals** still saying "nccs": bucket `nccsdata`, table `nccs_core` /
  `CORE_FULL_V0_1`, `nccsLambda` / `nccsAPI` in `template.yaml`, README, `samconfig.toml`.
- **Restructure** `src/` scratch scripts vs. the real `query/query.py`; fix the broken `src/` bits.
- Possible behavioral/API changes, plus tests/CI/dependency cleanup.

Ask the user for the overhaul scope before making changes.

## Conventions

- Conventional Commits for messages. Don't commit `.claude/settings.local.json`,
  secrets, or notebooks with embedded credentials (see `.gitignore`).
