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
- `phase0/` — Phase-0 vertical-slice spike + findings (ADR 0008/0026): the
  DuckDB-on-parquet join→materialize→presign path (`duckdb_query.py`), the real
  result-size measurement (`measure_results.py`), and the host-decision writeup
  (`FINDINGS.md`). Not the production handler; informs it.
- `src/` — **deleted** in Phase-0 (the Athena setup/scratch scripts, per ADR 0008).
- `data/core_schema.csv` — schema used to generate the Athena `CREATE TABLE`
  (legacy; slated for removal once the rewrite settles column-allowlist validation).
- `.github/workflows/sam_pipeline.yaml` — CI/CD (stg/prod).
- `tests/test_query.py`, `events/event.json` — tests + sample event.

## API overhaul (planned; not yet built)

This repo is the modernized API. The architecture is **decided and canonical in
`nccs-contracts`** — do not re-derive it here:

- **ADR 0008** — *Modernize the Dataexplorer API*: the canonical API design
  (DuckDB runtime, result delivery, results bucket + 30-day lifecycle, usage
  telemetry, repo/naming). Its "Repo and naming" section (finalized 2026-06-04)
  is what realizes this as **`sector-in-brief-api`**.
- **ADR 0026** — *Data-Download UX: Durable Links, Email Receipt, Download
  Telemetry*: refines 0008's download path — always materialize→S3→presigned
  URL ("pattern B"), a durable `/download/{job_id}` endpoint backed by an S3
  request registry, default-on email receipt, and a distinct `download`
  telemetry event.
- Both rest on **ADR 0003** (DuckDB, not Athena) and **ADR 0016** (join the
  separate contracts at query time; no pre-merged table).

In short: replace the Athena handler with **DuckDB on parquet**, reading the
contracted CORE tiers + BMF-geocoded from `s3://nccsdata/...` **read-only** and
joining on `EIN` at query time; write results to a new
`sector-in-brief-api-results-{stg|prod}` bucket (30-day lifecycle). Deliver via
**pattern B** — materialize the result to S3 and hand back a presigned URL — and
email the requester a **durable `/download/{job_id}` link by default** (re-runs
the query if the result object has been swept), backed by an S3 request registry
(`requests/{job_id}.json`; no runtime database). Log every request / materialize
/ download as NDJSON for the monthly rollup into the contracted `usage-api`
artifact. Keep the `sector-in-brief` dashboard's Data-Download payload
backward-compatible. Delete the old Athena setup and `src/` scratch. Runtime host
(AWS App Runner vs Lambda) is provisional pending a Phase-0 measurement of real
result sizes.

When executing, reference **ADR 0008 / 0026** in commit messages and reconcile
back into `nccs-contracts` (see its `CONTRIBUTING.md`).

## Conventions

- Conventional Commits for messages. Don't commit `.claude/settings.local.json`,
  secrets, or notebooks with embedded credentials (see `.gitignore`).
