# sector-in-brief-api

AWS SAM serverless API that exports NCCS nonprofit data on demand: it runs
**DuckDB on Parquet** over the contracted NCCS archives in S3, materializes the
result to a dedicated results bucket, and hands back a durable download link
(emailed to the requester by default).

Replaces the archived **`nccs-dataexplorer-api`** (which used AWS Athena). The
architecture is decided and canonical in **`nccs-contracts`** — ADR 0008
(modernize the API: DuckDB, not Athena), ADR 0026 (durable links + email receipt
+ telemetry), ADR 0016 (join the contracts at query time), ADR 0029 (BMF
org-level query mode), and ADR 0030 (async giant-export worker).

## How it works

1. **`POST /data`** (Lambda Function URL, `AWS_IAM`-authed — no API Gateway, so no
   29s cap) triggers the query Lambda.
2. The Lambda validates the request against the live Parquet schema, builds a
   parameterized SQL query, and runs it with **DuckDB** directly against the
   CORE tiers + BMF-geocoded Parquet in `s3://nccsdata/...` (read-only), joining
   on `EIN` at query time. Filter values are bound parameters.
3. The result is materialized to `s3://sector-in-brief-api-results-{stg|prod}/`
   (30-day lifecycle) as **CSV or Parquet**, alongside a data dictionary.
4. An SES email with a **durable `/download/{job_id}` link** is sent to the
   requester. Every request / materialize / download is logged as NDJSON for the
   monthly usage rollup.

### Query modes

`POST /data` runs in one of two modes, chosen by the `source` field:

- **`source: "core"`** (default) — the **per-filing** export. Joins the
  requested CORE form tiers (`tax_years` × `forms`) to **BMF-geocoded** on `EIN`,
  so each row is an organization's filing for a tax year.
- **`source: "bmf"`** (ADR 0029) — an **org-level** export straight from the
  **BMF master registry** (one row per organization; no `tax_years`/`forms`).
  Use `active_years` to keep only orgs whose lifespan overlaps the given years.

Either way the result is enriched from the **BMF-geocoded** archive with
crosswalk-derived geography (county / CBSA / Census region) and the
dashboard-canonical `org_type` and `nteev2_subsector` labels, mirroring
`sector-in-brief-data`.

### Request shape

```json
{
  "tax_years": [2019],
  "forms": ["990", "990ez"],
  "columns": ["ein", "org_name_display", "geo_state_abbr", "total_revenue"],
  "filters": { "geo_state_abbr": ["CA"] },
  "format": "csv",
  "email": "requester@example.org"
}
```

- `forms` — subset of `["990", "990ez", "990pf", "990combined"]` (defaults to all).
- `format` — `"csv"` (default) or `"parquet"`.
- `"estimate": true` — return an exact row count + sampled byte estimate only
  (no S3 write, no email), for a size pre-check.
- `source` / `active_years` — select the BMF org-level mode (see **Query modes**).

### Response shapes

`POST /data` returns one of **three** shapes — clients must branch on `statusCode`:

- **`200`** — result ready: a fresh presigned `result.url`, the durable
  `download_url`, the data dictionary, and `row_count`.
- **`202`** — large export routed to the async worker (ADR 0030): `status:
  "pending"` plus the durable `download_path`. The worker materializes the result
  and emails the link when done; clients poll `GET /download/{job_id}`
  (`202` while pending → `302` to the presigned URL when ready) and/or wait for
  the email.
- **`400`** — validation error (bad columns/filters/format/etc.).

**`GET /download/{job_id}`** (public) is the durable emailed link: it `302`s to a
freshly-issued presigned URL, re-running the query from the stored registry
params if the 30-day lifecycle has already swept the result object.

## Async routing (ADR 0030)

Geographically-broad requests are size-estimated first; if the estimate exceeds
`AsyncThresholdBytes` (default **8 GB**, under Lambda's 10 GB cap), the job is
dispatched to a one-shot **Fargate worker** (same handler code) and the API
returns `202`. State/region-bounded requests skip the estimate and stay on the
fast synchronous path. Deploying with an empty `WorkerVpcId` disables the worker
— the stack then runs everything synchronously and degrades safely.

## Layout

- `template.yaml` — SAM template: query Lambda (`sector-in-brief-api-query-${Stage}`),
  public download Lambda (`-download-`, gated `DOWNLOAD_ONLY`), usage-rollup
  Lambda, results bucket + lifecycle, and the Fargate worker (ECS cluster, task
  def, IAM).
- `query/query.py` — **the deployed Lambda** (`CodeUri: ./query/`, handler
  `query.lambda_handler`); also the Fargate worker entrypoint (`run_async_job`).
  Deps in `query/requirements.txt`.
- `openapi.yaml` — API contract.
- `docs/` — deploy guide, dashboard-integration handoffs (base / BMF mode /
  async), and the `nccs-contracts` reconcile notes.
- `phase0/` — Phase-0 vertical-slice spike + host-decision findings (ADR 0008/0026).
- `tests/test_query.py`, `events/event.json` — tests + sample event.
- `.github/workflows/sam_pipeline.yaml` — CI/CD (stg/prod).

## Links

- GitHub: <https://github.com/UrbanInstitute/sector-in-brief-api> (public; org
  canonical name is `UI-Research`).
- Architecture of record: `nccs-contracts` — ADR 0008 / 0016 / 0026 / 0029 / 0030.
