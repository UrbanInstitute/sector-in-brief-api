# Reconcile prompt (decision-level) — paste into a Claude session in `nccs-contracts`

You are in the **`nccs-contracts`** repo. The modernized **`sector-in-brief-api`**
(`../sector-in-brief-api`) is now BUILT (slices 1–5.1) and DEPLOYED TO STAGING via a
green CI/CD pipeline with a post-deploy smoke gate. Reconcile the now-**final
decisions** back into the contracts per this repo's `CONTRIBUTING.md`. Don't
re-derive anything — record what was decided and realized.

**Scope — DECISION-level only.** Do NOT yet touch:
- **ADR 0003** — the maintainer is updating it separately (the core-parquet
  prerequisite). Reference it; don't edit it.
- **Realized schemas / "as running" state** — `usage-api.yml`'s actual parquet
  columns and `ARCHITECTURE.md`'s "as running in prod" wait until the API is in
  prod and the monthly rollup has run once (per `usage-api.yml`'s own "fields fill
  in when the job ships" and ADR 0008's follow-up #2).

If a prior in-flight reconcile already landed some of this, treat this as the
superset and only add what's missing. (Supersedes the Phase-0-era
`../sector-in-brief-api/phase0/nccs-contracts-reconcile-prompt.md`.)

## Read first
`../sector-in-brief-api/`: `openapi.yaml`, `phase0/FINDINGS.md`, `template.yaml`,
`query/query.py`. This repo: `decisions/0008`, `0026`, `0016`, `0021`, `0023`,
`contracts/usage-api.yml`, `bmf-master-geocoded.yml`, `county-fips-crosswalk.yml`,
`cbsa-crosswalk.yml`, `ct-planning-region-crosswalk.yml`, `core-990.yml`,
`CONTRIBUTING.md`.

## Decisions to reconcile

1. **ADR 0008 — host is no longer provisional → Lambda-first.** Phase-0 measured
   it in a real Lambda: the worst realistic query (widest projection, all 990
   years/states) completed in **76s using 6.0 GB of 10 GB**, materializing 4.7 GB
   at ~67 MB/s; at ~100 MB/s in-region the 15-min wall is ~60 GB of headroom > the
   51 GB observed max. So replace the "App Runner vs Lambda, provisional" text with
   **Lambda-first**: Lambda materializes the bulk of the distribution; an async
   non-Lambda worker is reserved only for the rare multi-tier giant. Pattern B
   (ADR 0026) confirmed by data (38.5% of real results exceed the 6 MB inline cap).

2. **ADR 0008 — finalize naming** (the long-deferred follow-up). Working names
   `nccs-data-api-results` / `s3://nccs-data-api/logs/…` → realized:
   results bucket **`sector-in-brief-api-results-{stg|prod}`** (30-day lifecycle,
   scoped to the `results/` prefix); per-query logs at `logs/queries/{YYYY-MM-DD}/`
   in that bucket; durable-link registry at `requests/{job_id}.json` (longer clock).

3. **ADR 0026 — realized as-built.** Pattern B, durable `GET /download/{job_id}` +
   S3 request registry (re-materialize on lifecycle sweep), default-on SES email
   receipt, and the three NDJSON telemetry events (`request_created`,
   `export_materialized`, `download`) are all built. Two concrete realizations to
   record:
   - **Auth split (refines §5):** `POST /data` is a Function URL with
     `AuthType: AWS_IAM` (the Shiny server signs via a dedicated invoke IAM user);
     `GET /download/{job_id}` is a **separate, public** Function URL (`AuthType:
     NONE`) so the emailed link is clickable. The interface is **OpenAPI in the API
     repo** (`openapi.yaml`) — reference it, don't add a non-S3 contract here.
   - **Size pre-check (§6):** realized as a `"estimate": true` flag on `POST /data`
     (exact row_count + sampled bytes, no materialize) — API computes, dashboard
     presents.

4. **ADR 0016 / 0021 / 0023 — the API is a new consumer that composes joins.** It
   opens DuckDB over the contracted parquets and serves the **BMF × core EIN join**
   at query time (0016), AND composes the **county-fips / cbsa / ct-planning-region
   crosswalk joins** (0021/0023) — mirroring `sector-in-brief-data`'s
   `read_bmf.R`/`county_crosswalk.R`/`derive_dimensions.R` exactly (county label
   join with ambiguous→NULL, CT by `%.2f` coordinate, CBSA on coalesced FIPS,
   census region from state). **Add `UrbanInstitute/sector-in-brief-api` as a
   consumer** of `bmf-master-geocoded`, `county-fips-crosswalk`, `cbsa-crosswalk`,
   and `ct-planning-region-crosswalk`.

5. **`usage-api.yml` — confirm the decided shape** (schema columns still TODO until
   the rollup runs): producer repo `UrbanInstitute/sector-in-brief-api`; per-query
   logs under the results bucket's `logs/queries/`; monthly rollup → contracted
   `s3://nccsdata/usage/api/{YYYY_MM}/queries.parquet` (zstd) + an ADR-0014
   `_manifest.json`; event taxonomy = the three §4 events above. The rollup is
   **stage-aware**: only prod publishes to `nccsdata/usage/`; staging writes to its
   own results bucket so it can't clobber the contracted artifact.

6. **`core-990.yml` open item #3 — note the dependency, don't resolve it.** The API
   service tier reads the core **parquet**, which is present but not yet
   contract-canonical (CSV is). Flag that this is now a live dependency and
   cross-reference the maintainer's ADR 0003 update; do not edit ADR 0003.

## How to proceed
Follow `CONTRIBUTING.md` (ADR status/date/cross-link conventions, drift entries).
Keep edits minimal and cited to the as-built. **Summarize the proposed changes
before applying**, and surface anything ambiguous rather than guessing. Reference
ADR 0008 / 0026 in commit messages.
