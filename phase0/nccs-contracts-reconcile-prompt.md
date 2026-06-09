# Reconcile prompt — paste into a fresh Claude session opened in `nccs-contracts`

You are working in the **`nccs-contracts`** repo (the canonical decision/contract
surface for the NCCS data ecosystem). A sibling repo, **`sector-in-brief-api`**
(the modernized data API, at `../sector-in-brief-api`), just completed **Phase-0**
of its build, and several findings need reconciling back into the contracts per
this repo's `CONTRIBUTING.md`. Do not re-derive the architecture — it is decided
in the build; your job is to bring the contracts into line with what Phase-0
measured and confirmed, following the ADR/contract conventions here.

## Read first (evidence)

- `../sector-in-brief-api/phase0/FINDINGS.md` — the Phase-0 writeup (size
  distribution, in-region latency, the host decision). The sector-in-brief-api
  work is on local branch `docs/adr-0020-pointer` (not yet pushed) — read it at
  the sibling path.
- This repo: `decisions/0008-modernize-dataexplorer-api.md`,
  `decisions/0026-data-download-durable-links-and-telemetry.md`,
  `decisions/0003-retire-athena-for-duckdb.md`, `contracts/core-990.yml`,
  `contracts/bmf-master-geocoded.yml`, `contracts/usage-api.yml`, and
  `CONTRIBUTING.md`.

## What Phase-0 decided / confirmed (the facts to reconcile)

1. **Host decision is no longer provisional.** ADR 0008 left the runtime host
   open ("App Runner vs Lambda, pending a Phase-0 measurement of result sizes").
   Phase-0 measured it two ways and **reversed the earlier App-Runner lean** to a
   **Lambda-first hybrid**:
   - Real result-size distribution (2,539 production result CSVs): violently
     bimodal — p50 0.1 MB, p95 11.7 GB, max 51 GB; 25.6% > 100 MB, 5.6% > 10 GB.
   - In-region throughput (EC2 c5.2xlarge, us-east-1): all-years DuckDB join
     materialized 331 MB in 3.2s = **~104 MB/s** (vs 219s/1.5 MB/s from a local
     machine — the local number was pure egress, not a host signal).
   - At ~100 MB/s, Lambda's 15-min wall ≈ 90 GB of headroom > the 51 GB max, so
     throughput/wall-time no longer rules Lambda out. **Decision: Lambda
     materializes p50–p95 (≤ ~10 GB); an async non-Lambda worker handles only the
     p99+ giant**, surfaced through ADR 0026's durable `/download/{job_id}`. The
     binding Lambda limit narrows to join memory (10 GB), still under test as the
     first build step.
   → **Reconcile ADR 0008's "Result delivery"/runtime-host text** to record the
     measured Lambda-first hybrid (cite Phase-0), and mark **ADR 0026 pattern B
     (materialize→S3→presigned URL) as confirmed-by-data** (38.5% of results
     exceed the 6 MB inline cap, so bytes can never flow through the API).

2. **Bucket naming is finalized** (this was flagged as a deferred follow-up when
   `sector-in-brief-api/CLAUDE.md` was repointed; now is the time). Standardize on
   **`sector-in-brief-api-results-{stg|prod}`** for results. ADR 0008's working
   names **`nccs-data-api-results`** and **`s3://nccs-data-api/logs/...`** are
   stale — reconcile 0008's text to the final names. Check `usage-api.yml`'s log
   path (`s3://sector-in-brief-api/logs/queries/...`) is consistent.

3. **Ground-truth confirmations for the contracts:**
   - `core-990.yml` **open item #3 (parquet not yet contract-canonical) is now
     load-bearing**: Phase-0 confirmed core parquet exists physically
     (`processed/core/{yr}/990/core_{yr}_990.parquet`, written by the producer's
     `R/09_parquet.R`) and DuckDB reads it, but CSV is still the canonical format.
     The API service tier depends on **promoting core parquet to canonical** —
     note this dependency on the open item. Also confirmed: the join key is
     lowercase **`ein`**.
   - `bmf-master-geocoded.yml`: confirmed the parquet is canonical at the
     contracted path and **`ein` is unique** (3,672,933 rows, all distinct) — so
     the consumer-side query-time EIN join (ADR 0016) does not fan out. Worth a
     one-line note supporting the no-canonical-merge decision.
   - `usage-api.yml`: when filling its TODO schema, align to **ADR 0026 §4's event
     taxonomy** — `request_created`, `export_materialized`, `download` — and the
     finalized bucket naming above. (Schema realizes once the rollup ships; just
     align names/taxonomy now.)

## Out of scope for you

- **ADR 0003** is being updated **by the maintainer separately** (the core-parquet
  prerequisite). Do **not** edit 0003; if you reference the parquet-canonical
  dependency, point at 0003 rather than changing it, and flag any coordination
  needed.

## How to proceed

Follow `CONTRIBUTING.md`: propose the edits (ADR status/date/cross-link
conventions, drift entries if applicable), keep changes minimal and well-cited to
Phase-0, and surface anything ambiguous for maintainer review rather than guessing.
Reference **ADR 0008 / 0026** in commit messages. Summarize the proposed changes
before applying.
